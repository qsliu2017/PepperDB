//! In-memory store of rewrite-rule action trees, keyed by relation OID.
//!
//! No C counterpart: it stands in for the `pg_rewrite.ev_action` /
//! `ev_qual` `pg_node_tree` columns. PostgreSQL serializes a rule's action
//! `Query` to text (`nodeToString`) on `InsertRule` and re-parses it
//! (`stringToNode`) when the relcache builds `rd_rules`. The port has not yet
//! crossed the `nodeToString`/`stringToNode` divide, so the live `Query` tree is
//! held here instead. The `pg_rewrite` catalog row is still written (metadata +
//! a deparsed `ev_action` for inspection); this registry is the source of truth
//! for the action tree the rewriter splices in.
//!
//! Ex-shared-memory state (rules.md s6.2): an `Arc` field on `SharedState`, owning
//! its own `parking_lot::Mutex`. `RewriteRuleData` is fully owned (`Node` carries
//! no `Rc`/`Cell`), hence `Send + Sync`.

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::Mutex;

use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::parsenodes::Query;
use crate::postgres_ext::Oid;

/// One rewrite rule's stored data (the registry's owned analog of a
/// `pg_rewrite` row plus its action tree). Cloneable so the relcache can build a
/// `RuleLock` from a snapshot without holding the registry lock.
#[derive(Clone)]
pub struct RewriteRuleData {
    pub rule_id: Oid,
    pub event: CmdType,
    pub is_instead: bool,
    pub enabled: u8,
    pub qual: Option<Node>,
    /// The rule action statements (for ON SELECT views, the single view `Query`).
    pub actions: Vec<Query>,
}

/// All rules for one relation, in catalog order.
#[derive(Clone, Default)]
pub struct RelationRules {
    pub rules: Vec<RewriteRuleData>,
}

/// The rule store. Published process-wide so the rewriter (`query_rewrite`) can
/// reach it without threading `&SharedState` through its many call sites.
///
/// Unlike the truly process-lifetime subsystems (VariableCache / LockManager),
/// the rule store's logical lifetime is one database: a fresh `SharedState`
/// (a new tempdir in tests) must get a fresh store, so the published handle is
/// re-bound on every `SharedState::new` rather than set-once. The `SharedState`
/// stores the same `Arc` it published, so its `rule_registry()` field and the
/// global always agree.
#[derive(Default)]
pub struct RuleRegistry {
    by_relation: Mutex<HashMap<Oid, RelationRules>>,
}

/// The published handle (re-bound per `SharedState::new`; see the type doc).
static GLOBAL: Mutex<Option<Arc<RuleRegistry>>> = Mutex::new(None);

impl RuleRegistry {
    /// (Re)publish the process-wide instance. Replaces any previous handle.
    pub fn set(instance: Arc<Self>) {
        *GLOBAL.lock() = Some(instance);
    }

    /// The currently-published instance, if any.
    #[must_use]
    pub fn get() -> Option<Arc<Self>> {
        GLOBAL.lock().clone()
    }
}

impl RuleRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Install (or, on replace, overwrite same-OID) a rule for `relid`. A rule of
    /// the same `rule_id` already present is replaced in place; otherwise appended.
    pub fn insert(&self, relid: Oid, rule: RewriteRuleData) {
        let mut map = self.by_relation.lock();
        let entry = map.entry(relid).or_default();
        if let Some(slot) = entry.rules.iter_mut().find(|r| r.rule_id == rule.rule_id) {
            *slot = rule;
        } else {
            entry.rules.push(rule);
        }
    }

    /// A snapshot of all rules for `relid`, or `None` if the relation has none.
    #[must_use]
    pub fn rules_for(&self, relid: Oid) -> Option<RelationRules> {
        self.by_relation.lock().get(&relid).cloned()
    }

    /// Whether a rule named-by-OID exists for `relid` (any rule at all).
    #[must_use]
    pub fn has_rules(&self, relid: Oid) -> bool {
        self.by_relation.lock().get(&relid).is_some_and(|r| !r.rules.is_empty())
    }

    /// Drop all rules for `relid` (e.g. DROP VIEW). No-op if absent.
    pub fn forget(&self, relid: Oid) {
        self.by_relation.lock().remove(&relid);
    }
}
