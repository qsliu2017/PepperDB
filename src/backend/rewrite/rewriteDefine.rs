//! Rule definition. Translated from backend/rewrite/rewriteDefine.c
//! (disposition: full leaf, M11-reachable subset).
//!
//! `DefineQueryRewrite` is reached two ways: from `DefineViewRules` with the view's
//! analyzed SELECT `Query` (the `_RETURN` ON SELECT DO INSTEAD rule), and from
//! `DefineRule` for an explicit CREATE RULE. The ON SELECT branch is fully
//! translated; the non-SELECT (INSERT/UPDATE/DELETE) rule branch is staged with a
//! catchable ereport (rules.md s4) -- the substrate is here, the product-query
//! transform grows with updatable rules.
//!
//! The rule action `Query` tree is stored in the in-memory rule registry
//! (rule_registry.rs); `InsertRule` also writes the `pg_rewrite` catalog row
//! (metadata + a placeholder pg_node_tree text) for catalog completeness.

use std::sync::Arc;

use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::parsenodes::Query;
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;

pub const RULE_FIRES_ON_ORIGIN: u8 = b'O';
pub const RULE_FIRES_ALWAYS: u8 = b'A';
pub const RULE_FIRES_ON_REPLICA: u8 = b'R';
pub const RULE_DISABLED: u8 = b'D';

/// The ON SELECT rule of a view is always named this.
const VIEW_SELECT_RULE_NAME: &str = "_RETURN";

#[cold]
fn not_yet_reachable(what: &str) -> ! {
    unimplemented!("{what}: not yet translated for this milestone");
}

/// PG `DefineQueryRewrite`: install a rewrite rule on `event_relid`. For the view
/// `_RETURN` rule (`event_type == CMD_SELECT`, `is_instead`, single SELECT action,
/// no qual, name `_RETURN`) this validates the action against the event relation,
/// records the rule (InsertRule) and flips `relhasrules`. Non-SELECT rules are
/// staged. Returns the new rule's ObjectAddress.
pub async fn define_query_rewrite(
    shared: &Arc<SharedState>,
    rulename: &str,
    event_relid: Oid,
    event_qual: Option<Node>,
    event_type: CmdType,
    is_instead: bool,
    replace: bool,
    action: Vec<Node>,
) -> ObjectAddress {
    use crate::backend::utils::cache::relcache::{
        relation_build_desc, relation_close, relation_id_get_relation,
    };
    use crate::catalog::pg_class::{RELKIND_MATVIEW, RELKIND_RELATION, RELKIND_VIEW};
    use crate::catalog::pg_rewrite::RewriteRelationId;

    // The relation was just created (CREATE VIEW path); warm its relcache entry
    // from pg_class so the relkind checks can read its form.
    let relation = match relation_id_get_relation(event_relid) {
        Some(r) => r,
        None => relation_build_desc(shared, event_relid).await.unwrap_or_else(|| {
            unreachable!("DefineQueryRewrite: event relation {event_relid:?} must exist")
        }),
    };
    let relkind = relation.form().relkind;

    if event_type == CmdType::SELECT {
        // ON SELECT rules belong only on a view (or matview).
        if relkind != RELKIND_VIEW && relkind != RELKIND_MATVIEW {
            relation_close(relation);
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_WRONG_OBJECT_TYPE)
                    .errmsg("relation cannot have ON SELECT rules".to_owned());
            });
            unreachable!("ereport(ERROR) diverges");
        }
        // Exactly one INSTEAD SELECT action, no qual, named _RETURN.
        if action.len() != 1 {
            not_yet_reachable("DefineQueryRewrite: ON SELECT rule with multiple actions");
        }
        let Node::Query(q) = &action[0] else {
            not_yet_reachable("DefineQueryRewrite: ON SELECT action is not a Query");
        };
        if !is_instead || q.commandType != CmdType::SELECT {
            relation_close(relation);
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_INVALID_OBJECT_DEFINITION)
                    .errmsg("rules on SELECT must have action INSTEAD SELECT".to_owned());
            });
            unreachable!("ereport(ERROR) diverges");
        }
        if event_qual.is_some() {
            relation_close(relation);
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_FEATURE_NOT_SUPPORTED)
                    .errmsg("event qualifications are not implemented for rules on SELECT".to_owned());
            });
            unreachable!("ereport(ERROR) diverges");
        }
        // PG accepts the legacy _RET<viewname>; the port requires _RETURN.
        if rulename != VIEW_SELECT_RULE_NAME {
            not_yet_reachable("DefineQueryRewrite: ON SELECT rule not named _RETURN");
        }
    } else {
        // INSERT/UPDATE/DELETE rules: substrate present, product-query transform
        // (the DO ALSO / DO INSTEAD non-SELECT path) grows with updatable rules.
        let _ = (RELKIND_RELATION, replace);
        relation_close(relation);
        not_yet_reachable("DefineQueryRewrite: non-SELECT (INSERT/UPDATE/DELETE) rule firing");
    }

    relation_close(relation);

    // Install the rule (writes the registry + the pg_rewrite row).
    let rule_id = insert_rule(
        shared, rulename, event_type, event_relid, is_instead, event_qual, action, replace,
    )
    .await;

    // Set the relation's relhasrules flag and evict its relcache entry.
    crate::backend::rewrite::rewriteSupport::set_relation_rule_status(shared, event_relid, true)
        .await;

    ObjectAddress { classId: RewriteRelationId, objectId: rule_id, objectSubId: 0 }
}

/// PG `InsertRule`: write the rule into `pg_rewrite` and assign its OID. The action
/// `Query` and qual are also recorded in the in-memory rule registry (the live
/// trees the rewriter splices). Returns the new rule OID.
async fn insert_rule(
    shared: &Arc<SharedState>,
    rulename: &str,
    event_type: CmdType,
    event_rel_id: Oid,
    is_instead: bool,
    event_qual: Option<Node>,
    action: Vec<Node>,
    replace: bool,
) -> Oid {
    use crate::backend::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
    use crate::backend::catalog::catalog::get_new_oid_with_index;
    use crate::backend::catalog::indexing::catalog_tuple_insert;
    use crate::backend::utils::cache::relcache::{relation_close, relation_id_get_relation};
    use crate::catalog::pg_rewrite::{self as r, RewriteOidIndexId, RewriteRelationId};
    use crate::postgres::{BoolGetDatum, CharGetDatum, Datum, ObjectIdGetDatum, PointerGetDatum};

    let already = shared.rule_registry().rules_for(event_rel_id).is_some_and(|rl| {
        rl.rules.iter().any(|rr| rr.event == event_type)
    });
    if already && !replace {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_DUPLICATE_OBJECT)
                .errmsg(format!("rule \"{rulename}\" for relation already exists"));
        });
        unreachable!("ereport(ERROR) diverges");
    }

    let pg_rewrite = relation_id_get_relation(RewriteRelationId)
        .unwrap_or_else(|| unreachable!("pg_rewrite is nailed"));
    let desc = pg_rewrite.rd_att.clone().unwrap_or_else(|| unreachable!("pg_rewrite desc"));

    // Reuse an existing rule's OID on replace; otherwise assign a fresh one.
    let rule_oid = if already {
        shared
            .rule_registry()
            .rules_for(event_rel_id)
            .and_then(|rl| rl.rules.iter().find(|rr| rr.event == event_type).map(|rr| rr.rule_id))
            .unwrap_or(Oid::INVALID)
    } else {
        get_new_oid_with_index(shared, &pg_rewrite, RewriteOidIndexId, r::Anum_pg_rewrite_oid as i16)
            .await
    };

    // Build the row. ev_type is stored as the char digit (CmdType + '0'); a
    // placeholder pg_node_tree text stands in for the serialized trees (the live
    // trees live in the registry).
    let natts = desc.natts as usize;
    let mut values = vec![Datum(0); natts];
    let mut isnull = vec![false; natts];
    let set = |v: &mut [Datum], anum: i32, d: Datum| v[(anum - 1) as usize] = d;

    let rname = crate::backend::catalog::heap::name_data(rulename);
    let ev_type_char = (cmd_type_to_char(event_type) as u8 + b'0') as i8;
    let placeholder = crate::backend::utils::adt::varlena::cstring_to_text("<stored in registry>");

    set(&mut values, r::Anum_pg_rewrite_oid, ObjectIdGetDatum(rule_oid));
    set(&mut values, r::Anum_pg_rewrite_rulename, crate::postgres::NameGetDatum(&rname));
    set(&mut values, r::Anum_pg_rewrite_ev_class, ObjectIdGetDatum(event_rel_id));
    set(&mut values, r::Anum_pg_rewrite_ev_type, CharGetDatum(ev_type_char));
    set(&mut values, r::Anum_pg_rewrite_ev_enabled, CharGetDatum(RULE_FIRES_ON_ORIGIN as i8));
    set(&mut values, r::Anum_pg_rewrite_is_instead, BoolGetDatum(is_instead));
    // ev_qual is BKI_FORCE_NOT_NULL in PG (an empty-list node text when no qual).
    set(&mut values, r::Anum_pg_rewrite_ev_qual, PointerGetDatum(placeholder.cast::<u8>()));
    set(
        &mut values,
        r::Anum_pg_rewrite_ev_action,
        PointerGetDatum(crate::backend::utils::adt::varlena::cstring_to_text("<stored in registry>").cast::<u8>()),
    );

    if !already {
        let mut tup = heap_form_tuple(&desc, &values, &isnull);
        catalog_tuple_insert(shared, &pg_rewrite, &mut tup).await;
        heap_freetuple(tup);
    }
    let _ = &mut isnull;
    relation_close(pg_rewrite);

    // Record the live action/qual trees in the registry (the rewriter's source).
    let actions: Vec<Query> = action
        .into_iter()
        .map(|n| match n {
            Node::Query(q) => *q,
            other => {
                let _ = other;
                not_yet_reachable("InsertRule: rule action is not a Query")
            }
        })
        .collect();
    shared.rule_registry().insert(
        event_rel_id,
        crate::backend::rewrite::rule_registry::RewriteRuleData {
            rule_id: rule_oid,
            event: event_type,
            is_instead,
            enabled: RULE_FIRES_ON_ORIGIN,
            qual: event_qual,
            actions,
        },
    );

    rule_oid
}

/// PG's `CmdType` -> the small integer stored in `pg_rewrite.ev_type` (as a digit).
/// Matches the `CmdType` enum order: SELECT=1, UPDATE=2, INSERT=3, DELETE=4.
fn cmd_type_to_char(event: CmdType) -> i8 {
    match event {
        CmdType::SELECT => 1,
        CmdType::UPDATE => 2,
        CmdType::INSERT => 3,
        CmdType::DELETE => 4,
        other => {
            let _ = other;
            not_yet_reachable("InsertRule: unsupported rule event type")
        }
    }
}

/// PG `DefineRule`: parse-analyze a raw RuleStmt then call `DefineQueryRewrite`.
/// The view path uses `DefineQueryRewrite` directly (the query is already
/// analyzed); standalone CREATE RULE analysis (transformRuleStmt) is staged.
pub async fn define_rule(
    _shared: &Arc<SharedState>,
    _stmt: &crate::nodes::parsenodes::RuleStmt,
    _query_string: &str,
) -> ObjectAddress {
    not_yet_reachable("DefineRule: standalone CREATE RULE (transformRuleStmt)");
}
