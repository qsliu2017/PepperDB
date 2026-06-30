//! PG `src/backend/utils/misc/guc.c` -- Grand Unified Configuration: the core.
//!
//! Replaces the step-09 defaults-only shim. This is the reachable GUC machinery:
//! the per-backend variable registry, `set_config_option` /
//! `set_config_option_ext` / `SetConfigOption` / `GetConfigOption` /
//! `GetConfigOptionByName`, the PGC_* context-permission check, the bool/int/
//! real/string/enum parse-and-validate path with assign/check-hook invocation,
//! and the transaction-level set/rollback stack (`push_old_value`,
//! `NewGUCNestLevel`, `AtEOXact_GUC`, `ResetAllOptions`).
//!
//! Redesign vs the C original (rules.md s10): C threads a `void *variable`
//! pointer into a backing global plus a `union config_var_val`; here a
//! [`GucVariable`] owns its value inline as a [`GucVal`] enum, so there are no
//! raw pointers and no aliasing. The maintenance `dlist`/`slist` links and the
//! `guc_hashtab` collapse to an owned `Vec<GucVariable>` + a name->index map.
//! Per-backend state (a `.c` process-private static) becomes a `task_local!`
//! holding a `RefCell<GucState>` (rules.md s10, modelled on `xact.rs`); the
//! borrow is never held across an `.await` (rules.md s5 -- all of GUC is sync).
//!
//! STAGED (not_yet_reachable per rules.md s4, left in the header
//! `crate::utils::guc`): config-file parsing (`ProcessConfigFile`/
//! `ParseConfigFp`), SIGHUP reload, the `pg_settings` view, GUC serialization
//! for parallel workers, the `.auto.conf` ALTER SYSTEM writer, and custom/
//! placeholder variables.

use std::cell::RefCell;
use std::collections::HashMap;

use crate::utils::guc::{config_enum_entry, GucAction, GucContext, GucFlags, GucSource};
use crate::utils::guc_tables::{ConfigKind, ConfigVarDef};

/// The fixed search_path RestrictSearchPath() installs (PG `GUC_SAFE_SEARCH_PATH`).
pub const GUC_SAFE_SEARCH_PATH: &str = "pg_catalog, pg_temp";

/// A GUC's live value. Replaces the C `union config_var_val` + the typed
/// `*variable` backing global with one owned tagged value.
#[derive(Debug, Clone, PartialEq)]
pub enum GucVal {
    Bool(bool),
    Int(i32),
    Real(f64),
    /// String GUCs may be unset (C NULL boot_val).
    Str(Option<String>),
    /// Enum GUCs store the resolved integer code; the name table lives in the def.
    Enum(i32),
}

impl GucVal {
    /// Format as PG does for SHOW / GetConfigOption (no unit conversion -- the
    /// table here carries no GUC_UNIT, so ShowGUCOption's unit branch is moot).
    fn show(&self, def: &ConfigVarDef) -> String {
        match self {
            Self::Bool(b) => if *b { "on" } else { "off" }.to_string(),
            Self::Int(i) => i.to_string(),
            Self::Real(r) => format!("{r}"),
            Self::Str(Some(s)) => s.clone(),
            Self::Str(None) => String::new(),
            Self::Enum(v) => config_enum_lookup_by_value(def, *v)
                .unwrap_or("???")
                .to_string(),
        }
    }
}

/// Stack-entry state (C `GucStackState`): SET overrides, LOCAL masks. SET_LOCAL
/// is the "SET then SET LOCAL" combination.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StackState {
    Save,
    Set,
    Local,
    SetLocal,
}

/// One saved prior value for an uncommitted transactional change (C `GucStack`).
#[derive(Debug, Clone)]
struct GucStackEntry {
    nest_level: i32,
    state: StackState,
    source: GucSource,
    scontext: GucContext,
    prior: GucVal,
    /// The masked SET value when state == SetLocal (C `masked`).
    masked: Option<GucVal>,
    masked_scontext: GucContext,
}

/// A live GUC variable: the static definition plus the runtime value/source and
/// the per-nest-level change stack. Combines C's `config_generic` + the
/// type-specific `config_bool`/etc. into one owned record.
pub struct GucVariable {
    def: &'static ConfigVarDef,
    val: GucVal,
    source: GucSource,
    scontext: GucContext,
    reset_val: GucVal,
    reset_source: GucSource,
    reset_scontext: GucContext,
    stack: Vec<GucStackEntry>,
}

impl GucVariable {
    fn boot(def: &'static ConfigVarDef) -> Self {
        let boot = def.boot_val();
        Self {
            val: boot.clone(),
            source: GucSource::DEFAULT,
            scontext: GucContext::INTERNAL,
            reset_val: boot,
            reset_source: GucSource::DEFAULT,
            reset_scontext: GucContext::INTERNAL,
            stack: Vec::new(),
            def,
        }
    }
}

/// Per-backend GUC store (C's process-private GUC globals + `GUCNestLevel`).
pub struct GucState {
    vars: Vec<GucVariable>,
    /// Lower-cased name -> index into `vars` (C `guc_hashtab`, case-folded key).
    by_name: HashMap<String, usize>,
    nest_level: i32,
}

impl GucState {
    fn new() -> Self {
        let defs = crate::backend::utils::misc::guc_tables::all_config_vars();
        let mut vars = Vec::with_capacity(defs.len());
        let mut by_name = HashMap::with_capacity(defs.len());
        for def in defs {
            by_name.insert(def.name.to_ascii_lowercase(), vars.len());
            vars.push(GucVariable::boot(def));
        }
        Self {
            vars,
            by_name,
            nest_level: 0,
        }
    }

    /// PG `find_option` (the reachable part): hash lookup by case-folded name. No
    /// obsolete-name map / placeholder creation (staged).
    fn find(&self, name: &str) -> Option<usize> {
        self.by_name.get(&name.to_ascii_lowercase()).copied()
    }
}

tokio::task_local! {
    /// The current backend's GUC store. Established by [`guc_scope`]; mirrors PG
    /// having one process-wide set of GUC globals per backend.
    static GUC: RefCell<GucState>;
}

/// Run `f` with a fresh per-task GUC store in scope (boot values). A backend
/// task wraps its body in this once.
pub async fn guc_scope<F, T>(f: F) -> T
where
    F: std::future::Future<Output = T>,
{
    GUC.scope(RefCell::new(GucState::new()), f).await
}

/// Borrow the per-task GUC store synchronously. NEVER hold the borrow across an
/// `.await` (rules.md s5).
fn with_guc<R>(f: impl FnOnce(&mut GucState) -> R) -> R {
    GUC.with(|cell| f(&mut cell.borrow_mut()))
}

/// Like [`with_guc`] but lazily creates a store when no scope is active. Read/SET
/// paths invoked from tests or pre-scope init still see boot defaults this way,
/// matching how the old shim returned compiled defaults unconditionally.
fn with_guc_or_default<R>(f: impl FnOnce(&mut GucState) -> R) -> R {
    if GUC.try_with(|_| ()).is_ok() {
        with_guc(f)
    } else {
        f(&mut GucState::new())
    }
}

// ===========================================================================
// enum option helpers (PG guc_tables.c)
// ===========================================================================

/// PG `config_enum_lookup_by_value`: enum code -> option name.
pub fn config_enum_lookup_by_value(def: &ConfigVarDef, val: i32) -> Option<&'static str> {
    def.enum_options()
        .iter()
        .find(|e| e.val == val)
        .map(|e| e.name)
}

/// PG `config_enum_lookup_by_name`: option name (case-insensitive) -> code.
pub fn config_enum_lookup_by_name(def: &ConfigVarDef, value: &str) -> Option<i32> {
    def.enum_options()
        .iter()
        .find(|e| e.name.eq_ignore_ascii_case(value))
        .map(|e| e.val)
}

/// PG `config_enum_get_options`: list the non-hidden option names for a hint.
fn config_enum_get_options(opts: &[config_enum_entry], prefix: &str, suffix: &str) -> String {
    let names: Vec<&str> = opts.iter().filter(|e| !e.hidden).map(|e| e.name).collect();
    format!("{prefix}{}{suffix}", names.join(", "))
}

// ===========================================================================
// parse + validate
// ===========================================================================

/// PG `parse_bool` (the recognized literal set).
fn parse_bool(value: &str) -> Option<bool> {
    let v = value.trim();
    for t in ["true", "yes", "on", "1"] {
        if t.eq_ignore_ascii_case(v) {
            return Some(true);
        }
    }
    for f in ["false", "no", "off", "0"] {
        if f.eq_ignore_ascii_case(v) {
            return Some(false);
        }
    }
    None
}

/// Outcome of validating a proposed value. `Err` carries a ready-to-report
/// message (PG `ereport`s inside `parse_and_validate_value`; here we bubble the
/// text so `set_config_with_handle` can elog/return per `elevel`).
type ParseResult = Result<GucVal, String>;

/// PG `parse_and_validate_value`: convert + range-check + run the check hook.
/// Redesigned to return the converted [`GucVal`] (C used out-params + bool).
fn parse_and_validate_value(def: &ConfigVarDef, value: &str, source: GucSource) -> ParseResult {
    match &def.kind {
        ConfigKind::Bool { check, .. } => {
            let mut b = parse_bool(value)
                .ok_or_else(|| format!("parameter \"{}\" requires a Boolean value", def.name))?;
            if check.is_some_and(|h| !h(&mut b, source)) {
                return Err(check_hook_error(def.name, value));
            }
            Ok(GucVal::Bool(b))
        }
        ConfigKind::Int {
            min, max, check, ..
        } => {
            let mut i: i32 = value.trim().parse().map_err(|_| {
                format!("invalid value for parameter \"{}\": \"{value}\"", def.name)
            })?;
            if i < *min || i > *max {
                return Err(format!(
                    "{i} is outside the valid range for parameter \"{}\" ({min} .. {max})",
                    def.name
                ));
            }
            if check.is_some_and(|h| !h(&mut i, source)) {
                return Err(check_hook_error(def.name, value));
            }
            Ok(GucVal::Int(i))
        }
        ConfigKind::Real {
            min, max, check, ..
        } => {
            let mut r: f64 = value.trim().parse().map_err(|_| {
                format!("invalid value for parameter \"{}\": \"{value}\"", def.name)
            })?;
            if r < *min || r > *max {
                return Err(format!(
                    "{r} is outside the valid range for parameter \"{}\" ({min} .. {max})",
                    def.name
                ));
            }
            if check.is_some_and(|h| !h(&mut r, source)) {
                return Err(check_hook_error(def.name, value));
            }
            Ok(GucVal::Real(r))
        }
        ConfigKind::Str { check, .. } => {
            let mut s = Some(value.to_string());
            if check.is_some_and(|h| !h(&mut s, source)) {
                return Err(check_hook_error(def.name, value));
            }
            Ok(GucVal::Str(s))
        }
        ConfigKind::Enum { options, check, .. } => {
            let Some(mut code) = config_enum_lookup_by_name(def, value) else {
                let hint = config_enum_get_options(options, "Available values: ", ".");
                return Err(format!(
                    "invalid value for parameter \"{}\": \"{value}\". {hint}",
                    def.name
                ));
            };
            if check.is_some_and(|h| !h(&mut code, source)) {
                return Err(check_hook_error(def.name, value));
            }
            Ok(GucVal::Enum(code))
        }
    }
}

fn check_hook_error(name: &str, value: &str) -> String {
    format!("invalid value for parameter \"{name}\": \"{value}\"")
}

// ===========================================================================
// context-permission check
// ===========================================================================

/// PG's context-permission switch in `set_config_with_handle`. Returns `Ok` if
/// the variable may be set in `context`, else `Err(message)`. The reachable
/// subset: INTERNAL/POSTMASTER/SIGHUP/SU_BACKEND/BACKEND/SUSET/USERSET. The ACL
/// branches (pg_parameter_aclcheck) and the EXEC_BACKEND/IsUnderPostmaster
/// SIGHUP klugery are staged -- under the single-process model the requesting
/// role is always the bootstrap superuser, so SUSET/SU_BACKEND from a USERSET
/// caller is the only denial path that bites here.
fn check_context(var_context: GucContext, context: GucContext, name: &str) -> Result<(), String> {
    match var_context {
        GucContext::INTERNAL => {
            if context != GucContext::INTERNAL {
                return Err(format!("parameter \"{name}\" cannot be changed"));
            }
        }
        GucContext::POSTMASTER => {
            if context != GucContext::POSTMASTER {
                return Err(format!(
                    "parameter \"{name}\" cannot be changed without restarting the server"
                ));
            }
        }
        GucContext::SIGHUP => {
            if context != GucContext::SIGHUP && context != GucContext::POSTMASTER {
                return Err(format!("parameter \"{name}\" cannot be changed now"));
            }
        }
        GucContext::SU_BACKEND | GucContext::BACKEND => {
            if context != GucContext::POSTMASTER
                && context != GucContext::BACKEND
                && context != GucContext::SU_BACKEND
            {
                return Err(format!(
                    "parameter \"{name}\" cannot be set after connection start"
                ));
            }
        }
        GucContext::SUSET => {
            // ACL check on USERSET/BACKEND callers is staged; the single-process
            // bootstrap role lacks a grant, so reject the runtime-USERSET path.
            if context == GucContext::USERSET || context == GucContext::BACKEND {
                return Err(format!("permission denied to set parameter \"{name}\""));
            }
        }
        GucContext::USERSET => {}
    }
    Ok(())
}

// ===========================================================================
// transaction set/rollback stack
// ===========================================================================

impl GucVariable {
    /// PG `push_old_value`: record the current value so a later abort (or LOCAL
    /// pop) can restore it. Coalesces with an existing entry at the current nest
    /// level, matching the SET/LOCAL/SAVE state transitions.
    fn push_old_value(&mut self, action: GucAction, nest_level: i32) {
        if nest_level == 0 {
            return;
        }
        if self.stack.last().is_some_and(|t| t.nest_level >= nest_level) {
            let scontext = self.scontext;
            let cur_val = self.val.clone();
            let top = self
                .stack
                .last_mut()
                .unwrap_or_else(|| unreachable!("checked non-empty"));
            match action {
                GucAction::SET => {
                    top.state = StackState::Set;
                    top.masked = None;
                }
                GucAction::LOCAL => {
                    if top.state == StackState::Set {
                        top.masked_scontext = scontext;
                        top.masked = Some(cur_val);
                        top.state = StackState::SetLocal;
                    }
                }
                GucAction::SAVE => {}
            }
            return;
        }
        let state = match action {
            GucAction::SET => StackState::Set,
            GucAction::LOCAL => StackState::Local,
            GucAction::SAVE => StackState::Save,
        };
        self.stack.push(GucStackEntry {
            nest_level,
            state,
            source: self.source,
            scontext: self.scontext,
            prior: self.val.clone(),
            masked: None,
            masked_scontext: self.scontext,
        });
    }
}

/// PG `NewGUCNestLevel`: enter a new nesting level for transient GUC changes.
pub fn NewGUCNestLevel() -> i32 {
    with_guc_or_default(|g| {
        g.nest_level += 1;
        g.nest_level
    })
}

/// PG `AtStart_GUC`: set the nest level to 1 at main transaction start.
pub fn AtStart_GUC() {
    with_guc_or_default(|g| g.nest_level = 1);
}

/// PG `AtEOXact_GUC`: at (sub)transaction commit/abort, pop every stack entry at
/// nesting level >= `nest_level`. On abort all priors are restored; on commit a
/// plain SET keeps its value while LOCAL/SET_LOCAL revert (the masked-or-prior
/// rule). Entries straddling >1 level merge down into the previous entry.
pub fn AtEOXact_GUC(is_commit: bool, nest_level: i32) {
    with_guc_or_default(|g| {
        for var in &mut g.vars {
            while var.stack.last().is_some_and(|t| t.nest_level >= nest_level) {
                let Some(entry) = var.stack.pop() else {
                    break;
                };
                let prev_level = var.stack.last().map(|s| s.nest_level);

                let (restore, scontext): (Option<GucVal>, GucContext) = if !is_commit
                    || entry.state == StackState::Save
                {
                    (Some(entry.prior.clone()), entry.scontext)
                } else if entry.nest_level == 1 {
                    match entry.state {
                        StackState::SetLocal => {
                            (entry.masked.clone(), entry.masked_scontext)
                        }
                        StackState::Set => (None, entry.scontext), // keep active value
                        _ => (Some(entry.prior.clone()), entry.scontext), // Local
                    }
                } else if prev_level.is_none_or(|p| p < entry.nest_level - 1) {
                    // Straddles a skipped level: decrement and keep.
                    let mut e = entry;
                    e.nest_level -= 1;
                    var.stack.push(e);
                    continue;
                } else {
                    merge_into_prev(var, entry);
                    continue;
                };

                if let Some(newval) = restore {
                    var.val = newval;
                    var.scontext = scontext;
                    var.source = if is_commit && entry.state == StackState::SetLocal {
                        GucSource::SESSION
                    } else {
                        entry.source
                    };
                    var.def.call_assign(&var.val);
                }
            }
        }
        g.nest_level = nest_level - 1;
    });
}

/// Merge a popped stack entry into the now-top (previous) entry (PG's
/// state-transition switch when `prev->nest_level == stack->nest_level - 1`).
fn merge_into_prev(var: &mut GucVariable, entry: GucStackEntry) {
    let Some(prev) = var.stack.last_mut() else {
        return;
    };
    match entry.state {
        StackState::Save => {}
        StackState::Set => {
            prev.masked = None;
            prev.state = StackState::Set;
        }
        StackState::Local => {
            if prev.state == StackState::Set {
                prev.masked_scontext = entry.scontext;
                prev.masked = Some(entry.prior);
                prev.state = StackState::SetLocal;
            }
        }
        StackState::SetLocal => {
            prev.masked_scontext = entry.masked_scontext;
            prev.masked = entry.masked;
            prev.state = StackState::SetLocal;
        }
    }
}

// ===========================================================================
// set_config_option family
// ===========================================================================

/// PG `set_config_option`: the public set entry. Returns +1 applied, 0 rejected
/// (would-error at sub-ERROR elevel), -1 ok-but-not-applied. `value == None`
/// means reset to default (reset_val, or boot_val when source == DEFAULT).
#[allow(
    clippy::too_many_arguments,
    reason = "1:1 with PG set_config_option signature"
)]
pub fn set_config_option(
    name: &str,
    value: Option<&str>,
    context: GucContext,
    source: GucSource,
    action: GucAction,
    change_val: bool,
    _elevel: i32,
    _is_reload: bool,
) -> i32 {
    with_guc_or_default(|g| set_config_inner(g, name, value, context, source, action, change_val))
}

fn set_config_inner(
    g: &mut GucState,
    name: &str,
    value: Option<&str>,
    mut context: GucContext,
    mut source: GucSource,
    action: GucAction,
    mut change_val: bool,
) -> i32 {
    let Some(idx) = g.find(name) else {
        // find_option(create_placeholders=true) emits "unrecognized parameter".
        return 0;
    };

    // Context-permission check (against the variable's own context).
    {
        let var_context = g.vars[idx].def.context;
        if check_context(var_context, context, name).is_err() {
            return 0;
        }
    }

    // makeDefault + the override check use the ORIGINAL passed source (PG
    // computes both before the value-eval switch reassigns source in the reset
    // branch).
    let make_default = change_val
        && (source <= GucSource::OVERRIDE)
        && (value.is_some() || source == GucSource::DEFAULT);

    // Ignore if overridden by a higher-priority prior source.
    if g.vars[idx].source > source {
        if change_val && !make_default {
            return -1;
        }
        change_val = false;
    }

    // Compute the would-be new value. In the reset branch (value == None and not
    // a DEFAULT source) the stored source/context become the reset record's.
    let def = g.vars[idx].def;
    let newval: GucVal = match value {
        Some(v) => match parse_and_validate_value(def, v, source) {
            Ok(nv) => nv,
            Err(_) => return 0,
        },
        None if source == GucSource::DEFAULT => def.boot_val(),
        None => {
            let var = &g.vars[idx];
            source = var.reset_source;
            context = var.reset_scontext;
            var.reset_val.clone()
        }
    };

    if change_val {
        if !make_default {
            let level = g.nest_level;
            g.vars[idx].push_old_value(action, level);
        }
        let var = &mut g.vars[idx];
        var.val = newval.clone();
        var.source = source;
        var.scontext = context;
        var.def.call_assign(&newval);
    }

    if make_default {
        let var = &mut g.vars[idx];
        if var.reset_source <= source {
            var.reset_val = newval.clone();
            var.reset_source = source;
            var.reset_scontext = context;
        }
        for entry in &mut var.stack {
            if entry.source <= source {
                entry.prior = newval.clone();
                entry.source = source;
                entry.scontext = context;
            }
        }
    }

    1
}

/// PG `set_config_option_ext`: same as `set_config_option` plus an explicit role
/// OID. The role only feeds the staged ACL check, so it is ignored here.
#[allow(
    clippy::too_many_arguments,
    reason = "1:1 with PG set_config_option_ext signature"
)]
pub fn set_config_option_ext(
    name: &str,
    value: Option<&str>,
    context: GucContext,
    source: GucSource,
    _srole: crate::postgres_ext::Oid,
    action: GucAction,
    change_val: bool,
    elevel: i32,
    is_reload: bool,
) -> i32 {
    set_config_option(
        name, value, context, source, action, change_val, elevel, is_reload,
    )
}

/// PG `SetConfigOption`: the simple external setter (always GUC_ACTION_SET,
/// changeVal=true, elevel=0 -> ERROR on bad input).
pub fn SetConfigOption(name: &str, value: &str, context: GucContext, source: GucSource) {
    set_config_option(
        name,
        Some(value),
        context,
        source,
        GucAction::SET,
        true,
        0,
        false,
    );
}

/// PG `RestrictSearchPath`: pin search_path for maintenance ops (a saved SET).
pub fn RestrictSearchPath() {
    set_config_option(
        "search_path",
        Some(GUC_SAFE_SEARCH_PATH),
        GucContext::USERSET,
        GucSource::SESSION,
        GucAction::SAVE,
        true,
        0,
        false,
    );
}

/// PG `ResetAllOptions`: reset every (resettable) variable to its reset_val.
pub fn ResetAllOptions() {
    with_guc_or_default(|g| {
        for var in &mut g.vars {
            if var.def.flags.contains(GucFlags::NO_RESET_ALL) {
                continue;
            }
            var.push_old_value(GucAction::SET, g.nest_level);
            var.val = var.reset_val.clone();
            var.source = var.reset_source;
            var.scontext = var.reset_scontext;
            var.def.call_assign(&var.val);
        }
    });
}

// ===========================================================================
// read accessors
// ===========================================================================

/// PG `GetConfigOption`: current value of `name` as a string; None when missing
/// (and missing_ok). `restrict_privileged` (the pg_read_all_settings gate) is a
/// no-op under the single-process bootstrap role.
pub fn GetConfigOption(name: &str, missing_ok: bool, _restrict_privileged: bool) -> Option<String> {
    with_guc_or_default(|g| match g.find(name) {
        Some(idx) => Some(g.vars[idx].val.show(g.vars[idx].def)),
        None if missing_ok => None,
        None => None,
    })
}

/// PG `GetConfigOptionResetString`: the RESET value of `name` as a string.
pub fn GetConfigOptionResetString(name: &str) -> Option<String> {
    with_guc_or_default(|g| {
        g.find(name)
            .map(|idx| g.vars[idx].reset_val.show(g.vars[idx].def))
    })
}

/// The option flags of `name` (PG `GetConfigOptionFlags`); None if unknown. Used
/// by `flatten_set_variable_args` to honor GUC_LIST_INPUT / GUC_LIST_QUOTE.
pub fn option_flags(name: &str) -> Option<GucFlags> {
    with_guc_or_default(|g| g.find(name).map(|idx| g.vars[idx].def.flags))
}

/// PG `GetConfigOptionByName`: returns (canonical_name, value) for SHOW; None
/// when missing (and missing_ok).
pub fn GetConfigOptionByName(name: &str, missing_ok: bool) -> Option<(String, String)> {
    with_guc_or_default(|g| match g.find(name) {
        Some(idx) => {
            let var = &g.vars[idx];
            Some((var.def.name.to_string(), var.val.show(var.def)))
        }
        None if missing_ok => None,
        None => None,
    })
}

#[cfg(test)]
mod tests;
