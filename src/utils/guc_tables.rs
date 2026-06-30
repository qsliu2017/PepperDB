//! Translated from PostgreSQL src/include/utils/guc_tables.h
//! Declarations of tables used by GUC. See src/backend/utils/misc/README.
//!
//! The record types this header defines in C (`config_type`, `config_var_val`,
//! `config_var_value`, `config_group`, `GucStackState`, `GucStack`,
//! `config_generic`, `config_bool`/`int`/`real`/`string`/`enum`, the status
//! bits) are translated in `crate::utils::guc` (the C-literal header shape, with
//! `*mut` backing-global pointers) and re-exported here so the canonical
//! guc_tables.h items resolve from this module.
//!
//! The engine (`src/backend/utils/misc/guc.rs`) does NOT use those pointer-based
//! records; it reads a redesigned, owned [`ConfigVarDef`] (a `static` definition
//! whose [`ConfigKind`] carries the boot value, range, enum options, and the
//! sync check/assign/show hooks). That redesign lives here next to the literal
//! types so guc_tables.h's role -- "the variable definition tables" -- resolves
//! from one place. The built-in table arrays live in
//! `crate::backend::utils::misc::guc_tables`.

use crate::utils::guc::{config_enum_entry, config_group, GucContext, GucFlags, GucSource};

pub use crate::utils::guc::{
    config_bool, config_enum, config_generic, config_int, config_real, config_string, config_type,
    config_var_val, config_var_value, GucStack, GucStackState, GUC_IS_IN_FILE, GUC_NEEDS_REPORT,
    GUC_PENDING_RESTART,
};

use crate::backend::utils::misc::guc::GucVal;

// Constant tables corresponding to the enums above and in guc.h.
// (C `const char *const xxx[]`; indexed by the matching enum's discriminant.)
pub static config_group_names: &[&str] = &[];
pub static config_type_names: &[&str] = &[];
pub static GucContext_Names: &[&str] = &[];
pub static GucSource_Names: &[&str] = &[];

// --- Redesigned (owned) variable-definition model used by the engine ---

/// A bool/enum check hook (PG `Guc{Bool,Enum}CheckHook`): validate/canonicalize
/// the proposed value in place, returning false to reject. The opaque `extra`
/// out-param is dropped (rules.md s10) -- check hooks that produced it are staged
/// or recompute in their assign hook.
pub type BoolCheckHook = fn(&mut bool, GucSource) -> bool;
pub type IntCheckHook = fn(&mut i32, GucSource) -> bool;
pub type RealCheckHook = fn(&mut f64, GucSource) -> bool;
pub type StringCheckHook = fn(&mut Option<String>, GucSource) -> bool;
pub type EnumCheckHook = fn(&mut i32, GucSource) -> bool;

/// An assign hook (PG `Guc*AssignHook`): apply the now-validated value. Takes the
/// owned [`GucVal`] by reference; hooks downcast to their type.
pub type AssignHook = fn(&GucVal);
/// A show hook (PG `GucShowHook`): a custom string rendering for SHOW.
pub type ShowHook = fn() -> String;

/// Per-type payload of a [`ConfigVarDef`]: boot value, range/options, and hooks.
/// Replaces C's five parallel `config_bool`/etc. record structs.
pub enum ConfigKind {
    Bool {
        boot: bool,
        check: Option<BoolCheckHook>,
        assign: Option<AssignHook>,
    },
    Int {
        boot: i32,
        min: i32,
        max: i32,
        check: Option<IntCheckHook>,
        assign: Option<AssignHook>,
    },
    Real {
        boot: f64,
        min: f64,
        max: f64,
        check: Option<RealCheckHook>,
        assign: Option<AssignHook>,
    },
    Str {
        boot: Option<&'static str>,
        check: Option<StringCheckHook>,
        assign: Option<AssignHook>,
        show: Option<ShowHook>,
    },
    Enum {
        boot: i32,
        options: &'static [config_enum_entry],
        check: Option<EnumCheckHook>,
        assign: Option<AssignHook>,
    },
}

/// One built-in GUC variable's static definition (the merge of C's
/// `config_generic` constant fields with the type-specific record). The runtime
/// value, source, and change stack live in the engine's per-task store.
pub struct ConfigVarDef {
    pub name: &'static str,
    pub context: GucContext,
    pub group: config_group,
    pub short_desc: &'static str,
    pub flags: GucFlags,
    pub kind: ConfigKind,
}

impl ConfigVarDef {
    /// The boot (compiled-default) value as an owned [`GucVal`].
    #[must_use]
    pub fn boot_val(&self) -> GucVal {
        match &self.kind {
            ConfigKind::Bool { boot, .. } => GucVal::Bool(*boot),
            ConfigKind::Int { boot, .. } => GucVal::Int(*boot),
            ConfigKind::Real { boot, .. } => GucVal::Real(*boot),
            ConfigKind::Str { boot, .. } => GucVal::Str(boot.map(str::to_string)),
            ConfigKind::Enum { boot, .. } => GucVal::Enum(*boot),
        }
    }

    /// The enum option table (empty for non-enum variables).
    #[must_use]
    pub fn enum_options(&self) -> &'static [config_enum_entry] {
        match &self.kind {
            ConfigKind::Enum { options, .. } => options,
            _ => &[],
        }
    }

    /// Invoke the assign hook (if any) for a newly-applied value.
    pub fn call_assign(&self, val: &GucVal) {
        let hook = match &self.kind {
            ConfigKind::Bool { assign, .. }
            | ConfigKind::Int { assign, .. }
            | ConfigKind::Real { assign, .. }
            | ConfigKind::Str { assign, .. }
            | ConfigKind::Enum { assign, .. } => *assign,
        };
        if let Some(hook) = hook {
            hook(val);
        }
    }
}

/// Look up a GUC variable, returning a config_generic. None if unknown (the C
/// NULL return when not found / skip_errors). STAGED: the engine uses its own
/// per-task `find` over [`ConfigVarDef`]; this header form is unused.
pub fn find_option(
    _name: &str,
    _create_placeholders: bool,
    _skip_errors: bool,
    _elevel: i32,
) -> Option<&'static mut config_generic> {
    unimplemented!()
}

/// Variables to show in EXPLAIN (C returns array + count out-param).
pub fn get_explain_guc_options() -> Vec<&'static mut config_generic> {
    unimplemented!()
}

/// Get the string value of a variable.
pub fn ShowGUCOption(_record: &config_generic, _use_units: bool) -> String {
    unimplemented!()
}

/// Whether the GUC variable is visible to the current user.
pub fn ConfigOptionIsVisible(_conf: &config_generic) -> bool {
    unimplemented!()
}

/// The current set of variables (C returns array + count out-param).
pub fn get_guc_variables() -> Vec<&'static mut config_generic> {
    unimplemented!()
}

pub fn build_guc_variables() {
    unimplemented!()
}

// Search in enum options. The engine's reachable lookups live in
// `crate::backend::utils::misc::guc`; these header forms are unused.

/// Map an enum GUC's value to its option name; None if not found.
pub fn config_enum_lookup_by_value(_record: &config_enum, _val: i32) -> Option<&'static str> {
    unimplemented!()
}

/// Map an enum GUC's option name to its value; None if not a valid name (C bool
/// + int out-param).
pub fn config_enum_lookup_by_name(_record: &config_enum, _value: &str) -> Option<i32> {
    unimplemented!()
}

pub fn config_enum_get_options(
    _record: &config_enum,
    _prefix: &str,
    _suffix: &str,
    _separator: &str,
) -> String {
    unimplemented!()
}
