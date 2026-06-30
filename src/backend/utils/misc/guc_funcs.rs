//! PG `src/backend/utils/misc/guc_funcs.c` -- the SET / RESET / SHOW statement
//! handlers that sit on top of the GUC core (`guc.rs`).
//!
//! `ExecSetVariableStmt` dispatches SET value / SET DEFAULT / RESET / RESET ALL /
//! SET MULTI to `set_config_option`; `SetPGVariable` is the C-callable SET;
//! `GetPGVariable` is SHOW. The SET MULTI sub-cases route TRANSACTION /
//! SESSION CHARACTERISTICS to `SetPGVariable` per element; TRANSACTION SNAPSHOT
//! (ImportSnapshot) and the InvokeObjectPostAlterHook are STAGED (rules.md s4).

use crate::backend::utils::misc::guc::{
    option_flags, set_config_option, GetConfigOptionByName, ResetAllOptions,
};
use crate::miscadmin::superuser;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{A_Const, DefElem, ValUnion, VariableSetKind, VariableSetStmt};
use crate::tcop::dest::DestReceiver;
use crate::utils::elog::ERROR;
use crate::utils::guc::{GucAction, GucContext, GucFlags, GucSource};

/// Raise an ERROR (which unwinds) for a malformed SET statement. ERROR diverges
/// via the elog/ereport panic path, so this returns `!`.
#[cold]
fn set_error(msg: &str) -> ! {
    crate::elog!(ERROR, msg);
    unreachable!("elog!(ERROR) unwinds");
}

/// The GUC context a SQL SET runs under: superuser -> SUSET, else USERSET.
fn set_context() -> GucContext {
    if superuser() {
        GucContext::SUSET
    } else {
        GucContext::USERSET
    }
}

/// PG `ExecSetVariableStmt`: execute a SET/RESET statement. Parallel-mode
/// rejection is dropped (single-process). `is_top_level` feeds the
/// WarnNoTransactionBlock warning for SET LOCAL.
pub fn ExecSetVariableStmt(stmt: &VariableSetStmt, is_top_level: bool) {
    let action = if stmt.is_local {
        GucAction::LOCAL
    } else {
        GucAction::SET
    };
    let name = stmt.name.as_deref().unwrap_or("");

    match stmt.kind {
        VariableSetKind::SET_VALUE | VariableSetKind::SET_CURRENT => {
            if stmt.is_local {
                crate::backend::access::transam::xact::WarnNoTransactionBlock(
                    is_top_level,
                    "SET LOCAL",
                );
            }
            let value = ExtractSetVariableArgs(stmt);
            set_config_option(
                name,
                value.as_deref(),
                set_context(),
                GucSource::SESSION,
                action,
                true,
                0,
                false,
            );
        }
        VariableSetKind::SET_MULTI => exec_set_multi(stmt, is_top_level),
        VariableSetKind::SET_DEFAULT | VariableSetKind::RESET => {
            if stmt.kind == VariableSetKind::SET_DEFAULT && stmt.is_local {
                crate::backend::access::transam::xact::WarnNoTransactionBlock(
                    is_top_level,
                    "SET LOCAL",
                );
            }
            set_config_option(
                name,
                None,
                set_context(),
                GucSource::SESSION,
                action,
                true,
                0,
                false,
            );
        }
        VariableSetKind::RESET_ALL => ResetAllOptions(),
    }
    // STAGED: InvokeObjectPostAlterHook (parameter ACL audit) lands with ACLs.
}

/// PG `ExecSetVariableStmt` SET MULTI arm: the TRANSACTION / SESSION
/// CHARACTERISTICS / TRANSACTION SNAPSHOT special syntaxes.
fn exec_set_multi(stmt: &VariableSetStmt, is_top_level: bool) {
    let name = stmt.name.as_deref().unwrap_or("");
    match name {
        "TRANSACTION" | "SESSION CHARACTERISTICS" => {
            if name == "TRANSACTION" {
                crate::backend::access::transam::xact::WarnNoTransactionBlock(
                    is_top_level,
                    "SET TRANSACTION",
                );
            }
            let session = name == "SESSION CHARACTERISTICS";
            for item in &stmt.args {
                let Node::DefElem(item) = item else {
                    set_error("unexpected SET TRANSACTION element");
                };
                set_transaction_item(item, session, stmt.is_local);
            }
        }
        // STAGED: TRANSACTION SNAPSHOT -> ImportSnapshot (snapshot import path).
        "TRANSACTION SNAPSHOT" => {
            set_error("SET TRANSACTION SNAPSHOT is not yet supported");
        }
        other => set_error(&format!("unexpected SET MULTI element: {other}")),
    }
}

/// Route one TRANSACTION / SESSION CHARACTERISTICS DefElem to its GUC.
fn set_transaction_item(item: &DefElem, session: bool, is_local: bool) {
    let defname = item.defname.as_deref().unwrap_or("");
    let target = match (defname, session) {
        ("transaction_isolation", false) => "transaction_isolation",
        ("transaction_isolation", true) => "default_transaction_isolation",
        ("transaction_read_only", false) => "transaction_read_only",
        ("transaction_read_only", true) => "default_transaction_read_only",
        ("transaction_deferrable", false) => "transaction_deferrable",
        ("transaction_deferrable", true) => "default_transaction_deferrable",
        _ => set_error(&format!("unexpected SET TRANSACTION element: {defname}")),
    };
    let args: Vec<Node> = item.arg.iter().cloned().collect();
    SetPGVariable(target, &args, is_local);
}

/// PG `ExtractSetVariableArgs`: the value string for a SET, or None for RESET /
/// SET DEFAULT. SET FROM CURRENT reads the current value via GetConfigOptionByName.
pub fn ExtractSetVariableArgs(stmt: &VariableSetStmt) -> Option<String> {
    let name = stmt.name.as_deref().unwrap_or("");
    match stmt.kind {
        VariableSetKind::SET_VALUE => flatten_set_variable_args(name, &stmt.args),
        VariableSetKind::SET_CURRENT => GetConfigOptionByName(name, false).map(|(_, v)| v),
        _ => None,
    }
}

/// PG `flatten_set_variable_args`: collapse the grammar's arg list to the flat
/// string GUC stores. None for an empty list (SET ... TO DEFAULT). The TypeCast
/// (SET TIME ZONE INTERVAL) branch is STAGED.
fn flatten_set_variable_args(name: &str, args: &[Node]) -> Option<String> {
    if args.is_empty() {
        return None;
    }
    let flags = option_flags(name).unwrap_or_else(GucFlags::empty);
    if !flags.contains(GucFlags::LIST_INPUT) && args.len() != 1 {
        set_error(&format!("SET {name} takes only one argument"));
    }

    let parts: Vec<String> = args
        .iter()
        .map(|arg| {
            let Node::A_Const(con) = arg else {
                set_error("unrecognized node type in SET argument");
            };
            flatten_one_const(con, flags)
        })
        .collect();
    Some(parts.join(", "))
}

fn flatten_one_const(con: &A_Const, flags: GucFlags) -> String {
    match &con.val {
        ValUnion::Integer(i) => i.ival.to_string(),
        ValUnion::Float(f) => f.fval.clone(),
        ValUnion::Boolean(b) => if b.boolval { "true" } else { "false" }.to_string(),
        ValUnion::String(s) => {
            if flags.contains(GucFlags::LIST_QUOTE) {
                crate::utils::builtins::quote_identifier(&s.sval).to_string()
            } else {
                s.sval.clone()
            }
        }
        other => set_error(&format!("unrecognized node type in SET argument: {other:?}")),
    }
}

/// PG `SetPGVariable`: SET name = args (or RESET when args is empty).
pub fn SetPGVariable(name: &str, args: &[Node], is_local: bool) {
    let argstring = flatten_set_variable_args(name, args);
    set_config_option(
        name,
        argstring.as_deref(),
        set_context(),
        GucSource::SESSION,
        if is_local {
            GucAction::LOCAL
        } else {
            GucAction::SET
        },
        true,
        0,
        false,
    );
}

/// PG `GetPGVariable`: the SHOW command. SHOW ALL and the per-variable tuple
/// emission go through the tuple-output machinery (`begin_tup_output_tupdesc` /
/// `do_text_output_oneline`), which is STAGED -- this stays 1:1 with PG and will
/// fail loudly until that path lands. The value lookup itself
/// (`GetConfigOptionByName`) is reachable and is what the tests exercise.
pub fn GetPGVariable(name: &str, dest: &mut dyn DestReceiver) {
    if name.eq_ignore_ascii_case("all") {
        show_all_guc_config(dest);
    } else {
        show_guc_config_option(name, dest);
    }
}

fn show_guc_config_option(name: &str, _dest: &mut dyn DestReceiver) {
    let (_varname, _value) = GetConfigOptionByName(name, false)
        .unwrap_or_else(|| set_error(&format!("unrecognized configuration parameter \"{name}\"")));
    // STAGED: emit (_varname, _value) through begin_tup_output_tupdesc /
    // do_text_output_oneline once the tuple-output machinery is reachable.
    unimplemented!("SHOW output path: tuple-output machinery not yet translated");
}

fn show_all_guc_config(_dest: &mut dyn DestReceiver) {
    // STAGED: SHOW ALL needs the tuple-output machinery + get_guc_variables().
    unimplemented!("SHOW ALL: tuple-output machinery not yet translated");
}
