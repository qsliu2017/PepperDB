//! discard.c - The implementation of the DISCARD command.

use crate::prelude::*;

use crate::nodes::parsenodes::{
    DiscardStmt, DiscardMode, DISCARD_ALL, DISCARD_PLANS, DISCARD_SEQUENCES, DISCARD_TEMP,
};
use crate::nodes::pg_list::{List, NIL};
use crate::utils::portal::PortalHashTableDeleteAll;

// ---------------------------------------------------------------------------
// Dependencies not yet ported. Declared as local stubs.
// ---------------------------------------------------------------------------

// USER_LOCKMETHOD from storage/lock.h (the 2nd lock method table, user locks).
// TODO: dep not ported - storage/lock.h
const USER_LOCKMETHOD: c_int = 2;

// access/xact.h
// TODO: dep not ported
unsafe fn PreventInTransactionBlock(_isTopLevel: bool, _stmtType: *const c_char) {
    unimplemented!()
}

// commands/prepare.h
// TODO: dep not ported
unsafe fn ResetPlanCache() {
    unimplemented!()
}

// commands/prepare.h
// TODO: dep not ported
unsafe fn DropAllPreparedStatements() {
    unimplemented!()
}

// commands/sequence.h
// TODO: dep not ported
unsafe fn ResetSequenceCaches() {
    unimplemented!()
}

// catalog/namespace.h
// TODO: dep not ported
unsafe fn ResetTempTableNamespace() {
    unimplemented!()
}

// commands/async.h
// TODO: dep not ported
unsafe fn Async_UnlistenAll() {
    unimplemented!()
}

// utils/guc.h
// SetPGVariable(name, args, is_local)
// TODO: dep not ported
unsafe fn SetPGVariable(_name: *const c_char, _args: *mut List, _is_local: bool) {
    unimplemented!()
}

// utils/guc.h
// TODO: dep not ported
unsafe fn ResetAllOptions() {
    unimplemented!()
}

// storage/lock.h
// LockReleaseAll(lockmethodid, allLocks)
// TODO: dep not ported
unsafe fn LockReleaseAll(_lockmethodid: c_int, _allLocks: bool) {
    unimplemented!()
}

// ---------------------------------------------------------------------------

/*
 * DISCARD { ALL | SEQUENCES | TEMP | PLANS }
 */
pub unsafe fn DiscardCommand(stmt: *mut DiscardStmt, isTopLevel: bool) {
    match (*stmt).target {
        DISCARD_ALL => {
            DiscardAll(isTopLevel);
        }
        DISCARD_PLANS => {
            ResetPlanCache();
        }
        DISCARD_SEQUENCES => {
            ResetSequenceCaches();
        }
        DISCARD_TEMP => {
            ResetTempTableNamespace();
        }
        #[allow(unreachable_patterns)]
        _ => {
            elog!(ERROR, "unrecognized DISCARD target: {}", (*stmt).target as c_int);
        }
    }
}

unsafe fn DiscardAll(isTopLevel: bool) {
    /*
     * Disallow DISCARD ALL in a transaction block. This is arguably
     * inconsistent (we don't make a similar check in the command sequence
     * that DISCARD ALL is equivalent to), but the idea is to catch mistakes:
     * DISCARD ALL inside a transaction block would leave the transaction
     * still uncommitted.
     */
    PreventInTransactionBlock(isTopLevel, c"DISCARD ALL".as_ptr());

    /* Closing portals might run user-defined code, so do that first. */
    PortalHashTableDeleteAll();
    SetPGVariable(c"session_authorization".as_ptr(), NIL, false);
    ResetAllOptions();
    DropAllPreparedStatements();
    Async_UnlistenAll();
    LockReleaseAll(USER_LOCKMETHOD, true);
    ResetPlanCache();
    ResetTempTableNamespace();
    ResetSequenceCaches();
}
