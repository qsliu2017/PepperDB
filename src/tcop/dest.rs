//! Translation of postgres/src/include/tcop/dest.h (+ the destination-management
//! glue from postgres/src/backend/tcop/dest.c).
//!
//! The DestReceiver abstraction: a vtable that query output is funneled through
//! (frontend wire protocol, SPI, tuplestore, COPY, etc.).  The executor and the
//! command processor call receiveSlot/rStartup/rShutdown/rDestroy without caring
//! which concrete receiver is installed.
//!
//! #include "executor/tuptable.h"  -> crate::executor::tuptable
//! #include "tcop/cmdtag.h"        -> CommandTag/QueryCompletion are STUBBED here
//!                                    (tcop/cmdtag.h not yet ported).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::access::common::tupdesc::TupleDesc;
use crate::executor::tuptable::TupleTableSlot;
use crate::libpq::libpq::{pq_flush, pq_putmessage};
use crate::libpq::pqformat::{pq_beginmessage, pq_endmessage, pq_putemptymessage, pq_sendint8};
use crate::libpq::protocol::{PqMsg_CommandComplete, PqMsg_EmptyQueryResponse, PqMsg_ReadyForQuery};
use crate::lib::stringinfo::StringInfoData;
use crate::access::transam::xact::TransactionBlockStatusCode;
use core::ffi::{c_char, c_int};

extern "C" {
    fn strlen(s: *const c_char) -> usize;
}

/*
 * CommandDest is a simplified representation of the command-result-destination
 * concept; it identifies which kind of receiver a query's output goes to.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CommandDest {
    DestNone = 0,        /* results are discarded */
    DestDebug,           /* results go to debugging output */
    DestRemote,          /* results sent to frontend process */
    DestRemoteExecute,   /* sent to frontend, in Execute command */
    DestRemoteSimple,    /* sent to frontend, w/no catalog access */
    DestSPI,             /* results sent to SPI manager */
    DestTuplestore,      /* results sent to Tuplestore */
    DestIntoRel,         /* results sent to relation (SELECT INTO) */
    DestCopyOut,         /* results sent to COPY TO code */
    DestSQLFunction,     /* results sent to SQL-language func mgr */
    DestTransientRel,    /* results sent to transient relation */
    DestTupleQueue,      /* results sent to tuple queue */
    DestExplainSerialize, /* results are serialized and discarded */
}
pub use CommandDest::*;

// TODO(pg-port): tcop/cmdtag.h not yet ported.  CommandTag is really an enum and
// QueryCompletion a small struct; modeled minimally so the dest signatures are
// faithful.  Replace when cmdtag.h lands.
pub type CommandTag = c_int;
#[repr(C)]
#[derive(Clone, Copy)]
pub struct QueryCompletion {
    pub commandTag: CommandTag,
    pub nprocessed: u64,
}

/*
 * DestReceiver - the output-sink vtable.  (The C struct is `struct _DestReceiver`
 * with a typedef DestReceiver.)  Function-pointer fields are modeled as
 * Option<unsafe fn(...)> to match the rest of this crate (pure-Rust, no FFI).
 * Concrete receivers embed this as their first field and may carry private state
 * after it.
 */
#[repr(C)]
pub struct DestReceiver {
    /// Called for each tuple to be output.
    pub receiveSlot: Option<unsafe fn(slot: *mut TupleTableSlot, self_: *mut DestReceiver) -> bool>,
    /// Per-executor-run initialization.
    pub rStartup: Option<unsafe fn(self_: *mut DestReceiver, operation: c_int, typeinfo: TupleDesc)>,
    /// Per-executor-run shutdown.
    pub rShutdown: Option<unsafe fn(self_: *mut DestReceiver)>,
    /// Destroy the receiver object itself (if dynamically allocated).
    pub rDestroy: Option<unsafe fn(self_: *mut DestReceiver)>,
    /// CommandDest code for this receiver.
    pub mydest: CommandDest,
    /* Private fields might appear beyond this point in concrete receivers. */
}

/* ---- the "do nothing" receiver used for DestNone (from dest.c) ---- */

unsafe fn donothingReceive(_slot: *mut TupleTableSlot, _self_: *mut DestReceiver) -> bool {
    true
}
unsafe fn donothingStartup(_self_: *mut DestReceiver, _operation: c_int, _typeinfo: TupleDesc) {}
unsafe fn donothingCleanup(_self_: *mut DestReceiver) {
    /* this is used for both shutdown and destroy methods */
}

/*
 * Permanent receiver for DestNone.  In C this is a file-static `donothingDR`
 * plus an exported `DestReceiver *None_Receiver`.  We expose the static and a
 * None_Receiver() accessor returning a (cast-away-const) pointer to it.
 */
pub static donothingDR: DestReceiver = DestReceiver {
    receiveSlot: Some(donothingReceive),
    rStartup: Some(donothingStartup),
    rShutdown: Some(donothingCleanup),
    rDestroy: Some(donothingCleanup),
    mydest: DestNone,
};

/// Equivalent of C's global `None_Receiver`.
#[inline]
pub unsafe fn None_Receiver() -> *mut DestReceiver {
    &donothingDR as *const DestReceiver as *mut DestReceiver
}

/*
 * CreateDestReceiver - return a receiver object for the given destination.
 *
 * Only DestNone is wired up so far; the others dispatch to receivers whose
 * modules are not yet ported (printtup for DestRemote*, printsimple for
 * DestRemoteSimple, tuplestore/copy/SPI/... for the rest).
 *
 * TODO(pg-port): wire DestRemoteSimple -> crate::access::common::printsimple and
 * DestRemote/Execute -> crate::access::common::printtup once those land.
 */
#[no_mangle]
pub unsafe fn CreateDestReceiver(dest: CommandDest) -> *mut DestReceiver {
    match dest {
        DestRemote | DestRemoteExecute => {
            crate::access::common::printtup::printtup_create_DR(dest)
        }
        DestRemoteSimple => {
            &crate::access::common::printsimple::printsimpleDR as *const DestReceiver
                as *mut DestReceiver
        }
        DestNone => None_Receiver(),
        DestTuplestore => crate::executor::tstoreReceiver::CreateTuplestoreDestReceiver(),
        _ => {
            // TODO(pg-port): debug/SPI/copy/SQLfunction/transientrel/
            // tuplequeue/explain-serialize receivers (modules not yet ported).
            unimplemented!("CreateDestReceiver: receiver for {:?} not yet ported", dest)
        }
    }
}

/*
 * The command-processor entry points (BeginCommand/EndCommand/NullCommand/
 * ReadyForQuery) drive the frontend wire protocol via the libpq comm layer,
 * which is not yet ported.  Stubbed.
 */
pub unsafe fn BeginCommand(_commandTag: CommandTag, _dest: CommandDest) {
    /* no-op in C too for most dests; full impl needs the comm layer */
}
pub unsafe fn EndCommand(
    qc: *const QueryCompletion,
    dest: CommandDest,
    force_undecorated_output: bool,
) {
    match dest {
        DestRemote | DestRemoteExecute | DestRemoteSimple => {
            let mut completion_tag: [c_char; crate::tcop::cmdtag::COMPLETION_TAG_BUFSIZE] =
                [0; crate::tcop::cmdtag::COMPLETION_TAG_BUFSIZE];
            let len = crate::tcop::cmdtag::BuildQueryCompletionString(
                completion_tag.as_mut_ptr(),
                &*(qc as *const crate::tcop::cmdtag::QueryCompletion),
                force_undecorated_output,
            );
            let _ = pq_putmessage(
                PqMsg_CommandComplete as c_char,
                completion_tag.as_ptr(),
                len + 1,
            );
        }
        _ => {}
    }
}
/* ----------------
 *		EndReplicationCommand - stripped down version of EndCommand
 *
 *		For use by replication commands.
 * ----------------
 */
pub unsafe fn EndReplicationCommand(commandTag: *const c_char) {
    pq_putmessage(
        PqMsg_CommandComplete as c_char,
        commandTag,
        strlen(commandTag) + 1,
    );
}

pub unsafe fn NullCommand(dest: CommandDest) {
    match dest {
        DestRemote | DestRemoteExecute | DestRemoteSimple => {
            /* Tell the FE that we saw an empty query string */
            pq_putemptymessage(PqMsg_EmptyQueryResponse as c_char);
        }
        _ => {}
    }
}
pub unsafe fn ReadyForQuery(dest: CommandDest) {
    match dest {
        DestRemote | DestRemoteExecute | DestRemoteSimple => {
            let mut buf: StringInfoData = core::mem::zeroed();
            pq_beginmessage(&mut buf, PqMsg_ReadyForQuery as c_char);
            pq_sendint8(&mut buf, TransactionBlockStatusCode());
            pq_endmessage(&mut buf);
            /* Flush output at end of cycle in any case. */
            pq_flush();
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn donothing_receiver() {
        unsafe {
            let dr = None_Receiver();
            assert_eq!((*dr).mydest, DestNone);
            // receiveSlot returns true and does nothing.
            assert!((*dr).receiveSlot.unwrap()(null_mut(), dr));
            // startup/shutdown/destroy are callable no-ops.
            (*dr).rStartup.unwrap()(dr, 0, null_mut());
            (*dr).rShutdown.unwrap()(dr);
            (*dr).rDestroy.unwrap()(dr);
        }
    }

    #[test]
    fn command_dest_discriminants() {
        assert_eq!(DestNone as c_int, 0);
        assert_eq!(DestRemoteSimple as c_int, 4);
        assert_eq!(DestExplainSerialize as c_int, 12);
    }
}
