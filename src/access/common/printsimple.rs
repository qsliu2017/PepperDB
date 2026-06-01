//! Translation of postgres/src/backend/access/common/printsimple.c
//! (merged with postgres/src/include/access/printsimple.h).
//!
//! Routines to print out tuples containing only a limited range of builtin
//! types without catalog access.  This is intended for backends that don't have
//! catalog access because they are not bound to a specific database, such as
//! some walsender processes.  It doesn't handle standalone backends or protocol
//! versions other than 3.0, because we don't need such handling for current
//! applications.
//!
//! Because there is no catalog access, the regular type output functions cannot
//! be used; instead the required types are hard-wired here (see `printsimple`).
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include` mapping:
//!   postgres.h            -> crate::prelude
//!   access/printsimple.h  -> merged here (declares printsimple / printsimple_startup)
//!   catalog/pg_type.h     -> crate::catalog::pg_type_d (TEXTOID/INT4OID/INT8OID/OIDOID)
//!   libpq/pqformat.h      -> crate::libpq::pqformat (pq_beginmessage/pq_sendint16/
//!                            pq_sendint32/pq_sendstring/pq_sendcountedtext/pq_endmessage)
//!   libpq/protocol.h      -> NOT yet ported; PqMsg_RowDescription ('T') and
//!                            PqMsg_DataRow ('D') are defined module-locally below
//!                            (TODO(pg-port): pull from a ported libpq/protocol).
//!   utils/builtins.h      -> pg_ltoa/pg_lltoa/pg_ultoa_n from crate::utils::adt::numutils;
//!                            MAXINT8LEN defined locally (mirrors numutils.c).
//!   tcop/dest.h           -> crate::tcop::dest {DestReceiver, CommandDest}.
//!
//! WHAT IS REAL vs STUBBED:
//!   REAL: printsimple_startup (RowDescription assembly), printsimple (DataRow
//!     assembly + the per-attribute hard-wired value conversion: TEXTOID,
//!     INT4OID, INT8OID, OIDOID), and the static `printsimpleDR` DestReceiver.
//!   STUBBED (transitively, via pqformat): pq_beginmessage/pq_endmessage need the
//!     libpq comm layer (pq_putmessage), and pq_sendstring/pq_sendcountedtext need
//!     mb/mbutils (pg_server_to_client).  Those are called here exactly as the C
//!     does; only the final wire-put / charset-conversion is unported.  The
//!     attribute value conversion -- the load-bearing logic of this file -- is real.

use crate::prelude::*;

use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr};
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::catalog::pg_type_d::{INT4OID, INT8OID, OIDOID, TEXTOID};
use crate::executor::tuptable::{slot_getallattrs, TupleTableSlot};
use crate::lib::stringinfo::StringInfoData;
use crate::libpq::pqformat::{
    pq_beginmessage, pq_endmessage, pq_sendcountedtext, pq_sendint16, pq_sendint32, pq_sendstring,
};
use crate::tcop::dest::{CommandDest, DestReceiver};
use crate::utils::adt::numutils::{pg_ltoa, pg_lltoa, pg_ultoa_n};
use crate::varatt::{pg_detoast_datum_packed, VARDATA_ANY, VARSIZE_ANY_EXHDR};

use core::ffi::{c_char, c_int};

// ----------------------------------------------------------------------------
//   Constants from not-yet-ported headers.
// ----------------------------------------------------------------------------

/* libpq/protocol.h: backend message type bytes. */
// TODO(pg-port): source these from a ported crate::libpq::protocol.
const PqMsg_RowDescription: c_char = b'T' as c_char;
const PqMsg_DataRow: c_char = b'D' as c_char;

/* numutils.c: max strlen of a printed int8 (sign + 19 digits). */
const MAXINT8LEN: usize = 20;

// ----------------------------------------------------------------------------
//   DatumGetTextPP (fmgr.h: ((text *) PG_DETOAST_DATUM_PACKED(X)))
//
// Not provided by fmgr.rs yet; replicate inline using the detoast identity for
// in-line varlenas from crate::varatt.  For printsimple the value is always a
// plain in-memory text Datum, so this is the identity in practice.
// ----------------------------------------------------------------------------
#[inline]
unsafe fn DatumGetTextPP(x: Datum) -> *mut crate::c::text {
    pg_detoast_datum_packed(DatumGetPointer(x) as *mut core::ffi::c_void) as *mut crate::c::text
}

/*
 * At startup time, send a RowDescription message.
 *
 * # Safety
 * `tupdesc` must be a valid TupleDesc; `self` a valid DestReceiver pointer.
 */
pub unsafe fn printsimple_startup(_self: *mut DestReceiver, _operation: c_int, tupdesc: TupleDesc) {
    let mut buf: StringInfoData = core::mem::zeroed();
    let buf: *mut StringInfoData = &mut buf;

    pq_beginmessage(buf, PqMsg_RowDescription);
    pq_sendint16(buf, (*tupdesc).natts as u16);

    let mut i: c_int = 0;
    while i < (*tupdesc).natts {
        let attr: Form_pg_attribute = TupleDescAttr(tupdesc, i);

        pq_sendstring(buf, NameStr(&(*attr).attname));
        pq_sendint32(buf, 0); /* table oid */
        pq_sendint16(buf, 0); /* attnum */
        pq_sendint32(buf, (*attr).atttypid as u32); /* (int) attr->atttypid */
        pq_sendint16(buf, (*attr).attlen as u16);
        pq_sendint32(buf, (*attr).atttypmod as u32);
        pq_sendint16(buf, 0); /* format code */

        i += 1;
    }

    pq_endmessage(buf);
}

/*
 * For each tuple, send a DataRow message.
 *
 * # Safety
 * `slot` must be a valid TupleTableSlot whose tuple descriptor's attributes are
 * all of the hard-wired supported types; `self` a valid DestReceiver pointer.
 */
pub unsafe fn printsimple(slot: *mut TupleTableSlot, _self: *mut DestReceiver) -> bool {
    let tupdesc: TupleDesc = (*slot).tts_tupleDescriptor;
    let mut buf: StringInfoData = core::mem::zeroed();
    let buf: *mut StringInfoData = &mut buf;

    /* Make sure the tuple is fully deconstructed */
    slot_getallattrs(slot);

    /* Prepare and send message */
    pq_beginmessage(buf, PqMsg_DataRow);
    pq_sendint16(buf, (*tupdesc).natts as u16);

    let mut i: c_int = 0;
    while i < (*tupdesc).natts {
        let attr: Form_pg_attribute = TupleDescAttr(tupdesc, i);

        if *(*slot).tts_isnull.offset(i as isize) {
            pq_sendint32(buf, (-1i32) as u32);
            i += 1;
            continue;
        }

        let value: Datum = *(*slot).tts_values.offset(i as isize);

        /*
         * We can't call the regular type output functions here because we
         * might not have catalog access.  Instead, we must hard-wire knowledge
         * of the required types.
         */
        match (*attr).atttypid {
            TEXTOID => {
                let t: *mut crate::c::text = DatumGetTextPP(value);

                pq_sendcountedtext(
                    buf,
                    VARDATA_ANY(t as *const c_char),
                    VARSIZE_ANY_EXHDR(t as *const c_char) as c_int,
                );
            }

            INT4OID => {
                let num: i32 = DatumGetInt32(value);
                let mut str: [c_char; 12] = [0; 12]; /* sign, 10 digits and '\0' */
                let len: c_int = pg_ltoa(num, str.as_mut_ptr());
                pq_sendcountedtext(buf, str.as_ptr(), len);
            }

            INT8OID => {
                let num: i64 = DatumGetInt64(value);
                let mut str: [c_char; MAXINT8LEN + 1] = [0; MAXINT8LEN + 1];
                let len: c_int = pg_lltoa(num, str.as_mut_ptr());
                pq_sendcountedtext(buf, str.as_ptr(), len);
            }

            OIDOID => {
                /* C: `Oid num = ObjectIdGetDatum(value);` -- value flows Datum->Oid */
                let num: Oid = DatumGetObjectId(value);
                let mut str: [c_char; 10] = [0; 10]; /* 10 digits */
                let len: c_int = pg_ultoa_n(num, str.as_mut_ptr());
                pq_sendcountedtext(buf, str.as_ptr(), len);
            }

            _ => {
                elog!(ERROR, "unsupported type OID: {}", (*attr).atttypid);
            }
        }

        i += 1;
    }

    pq_endmessage(buf);

    true
}

/*
 * The permanent DestReceiver for DestRemoteSimple.  In C this is the file-static
 * `printsimpleDR`; exposed `pub` here so tcop/dest.rs's CreateDestReceiver can
 * reference it for DestRemoteSimple.  Note: in C this object has no rShutdown /
 * rDestroy (NULL), matching the bootstrap/replication usage.
 */
pub static printsimpleDR: DestReceiver = DestReceiver {
    receiveSlot: Some(printsimple),
    rStartup: Some(printsimple_startup),
    rShutdown: None,
    rDestroy: None,
    mydest: CommandDest::DestRemoteSimple,
};

#[cfg(test)]
mod tests {
    use super::*;

    // Smoke test: the static receiver is wired for DestRemoteSimple and its
    // method pointers point at the real functions defined here.
    #[test]
    fn printsimple_receiver_construction() {
        assert_eq!(printsimpleDR.mydest, CommandDest::DestRemoteSimple);
        assert!(printsimpleDR.receiveSlot.is_some());
        assert!(printsimpleDR.rStartup.is_some());
        // C leaves shutdown/destroy NULL for this receiver.
        assert!(printsimpleDR.rShutdown.is_none());
        assert!(printsimpleDR.rDestroy.is_none());

        // The fn pointers must be exactly printsimple / printsimple_startup.
        let recv = printsimpleDR.receiveSlot.unwrap();
        let start = printsimpleDR.rStartup.unwrap();
        assert_eq!(recv as usize, printsimple as usize);
        assert_eq!(start as usize, printsimple_startup as usize);
    }

    // Sanity: the locally-mirrored protocol bytes match the wire spec.
    #[test]
    fn protocol_message_bytes() {
        assert_eq!(PqMsg_RowDescription, b'T' as c_char);
        assert_eq!(PqMsg_DataRow, b'D' as c_char);
    }
}
