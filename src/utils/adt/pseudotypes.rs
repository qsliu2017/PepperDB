//! Translation of postgres/src/backend/utils/adt/pseudotypes.c
//!
//! Functions for the system pseudo-types.
//!
//! A pseudo-type isn't really a type and never has any operations, but we do
//! need to supply input and output functions to satisfy the links in the
//! pseudo-type's entry in pg_type.  In most cases the functions just throw an
//! error if invoked.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! The four C macros PSEUDOTYPE_DUMMY_INPUT_FUNC / PSEUDOTYPE_DUMMY_IO_FUNCS /
//! PSEUDOTYPE_DUMMY_RECEIVE_FUNC / PSEUDOTYPE_DUMMY_BINARY_IO_FUNCS are not
//! reproduced as Rust macros; each invocation is expanded into the explicit
//! `pub unsafe fn`s it would have generated (clearer and matches the established
//! style).  Each expanded body ends with `PG_RETURN_VOID!()` to mirror the C
//! "keep compiler quiet" trailing return after the divergent `ereport(ERROR)`.
//!
//! `#include`s mapped:
//!   - postgres.h            -> crate::prelude::*
//!   - libpq/pqformat.h      -> crate::libpq::pqformat::* (binary send/recv)
//!   - utils/fmgrprotos.h    -> crate::utils::fmgr::* (fmgr interface) +
//!                              crate::utils::adt::varlena::{textout,textsend}
//!                              for the pg_node_tree output delegations.
//!
//! STUBBED (deps not yet ported):
//!   - The "allow output" delegations to the array/enum/range/multirange I/O
//!     routines, which are not yet translated: anyarray_out/anyarray_send and
//!     anycompatiblearray_out/_send (array_out/array_send), anyenum_out
//!     (enum_out), anyrange_out and anycompatiblerange_out (range_out),
//!     anymultirange_out and anycompatiblemultirange_out (multirange_out).
//!
//! FULLY translated (the heart of the file): every `<type>_in` / `<type>_out`
//! that is a pure `ereport(ERROR, "cannot accept/display ...")`, plus the real
//! cstring_in/cstring_out/cstring_recv/cstring_send, void_in/void_out/void_recv/
//! void_send, shell_in/shell_out, and pg_node_tree_out/_send (delegating to the
//! ported textout/textsend).

use crate::prelude::*;
use crate::utils::fmgr::*;
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::libpq::pqformat::{
    pq_begintypsend, pq_endtypsend, pq_getmsgtext, pq_sendtext,
};
use crate::utils::adt::varlena::{textout, textsend};
use crate::utils::adt::arrayfuncs::{array_out, array_send};
use crate::utils::adt::r#enum::enum_out;
use crate::utils::adt::rangetypes::range_out;
use crate::utils::adt::multirangetypes::multirange_out;
use crate::{
    PG_GETARG_CSTRING, PG_GETARG_POINTER, PG_RETURN_BYTEA_P, PG_RETURN_CSTRING,
    PG_RETURN_VOID,
};
use core::ffi::c_char;

extern "C" {
    fn strlen(s: *const c_char) -> usize;
}

/*
 * errcodes.h classification used by the dummy functions (the errcode() shim
 * ignores the value, but we keep the symbol for fidelity with the C source).
 */
const ERRCODE_FEATURE_NOT_SUPPORTED: c_int = 0;

/*
 * These functions implement the body that the C macros
 *   PSEUDOTYPE_DUMMY_INPUT_FUNC / PSEUDOTYPE_DUMMY_IO_FUNCS /
 *   PSEUDOTYPE_DUMMY_RECEIVE_FUNC / PSEUDOTYPE_DUMMY_BINARY_IO_FUNCS
 * expand to: reject all input/output/receive/send attempts with an error.
 *
 * They are factored into small reporter helpers so each expanded fmgr function
 * is a one-liner, the same way uuid.rs factors string_to_uuid_syntax_error.
 */
unsafe fn cannot_accept_value_of_type(typname: &str) -> ! {
    let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
    ereport!(
        ERROR,
        errmsg!("cannot accept a value of type {}", typname)
    );
    /* ereport(ERROR) diverges at runtime; satisfy the never type for callers */
    unreachable!()
}

unsafe fn cannot_display_value_of_type(typname: &str) -> ! {
    let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
    ereport!(
        ERROR,
        errmsg!("cannot display a value of type {}", typname)
    );
    unreachable!()
}

/*
 * cstring
 *
 * cstring is marked as a pseudo-type because we don't want people using it
 * in tables.  But it's really a perfectly functional type, so provide
 * a full set of working I/O functions for it.  Among other things, this
 * allows manual invocation of datatype I/O functions, along the lines of
 * "SELECT foo_in('blah')" or "SELECT foo_out(some-foo-value)".
 */
pub unsafe fn cstring_in(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);

    PG_RETURN_CSTRING!(pstrdup(str));
}

pub unsafe fn cstring_out(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);

    PG_RETURN_CSTRING!(pstrdup(str));
}

pub unsafe fn cstring_recv(fcinfo: FunctionCallInfo) -> Datum {
    let buf: StringInfo = PG_GETARG_POINTER!(fcinfo, 0) as StringInfo;
    let str: *mut c_char;
    let mut nbytes: c_int = 0;

    str = pq_getmsgtext(buf, (*buf).len - (*buf).cursor, &mut nbytes);
    PG_RETURN_CSTRING!(str);
}

pub unsafe fn cstring_send(fcinfo: FunctionCallInfo) -> Datum {
    let str: *mut c_char = PG_GETARG_CSTRING!(fcinfo, 0);
    let mut buf: StringInfoData = core::mem::zeroed();

    pq_begintypsend(&mut buf);
    pq_sendtext(&mut buf, str, strlen(str) as c_int);
    PG_RETURN_BYTEA_P!(pq_endtypsend(&mut buf));
}

/*
 * anyarray
 *
 * We need to allow output of anyarray so that, e.g., pg_statistic columns
 * can be printed.  Input has to be disallowed, however.
 *
 * XXX anyarray_recv could actually be made to work, since the incoming
 * array data would contain the element type OID.  It seems unlikely that
 * it'd be sufficiently type-safe, though.
 */
// PSEUDOTYPE_DUMMY_INPUT_FUNC(anyarray);
pub unsafe fn anyarray_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("anyarray");
}
// PSEUDOTYPE_DUMMY_RECEIVE_FUNC(anyarray);
pub unsafe fn anyarray_recv(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("anyarray");
}

pub unsafe fn anyarray_out(fcinfo: FunctionCallInfo) -> Datum {
    return array_out(fcinfo);
}

pub unsafe fn anyarray_send(fcinfo: FunctionCallInfo) -> Datum {
    return array_send(fcinfo);
}

/*
 * anycompatiblearray
 *
 * We may as well allow output, since we do for anyarray.
 */
// PSEUDOTYPE_DUMMY_INPUT_FUNC(anycompatiblearray);
pub unsafe fn anycompatiblearray_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("anycompatiblearray");
}
// PSEUDOTYPE_DUMMY_RECEIVE_FUNC(anycompatiblearray);
pub unsafe fn anycompatiblearray_recv(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("anycompatiblearray");
}

pub unsafe fn anycompatiblearray_out(fcinfo: FunctionCallInfo) -> Datum {
    return array_out(fcinfo);
}

pub unsafe fn anycompatiblearray_send(fcinfo: FunctionCallInfo) -> Datum {
    return array_send(fcinfo);
}

/*
 * anyenum
 *
 * We may as well allow output, since enum_out will in fact work.
 */
// PSEUDOTYPE_DUMMY_INPUT_FUNC(anyenum);
pub unsafe fn anyenum_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("anyenum");
}

pub unsafe fn anyenum_out(fcinfo: FunctionCallInfo) -> Datum {
    return enum_out(fcinfo);
}

/*
 * anyrange
 *
 * We may as well allow output, since range_out will in fact work.
 */
// PSEUDOTYPE_DUMMY_INPUT_FUNC(anyrange);
pub unsafe fn anyrange_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("anyrange");
}

pub unsafe fn anyrange_out(fcinfo: FunctionCallInfo) -> Datum {
    return range_out(fcinfo);
}

/*
 * anycompatiblerange
 *
 * We may as well allow output, since range_out will in fact work.
 */
// PSEUDOTYPE_DUMMY_INPUT_FUNC(anycompatiblerange);
pub unsafe fn anycompatiblerange_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("anycompatiblerange");
}

pub unsafe fn anycompatiblerange_out(fcinfo: FunctionCallInfo) -> Datum {
    return range_out(fcinfo);
}

/*
 * anymultirange
 *
 * We may as well allow output, since multirange_out will in fact work.
 */
// PSEUDOTYPE_DUMMY_INPUT_FUNC(anymultirange);
pub unsafe fn anymultirange_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("anymultirange");
}

pub unsafe fn anymultirange_out(fcinfo: FunctionCallInfo) -> Datum {
    return multirange_out(fcinfo);
}

/*
 * anycompatiblemultirange
 *
 * We may as well allow output, since multirange_out will in fact work.
 */
// PSEUDOTYPE_DUMMY_INPUT_FUNC(anycompatiblemultirange);
pub unsafe fn anycompatiblemultirange_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("anycompatiblemultirange");
}

pub unsafe fn anycompatiblemultirange_out(fcinfo: FunctionCallInfo) -> Datum {
    return multirange_out(fcinfo);
}

/*
 * void
 *
 * We support void_in so that PL functions can return VOID without any
 * special hack in the PL handler.  Whatever value the PL thinks it's
 * returning will just be ignored.  Conversely, void_out and void_send
 * are needed so that "SELECT function_returning_void(...)" works.
 */
pub unsafe fn void_in(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    PG_RETURN_VOID!(); /* you were expecting something different? */
}

pub unsafe fn void_out(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    PG_RETURN_CSTRING!(pstrdup(c"".as_ptr()));
}

pub unsafe fn void_recv(fcinfo: FunctionCallInfo) -> Datum {
    /*
     * Note that since we consume no bytes, an attempt to send anything but an
     * empty string will result in an "invalid message format" error.
     */
    let _ = fcinfo;
    PG_RETURN_VOID!();
}

pub unsafe fn void_send(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    let mut buf: StringInfoData = core::mem::zeroed();

    /* send an empty string */
    pq_begintypsend(&mut buf);
    PG_RETURN_BYTEA_P!(pq_endtypsend(&mut buf));
}

/*
 * shell
 *
 * shell_in and shell_out are entered in pg_type for "shell" types
 * (those not yet filled in).  They should be unreachable, but we
 * set them up just in case some code path tries to do I/O without
 * having checked pg_type.typisdefined anywhere along the way.
 */
pub unsafe fn shell_in(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
    ereport!(
        ERROR,
        errmsg!("cannot accept a value of a shell type")
    );

    PG_RETURN_VOID!(); /* keep compiler quiet */
}

pub unsafe fn shell_out(fcinfo: FunctionCallInfo) -> Datum {
    let _ = fcinfo;
    let _ = errcode(ERRCODE_FEATURE_NOT_SUPPORTED);
    ereport!(
        ERROR,
        errmsg!("cannot display a value of a shell type")
    );

    PG_RETURN_VOID!(); /* keep compiler quiet */
}

/*
 * pg_node_tree
 *
 * pg_node_tree isn't really a pseudotype --- it's real enough to be a table
 * column --- but it presently has no operations of its own, and disallows
 * input too, so its I/O functions seem to fit here as much as anywhere.
 *
 * We must disallow input of pg_node_tree values because the SQL functions
 * that operate on the type are not secure against malformed input.
 * We do want to allow output, though.
 */
// PSEUDOTYPE_DUMMY_INPUT_FUNC(pg_node_tree);
pub unsafe fn pg_node_tree_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("pg_node_tree");
}
// PSEUDOTYPE_DUMMY_RECEIVE_FUNC(pg_node_tree);
pub unsafe fn pg_node_tree_recv(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("pg_node_tree");
}

pub unsafe fn pg_node_tree_out(fcinfo: FunctionCallInfo) -> Datum {
    return textout(fcinfo);
}

pub unsafe fn pg_node_tree_send(fcinfo: FunctionCallInfo) -> Datum {
    return textsend(fcinfo);
}

/*
 * pg_ddl_command
 *
 * Like pg_node_tree, pg_ddl_command isn't really a pseudotype; it's here
 * for the same reasons as that one.
 *
 * We don't have any good way to output this type directly, so punt
 * for output as well as input.
 */
// PSEUDOTYPE_DUMMY_IO_FUNCS(pg_ddl_command);
pub unsafe fn pg_ddl_command_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("pg_ddl_command");
}
pub unsafe fn pg_ddl_command_out(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_display_value_of_type("pg_ddl_command");
}
// PSEUDOTYPE_DUMMY_BINARY_IO_FUNCS(pg_ddl_command);
pub unsafe fn pg_ddl_command_recv(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("pg_ddl_command");
}
pub unsafe fn pg_ddl_command_send(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_display_value_of_type("pg_ddl_command");
}

/*
 * Dummy I/O functions for various other pseudotypes.
 */

// PSEUDOTYPE_DUMMY_IO_FUNCS(any);
pub unsafe fn any_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("any");
}
pub unsafe fn any_out(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_display_value_of_type("any");
}

// PSEUDOTYPE_DUMMY_IO_FUNCS(trigger);
pub unsafe fn trigger_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("trigger");
}
pub unsafe fn trigger_out(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_display_value_of_type("trigger");
}

// PSEUDOTYPE_DUMMY_IO_FUNCS(event_trigger);
pub unsafe fn event_trigger_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("event_trigger");
}
pub unsafe fn event_trigger_out(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_display_value_of_type("event_trigger");
}

// PSEUDOTYPE_DUMMY_IO_FUNCS(language_handler);
pub unsafe fn language_handler_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("language_handler");
}
pub unsafe fn language_handler_out(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_display_value_of_type("language_handler");
}

// PSEUDOTYPE_DUMMY_IO_FUNCS(fdw_handler);
pub unsafe fn fdw_handler_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("fdw_handler");
}
pub unsafe fn fdw_handler_out(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_display_value_of_type("fdw_handler");
}

// PSEUDOTYPE_DUMMY_IO_FUNCS(table_am_handler);
pub unsafe fn table_am_handler_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("table_am_handler");
}
pub unsafe fn table_am_handler_out(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_display_value_of_type("table_am_handler");
}

// PSEUDOTYPE_DUMMY_IO_FUNCS(index_am_handler);
pub unsafe fn index_am_handler_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("index_am_handler");
}
pub unsafe fn index_am_handler_out(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_display_value_of_type("index_am_handler");
}

// PSEUDOTYPE_DUMMY_IO_FUNCS(tsm_handler);
pub unsafe fn tsm_handler_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("tsm_handler");
}
pub unsafe fn tsm_handler_out(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_display_value_of_type("tsm_handler");
}

// PSEUDOTYPE_DUMMY_IO_FUNCS(internal);
pub unsafe fn internal_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("internal");
}
pub unsafe fn internal_out(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_display_value_of_type("internal");
}

// PSEUDOTYPE_DUMMY_IO_FUNCS(anyelement);
pub unsafe fn anyelement_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("anyelement");
}
pub unsafe fn anyelement_out(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_display_value_of_type("anyelement");
}

// PSEUDOTYPE_DUMMY_IO_FUNCS(anynonarray);
pub unsafe fn anynonarray_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("anynonarray");
}
pub unsafe fn anynonarray_out(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_display_value_of_type("anynonarray");
}

// PSEUDOTYPE_DUMMY_IO_FUNCS(anycompatible);
pub unsafe fn anycompatible_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("anycompatible");
}
pub unsafe fn anycompatible_out(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_display_value_of_type("anycompatible");
}

// PSEUDOTYPE_DUMMY_IO_FUNCS(anycompatiblenonarray);
pub unsafe fn anycompatiblenonarray_in(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_accept_value_of_type("anycompatiblenonarray");
}
pub unsafe fn anycompatiblenonarray_out(_fcinfo: FunctionCallInfo) -> Datum {
    cannot_display_value_of_type("anycompatiblenonarray");
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::postgres::{CStringGetDatum, DatumGetCString};
    use crate::postgres_ext::InvalidOid;
    use crate::utils::fmgr::DirectFunctionCall1Coll;

    unsafe fn cstr_eq(p: *mut c_char, want: &str) -> bool {
        let mut n = 0usize;
        while *p.add(n) != 0 {
            n += 1;
        }
        core::slice::from_raw_parts(p as *const u8, n) == want.as_bytes()
    }

    /* cstring_in/cstring_out are real: they copy the input string. */
    #[test]
    fn cstring_in_out_roundtrip() {
        unsafe {
            let input = c"hello, pseudotype";
            let d = DirectFunctionCall1Coll(cstring_in, InvalidOid, CStringGetDatum(input.as_ptr()));
            // Returned datum must be a freshly-pstrdup'd copy (distinct pointer)
            // that still compares equal.
            assert_ne!(DatumGetCString(d) as *const c_char, input.as_ptr());
            assert!(cstr_eq(DatumGetCString(d), "hello, pseudotype"));

            let out = DatumGetCString(DirectFunctionCall1Coll(cstring_out, InvalidOid, d));
            assert!(cstr_eq(out, "hello, pseudotype"));
            assert_ne!(out as *const c_char, DatumGetCString(d) as *const c_char);
        }
    }

    /*
     * void_send produces an empty bytea: just the VARHDRSZ length word, no
     * payload.  cstring_send produces a bytea whose payload is the raw string
     * bytes (VARHDRSZ + strlen).  Both go through the real pqformat path.
     */
    #[test]
    fn void_send_is_empty_bytea() {
        unsafe {
            use crate::postgres::DatumGetPointer;
            use crate::varatt::VARSIZE;
            let varhdrsz = crate::varatt::VARHDRSZ as usize;
            let d = DirectFunctionCall1Coll(void_send, InvalidOid, 0 as Datum);
            let p = DatumGetPointer(d) as *const c_char;
            assert_eq!(VARSIZE(p) as usize, varhdrsz);
        }
    }

    /*
     * NB: a cstring_send payload test is intentionally omitted - cstring_send
     * routes through pq_sendtext, which is still stubbed pending mb/mbutils
     * (pg_server_to_client).  void_send only uses pq_begintypsend/endtypsend,
     * so void_send_is_empty_bytea above is the live pqformat coverage here.
     */

    /* void_in returns (Datum)0; void_out returns a palloc'd empty string. */
    #[test]
    fn void_in_and_out() {
        unsafe {
            // void_in ignores its argument and returns VOID == (Datum) 0.
            let v = DirectFunctionCall1Coll(void_in, InvalidOid, CStringGetDatum(c"ignored".as_ptr()));
            assert_eq!(v, 0 as Datum);

            // void_out always yields the empty string.
            let out = DatumGetCString(DirectFunctionCall1Coll(void_out, InvalidOid, 0 as Datum));
            assert!(cstr_eq(out, ""));
        }
    }

    /* The dummy input functions must reject all input with an ERROR (panic). */
    #[test]
    #[should_panic]
    fn anyelement_in_rejects() {
        unsafe {
            DirectFunctionCall1Coll(anyelement_in, InvalidOid, 0 as Datum);
        }
    }

    #[test]
    #[should_panic]
    fn anyelement_out_rejects() {
        unsafe {
            DirectFunctionCall1Coll(anyelement_out, InvalidOid, 0 as Datum);
        }
    }

    #[test]
    #[should_panic]
    fn internal_in_rejects() {
        unsafe {
            DirectFunctionCall1Coll(internal_in, InvalidOid, 0 as Datum);
        }
    }

    #[test]
    #[should_panic]
    fn shell_in_rejects() {
        unsafe {
            DirectFunctionCall1Coll(shell_in, InvalidOid, 0 as Datum);
        }
    }

    #[test]
    #[should_panic]
    fn anyarray_in_rejects() {
        unsafe {
            DirectFunctionCall1Coll(anyarray_in, InvalidOid, 0 as Datum);
        }
    }
}
