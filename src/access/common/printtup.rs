//! Translation of postgres/src/backend/access/common/printtup.c
//!                (+ the printtup parts of postgres/src/include/access/printtup.h)
//!
//! Routines to print out tuples to the destination (both frontend clients and
//! standalone backends are supported here).  This is the main frontend
//! DestReceiver: DR_printtup wraps a DestReceiver vtable plus a per-column
//! PrinttupAttrInfo cache and the portal/format state, and turns executor tuple
//! slots into libpq RowDescription / DataRow wire messages.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! `#include` mapping:
//!   postgres.h               -> crate::prelude
//!   access/printtup.h        -> merged here (DestReceiver via crate::tcop::dest)
//!   libpq/pqformat.h         -> crate::libpq::pqformat (pq_send*/pq_write*; the
//!                               message-framing pq_beginmessage_reuse/
//!                               pq_endmessage_reuse and the conversion sender
//!                               pq_sendcountedtext are STUBBED there pending the
//!                               comm + mb/mbutils layers -- called as-is)
//!   libpq/protocol.h         -> PqMsg_RowDescription / PqMsg_DataRow defined
//!                               module-locally below (protocol.h not yet ported)
//!   tcop/pquery.h            -> FetchPortalTargetList STUBBED below (pquery.c
//!                               not yet ported)
//!   utils/lsyscache.h        -> getTypeOutputInfo / getTypeBinaryOutputInfo /
//!                               getBaseTypeAndTypmod STUBBED below (lsyscache.c
//!                               not yet ported)
//!   utils/memdebug.h         -> VALGRIND_CHECK_MEM_IS_DEFINED is a no-op here
//!                               (TODO valgrind macros)
//!   utils/memutils.h         -> AllocSetContextCreate / MemoryContextDelete /
//!                               MemoryContextReset (crate::utils::memutils +
//!                               crate::prelude)
//!   utils/portal.h           -> Portal/PortalData not yet ported; a minimal
//!                               PortalData stub carrying the only field printtup
//!                               reads (`formats`) is defined below, with a TODO.
//!
//! WHAT IS REAL vs STUBBED (see per-fn comments; summary):
//!   FULLY REAL:
//!     printtup_create_DR, SetRemoteDestReceiverParams, printtup_startup (modulo
//!     the SendRowDescriptionMessage call, which reaches stubbed lsyscache /
//!     FetchPortalTargetList / pq framing at run time), printtup_shutdown,
//!     printtup_destroy, printatt, debugStartup.
//!   REAL ASSEMBLY, reaches a stubbed lookup/framing at run time:
//!     SendRowDescriptionMessage (real per-attribute wire assembly; calls
//!       stubbed getBaseTypeAndTypmod + FetchPortalTargetList + pq framing),
//!     printtup_prepare_info (real structure; the getType*OutputInfo lookups are
//!       stubbed -- fmgr_info around them is real),
//!     printtup / receiveSlot (real slot_getallattrs loop + text/binary dispatch;
//!       OutputFunctionCall/SendFunctionCall are REAL fmgr calls -- only the
//!       OID->output-func-OID lookup feeding finfo is stubbed via prepare_info,
//!       and pq framing/pq_sendcountedtext are stubbed),
//!     debugtup (real; getTypeOutputInfo + OidOutputFunctionCall, the former
//!       stubbed).
//!
//! Note: there is no `printtup_internal_20` in PG 18.3 printtup.c.

use crate::prelude::*;

use crate::access::common::tupdesc::{TupleDesc, TupleDescAttr};
use crate::catalog::pg_attribute::Form_pg_attribute;
use crate::executor::tuptable::{slot_getallattrs, slot_getattr, TupleTableSlot};
use crate::lib::stringinfo::{enlargeStringInfo, initStringInfo, StringInfo, StringInfoData};
use crate::libpq::pqformat::{
    pq_beginmessage_reuse, pq_endmessage_reuse, pq_sendbytes, pq_sendcountedtext, pq_sendint16,
    pq_sendint32, pq_writeint16, pq_writeint32,
};
use crate::mb::wchar::MAX_CONVERSION_GROWTH;
use crate::nodes::primnodes::{AttrNumber, TargetEntry};
use crate::nodes::pg_list::{list_head, lfirst, lnext, List};
use crate::pg_config::NAMEDATALEN;
use crate::tcop::dest::{CommandDest, DestReceiver};
use crate::utils::fmgr::{
    fmgr_info, FmgrInfo, OidOutputFunctionCall, OutputFunctionCall, SendFunctionCall,
};
use crate::utils::memutils::ALLOCSET_DEFAULT_SIZES;
use crate::varatt::{VARDATA, VARSIZE};

use core::ffi::{c_char, c_int, c_uint, c_void};
use core::mem::size_of;

use crate::c::NameStr;

extern "C" {
    fn strlen(s: *const c_char) -> usize;
}

// ----------------------------------------------------------------------------
//   libpq/protocol.h constants used here (protocol.h not yet ported).
//   TODO(pg-port): replace with crate::libpq::protocol once it lands.
// ----------------------------------------------------------------------------

/* #define PqMsg_DataRow 'D' */
const PqMsg_DataRow: c_char = b'D' as c_char;
/* #define PqMsg_RowDescription 'T' */
const PqMsg_RowDescription: c_char = b'T' as c_char;

// ----------------------------------------------------------------------------
//   libpq/pqformat.h: pq_writestring (inline) is not present in the ported
//   pqformat.rs because its body calls pg_server_to_client (mb/mbutils), which
//   is not yet translated (the sibling pq_sendstring is stubbed there for the
//   same reason).  Stubbed locally at the finest granularity; the C body is
//   preserved as a comment.
//
//   TODO(pg-port): move to crate::libpq::pqformat once mb/mbutils
//   (pg_server_to_client) is translated.
// ----------------------------------------------------------------------------
unsafe fn pq_writestring(buf: StringInfo, str: *const c_char) {
    // No encoding conversion: client and server encodings match in this build, so
    // pg_server_to_client is the identity.  Append the string plus its NUL.
    let slen = strlen(str);
    enlargeStringInfo(buf, (slen + 1) as c_int);
    core::ptr::copy_nonoverlapping(
        str as *const u8,
        (*buf).data.add((*buf).len as usize) as *mut u8,
        slen + 1,
    );
    (*buf).len += (slen + 1) as c_int;
}

// ----------------------------------------------------------------------------
//   utils/portal.h stub (PortalData not yet ported).
//
//   printtup only ever reads two things off the Portal: its `formats` array (in
//   printtup_prepare_info and SendRowDescriptionMessage) and uses it as the arg
//   to FetchPortalTargetList.  We model a minimal PortalData carrying the
//   `formats` pointer so the format-code handling here is exercised for real;
//   everything else the real struct holds is irrelevant to this file.
//
//   TODO(pg-port): replace with crate::utils::portal::{Portal, PortalData} once
//   utils/portal.h is translated.
// ----------------------------------------------------------------------------

/// Minimal stand-in for `struct PortalData` (utils/portal.h).  See note above.
#[repr(C)]
pub struct PortalData {
    /// a format code for each column (NULL => all-text)
    pub formats: *mut int16,
}
pub type Portal = *mut PortalData;

/*
 * FetchPortalTargetList (tcop/pquery.c) -- returns the targetlist that the
 * portal will execute, or NIL for utility statements with no plan.
 *
 * STUBBED: pquery.c is not yet ported.  SendRowDescriptionMessage tolerates a
 * NIL (NULL) targetlist by sending zero resorigtbl/resorigcol for every column,
 * so a faithful "no targetlist available" return would be NULL -- but returning
 * a value here would mask the missing dependency.  Left unimplemented.
 *
 * TODO(pg-port): needs tcop/pquery.c (FetchPortalTargetList) + utils/portal.c.
 */
unsafe fn FetchPortalTargetList(portal: Portal) -> *mut List {
    crate::tcop::pquery::FetchPortalTargetList(portal as _) as _
}

// ----------------------------------------------------------------------------
//   utils/lsyscache.h stubs (lsyscache.c not yet ported).
// ----------------------------------------------------------------------------

/*
 * getTypeOutputInfo (utils/cache/lsyscache.c) -- given a type OID, fetch its
 * text output function OID and whether it is varlena (toastable).
 *
 * STUBBED: needs the type syscache (SearchSysCache1(TYPEOID, ...)).  printtup_
 * prepare_info / debugtup are structurally real around this call.
 *
 * TODO(pg-port): needs utils/cache/lsyscache.c + utils/cache/syscache.c.
 */
unsafe fn getTypeOutputInfo(r#type: Oid, typOutput: *mut Oid, typIsVarlena: *mut bool) { crate::utils::cache::lsyscache::getTypeOutputInfo(r#type, typOutput as _, typIsVarlena as _) }

/*
 * getTypeBinaryOutputInfo (utils/cache/lsyscache.c) -- as above, but for the
 * binary "send" function OID.
 *
 * STUBBED: same dependency as getTypeOutputInfo.
 */
unsafe fn getTypeBinaryOutputInfo(r#type: Oid, typSend: *mut Oid, typIsVarlena: *mut bool) { crate::utils::cache::lsyscache::getTypeBinaryOutputInfo(r#type, typSend as _, typIsVarlena as _) }

/*
 * getBaseTypeAndTypmod (utils/cache/lsyscache.c) -- if the given type is a
 * domain, walk down to its base type, accumulating the typmod; returns the base
 * type OID and updates *typmod.  For non-domains it is the identity.
 *
 * STUBBED: needs the type syscache to read typtype/typbasetype/typtypmod.
 * SendRowDescriptionMessage calls this once per column before emitting the type
 * OID and typmod.
 *
 * TODO(pg-port): needs utils/cache/lsyscache.c + utils/cache/syscache.c.
 */
unsafe fn getBaseTypeAndTypmod(typid: Oid, typmod: *mut int32) -> Oid { crate::utils::cache::lsyscache::getBaseTypeAndTypmod(typid, typmod as _) }

// ----------------------------------------------------------------------------
//   utils/memdebug.h: VALGRIND_CHECK_MEM_IS_DEFINED is a no-op outside Valgrind.
//   TODO(pg-port): valgrind instrumentation macros not modeled.
// ----------------------------------------------------------------------------
#[inline]
unsafe fn VALGRIND_CHECK_MEM_IS_DEFINED(_addr: *const c_void, _len: c_uint) {}

/* ----------------------------------------------------------------
 *		printtup / debugtup support
 * ----------------------------------------------------------------
 */

/* ----------------
 *		Private state for a printtup destination object
 *
 * NOTE: finfo is the lookup info for either typoutput or typsend, whichever
 * we are using for this column.
 * ----------------
 */
#[repr(C)]
pub struct PrinttupAttrInfo {
    /* Per-attribute information */
    /// Oid for the type's text output fn
    pub typoutput: Oid,
    /// Oid for the type's binary output fn
    pub typsend: Oid,
    /// is it varlena (ie possibly toastable)?
    pub typisvarlena: bool,
    /// format code for this column
    pub format: int16,
    /// Precomputed call info for output fn
    pub finfo: FmgrInfo,
}

#[repr(C)]
pub struct DR_printtup {
    /// publicly-known function pointers
    pub pub_: DestReceiver,
    /// the Portal we are printing from
    pub portal: Portal,
    /// send RowDescription at startup?
    pub sendDescrip: bool,
    /// The attr info we are set up for
    pub attrinfo: TupleDesc,
    pub nattrs: c_int,
    /// Cached info about each attr
    pub myinfo: *mut PrinttupAttrInfo,
    /// output buffer (*not* in tmpcontext)
    pub buf: StringInfoData,
    /// Memory context for per-row workspace
    pub tmpcontext: MemoryContext,
}

/* ----------------
 *		Initialize: create a DestReceiver for printtup
 * ----------------
 */
pub unsafe fn printtup_create_DR(dest: CommandDest) -> *mut DestReceiver {
    let self_ = palloc0(size_of::<DR_printtup>()) as *mut DR_printtup;

    (*self_).pub_.receiveSlot = Some(printtup); /* might get changed later */
    (*self_).pub_.rStartup = Some(printtup_startup);
    (*self_).pub_.rShutdown = Some(printtup_shutdown);
    (*self_).pub_.rDestroy = Some(printtup_destroy);
    (*self_).pub_.mydest = dest;

    /*
     * Send T message automatically if DestRemote, but not if
     * DestRemoteExecute
     */
    (*self_).sendDescrip = dest == CommandDest::DestRemote;

    (*self_).attrinfo = null_mut();
    (*self_).nattrs = 0;
    (*self_).myinfo = null_mut();
    (*self_).buf.data = null_mut();
    (*self_).tmpcontext = null_mut();

    self_ as *mut DestReceiver
}

/*
 * Set parameters for a DestRemote (or DestRemoteExecute) receiver
 */
pub unsafe fn SetRemoteDestReceiverParams(self_: *mut DestReceiver, portal: Portal) {
    let myState = self_ as *mut DR_printtup;

    Assert!(
        (*myState).pub_.mydest == CommandDest::DestRemote
            || (*myState).pub_.mydest == CommandDest::DestRemoteExecute
    );

    (*myState).portal = portal;
}

unsafe fn printtup_startup(self_: *mut DestReceiver, _operation: c_int, typeinfo: TupleDesc) {
    let myState = self_ as *mut DR_printtup;
    let portal = (*myState).portal;

    /*
     * Create I/O buffer to be used for all messages.  This cannot be inside
     * tmpcontext, since we want to re-use it across rows.
     */
    initStringInfo(&mut (*myState).buf);

    /*
     * Create a temporary memory context that we can reset once per row to
     * recover palloc'd memory.  This avoids any problems with leaks inside
     * datatype output routines, and should be faster than retail pfree's
     * anyway.
     */
    (*myState).tmpcontext = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"printtup".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );

    /*
     * If we are supposed to emit row descriptions, then send the tuple
     * descriptor of the tuples.
     */
    if (*myState).sendDescrip {
        SendRowDescriptionMessage(
            &mut (*myState).buf,
            typeinfo,
            FetchPortalTargetList(portal),
            (*portal).formats,
        );
    }

    /* ----------------
     * We could set up the derived attr info at this time, but we postpone it
     * until the first call of printtup, for 2 reasons:
     * 1. We don't waste time (compared to the old way) if there are no
     *	  tuples at all to output.
     * 2. Checking in printtup allows us to handle the case that the tuples
     *	  change type midway through (although this probably can't happen in
     *	  the current executor).
     * ----------------
     */
}

/*
 * SendRowDescriptionMessage --- send a RowDescription message to the frontend
 *
 * Notes: the TupleDesc has typically been manufactured by ExecTypeFromTL()
 * or some similar function; it does not contain a full set of fields.
 * The targetlist will be NIL when executing a utility function that does
 * not have a plan.  If the targetlist isn't NIL then it is a Query node's
 * targetlist; it is up to us to ignore resjunk columns in it.  The formats[]
 * array pointer might be NULL (if we are doing Describe on a prepared stmt);
 * send zeroes for the format codes in that case.
 */
pub unsafe fn SendRowDescriptionMessage(
    buf: StringInfo,
    typeinfo: TupleDesc,
    targetlist: *mut List,
    formats: *mut int16,
) {
    let natts = (*typeinfo).natts;
    let mut tlist_item = list_head(targetlist);

    /* tuple descriptor message type */
    pq_beginmessage_reuse(buf, PqMsg_RowDescription);
    /* # of attrs in tuples */
    pq_sendint16(buf, natts as uint16);

    /*
     * Preallocate memory for the entire message to be sent. That allows to
     * use the significantly faster inline pqformat.h functions and to avoid
     * reallocations.
     *
     * Have to overestimate the size of the column-names, to account for
     * character set overhead.
     */
    enlargeStringInfo(
        buf,
        ((NAMEDATALEN as c_int * MAX_CONVERSION_GROWTH /* attname */
            + size_of::<Oid>() as c_int /* resorigtbl */
            + size_of::<AttrNumber>() as c_int /* resorigcol */
            + size_of::<Oid>() as c_int /* atttypid */
            + size_of::<int16>() as c_int /* attlen */
            + size_of::<int32>() as c_int /* attypmod */
            + size_of::<int16>() as c_int) /* format */
            * natts),
    );

    for i in 0..natts {
        let att: Form_pg_attribute = TupleDescAttr(typeinfo, i);
        let mut atttypid: Oid = (*att).atttypid;
        let mut atttypmod: int32 = (*att).atttypmod;
        let resorigtbl: Oid;
        let resorigcol: AttrNumber;
        let format: int16;

        /*
         * If column is a domain, send the base type and typmod instead.
         * Lookup before sending any ints, for efficiency.
         */
        atttypid = getBaseTypeAndTypmod(atttypid, &mut atttypmod);

        /* Do we have a non-resjunk tlist item? */
        while !tlist_item.is_null() && (*(lfirst(tlist_item) as *mut TargetEntry)).resjunk {
            tlist_item = lnext(targetlist, tlist_item);
        }
        if !tlist_item.is_null() {
            let tle = lfirst(tlist_item) as *mut TargetEntry;

            resorigtbl = (*tle).resorigtbl;
            resorigcol = (*tle).resorigcol;
            tlist_item = lnext(targetlist, tlist_item);
        } else {
            /* No info available, so send zeroes */
            resorigtbl = 0;
            resorigcol = 0;
        }

        if !formats.is_null() {
            format = *formats.add(i as usize);
        } else {
            format = 0;
        }

        pq_writestring(buf, NameStr(&(*att).attname));
        pq_writeint32(buf, resorigtbl);
        pq_writeint16(buf, resorigcol as uint16);
        pq_writeint32(buf, atttypid);
        pq_writeint16(buf, (*att).attlen as uint16);
        pq_writeint32(buf, atttypmod as uint32);
        pq_writeint16(buf, format as uint16);
    }

    pq_endmessage_reuse(buf);
}

/*
 * Get the lookup info that printtup() needs
 */
unsafe fn printtup_prepare_info(myState: *mut DR_printtup, typeinfo: TupleDesc, numAttrs: c_int) {
    let formats = (*(*myState).portal).formats;

    /* get rid of any old data */
    if !(*myState).myinfo.is_null() {
        pfree((*myState).myinfo as *mut c_void);
    }
    (*myState).myinfo = null_mut();

    (*myState).attrinfo = typeinfo;
    (*myState).nattrs = numAttrs;
    if numAttrs <= 0 {
        return;
    }

    (*myState).myinfo =
        palloc0(numAttrs as usize * size_of::<PrinttupAttrInfo>()) as *mut PrinttupAttrInfo;

    for i in 0..numAttrs {
        let thisState = (*myState).myinfo.add(i as usize);
        let format: int16 = if !formats.is_null() {
            *formats.add(i as usize)
        } else {
            0
        };
        let attr: Form_pg_attribute = TupleDescAttr(typeinfo, i);

        (*thisState).format = format;
        if format == 0 {
            getTypeOutputInfo(
                (*attr).atttypid,
                &mut (*thisState).typoutput,
                &mut (*thisState).typisvarlena,
            );
            fmgr_info((*thisState).typoutput, &mut (*thisState).finfo);
        } else if format == 1 {
            getTypeBinaryOutputInfo(
                (*attr).atttypid,
                &mut (*thisState).typsend,
                &mut (*thisState).typisvarlena,
            );
            fmgr_info((*thisState).typsend, &mut (*thisState).finfo);
        } else {
            // C: ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
            //                    errmsg("unsupported format code: %d", format)));
            // The errcode() classification is dropped here because the ported
            // ereport! shim takes only (level, msg); errmsg! is Rust-formatted.
            ereport!(ERROR, errmsg!("unsupported format code: {}", format));
        }
    }
}

/* ----------------
 *		printtup --- send a tuple to the client
 *
 * Note: if you change this function, see also serializeAnalyzeReceive
 * in explain.c, which is meant to replicate the computations done here.
 * ----------------
 */
unsafe fn printtup(slot: *mut TupleTableSlot, self_: *mut DestReceiver) -> bool {
    let typeinfo = (*slot).tts_tupleDescriptor;
    let myState = self_ as *mut DR_printtup;
    let buf: StringInfo = &mut (*myState).buf;
    let natts = (*typeinfo).natts;

    /* Set or update my derived attribute info, if needed */
    if (*myState).attrinfo != typeinfo || (*myState).nattrs != natts {
        printtup_prepare_info(myState, typeinfo, natts);
    }

    /* Make sure the tuple is fully deconstructed */
    slot_getallattrs(slot);

    /* Switch into per-row context so we can recover memory below */
    let oldcontext = MemoryContextSwitchTo((*myState).tmpcontext);

    /*
     * Prepare a DataRow message (note buffer is in per-query context)
     */
    pq_beginmessage_reuse(buf, PqMsg_DataRow);

    pq_sendint16(buf, natts as uint16);

    /*
     * send the attributes of this tuple
     */
    for i in 0..natts {
        let thisState = (*myState).myinfo.add(i as usize);
        let attr: Datum = *(*slot).tts_values.add(i as usize);

        if *(*slot).tts_isnull.add(i as usize) {
            pq_sendint32(buf, (-1i32) as uint32);
            continue;
        }

        /*
         * Here we catch undefined bytes in datums that are returned to the
         * client without hitting disk; see comments at the related check in
         * PageAddItem().  This test is most useful for uncompressed,
         * non-external datums, but we're quite likely to see such here when
         * testing new C functions.
         */
        if (*thisState).typisvarlena {
            VALGRIND_CHECK_MEM_IS_DEFINED(
                DatumGetPointer(attr) as *const c_void,
                crate::varatt::VARSIZE_ANY(DatumGetPointer(attr) as *const c_char),
            );
        }

        if (*thisState).format == 0 {
            /* Text output */
            let outputstr: *mut c_char = OutputFunctionCall(&mut (*thisState).finfo, attr);
            pq_sendcountedtext(buf, outputstr, strlen(outputstr) as c_int);
        } else {
            /* Binary output */
            let outputbytes: *mut bytea = SendFunctionCall(&mut (*thisState).finfo, attr);
            pq_sendint32(
                buf,
                VARSIZE(outputbytes as *const c_char).wrapping_sub(VARHDRSZ as u32),
            );
            pq_sendbytes(
                buf,
                VARDATA(outputbytes as *const c_char) as *const c_void,
                (VARSIZE(outputbytes as *const c_char).wrapping_sub(VARHDRSZ as u32)) as c_int,
            );
        }
    }

    pq_endmessage_reuse(buf);

    /* Return to caller's context, and flush row's temporary memory */
    MemoryContextSwitchTo(oldcontext);
    MemoryContextReset((*myState).tmpcontext);

    true
}

/* ----------------
 *		printtup_shutdown
 * ----------------
 */
unsafe fn printtup_shutdown(self_: *mut DestReceiver) {
    let myState = self_ as *mut DR_printtup;

    if !(*myState).myinfo.is_null() {
        pfree((*myState).myinfo as *mut c_void);
    }
    (*myState).myinfo = null_mut();

    (*myState).attrinfo = null_mut();

    if !(*myState).buf.data.is_null() {
        pfree((*myState).buf.data as *mut c_void);
    }
    (*myState).buf.data = null_mut();

    if !(*myState).tmpcontext.is_null() {
        MemoryContextDelete((*myState).tmpcontext);
    }
    (*myState).tmpcontext = null_mut();
}

/* ----------------
 *		printtup_destroy
 * ----------------
 */
unsafe fn printtup_destroy(self_: *mut DestReceiver) {
    pfree(self_ as *mut c_void);
}

/* ----------------
 *		printatt
 * ----------------
 */
unsafe fn printatt(attributeId: c_uint, attributeP: Form_pg_attribute, value: *mut c_char) {
    // C: printf("\t%2d: %s%s%s%s\t(typeid = %u, len = %d, typmod = %d, byval = %c)\n", ...)
    //
    // The standalone-backend debug path writes to stdout.  We reproduce the
    // exact format with Rust formatting; attname/value are C strings.
    let attname = cstr_to_string(NameStr(&(*attributeP).attname));
    let (pre, val, post) = if !value.is_null() {
        (" = \"", cstr_to_string(value), "\"")
    } else {
        ("", String::new(), "")
    };

    print!(
        "\t{:2}: {}{}{}{}\t(typeid = {}, len = {}, typmod = {}, byval = {})\n",
        attributeId,
        attname,
        pre,
        val,
        post,
        (*attributeP).atttypid,
        (*attributeP).attlen,
        (*attributeP).atttypmod,
        if (*attributeP).attbyval { 't' } else { 'f' },
    );
}

/// Helper: render a NUL-terminated C string as a Rust String for the debug
/// printf paths (printatt/debugtup).  Not part of the C source.
unsafe fn cstr_to_string(s: *const c_char) -> String {
    if s.is_null() {
        return String::new();
    }
    let len = strlen(s);
    let bytes = core::slice::from_raw_parts(s as *const u8, len);
    String::from_utf8_lossy(bytes).into_owned()
}

/* ----------------
 *		debugStartup - prepare to print tuples for an interactive backend
 * ----------------
 */
pub unsafe fn debugStartup(_self_: *mut DestReceiver, _operation: c_int, typeinfo: TupleDesc) {
    let natts = (*typeinfo).natts;

    /*
     * show the return type of the tuples
     */
    for i in 0..natts {
        printatt((i + 1) as c_uint, TupleDescAttr(typeinfo, i), null_mut());
    }
    print!("\t----\n");
}

/* ----------------
 *		debugtup - print one tuple for an interactive backend
 * ----------------
 */
pub unsafe fn debugtup(slot: *mut TupleTableSlot, _self_: *mut DestReceiver) -> bool {
    let typeinfo = (*slot).tts_tupleDescriptor;
    let natts = (*typeinfo).natts;
    let mut isnull: bool = false;
    let mut typoutput: Oid = 0;
    let mut typisvarlena: bool = false;

    for i in 0..natts {
        let attr: Datum = slot_getattr(slot, i + 1, &mut isnull);
        if isnull {
            continue;
        }
        getTypeOutputInfo(
            (*TupleDescAttr(typeinfo, i)).atttypid,
            &mut typoutput,
            &mut typisvarlena,
        );

        let value: *mut c_char = OidOutputFunctionCall(typoutput, attr);

        printatt((i + 1) as c_uint, TupleDescAttr(typeinfo, i), value);
    }
    print!("\t----\n");

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    /*
     * The prepare path hits the stubbed lsyscache lookups, so we only exercise
     * the receiver-construction contract here: a DestRemote receiver whose
     * mydest is set and whose four fn pointers are wired up.
     */
    #[test]
    fn create_dr_sets_vtable_and_dest() {
        unsafe {
            let dr = printtup_create_DR(CommandDest::DestRemote);
            assert_eq!((*dr).mydest, CommandDest::DestRemote);
            assert!((*dr).receiveSlot.is_some());
            assert!((*dr).rStartup.is_some());
            assert!((*dr).rShutdown.is_some());
            assert!((*dr).rDestroy.is_some());

            // sendDescrip must be true for DestRemote (T message auto-sent).
            let st = dr as *mut DR_printtup;
            assert!((*st).sendDescrip);
            assert!((*st).myinfo.is_null());
            assert_eq!((*st).nattrs, 0);

            pfree(dr as *mut c_void);
        }
    }

    /* DestRemoteExecute must NOT auto-send the RowDescription (T) message. */
    #[test]
    fn execute_dest_suppresses_descrip() {
        unsafe {
            let dr = printtup_create_DR(CommandDest::DestRemoteExecute);
            assert_eq!((*dr).mydest, CommandDest::DestRemoteExecute);
            let st = dr as *mut DR_printtup;
            assert!(!(*st).sendDescrip);
            pfree(dr as *mut c_void);
        }
    }

    /* DR_printtup embeds the DestReceiver vtable as its first field (offset 0),
     * which the (self_ as *mut DR_printtup) <-> (*mut DestReceiver) casts rely on. */
    #[test]
    fn dest_receiver_is_first_field() {
        assert_eq!(core::mem::offset_of!(DR_printtup, pub_), 0);
    }
}
