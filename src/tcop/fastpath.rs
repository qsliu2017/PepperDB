//! tcop/fastpath.c - routines to handle function requests from the frontend (server side of PQfn).

use crate::prelude::*;
use crate::port::strlcpy::strlcpy;

use crate::{InitFunctionCallInfoData, LOCAL_FCINFO, FunctionCallInvoke};

use crate::access::htup_details::{GETSTRUCT, HeapTuple, HeapTupleIsValid};
use crate::catalog::catalog_oids::{NamespaceRelationId, ProcedureRelationId};
use crate::catalog::objectaccess::{RunFunctionExecuteHook, RunNamespaceSearchHook};
use crate::catalog::pg_proc::{Form_pg_proc, PROKIND_FUNCTION};
use crate::lib::stringinfo::{StringInfo, StringInfoData};
use crate::libpq::protocol::PqMsg_FunctionCallResponse;
use crate::miscadmin::{GetUserId, CHECK_FOR_INTERRUPTS};
use crate::nodes::parsenodes::{AclMode, ObjectType, ACL_EXECUTE, ACL_USAGE};
use crate::nodes::parsenodes::ObjectType::{OBJECT_FUNCTION, OBJECT_SCHEMA};
use crate::pg_config_manual::{FUNC_MAX_ARGS, NAMEDATALEN};
use crate::tcop::tcopprot::{log_statement, LOGSTMT_ALL};
use crate::utils::fmgr::{fmgr_info, FmgrInfo, FunctionCallInfo};
use crate::utils::palloc::palloc;
use crate::varatt::{VARDATA, VARSIZE};
use crate::c::NameStr;

use std::ffi::c_short;

/*
 * Formerly, this code attempted to cache the function and type info
 * looked up by fetch_fp_info, but only for the duration of a single
 * transaction command (since in theory the info could change between
 * commands).  This was utterly useless, because postgres.c executes
 * each fastpath call as a separate transaction command, and so the
 * cached data could never actually have been reused.  If it had worked
 * as intended, it would have had problems anyway with dangling references
 * in the FmgrInfo struct.  So, forget about caching and just repeat the
 * syscache fetches on each usage.  They're not *that* expensive.
 */
#[repr(C)]
pub struct fp_info {
    pub funcid: Oid,
    pub flinfo: FmgrInfo, /* function lookup info for funcid */
    pub namespace: Oid,   /* other stuff from pg_proc */
    pub rettype: Oid,
    pub argtypes: [Oid; FUNC_MAX_ARGS],
    pub fname: [c_char; NAMEDATALEN], /* function name for logging */
}

// ---------------------------------------------------------------------------
// Local stubs for callees whose source files are not yet ported.
// ---------------------------------------------------------------------------

pub type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;

const PROCOID: c_int = 0; // syscache id; stubbed

/* access/xact.c */
unsafe fn IsAbortedTransactionBlockState() -> bool {
    unimplemented!("STUB: access/xact.c not ported")
}

/* utils/time/snapmgr.c */
type Snapshot = *mut c_void;
unsafe fn GetTransactionSnapshot() -> Snapshot {
    unimplemented!("STUB: utils/time/snapmgr.c not ported")
}
unsafe fn PushActiveSnapshot(_snapshot: Snapshot) {
    unimplemented!("STUB: utils/time/snapmgr.c not ported")
}
unsafe fn PopActiveSnapshot() {
    unimplemented!("STUB: utils/time/snapmgr.c not ported")
}

/* utils/cache/syscache.c */
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!("STUB: utils/cache/syscache.c not ported")
}
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!("STUB: utils/cache/syscache.c not ported")
}

/* utils/adt/acl.c + catalog acl */
unsafe fn object_aclcheck(
    _classid: Oid,
    _objectid: Oid,
    _roleid: Oid,
    _mode: AclMode,
) -> AclResult {
    unimplemented!("STUB: catalog/aclchk.c not ported")
}
unsafe fn aclcheck_error(_aclerr: AclResult, _objtype: ObjectType, _objectname: *const c_char) {
    unimplemented!("STUB: catalog/aclchk.c not ported")
}

/* utils/cache/lsyscache.c */
unsafe fn getTypeOutputInfo(_type: Oid, _typOutput: *mut Oid, _typIsVarlena: *mut bool) {
    unimplemented!("STUB: utils/cache/lsyscache.c not ported")
}
unsafe fn getTypeBinaryOutputInfo(_type: Oid, _typSend: *mut Oid, _typIsVarlena: *mut bool) {
    unimplemented!("STUB: utils/cache/lsyscache.c not ported")
}
unsafe fn getTypeInputInfo(_type: Oid, _typInput: *mut Oid, _typIOParam: *mut Oid) {
    unimplemented!("STUB: utils/cache/lsyscache.c not ported")
}
unsafe fn getTypeBinaryInputInfo(_type: Oid, _typReceive: *mut Oid, _typIOParam: *mut Oid) {
    unimplemented!("STUB: utils/cache/lsyscache.c not ported")
}
unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char {
    unimplemented!("STUB: utils/cache/lsyscache.c not ported")
}
unsafe fn get_func_name(_funcid: Oid) -> *mut c_char {
    unimplemented!("STUB: utils/cache/lsyscache.c not ported")
}

/* fmgr.c convenience callers */
unsafe fn OidOutputFunctionCall(_functionId: Oid, _val: Datum) -> *mut c_char {
    unimplemented!("STUB: utils/fmgr/fmgr.c OidOutputFunctionCall not ported")
}
unsafe fn OidSendFunctionCall(_functionId: Oid, _val: Datum) -> *mut bytea {
    unimplemented!("STUB: utils/fmgr/fmgr.c OidSendFunctionCall not ported")
}
unsafe fn OidInputFunctionCall(
    _functionId: Oid,
    _str: *mut c_char,
    _typioparam: Oid,
    _typmod: int32,
) -> Datum {
    unimplemented!("STUB: utils/fmgr/fmgr.c OidInputFunctionCall not ported")
}
unsafe fn OidReceiveFunctionCall(
    _functionId: Oid,
    _buf: StringInfo,
    _typioparam: Oid,
    _typmod: int32,
) -> Datum {
    unimplemented!("STUB: utils/fmgr/fmgr.c OidReceiveFunctionCall not ported")
}

/* mb/mbutils.c */
unsafe fn pg_client_to_server(_s: *const c_char, _len: c_int) -> *mut c_char {
    unimplemented!("STUB: mb/mbutils.c not ported")
}

/* tcop/postgres.c */
unsafe fn check_log_duration(_msec_str: *mut c_char, _was_logged: bool) -> c_int {
    unimplemented!("STUB: tcop/postgres.c check_log_duration not ported")
}

/* libpq/pqformat.c + libpq/pqcomm.c */
unsafe fn pq_beginmessage(_buf: *mut StringInfoData, _msgtype: u8) {
    unimplemented!("STUB: libpq/pqformat.c not ported")
}
unsafe fn pq_sendint32(_buf: *mut StringInfoData, _i: int32) {
    unimplemented!("STUB: libpq/pqformat.c not ported")
}
unsafe fn pq_sendcountedtext(_buf: *mut StringInfoData, _str: *const c_char, _slen: c_int) {
    unimplemented!("STUB: libpq/pqformat.c not ported")
}
unsafe fn pq_sendbytes(_buf: *mut StringInfoData, _data: *const c_char, _datalen: c_int) {
    unimplemented!("STUB: libpq/pqformat.c not ported")
}
unsafe fn pq_endmessage(_buf: *mut StringInfoData) {
    unimplemented!("STUB: libpq/pqformat.c not ported")
}
unsafe fn pq_getmsgint(_msg: StringInfo, _b: c_int) -> c_uint {
    unimplemented!("STUB: libpq/pqformat.c not ported")
}
unsafe fn pq_getmsgbytes(_msg: StringInfo, _datalen: c_int) -> *const c_char {
    unimplemented!("STUB: libpq/pqformat.c not ported")
}
unsafe fn pq_getmsgend(_msg: StringInfo) {
    unimplemented!("STUB: libpq/pqformat.c not ported")
}

/* lib/stringinfo.c */
unsafe fn initStringInfo(_str: *mut StringInfoData) {
    unimplemented!("STUB: lib/stringinfo.c not ported")
}
unsafe fn resetStringInfo(_str: *mut StringInfoData) {
    unimplemented!("STUB: lib/stringinfo.c not ported")
}
unsafe fn appendBinaryStringInfo(_str: *mut StringInfoData, _data: *const c_char, _datalen: c_int) {
    unimplemented!("STUB: lib/stringinfo.c not ported")
}

/*
 * proargtypes is an oidvector that lives beyond the CATALOG_VARLEN cutoff of
 * FormData_pg_proc, so it is not a fixed field of the ported struct.  This
 * helper stands in for `pp->proargtypes.values`, which fetch_fp_info copies
 * from.  (oidvector layout: ArrayType header followed by Oid values[].)
 */
unsafe fn pg_proc_proargtypes_values(_pp: Form_pg_proc) -> *const Oid {
    unimplemented!("STUB: pg_proc.proargtypes (CATALOG_VARLEN) accessor not ported")
}

/* ----------------
 *		SendFunctionResult
 * ----------------
 */
unsafe fn SendFunctionResult(retval: Datum, isnull: bool, rettype: Oid, format: int16) {
    let mut buf: StringInfoData = std::mem::zeroed();

    pq_beginmessage(&mut buf, PqMsg_FunctionCallResponse);

    if isnull {
        pq_sendint32(&mut buf, -1);
    } else {
        if format == 0 {
            let mut typoutput: Oid = 0;
            let mut typisvarlena: bool = false;

            getTypeOutputInfo(rettype, &mut typoutput, &mut typisvarlena);
            let outputstr = OidOutputFunctionCall(typoutput, retval);
            pq_sendcountedtext(&mut buf, outputstr, libc_strlen(outputstr) as c_int);
            pfree(outputstr as *mut c_void);
        } else if format == 1 {
            let mut typsend: Oid = 0;
            let mut typisvarlena: bool = false;

            getTypeBinaryOutputInfo(rettype, &mut typsend, &mut typisvarlena);
            let outputbytes = OidSendFunctionCall(typsend, retval);
            pq_sendint32(&mut buf, VARSIZE(outputbytes as *const c_char) as int32 - VARHDRSZ);
            pq_sendbytes(
                &mut buf,
                VARDATA(outputbytes as *const c_char),
                VARSIZE(outputbytes as *const c_char) as int32 - VARHDRSZ,
            );
            pfree(outputbytes as *mut c_void);
        } else {
            ereport!(ERROR, "unsupported format code");
            elog!(ERROR, "unsupported format code: {}", format);
        }
    }

    pq_endmessage(&mut buf);
}

/* strlen for a C string. */
unsafe fn libc_strlen(s: *const c_char) -> usize {
    let mut n: usize = 0;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/*
 * fetch_fp_info
 *
 * Performs catalog lookups to load a struct fp_info 'fip' for the
 * function 'func_id'.
 */
unsafe fn fetch_fp_info(func_id: Oid, fip: *mut fp_info) {
    Assert!(!fip.is_null());

    /*
     * Since the validity of this structure is determined by whether the
     * funcid is OK, we clear the funcid here.  It must not be set to the
     * correct value until we are about to return with a good struct fp_info,
     * since we can be interrupted (i.e., with an ereport(ERROR, ...)) at any
     * time.  [No longer really an issue since we don't save the struct
     * fp_info across transactions anymore, but keep it anyway.]
     */
    std::ptr::write_bytes(fip as *mut u8, 0, std::mem::size_of::<fp_info>());
    (*fip).funcid = InvalidOid;

    let func_htp: HeapTuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func_id));
    if !HeapTupleIsValid(func_htp) {
        ereport!(ERROR, "function with OID does not exist");
        elog!(ERROR, "function with OID {} does not exist", func_id);
    }
    let pp: Form_pg_proc = GETSTRUCT(func_htp) as Form_pg_proc;

    /* reject pg_proc entries that are unsafe to call via fastpath */
    if (*pp).prokind != PROKIND_FUNCTION || (*pp).proretset {
        ereport!(ERROR, "cannot call function via fastpath interface");
        elog!(
            ERROR,
            "cannot call function \"{:?}\" via fastpath interface",
            NameStr(&(*pp).proname)
        );
    }

    /* watch out for catalog entries with more than FUNC_MAX_ARGS args */
    if (*pp).pronargs as usize > FUNC_MAX_ARGS {
        elog!(
            ERROR,
            "function {:?} has more than {} arguments",
            NameStr(&(*pp).proname),
            FUNC_MAX_ARGS
        );
    }

    (*fip).namespace = (*pp).pronamespace;
    (*fip).rettype = (*pp).prorettype;
    std::ptr::copy_nonoverlapping(
        pg_proc_proargtypes_values(pp),
        (*fip).argtypes.as_mut_ptr(),
        (*pp).pronargs as usize,
    );
    strlcpy(
        (*fip).fname.as_mut_ptr(),
        NameStr(&(*pp).proname),
        NAMEDATALEN as Size,
    );

    ReleaseSysCache(func_htp);

    fmgr_info(func_id, &mut (*fip).flinfo);

    /*
     * This must be last!
     */
    (*fip).funcid = func_id;
}

/*
 * HandleFunctionRequest
 *
 * Server side of PQfn (fastpath function calls from the frontend).
 * This corresponds to the libpq protocol symbol "F".
 *
 * INPUT:
 *		postgres.c has already read the message body and will pass it in
 *		msgBuf.
 *
 * Note: palloc()s done here and in the called function do not need to be
 * cleaned up explicitly.  We are called from PostgresMain() in the
 * MessageContext memory context, which will be automatically reset when
 * control returns to PostgresMain.
 */
pub unsafe fn HandleFunctionRequest(msgBuf: StringInfo) {
    LOCAL_FCINFO!(fcinfo, FUNC_MAX_ARGS);
    let fid: Oid;
    let mut aclresult: AclResult;
    let rformat: int16;
    let retval: Datum;
    let mut my_fp: fp_info = std::mem::zeroed();
    let fip: *mut fp_info;
    let callit: bool;
    let mut was_logged = false;
    let mut msec_str: [c_char; 32] = [0; 32];

    /*
     * We only accept COMMIT/ABORT if we are in an aborted transaction, and
     * COMMIT/ABORT cannot be executed through the fastpath interface.
     */
    if IsAbortedTransactionBlockState() {
        ereport!(
            ERROR,
            "current transaction is aborted, commands ignored until end of transaction block"
        );
    }

    /*
     * Now that we know we are in a valid transaction, set snapshot in case
     * needed by function itself or one of the datatype I/O routines.
     */
    PushActiveSnapshot(GetTransactionSnapshot());

    /*
     * Begin parsing the buffer contents.
     */
    fid = pq_getmsgint(msgBuf, 4) as Oid; /* function oid */

    /*
     * There used to be a lame attempt at caching lookup info here. Now we
     * just do the lookups on every call.
     */
    fip = &mut my_fp;
    fetch_fp_info(fid, fip);

    /* Log as soon as we have the function OID and name */
    if log_statement == LOGSTMT_ALL {
        ereport!(LOG, "fastpath function call");
        elog!(
            LOG,
            "fastpath function call: \"{:?}\" (OID {})",
            (*fip).fname.as_ptr(),
            fid
        );
        was_logged = true;
    }

    /*
     * Check permission to access and call function.  Since we didn't go
     * through a normal name lookup, we need to check schema usage too.
     */
    aclresult = object_aclcheck(NamespaceRelationId, (*fip).namespace, GetUserId(), ACL_USAGE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_SCHEMA, get_namespace_name((*fip).namespace));
    }
    RunNamespaceSearchHook((*fip).namespace, true);

    aclresult = object_aclcheck(ProcedureRelationId, fid, GetUserId(), ACL_EXECUTE);
    if aclresult != ACLCHECK_OK {
        aclcheck_error(aclresult, OBJECT_FUNCTION, get_func_name(fid));
    }
    RunFunctionExecuteHook(fid);

    /*
     * Prepare function call info block and insert arguments.
     *
     * Note: for now we pass collation = InvalidOid, so collation-sensitive
     * functions can't be called this way.  Perhaps we should pass
     * DEFAULT_COLLATION_OID, instead?
     */
    InitFunctionCallInfoData!(fcinfo, &mut (*fip).flinfo, 0, InvalidOid, null_mut(), null_mut());

    rformat = parse_fcall_arguments(msgBuf, fip, fcinfo);

    /* Verify we reached the end of the message where expected. */
    pq_getmsgend(msgBuf);

    /*
     * If func is strict, must not call it for null args.
     */
    callit = {
        let mut c = true;
        if (*fip).flinfo.fn_strict {
            for i in 0..(*fcinfo).nargs as usize {
                if (*fcinfo).args.as_ptr().add(i).read().isnull {
                    c = false;
                    break;
                }
            }
        }
        c
    };

    if callit {
        /* Okay, do it ... */
        retval = FunctionCallInvoke!(fcinfo);
    } else {
        (*fcinfo).isnull = true;
        retval = 0 as Datum;
    }

    /* ensure we do at least one CHECK_FOR_INTERRUPTS per function call */
    CHECK_FOR_INTERRUPTS();

    SendFunctionResult(retval, (*fcinfo).isnull, (*fip).rettype, rformat);

    /* We no longer need the snapshot */
    PopActiveSnapshot();

    /*
     * Emit duration logging if appropriate.
     */
    match check_log_duration(msec_str.as_mut_ptr(), was_logged) {
        1 => {
            ereport!(LOG, "duration ms");
            elog!(LOG, "duration: {:?} ms", msec_str.as_ptr());
        }
        2 => {
            ereport!(LOG, "duration ms fastpath function call");
            elog!(
                LOG,
                "duration: {:?} ms  fastpath function call: \"{:?}\" (OID {})",
                msec_str.as_ptr(),
                (*fip).fname.as_ptr(),
                fid
            );
        }
        _ => {}
    }
}

/*
 * Parse function arguments in a 3.0 protocol message
 *
 * Argument values are loaded into *fcinfo, and the desired result format
 * is returned.
 */
unsafe fn parse_fcall_arguments(
    msgBuf: StringInfo,
    fip: *mut fp_info,
    fcinfo: FunctionCallInfo,
) -> int16 {
    let nargs: c_int;
    let mut i: c_int;
    let numAFormats: c_int;
    let mut aformats: *mut int16 = null_mut();
    let mut abuf: StringInfoData = std::mem::zeroed();

    /* Get the argument format codes */
    numAFormats = pq_getmsgint(msgBuf, 2) as c_int;
    if numAFormats > 0 {
        aformats = palloc(numAFormats as usize * std::mem::size_of::<int16>()) as *mut int16;
        i = 0;
        while i < numAFormats {
            *aformats.add(i as usize) = pq_getmsgint(msgBuf, 2) as int16;
            i += 1;
        }
    }

    nargs = pq_getmsgint(msgBuf, 2) as c_int; /* # of arguments */

    if (*fip).flinfo.fn_nargs as c_int != nargs || nargs as usize > FUNC_MAX_ARGS {
        ereport!(ERROR, "function call message contains wrong number of arguments");
        elog!(
            ERROR,
            "function call message contains {} arguments but function requires {}",
            nargs,
            (*fip).flinfo.fn_nargs
        );
    }

    (*fcinfo).nargs = nargs as c_short;

    if numAFormats > 1 && numAFormats != nargs {
        ereport!(ERROR, "function call message contains wrong number of argument formats");
        elog!(
            ERROR,
            "function call message contains {} argument formats but {} arguments",
            numAFormats,
            nargs
        );
    }

    initStringInfo(&mut abuf);

    /*
     * Copy supplied arguments into arg vector.
     */
    i = 0;
    while i < nargs {
        let argsize: c_int;
        let aformat: int16;

        argsize = pq_getmsgint(msgBuf, 4) as c_int;
        if argsize == -1 {
            (*(*fcinfo).args.as_mut_ptr().add(i as usize)).isnull = true;
        } else {
            (*(*fcinfo).args.as_mut_ptr().add(i as usize)).isnull = false;
            if argsize < 0 {
                ereport!(ERROR, "invalid argument size in function call message");
                elog!(
                    ERROR,
                    "invalid argument size {} in function call message",
                    argsize
                );
            }

            /* Reset abuf to empty, and insert raw data into it */
            resetStringInfo(&mut abuf);
            appendBinaryStringInfo(&mut abuf, pq_getmsgbytes(msgBuf, argsize), argsize);
        }

        if numAFormats > 1 {
            aformat = *aformats.add(i as usize);
        } else if numAFormats > 0 {
            aformat = *aformats;
        } else {
            aformat = 0; /* default = text */
        }

        if aformat == 0 {
            let mut typinput: Oid = 0;
            let mut typioparam: Oid = 0;
            let pstring: *mut c_char;

            getTypeInputInfo((*fip).argtypes[i as usize], &mut typinput, &mut typioparam);

            /*
             * Since stringinfo.c keeps a trailing null in place even for
             * binary data, the contents of abuf are a valid C string.  We
             * have to do encoding conversion before calling the typinput
             * routine, though.
             */
            if argsize == -1 {
                pstring = null_mut();
            } else {
                pstring = pg_client_to_server(abuf.data, argsize);
            }

            (*(*fcinfo).args.as_mut_ptr().add(i as usize)).value =
                OidInputFunctionCall(typinput, pstring, typioparam, -1);
            /* Free result of encoding conversion, if any */
            if !pstring.is_null() && pstring != abuf.data {
                pfree(pstring as *mut c_void);
            }
        } else if aformat == 1 {
            let mut typreceive: Oid = 0;
            let mut typioparam: Oid = 0;
            let bufptr: StringInfo;

            /* Call the argument type's binary input converter */
            getTypeBinaryInputInfo((*fip).argtypes[i as usize], &mut typreceive, &mut typioparam);

            if argsize == -1 {
                bufptr = null_mut();
            } else {
                bufptr = &mut abuf;
            }

            (*(*fcinfo).args.as_mut_ptr().add(i as usize)).value =
                OidReceiveFunctionCall(typreceive, bufptr, typioparam, -1);

            /* Trouble if it didn't eat the whole buffer */
            if argsize != -1 && abuf.cursor != abuf.len {
                ereport!(ERROR, "incorrect binary data format in function argument");
                elog!(
                    ERROR,
                    "incorrect binary data format in function argument {}",
                    i + 1
                );
            }
        } else {
            ereport!(ERROR, "unsupported format code");
            elog!(ERROR, "unsupported format code: {}", aformat);
        }

        i += 1;
    }

    /* Return result format code */
    pq_getmsgint(msgBuf, 2) as int16
}
