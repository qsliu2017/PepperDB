//! src/backend/utils/adt/hbafuncs.c
//!
//! hbafuncs.c
//!	  Support functions for SQL views of authentication files.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!	  src/backend/utils/adt/hbafuncs.c

use crate::prelude::*;


// PG_RETURN_* are #[macro_export] macros living at the crate root.
use crate::PG_RETURN_NULL;

// foreach! declares its own cursor; current_cell!/lfirst extract the datum.
use crate::{current_cell, foreach};

use std::ffi::{c_char, c_int, c_void};

// ---------------------------------------------------------------------------
// Local stubs / external type placeholders
// ---------------------------------------------------------------------------

type ArrayType = c_void;
type Tuplestorestate = c_void;
type TupleDesc = *mut c_void;
type HeapTuple = *mut c_void;
use crate::utils::fmgr::FunctionCallInfo;
type ReturnSetInfo = c_void;
type List = crate::nodes::pg_list::List;
type ListCell = crate::nodes::pg_list::ListCell;

use crate::nodes::pg_list::{lappend, lfirst, NIL};

// FILE is an opaque C type.
type FILE = c_void;

// NI_MAXHOST from netdb.h
const NI_MAXHOST: usize = 1025;
// NI_NUMERICHOST from netdb.h
const NI_NUMERICHOST: c_int = 1;

// TEXTOID from pg_type_d.h
const TEXTOID: Oid = 25;

// ---------------------------------------------------------------------------
// libpq/hba.h types and enums
// ---------------------------------------------------------------------------

// UserAuth (libpq/hba.h)
type UserAuth = c_int;
const uaReject: UserAuth = 0;
const uaImplicitReject: UserAuth = 1;
const uaTrust: UserAuth = 2;
const uaIdent: UserAuth = 3;
const uaPassword: UserAuth = 4;
const uaMD5: UserAuth = 5;
const uaSCRAM: UserAuth = 6;
const uaGSS: UserAuth = 7;
const uaSSPI: UserAuth = 8;
const uaPAM: UserAuth = 9;
const uaBSD: UserAuth = 10;
const uaLDAP: UserAuth = 11;
const uaCert: UserAuth = 12;
const uaRADIUS: UserAuth = 13;
const uaPeer: UserAuth = 14;
const uaOAuth: UserAuth = 15;

// ConnType (libpq/hba.h)
type ConnType = c_int;
const ctLocal: ConnType = 0;
const ctHost: ConnType = 1;
const ctHostSSL: ConnType = 2;
const ctHostNoSSL: ConnType = 3;
const ctHostGSS: ConnType = 4;
const ctHostNoGSS: ConnType = 5;

// IPCompareMethod (libpq/hba.h)
type IPCompareMethod = c_int;
const ipCmpMask: IPCompareMethod = 0;
const ipCmpSameHost: IPCompareMethod = 1;
const ipCmpSameNet: IPCompareMethod = 2;
const ipCmpAll: IPCompareMethod = 3;

// ClientCertMode (libpq/hba.h)
type ClientCertMode = c_int;
const clientCertOff: ClientCertMode = 0;
const clientCertCA: ClientCertMode = 1;
const clientCertFull: ClientCertMode = 2;

// HbaLine (libpq/hba.h) - opaque mirror, accessed via field helper stubs.
#[repr(C)]
struct HbaLine {
    _opaque: [u8; 0],
}

// IdentLine (libpq/hba.h)
#[repr(C)]
struct IdentLine {
    _opaque: [u8; 0],
}

// AuthToken (libpq/hba.h)
#[repr(C)]
struct AuthToken {
    _opaque: [u8; 0],
}

// TokenizedAuthLine (libpq/hba.h)
#[repr(C)]
struct TokenizedAuthLine {
    _opaque: [u8; 0],
}

// sockaddr_storage (sys/socket.h)
#[repr(C)]
struct sockaddr_storage {
    _opaque: [u8; 0],
}

// ---------------------------------------------------------------------------
// HbaLine field accessor stubs (libpq/hba.h)
// ---------------------------------------------------------------------------

unsafe fn hba_auth_method(_hba: *mut HbaLine) -> UserAuth {
    unimplemented!() // TODO: libpq/hba.h HbaLine.auth_method
}
unsafe fn hba_include_realm(_hba: *mut HbaLine) -> bool {
    unimplemented!() // TODO: libpq/hba.h HbaLine.include_realm
}
unsafe fn hba_krb_realm(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.krb_realm
}
unsafe fn hba_usermap(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.usermap
}
unsafe fn hba_clientcert(_hba: *mut HbaLine) -> ClientCertMode {
    unimplemented!() // TODO: libpq/hba.h HbaLine.clientcert
}
unsafe fn hba_pamservice(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.pamservice
}
unsafe fn hba_ldapserver(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.ldapserver
}
unsafe fn hba_ldapport(_hba: *mut HbaLine) -> c_int {
    unimplemented!() // TODO: libpq/hba.h HbaLine.ldapport
}
unsafe fn hba_ldapscheme(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.ldapscheme
}
unsafe fn hba_ldaptls(_hba: *mut HbaLine) -> bool {
    unimplemented!() // TODO: libpq/hba.h HbaLine.ldaptls
}
unsafe fn hba_ldapprefix(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.ldapprefix
}
unsafe fn hba_ldapsuffix(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.ldapsuffix
}
unsafe fn hba_ldapbasedn(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.ldapbasedn
}
unsafe fn hba_ldapbinddn(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.ldapbinddn
}
unsafe fn hba_ldapbindpasswd(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.ldapbindpasswd
}
unsafe fn hba_ldapsearchattribute(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.ldapsearchattribute
}
unsafe fn hba_ldapsearchfilter(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.ldapsearchfilter
}
unsafe fn hba_ldapscope(_hba: *mut HbaLine) -> c_int {
    unimplemented!() // TODO: libpq/hba.h HbaLine.ldapscope
}
unsafe fn hba_radiusservers_s(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.radiusservers_s
}
unsafe fn hba_radiussecrets_s(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.radiussecrets_s
}
unsafe fn hba_radiusidentifiers_s(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.radiusidentifiers_s
}
unsafe fn hba_radiusports_s(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.radiusports_s
}
unsafe fn hba_oauth_issuer(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.oauth_issuer
}
unsafe fn hba_oauth_scope(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.oauth_scope
}
unsafe fn hba_oauth_validator(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.oauth_validator
}
unsafe fn hba_oauth_skip_usermap(_hba: *mut HbaLine) -> bool {
    unimplemented!() // TODO: libpq/hba.h HbaLine.oauth_skip_usermap
}
unsafe fn hba_conntype(_hba: *mut HbaLine) -> ConnType {
    unimplemented!() // TODO: libpq/hba.h HbaLine.conntype
}
unsafe fn hba_databases(_hba: *mut HbaLine) -> *mut List {
    unimplemented!() // TODO: libpq/hba.h HbaLine.databases
}
unsafe fn hba_roles(_hba: *mut HbaLine) -> *mut List {
    unimplemented!() // TODO: libpq/hba.h HbaLine.roles
}
unsafe fn hba_ip_cmp_method(_hba: *mut HbaLine) -> IPCompareMethod {
    unimplemented!() // TODO: libpq/hba.h HbaLine.ip_cmp_method
}
unsafe fn hba_hostname(_hba: *mut HbaLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h HbaLine.hostname
}
unsafe fn hba_addr(_hba: *mut HbaLine) -> *mut sockaddr_storage {
    unimplemented!() // TODO: libpq/hba.h HbaLine.addr
}
unsafe fn hba_addrlen(_hba: *mut HbaLine) -> c_int {
    unimplemented!() // TODO: libpq/hba.h HbaLine.addrlen
}
unsafe fn hba_mask(_hba: *mut HbaLine) -> *mut sockaddr_storage {
    unimplemented!() // TODO: libpq/hba.h HbaLine.mask
}
unsafe fn hba_masklen(_hba: *mut HbaLine) -> c_int {
    unimplemented!() // TODO: libpq/hba.h HbaLine.masklen
}
unsafe fn ss_family(_ss: *mut sockaddr_storage) -> c_int {
    unimplemented!() // TODO: sockaddr_storage.ss_family
}

// AuthToken field accessor (libpq/hba.h)
unsafe fn authtoken_string(_tok: *mut AuthToken) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h AuthToken.string
}

// IdentLine field accessors (libpq/hba.h)
unsafe fn ident_usermap(_ident: *mut IdentLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h IdentLine.usermap
}
unsafe fn ident_system_user_string(_ident: *mut IdentLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h IdentLine.system_user->string
}
unsafe fn ident_pg_user_string(_ident: *mut IdentLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h IdentLine.pg_user->string
}

// TokenizedAuthLine field accessors (libpq/hba.h)
unsafe fn tok_err_msg(_tok_line: *mut TokenizedAuthLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h TokenizedAuthLine.err_msg
}
unsafe fn tok_file_name(_tok_line: *mut TokenizedAuthLine) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.h TokenizedAuthLine.file_name
}
unsafe fn tok_line_num(_tok_line: *mut TokenizedAuthLine) -> c_int {
    unimplemented!() // TODO: libpq/hba.h TokenizedAuthLine.line_num
}

// ---------------------------------------------------------------------------
// External function stubs
// ---------------------------------------------------------------------------

unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO: utils/builtins.h
}
unsafe fn psprintf_s(_fmt: *const c_char, _arg: *const c_char) -> *mut c_char {
    unimplemented!() // TODO: utils/palloc.h psprintf
}
unsafe fn psprintf_d(_fmt: *const c_char, _arg: c_int) -> *mut c_char {
    unimplemented!() // TODO: utils/palloc.h psprintf
}
unsafe fn construct_array_builtin(
    _elems: *mut Datum,
    _nelems: c_int,
    _elmtype: Oid,
) -> *mut ArrayType {
    unimplemented!() // TODO: utils/array.c
}
unsafe fn strlist_to_textarray(_list: *mut List) -> *mut ArrayType {
    unimplemented!() // TODO: utils/adt/varlena.c
}
unsafe fn pg_getnameinfo_all(
    _addr: *const sockaddr_storage,
    _salen: c_int,
    _node: *mut c_char,
    _nodelen: c_int,
    _service: *mut c_char,
    _servicelen: c_int,
    _flags: c_int,
) -> c_int {
    unimplemented!() // TODO: common/ip.c
}
unsafe fn clean_ipv6_addr(_addr_family: c_int, _addr: *mut c_char) {
    unimplemented!() // TODO: utils/adt/network.c
}
unsafe fn hba_authname(_auth_method: UserAuth) -> *mut c_char {
    unimplemented!() // TODO: libpq/hba.c
}
unsafe fn heap_form_tuple(
    _tupdesc: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) -> HeapTuple {
    unimplemented!() // TODO: access/common/heaptuple.c
}
unsafe fn tuplestore_puttuple(_state: *mut Tuplestorestate, _tuple: HeapTuple) {
    unimplemented!() // TODO: utils/sort/tuplestore.c
}
unsafe fn open_auth_file(
    _filename: *const c_char,
    _elevel: c_int,
    _depth: c_int,
    _tok_line: *mut TokenizedAuthLine,
) -> *mut FILE {
    unimplemented!() // TODO: libpq/hba.c
}
unsafe fn tokenize_auth_file(
    _filename: *const c_char,
    _file: *mut FILE,
    _tok_lines: *mut *mut List,
    _elevel: c_int,
    _depth: c_int,
) {
    unimplemented!() // TODO: libpq/hba.c
}
unsafe fn parse_hba_line(_tok_line: *mut TokenizedAuthLine, _elevel: c_int) -> *mut HbaLine {
    unimplemented!() // TODO: libpq/hba.c
}
unsafe fn parse_ident_line(_tok_line: *mut TokenizedAuthLine, _elevel: c_int) -> *mut IdentLine {
    unimplemented!() // TODO: libpq/hba.c
}
unsafe fn free_auth_file(_file: *mut FILE, _depth: c_int) {
    unimplemented!() // TODO: libpq/hba.c
}
unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: c_int) {
    unimplemented!() // TODO: utils/fmgr/funcapi.c
}
unsafe fn fcinfo_resultinfo(_fcinfo: FunctionCallInfo) -> *mut ReturnSetInfo {
    unimplemented!() // TODO: fmgr.h FunctionCallInfo.resultinfo
}
unsafe fn rsinfo_setResult(_rsinfo: *mut ReturnSetInfo) -> *mut Tuplestorestate {
    unimplemented!() // TODO: nodes/execnodes.h ReturnSetInfo.setResult
}
unsafe fn rsinfo_setDesc(_rsinfo: *mut ReturnSetInfo) -> TupleDesc {
    unimplemented!() // TODO: nodes/execnodes.h ReturnSetInfo.setDesc
}
unsafe fn tupdesc_natts(_tupdesc: TupleDesc) -> c_int {
    unimplemented!() // TODO: access/tupdesc.h TupleDesc.natts
}

// Int32GetDatum / PointerGetDatum from postgres.h (faithful inline).
unsafe fn Int32GetDatum(x: i32) -> Datum {
    x as u32 as Datum
}
unsafe fn PointerGetDatum(p: *mut c_void) -> Datum {
    p as usize as Datum
}

// GUC variables (libpq/auth.h / guc).
static mut HbaFileName: *mut c_char = null_mut();
static mut IdentFileName: *mut c_char = null_mut();

// ---------------------------------------------------------------------------

/*
 * This macro specifies the maximum number of authentication options
 * that are possible with any given authentication method that is supported.
 * Currently LDAP supports 12, and there are 3 that are not dependent on
 * the auth method here.  It may not actually be possible to set all of them
 * at the same time, but we'll set the macro value high enough to be
 * conservative and avoid warnings from static analysis tools.
 */
const MAX_HBA_OPTIONS: usize = 15;

/*
 * Create a text array listing the options specified in the HBA line.
 * Return NULL if no options are specified.
 */
unsafe fn get_hba_options(hba: *mut HbaLine) -> *mut ArrayType {
    let mut noptions: c_int;
    let mut options: [Datum; MAX_HBA_OPTIONS] = [0; MAX_HBA_OPTIONS];

    noptions = 0;

    if hba_auth_method(hba) == uaGSS || hba_auth_method(hba) == uaSSPI {
        if hba_include_realm(hba) {
            options[noptions as usize] = CStringGetTextDatum(c"include_realm=true".as_ptr());
            noptions += 1;
        }

        if !hba_krb_realm(hba).is_null() {
            options[noptions as usize] =
                CStringGetTextDatum(psprintf_s(c"krb_realm=%s".as_ptr(), hba_krb_realm(hba)));
            noptions += 1;
        }
    }

    if !hba_usermap(hba).is_null() {
        options[noptions as usize] =
            CStringGetTextDatum(psprintf_s(c"map=%s".as_ptr(), hba_usermap(hba)));
        noptions += 1;
    }

    if hba_clientcert(hba) != clientCertOff {
        let mode = if hba_clientcert(hba) == clientCertCA {
            c"verify-ca".as_ptr()
        } else {
            c"verify-full".as_ptr()
        };
        options[noptions as usize] =
            CStringGetTextDatum(psprintf_s(c"clientcert=%s".as_ptr(), mode));
        noptions += 1;
    }

    if !hba_pamservice(hba).is_null() {
        options[noptions as usize] =
            CStringGetTextDatum(psprintf_s(c"pamservice=%s".as_ptr(), hba_pamservice(hba)));
        noptions += 1;
    }

    if hba_auth_method(hba) == uaLDAP {
        if !hba_ldapserver(hba).is_null() {
            options[noptions as usize] =
                CStringGetTextDatum(psprintf_s(c"ldapserver=%s".as_ptr(), hba_ldapserver(hba)));
            noptions += 1;
        }

        if hba_ldapport(hba) != 0 {
            options[noptions as usize] =
                CStringGetTextDatum(psprintf_d(c"ldapport=%d".as_ptr(), hba_ldapport(hba)));
            noptions += 1;
        }

        if !hba_ldapscheme(hba).is_null() {
            options[noptions as usize] =
                CStringGetTextDatum(psprintf_s(c"ldapscheme=%s".as_ptr(), hba_ldapscheme(hba)));
            noptions += 1;
        }

        if hba_ldaptls(hba) {
            options[noptions as usize] = CStringGetTextDatum(c"ldaptls=true".as_ptr());
            noptions += 1;
        }

        if !hba_ldapprefix(hba).is_null() {
            options[noptions as usize] =
                CStringGetTextDatum(psprintf_s(c"ldapprefix=%s".as_ptr(), hba_ldapprefix(hba)));
            noptions += 1;
        }

        if !hba_ldapsuffix(hba).is_null() {
            options[noptions as usize] =
                CStringGetTextDatum(psprintf_s(c"ldapsuffix=%s".as_ptr(), hba_ldapsuffix(hba)));
            noptions += 1;
        }

        if !hba_ldapbasedn(hba).is_null() {
            options[noptions as usize] =
                CStringGetTextDatum(psprintf_s(c"ldapbasedn=%s".as_ptr(), hba_ldapbasedn(hba)));
            noptions += 1;
        }

        if !hba_ldapbinddn(hba).is_null() {
            options[noptions as usize] =
                CStringGetTextDatum(psprintf_s(c"ldapbinddn=%s".as_ptr(), hba_ldapbinddn(hba)));
            noptions += 1;
        }

        if !hba_ldapbindpasswd(hba).is_null() {
            options[noptions as usize] = CStringGetTextDatum(psprintf_s(
                c"ldapbindpasswd=%s".as_ptr(),
                hba_ldapbindpasswd(hba),
            ));
            noptions += 1;
        }

        if !hba_ldapsearchattribute(hba).is_null() {
            options[noptions as usize] = CStringGetTextDatum(psprintf_s(
                c"ldapsearchattribute=%s".as_ptr(),
                hba_ldapsearchattribute(hba),
            ));
            noptions += 1;
        }

        if !hba_ldapsearchfilter(hba).is_null() {
            options[noptions as usize] = CStringGetTextDatum(psprintf_s(
                c"ldapsearchfilter=%s".as_ptr(),
                hba_ldapsearchfilter(hba),
            ));
            noptions += 1;
        }

        if hba_ldapscope(hba) != 0 {
            options[noptions as usize] =
                CStringGetTextDatum(psprintf_d(c"ldapscope=%d".as_ptr(), hba_ldapscope(hba)));
            noptions += 1;
        }
    }

    if hba_auth_method(hba) == uaRADIUS {
        if !hba_radiusservers_s(hba).is_null() {
            options[noptions as usize] = CStringGetTextDatum(psprintf_s(
                c"radiusservers=%s".as_ptr(),
                hba_radiusservers_s(hba),
            ));
            noptions += 1;
        }

        if !hba_radiussecrets_s(hba).is_null() {
            options[noptions as usize] = CStringGetTextDatum(psprintf_s(
                c"radiussecrets=%s".as_ptr(),
                hba_radiussecrets_s(hba),
            ));
            noptions += 1;
        }

        if !hba_radiusidentifiers_s(hba).is_null() {
            options[noptions as usize] = CStringGetTextDatum(psprintf_s(
                c"radiusidentifiers=%s".as_ptr(),
                hba_radiusidentifiers_s(hba),
            ));
            noptions += 1;
        }

        if !hba_radiusports_s(hba).is_null() {
            options[noptions as usize] = CStringGetTextDatum(psprintf_s(
                c"radiusports=%s".as_ptr(),
                hba_radiusports_s(hba),
            ));
            noptions += 1;
        }
    }

    if hba_auth_method(hba) == uaOAuth {
        if !hba_oauth_issuer(hba).is_null() {
            options[noptions as usize] =
                CStringGetTextDatum(psprintf_s(c"issuer=%s".as_ptr(), hba_oauth_issuer(hba)));
            noptions += 1;
        }

        if !hba_oauth_scope(hba).is_null() {
            options[noptions as usize] =
                CStringGetTextDatum(psprintf_s(c"scope=%s".as_ptr(), hba_oauth_scope(hba)));
            noptions += 1;
        }

        if !hba_oauth_validator(hba).is_null() {
            options[noptions as usize] = CStringGetTextDatum(psprintf_s(
                c"validator=%s".as_ptr(),
                hba_oauth_validator(hba),
            ));
            noptions += 1;
        }

        if hba_oauth_skip_usermap(hba) {
            options[noptions as usize] =
                CStringGetTextDatum(c"delegate_ident_mapping=true".as_ptr());
            noptions += 1;
        }
    }

    /* If you add more options, consider increasing MAX_HBA_OPTIONS. */
    Assert!(noptions as usize <= MAX_HBA_OPTIONS);

    if noptions > 0 {
        construct_array_builtin(options.as_mut_ptr(), noptions, TEXTOID)
    } else {
        null_mut()
    }
}

/* Number of columns in pg_hba_file_rules view */
const NUM_PG_HBA_FILE_RULES_ATTS: usize = 11;

/*
 * fill_hba_line
 *		Build one row of pg_hba_file_rules view, add it to tuplestore.
 *
 * tuple_store: where to store data
 * tupdesc: tuple descriptor for the view
 * rule_number: unique identifier among all valid rules
 * filename: configuration file name (must always be valid)
 * lineno: line number of configuration file (must always be valid)
 * hba: parsed line data (can be NULL, in which case err_msg should be set)
 * err_msg: error message (NULL if none)
 *
 * Note: leaks memory, but we don't care since this is run in a short-lived
 * memory context.
 */
unsafe fn fill_hba_line(
    tuple_store: *mut Tuplestorestate,
    tupdesc: TupleDesc,
    rule_number: c_int,
    filename: *mut c_char,
    lineno: c_int,
    hba: *mut HbaLine,
    err_msg: *const c_char,
) {
    let mut values: [Datum; NUM_PG_HBA_FILE_RULES_ATTS] = [0; NUM_PG_HBA_FILE_RULES_ATTS];
    let mut nulls: [bool; NUM_PG_HBA_FILE_RULES_ATTS] = [false; NUM_PG_HBA_FILE_RULES_ATTS];
    let mut buffer: [c_char; NI_MAXHOST] = [0; NI_MAXHOST];
    let tuple: HeapTuple;
    let mut index: usize;
    let typestr: *const c_char;
    let mut addrstr: *const c_char;
    let mut maskstr: *const c_char;
    let options: *mut ArrayType;

    Assert!(tupdesc_natts(tupdesc) as usize == NUM_PG_HBA_FILE_RULES_ATTS);

    // memset(values, 0, ...); memset(nulls, 0, ...); already zero-initialized.
    index = 0;

    /* rule_number, nothing on error */
    if !err_msg.is_null() {
        nulls[index] = true;
        index += 1;
    } else {
        values[index] = Int32GetDatum(rule_number);
        index += 1;
    }

    /* file_name */
    values[index] = CStringGetTextDatum(filename);
    index += 1;

    /* line_number */
    values[index] = Int32GetDatum(lineno);
    index += 1;

    if !hba.is_null() {
        /* type */
        /* Avoid a default: case so compiler will warn about missing cases */
        typestr = match hba_conntype(hba) {
            ctLocal => c"local".as_ptr(),
            ctHost => c"host".as_ptr(),
            ctHostSSL => c"hostssl".as_ptr(),
            ctHostNoSSL => c"hostnossl".as_ptr(),
            ctHostGSS => c"hostgssenc".as_ptr(),
            ctHostNoGSS => c"hostnogssenc".as_ptr(),
            _ => null(),
        };
        if !typestr.is_null() {
            values[index] = CStringGetTextDatum(typestr);
            index += 1;
        } else {
            nulls[index] = true;
            index += 1;
        }

        /* database */
        if !hba_databases(hba).is_null() {
            /*
             * Flatten AuthToken list to string list.  It might seem that we
             * should re-quote any quoted tokens, but that has been rejected
             * on the grounds that it makes it harder to compare the array
             * elements to other system catalogs.  That makes entries like
             * "all" or "samerole" formally ambiguous ... but users who name
             * databases/roles that way are inflicting their own pain.
             */
            let mut names: *mut List = NIL;

            foreach!(lc, hba_databases(hba), {
                let tok = lfirst(current_cell!(lc)) as *mut AuthToken;

                names = lappend(names, authtoken_string(tok) as *mut c_void);
            });
            values[index] = PointerGetDatum(strlist_to_textarray(names) as *mut c_void);
            index += 1;
        } else {
            nulls[index] = true;
            index += 1;
        }

        /* user */
        if !hba_roles(hba).is_null() {
            /* Flatten AuthToken list to string list; see comment above */
            let mut roles: *mut List = NIL;

            foreach!(lc, hba_roles(hba), {
                let tok = lfirst(current_cell!(lc)) as *mut AuthToken;

                roles = lappend(roles, authtoken_string(tok) as *mut c_void);
            });
            values[index] = PointerGetDatum(strlist_to_textarray(roles) as *mut c_void);
            index += 1;
        } else {
            nulls[index] = true;
            index += 1;
        }

        /* address and netmask */
        /* Avoid a default: case so compiler will warn about missing cases */
        addrstr = null();
        maskstr = null();
        match hba_ip_cmp_method(hba) {
            ipCmpMask => {
                if !hba_hostname(hba).is_null() {
                    addrstr = hba_hostname(hba);
                } else {
                    /*
                     * Note: if pg_getnameinfo_all fails, it'll set buffer to
                     * "???", which we want to return.
                     */
                    if hba_addrlen(hba) > 0 {
                        if pg_getnameinfo_all(
                            hba_addr(hba),
                            hba_addrlen(hba),
                            buffer.as_mut_ptr(),
                            core::mem::size_of_val(&buffer) as c_int,
                            null_mut(),
                            0,
                            NI_NUMERICHOST,
                        ) == 0
                        {
                            clean_ipv6_addr(ss_family(hba_addr(hba)), buffer.as_mut_ptr());
                        }
                        addrstr = pstrdup(buffer.as_ptr());
                    }
                    if hba_masklen(hba) > 0 {
                        if pg_getnameinfo_all(
                            hba_mask(hba),
                            hba_masklen(hba),
                            buffer.as_mut_ptr(),
                            core::mem::size_of_val(&buffer) as c_int,
                            null_mut(),
                            0,
                            NI_NUMERICHOST,
                        ) == 0
                        {
                            clean_ipv6_addr(ss_family(hba_mask(hba)), buffer.as_mut_ptr());
                        }
                        maskstr = pstrdup(buffer.as_ptr());
                    }
                }
            }
            ipCmpAll => {
                addrstr = c"all".as_ptr();
            }
            ipCmpSameHost => {
                addrstr = c"samehost".as_ptr();
            }
            ipCmpSameNet => {
                addrstr = c"samenet".as_ptr();
            }
            _ => {}
        }
        if !addrstr.is_null() {
            values[index] = CStringGetTextDatum(addrstr);
            index += 1;
        } else {
            nulls[index] = true;
            index += 1;
        }
        if !maskstr.is_null() {
            values[index] = CStringGetTextDatum(maskstr);
            index += 1;
        } else {
            nulls[index] = true;
            index += 1;
        }

        /* auth_method */
        values[index] = CStringGetTextDatum(hba_authname(hba_auth_method(hba)));
        index += 1;

        /* options */
        options = get_hba_options(hba);
        if !options.is_null() {
            values[index] = PointerGetDatum(options as *mut c_void);
            index += 1;
        } else {
            nulls[index] = true;
            index += 1;
        }
    } else {
        /* no parsing result, so set relevant fields to nulls */
        for i in 3..(NUM_PG_HBA_FILE_RULES_ATTS - 1) {
            nulls[i] = true;
        }
    }

    /* error */
    if !err_msg.is_null() {
        values[NUM_PG_HBA_FILE_RULES_ATTS - 1] = CStringGetTextDatum(err_msg);
    } else {
        nulls[NUM_PG_HBA_FILE_RULES_ATTS - 1] = true;
    }

    tuple = heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
    tuplestore_puttuple(tuple_store, tuple);
}

/*
 * fill_hba_view
 *		Read the pg_hba.conf file and fill the tuplestore with view records.
 */
unsafe fn fill_hba_view(tuple_store: *mut Tuplestorestate, tupdesc: TupleDesc) {
    let file: *mut FILE;
    let mut hba_lines: *mut List = NIL;
    let mut rule_number: c_int = 0;
    let hbacxt: MemoryContext;
    let oldcxt: MemoryContext;

    /*
     * In the unlikely event that we can't open pg_hba.conf, we throw an
     * error, rather than trying to report it via some sort of view entry.
     * (Most other error conditions should result in a message in a view
     * entry.)
     */
    file = open_auth_file(HbaFileName, ERROR, 0, null_mut());

    tokenize_auth_file(HbaFileName, file, &mut hba_lines, DEBUG3, 0);

    /* Now parse all the lines */
    hbacxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"hba parser context".as_ptr(),
        ALLOCSET_SMALL_SIZES
    );
    oldcxt = MemoryContextSwitchTo(hbacxt);
    foreach!(line, hba_lines, {
        let tok_line = lfirst(current_cell!(line)) as *mut TokenizedAuthLine;
        let mut hbaline: *mut HbaLine = null_mut();

        /* don't parse lines that already have errors */
        if tok_err_msg(tok_line).is_null() {
            hbaline = parse_hba_line(tok_line, DEBUG3);
        }

        /* No error, set a new rule number */
        if tok_err_msg(tok_line).is_null() {
            rule_number += 1;
        }

        fill_hba_line(
            tuple_store,
            tupdesc,
            rule_number,
            tok_file_name(tok_line),
            tok_line_num(tok_line),
            hbaline,
            tok_err_msg(tok_line),
        );
    });

    /* Free tokenizer memory */
    free_auth_file(file, 0);
    /* Free parse_hba_line memory */
    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(hbacxt);
}

/*
 * pg_hba_file_rules
 *
 * SQL-accessible set-returning function to return all the entries in the
 * pg_hba.conf file.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_hba_file_rules(fcinfo: FunctionCallInfo) -> Datum {
    let rsi: *mut ReturnSetInfo;

    /*
     * Build tuplestore to hold the result rows.  We must use the Materialize
     * mode to be safe against HBA file changes while the cursor is open. It's
     * also more efficient than having to look up our current position in the
     * parsed list every time.
     */
    InitMaterializedSRF(fcinfo, 0);

    /* Fill the tuplestore */
    rsi = fcinfo_resultinfo(fcinfo);
    fill_hba_view(rsinfo_setResult(rsi), rsinfo_setDesc(rsi));

    PG_RETURN_NULL!(fcinfo);
}

/* Number of columns in pg_ident_file_mappings view */
const NUM_PG_IDENT_FILE_MAPPINGS_ATTS: usize = 7;

/*
 * fill_ident_line: build one row of pg_ident_file_mappings view, add it to
 * tuplestore
 *
 * tuple_store: where to store data
 * tupdesc: tuple descriptor for the view
 * map_number: unique identifier among all valid maps
 * filename: configuration file name (must always be valid)
 * lineno: line number of configuration file (must always be valid)
 * ident: parsed line data (can be NULL, in which case err_msg should be set)
 * err_msg: error message (NULL if none)
 *
 * Note: leaks memory, but we don't care since this is run in a short-lived
 * memory context.
 */
unsafe fn fill_ident_line(
    tuple_store: *mut Tuplestorestate,
    tupdesc: TupleDesc,
    map_number: c_int,
    filename: *mut c_char,
    lineno: c_int,
    ident: *mut IdentLine,
    err_msg: *const c_char,
) {
    let mut values: [Datum; NUM_PG_IDENT_FILE_MAPPINGS_ATTS] =
        [0; NUM_PG_IDENT_FILE_MAPPINGS_ATTS];
    let mut nulls: [bool; NUM_PG_IDENT_FILE_MAPPINGS_ATTS] =
        [false; NUM_PG_IDENT_FILE_MAPPINGS_ATTS];
    let tuple: HeapTuple;
    let mut index: usize;

    Assert!(tupdesc_natts(tupdesc) as usize == NUM_PG_IDENT_FILE_MAPPINGS_ATTS);

    // memset(values, 0, ...); memset(nulls, 0, ...); already zero-initialized.
    index = 0;

    /* map_number, nothing on error */
    if !err_msg.is_null() {
        nulls[index] = true;
        index += 1;
    } else {
        values[index] = Int32GetDatum(map_number);
        index += 1;
    }

    /* file_name */
    values[index] = CStringGetTextDatum(filename);
    index += 1;

    /* line_number */
    values[index] = Int32GetDatum(lineno);
    index += 1;

    if !ident.is_null() {
        values[index] = CStringGetTextDatum(ident_usermap(ident));
        index += 1;
        values[index] = CStringGetTextDatum(ident_system_user_string(ident));
        index += 1;
        values[index] = CStringGetTextDatum(ident_pg_user_string(ident));
        index += 1;
    } else {
        /* no parsing result, so set relevant fields to nulls */
        for i in 3..(NUM_PG_IDENT_FILE_MAPPINGS_ATTS - 1) {
            nulls[i] = true;
        }
    }

    /* error */
    if !err_msg.is_null() {
        values[NUM_PG_IDENT_FILE_MAPPINGS_ATTS - 1] = CStringGetTextDatum(err_msg);
    } else {
        nulls[NUM_PG_IDENT_FILE_MAPPINGS_ATTS - 1] = true;
    }

    tuple = heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
    tuplestore_puttuple(tuple_store, tuple);
}

/*
 * Read the pg_ident.conf file and fill the tuplestore with view records.
 */
unsafe fn fill_ident_view(tuple_store: *mut Tuplestorestate, tupdesc: TupleDesc) {
    let file: *mut FILE;
    let mut ident_lines: *mut List = NIL;
    let mut map_number: c_int = 0;
    let identcxt: MemoryContext;
    let oldcxt: MemoryContext;

    /*
     * In the unlikely event that we can't open pg_ident.conf, we throw an
     * error, rather than trying to report it via some sort of view entry.
     * (Most other error conditions should result in a message in a view
     * entry.)
     */
    file = open_auth_file(IdentFileName, ERROR, 0, null_mut());

    tokenize_auth_file(IdentFileName, file, &mut ident_lines, DEBUG3, 0);

    /* Now parse all the lines */
    identcxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"ident parser context".as_ptr(),
        ALLOCSET_SMALL_SIZES
    );
    oldcxt = MemoryContextSwitchTo(identcxt);
    foreach!(line, ident_lines, {
        let tok_line = lfirst(current_cell!(line)) as *mut TokenizedAuthLine;
        let mut identline: *mut IdentLine = null_mut();

        /* don't parse lines that already have errors */
        if tok_err_msg(tok_line).is_null() {
            identline = parse_ident_line(tok_line, DEBUG3);
        }

        /* no error, set a new mapping number */
        if tok_err_msg(tok_line).is_null() {
            map_number += 1;
        }

        fill_ident_line(
            tuple_store,
            tupdesc,
            map_number,
            tok_file_name(tok_line),
            tok_line_num(tok_line),
            identline,
            tok_err_msg(tok_line),
        );
    });

    /* Free tokenizer memory */
    free_auth_file(file, 0);
    /* Free parse_ident_line memory */
    MemoryContextSwitchTo(oldcxt);
    MemoryContextDelete(identcxt);
}

/*
 * SQL-accessible SRF to return all the entries in the pg_ident.conf file.
 */
#[no_mangle]
pub unsafe extern "C" fn pg_ident_file_mappings(fcinfo: FunctionCallInfo) -> Datum {
    let rsi: *mut ReturnSetInfo;

    /*
     * Build tuplestore to hold the result rows.  We must use the Materialize
     * mode to be safe against HBA file changes while the cursor is open. It's
     * also more efficient than having to look up our current position in the
     * parsed list every time.
     */
    InitMaterializedSRF(fcinfo, 0);

    /* Fill the tuplestore */
    rsi = fcinfo_resultinfo(fcinfo);
    fill_ident_view(rsinfo_setResult(rsi), rsinfo_setDesc(rsi));

    PG_RETURN_NULL!(fcinfo);
}
