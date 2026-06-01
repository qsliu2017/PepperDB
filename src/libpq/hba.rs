//! libpq/hba.c - Routines to handle host based authentication (that's the scheme
//! wherein you authenticate a user by seeing what IP address the system says he
//! comes from and choosing authentication method based on it).
//!
//! Translated 1:1 from postgres/src/backend/libpq/hba.c.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/libpq/hba.c

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

use crate::prelude::*;

// foreach! declares its own cursor; current_cell!/lfirst extract the datum.
use crate::{current_cell, foreach, list_make1};

use core::ffi::CStr;

use crate::nodes::pg_list::{
    lappend, lfirst, linitial, list_head, list_length, lnext, lsecond, list_free, List, ListCell,
    NIL,
};
use crate::lib::stringinfo::{
    appendStringInfoChar, appendStringInfoString, initStringInfo, resetStringInfo, StringInfo,
    StringInfoData,
};

// Network address structs and constants (shared from libpq/ifaddr.c translation).
use crate::libpq::ifaddr::{
    in6_addr, in_addr, sockaddr, sockaddr_in, sockaddr_in6, sockaddr_storage, PgIfAddrCallback,
};

// regex engine (regex/regex.h)
use crate::regex::regex::{
    pg_regcomp, pg_regerror, pg_regexec, pg_regfree, regex_t, regmatch_t, REG_ADVANCED, REG_NOMATCH,
    REG_OKAY,
};

// multibyte conversion (mb/pg_wchar.h)
use crate::mb::mbutils::pg_mb2wchar_with_len;
use crate::mb::wchar::pg_wchar;

// catalog/pg_collation.h
use crate::catalog::pg_known_oids::C_COLLATION_OID;

// utils/acl.h
use crate::utils::adt::acl::{get_role_oid, is_member_of_role_nosuper};

// utils/conffiles.h
use crate::utils::misc::conffiles::{AbsoluteConfigLocation, GetConfFilesInDir};

// storage/fd.h
use crate::storage::file::fd::{AllocateFile, FreeFile};

// error context (utils/elog.h)
use crate::utils::error::elog_impl::{error_context_stack, ErrorContextCallback};

// memory contexts (prelude provides MemoryContext, AllocSetContextCreate, etc.)
use crate::utils::mmgr::mcxt::PostmasterContext;

// replication/walsender.h
use crate::replication::walsender::{am_db_walsender, am_walsender};

// port/pgstrcasecmp
use crate::port::pgstrcasecmp::pg_strcasecmp;

// ---------------------------------------------------------------------------
// FILE is an opaque C type (stdio.h).
// ---------------------------------------------------------------------------
pub type FILE = c_void;

// ---------------------------------------------------------------------------
// libpq/hba.h: the canonical type definitions owned by this module.
// ---------------------------------------------------------------------------

// UserAuth enum (keep in sync with UserAuthName[] below).
pub type UserAuth = c_int;
pub const uaReject: UserAuth = 0;
pub const uaImplicitReject: UserAuth = 1;
pub const uaTrust: UserAuth = 2;
pub const uaIdent: UserAuth = 3;
pub const uaPassword: UserAuth = 4;
pub const uaMD5: UserAuth = 5;
pub const uaSCRAM: UserAuth = 6;
pub const uaGSS: UserAuth = 7;
pub const uaSSPI: UserAuth = 8;
pub const uaPAM: UserAuth = 9;
pub const uaBSD: UserAuth = 10;
pub const uaLDAP: UserAuth = 11;
pub const uaCert: UserAuth = 12;
pub const uaRADIUS: UserAuth = 13;
pub const uaPeer: UserAuth = 14;
pub const uaOAuth: UserAuth = 15;
pub const USER_AUTH_LAST: UserAuth = uaOAuth;

// IPCompareMethod enum
pub type IPCompareMethod = c_int;
pub const ipCmpMask: IPCompareMethod = 0;
pub const ipCmpSameHost: IPCompareMethod = 1;
pub const ipCmpSameNet: IPCompareMethod = 2;
pub const ipCmpAll: IPCompareMethod = 3;

// ConnType enum
pub type ConnType = c_int;
pub const ctLocal: ConnType = 0;
pub const ctHost: ConnType = 1;
pub const ctHostSSL: ConnType = 2;
pub const ctHostNoSSL: ConnType = 3;
pub const ctHostGSS: ConnType = 4;
pub const ctHostNoGSS: ConnType = 5;

// ClientCertMode enum
pub type ClientCertMode = c_int;
pub const clientCertOff: ClientCertMode = 0;
pub const clientCertCA: ClientCertMode = 1;
pub const clientCertFull: ClientCertMode = 2;

// ClientCertName enum
pub type ClientCertName = c_int;
pub const clientCertCN: ClientCertName = 0;
pub const clientCertDN: ClientCertName = 1;

#[repr(C)]
pub struct AuthToken {
    pub string: *mut c_char,
    pub quoted: bool,
    pub regex: *mut regex_t,
}

#[repr(C)]
pub struct HbaLine {
    pub sourcefile: *mut c_char,
    pub linenumber: c_int,
    pub rawline: *mut c_char,
    pub conntype: ConnType,
    pub databases: *mut List,
    pub roles: *mut List,
    pub addr: sockaddr_storage,
    pub addrlen: c_int, // zero if we don't have a valid addr
    pub mask: sockaddr_storage,
    pub masklen: c_int, // zero if we don't have a valid mask
    pub ip_cmp_method: IPCompareMethod,
    pub hostname: *mut c_char,
    pub auth_method: UserAuth,
    pub usermap: *mut c_char,
    pub pamservice: *mut c_char,
    pub pam_use_hostname: bool,
    pub ldaptls: bool,
    pub ldapscheme: *mut c_char,
    pub ldapserver: *mut c_char,
    pub ldapport: c_int,
    pub ldapbinddn: *mut c_char,
    pub ldapbindpasswd: *mut c_char,
    pub ldapsearchattribute: *mut c_char,
    pub ldapsearchfilter: *mut c_char,
    pub ldapbasedn: *mut c_char,
    pub ldapscope: c_int,
    pub ldapprefix: *mut c_char,
    pub ldapsuffix: *mut c_char,
    pub clientcert: ClientCertMode,
    pub clientcertname: ClientCertName,
    pub krb_realm: *mut c_char,
    pub include_realm: bool,
    pub compat_realm: bool,
    pub upn_username: bool,
    pub radiusservers: *mut List,
    pub radiusservers_s: *mut c_char,
    pub radiussecrets: *mut List,
    pub radiussecrets_s: *mut c_char,
    pub radiusidentifiers: *mut List,
    pub radiusidentifiers_s: *mut c_char,
    pub radiusports: *mut List,
    pub radiusports_s: *mut c_char,
    pub oauth_issuer: *mut c_char,
    pub oauth_scope: *mut c_char,
    pub oauth_validator: *mut c_char,
    pub oauth_skip_usermap: bool,
}

#[repr(C)]
pub struct IdentLine {
    pub linenumber: c_int,
    pub usermap: *mut c_char,
    pub system_user: *mut AuthToken,
    pub pg_user: *mut AuthToken,
}

#[repr(C)]
pub struct TokenizedAuthLine {
    pub fields: *mut List, // List of lists of AuthTokens
    pub file_name: *mut c_char,
    pub line_num: c_int,
    pub raw_line: *mut c_char,
    pub err_msg: *mut c_char,
}

// kluge to avoid including libpq/libpq-be.h here
pub type hbaPort = crate::libpq::libpq_be::Port;

// ---------------------------------------------------------------------------
// SockAddr (libpq/pqcomm.h): { sockaddr_storage addr; socklen_t salen; }
// The Port struct mirrors raddr/laddr as opaque c_void; we view them as this.
// ---------------------------------------------------------------------------
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SockAddr {
    pub addr: sockaddr_storage,
    pub salen: socklen_t, // ACCEPT_TYPE_ARG3
}

pub type socklen_t = c_uint;

// ---------------------------------------------------------------------------
// System constants from <sys/socket.h> / <netdb.h>.
// AF_UNIX is 1; AF_INET is 2; AF_INET6 is 30 on macOS, 10 on Linux.
// ---------------------------------------------------------------------------
const AF_UNIX: c_int = 1;
const AF_INET: c_int = 2;
#[cfg(any(target_os = "macos", target_os = "ios"))]
const AF_INET6: c_int = 30;
#[cfg(not(any(target_os = "macos", target_os = "ios")))]
const AF_INET6: c_int = 10;
const AF_UNSPEC: c_int = 0;

// SOCK_DGRAM from <sys/socket.h>: 2 on both macOS and Linux.
const SOCK_DGRAM: c_int = 2;

// AI_NUMERICHOST from <netdb.h>: 4 on macOS, 4 on Linux.
const AI_NUMERICHOST: c_int = 4;
// EAI_NONAME from <netdb.h>: 8 on macOS, -2 on Linux.
#[cfg(any(target_os = "macos", target_os = "ios"))]
const EAI_NONAME: c_int = 8;
#[cfg(not(any(target_os = "macos", target_os = "ios")))]
const EAI_NONAME: c_int = -2;

// NI_MAXHOST / NI_NAMEREQD from <netdb.h>.
const NI_MAXHOST: usize = 1025;
const NI_NAMEREQD: c_int = 4;

// ENOENT from <errno.h>.
const ENOENT: c_int = 2;

// CONF_FILE_* recursion-depth limits (utils/conffiles.h).
const CONF_FILE_START_DEPTH: c_int = 0;
const CONF_FILE_MAX_DEPTH: c_int = 10;

// ERRCODE_* (utils/errcodes.h) used in C ereport() calls; folded into comments
// per the single-message ereport convention, so kept here only for reference.

// ---------------------------------------------------------------------------
// struct addrinfo (<netdb.h>) - used for name/address resolution.
// ---------------------------------------------------------------------------
#[repr(C)]
#[derive(Clone, Copy)]
pub struct addrinfo {
    pub ai_flags: c_int,
    pub ai_family: c_int,
    pub ai_socktype: c_int,
    pub ai_protocol: c_int,
    pub ai_addrlen: socklen_t,
    // macOS orders ai_canonname before ai_addr; Linux orders ai_addr first.
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub ai_canonname: *mut c_char,
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    pub ai_addr: *mut sockaddr,
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    pub ai_addr: *mut sockaddr,
    #[cfg(not(any(target_os = "macos", target_os = "ios")))]
    pub ai_canonname: *mut c_char,
    pub ai_next: *mut addrinfo,
}

// ---------------------------------------------------------------------------
// Stubs for dependencies that live in OTHER .c files not yet ported.
// ---------------------------------------------------------------------------

// common/ip.h
unsafe fn pg_range_sockaddr(
    _addr: *const sockaddr_storage,
    _netaddr: *const sockaddr_storage,
    _netmask: *const sockaddr_storage,
) -> c_int {
    unimplemented!() // TODO(pg-port): common/ip.c pg_range_sockaddr
}

unsafe fn pg_sockaddr_cidr_mask(
    _mask: *mut sockaddr_storage,
    _numbits: *mut c_char,
    _family: c_int,
) -> c_int {
    unimplemented!() // TODO(pg-port): common/ip.c pg_sockaddr_cidr_mask
}

unsafe fn pg_getaddrinfo_all(
    _hostname: *const c_char,
    _servname: *const c_char,
    _hintp: *const addrinfo,
    _result: *mut *mut addrinfo,
) -> c_int {
    unimplemented!() // TODO(pg-port): common/ip.c pg_getaddrinfo_all
}

unsafe fn pg_freeaddrinfo_all(_hint_ai_family: c_int, _ai: *mut addrinfo) {
    unimplemented!() // TODO(pg-port): common/ip.c pg_freeaddrinfo_all
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
    unimplemented!() // TODO(pg-port): common/ip.c pg_getnameinfo_all
}

// libpq/ifaddr.h
unsafe fn pg_foreach_ifaddr(_callback: PgIfAddrCallback, _cb_data: *mut c_void) -> c_int {
    unimplemented!() // TODO(pg-port): libpq/ifaddr.c pg_foreach_ifaddr
}

// libpq/oauth.h
unsafe fn check_oauth_validator(
    _hba: *mut HbaLine,
    _elevel: c_int,
    _err_msg: *mut *mut c_char,
) -> bool {
    unimplemented!() // TODO(pg-port): libpq/auth-oauth.c check_oauth_validator
}

// utils/varlena.h
unsafe fn SplitGUCList(
    _rawstring: *mut c_char,
    _separator: c_char,
    _namelist: *mut *mut List,
) -> bool {
    unimplemented!() // TODO(pg-port): utils/adt/varlena.c SplitGUCList
}

// utils/builtins.h - psprintf-family vararg helpers are stubbed (see callers,
// which build messages from CStr in this port).
unsafe fn psprintf_stub(s: *const c_char) -> *mut c_char {
    // Single-arg passthrough used where the C psprintf has no live format args.
    pstrdup(s)
}

// common/string.h
unsafe fn pg_strip_crlf(_str: *mut c_char) -> c_int {
    unimplemented!() // TODO(pg-port): common/string.c pg_strip_crlf
}

// common/pg_get_line.h
unsafe fn pg_get_line_append(
    _stream: *mut FILE,
    _buf: StringInfo,
    _hint: *mut c_void,
) -> bool {
    unimplemented!() // TODO(pg-port): common/pg_get_line.c pg_get_line_append
}

// stdio.h FILE state checks.
unsafe fn feof(_stream: *mut FILE) -> c_int {
    unimplemented!() // TODO(pg-port): libc feof
}
unsafe fn ferror(_stream: *mut FILE) -> c_int {
    unimplemented!() // TODO(pg-port): libc ferror
}

// <netdb.h> resolver (system).
unsafe fn getaddrinfo(
    _node: *const c_char,
    _service: *const c_char,
    _hints: *const addrinfo,
    _res: *mut *mut addrinfo,
) -> c_int {
    unimplemented!() // TODO(pg-port): system getaddrinfo
}
unsafe fn freeaddrinfo(_res: *mut addrinfo) {
    unimplemented!() // TODO(pg-port): system freeaddrinfo
}
unsafe fn gai_strerror(_ecode: c_int) -> *const c_char {
    unimplemented!() // TODO(pg-port): system gai_strerror
}

// libc string/mem primitives (string.h, stdlib.h).
unsafe fn strlen(_s: *const c_char) -> usize {
    unimplemented!() // TODO(pg-port): libc strlen
}
unsafe fn strcmp(_a: *const c_char, _b: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port): libc strcmp
}
unsafe fn strchr(_s: *const c_char, _c: c_int) -> *mut c_char {
    unimplemented!() // TODO(pg-port): libc strchr
}
unsafe fn strstr(_haystack: *const c_char, _needle: *const c_char) -> *mut c_char {
    unimplemented!() // TODO(pg-port): libc strstr
}
unsafe fn strcat(_dst: *mut c_char, _src: *const c_char) -> *mut c_char {
    unimplemented!() // TODO(pg-port): libc strcat
}
unsafe fn atoi(_s: *const c_char) -> c_int {
    unimplemented!() // TODO(pg-port): libc atoi
}
unsafe fn memcpy_c(_dst: *mut c_void, _src: *const c_void, _n: usize) -> *mut c_void {
    unimplemented!() // TODO(pg-port): libc memcpy
}
unsafe fn memset_c(_dst: *mut c_void, _ch: c_int, _n: usize) -> *mut c_void {
    unimplemented!() // TODO(pg-port): libc memset
}

// HBA/ident file path GUCs (guc.c / postmaster).
extern "C" {
    static mut HbaFileName: *mut c_char;
    static mut IdentFileName: *mut c_char;
}

// errno access (Darwin uses __error()).
#[cfg(any(target_os = "macos", target_os = "ios"))]
extern "C" {
    fn __error() -> *mut c_int;
}
#[cfg(any(target_os = "macos", target_os = "ios"))]
unsafe fn get_errno() -> c_int {
    *__error()
}
#[cfg(any(target_os = "macos", target_os = "ios"))]
unsafe fn set_errno(e: c_int) {
    *__error() = e;
}
#[cfg(not(any(target_os = "macos", target_os = "ios")))]
extern "C" {
    fn __errno_location() -> *mut c_int;
}
#[cfg(not(any(target_os = "macos", target_os = "ios")))]
unsafe fn get_errno() -> c_int {
    *__errno_location()
}
#[cfg(not(any(target_os = "macos", target_os = "ios")))]
unsafe fn set_errno(e: c_int) {
    *__errno_location() = e;
}

// ---------------------------------------------------------------------------
// callback data for check_network_callback
// ---------------------------------------------------------------------------
#[repr(C)]
struct check_network_data {
    method: IPCompareMethod, // test method
    raddr: *mut SockAddr,    // client's actual address
    result: bool,            // set to true if match
}

#[repr(C)]
struct tokenize_error_callback_arg {
    filename: *const c_char,
    linenum: c_int,
}

// token_* helper macros from hba.c, expressed as unsafe fns operating on *AuthToken.
unsafe fn token_has_regexp(t: *const AuthToken) -> bool {
    !(*t).regex.is_null()
}
unsafe fn token_is_member_check(t: *const AuthToken) -> bool {
    !(*t).quoted && *(*t).string == b'+' as c_char
}
unsafe fn token_is_keyword(t: *const AuthToken, k: &CStr) -> bool {
    !(*t).quoted && strcmp((*t).string, k.as_ptr()) == 0
}
unsafe fn token_matches(t: *const AuthToken, k: *const c_char) -> bool {
    strcmp((*t).string, k) == 0
}
unsafe fn token_matches_insensitive(t: *const AuthToken, k: *const c_char) -> bool {
    pg_strcasecmp((*t).string, k) == 0
}

// ---------------------------------------------------------------------------
// Module-static state.
// ---------------------------------------------------------------------------

// Memory context holding the list of TokenizedAuthLines when parsing HBA or
// ident configuration files.  Created when opening the first file.
static mut tokenize_context: MemoryContext = core::ptr::null_mut();

// pre-parsed content of HBA config file: list of HbaLine structs.
static mut parsed_hba_lines: *mut List = NIL;
static mut parsed_hba_context: MemoryContext = core::ptr::null_mut();

// pre-parsed content of ident mapping file: list of IdentLine structs.
static mut parsed_ident_lines: *mut List = NIL;
static mut parsed_ident_context: MemoryContext = core::ptr::null_mut();

// The names of the authentication methods supported by PostgreSQL.
// Note: keep this in sync with the UserAuth enum.
static UserAuthName: [&CStr; (USER_AUTH_LAST + 1) as usize] = [
    c"reject",
    c"implicit reject", // Not a user-visible option
    c"trust",
    c"ident",
    c"password",
    c"md5",
    c"scram-sha-256",
    c"gss",
    c"sspi",
    c"pam",
    c"bsd",
    c"ldap",
    c"cert",
    c"radius",
    c"peer",
    c"oauth",
];

// StaticAssertDecl(lengthof(UserAuthName) == USER_AUTH_LAST + 1, ...)
const _: () = assert!(UserAuthName.len() == (USER_AUTH_LAST + 1) as usize);

/*
 * isblank() exists in the ISO C99 spec, but it's not very portable yet,
 * so provide our own version.
 */
pub fn pg_isblank(c: c_char) -> bool {
    c == b' ' as c_char || c == b'\t' as c_char || c == b'\r' as c_char
}

/*
 * Grab one token out of the string pointed to by *lineptr.
 *
 * Tokens are strings of non-blank characters bounded by blank characters,
 * commas, beginning of line, and end of line.  Blank means space or tab.
 *
 * Tokens can be delimited by double quotes (this allows the inclusion of
 * commas, blanks, and '#', but not newlines).  As in SQL, write two
 * double-quotes to represent a double quote.
 *
 * Comments (started by an unquoted '#') are skipped, i.e. the remainder
 * of the line is ignored.
 *
 * (Note that line continuation processing happens before tokenization.
 * Thus, if a continuation occurs within quoted text or a comment, the
 * quoted text or comment is considered to continue to the next line.)
 *
 * The token, if any, is returned into buf (replacing any previous
 * contents), and *lineptr is advanced past the token.
 *
 * Also, we set *initial_quote to indicate whether there was quoting before
 * the first character.  (We use that to prevent "@x" from being treated
 * as a file inclusion request.  Note that @"x" should be so treated;
 * we want to allow that to support embedded spaces in file paths.)
 *
 * We set *terminating_comma to indicate whether the token is terminated by a
 * comma (which is not returned, nor advanced over).
 *
 * The only possible error condition is lack of terminating quote, but we
 * currently do not detect that, but just return the rest of the line.
 *
 * If successful: store dequoted token in buf and return true.
 * If no more tokens on line: set buf to empty and return false.
 */
unsafe fn next_token(
    lineptr: *mut *mut c_char,
    buf: StringInfo,
    initial_quote: *mut bool,
    terminating_comma: *mut bool,
) -> bool {
    let mut c: c_int;
    let mut in_quote: bool = false;
    let mut was_quote: bool = false;
    let mut saw_quote: bool = false;

    /* Initialize output parameters */
    resetStringInfo(buf);
    *initial_quote = false;
    *terminating_comma = false;

    /* Move over any whitespace and commas preceding the next token */
    loop {
        c = *(*lineptr) as c_int;
        *lineptr = (*lineptr).add(1);
        if !(c != b'\0' as c_int && (pg_isblank(c as c_char) || c == b',' as c_int)) {
            break;
        }
    }

    /*
     * Build a token in buf of next characters up to EOL, unquoted comma, or
     * unquoted whitespace.
     */
    while c != b'\0' as c_int && (!pg_isblank(c as c_char) || in_quote) {
        /* skip comments to EOL */
        if c == b'#' as c_int && !in_quote {
            loop {
                c = *(*lineptr) as c_int;
                *lineptr = (*lineptr).add(1);
                if c == b'\0' as c_int {
                    break;
                }
            }
            break;
        }

        /* we do not pass back a terminating comma in the token */
        if c == b',' as c_int && !in_quote {
            *terminating_comma = true;
            break;
        }

        if c != b'"' as c_int || was_quote {
            appendStringInfoChar(buf, c as c_char);
        }

        /* Literal double-quote is two double-quotes */
        if in_quote && c == b'"' as c_int {
            was_quote = !was_quote;
        } else {
            was_quote = false;
        }

        if c == b'"' as c_int {
            in_quote = !in_quote;
            saw_quote = true;
            if (*buf).len == 0 {
                *initial_quote = true;
            }
        }

        c = *(*lineptr) as c_int;
        *lineptr = (*lineptr).add(1);
    }

    /*
     * Un-eat the char right after the token (critical in case it is '\0',
     * else next call will read past end of string).
     */
    *lineptr = (*lineptr).sub(1);

    saw_quote || (*buf).len > 0
}

/*
 * Construct a palloc'd AuthToken struct, copying the given string.
 */
unsafe fn make_auth_token(token: *const c_char, quoted: bool) -> *mut AuthToken {
    let authtoken: *mut AuthToken;
    let toklen: c_int;

    toklen = strlen(token) as c_int;
    /* we copy string into same palloc block as the struct */
    authtoken = palloc0(core::mem::size_of::<AuthToken>() + toklen as usize + 1) as *mut AuthToken;
    (*authtoken).string =
        (authtoken as *mut c_char).add(core::mem::size_of::<AuthToken>());
    (*authtoken).quoted = quoted;
    (*authtoken).regex = core::ptr::null_mut();
    memcpy_c(
        (*authtoken).string as *mut c_void,
        token as *const c_void,
        toklen as usize + 1,
    );

    authtoken
}

/*
 * Free an AuthToken, that may include a regular expression that needs
 * to be cleaned up explicitly.
 */
unsafe fn free_auth_token(token: *mut AuthToken) {
    if token_has_regexp(token) {
        pg_regfree((*token).regex);
    }
}

/*
 * Copy a AuthToken struct into freshly palloc'd memory.
 */
unsafe fn copy_auth_token(input: *mut AuthToken) -> *mut AuthToken {
    let out: *mut AuthToken = make_auth_token((*input).string, (*input).quoted);

    out
}

/*
 * Compile the regular expression and store it in the AuthToken given in
 * input.  Returns the result of pg_regcomp().  On error, the details are
 * stored in "err_msg".
 */
unsafe fn regcomp_auth_token(
    token: *mut AuthToken,
    filename: *mut c_char,
    line_num: c_int,
    err_msg: *mut *mut c_char,
    elevel: c_int,
) -> c_int {
    let wstr: *mut pg_wchar;
    let wlen: c_int;
    let rc: c_int;

    Assert!((*token).regex.is_null());

    if *(*token).string != b'/' as c_char {
        return 0; /* nothing to compile */
    }

    (*token).regex = palloc0(core::mem::size_of::<regex_t>()) as *mut regex_t;
    wstr = palloc(
        (strlen((*token).string.add(1)) + 1) * core::mem::size_of::<pg_wchar>(),
    ) as *mut pg_wchar;
    wlen = pg_mb2wchar_with_len(
        (*token).string.add(1),
        wstr,
        strlen((*token).string.add(1)) as c_int,
    );

    rc = pg_regcomp(
        (*token).regex,
        wstr,
        wlen as crate::c::Size,
        REG_ADVANCED,
        C_COLLATION_OID,
    );

    if rc != 0 {
        let mut errstr: [c_char; 100] = [0; 100];

        pg_regerror(rc, (*token).regex, errstr.as_mut_ptr(), 100);
        // C also: errcode(ERRCODE_INVALID_REGULAR_EXPRESSION),
        //         errcontext("line %d of configuration file \"%s\"", line_num, filename)
        ereport!(
            elevel,
            errmsg!(
                "invalid regular expression \"{}\": {}",
                CStr::from_ptr((*token).string.add(1)).to_string_lossy(),
                CStr::from_ptr(errstr.as_ptr()).to_string_lossy()
            )
        );

        *err_msg = psprintf_stub((*token).string.add(1));
    }

    pfree(wstr as *mut c_void);
    rc
}

/*
 * Execute a regular expression computed in an AuthToken, checking for a match
 * with the string specified in "match".  The caller may optionally give an
 * array to store the matches.  Returns the result of pg_regexec().
 */
unsafe fn regexec_auth_token(
    r#match: *const c_char,
    token: *mut AuthToken,
    nmatch: usize,
    pmatch: *mut regmatch_t,
) -> c_int {
    let wmatchstr: *mut pg_wchar;
    let wmatchlen: c_int;
    let r: c_int;

    Assert!(*(*token).string == b'/' as c_char && !(*token).regex.is_null());

    wmatchstr =
        palloc((strlen(r#match) + 1) * core::mem::size_of::<pg_wchar>()) as *mut pg_wchar;
    wmatchlen = pg_mb2wchar_with_len(r#match, wmatchstr, strlen(r#match) as c_int);

    r = pg_regexec(
        (*token).regex,
        wmatchstr,
        wmatchlen as crate::c::Size,
        0,
        core::ptr::null_mut(),
        nmatch as crate::c::Size,
        pmatch,
        0,
    );

    pfree(wmatchstr as *mut c_void);
    r
}

/*
 * Tokenize one HBA field from a line, handling file inclusion and comma lists.
 *
 * filename: current file's pathname (needed to resolve relative pathnames)
 * *lineptr: current line pointer, which will be advanced past field
 *
 * In event of an error, log a message at ereport level elevel, and also
 * set *err_msg to a string describing the error.  Note that the result
 * may be non-NIL anyway, so *err_msg must be tested to determine whether
 * there was an error.
 *
 * The result is a List of AuthToken structs, one for each token in the field,
 * or NIL if we reached EOL.
 */
unsafe fn next_field_expand(
    filename: *const c_char,
    lineptr: *mut *mut c_char,
    elevel: c_int,
    depth: c_int,
    err_msg: *mut *mut c_char,
) -> *mut List {
    let mut buf: StringInfoData = core::mem::zeroed();
    let mut trailing_comma: bool = false;
    let mut initial_quote: bool = false;
    let mut tokens: *mut List = NIL;

    initStringInfo(&mut buf);

    loop {
        if !next_token(lineptr, &mut buf, &mut initial_quote, &mut trailing_comma) {
            break;
        }

        /* Is this referencing a file? */
        if !initial_quote && buf.len > 1 && *buf.data == b'@' as c_char {
            tokens = tokenize_expand_file(
                tokens,
                filename,
                buf.data.add(1),
                elevel,
                depth + 1,
                err_msg,
            );
        } else {
            /*
             * lappend() may do its own allocations, so move to the context
             * for the list of tokens.
             */
            let oldcxt = MemoryContextSwitchTo(tokenize_context);
            tokens = lappend(
                tokens,
                make_auth_token(buf.data, initial_quote) as *mut c_void,
            );
            MemoryContextSwitchTo(oldcxt);
        }

        if !(trailing_comma && (*err_msg).is_null()) {
            break;
        }
    }

    pfree(buf.data as *mut c_void);

    tokens
}

/*
 * tokenize_include_file
 *		Include a file from another file into an hba "field".
 *
 * Opens and tokenises a file included from another authentication file
 * with one of the include records ("include", "include_if_exists" or
 * "include_dir"), and assign all values found to an existing list of
 * list of AuthTokens.
 *
 * All new tokens are allocated in the memory context dedicated to the
 * tokenization, aka tokenize_context.
 *
 * If missing_ok is true, ignore a missing file.
 *
 * In event of an error, log a message at ereport level elevel, and also
 * set *err_msg to a string describing the error.  Note that the result
 * may be non-NIL anyway, so *err_msg must be tested to determine whether
 * there was an error.
 */
unsafe fn tokenize_include_file(
    outer_filename: *const c_char,
    inc_filename: *const c_char,
    tok_lines: *mut *mut List,
    elevel: c_int,
    depth: c_int,
    missing_ok: bool,
    err_msg: *mut *mut c_char,
) {
    let inc_fullname: *mut c_char;
    let inc_file: *mut FILE;

    inc_fullname = AbsoluteConfigLocation(inc_filename, outer_filename);
    inc_file = open_auth_file(inc_fullname, elevel, depth, err_msg);

    if inc_file.is_null() {
        if get_errno() == ENOENT && missing_ok {
            ereport!(
                elevel,
                errmsg!(
                    "skipping missing authentication file \"{}\"",
                    CStr::from_ptr(inc_fullname).to_string_lossy()
                )
            );
            *err_msg = core::ptr::null_mut();
            pfree(inc_fullname as *mut c_void);
            return;
        }

        /* error in err_msg, so leave and report */
        pfree(inc_fullname as *mut c_void);
        Assert!(!err_msg.is_null());
        return;
    }

    tokenize_auth_file(inc_fullname, inc_file, tok_lines, elevel, depth);
    free_auth_file(inc_file, depth);
    pfree(inc_fullname as *mut c_void);
}

/*
 * tokenize_expand_file
 *		Expand a file included from another file into an hba "field"
 *
 * Opens and tokenises a file included from another HBA config file with @,
 * and returns all values found therein as a flat list of AuthTokens.  If a
 * @-token or include record is found, recursively expand it.  The newly
 * read tokens are appended to "tokens" (so that foo,bar,@baz does what you
 * expect).  All new tokens are allocated in the memory context dedicated
 * to the list of TokenizedAuthLines, aka tokenize_context.
 *
 * In event of an error, log a message at ereport level elevel, and also
 * set *err_msg to a string describing the error.  Note that the result
 * may be non-NIL anyway, so *err_msg must be tested to determine whether
 * there was an error.
 */
unsafe fn tokenize_expand_file(
    mut tokens: *mut List,
    outer_filename: *const c_char,
    inc_filename: *const c_char,
    elevel: c_int,
    depth: c_int,
    err_msg: *mut *mut c_char,
) -> *mut List {
    let inc_fullname: *mut c_char;
    let inc_file: *mut FILE;
    let mut inc_lines: *mut List = NIL;

    inc_fullname = AbsoluteConfigLocation(inc_filename, outer_filename);
    inc_file = open_auth_file(inc_fullname, elevel, depth, err_msg);

    if inc_file.is_null() {
        /* error already logged */
        pfree(inc_fullname as *mut c_void);
        return tokens;
    }

    /*
     * There is possible recursion here if the file contains @ or an include
     * record.
     */
    tokenize_auth_file(inc_fullname, inc_file, &mut inc_lines, elevel, depth);

    pfree(inc_fullname as *mut c_void);

    /*
     * Move all the tokens found in the file to the tokens list.  These are
     * already saved in tokenize_context.
     */
    foreach!(inc_line, inc_lines, {
        let tok_line: *mut TokenizedAuthLine =
            lfirst(current_cell!(inc_line)) as *mut TokenizedAuthLine;

        /* If any line has an error, propagate that up to caller */
        if !(*tok_line).err_msg.is_null() {
            *err_msg = pstrdup((*tok_line).err_msg);
            break;
        }

        foreach!(inc_field, (*tok_line).fields, {
            let inc_tokens: *mut List = lfirst(current_cell!(inc_field)) as *mut List;

            foreach!(inc_token, inc_tokens, {
                let token: *mut AuthToken = lfirst(current_cell!(inc_token)) as *mut AuthToken;

                /*
                 * lappend() may do its own allocations, so move to the
                 * context for the list of tokens.
                 */
                let oldcxt = MemoryContextSwitchTo(tokenize_context);
                tokens = lappend(tokens, token as *mut c_void);
                MemoryContextSwitchTo(oldcxt);
            });
        });
    });

    free_auth_file(inc_file, depth);
    tokens
}

/*
 * free_auth_file
 *		Free a file opened by open_auth_file().
 */
pub unsafe fn free_auth_file(file: *mut FILE, depth: c_int) {
    FreeFile(file);

    /* If this is the last cleanup, remove the tokenization context */
    if depth == CONF_FILE_START_DEPTH {
        MemoryContextDelete(tokenize_context);
        tokenize_context = core::ptr::null_mut();
    }
}

/*
 * open_auth_file
 *		Open the given file.
 *
 * filename: the absolute path to the target file
 * elevel: message logging level
 * depth: recursion level when opening the file
 * err_msg: details about the error
 *
 * Return value is the opened file.  On error, returns NULL with details
 * about the error stored in "err_msg".
 */
pub unsafe fn open_auth_file(
    filename: *const c_char,
    elevel: c_int,
    depth: c_int,
    err_msg: *mut *mut c_char,
) -> *mut FILE {
    let file: *mut FILE;

    /*
     * Reject too-deep include nesting depth.  This is just a safety check to
     * avoid dumping core due to stack overflow if an include file loops back
     * to itself.  The maximum nesting depth is pretty arbitrary.
     */
    if depth > CONF_FILE_MAX_DEPTH {
        // C also: errcode_for_file_access()
        ereport!(
            elevel,
            errmsg!(
                "could not open file \"{}\": maximum nesting depth exceeded",
                CStr::from_ptr(filename).to_string_lossy()
            )
        );
        if !err_msg.is_null() {
            *err_msg = psprintf_stub(filename);
        }
        return core::ptr::null_mut();
    }

    file = AllocateFile(filename, c"r".as_ptr());
    if file.is_null() {
        let save_errno = get_errno();

        // C also: errcode_for_file_access()
        ereport!(
            elevel,
            errmsg!(
                "could not open file \"{}\": %m",
                CStr::from_ptr(filename).to_string_lossy()
            )
        );
        if !err_msg.is_null() {
            set_errno(save_errno);
            *err_msg = psprintf_stub(filename);
        }
        /* the caller may care about some specific errno */
        set_errno(save_errno);
        return core::ptr::null_mut();
    }

    /*
     * When opening the top-level file, create the memory context used for the
     * tokenization.  This will be closed with this file when coming back to
     * this level of cleanup.
     */
    if depth == CONF_FILE_START_DEPTH {
        /*
         * A context may be present, but assume that it has been eliminated
         * already.
         */
        tokenize_context = AllocSetContextCreate!(
            CurrentMemoryContext,
            c"tokenize_context".as_ptr(),
            ALLOCSET_START_SMALL_SIZES
        );
    }

    file
}

/*
 * error context callback for tokenize_auth_file()
 */
unsafe extern "C" fn tokenize_error_callback(arg: *mut c_void) {
    let callback_arg: *mut tokenize_error_callback_arg = arg as *mut tokenize_error_callback_arg;

    // errcontext("line %d of configuration file \"%s\"",
    //            callback_arg->linenum, callback_arg->filename);
    let _ = (*callback_arg).linenum;
    let _ = (*callback_arg).filename;
}

/*
 * tokenize_auth_file
 *		Tokenize the given file.
 *
 * The output is a list of TokenizedAuthLine structs; see the struct definition
 * in libpq/hba.h.  This is the central piece in charge of parsing the
 * authentication files.  All the operations of this function happen in its own
 * local memory context, easing the cleanup of anything allocated here.  This
 * matters a lot when reloading authentication files in the postmaster.
 *
 * filename: the absolute path to the target file
 * file: the already-opened target file
 * tok_lines: receives output list, saved into tokenize_context
 * elevel: message logging level
 * depth: level of recursion when tokenizing the target file
 *
 * Errors are reported by logging messages at ereport level elevel and by
 * adding TokenizedAuthLine structs containing non-null err_msg fields to the
 * output list.
 */
pub unsafe fn tokenize_auth_file(
    filename: *const c_char,
    file: *mut FILE,
    tok_lines: *mut *mut List,
    elevel: c_int,
    depth: c_int,
) {
    let mut line_number: c_int = 1;
    let mut buf: StringInfoData = core::mem::zeroed();
    let linecxt: MemoryContext;
    let funccxt: MemoryContext; /* context of this function's caller */
    let mut tokenerrcontext: ErrorContextCallback = core::mem::zeroed();
    let mut callback_arg: tokenize_error_callback_arg = core::mem::zeroed();

    Assert!(!tokenize_context.is_null());

    callback_arg.filename = filename;
    callback_arg.linenum = line_number;

    tokenerrcontext.callback = tokenize_error_callback;
    tokenerrcontext.arg = &mut callback_arg as *mut tokenize_error_callback_arg as *mut c_void;
    tokenerrcontext.previous = error_context_stack;
    error_context_stack = &mut tokenerrcontext;

    /*
     * Do all the local tokenization in its own context, to ease the cleanup
     * of any memory allocated while tokenizing.
     */
    linecxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"tokenize_auth_file".as_ptr(),
        ALLOCSET_SMALL_SIZES
    );
    funccxt = MemoryContextSwitchTo(linecxt);

    initStringInfo(&mut buf);

    if depth == CONF_FILE_START_DEPTH {
        *tok_lines = NIL;
    }

    while feof(file) == 0 && ferror(file) == 0 {
        let tok_line: *mut TokenizedAuthLine;
        let mut oldcxt: MemoryContext;
        let mut lineptr: *mut c_char;
        let mut current_line: *mut List = NIL;
        let mut err_msg: *mut c_char = core::ptr::null_mut();
        let mut last_backslash_buflen: c_int = 0;
        let mut continuations: c_int = 0;

        /* Collect the next input line, handling backslash continuations */
        resetStringInfo(&mut buf);

        while pg_get_line_append(file, &mut buf, core::ptr::null_mut()) {
            /* Strip trailing newline, including \r in case we're on Windows */
            buf.len = pg_strip_crlf(buf.data);

            /*
             * Check for backslash continuation.  The backslash must be after
             * the last place we found a continuation, else two backslashes
             * followed by two \n's would behave surprisingly.
             */
            if buf.len > last_backslash_buflen
                && *buf.data.add(buf.len as usize - 1) == b'\\' as c_char
            {
                /* Continuation, so strip it and keep reading */
                buf.len -= 1;
                *buf.data.add(buf.len as usize) = b'\0' as c_char;
                last_backslash_buflen = buf.len;
                continuations += 1;
                continue;
            }

            /* Nope, so we have the whole line */
            break;
        }

        if ferror(file) != 0 {
            /* I/O error! */
            let save_errno = get_errno();

            // C also: errcode_for_file_access()
            ereport!(
                elevel,
                errmsg!(
                    "could not read file \"{}\": %m",
                    CStr::from_ptr(filename).to_string_lossy()
                )
            );
            set_errno(save_errno);
            err_msg = psprintf_stub(filename);
            break;
        }

        /*
         * The C control flow uses gotos (next_line / process_line).  We model
         * those with a labeled block: `break 'process_line` jumps to the
         * general processing that follows the block, and we use a boolean to
         * indicate when a line is fully handled and should skip that block
         * (the C "goto next_line").
         */
        let mut emit_line = true;

        'process_line: {
            /* Parse fields */
            lineptr = buf.data;
            while *lineptr != b'\0' as c_char && err_msg.is_null() {
                let current_field: *mut List;

                current_field = next_field_expand(filename, &mut lineptr, elevel, depth, &mut err_msg);
                /* add field to line, unless we are at EOL or comment start */
                if current_field != NIL {
                    /*
                     * lappend() may do its own allocations, so move to the
                     * context for the list of tokens.
                     */
                    oldcxt = MemoryContextSwitchTo(tokenize_context);
                    current_line = lappend(current_line, current_field as *mut c_void);
                    MemoryContextSwitchTo(oldcxt);
                }
            }

            /*
             * Reached EOL; no need to emit line to TokenizedAuthLine list if it's
             * boring.
             */
            if current_line == NIL && err_msg.is_null() {
                /* goto next_line */
                emit_line = false;
                break 'process_line;
            }

            /* If the line is valid, check if that's an include directive */
            if err_msg.is_null() && list_length(current_line) == 2 {
                let first: *mut AuthToken;
                let second: *mut AuthToken;

                first = linitial(linitial(current_line) as *const List) as *mut AuthToken;
                second = linitial(lsecond(current_line) as *const List) as *mut AuthToken;

                if strcmp((*first).string, c"include".as_ptr()) == 0 {
                    tokenize_include_file(
                        filename,
                        (*second).string,
                        tok_lines,
                        elevel,
                        depth + 1,
                        false,
                        &mut err_msg,
                    );

                    if !err_msg.is_null() {
                        /* goto process_line */
                        break 'process_line;
                    }

                    /*
                     * tokenize_auth_file() has taken care of creating the
                     * TokenizedAuthLines.
                     */
                    /* goto next_line */
                    emit_line = false;
                    break 'process_line;
                } else if strcmp((*first).string, c"include_dir".as_ptr()) == 0 {
                    let filenames: *mut *mut c_char;
                    let dir_name: *mut c_char = (*second).string;
                    let mut num_filenames: c_int = 0;
                    let mut err_buf: StringInfoData = core::mem::zeroed();

                    filenames = GetConfFilesInDir(
                        dir_name,
                        filename,
                        elevel,
                        &mut num_filenames,
                        &mut err_msg,
                    );

                    if filenames.is_null() {
                        /* the error is in err_msg, so create an entry */
                        /* goto process_line */
                        break 'process_line;
                    }

                    initStringInfo(&mut err_buf);
                    let mut i: c_int = 0;
                    while i < num_filenames {
                        tokenize_include_file(
                            filename,
                            *filenames.add(i as usize),
                            tok_lines,
                            elevel,
                            depth + 1,
                            false,
                            &mut err_msg,
                        );
                        /* cumulate errors if any */
                        if !err_msg.is_null() {
                            if err_buf.len > 0 {
                                appendStringInfoChar(&mut err_buf, b'\n' as c_char);
                            }
                            appendStringInfoString(&mut err_buf, err_msg);
                        }
                        i += 1;
                    }

                    /* clean up things */
                    let mut i: c_int = 0;
                    while i < num_filenames {
                        pfree(*filenames.add(i as usize) as *mut c_void);
                        i += 1;
                    }
                    pfree(filenames as *mut c_void);

                    /*
                     * If there were no errors, the line is fully processed,
                     * bypass the general TokenizedAuthLine processing.
                     */
                    if err_buf.len == 0 {
                        /* goto next_line */
                        emit_line = false;
                        break 'process_line;
                    }

                    /* Otherwise, process the cumulated errors, if any. */
                    err_msg = err_buf.data;
                    /* goto process_line */
                    break 'process_line;
                } else if strcmp((*first).string, c"include_if_exists".as_ptr()) == 0 {
                    tokenize_include_file(
                        filename,
                        (*second).string,
                        tok_lines,
                        elevel,
                        depth + 1,
                        true,
                        &mut err_msg,
                    );
                    if !err_msg.is_null() {
                        /* goto process_line */
                        break 'process_line;
                    }

                    /*
                     * tokenize_auth_file() has taken care of creating the
                     * TokenizedAuthLines.
                     */
                    /* goto next_line */
                    emit_line = false;
                    break 'process_line;
                }
            }
        } // 'process_line

        if emit_line {
            // process_line:
            /*
             * General processing: report the error if any and emit line to the
             * TokenizedAuthLine.  This is saved in the memory context dedicated
             * to this list.
             */
            oldcxt = MemoryContextSwitchTo(tokenize_context);
            tok_line =
                palloc0(core::mem::size_of::<TokenizedAuthLine>()) as *mut TokenizedAuthLine;
            (*tok_line).fields = current_line;
            (*tok_line).file_name = pstrdup(filename);
            (*tok_line).line_num = line_number;
            (*tok_line).raw_line = pstrdup(buf.data);
            (*tok_line).err_msg = if !err_msg.is_null() {
                pstrdup(err_msg)
            } else {
                core::ptr::null_mut()
            };
            *tok_lines = lappend(*tok_lines, tok_line as *mut c_void);
            MemoryContextSwitchTo(oldcxt);
        }

        // next_line:
        line_number += continuations + 1;
        callback_arg.linenum = line_number;
    }

    MemoryContextSwitchTo(funccxt);
    MemoryContextDelete(linecxt);

    error_context_stack = tokenerrcontext.previous;
}

/*
 * Does user belong to role?
 *
 * userid is the OID of the role given as the attempted login identifier.
 * We check to see if it is a member of the specified role name.
 */
unsafe fn is_member(userid: Oid, role: *const c_char) -> bool {
    let roleid: Oid;

    if !OidIsValid(userid) {
        return false; /* if user not exist, say "no" */
    }

    roleid = get_role_oid(role, true);

    if !OidIsValid(roleid) {
        return false; /* if target role not exist, say "no" */
    }

    /*
     * See if user is directly or indirectly a member of role. For this
     * purpose, a superuser is not considered to be automatically a member of
     * the role, so group auth only applies to explicit membership.
     */
    is_member_of_role_nosuper(userid, roleid)
}

/*
 * Check AuthToken list for a match to role, allowing group names.
 *
 * Each AuthToken listed is checked one-by-one.  Keywords are processed
 * first (these cannot have regular expressions), followed by regular
 * expressions (if any), the case-insensitive match (if requested) and
 * the exact match.
 */
unsafe fn check_role(
    role: *const c_char,
    roleid: Oid,
    tokens: *mut List,
    case_insensitive: bool,
) -> bool {
    let mut tok: *mut AuthToken;

    foreach!(cell, tokens, {
        tok = lfirst(current_cell!(cell)) as *mut AuthToken;
        if token_is_member_check(tok) {
            if is_member(roleid, (*tok).string.add(1)) {
                return true;
            }
        } else if token_is_keyword(tok, c"all") {
            return true;
        } else if token_has_regexp(tok) {
            if regexec_auth_token(role, tok, 0, core::ptr::null_mut()) == REG_OKAY {
                return true;
            }
        } else if case_insensitive {
            if token_matches_insensitive(tok, role) {
                return true;
            }
        } else if token_matches(tok, role) {
            return true;
        }
    });
    false
}

/*
 * Check to see if db/role combination matches AuthToken list.
 *
 * Each AuthToken listed is checked one-by-one.  Keywords are checked
 * first (these cannot have regular expressions), followed by regular
 * expressions (if any) and the exact match.
 */
unsafe fn check_db(
    dbname: *const c_char,
    role: *const c_char,
    roleid: Oid,
    tokens: *mut List,
) -> bool {
    let mut tok: *mut AuthToken;

    foreach!(cell, tokens, {
        tok = lfirst(current_cell!(cell)) as *mut AuthToken;
        if am_walsender && !am_db_walsender {
            /*
             * physical replication walsender connections can only match
             * replication keyword
             */
            if token_is_keyword(tok, c"replication") {
                return true;
            }
        } else if token_is_keyword(tok, c"all") {
            return true;
        } else if token_is_keyword(tok, c"sameuser") {
            if strcmp(dbname, role) == 0 {
                return true;
            }
        } else if token_is_keyword(tok, c"samegroup") || token_is_keyword(tok, c"samerole") {
            if is_member(roleid, dbname) {
                return true;
            }
        } else if token_is_keyword(tok, c"replication") {
            continue; /* never match this if not walsender */
        } else if token_has_regexp(tok) {
            if regexec_auth_token(dbname, tok, 0, core::ptr::null_mut()) == REG_OKAY {
                return true;
            }
        } else if token_matches(tok, dbname) {
            return true;
        }
    });
    false
}

unsafe fn ipv4eq(a: *mut sockaddr_in, b: *mut sockaddr_in) -> bool {
    (*a).sin_addr.s_addr == (*b).sin_addr.s_addr
}

unsafe fn ipv6eq(a: *mut sockaddr_in6, b: *mut sockaddr_in6) -> bool {
    let mut i: c_int = 0;

    while i < 16 {
        if (*a).sin6_addr.s6_addr[i as usize] != (*b).sin6_addr.s6_addr[i as usize] {
            return false;
        }
        i += 1;
    }

    true
}

/*
 * Check whether host name matches pattern.
 */
unsafe fn hostname_match(pattern: *const c_char, actual_hostname: *const c_char) -> bool {
    if *pattern == b'.' as c_char {
        /* suffix match */
        let plen: usize = strlen(pattern);
        let hlen: usize = strlen(actual_hostname);

        if hlen < plen {
            return false;
        }

        pg_strcasecmp(pattern, actual_hostname.add(hlen - plen)) == 0
    } else {
        pg_strcasecmp(pattern, actual_hostname) == 0
    }
}

/*
 * Check to see if a connecting IP matches a given host name.
 */
unsafe fn check_hostname(port: *mut hbaPort, hostname: *const c_char) -> bool {
    let mut gai_result: *mut addrinfo;
    let mut gai: *mut addrinfo;
    let mut ret: c_int;
    let mut found: bool;

    // Port.raddr is modeled as opaque c_void in libpq_be; view it as SockAddr.
    let raddr = &mut (*port).raddr as *mut _ as *mut SockAddr;

    /* Quick out if remote host name already known bad */
    if (*port).remote_hostname_resolv < 0 {
        return false;
    }

    /* Lookup remote host name if not already done */
    if (*port).remote_hostname.is_null() {
        let mut remote_hostname: [c_char; NI_MAXHOST] = [0; NI_MAXHOST];

        ret = pg_getnameinfo_all(
            &(*raddr).addr,
            (*raddr).salen as c_int,
            remote_hostname.as_mut_ptr(),
            NI_MAXHOST as c_int,
            core::ptr::null_mut(),
            0,
            NI_NAMEREQD,
        );
        if ret != 0 {
            /* remember failure; don't complain in the postmaster log yet */
            (*port).remote_hostname_resolv = -2;
            (*port).remote_hostname_errcode = ret;
            return false;
        }

        (*port).remote_hostname = pstrdup(remote_hostname.as_ptr());
    }

    /* Now see if remote host name matches this pg_hba line */
    if !hostname_match(hostname, (*port).remote_hostname) {
        return false;
    }

    /* If we already verified the forward lookup, we're done */
    if (*port).remote_hostname_resolv == 1 {
        return true;
    }

    /* Lookup IP from host name and check against original IP */
    gai_result = core::ptr::null_mut();
    ret = getaddrinfo(
        (*port).remote_hostname,
        core::ptr::null(),
        core::ptr::null(),
        &mut gai_result,
    );
    if ret != 0 {
        /* remember failure; don't complain in the postmaster log yet */
        (*port).remote_hostname_resolv = -2;
        (*port).remote_hostname_errcode = ret;
        return false;
    }

    found = false;
    gai = gai_result;
    while !gai.is_null() {
        if (*(*gai).ai_addr).sa_family as c_int == (*raddr).addr.ss_family as c_int {
            if (*(*gai).ai_addr).sa_family as c_int == AF_INET {
                if ipv4eq(
                    (*gai).ai_addr as *mut sockaddr_in,
                    &mut (*raddr).addr as *mut sockaddr_storage as *mut sockaddr_in,
                ) {
                    found = true;
                    break;
                }
            } else if (*(*gai).ai_addr).sa_family as c_int == AF_INET6 {
                if ipv6eq(
                    (*gai).ai_addr as *mut sockaddr_in6,
                    &mut (*raddr).addr as *mut sockaddr_storage as *mut sockaddr_in6,
                ) {
                    found = true;
                    break;
                }
            }
        }
        gai = (*gai).ai_next;
    }

    if !gai_result.is_null() {
        freeaddrinfo(gai_result);
    }

    if !found {
        elog!(
            DEBUG2,
            "pg_hba.conf host name \"{}\" rejected because address resolution did not return a match with IP address of client",
            CStr::from_ptr(hostname).to_string_lossy()
        );
    }

    (*port).remote_hostname_resolv = if found { 1 } else { -1 };

    found
}

/*
 * Check to see if a connecting IP matches the given address and netmask.
 */
unsafe fn check_ip(raddr: *mut SockAddr, addr: *mut sockaddr, mask: *mut sockaddr) -> bool {
    if (*raddr).addr.ss_family as c_int == (*addr).sa_family as c_int
        && pg_range_sockaddr(
            &(*raddr).addr,
            addr as *mut sockaddr_storage,
            mask as *mut sockaddr_storage,
        ) != 0
    {
        return true;
    }
    false
}

/*
 * pg_foreach_ifaddr callback: does client addr match this machine interface?
 */
unsafe extern "C" fn check_network_callback(
    addr: *mut sockaddr,
    netmask: *mut sockaddr,
    cb_data: *mut c_void,
) {
    let cn: *mut check_network_data = cb_data as *mut check_network_data;
    let mut mask: sockaddr_storage = core::mem::zeroed();

    /* Already found a match? */
    if (*cn).result {
        return;
    }

    if (*cn).method == ipCmpSameHost {
        /* Make an all-ones netmask of appropriate length for family */
        pg_sockaddr_cidr_mask(&mut mask, core::ptr::null_mut(), (*addr).sa_family as c_int);
        (*cn).result = check_ip((*cn).raddr, addr, &mut mask as *mut sockaddr_storage as *mut sockaddr);
    } else {
        /* Use the netmask of the interface itself */
        (*cn).result = check_ip((*cn).raddr, addr, netmask);
    }
}

/*
 * Use pg_foreach_ifaddr to check a samehost or samenet match
 */
unsafe fn check_same_host_or_net(raddr: *mut SockAddr, method: IPCompareMethod) -> bool {
    let mut cn: check_network_data = core::mem::zeroed();

    cn.method = method;
    cn.raddr = raddr;
    cn.result = false;

    set_errno(0);
    if pg_foreach_ifaddr(check_network_callback, &mut cn as *mut check_network_data as *mut c_void)
        < 0
    {
        // C also: errmsg uses %m
        ereport!(LOG, errmsg!("error enumerating network interfaces: %m"));
        return false;
    }

    cn.result
}

/*
 * Macros used to check and report on invalid configuration options.
 * On error: log a message at level elevel, set *err_msg, and exit the function.
 * These macros are not as general-purpose as they look, because they know
 * what the calling function's error-exit value is.
 *
 * INVALID_AUTH_OPTION = reports when an option is specified for a method where it's
 *						 not supported.
 * REQUIRE_AUTH_OPTION = same as INVALID_AUTH_OPTION, except it also checks if the
 *						 method is actually the one specified. Used as a shortcut when
 *						 the option is only valid for one authentication method.
 * MANDATORY_AUTH_ARG  = check if a required option is set for an authentication method,
 *						 reporting error if it's not.
 *
 * These C macros have function-specific error-exit values, so they are
 * open-coded at each use site below rather than defined as helpers.
 */

/*
 * Parse one tokenised line from the hba config file and store the result in a
 * HbaLine structure.
 *
 * If parsing fails, log a message at ereport level elevel, store an error
 * string in tok_line->err_msg, and return NULL.  (Some non-error conditions
 * can also result in such messages.)
 *
 * Note: this function leaks memory when an error occurs.  Caller is expected
 * to have set a memory context that will be reset if this function returns
 * NULL.
 */
pub unsafe fn parse_hba_line(tok_line: *mut TokenizedAuthLine, elevel: c_int) -> *mut HbaLine {
    let line_num: c_int = (*tok_line).line_num;
    let file_name: *mut c_char = (*tok_line).file_name;
    let err_msg: *mut *mut c_char = &mut (*tok_line).err_msg;
    let mut str: *mut c_char;
    let mut gai_result: *mut addrinfo;
    let mut hints: addrinfo = core::mem::zeroed();
    let mut ret: c_int;
    let cidr_slash: *mut c_char;
    let mut unsupauth: *const c_char;
    let mut field: *mut ListCell;
    let mut tokens: *mut List;
    let mut token: *mut AuthToken;
    let parsedline: *mut HbaLine;

    parsedline = palloc0(core::mem::size_of::<HbaLine>()) as *mut HbaLine;
    (*parsedline).sourcefile = pstrdup(file_name);
    (*parsedline).linenumber = line_num;
    (*parsedline).rawline = pstrdup((*tok_line).raw_line);

    /* Check the record type. */
    Assert!((*tok_line).fields != NIL);
    field = list_head((*tok_line).fields);
    tokens = lfirst(field) as *mut List;
    if (*tokens).length > 1 {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR),
        //         errhint("Specify exactly one connection type per line."),
        //         errcontext("line %d of configuration file \"%s\"", line_num, file_name)
        ereport!(elevel, errmsg!("multiple values specified for connection type"));
        *err_msg = pstrdup(c"multiple values specified for connection type".as_ptr());
        return core::ptr::null_mut();
    }
    token = linitial(tokens) as *mut AuthToken;
    if strcmp((*token).string, c"local".as_ptr()) == 0 {
        (*parsedline).conntype = ctLocal;
    } else if strcmp((*token).string, c"host".as_ptr()) == 0
        || strcmp((*token).string, c"hostssl".as_ptr()) == 0
        || strcmp((*token).string, c"hostnossl".as_ptr()) == 0
        || strcmp((*token).string, c"hostgssenc".as_ptr()) == 0
        || strcmp((*token).string, c"hostnogssenc".as_ptr()) == 0
    {
        if *(*token).string.add(4) == b's' as c_char {
            /* "hostssl" */
            (*parsedline).conntype = ctHostSSL;
            /* Log a warning if SSL support is not active */
            // not USE_SSL in this build:
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR),
            //         errcontext("line %d of configuration file \"%s\"", ...)
            ereport!(
                elevel,
                errmsg!("hostssl record cannot match because SSL is not supported by this build")
            );
            *err_msg = pstrdup(
                c"hostssl record cannot match because SSL is not supported by this build".as_ptr(),
            );
        } else if *(*token).string.add(4) == b'g' as c_char {
            /* "hostgssenc" */
            (*parsedline).conntype = ctHostGSS;
            // not ENABLE_GSS in this build:
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "hostgssenc record cannot match because GSSAPI is not supported by this build"
                )
            );
            *err_msg = pstrdup(
                c"hostgssenc record cannot match because GSSAPI is not supported by this build"
                    .as_ptr(),
            );
        } else if *(*token).string.add(4) == b'n' as c_char
            && *(*token).string.add(6) == b's' as c_char
        {
            (*parsedline).conntype = ctHostNoSSL;
        } else if *(*token).string.add(4) == b'n' as c_char
            && *(*token).string.add(6) == b'g' as c_char
        {
            (*parsedline).conntype = ctHostNoGSS;
        } else {
            /* "host" */
            (*parsedline).conntype = ctHost;
        }
    }
    /* record type */
    else {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
        ereport!(
            elevel,
            errmsg!(
                "invalid connection type \"{}\"",
                CStr::from_ptr((*token).string).to_string_lossy()
            )
        );
        *err_msg = psprintf_stub((*token).string);
        return core::ptr::null_mut();
    }

    /* Get the databases. */
    field = lnext((*tok_line).fields, field);
    if field.is_null() {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
        ereport!(elevel, errmsg!("end-of-line before database specification"));
        *err_msg = pstrdup(c"end-of-line before database specification".as_ptr());
        return core::ptr::null_mut();
    }
    (*parsedline).databases = NIL;
    tokens = lfirst(field) as *mut List;
    foreach!(tokencell, tokens, {
        let tok: *mut AuthToken = copy_auth_token(lfirst(current_cell!(tokencell)) as *mut AuthToken);

        /* Compile a regexp for the database token, if necessary */
        if regcomp_auth_token(tok, file_name, line_num, err_msg, elevel) != 0 {
            return core::ptr::null_mut();
        }

        (*parsedline).databases = lappend((*parsedline).databases, tok as *mut c_void);
    });

    /* Get the roles. */
    field = lnext((*tok_line).fields, field);
    if field.is_null() {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
        ereport!(elevel, errmsg!("end-of-line before role specification"));
        *err_msg = pstrdup(c"end-of-line before role specification".as_ptr());
        return core::ptr::null_mut();
    }
    (*parsedline).roles = NIL;
    tokens = lfirst(field) as *mut List;
    foreach!(tokencell, tokens, {
        let tok: *mut AuthToken = copy_auth_token(lfirst(current_cell!(tokencell)) as *mut AuthToken);

        /* Compile a regexp from the role token, if necessary */
        if regcomp_auth_token(tok, file_name, line_num, err_msg, elevel) != 0 {
            return core::ptr::null_mut();
        }

        (*parsedline).roles = lappend((*parsedline).roles, tok as *mut c_void);
    });

    if (*parsedline).conntype != ctLocal {
        /* Read the IP address field. (with or without CIDR netmask) */
        field = lnext((*tok_line).fields, field);
        if field.is_null() {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(elevel, errmsg!("end-of-line before IP address specification"));
            *err_msg = pstrdup(c"end-of-line before IP address specification".as_ptr());
            return core::ptr::null_mut();
        }
        tokens = lfirst(field) as *mut List;
        if (*tokens).length > 1 {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR),
            //         errhint("Specify one address range per line."), errcontext(...)
            ereport!(elevel, errmsg!("multiple values specified for host address"));
            *err_msg = pstrdup(c"multiple values specified for host address".as_ptr());
            return core::ptr::null_mut();
        }
        token = linitial(tokens) as *mut AuthToken;

        if token_is_keyword(token, c"all") {
            (*parsedline).ip_cmp_method = ipCmpAll;
        } else if token_is_keyword(token, c"samehost") {
            /* Any IP on this host is allowed to connect */
            (*parsedline).ip_cmp_method = ipCmpSameHost;
        } else if token_is_keyword(token, c"samenet") {
            /* Any IP on the host's subnets is allowed to connect */
            (*parsedline).ip_cmp_method = ipCmpSameNet;
        } else {
            /* IP and netmask are specified */
            (*parsedline).ip_cmp_method = ipCmpMask;

            /* need a modifiable copy of token */
            str = pstrdup((*token).string);

            /* Check if it has a CIDR suffix and if so isolate it */
            cidr_slash = strchr(str, b'/' as c_int);
            if !cidr_slash.is_null() {
                *cidr_slash = b'\0' as c_char;
            }

            /* Get the IP address either way */
            hints.ai_flags = AI_NUMERICHOST;
            hints.ai_family = AF_UNSPEC;
            hints.ai_socktype = 0;
            hints.ai_protocol = 0;
            hints.ai_addrlen = 0;
            hints.ai_canonname = core::ptr::null_mut();
            hints.ai_addr = core::ptr::null_mut();
            hints.ai_next = core::ptr::null_mut();

            gai_result = core::ptr::null_mut();
            ret = pg_getaddrinfo_all(str, core::ptr::null(), &hints, &mut gai_result);
            if ret == 0 && !gai_result.is_null() {
                memcpy_c(
                    &mut (*parsedline).addr as *mut sockaddr_storage as *mut c_void,
                    (*gai_result).ai_addr as *const c_void,
                    (*gai_result).ai_addrlen as usize,
                );
                (*parsedline).addrlen = (*gai_result).ai_addrlen as c_int;
            } else if ret == EAI_NONAME {
                (*parsedline).hostname = str;
            } else {
                // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
                ereport!(
                    elevel,
                    errmsg!(
                        "invalid IP address \"{}\": {}",
                        CStr::from_ptr(str).to_string_lossy(),
                        CStr::from_ptr(gai_strerror(ret)).to_string_lossy()
                    )
                );
                *err_msg = psprintf_stub(str);
                if !gai_result.is_null() {
                    pg_freeaddrinfo_all(hints.ai_family, gai_result);
                }
                return core::ptr::null_mut();
            }

            pg_freeaddrinfo_all(hints.ai_family, gai_result);

            /* Get the netmask */
            if !cidr_slash.is_null() {
                if !(*parsedline).hostname.is_null() {
                    // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
                    ereport!(
                        elevel,
                        errmsg!(
                            "specifying both host name and CIDR mask is invalid: \"{}\"",
                            CStr::from_ptr((*token).string).to_string_lossy()
                        )
                    );
                    *err_msg = psprintf_stub((*token).string);
                    return core::ptr::null_mut();
                }

                if pg_sockaddr_cidr_mask(
                    &mut (*parsedline).mask,
                    cidr_slash.add(1),
                    (*parsedline).addr.ss_family as c_int,
                ) < 0
                {
                    // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
                    ereport!(
                        elevel,
                        errmsg!(
                            "invalid CIDR mask in address \"{}\"",
                            CStr::from_ptr((*token).string).to_string_lossy()
                        )
                    );
                    *err_msg = psprintf_stub((*token).string);
                    return core::ptr::null_mut();
                }
                (*parsedline).masklen = (*parsedline).addrlen;
                pfree(str as *mut c_void);
            } else if (*parsedline).hostname.is_null() {
                /* Read the mask field. */
                pfree(str as *mut c_void);
                field = lnext((*tok_line).fields, field);
                if field.is_null() {
                    // C also: errcode(ERRCODE_CONFIG_FILE_ERROR),
                    //  errhint("Specify an address range in CIDR notation, or provide a separate netmask."),
                    //  errcontext(...)
                    ereport!(elevel, errmsg!("end-of-line before netmask specification"));
                    *err_msg = pstrdup(c"end-of-line before netmask specification".as_ptr());
                    return core::ptr::null_mut();
                }
                tokens = lfirst(field) as *mut List;
                if (*tokens).length > 1 {
                    // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
                    ereport!(elevel, errmsg!("multiple values specified for netmask"));
                    *err_msg = pstrdup(c"multiple values specified for netmask".as_ptr());
                    return core::ptr::null_mut();
                }
                token = linitial(tokens) as *mut AuthToken;

                gai_result = core::ptr::null_mut();
                ret = pg_getaddrinfo_all((*token).string, core::ptr::null(), &hints, &mut gai_result);
                if ret != 0 || gai_result.is_null() {
                    // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
                    ereport!(
                        elevel,
                        errmsg!(
                            "invalid IP mask \"{}\": {}",
                            CStr::from_ptr((*token).string).to_string_lossy(),
                            CStr::from_ptr(gai_strerror(ret)).to_string_lossy()
                        )
                    );
                    *err_msg = psprintf_stub((*token).string);
                    if !gai_result.is_null() {
                        pg_freeaddrinfo_all(hints.ai_family, gai_result);
                    }
                    return core::ptr::null_mut();
                }

                memcpy_c(
                    &mut (*parsedline).mask as *mut sockaddr_storage as *mut c_void,
                    (*gai_result).ai_addr as *const c_void,
                    (*gai_result).ai_addrlen as usize,
                );
                (*parsedline).masklen = (*gai_result).ai_addrlen as c_int;
                pg_freeaddrinfo_all(hints.ai_family, gai_result);

                if (*parsedline).addr.ss_family != (*parsedline).mask.ss_family {
                    // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
                    ereport!(elevel, errmsg!("IP address and mask do not match"));
                    *err_msg = pstrdup(c"IP address and mask do not match".as_ptr());
                    return core::ptr::null_mut();
                }
            }
        }
    } /* != ctLocal */

    /* Get the authentication method */
    field = lnext((*tok_line).fields, field);
    if field.is_null() {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
        ereport!(elevel, errmsg!("end-of-line before authentication method"));
        *err_msg = pstrdup(c"end-of-line before authentication method".as_ptr());
        return core::ptr::null_mut();
    }
    tokens = lfirst(field) as *mut List;
    if (*tokens).length > 1 {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR),
        //  errhint("Specify exactly one authentication type per line."), errcontext(...)
        ereport!(elevel, errmsg!("multiple values specified for authentication type"));
        *err_msg = pstrdup(c"multiple values specified for authentication type".as_ptr());
        return core::ptr::null_mut();
    }
    token = linitial(tokens) as *mut AuthToken;

    unsupauth = core::ptr::null();
    if strcmp((*token).string, c"trust".as_ptr()) == 0 {
        (*parsedline).auth_method = uaTrust;
    } else if strcmp((*token).string, c"ident".as_ptr()) == 0 {
        (*parsedline).auth_method = uaIdent;
    } else if strcmp((*token).string, c"peer".as_ptr()) == 0 {
        (*parsedline).auth_method = uaPeer;
    } else if strcmp((*token).string, c"password".as_ptr()) == 0 {
        (*parsedline).auth_method = uaPassword;
    } else if strcmp((*token).string, c"gss".as_ptr()) == 0 {
        // not ENABLE_GSS
        unsupauth = c"gss".as_ptr();
    } else if strcmp((*token).string, c"sspi".as_ptr()) == 0 {
        // not ENABLE_SSPI
        unsupauth = c"sspi".as_ptr();
    } else if strcmp((*token).string, c"reject".as_ptr()) == 0 {
        (*parsedline).auth_method = uaReject;
    } else if strcmp((*token).string, c"md5".as_ptr()) == 0 {
        (*parsedline).auth_method = uaMD5;
    } else if strcmp((*token).string, c"scram-sha-256".as_ptr()) == 0 {
        (*parsedline).auth_method = uaSCRAM;
    } else if strcmp((*token).string, c"pam".as_ptr()) == 0 {
        // not USE_PAM
        unsupauth = c"pam".as_ptr();
    } else if strcmp((*token).string, c"bsd".as_ptr()) == 0 {
        // not USE_BSD_AUTH
        unsupauth = c"bsd".as_ptr();
    } else if strcmp((*token).string, c"ldap".as_ptr()) == 0 {
        // not USE_LDAP
        unsupauth = c"ldap".as_ptr();
    } else if strcmp((*token).string, c"cert".as_ptr()) == 0 {
        // not USE_SSL
        unsupauth = c"cert".as_ptr();
    } else if strcmp((*token).string, c"radius".as_ptr()) == 0 {
        (*parsedline).auth_method = uaRADIUS;
    } else if strcmp((*token).string, c"oauth".as_ptr()) == 0 {
        (*parsedline).auth_method = uaOAuth;
    } else {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
        ereport!(
            elevel,
            errmsg!(
                "invalid authentication method \"{}\"",
                CStr::from_ptr((*token).string).to_string_lossy()
            )
        );
        *err_msg = psprintf_stub((*token).string);
        return core::ptr::null_mut();
    }

    if !unsupauth.is_null() {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
        ereport!(
            elevel,
            errmsg!(
                "invalid authentication method \"{}\": not supported by this build",
                CStr::from_ptr((*token).string).to_string_lossy()
            )
        );
        *err_msg = psprintf_stub((*token).string);
        return core::ptr::null_mut();
    }

    /*
     * XXX: When using ident on local connections, change it to peer, for
     * backwards compatibility.
     */
    if (*parsedline).conntype == ctLocal && (*parsedline).auth_method == uaIdent {
        (*parsedline).auth_method = uaPeer;
    }

    /* Invalid authentication combinations */
    if (*parsedline).conntype == ctLocal && (*parsedline).auth_method == uaGSS {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
        ereport!(elevel, errmsg!("gssapi authentication is not supported on local sockets"));
        *err_msg = pstrdup(c"gssapi authentication is not supported on local sockets".as_ptr());
        return core::ptr::null_mut();
    }

    if (*parsedline).conntype != ctLocal && (*parsedline).auth_method == uaPeer {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
        ereport!(elevel, errmsg!("peer authentication is only supported on local sockets"));
        *err_msg = pstrdup(c"peer authentication is only supported on local sockets".as_ptr());
        return core::ptr::null_mut();
    }

    /*
     * SSPI authentication can never be enabled on ctLocal connections,
     * because it's only supported on Windows, where ctLocal isn't supported.
     */

    if (*parsedline).conntype != ctHostSSL && (*parsedline).auth_method == uaCert {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
        ereport!(
            elevel,
            errmsg!("cert authentication is only supported on hostssl connections")
        );
        *err_msg = pstrdup(c"cert authentication is only supported on hostssl connections".as_ptr());
        return core::ptr::null_mut();
    }

    /*
     * For GSS and SSPI, set the default value of include_realm to true.
     * Having include_realm set to false is dangerous in multi-realm
     * situations and is generally considered bad practice.  We keep the
     * capability around for backwards compatibility, but we might want to
     * remove it at some point in the future.  Users who still need to strip
     * the realm off would be better served by using an appropriate regex in a
     * pg_ident.conf mapping.
     */
    if (*parsedline).auth_method == uaGSS || (*parsedline).auth_method == uaSSPI {
        (*parsedline).include_realm = true;
    }

    /*
     * For SSPI, include_realm defaults to the SAM-compatible domain (aka
     * NetBIOS name) and user names instead of the Kerberos principal name for
     * compatibility.
     */
    if (*parsedline).auth_method == uaSSPI {
        (*parsedline).compat_realm = true;
        (*parsedline).upn_username = false;
    }

    /* Parse remaining arguments */
    loop {
        field = lnext((*tok_line).fields, field);
        if field.is_null() {
            break;
        }
        tokens = lfirst(field) as *mut List;
        foreach!(tokencell, tokens, {
            let val: *mut c_char;

            token = lfirst(current_cell!(tokencell)) as *mut AuthToken;

            str = pstrdup((*token).string);
            val = strchr(str, b'=' as c_int);
            if val.is_null() {
                /*
                 * Got something that's not a name=value pair.
                 */
                // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
                ereport!(
                    elevel,
                    errmsg!(
                        "authentication option not in name=value format: {}",
                        CStr::from_ptr((*token).string).to_string_lossy()
                    )
                );
                *err_msg = psprintf_stub((*token).string);
                return core::ptr::null_mut();
            }

            *val = b'\0' as c_char; /* str now holds "name", val holds "value" */
            let val = val.add(1);
            if !parse_hba_auth_opt(str, val, parsedline, elevel, err_msg) {
                /* parse_hba_auth_opt already logged the error message */
                return core::ptr::null_mut();
            }
            pfree(str as *mut c_void);
        });
    }

    /*
     * Check if the selected authentication method has any mandatory arguments
     * that are not set.
     */
    if (*parsedline).auth_method == uaLDAP {
        // not HAVE_LDAP_INITIALIZE: MANDATORY_AUTH_ARG(ldapserver, "ldapserver", "ldap")
        if (*parsedline).ldapserver.is_null() {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication method \"{}\" requires argument \"{}\" to be set",
                    "ldap", "ldapserver"
                )
            );
            *err_msg =
                pstrdup(c"authentication method \"ldap\" requires argument \"ldapserver\" to be set".as_ptr());
            return core::ptr::null_mut();
        }

        /*
         * LDAP can operate in two modes: either with a direct bind, using
         * ldapprefix and ldapsuffix, or using a search+bind, using
         * ldapbasedn, ldapbinddn, ldapbindpasswd and one of
         * ldapsearchattribute or ldapsearchfilter.  Disallow mixing these
         * parameters.
         */
        if !(*parsedline).ldapprefix.is_null() || !(*parsedline).ldapsuffix.is_null() {
            if !(*parsedline).ldapbasedn.is_null()
                || !(*parsedline).ldapbinddn.is_null()
                || !(*parsedline).ldapbindpasswd.is_null()
                || !(*parsedline).ldapsearchattribute.is_null()
                || !(*parsedline).ldapsearchfilter.is_null()
            {
                // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
                ereport!(
                    elevel,
                    errmsg!("cannot mix options for simple bind and search+bind modes")
                );
                *err_msg = pstrdup(c"cannot mix options for simple bind and search+bind modes".as_ptr());
                return core::ptr::null_mut();
            }
        } else if (*parsedline).ldapbasedn.is_null() {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication method \"ldap\" requires argument \"ldapbasedn\", \"ldapprefix\", or \"ldapsuffix\" to be set"
                )
            );
            *err_msg = pstrdup(
                c"authentication method \"ldap\" requires argument \"ldapbasedn\", \"ldapprefix\", or \"ldapsuffix\" to be set"
                    .as_ptr(),
            );
            return core::ptr::null_mut();
        }

        /*
         * When using search+bind, you can either use a simple attribute
         * (defaulting to "uid") or a fully custom search filter.  You can't
         * do both.
         */
        if !(*parsedline).ldapsearchattribute.is_null() && !(*parsedline).ldapsearchfilter.is_null()
        {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!("cannot use ldapsearchattribute together with ldapsearchfilter")
            );
            *err_msg = pstrdup(c"cannot use ldapsearchattribute together with ldapsearchfilter".as_ptr());
            return core::ptr::null_mut();
        }
    }

    if (*parsedline).auth_method == uaRADIUS {
        // MANDATORY_AUTH_ARG(radiusservers, "radiusservers", "radius")
        if (*parsedline).radiusservers.is_null() {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication method \"{}\" requires argument \"{}\" to be set",
                    "radius", "radiusservers"
                )
            );
            *err_msg =
                pstrdup(c"authentication method \"radius\" requires argument \"radiusservers\" to be set".as_ptr());
            return core::ptr::null_mut();
        }
        // MANDATORY_AUTH_ARG(radiussecrets, "radiussecrets", "radius")
        if (*parsedline).radiussecrets.is_null() {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication method \"{}\" requires argument \"{}\" to be set",
                    "radius", "radiussecrets"
                )
            );
            *err_msg =
                pstrdup(c"authentication method \"radius\" requires argument \"radiussecrets\" to be set".as_ptr());
            return core::ptr::null_mut();
        }

        if (*parsedline).radiusservers == NIL {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(elevel, errmsg!("list of RADIUS servers cannot be empty"));
            *err_msg = pstrdup(c"list of RADIUS servers cannot be empty".as_ptr());
            return core::ptr::null_mut();
        }

        if (*parsedline).radiussecrets == NIL {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(elevel, errmsg!("list of RADIUS secrets cannot be empty"));
            *err_msg = pstrdup(c"list of RADIUS secrets cannot be empty".as_ptr());
            return core::ptr::null_mut();
        }

        /*
         * Verify length of option lists - each can be 0 (except for secrets,
         * but that's already checked above), 1 (use the same value
         * everywhere) or the same as the number of servers.
         */
        if !(list_length((*parsedline).radiussecrets) == 1
            || list_length((*parsedline).radiussecrets)
                == list_length((*parsedline).radiusservers))
        {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "the number of RADIUS secrets ({}) must be 1 or the same as the number of RADIUS servers ({})",
                    list_length((*parsedline).radiussecrets),
                    list_length((*parsedline).radiusservers)
                )
            );
            *err_msg = psprintf_stub(c"the number of RADIUS secrets must be 1 or the same as the number of RADIUS servers".as_ptr());
            return core::ptr::null_mut();
        }
        if !(list_length((*parsedline).radiusports) == 0
            || list_length((*parsedline).radiusports) == 1
            || list_length((*parsedline).radiusports)
                == list_length((*parsedline).radiusservers))
        {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "the number of RADIUS ports ({}) must be 1 or the same as the number of RADIUS servers ({})",
                    list_length((*parsedline).radiusports),
                    list_length((*parsedline).radiusservers)
                )
            );
            *err_msg = psprintf_stub(c"the number of RADIUS ports must be 1 or the same as the number of RADIUS servers".as_ptr());
            return core::ptr::null_mut();
        }
        if !(list_length((*parsedline).radiusidentifiers) == 0
            || list_length((*parsedline).radiusidentifiers) == 1
            || list_length((*parsedline).radiusidentifiers)
                == list_length((*parsedline).radiusservers))
        {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "the number of RADIUS identifiers ({}) must be 1 or the same as the number of RADIUS servers ({})",
                    list_length((*parsedline).radiusidentifiers),
                    list_length((*parsedline).radiusservers)
                )
            );
            *err_msg = psprintf_stub(c"the number of RADIUS identifiers must be 1 or the same as the number of RADIUS servers".as_ptr());
            return core::ptr::null_mut();
        }
    }

    /*
     * Enforce any parameters implied by other settings.
     */
    if (*parsedline).auth_method == uaCert {
        /*
         * For auth method cert, client certificate validation is mandatory,
         * and it implies the level of verify-full.
         */
        (*parsedline).clientcert = clientCertFull;
    }

    /*
     * Enforce proper configuration of OAuth authentication.
     */
    if (*parsedline).auth_method == uaOAuth {
        // MANDATORY_AUTH_ARG(oauth_scope, "scope", "oauth")
        if (*parsedline).oauth_scope.is_null() {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication method \"{}\" requires argument \"{}\" to be set",
                    "oauth", "scope"
                )
            );
            *err_msg = pstrdup(c"authentication method \"oauth\" requires argument \"scope\" to be set".as_ptr());
            return core::ptr::null_mut();
        }
        // MANDATORY_AUTH_ARG(oauth_issuer, "issuer", "oauth")
        if (*parsedline).oauth_issuer.is_null() {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication method \"{}\" requires argument \"{}\" to be set",
                    "oauth", "issuer"
                )
            );
            *err_msg = pstrdup(c"authentication method \"oauth\" requires argument \"issuer\" to be set".as_ptr());
            return core::ptr::null_mut();
        }

        /* Ensure a validator library is set and permitted by the config. */
        if !check_oauth_validator(parsedline, elevel, err_msg) {
            return core::ptr::null_mut();
        }

        /*
         * Supplying a usermap combined with the option to skip usermapping is
         * nonsensical and indicates a configuration error.
         */
        if (*parsedline).oauth_skip_usermap && !(*parsedline).usermap.is_null() {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR),
            //  errmsg("%s cannot be used in combination with %s", "map", "delegate_ident_mapping"),
            //  errcontext(...)
            ereport!(
                elevel,
                errmsg!("{} cannot be used in combination with {}", "map", "delegate_ident_mapping")
            );
            *err_msg = pstrdup(c"map cannot be used in combination with delegate_ident_mapping".as_ptr());
            return core::ptr::null_mut();
        }
    }

    parsedline
}

/*
 * Parse one name-value pair as an authentication option into the given
 * HbaLine.  Return true if we successfully parse the option, false if we
 * encounter an error.  In the event of an error, also log a message at
 * ereport level elevel, and store a message string into *err_msg.
 */
unsafe fn parse_hba_auth_opt(
    name: *mut c_char,
    val: *mut c_char,
    hbaline: *mut HbaLine,
    elevel: c_int,
    err_msg: *mut *mut c_char,
) -> bool {
    let line_num: c_int = (*hbaline).linenumber;
    let file_name: *mut c_char = (*hbaline).sourcefile;

    // not USE_LDAP: hbaline->ldapscope = LDAP_SCOPE_SUBTREE;

    if strcmp(name, c"map".as_ptr()) == 0 {
        if (*hbaline).auth_method != uaIdent
            && (*hbaline).auth_method != uaPeer
            && (*hbaline).auth_method != uaGSS
            && (*hbaline).auth_method != uaSSPI
            && (*hbaline).auth_method != uaCert
            && (*hbaline).auth_method != uaOAuth
        {
            // INVALID_AUTH_OPTION("map", gettext_noop("ident, peer, gssapi, sspi, cert, and oauth"))
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "map", "ident, peer, gssapi, sspi, cert, and oauth"
                )
            );
            *err_msg = pstrdup(c"authentication option \"map\" is only valid for authentication methods ident, peer, gssapi, sspi, cert, and oauth".as_ptr());
            return false;
        }
        (*hbaline).usermap = pstrdup(val);
    } else if strcmp(name, c"clientcert".as_ptr()) == 0 {
        if (*hbaline).conntype != ctHostSSL {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!("clientcert can only be configured for \"hostssl\" rows")
            );
            *err_msg = pstrdup(c"clientcert can only be configured for \"hostssl\" rows".as_ptr());
            return false;
        }

        if strcmp(val, c"verify-full".as_ptr()) == 0 {
            (*hbaline).clientcert = clientCertFull;
        } else if strcmp(val, c"verify-ca".as_ptr()) == 0 {
            if (*hbaline).auth_method == uaCert {
                // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
                ereport!(
                    elevel,
                    errmsg!("clientcert only accepts \"verify-full\" when using \"cert\" authentication")
                );
                *err_msg = pstrdup(c"clientcert can only be set to \"verify-full\" when using \"cert\" authentication".as_ptr());
                return false;
            }

            (*hbaline).clientcert = clientCertCA;
        } else {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "invalid value for clientcert: \"{}\"",
                    CStr::from_ptr(val).to_string_lossy()
                )
            );
            return false;
        }
    } else if strcmp(name, c"clientname".as_ptr()) == 0 {
        if (*hbaline).conntype != ctHostSSL {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!("clientname can only be configured for \"hostssl\" rows")
            );
            *err_msg = pstrdup(c"clientname can only be configured for \"hostssl\" rows".as_ptr());
            return false;
        }

        if strcmp(val, c"CN".as_ptr()) == 0 {
            (*hbaline).clientcertname = clientCertCN;
        } else if strcmp(val, c"DN".as_ptr()) == 0 {
            (*hbaline).clientcertname = clientCertDN;
        } else {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "invalid value for clientname: \"{}\"",
                    CStr::from_ptr(val).to_string_lossy()
                )
            );
            return false;
        }
    } else if strcmp(name, c"pamservice".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaPAM, "pamservice", "pam")
        if (*hbaline).auth_method != uaPAM {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "pamservice", "pam"
                )
            );
            *err_msg = pstrdup(c"authentication option \"pamservice\" is only valid for authentication methods pam".as_ptr());
            return false;
        }
        (*hbaline).pamservice = pstrdup(val);
    } else if strcmp(name, c"pam_use_hostname".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaPAM, "pam_use_hostname", "pam")
        if (*hbaline).auth_method != uaPAM {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "pam_use_hostname", "pam"
                )
            );
            *err_msg = pstrdup(c"authentication option \"pam_use_hostname\" is only valid for authentication methods pam".as_ptr());
            return false;
        }
        if strcmp(val, c"1".as_ptr()) == 0 {
            (*hbaline).pam_use_hostname = true;
        } else {
            (*hbaline).pam_use_hostname = false;
        }
    } else if strcmp(name, c"ldapurl".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaLDAP, "ldapurl", "ldap")
        if (*hbaline).auth_method != uaLDAP {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "ldapurl", "ldap"
                )
            );
            *err_msg = pstrdup(c"authentication option \"ldapurl\" is only valid for authentication methods ldap".as_ptr());
            return false;
        }
        // not OpenLDAP (LDAP_API_FEATURE_X_OPENLDAP):
        // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)
        ereport!(
            elevel,
            errmsg!("LDAP URLs not supported on this platform")
        );
        *err_msg = pstrdup(c"LDAP URLs not supported on this platform".as_ptr());
    } else if strcmp(name, c"ldaptls".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaLDAP, "ldaptls", "ldap")
        if (*hbaline).auth_method != uaLDAP {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "ldaptls", "ldap"
                )
            );
            *err_msg = pstrdup(c"authentication option \"ldaptls\" is only valid for authentication methods ldap".as_ptr());
            return false;
        }
        if strcmp(val, c"1".as_ptr()) == 0 {
            (*hbaline).ldaptls = true;
        } else {
            (*hbaline).ldaptls = false;
        }
    } else if strcmp(name, c"ldapscheme".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaLDAP, "ldapscheme", "ldap")
        if (*hbaline).auth_method != uaLDAP {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "ldapscheme", "ldap"
                )
            );
            *err_msg = pstrdup(c"authentication option \"ldapscheme\" is only valid for authentication methods ldap".as_ptr());
            return false;
        }
        if strcmp(val, c"ldap".as_ptr()) != 0 && strcmp(val, c"ldaps".as_ptr()) != 0 {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "invalid ldapscheme value: \"{}\"",
                    CStr::from_ptr(val).to_string_lossy()
                )
            );
        }
        (*hbaline).ldapscheme = pstrdup(val);
    } else if strcmp(name, c"ldapserver".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaLDAP, "ldapserver", "ldap")
        if (*hbaline).auth_method != uaLDAP {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "ldapserver", "ldap"
                )
            );
            *err_msg = pstrdup(c"authentication option \"ldapserver\" is only valid for authentication methods ldap".as_ptr());
            return false;
        }
        (*hbaline).ldapserver = pstrdup(val);
    } else if strcmp(name, c"ldapport".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaLDAP, "ldapport", "ldap")
        if (*hbaline).auth_method != uaLDAP {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "ldapport", "ldap"
                )
            );
            *err_msg = pstrdup(c"authentication option \"ldapport\" is only valid for authentication methods ldap".as_ptr());
            return false;
        }
        (*hbaline).ldapport = atoi(val);
        if (*hbaline).ldapport == 0 {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "invalid LDAP port number: \"{}\"",
                    CStr::from_ptr(val).to_string_lossy()
                )
            );
            *err_msg = psprintf_stub(val);
            return false;
        }
    } else if strcmp(name, c"ldapbinddn".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaLDAP, "ldapbinddn", "ldap")
        if (*hbaline).auth_method != uaLDAP {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "ldapbinddn", "ldap"
                )
            );
            *err_msg = pstrdup(c"authentication option \"ldapbinddn\" is only valid for authentication methods ldap".as_ptr());
            return false;
        }
        (*hbaline).ldapbinddn = pstrdup(val);
    } else if strcmp(name, c"ldapbindpasswd".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaLDAP, "ldapbindpasswd", "ldap")
        if (*hbaline).auth_method != uaLDAP {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "ldapbindpasswd", "ldap"
                )
            );
            *err_msg = pstrdup(c"authentication option \"ldapbindpasswd\" is only valid for authentication methods ldap".as_ptr());
            return false;
        }
        (*hbaline).ldapbindpasswd = pstrdup(val);
    } else if strcmp(name, c"ldapsearchattribute".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaLDAP, "ldapsearchattribute", "ldap")
        if (*hbaline).auth_method != uaLDAP {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "ldapsearchattribute", "ldap"
                )
            );
            *err_msg = pstrdup(c"authentication option \"ldapsearchattribute\" is only valid for authentication methods ldap".as_ptr());
            return false;
        }
        (*hbaline).ldapsearchattribute = pstrdup(val);
    } else if strcmp(name, c"ldapsearchfilter".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaLDAP, "ldapsearchfilter", "ldap")
        if (*hbaline).auth_method != uaLDAP {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "ldapsearchfilter", "ldap"
                )
            );
            *err_msg = pstrdup(c"authentication option \"ldapsearchfilter\" is only valid for authentication methods ldap".as_ptr());
            return false;
        }
        (*hbaline).ldapsearchfilter = pstrdup(val);
    } else if strcmp(name, c"ldapbasedn".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaLDAP, "ldapbasedn", "ldap")
        if (*hbaline).auth_method != uaLDAP {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "ldapbasedn", "ldap"
                )
            );
            *err_msg = pstrdup(c"authentication option \"ldapbasedn\" is only valid for authentication methods ldap".as_ptr());
            return false;
        }
        (*hbaline).ldapbasedn = pstrdup(val);
    } else if strcmp(name, c"ldapprefix".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaLDAP, "ldapprefix", "ldap")
        if (*hbaline).auth_method != uaLDAP {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "ldapprefix", "ldap"
                )
            );
            *err_msg = pstrdup(c"authentication option \"ldapprefix\" is only valid for authentication methods ldap".as_ptr());
            return false;
        }
        (*hbaline).ldapprefix = pstrdup(val);
    } else if strcmp(name, c"ldapsuffix".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaLDAP, "ldapsuffix", "ldap")
        if (*hbaline).auth_method != uaLDAP {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "ldapsuffix", "ldap"
                )
            );
            *err_msg = pstrdup(c"authentication option \"ldapsuffix\" is only valid for authentication methods ldap".as_ptr());
            return false;
        }
        (*hbaline).ldapsuffix = pstrdup(val);
    } else if strcmp(name, c"krb_realm".as_ptr()) == 0 {
        if (*hbaline).auth_method != uaGSS && (*hbaline).auth_method != uaSSPI {
            // INVALID_AUTH_OPTION("krb_realm", gettext_noop("gssapi and sspi"))
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "krb_realm", "gssapi and sspi"
                )
            );
            *err_msg = pstrdup(c"authentication option \"krb_realm\" is only valid for authentication methods gssapi and sspi".as_ptr());
            return false;
        }
        (*hbaline).krb_realm = pstrdup(val);
    } else if strcmp(name, c"include_realm".as_ptr()) == 0 {
        if (*hbaline).auth_method != uaGSS && (*hbaline).auth_method != uaSSPI {
            // INVALID_AUTH_OPTION("include_realm", gettext_noop("gssapi and sspi"))
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "include_realm", "gssapi and sspi"
                )
            );
            *err_msg = pstrdup(c"authentication option \"include_realm\" is only valid for authentication methods gssapi and sspi".as_ptr());
            return false;
        }
        if strcmp(val, c"1".as_ptr()) == 0 {
            (*hbaline).include_realm = true;
        } else {
            (*hbaline).include_realm = false;
        }
    } else if strcmp(name, c"compat_realm".as_ptr()) == 0 {
        if (*hbaline).auth_method != uaSSPI {
            // INVALID_AUTH_OPTION("compat_realm", gettext_noop("sspi"))
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "compat_realm", "sspi"
                )
            );
            *err_msg = pstrdup(c"authentication option \"compat_realm\" is only valid for authentication methods sspi".as_ptr());
            return false;
        }
        if strcmp(val, c"1".as_ptr()) == 0 {
            (*hbaline).compat_realm = true;
        } else {
            (*hbaline).compat_realm = false;
        }
    } else if strcmp(name, c"upn_username".as_ptr()) == 0 {
        if (*hbaline).auth_method != uaSSPI {
            // INVALID_AUTH_OPTION("upn_username", gettext_noop("sspi"))
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "upn_username", "sspi"
                )
            );
            *err_msg = pstrdup(c"authentication option \"upn_username\" is only valid for authentication methods sspi".as_ptr());
            return false;
        }
        if strcmp(val, c"1".as_ptr()) == 0 {
            (*hbaline).upn_username = true;
        } else {
            (*hbaline).upn_username = false;
        }
    } else if strcmp(name, c"radiusservers".as_ptr()) == 0 {
        let mut gai_result: *mut addrinfo;
        let mut hints: addrinfo = core::mem::zeroed();
        let mut ret: c_int;
        let mut parsed_servers: *mut List = core::ptr::null_mut();
        let dupval: *mut c_char = pstrdup(val);

        // REQUIRE_AUTH_OPTION(uaRADIUS, "radiusservers", "radius")
        if (*hbaline).auth_method != uaRADIUS {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "radiusservers", "radius"
                )
            );
            *err_msg = pstrdup(c"authentication option \"radiusservers\" is only valid for authentication methods radius".as_ptr());
            return false;
        }

        if !SplitGUCList(dupval, b',' as c_char, &mut parsed_servers) {
            /* syntax error in list */
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "could not parse RADIUS server list \"{}\"",
                    CStr::from_ptr(val).to_string_lossy()
                )
            );
            return false;
        }

        /* For each entry in the list, translate it */
        foreach!(l, parsed_servers, {
            core::ptr::write_bytes(&mut hints as *mut addrinfo, 0, 1);
            hints.ai_socktype = SOCK_DGRAM;
            hints.ai_family = AF_UNSPEC;

            gai_result = core::ptr::null_mut();
            ret = pg_getaddrinfo_all(
                lfirst(current_cell!(l)) as *mut c_char,
                core::ptr::null(),
                &hints,
                &mut gai_result,
            );
            if ret != 0 || gai_result.is_null() {
                // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
                ereport!(
                    elevel,
                    errmsg!(
                        "could not translate RADIUS server name \"{}\" to address: {}",
                        CStr::from_ptr(lfirst(current_cell!(l)) as *mut c_char).to_string_lossy(),
                        CStr::from_ptr(gai_strerror(ret)).to_string_lossy()
                    )
                );
                if !gai_result.is_null() {
                    pg_freeaddrinfo_all(hints.ai_family, gai_result);
                }

                list_free(parsed_servers);
                return false;
            }
            pg_freeaddrinfo_all(hints.ai_family, gai_result);
        });

        /* All entries are OK, so store them */
        (*hbaline).radiusservers = parsed_servers;
        (*hbaline).radiusservers_s = pstrdup(val);
    } else if strcmp(name, c"radiusports".as_ptr()) == 0 {
        let mut parsed_ports: *mut List = core::ptr::null_mut();
        let dupval: *mut c_char = pstrdup(val);

        // REQUIRE_AUTH_OPTION(uaRADIUS, "radiusports", "radius")
        if (*hbaline).auth_method != uaRADIUS {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "radiusports", "radius"
                )
            );
            *err_msg = pstrdup(c"authentication option \"radiusports\" is only valid for authentication methods radius".as_ptr());
            return false;
        }

        if !SplitGUCList(dupval, b',' as c_char, &mut parsed_ports) {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "could not parse RADIUS port list \"{}\"",
                    CStr::from_ptr(val).to_string_lossy()
                )
            );
            *err_msg = psprintf_stub(val);
            return false;
        }

        foreach!(l, parsed_ports, {
            if atoi(lfirst(current_cell!(l)) as *const c_char) == 0 {
                // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
                ereport!(
                    elevel,
                    errmsg!(
                        "invalid RADIUS port number: \"{}\"",
                        CStr::from_ptr(val).to_string_lossy()
                    )
                );

                return false;
            }
        });
        (*hbaline).radiusports = parsed_ports;
        (*hbaline).radiusports_s = pstrdup(val);
    } else if strcmp(name, c"radiussecrets".as_ptr()) == 0 {
        let mut parsed_secrets: *mut List = core::ptr::null_mut();
        let dupval: *mut c_char = pstrdup(val);

        // REQUIRE_AUTH_OPTION(uaRADIUS, "radiussecrets", "radius")
        if (*hbaline).auth_method != uaRADIUS {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "radiussecrets", "radius"
                )
            );
            *err_msg = pstrdup(c"authentication option \"radiussecrets\" is only valid for authentication methods radius".as_ptr());
            return false;
        }

        if !SplitGUCList(dupval, b',' as c_char, &mut parsed_secrets) {
            /* syntax error in list */
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "could not parse RADIUS secret list \"{}\"",
                    CStr::from_ptr(val).to_string_lossy()
                )
            );
            return false;
        }

        (*hbaline).radiussecrets = parsed_secrets;
        (*hbaline).radiussecrets_s = pstrdup(val);
    } else if strcmp(name, c"radiusidentifiers".as_ptr()) == 0 {
        let mut parsed_identifiers: *mut List = core::ptr::null_mut();
        let dupval: *mut c_char = pstrdup(val);

        // REQUIRE_AUTH_OPTION(uaRADIUS, "radiusidentifiers", "radius")
        if (*hbaline).auth_method != uaRADIUS {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "radiusidentifiers", "radius"
                )
            );
            *err_msg = pstrdup(c"authentication option \"radiusidentifiers\" is only valid for authentication methods radius".as_ptr());
            return false;
        }

        if !SplitGUCList(dupval, b',' as c_char, &mut parsed_identifiers) {
            /* syntax error in list */
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "could not parse RADIUS identifiers list \"{}\"",
                    CStr::from_ptr(val).to_string_lossy()
                )
            );
            return false;
        }

        (*hbaline).radiusidentifiers = parsed_identifiers;
        (*hbaline).radiusidentifiers_s = pstrdup(val);
    } else if strcmp(name, c"issuer".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaOAuth, "issuer", "oauth")
        if (*hbaline).auth_method != uaOAuth {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "issuer", "oauth"
                )
            );
            *err_msg = pstrdup(c"authentication option \"issuer\" is only valid for authentication methods oauth".as_ptr());
            return false;
        }
        (*hbaline).oauth_issuer = pstrdup(val);
    } else if strcmp(name, c"scope".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaOAuth, "scope", "oauth")
        if (*hbaline).auth_method != uaOAuth {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "scope", "oauth"
                )
            );
            *err_msg = pstrdup(c"authentication option \"scope\" is only valid for authentication methods oauth".as_ptr());
            return false;
        }
        (*hbaline).oauth_scope = pstrdup(val);
    } else if strcmp(name, c"validator".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaOAuth, "validator", "oauth")
        if (*hbaline).auth_method != uaOAuth {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "validator", "oauth"
                )
            );
            *err_msg = pstrdup(c"authentication option \"validator\" is only valid for authentication methods oauth".as_ptr());
            return false;
        }
        (*hbaline).oauth_validator = pstrdup(val);
    } else if strcmp(name, c"delegate_ident_mapping".as_ptr()) == 0 {
        // REQUIRE_AUTH_OPTION(uaOAuth, "delegate_ident_mapping", "oauth")
        if (*hbaline).auth_method != uaOAuth {
            // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
            ereport!(
                elevel,
                errmsg!(
                    "authentication option \"{}\" is only valid for authentication methods {}",
                    "delegate_ident_mapping", "oauth"
                )
            );
            *err_msg = pstrdup(c"authentication option \"delegate_ident_mapping\" is only valid for authentication methods oauth".as_ptr());
            return false;
        }
        if strcmp(val, c"1".as_ptr()) == 0 {
            (*hbaline).oauth_skip_usermap = true;
        } else {
            (*hbaline).oauth_skip_usermap = false;
        }
    } else {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
        ereport!(
            elevel,
            errmsg!(
                "unrecognized authentication option name: \"{}\"",
                CStr::from_ptr(name).to_string_lossy()
            )
        );
        *err_msg = psprintf_stub(name);
        return false;
    }
    true
}

/*
 *	Scan the pre-parsed hba file, looking for a match to the port's connection
 *	request.
 */
unsafe fn check_hba(port: *mut hbaPort) {
    let roleid: Oid;
    let mut hba: *mut HbaLine;

    /* Get the target role's OID.  Note we do not error out for bad role. */
    roleid = get_role_oid((*port).user_name, true);

    foreach!(line, parsed_hba_lines, {
        hba = lfirst(current_cell!(line)) as *mut HbaLine;

        /* Check connection type */
        if (*hba).conntype == ctLocal {
            if (*port).raddr.addr.ss_family as c_int != AF_UNIX {
                continue;
            }
        } else {
            if (*port).raddr.addr.ss_family as c_int == AF_UNIX {
                continue;
            }

            /* Check SSL state */
            if (*port).ssl_in_use {
                /* Connection is SSL, match both "host" and "hostssl" */
                if (*hba).conntype == ctHostNoSSL {
                    continue;
                }
            } else {
                /* Connection is not SSL, match both "host" and "hostnossl" */
                if (*hba).conntype == ctHostSSL {
                    continue;
                }
            }

            /* Check GSSAPI state */
            // not ENABLE_GSS in this build:
            if (*hba).conntype == ctHostGSS {
                continue;
            }

            /* Check IP address */
            if (*hba).ip_cmp_method == ipCmpMask {
                if !(*hba).hostname.is_null() {
                    if !check_hostname(port, (*hba).hostname) {
                        continue;
                    }
                } else if !check_ip(
                    &mut (*port).raddr,
                    &mut (*hba).addr as *mut sockaddr_storage as *mut sockaddr,
                    &mut (*hba).mask as *mut sockaddr_storage as *mut sockaddr,
                ) {
                    continue;
                }
            } else if (*hba).ip_cmp_method == ipCmpAll {
                // break: nothing to check
            } else if (*hba).ip_cmp_method == ipCmpSameHost
                || (*hba).ip_cmp_method == ipCmpSameNet
            {
                if !check_same_host_or_net(&mut (*port).raddr, (*hba).ip_cmp_method) {
                    continue;
                }
            } else {
                /* shouldn't get here, but deem it no-match if so */
                continue;
            }
        } /* != ctLocal */

        /* Check database and role */
        if !check_db(
            (*port).database_name,
            (*port).user_name,
            roleid,
            (*hba).databases,
        ) {
            continue;
        }

        if !check_role((*port).user_name, roleid, (*hba).roles, false) {
            continue;
        }

        /* Found a record that matched! */
        (*port).hba = hba;
        return;
    });

    /* If no matching entry was found, then implicitly reject. */
    hba = palloc0(core::mem::size_of::<HbaLine>()) as *mut HbaLine;
    (*hba).auth_method = uaImplicitReject;
    (*port).hba = hba;
}

/*
 * Read the config file and create a List of HbaLine records for the contents.
 *
 * The configuration is read into a temporary list, and if any parse error
 * occurs the old list is kept in place and false is returned.  Only if the
 * whole file parses OK is the list replaced, and the function returns true.
 *
 * On a false result, caller will take care of reporting a FATAL error in case
 * this is the initial startup.  If it happens on reload, we just keep running
 * with the old data.
 */
pub unsafe fn load_hba() -> bool {
    let file: *mut FILE;
    let mut hba_lines: *mut List = NIL;
    let mut ok: bool = true;
    let oldcxt: MemoryContext;
    let hbacxt: MemoryContext;
    let mut new_parsed_lines: *mut List = NIL;

    file = open_auth_file(HbaFileName, LOG, 0, core::ptr::null_mut());
    if file.is_null() {
        /* error already logged */
        return false;
    }

    tokenize_auth_file(HbaFileName, file, &mut hba_lines, LOG, 0);

    /* Now parse all the lines */
    Assert!(!PostmasterContext.is_null());
    hbacxt = AllocSetContextCreate!(
        PostmasterContext,
        c"hba parser context".as_ptr(),
        ALLOCSET_SMALL_SIZES
    );
    oldcxt = MemoryContextSwitchTo(hbacxt);
    foreach!(line, hba_lines, {
        let tok_line: *mut TokenizedAuthLine =
            lfirst(current_cell!(line)) as *mut TokenizedAuthLine;
        let newline: *mut HbaLine;

        /* don't parse lines that already have errors */
        if !(*tok_line).err_msg.is_null() {
            ok = false;
            continue;
        }

        newline = parse_hba_line(tok_line, LOG);
        if newline.is_null() {
            /* Parse error; remember there's trouble */
            ok = false;

            /*
             * Keep parsing the rest of the file so we can report errors on
             * more than the first line.  Error has already been logged, no
             * need for more chatter here.
             */
            continue;
        }

        new_parsed_lines = lappend(new_parsed_lines, newline as *mut c_void);
    });

    /*
     * A valid HBA file must have at least one entry; else there's no way to
     * connect to the postmaster.  But only complain about this if we didn't
     * already have parsing errors.
     */
    if ok && new_parsed_lines == NIL {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR)
        ereport!(
            LOG,
            errmsg!(
                "configuration file \"{}\" contains no entries",
                CStr::from_ptr(HbaFileName).to_string_lossy()
            )
        );
        ok = false;
    }

    /* Free tokenizer memory */
    free_auth_file(file, 0);
    MemoryContextSwitchTo(oldcxt);

    if !ok {
        /*
         * File contained one or more errors, so bail out. MemoryContextDelete
         * is enough to clean up everything, including regexes.
         */
        MemoryContextDelete(hbacxt);
        return false;
    }

    /* Loaded new file successfully, replace the one we use */
    if !parsed_hba_context.is_null() {
        MemoryContextDelete(parsed_hba_context);
    }
    parsed_hba_context = hbacxt;
    parsed_hba_lines = new_parsed_lines;

    true
}

/*
 * Parse one tokenised line from the ident config file and store the result in
 * an IdentLine structure.
 *
 * If parsing fails, log a message at ereport level elevel, store an error
 * string in tok_line->err_msg and return NULL.
 *
 * If ident_user is a regular expression (ie. begins with a slash), it is
 * compiled and stored in IdentLine structure.
 *
 * Note: this function leaks memory when an error occurs.  Caller is expected
 * to have set a memory context that will be reset if this function returns
 * NULL.
 */
pub unsafe fn parse_ident_line(
    tok_line: *mut TokenizedAuthLine,
    elevel: c_int,
) -> *mut IdentLine {
    let line_num: c_int = (*tok_line).line_num;
    let file_name: *mut c_char = (*tok_line).file_name;
    let err_msg: *mut *mut c_char = &mut (*tok_line).err_msg;
    let mut field: *mut ListCell;
    let mut tokens: *mut List;
    let mut token: *mut AuthToken;
    let parsedline: *mut IdentLine;

    Assert!((*tok_line).fields != NIL);
    field = list_head((*tok_line).fields);

    parsedline = palloc0(core::mem::size_of::<IdentLine>()) as *mut IdentLine;
    (*parsedline).linenumber = line_num;

    /* Get the map token (must exist) */
    tokens = lfirst(field) as *mut List;
    // IDENT_MULTI_VALUE(tokens)
    if (*tokens).length > 1 {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
        ereport!(elevel, errmsg!("multiple values in ident field"));
        *err_msg = pstrdup(c"multiple values in ident field".as_ptr());
        return core::ptr::null_mut();
    }
    token = linitial(tokens) as *mut AuthToken;
    (*parsedline).usermap = pstrdup((*token).string);

    /* Get the ident user token */
    field = lnext((*tok_line).fields, field);
    // IDENT_FIELD_ABSENT(field)
    if field.is_null() {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
        ereport!(elevel, errmsg!("missing entry at end of line"));
        *err_msg = pstrdup(c"missing entry at end of line".as_ptr());
        return core::ptr::null_mut();
    }
    tokens = lfirst(field) as *mut List;
    // IDENT_MULTI_VALUE(tokens)
    if (*tokens).length > 1 {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
        ereport!(elevel, errmsg!("multiple values in ident field"));
        *err_msg = pstrdup(c"multiple values in ident field".as_ptr());
        return core::ptr::null_mut();
    }
    token = linitial(tokens) as *mut AuthToken;

    /* Copy the ident user token */
    (*parsedline).system_user = copy_auth_token(token);

    /* Get the PG rolename token */
    field = lnext((*tok_line).fields, field);
    // IDENT_FIELD_ABSENT(field)
    if field.is_null() {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
        ereport!(elevel, errmsg!("missing entry at end of line"));
        *err_msg = pstrdup(c"missing entry at end of line".as_ptr());
        return core::ptr::null_mut();
    }
    tokens = lfirst(field) as *mut List;
    // IDENT_MULTI_VALUE(tokens)
    if (*tokens).length > 1 {
        // C also: errcode(ERRCODE_CONFIG_FILE_ERROR), errcontext(...)
        ereport!(elevel, errmsg!("multiple values in ident field"));
        *err_msg = pstrdup(c"multiple values in ident field".as_ptr());
        return core::ptr::null_mut();
    }
    token = linitial(tokens) as *mut AuthToken;
    (*parsedline).pg_user = copy_auth_token(token);

    /*
     * Now that the field validation is done, compile a regex from the user
     * tokens, if necessary.
     */
    if regcomp_auth_token((*parsedline).system_user, file_name, line_num, err_msg, elevel) != 0 {
        /* err_msg includes the error to report */
        return core::ptr::null_mut();
    }

    if regcomp_auth_token((*parsedline).pg_user, file_name, line_num, err_msg, elevel) != 0 {
        /* err_msg includes the error to report */
        return core::ptr::null_mut();
    }

    parsedline
}

/*
 *	Process one line from the parsed ident config lines.
 *
 *	Compare input parsed ident line to the needed map, pg_user and system_user.
 *	*found_p and *error_p are set according to our results.
 */
unsafe fn check_ident_usermap(
    identLine: *mut IdentLine,
    usermap_name: *const c_char,
    pg_user: *const c_char,
    system_user: *const c_char,
    case_insensitive: bool,
    found_p: *mut bool,
    error_p: *mut bool,
) {
    let roleid: Oid;

    *found_p = false;
    *error_p = false;

    if strcmp((*identLine).usermap, usermap_name) != 0 {
        /* Line does not match the map name we're looking for, so just abort */
        return;
    }

    /* Get the target role's OID.  Note we do not error out for bad role. */
    roleid = get_role_oid(pg_user, true);

    /* Match? */
    if token_has_regexp((*identLine).system_user) {
        /*
         * Process the system username as a regular expression that returns
         * exactly one match. This is replaced for \1 in the database username
         * string, if present.
         */
        let r: c_int;
        let mut matches: [regmatch_t; 2] = core::mem::zeroed();
        let ofs: *mut c_char;
        let expanded_pg_user_token: *mut AuthToken;
        let mut created_temporary_token: bool = false;

        r = regexec_auth_token(system_user, (*identLine).system_user, 2, matches.as_mut_ptr());
        if r != 0 {
            let mut errstr: [c_char; 100] = [0; 100];

            if r != REG_NOMATCH {
                /* REG_NOMATCH is not an error, everything else is */
                pg_regerror(
                    r,
                    (*(*identLine).system_user).regex,
                    errstr.as_mut_ptr(),
                    100,
                );
                // C also: errcode(ERRCODE_INVALID_REGULAR_EXPRESSION)
                ereport!(
                    LOG,
                    errmsg!(
                        "regular expression match for \"{}\" failed: {}",
                        CStr::from_ptr((*(*identLine).system_user).string.add(1)).to_string_lossy(),
                        CStr::from_ptr(errstr.as_ptr()).to_string_lossy()
                    )
                );
                *error_p = true;
            }
            return;
        }

        /*
         * Replace \1 with the first captured group unless the field already
         * has some special meaning, like a group membership or a regexp-based
         * check.
         */
        ofs = if !token_is_member_check((*identLine).pg_user)
            && !token_has_regexp((*identLine).pg_user)
        {
            strstr((*(*identLine).pg_user).string, c"\\1".as_ptr())
        } else {
            core::ptr::null_mut()
        };
        if !token_is_member_check((*identLine).pg_user)
            && !token_has_regexp((*identLine).pg_user)
            && !ofs.is_null()
        {
            let expanded_pg_user: *mut c_char;
            let offset: c_int;

            /* substitution of the first argument requested */
            if matches[1].rm_so < 0 {
                // C also: errcode(ERRCODE_INVALID_REGULAR_EXPRESSION)
                ereport!(
                    LOG,
                    errmsg!(
                        "regular expression \"{}\" has no subexpressions as requested by backreference in \"{}\"",
                        CStr::from_ptr((*(*identLine).system_user).string.add(1)).to_string_lossy(),
                        CStr::from_ptr((*(*identLine).pg_user).string).to_string_lossy()
                    )
                );
                *error_p = true;
                return;
            }

            /*
             * length: original length minus length of \1 plus length of match
             * plus null terminator
             */
            expanded_pg_user = palloc0(
                strlen((*(*identLine).pg_user).string) - 2
                    + (matches[1].rm_eo - matches[1].rm_so) as usize
                    + 1,
            ) as *mut c_char;
            offset = (ofs as isize - (*(*identLine).pg_user).string as isize) as c_int;
            memcpy_c(
                expanded_pg_user as *mut c_void,
                (*(*identLine).pg_user).string as *const c_void,
                offset as usize,
            );
            memcpy_c(
                expanded_pg_user.add(offset as usize) as *mut c_void,
                system_user.add(matches[1].rm_so as usize) as *const c_void,
                (matches[1].rm_eo - matches[1].rm_so) as usize,
            );
            strcat(expanded_pg_user, ofs.add(2));

            /*
             * Mark the token as quoted, so it will only be compared literally
             * and not for some special meaning, such as "all" or a group
             * membership check.
             */
            expanded_pg_user_token = make_auth_token(expanded_pg_user, true);
            created_temporary_token = true;
            pfree(expanded_pg_user as *mut c_void);
        } else {
            expanded_pg_user_token = (*identLine).pg_user;
        }

        /* check the Postgres user */
        *found_p = check_role(
            pg_user,
            roleid,
            list_make1!(expanded_pg_user_token as *mut c_void),
            case_insensitive,
        );

        if created_temporary_token {
            free_auth_token(expanded_pg_user_token);
        }

        return;
    } else {
        /*
         * Not a regular expression, so make a complete match.  If the system
         * user does not match, just leave.
         */
        if case_insensitive {
            if !token_matches_insensitive((*identLine).system_user, system_user) {
                return;
            }
        } else if !token_matches((*identLine).system_user, system_user) {
            return;
        }

        /* check the Postgres user */
        *found_p = check_role(
            pg_user,
            roleid,
            list_make1!((*identLine).pg_user as *mut c_void),
            case_insensitive,
        );
    }
}

/*
 *	Scan the (pre-parsed) ident usermap file line by line, looking for a match
 *
 *	See if the system user with ident username "system_user" is allowed to act as
 *	Postgres user "pg_user" according to usermap "usermap_name".
 *
 *	Special case: Usermap NULL, equivalent to what was previously called
 *	"sameuser" or "samerole", means don't look in the usermap file.
 *	That's an implied map wherein "pg_user" must be identical to
 *	"system_user" in order to be authorized.
 *
 *	Iff authorized, return STATUS_OK, otherwise return STATUS_ERROR.
 */
pub unsafe fn check_usermap(
    usermap_name: *const c_char,
    pg_user: *const c_char,
    system_user: *const c_char,
    case_insensitive: bool,
) -> c_int {
    let mut found_entry: bool = false;
    let mut error: bool = false;

    if usermap_name.is_null() || *usermap_name == b'\0' as c_char {
        if case_insensitive {
            if pg_strcasecmp(pg_user, system_user) == 0 {
                return STATUS_OK;
            }
        } else if strcmp(pg_user, system_user) == 0 {
            return STATUS_OK;
        }
        ereport!(
            LOG,
            errmsg!(
                "provided user name ({}) and authenticated user name ({}) do not match",
                CStr::from_ptr(pg_user).to_string_lossy(),
                CStr::from_ptr(system_user).to_string_lossy()
            )
        );
        return STATUS_ERROR;
    } else {
        foreach!(line_cell, parsed_ident_lines, {
            check_ident_usermap(
                lfirst(current_cell!(line_cell)) as *mut IdentLine,
                usermap_name,
                pg_user,
                system_user,
                case_insensitive,
                &mut found_entry,
                &mut error,
            );
            if found_entry || error {
                break;
            }
        });
    }
    if !found_entry && !error {
        ereport!(
            LOG,
            errmsg!(
                "no match in usermap \"{}\" for user \"{}\" authenticated as \"{}\"",
                CStr::from_ptr(usermap_name).to_string_lossy(),
                CStr::from_ptr(pg_user).to_string_lossy(),
                CStr::from_ptr(system_user).to_string_lossy()
            )
        );
    }
    if found_entry {
        STATUS_OK
    } else {
        STATUS_ERROR
    }
}

/*
 * Read the ident config file and create a List of IdentLine records for
 * the contents.
 *
 * This works the same as load_hba(), but for the user config file.
 */
pub unsafe fn load_ident() -> bool {
    let file: *mut FILE;
    let mut ident_lines: *mut List = NIL;
    let mut ok: bool = true;
    let oldcxt: MemoryContext;
    let ident_context: MemoryContext;
    let mut newline: *mut IdentLine;
    let mut new_parsed_lines: *mut List = NIL;

    /* not FATAL ... we just won't do any special ident maps */
    file = open_auth_file(IdentFileName, LOG, 0, core::ptr::null_mut());
    if file.is_null() {
        /* error already logged */
        return false;
    }

    tokenize_auth_file(IdentFileName, file, &mut ident_lines, LOG, 0);

    /* Now parse all the lines */
    Assert!(!PostmasterContext.is_null());
    ident_context = AllocSetContextCreate!(
        PostmasterContext,
        c"ident parser context".as_ptr(),
        ALLOCSET_SMALL_SIZES
    );
    oldcxt = MemoryContextSwitchTo(ident_context);
    foreach!(line_cell, ident_lines, {
        let tok_line: *mut TokenizedAuthLine =
            lfirst(current_cell!(line_cell)) as *mut TokenizedAuthLine;

        /* don't parse lines that already have errors */
        if !(*tok_line).err_msg.is_null() {
            ok = false;
            continue;
        }

        newline = parse_ident_line(tok_line, LOG);
        if newline.is_null() {
            /* Parse error; remember there's trouble */
            ok = false;

            /*
             * Keep parsing the rest of the file so we can report errors on
             * more than the first line.  Error has already been logged, no
             * need for more chatter here.
             */
            continue;
        }

        new_parsed_lines = lappend(new_parsed_lines, newline as *mut c_void);
    });

    /* Free tokenizer memory */
    free_auth_file(file, 0);
    MemoryContextSwitchTo(oldcxt);

    if !ok {
        /*
         * File contained one or more errors, so bail out. MemoryContextDelete
         * is enough to clean up everything, including regexes.
         */
        MemoryContextDelete(ident_context);
        return false;
    }

    /* Loaded new file successfully, replace the one we use */
    if !parsed_ident_context.is_null() {
        MemoryContextDelete(parsed_ident_context);
    }

    parsed_ident_context = ident_context;
    parsed_ident_lines = new_parsed_lines;

    true
}

/*
 *	Determine what authentication method should be used when accessing database
 *	"database" from frontend "raddr", user "user".  Return the method and
 *	an optional argument (stored in fields of *port), and STATUS_OK.
 *
 *	If the file does not contain any entry matching the request, we return
 *	method = uaImplicitReject.
 */
pub unsafe fn hba_getauthmethod(port: *mut hbaPort) {
    check_hba(port);
}

/*
 * Return the name of the auth method in use ("gss", "md5", "trust", etc.).
 *
 * The return value is statically allocated (see the UserAuthName array) and
 * should not be freed.
 */
pub unsafe fn hba_authname(auth_method: UserAuth) -> *const c_char {
    UserAuthName[auth_method as usize].as_ptr()
}
