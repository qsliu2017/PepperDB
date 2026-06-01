//! be-secure-openssl.c - functions for OpenSSL support in the backend.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/libpq/be-secure-openssl.c

use crate::prelude::*;

use std::ffi::CStr;

use crate::common::string::pg_clean_ascii;
use crate::lib::stringinfo::{
    appendStringInfoChar, appendStringInfoString, initStringInfo, StringInfoData,
};
use crate::libpq::be_secure::{secure_raw_read, secure_raw_write};
use crate::libpq::be_secure_common::{check_ssl_key_file_permissions, run_ssl_passphrase_command};
use crate::libpq::libpq_be::{
    openssl_tls_init_hook, Port, FILE_DH2048, SSL, SSL_CTX, X509,
};
use crate::mb::pg_wchar::{pg_any_to_server, PG_UTF8};
use crate::storage::ipc::latch::{
    WaitLatchOrSocket, WL_EXIT_ON_PM_DEATH, WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE,
};
use crate::utils::elog::COMMERROR;

use crate::libpq::be_secure::{
    ssl_ca_file, ssl_cert_file, ssl_crl_dir, ssl_crl_file, ssl_dh_params_file, ssl_key_file,
    ssl_loaded_verify_locations, ssl_max_protocol_version, ssl_min_protocol_version,
    ssl_passphrase_command, ssl_passphrase_command_supports_reload, SSLCipherList,
    SSLCipherSuites, SSLECDHCurve, SSLPreferServerCiphers,
};
use crate::libpq::libpq::{
    PG_TLS1_1_VERSION, PG_TLS1_2_VERSION, PG_TLS1_3_VERSION, PG_TLS1_VERSION, PG_TLS_ANY,
};

// ------------------------------------------------------------
//   OpenSSL FFI bindings (system <openssl/*.h>, not ported)
// ------------------------------------------------------------
//
// All of these are opaque to the Rust side; we only ever pass the pointers
// straight back into libssl/libcrypto.  We give them distinct c_void aliases so
// the function signatures read like the C originals.

pub type BIO = c_void;
pub type BIO_METHOD = c_void;
pub type X509_NAME = c_void;
pub type X509_NAME_ENTRY = c_void;
pub type X509_STORE = c_void;
pub type X509_STORE_CTX = c_void;
pub type ASN1_STRING = c_void;
pub type ASN1_INTEGER = c_void;
pub type ASN1_OBJECT = c_void;
pub type BIGNUM = c_void;
pub type DH = c_void;
pub type EVP_MD = c_void;
pub type SSL_METHOD = c_void;

/// `BUF_MEM` from <openssl/buffer.h>.  Only the first two fields are touched.
#[repr(C)]
pub struct BUF_MEM {
    pub length: usize,
    pub data: *mut c_char,
    pub max: usize,
    pub flags: c_ulong,
}

// Callback function-pointer typedefs used by the SSL setters.
pub type pem_password_cb =
    Option<unsafe extern "C" fn(buf: *mut c_char, size: c_int, rwflag: c_int, userdata: *mut c_void) -> c_int>;
pub type SSL_verify_cb =
    Option<unsafe extern "C" fn(preverify_ok: c_int, x509_ctx: *mut X509_STORE_CTX) -> c_int>;
pub type info_callback =
    Option<unsafe extern "C" fn(ssl: *const SSL, r#type: c_int, val: c_int)>;
pub type alpn_select_cb = Option<
    unsafe extern "C" fn(
        ssl: *mut SSL,
        out: *mut *const c_uchar,
        outlen: *mut c_uchar,
        r#in: *const c_uchar,
        inlen: c_uint,
        arg: *mut c_void,
    ) -> c_int,
>;
pub type bio_method_write = Option<unsafe extern "C" fn(h: *mut BIO, buf: *const c_char, size: c_int) -> c_int>;
pub type bio_method_read = Option<unsafe extern "C" fn(h: *mut BIO, buf: *mut c_char, size: c_int) -> c_int>;
pub type bio_method_ctrl =
    Option<unsafe extern "C" fn(h: *mut BIO, cmd: c_int, num: c_long, ptr: *mut c_void) -> c_long>;

// SSL mode / option / verify / cache flags (values from OpenSSL headers).
const SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER: c_long = 0x00000002;
const SSL_OP_NO_TICKET: u64 = 0x00004000;
const SSL_OP_NO_COMPRESSION: u64 = 0x00020000;
const SSL_OP_NO_RENEGOTIATION: u64 = 0x40000000;
const SSL_OP_NO_CLIENT_RENEGOTIATION: u64 = 0x0;
const SSL_OP_CIPHER_SERVER_PREFERENCE: u64 = 0x00400000;
const SSL_OP_SINGLE_DH_USE: u64 = 0x0;
const SSL_SESS_CACHE_OFF: c_long = 0x0000;
const SSL_VERIFY_PEER: c_int = 0x01;
const SSL_VERIFY_CLIENT_ONCE: c_int = 0x04;
const SSL_FILETYPE_PEM: c_int = 1;

// SSL_get_error() result codes.
const SSL_ERROR_NONE: c_int = 0;
const SSL_ERROR_SSL: c_int = 1;
const SSL_ERROR_WANT_READ: c_int = 2;
const SSL_ERROR_WANT_WRITE: c_int = 3;
const SSL_ERROR_ZERO_RETURN: c_int = 6;
const SSL_ERROR_SYSCALL: c_int = 5;

// ALPN selection / negotiation result codes.
const SSL_TLSEXT_ERR_OK: c_int = 0;
const SSL_TLSEXT_ERR_ALERT_FATAL: c_int = 2;
const SSL_TLSEXT_ERR_NOACK: c_int = 3;
const OPENSSL_NPN_NEGOTIATED: c_int = 1;

// info_cb callback type bits.
const SSL_CB_HANDSHAKE_START: c_int = 0x10;
const SSL_CB_HANDSHAKE_DONE: c_int = 0x20;
const SSL_CB_LOOP: c_int = 0x01;
const SSL_CB_EXIT: c_int = 0x02;
const SSL_CB_READ: c_int = 0x04;
const SSL_CB_WRITE: c_int = 0x08;
const SSL_CB_ALERT: c_int = 0x4000;
const SSL_ST_ACCEPT: c_int = 0x2000;
const SSL_ST_CONNECT: c_int = 0x1000;
const SSL_CB_ACCEPT_LOOP: c_int = SSL_ST_ACCEPT | SSL_CB_LOOP;
const SSL_CB_ACCEPT_EXIT: c_int = SSL_ST_ACCEPT | SSL_CB_EXIT;
const SSL_CB_CONNECT_LOOP: c_int = SSL_ST_CONNECT | SSL_CB_LOOP;
const SSL_CB_CONNECT_EXIT: c_int = SSL_ST_CONNECT | SSL_CB_EXIT;
const SSL_CB_READ_ALERT: c_int = SSL_CB_ALERT | SSL_CB_READ;
const SSL_CB_WRITE_ALERT: c_int = SSL_CB_ALERT | SSL_CB_WRITE;

// BIO flags / control codes.
const BIO_TYPE_SOURCE_SINK: c_int = 0x0400;
const BIO_CTRL_EOF: c_int = 2;
const BIO_CTRL_FLUSH: c_int = 11;
const BIO_CLOSE: c_long = 0x01;

// X509 store / verify flags.
const X509_V_FLAG_CRL_CHECK: c_ulong = 0x4;
const X509_V_FLAG_CRL_CHECK_ALL: c_ulong = 0x8;

// DH_check result bits.
const DH_CHECK_P_NOT_PRIME: c_int = 0x01;
const DH_CHECK_P_NOT_SAFE_PRIME: c_int = 0x02;
const DH_NOT_SUITABLE_GENERATOR: c_int = 0x08;

// ASN1 string-printing flags.
const ASN1_STRFLGS_ESC_MSB: c_ulong = 4;
const ASN1_STRFLGS_UTF8_CONVERT: c_ulong = 0x10;
const ASN1_STRFLGS_RFC2253: c_ulong = ASN1_STRFLGS_ESC_MSB
    | 1 /* ESC_2253 */
    | 2 /* ESC_CTRL */
    | 0x10 /* UTF8_CONVERT */
    | 0x100 /* DUMP_UNKNOWN */
    | 0x200 /* DUMP_DER */
    | 0x800 /* SEP_COMMA_PLUS */;

// X509_NAME_print_ex flags.
const XN_FLAG_RFC2253: c_ulong = 0x10254 | (1 << 23) | (2 << 16);

// NID constants for the fields we look up.
const NID_undef: c_int = 0;
const NID_commonName: c_int = 13;
const NID_md5: c_int = 4;
const NID_sha1: c_int = 64;

// OpenSSL protocol version numbers.
const TLS1_VERSION: c_int = 0x0301;
const TLS1_1_VERSION: c_int = 0x0302;
const TLS1_2_VERSION: c_int = 0x0303;
const TLS1_3_VERSION: c_int = 0x0304;

const EVP_MAX_MD_SIZE: usize = 64;

// ERR_GET_REASON() reason codes for the protocol-version hint.
const SSL_R_NO_PROTOCOLS_AVAILABLE: c_int = 181;
const SSL_R_UNSUPPORTED_PROTOCOL: c_int = 258;
const SSL_R_BAD_PROTOCOL_VERSION_NUMBER: c_int = 182;
const SSL_R_UNKNOWN_PROTOCOL: c_int = 252;
const SSL_R_UNKNOWN_SSL_VERSION: c_int = 254;
const SSL_R_UNSUPPORTED_SSL_VERSION: c_int = 259;
const SSL_R_WRONG_SSL_VERSION: c_int = 266;
const SSL_R_WRONG_VERSION_NUMBER: c_int = 267;
const SSL_R_TLSV1_ALERT_PROTOCOL_VERSION: c_int = 1070;
const SSL_R_VERSION_TOO_HIGH: c_int = 274;
const SSL_R_VERSION_TOO_LOW: c_int = 273;

// See pqcomm.h comments on OpenSSL implementation of ALPN (RFC 7301).
// PG_ALPN_PROTOCOL is "postgresql"; the vector form is the length-prefixed
// wire encoding.
const PG_ALPN_PROTOCOL: &CStr = c"postgresql";
const PG_ALPN_PROTOCOL_VECTOR: &[u8] = b"\x0apostgresql";

// pqcomm.h: bounding TLS version strings used in the protocol hint.
const MIN_OPENSSL_TLS_VERSION: &CStr = c"TLSv1.2";
const MAX_OPENSSL_TLS_VERSION: &CStr = c"TLSv1.3";

#[allow(improper_ctypes)]
extern "C" {
    // libssl
    fn SSLv23_method() -> *const SSL_METHOD;
    fn SSL_CTX_new(meth: *const SSL_METHOD) -> *mut SSL_CTX;
    fn SSL_CTX_free(ctx: *mut SSL_CTX);
    fn SSL_CTX_ctrl(ctx: *mut SSL_CTX, cmd: c_int, larg: c_long, parg: *mut c_void) -> c_long;
    fn SSL_CTX_set_options(ctx: *mut SSL_CTX, op: u64) -> u64;
    fn SSL_CTX_set_num_tickets(ctx: *mut SSL_CTX, num: usize) -> c_int;
    fn SSL_CTX_use_certificate_chain_file(ctx: *mut SSL_CTX, file: *const c_char) -> c_int;
    fn SSL_CTX_use_PrivateKey_file(ctx: *mut SSL_CTX, file: *const c_char, r#type: c_int) -> c_int;
    fn SSL_CTX_check_private_key(ctx: *mut SSL_CTX) -> c_int;
    fn SSL_CTX_set_cipher_list(ctx: *mut SSL_CTX, str: *const c_char) -> c_int;
    fn SSL_CTX_set_ciphersuites(ctx: *mut SSL_CTX, str: *const c_char) -> c_int;
    fn SSL_CTX_load_verify_locations(
        ctx: *mut SSL_CTX,
        cafile: *const c_char,
        capath: *const c_char,
    ) -> c_int;
    fn SSL_load_client_CA_file(file: *const c_char) -> *mut c_void;
    fn SSL_CTX_set_client_CA_list(ctx: *mut SSL_CTX, list: *mut c_void);
    fn SSL_CTX_set_verify(ctx: *mut SSL_CTX, mode: c_int, cb: SSL_verify_cb);
    fn SSL_CTX_get_cert_store(ctx: *const SSL_CTX) -> *mut X509_STORE;
    fn SSL_CTX_set_info_callback(ctx: *mut SSL_CTX, cb: info_callback);
    fn SSL_CTX_set_alpn_select_cb(ctx: *mut SSL_CTX, cb: alpn_select_cb, arg: *mut c_void);
    fn SSL_CTX_set_default_passwd_cb(ctx: *mut SSL_CTX, cb: pem_password_cb);
    fn SSL_new(ctx: *mut SSL_CTX) -> *mut SSL;
    fn SSL_free(ssl: *mut SSL);
    fn SSL_set_bio(ssl: *mut SSL, rbio: *mut BIO, wbio: *mut BIO);
    fn SSL_accept(ssl: *mut SSL) -> c_int;
    fn SSL_get_error(ssl: *const SSL, ret: c_int) -> c_int;
    fn SSL_read(ssl: *mut SSL, buf: *mut c_void, num: c_int) -> c_int;
    fn SSL_write(ssl: *mut SSL, buf: *const c_void, num: c_int) -> c_int;
    fn SSL_shutdown(ssl: *mut SSL) -> c_int;
    fn SSL_get0_alpn_selected(ssl: *const SSL, data: *mut *const c_uchar, len: *mut c_uint);
    fn SSL_get_peer_certificate(ssl: *const SSL) -> *mut X509;
    fn SSL_get_certificate(ssl: *const SSL) -> *mut X509;
    fn SSL_get_version(ssl: *const SSL) -> *const c_char;
    fn SSL_get_cipher_list(ssl: *const SSL, n: c_int) -> *const c_char;
    fn SSL_get_current_cipher(ssl: *const SSL) -> *const c_void;
    fn SSL_CIPHER_get_name(c: *const c_void) -> *const c_char;
    fn SSL_CIPHER_get_bits(c: *const c_void, alg_bits: *mut c_int) -> c_int;
    fn SSL_get_shared_ciphers(ssl: *const SSL, buf: *mut c_char, size: c_int) -> *mut c_char;
    fn SSL_state_string_long(ssl: *const SSL) -> *const c_char;
    fn SSL_select_next_proto(
        out: *mut *mut c_uchar,
        outlen: *mut c_uchar,
        server: *const c_uchar,
        server_len: c_uint,
        client: *const c_uchar,
        client_len: c_uint,
    ) -> c_int;

    // libcrypto: errors
    fn ERR_get_error() -> c_ulong;
    fn ERR_clear_error();
    fn ERR_reason_error_string(e: c_ulong) -> *const c_char;

    // libcrypto: BIO
    fn BIO_new(r#type: *const BIO_METHOD) -> *mut BIO;
    fn BIO_new_mem_buf(buf: *const c_void, len: c_int) -> *mut BIO;
    fn BIO_free(a: *mut BIO) -> c_int;
    fn BIO_s_mem() -> *const BIO_METHOD;
    fn BIO_ctrl(bp: *mut BIO, cmd: c_int, larg: c_long, parg: *mut c_void) -> c_long;
    fn BIO_write(b: *mut BIO, data: *const c_void, dlen: c_int) -> c_int;
    fn BIO_printf(bio: *mut BIO, format: *const c_char, ...) -> c_int;
    fn BIO_get_new_index() -> c_int;
    fn BIO_meth_new(r#type: c_int, name: *const c_char) -> *mut BIO_METHOD;
    fn BIO_meth_free(biom: *mut BIO_METHOD);
    fn BIO_meth_set_write(biom: *mut BIO_METHOD, write: bio_method_write) -> c_int;
    fn BIO_meth_set_read(biom: *mut BIO_METHOD, read: bio_method_read) -> c_int;
    fn BIO_meth_set_ctrl(biom: *mut BIO_METHOD, ctrl: bio_method_ctrl) -> c_int;
    fn BIO_set_data(a: *mut BIO, ptr: *mut c_void);
    fn BIO_get_data(a: *mut BIO) -> *mut c_void;
    fn BIO_set_init(a: *mut BIO, init: c_int);
    fn BIO_set_flags(b: *mut BIO, flags: c_int);
    fn BIO_clear_flags(b: *mut BIO, flags: c_int);

    // libcrypto: DH
    fn PEM_read_DHparams(
        fp: *mut c_void,
        x: *mut *mut DH,
        cb: pem_password_cb,
        u: *mut c_void,
    ) -> *mut DH;
    fn PEM_read_bio_DHparams(
        bp: *mut BIO,
        x: *mut *mut DH,
        cb: pem_password_cb,
        u: *mut c_void,
    ) -> *mut DH;
    fn DH_check(dh: *const DH, codes: *mut c_int) -> c_int;
    fn DH_free(dh: *mut DH);

    // libcrypto: X509 / names / serials
    fn X509_free(a: *mut X509);
    fn X509_get_subject_name(a: *const X509) -> *mut X509_NAME;
    fn X509_get_issuer_name(a: *const X509) -> *mut X509_NAME;
    fn X509_get_serialNumber(x: *mut X509) -> *mut ASN1_INTEGER;
    fn X509_NAME_get_text_by_NID(
        name: *mut X509_NAME,
        nid: c_int,
        buf: *mut c_char,
        len: c_int,
    ) -> c_int;
    fn X509_NAME_print_ex(out: *mut BIO, nm: *const X509_NAME, indent: c_int, flags: c_ulong)
        -> c_int;
    fn X509_NAME_entry_count(name: *const X509_NAME) -> c_int;
    fn X509_NAME_get_entry(name: *const X509_NAME, loc: c_int) -> *mut X509_NAME_ENTRY;
    fn X509_NAME_ENTRY_get_object(ne: *const X509_NAME_ENTRY) -> *mut ASN1_OBJECT;
    fn X509_NAME_ENTRY_get_data(ne: *const X509_NAME_ENTRY) -> *mut ASN1_STRING;
    fn X509_STORE_load_locations(
        ctx: *mut X509_STORE,
        file: *const c_char,
        dir: *const c_char,
    ) -> c_int;
    fn X509_STORE_set_flags(ctx: *mut X509_STORE, flags: c_ulong) -> c_int;
    fn X509_STORE_CTX_get_error_depth(ctx: *mut X509_STORE_CTX) -> c_int;
    fn X509_STORE_CTX_get_error(ctx: *mut X509_STORE_CTX) -> c_int;
    fn X509_STORE_CTX_get_current_cert(ctx: *mut X509_STORE_CTX) -> *mut X509;
    fn X509_verify_cert_error_string(n: c_long) -> *const c_char;
    fn X509_get_signature_nid(crt: *const X509) -> c_int;
    fn X509_digest(
        data: *const X509,
        r#type: *const EVP_MD,
        md: *mut c_uchar,
        len: *mut c_uint,
    ) -> c_int;

    // libcrypto: ASN1 / BN / OBJ
    fn ASN1_STRING_print_ex(out: *mut BIO, str: *const ASN1_STRING, flags: c_ulong) -> c_int;
    fn ASN1_INTEGER_to_BN(ai: *const ASN1_INTEGER, bn: *mut BIGNUM) -> *mut BIGNUM;
    fn BN_bn2dec(a: *const BIGNUM) -> *mut c_char;
    fn BN_free(a: *mut BIGNUM);
    fn OBJ_obj2nid(o: *const ASN1_OBJECT) -> c_int;
    fn OBJ_nid2sn(n: c_int) -> *const c_char;
    fn OBJ_nid2ln(n: c_int) -> *const c_char;
    fn OBJ_find_sigid_algs(signid: c_int, pdig_nid: *mut c_int, ppkey_nid: *mut c_int) -> c_int;

    // libcrypto: EVP / digests
    fn EVP_sha256() -> *const EVP_MD;
    fn EVP_get_digestbynid(r#type: c_int) -> *const EVP_MD;

    fn CRYPTO_free(ptr: *mut c_void, file: *const c_char, line: c_int);
}

// OpenSSL convenience macros, expressed as Rust helpers.
#[inline]
unsafe fn SSL_CTX_set_mode(ctx: *mut SSL_CTX, mode: c_long) -> c_long {
    // SSL_CTRL_MODE == 33
    SSL_CTX_ctrl(ctx, 33, mode, null_mut())
}

#[inline]
unsafe fn SSL_CTX_set_session_cache_mode(ctx: *mut SSL_CTX, mode: c_long) -> c_long {
    // SSL_CTRL_SET_SESS_CACHE_MODE == 44
    SSL_CTX_ctrl(ctx, 44, mode, null_mut())
}

#[inline]
unsafe fn SSL_CTX_set_min_proto_version(ctx: *mut SSL_CTX, version: c_int) -> c_long {
    // SSL_CTRL_SET_MIN_PROTO_VERSION == 123
    SSL_CTX_ctrl(ctx, 123, version as c_long, null_mut())
}

#[inline]
unsafe fn SSL_CTX_set_max_proto_version(ctx: *mut SSL_CTX, version: c_int) -> c_long {
    // SSL_CTRL_SET_MAX_PROTO_VERSION == 124
    SSL_CTX_ctrl(ctx, 124, version as c_long, null_mut())
}

#[inline]
unsafe fn SSL_CTX_set1_groups_list(ctx: *mut SSL_CTX, s: *const c_char) -> c_int {
    // SSL_CTRL_SET_GROUPS_LIST == 92
    SSL_CTX_ctrl(ctx, 92, 0, s as *mut c_void) as c_int
}

#[inline]
unsafe fn SSL_CTX_set_tmp_dh(ctx: *mut SSL_CTX, dh: *mut DH) -> c_long {
    // SSL_CTRL_SET_TMP_DH == 3
    SSL_CTX_ctrl(ctx, 3, 0, dh as *mut c_void)
}

#[inline]
unsafe fn BIO_get_mem_data(b: *mut BIO, pp: *mut *mut c_char) -> c_long {
    // BIO_CTRL_INFO == 3
    BIO_ctrl(b, 3, 0, pp as *mut c_void)
}

#[inline]
unsafe fn BIO_get_mem_ptr(b: *mut BIO, pp: *mut *mut BUF_MEM) -> c_long {
    // BIO_C_GET_BUF_MEM_PTR == 115
    BIO_ctrl(b, 115, 0, pp as *mut c_void)
}

#[inline]
unsafe fn BIO_set_close(b: *mut BIO, c: c_long) -> c_long {
    // BIO_CTRL_SET_CLOSE == 9
    BIO_ctrl(b, 9, c, null_mut())
}

#[inline]
unsafe fn BIO_clear_retry_flags(b: *mut BIO) {
    // BIO_FLAGS_RWS | BIO_FLAGS_SHOULD_RETRY == (0x01|0x02|0x04) | 0x08
    BIO_clear_flags(b, 0x01 | 0x02 | 0x04 | 0x08);
}

#[inline]
unsafe fn BIO_set_retry_read(b: *mut BIO) {
    // BIO_FLAGS_READ | BIO_FLAGS_SHOULD_RETRY
    BIO_set_flags(b, 0x01 | 0x08);
}

#[inline]
unsafe fn BIO_set_retry_write(b: *mut BIO) {
    // BIO_FLAGS_WRITE | BIO_FLAGS_SHOULD_RETRY
    BIO_set_flags(b, 0x02 | 0x08);
}

#[inline]
unsafe fn SSL_get_cipher_bits(ssl: *const SSL, np: *mut c_int) -> c_int {
    SSL_CIPHER_get_bits(SSL_get_current_cipher(ssl), np)
}

#[inline]
unsafe fn SSL_get_cipher(ssl: *const SSL) -> *const c_char {
    SSL_CIPHER_get_name(SSL_get_current_cipher(ssl))
}

#[inline]
unsafe fn OPENSSL_free(ptr: *mut c_void) {
    CRYPTO_free(ptr, c"be-secure-openssl.rs".as_ptr(), 0);
}

#[inline]
unsafe fn ERR_GET_REASON(ecode: c_ulong) -> c_int {
    (ecode & 0x7fffff) as c_int
}

// ------------------------------------------------------------
//   not-yet-ported callees (local stubs)
// ------------------------------------------------------------

// TODO(pg-port): port src/backend/storage/file/fd.c AllocateFile/FreeFile.
unsafe fn AllocateFile(_name: *const c_char, _mode: *const c_char) -> *mut c_void {
    unimplemented!()
}
unsafe fn FreeFile(_file: *mut c_void) -> c_int {
    unimplemented!()
}

// TODO(pg-port): port errcode_for_file_access()/errcode_for_socket_access() from elog.c.
fn errcode_for_file_access() -> c_int {
    0
}
fn errcode_for_socket_access() -> c_int {
    0
}

// errcode classification helpers (shimmed: classification is ignored by ereport).
const ERRCODE_CONFIG_FILE_ERROR: c_int = 0;
const ERRCODE_PROTOCOL_VIOLATION: c_int = 0;
const ERRCODE_OUT_OF_MEMORY: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;

// TODO(pg-port): port GetConfigOption() from src/backend/utils/misc/guc.c.
unsafe fn GetConfigOption(
    _name: *const c_char,
    _missing_ok: bool,
    _restrict_privileged: bool,
) -> *const c_char {
    unimplemented!()
}

// gettext no-op marker: C `_()` / `errmsg_internal` text passthrough.
#[inline]
fn gettext(s: *const c_char) -> *const c_char {
    s
}

// libc bits.
extern "C" {
    fn strlcpy(dst: *mut c_char, src: *const c_char, siz: usize) -> usize;
    fn strlen(s: *const c_char) -> usize;
    fn memcmp(s1: *const c_void, s2: *const c_void, n: usize) -> c_int;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn snprintf(s: *mut c_char, n: usize, format: *const c_char, ...) -> c_int;
    fn strerror(errnum: c_int) -> *mut c_char;
}

// errno access (platform errno location), mirroring be_secure.rs.
#[cfg(target_os = "macos")]
extern "C" {
    #[link_name = "__error"]
    fn errno_location() -> *mut c_int;
}
#[cfg(not(target_os = "macos"))]
extern "C" {
    #[link_name = "__errno_location"]
    fn errno_location() -> *mut c_int;
}

#[inline]
unsafe fn errno() -> c_int {
    *errno_location()
}
#[inline]
unsafe fn set_errno(v: c_int) {
    *errno_location() = v;
}

// errno constants (Darwin values).
const EINTR: c_int = 4;
const EAGAIN: c_int = 35;
const EWOULDBLOCK: c_int = EAGAIN;
const ECONNRESET: c_int = 54;

// ------------------------------------------------------------
//   File-scoped statics
// ------------------------------------------------------------

// default init hook can be overridden by a shared library; the global
// openssl_tls_init_hook (declared in libpq_be.rs) is initialized to this.

static mut SSL_context: *mut SSL_CTX = null_mut();
static mut dummy_ssl_passwd_cb_called: bool = false;
static mut ssl_is_server_start: bool = false;

// for passing data back from verify_cb()
static mut cert_errdetail: *const c_char = null_mut();

// ------------------------------------------------------------
//   Public interface
// ------------------------------------------------------------

pub unsafe fn be_tls_init(isServerStart: bool) -> c_int {
    let context: *mut SSL_CTX;
    let mut ssl_ver_min: c_int = -1;
    let mut ssl_ver_max: c_int = -1;

    /*
     * Create a new SSL context into which we'll load all the configuration
     * settings.  If we fail partway through, we can avoid memory leakage by
     * freeing this context; we don't install it as active until the end.
     *
     * We use SSLv23_method() because it can negotiate use of the highest
     * mutually supported protocol version, while alternatives like
     * TLSv1_2_method() permit only one specific version.  Note that we don't
     * actually allow SSL v2 or v3, only TLS protocols (see below).
     */
    context = SSL_CTX_new(SSLv23_method());
    if context.is_null() {
        ereport!(
            if isServerStart { FATAL } else { LOG },
            errmsg!(
                "could not create SSL context: {}",
                CStr::from_ptr(SSLerrmessage(ERR_get_error())).to_string_lossy()
            )
        );
        return be_tls_init_error(null_mut());
    }

    /*
     * Disable OpenSSL's moving-write-buffer sanity check, because it causes
     * unnecessary failures in nonblocking send cases.
     */
    SSL_CTX_set_mode(context, SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

    /*
     * Call init hook (usually to set password callback)
     */
    if let Some(hook) = openssl_tls_init_hook {
        hook(context, isServerStart);
    }

    /* used by the callback */
    ssl_is_server_start = isServerStart;

    /*
     * Load and verify server's certificate and private key
     */
    if SSL_CTX_use_certificate_chain_file(context, ssl_cert_file) != 1 {
        let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
        ereport!(
            if isServerStart { FATAL } else { LOG },
            errmsg!(
                "could not load server certificate file \"{}\": {}",
                CStr::from_ptr(ssl_cert_file).to_string_lossy(),
                CStr::from_ptr(SSLerrmessage(ERR_get_error())).to_string_lossy()
            )
        );
        return be_tls_init_error(context);
    }

    if !check_ssl_key_file_permissions(ssl_key_file, isServerStart) {
        return be_tls_init_error(context);
    }

    /*
     * OK, try to load the private key file.
     */
    dummy_ssl_passwd_cb_called = false;

    if SSL_CTX_use_PrivateKey_file(context, ssl_key_file, SSL_FILETYPE_PEM) != 1 {
        if dummy_ssl_passwd_cb_called {
            let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
            ereport!(
                if isServerStart { FATAL } else { LOG },
                errmsg!(
                    "private key file \"{}\" cannot be reloaded because it requires a passphrase",
                    CStr::from_ptr(ssl_key_file).to_string_lossy()
                )
            );
        } else {
            let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
            ereport!(
                if isServerStart { FATAL } else { LOG },
                errmsg!(
                    "could not load private key file \"{}\": {}",
                    CStr::from_ptr(ssl_key_file).to_string_lossy(),
                    CStr::from_ptr(SSLerrmessage(ERR_get_error())).to_string_lossy()
                )
            );
        }
        return be_tls_init_error(context);
    }

    if SSL_CTX_check_private_key(context) != 1 {
        let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
        ereport!(
            if isServerStart { FATAL } else { LOG },
            errmsg!(
                "check of private key failed: {}",
                CStr::from_ptr(SSLerrmessage(ERR_get_error())).to_string_lossy()
            )
        );
        return be_tls_init_error(context);
    }

    if ssl_min_protocol_version != 0 {
        ssl_ver_min = ssl_protocol_version_to_openssl(ssl_min_protocol_version);

        if ssl_ver_min == -1 {
            /* translator: first %s is a GUC option name, second %s is its value */
            ereport!(
                if isServerStart { FATAL } else { LOG },
                errmsg!(
                    "\"{}\" setting \"{}\" not supported by this build",
                    "ssl_min_protocol_version",
                    CStr::from_ptr(GetConfigOption(
                        c"ssl_min_protocol_version".as_ptr(),
                        false,
                        false
                    ))
                    .to_string_lossy()
                )
            );
            return be_tls_init_error(context);
        }

        if SSL_CTX_set_min_proto_version(context, ssl_ver_min) == 0 {
            ereport!(
                if isServerStart { FATAL } else { LOG },
                errmsg!("could not set minimum SSL protocol version")
            );
            return be_tls_init_error(context);
        }
    }

    if ssl_max_protocol_version != 0 {
        ssl_ver_max = ssl_protocol_version_to_openssl(ssl_max_protocol_version);

        if ssl_ver_max == -1 {
            /* translator: first %s is a GUC option name, second %s is its value */
            ereport!(
                if isServerStart { FATAL } else { LOG },
                errmsg!(
                    "\"{}\" setting \"{}\" not supported by this build",
                    "ssl_max_protocol_version",
                    CStr::from_ptr(GetConfigOption(
                        c"ssl_max_protocol_version".as_ptr(),
                        false,
                        false
                    ))
                    .to_string_lossy()
                )
            );
            return be_tls_init_error(context);
        }

        if SSL_CTX_set_max_proto_version(context, ssl_ver_max) == 0 {
            ereport!(
                if isServerStart { FATAL } else { LOG },
                errmsg!("could not set maximum SSL protocol version")
            );
            return be_tls_init_error(context);
        }
    }

    /* Check compatibility of min/max protocols */
    if ssl_min_protocol_version != 0 && ssl_max_protocol_version != 0 {
        /*
         * No need to check for invalid values (-1) for each protocol number
         * as the code above would have already generated an error.
         */
        if ssl_ver_min > ssl_ver_max {
            let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
            // C also: errdetail("\"%s\" cannot be higher than \"%s\"",
            // "ssl_min_protocol_version", "ssl_max_protocol_version")
            ereport!(
                if isServerStart { FATAL } else { LOG },
                errmsg!("could not set SSL protocol version range")
            );
            return be_tls_init_error(context);
        }
    }

    /*
     * Disallow SSL session tickets. OpenSSL use both stateful and stateless
     * tickets for TLSv1.3, and stateless ticket for TLSv1.2. SSL_OP_NO_TICKET
     * is available since 0.9.8f but only turns off stateless tickets. In
     * order to turn off stateful tickets we need SSL_CTX_set_num_tickets,
     * which is available since OpenSSL 1.1.1.  LibreSSL 3.5.4 (from OpenBSD
     * 7.1) introduced this API for compatibility, but doesn't support session
     * tickets at all so it's a no-op there.
     */
    // #ifdef HAVE_SSL_CTX_SET_NUM_TICKETS
    SSL_CTX_set_num_tickets(context, 0);
    // #endif
    SSL_CTX_set_options(context, SSL_OP_NO_TICKET);

    /* disallow SSL session caching, too */
    SSL_CTX_set_session_cache_mode(context, SSL_SESS_CACHE_OFF);

    /* disallow SSL compression */
    SSL_CTX_set_options(context, SSL_OP_NO_COMPRESSION);

    /*
     * Disallow SSL renegotiation.  This concerns only TLSv1.2 and older
     * protocol versions, as TLSv1.3 has no support for renegotiation.
     * SSL_OP_NO_RENEGOTIATION is available in OpenSSL since 1.1.0h (via a
     * backport from 1.1.1). SSL_OP_NO_CLIENT_RENEGOTIATION is available in
     * LibreSSL since 2.5.1 disallowing all client-initiated renegotiation
     * (this is usually on by default).
     */
    // #ifdef SSL_OP_NO_RENEGOTIATION
    SSL_CTX_set_options(context, SSL_OP_NO_RENEGOTIATION);
    // #endif
    // #ifdef SSL_OP_NO_CLIENT_RENEGOTIATION
    if SSL_OP_NO_CLIENT_RENEGOTIATION != 0 {
        SSL_CTX_set_options(context, SSL_OP_NO_CLIENT_RENEGOTIATION);
    }
    // #endif

    /* set up ephemeral DH and ECDH keys */
    if !initialize_dh(context, isServerStart) {
        return be_tls_init_error(context);
    }
    if !initialize_ecdh(context, isServerStart) {
        return be_tls_init_error(context);
    }

    /* set up the allowed cipher list for TLSv1.2 and below */
    if SSL_CTX_set_cipher_list(context, SSLCipherList) != 1 {
        let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
        ereport!(
            if isServerStart { FATAL } else { LOG },
            errmsg!("could not set the TLSv1.2 cipher list (no valid ciphers available)")
        );
        return be_tls_init_error(context);
    }

    /*
     * Set up the allowed cipher suites for TLSv1.3. If the GUC is an empty
     * string we leave the allowed suites to be the OpenSSL default value.
     */
    if *SSLCipherSuites.add(0) != 0 {
        /* set up the allowed cipher suites */
        if SSL_CTX_set_ciphersuites(context, SSLCipherSuites) != 1 {
            let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
            ereport!(
                if isServerStart { FATAL } else { LOG },
                errmsg!("could not set the TLSv1.3 cipher suites (no valid ciphers available)")
            );
            return be_tls_init_error(context);
        }
    }

    /* Let server choose order */
    if SSLPreferServerCiphers {
        SSL_CTX_set_options(context, SSL_OP_CIPHER_SERVER_PREFERENCE);
    }

    /*
     * Load CA store, so we can verify client certificates if needed.
     */
    if *ssl_ca_file.add(0) != 0 {
        let root_cert_list: *mut c_void;

        root_cert_list = SSL_load_client_CA_file(ssl_ca_file);
        if SSL_CTX_load_verify_locations(context, ssl_ca_file, null()) != 1
            || root_cert_list.is_null()
        {
            let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
            ereport!(
                if isServerStart { FATAL } else { LOG },
                errmsg!(
                    "could not load root certificate file \"{}\": {}",
                    CStr::from_ptr(ssl_ca_file).to_string_lossy(),
                    CStr::from_ptr(SSLerrmessage(ERR_get_error())).to_string_lossy()
                )
            );
            return be_tls_init_error(context);
        }

        /*
         * Tell OpenSSL to send the list of root certs we trust to clients in
         * CertificateRequests.  This lets a client with a keystore select the
         * appropriate client certificate to send to us.  Also, this ensures
         * that the SSL context will "own" the root_cert_list and remember to
         * free it when no longer needed.
         */
        SSL_CTX_set_client_CA_list(context, root_cert_list);

        /*
         * Always ask for SSL client cert, but don't fail if it's not
         * presented.  We might fail such connections later, depending on what
         * we find in pg_hba.conf.
         */
        SSL_CTX_set_verify(
            context,
            SSL_VERIFY_PEER | SSL_VERIFY_CLIENT_ONCE,
            Some(verify_cb),
        );
    }

    /*----------
     * Load the Certificate Revocation List (CRL).
     * http://searchsecurity.techtarget.com/sDefinition/0,,sid14_gci803160,00.html
     *----------
     */
    if *ssl_crl_file.add(0) != 0 || *ssl_crl_dir.add(0) != 0 {
        let cvstore: *mut X509_STORE = SSL_CTX_get_cert_store(context);

        if !cvstore.is_null() {
            /* Set the flags to check against the complete CRL chain */
            if X509_STORE_load_locations(
                cvstore,
                if *ssl_crl_file.add(0) != 0 {
                    ssl_crl_file
                } else {
                    null()
                },
                if *ssl_crl_dir.add(0) != 0 {
                    ssl_crl_dir
                } else {
                    null()
                },
            ) == 1
            {
                X509_STORE_set_flags(cvstore, X509_V_FLAG_CRL_CHECK | X509_V_FLAG_CRL_CHECK_ALL);
            } else if *ssl_crl_dir.add(0) == 0 {
                let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
                ereport!(
                    if isServerStart { FATAL } else { LOG },
                    errmsg!(
                        "could not load SSL certificate revocation list file \"{}\": {}",
                        CStr::from_ptr(ssl_crl_file).to_string_lossy(),
                        CStr::from_ptr(SSLerrmessage(ERR_get_error())).to_string_lossy()
                    )
                );
                return be_tls_init_error(context);
            } else if *ssl_crl_file.add(0) == 0 {
                let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
                ereport!(
                    if isServerStart { FATAL } else { LOG },
                    errmsg!(
                        "could not load SSL certificate revocation list directory \"{}\": {}",
                        CStr::from_ptr(ssl_crl_dir).to_string_lossy(),
                        CStr::from_ptr(SSLerrmessage(ERR_get_error())).to_string_lossy()
                    )
                );
                return be_tls_init_error(context);
            } else {
                let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
                ereport!(
                    if isServerStart { FATAL } else { LOG },
                    errmsg!(
                        "could not load SSL certificate revocation list file \"{}\" or directory \"{}\": {}",
                        CStr::from_ptr(ssl_crl_file).to_string_lossy(),
                        CStr::from_ptr(ssl_crl_dir).to_string_lossy(),
                        CStr::from_ptr(SSLerrmessage(ERR_get_error())).to_string_lossy()
                    )
                );
                return be_tls_init_error(context);
            }
        }
    }

    /*
     * Success!  Replace any existing SSL_context.
     */
    if !SSL_context.is_null() {
        SSL_CTX_free(SSL_context);
    }

    SSL_context = context;

    /*
     * Set flag to remember whether CA store has been loaded into SSL_context.
     */
    if *ssl_ca_file.add(0) != 0 {
        ssl_loaded_verify_locations = true;
    } else {
        ssl_loaded_verify_locations = false;
    }

    0
}

/// Clean up by releasing working context. (the C `error:` label)
#[inline]
unsafe fn be_tls_init_error(context: *mut SSL_CTX) -> c_int {
    if !context.is_null() {
        SSL_CTX_free(context);
    }
    -1
}

pub unsafe fn be_tls_destroy() {
    if !SSL_context.is_null() {
        SSL_CTX_free(SSL_context);
    }
    SSL_context = null_mut();
    ssl_loaded_verify_locations = false;
}

pub unsafe fn be_tls_open_server(port: *mut Port) -> c_int {
    let mut r: c_int;
    let err: c_int;
    let waitfor: c_int;
    let mut ecode: c_ulong;
    let mut give_proto_hint: bool;

    Assert!((*port).ssl.is_null());
    Assert!((*port).peer.is_null());

    if SSL_context.is_null() {
        let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
        ereport!(
            COMMERROR,
            errmsg!("could not initialize SSL connection: SSL context not set up")
        );
        return -1;
    }

    /* set up debugging/info callback */
    SSL_CTX_set_info_callback(SSL_context, Some(info_cb));

    /* enable ALPN */
    SSL_CTX_set_alpn_select_cb(SSL_context, Some(alpn_cb), port as *mut c_void);

    (*port).ssl = SSL_new(SSL_context);
    if (*port).ssl.is_null() {
        let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
        ereport!(
            COMMERROR,
            errmsg!(
                "could not initialize SSL connection: {}",
                CStr::from_ptr(SSLerrmessage(ERR_get_error())).to_string_lossy()
            )
        );
        return -1;
    }
    if ssl_set_port_bio(port) == 0 {
        let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
        ereport!(
            COMMERROR,
            errmsg!(
                "could not set SSL socket: {}",
                CStr::from_ptr(SSLerrmessage(ERR_get_error())).to_string_lossy()
            )
        );
        return -1;
    }
    (*port).ssl_in_use = true;

    'aloop: loop {
        /*
         * Prepare to call SSL_get_error() by clearing thread's OpenSSL error
         * queue.  In general, the current thread's error queue must be empty
         * before the TLS/SSL I/O operation is attempted, or SSL_get_error()
         * will not work reliably.  An extension may have failed to clear the
         * per-thread error queue following another call to an OpenSSL I/O
         * routine.
         */
        set_errno(0);
        ERR_clear_error();
        r = SSL_accept((*port).ssl);
        if r <= 0 {
            err = SSL_get_error((*port).ssl, r);

            /*
             * Other clients of OpenSSL in the backend may fail to call
             * ERR_get_error(), but we always do, so as to not cause problems
             * for OpenSSL clients that don't call ERR_clear_error()
             * defensively.  Be sure that this happens by calling now.
             * SSL_get_error() relies on the OpenSSL per-thread error queue
             * being intact, so this is the earliest possible point
             * ERR_get_error() may be called.
             */
            ecode = ERR_get_error();
            match err {
                SSL_ERROR_WANT_READ | SSL_ERROR_WANT_WRITE => {
                    /* not allowed during connection establishment */
                    Assert!(!(*port).noblock);

                    /*
                     * No need to care about timeouts/interrupts here. At this
                     * point authentication_timeout still employs
                     * StartupPacketTimeoutHandler() which directly exits.
                     */
                    if err == SSL_ERROR_WANT_READ {
                        waitfor = WL_SOCKET_READABLE | WL_EXIT_ON_PM_DEATH;
                    } else {
                        waitfor = WL_SOCKET_WRITEABLE | WL_EXIT_ON_PM_DEATH;
                    }

                    WaitLatchOrSocket(
                        null_mut(),
                        waitfor,
                        (*port).sock,
                        0,
                        WAIT_EVENT_SSL_OPEN_SERVER,
                    );
                    continue 'aloop;
                }
                SSL_ERROR_SYSCALL => {
                    if r < 0 && errno() != 0 {
                        let _ = errcode_for_socket_access();
                        ereport!(COMMERROR, errmsg!("could not accept SSL connection: %m"));
                    } else {
                        let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
                        ereport!(
                            COMMERROR,
                            errmsg!("could not accept SSL connection: EOF detected")
                        );
                    }
                }
                SSL_ERROR_SSL => {
                    match ERR_GET_REASON(ecode) {
                        /*
                         * UNSUPPORTED_PROTOCOL, WRONG_VERSION_NUMBER, and
                         * TLSV1_ALERT_PROTOCOL_VERSION have been observed when
                         * trying to communicate with an old OpenSSL library, or
                         * when the client and server specify disjoint protocol
                         * ranges.  NO_PROTOCOLS_AVAILABLE occurs if there's a
                         * local misconfiguration (which can happen despite our
                         * checks, if openssl.cnf injects a limit we didn't
                         * account for).  It's not very clear what would make
                         * OpenSSL return the other codes listed here, but a hint
                         * about protocol versions seems like it's appropriate for
                         * all.
                         */
                        SSL_R_NO_PROTOCOLS_AVAILABLE
                        | SSL_R_UNSUPPORTED_PROTOCOL
                        | SSL_R_BAD_PROTOCOL_VERSION_NUMBER
                        | SSL_R_UNKNOWN_PROTOCOL
                        | SSL_R_UNKNOWN_SSL_VERSION
                        | SSL_R_UNSUPPORTED_SSL_VERSION
                        | SSL_R_WRONG_SSL_VERSION
                        | SSL_R_WRONG_VERSION_NUMBER
                        | SSL_R_TLSV1_ALERT_PROTOCOL_VERSION
                        | SSL_R_VERSION_TOO_HIGH
                        | SSL_R_VERSION_TOO_LOW => {
                            give_proto_hint = true;
                        }
                        _ => {
                            give_proto_hint = false;
                        }
                    }
                    let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
                    // C also: cert_errdetail ? errdetail_internal("%s", cert_errdetail) : 0,
                    // give_proto_hint ? errhint("This may indicate that the client does not
                    // support any SSL protocol version between %s and %s.", min, max) : 0
                    let _ = give_proto_hint;
                    let hint_min = if ssl_min_protocol_version != 0 {
                        ssl_protocol_version_to_string(ssl_min_protocol_version)
                    } else {
                        MIN_OPENSSL_TLS_VERSION.as_ptr()
                    };
                    let hint_max = if ssl_max_protocol_version != 0 {
                        ssl_protocol_version_to_string(ssl_max_protocol_version)
                    } else {
                        MAX_OPENSSL_TLS_VERSION.as_ptr()
                    };
                    let _ = (hint_min, hint_max);
                    ereport!(
                        COMMERROR,
                        errmsg!(
                            "could not accept SSL connection: {}",
                            CStr::from_ptr(SSLerrmessage(ecode)).to_string_lossy()
                        )
                    );
                    cert_errdetail = null_mut();
                }
                SSL_ERROR_ZERO_RETURN => {
                    let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
                    ereport!(
                        COMMERROR,
                        errmsg!("could not accept SSL connection: EOF detected")
                    );
                }
                _ => {
                    let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
                    ereport!(
                        COMMERROR,
                        errmsg!("unrecognized SSL error code: {}", err)
                    );
                }
            }
            return -1;
        }

        break;
    }

    /* Get the protocol selected by ALPN */
    (*port).alpn_used = false;
    {
        let mut selected: *const c_uchar = null();
        let mut len: c_uint = 0;

        SSL_get0_alpn_selected((*port).ssl, &mut selected, &mut len);

        /* If ALPN is used, check that we negotiated the expected protocol */
        if !selected.is_null() {
            if len as usize == strlen(PG_ALPN_PROTOCOL.as_ptr())
                && memcmp(
                    selected as *const c_void,
                    PG_ALPN_PROTOCOL.as_ptr() as *const c_void,
                    strlen(PG_ALPN_PROTOCOL.as_ptr()),
                ) == 0
            {
                (*port).alpn_used = true;
            } else {
                /* shouldn't happen */
                let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
                ereport!(
                    COMMERROR,
                    errmsg!("received SSL connection request with unexpected ALPN protocol")
                );
            }
        }
    }

    /* Get client certificate, if available. */
    (*port).peer = SSL_get_peer_certificate((*port).ssl);

    /* and extract the Common Name and Distinguished Name from it. */
    (*port).peer_cn = null_mut();
    (*port).peer_dn = null_mut();
    (*port).peer_cert_valid = false;
    if !(*port).peer.is_null() {
        let mut len: c_int;
        let x509name: *mut X509_NAME = X509_get_subject_name((*port).peer);
        let peer_dn: *mut c_char;
        let mut bio: *mut BIO;
        let mut bio_buf: *mut BUF_MEM = null_mut();

        len = X509_NAME_get_text_by_NID(x509name, NID_commonName, null_mut(), 0);
        if len != -1 {
            let peer_cn: *mut c_char;

            peer_cn = MemoryContextAlloc(TopMemoryContext, (len + 1) as Size) as *mut c_char;
            r = X509_NAME_get_text_by_NID(x509name, NID_commonName, peer_cn, len + 1);
            *peer_cn.offset(len as isize) = b'\0' as c_char;
            if r != len {
                /* shouldn't happen */
                pfree(peer_cn as *mut c_void);
                return -1;
            }

            /*
             * Reject embedded NULLs in certificate common name to prevent
             * attacks like CVE-2009-4034.
             */
            if len as usize != strlen(peer_cn) {
                let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
                ereport!(
                    COMMERROR,
                    errmsg!("SSL certificate's common name contains embedded null")
                );
                pfree(peer_cn as *mut c_void);
                return -1;
            }

            (*port).peer_cn = peer_cn;
        }

        bio = BIO_new(BIO_s_mem());
        if bio.is_null() {
            if !(*port).peer_cn.is_null() {
                pfree((*port).peer_cn as *mut c_void);
                (*port).peer_cn = null_mut();
            }
            return -1;
        }

        /*
         * RFC2253 is the closest thing to an accepted standard format for DNs.
         * We have documented how to produce this format from a certificate. It
         * uses commas instead of slashes for delimiters, which make regular
         * expression matching a bit easier. Also note that it prints the
         * Subject fields in reverse order.
         */
        if X509_NAME_print_ex(bio, x509name, 0, XN_FLAG_RFC2253) == -1
            || BIO_get_mem_ptr(bio, &mut bio_buf) <= 0
        {
            BIO_free(bio);
            if !(*port).peer_cn.is_null() {
                pfree((*port).peer_cn as *mut c_void);
                (*port).peer_cn = null_mut();
            }
            return -1;
        }
        peer_dn = MemoryContextAlloc(TopMemoryContext, (*bio_buf).length + 1) as *mut c_char;
        memcpy(
            peer_dn as *mut c_void,
            (*bio_buf).data as *const c_void,
            (*bio_buf).length,
        );
        len = (*bio_buf).length as c_int;
        BIO_free(bio);
        *peer_dn.offset(len as isize) = b'\0' as c_char;
        if len as usize != strlen(peer_dn) {
            let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
            ereport!(
                COMMERROR,
                errmsg!("SSL certificate's distinguished name contains embedded null")
            );
            pfree(peer_dn as *mut c_void);
            if !(*port).peer_cn.is_null() {
                pfree((*port).peer_cn as *mut c_void);
                (*port).peer_cn = null_mut();
            }
            return -1;
        }

        (*port).peer_dn = peer_dn;

        (*port).peer_cert_valid = true;
    }

    0
}

pub unsafe fn be_tls_close(port: *mut Port) {
    if !(*port).ssl.is_null() {
        SSL_shutdown((*port).ssl);
        SSL_free((*port).ssl);
        (*port).ssl = null_mut();
        (*port).ssl_in_use = false;
    }

    if !(*port).peer.is_null() {
        X509_free((*port).peer);
        (*port).peer = null_mut();
    }

    if !(*port).peer_cn.is_null() {
        pfree((*port).peer_cn as *mut c_void);
        (*port).peer_cn = null_mut();
    }

    if !(*port).peer_dn.is_null() {
        pfree((*port).peer_dn as *mut c_void);
        (*port).peer_dn = null_mut();
    }
}

pub unsafe fn be_tls_read(
    port: *mut Port,
    ptr: *mut c_void,
    len: Size,
    waitfor: *mut c_int,
) -> isize {
    let mut n: isize;
    let err: c_int;
    let ecode: c_ulong;

    set_errno(0);
    ERR_clear_error();
    n = SSL_read((*port).ssl, ptr, len as c_int) as isize;
    err = SSL_get_error((*port).ssl, n as c_int);
    ecode = if err != SSL_ERROR_NONE || n < 0 {
        ERR_get_error()
    } else {
        0
    };
    match err {
        SSL_ERROR_NONE => {
            /* a-ok */
        }
        SSL_ERROR_WANT_READ => {
            *waitfor = WL_SOCKET_READABLE;
            set_errno(EWOULDBLOCK);
            n = -1;
        }
        SSL_ERROR_WANT_WRITE => {
            *waitfor = WL_SOCKET_WRITEABLE;
            set_errno(EWOULDBLOCK);
            n = -1;
        }
        SSL_ERROR_SYSCALL => {
            /* leave it to caller to ereport the value of errno */
            if n != -1 || errno() == 0 {
                set_errno(ECONNRESET);
                n = -1;
            }
        }
        SSL_ERROR_SSL => {
            let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
            ereport!(
                COMMERROR,
                errmsg!(
                    "SSL error: {}",
                    CStr::from_ptr(SSLerrmessage(ecode)).to_string_lossy()
                )
            );
            set_errno(ECONNRESET);
            n = -1;
        }
        SSL_ERROR_ZERO_RETURN => {
            /* connection was cleanly shut down by peer */
            n = 0;
        }
        _ => {
            let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
            ereport!(
                COMMERROR,
                errmsg!("unrecognized SSL error code: {}", err)
            );
            set_errno(ECONNRESET);
            n = -1;
        }
    }

    n
}

pub unsafe fn be_tls_write(
    port: *mut Port,
    ptr: *const c_void,
    len: Size,
    waitfor: *mut c_int,
) -> isize {
    let mut n: isize;
    let err: c_int;
    let ecode: c_ulong;

    set_errno(0);
    ERR_clear_error();
    n = SSL_write((*port).ssl, ptr, len as c_int) as isize;
    err = SSL_get_error((*port).ssl, n as c_int);
    ecode = if err != SSL_ERROR_NONE || n < 0 {
        ERR_get_error()
    } else {
        0
    };
    match err {
        SSL_ERROR_NONE => {
            /* a-ok */
        }
        SSL_ERROR_WANT_READ => {
            *waitfor = WL_SOCKET_READABLE;
            set_errno(EWOULDBLOCK);
            n = -1;
        }
        SSL_ERROR_WANT_WRITE => {
            *waitfor = WL_SOCKET_WRITEABLE;
            set_errno(EWOULDBLOCK);
            n = -1;
        }
        SSL_ERROR_SYSCALL => {
            /*
             * Leave it to caller to ereport the value of errno.  However, if
             * errno is still zero then assume it's a read EOF situation, and
             * report ECONNRESET.  (This seems possible because SSL_write can
             * also do reads.)
             */
            if n != -1 || errno() == 0 {
                set_errno(ECONNRESET);
                n = -1;
            }
        }
        SSL_ERROR_SSL => {
            let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
            ereport!(
                COMMERROR,
                errmsg!(
                    "SSL error: {}",
                    CStr::from_ptr(SSLerrmessage(ecode)).to_string_lossy()
                )
            );
            set_errno(ECONNRESET);
            n = -1;
        }
        SSL_ERROR_ZERO_RETURN => {
            /*
             * the SSL connection was closed, leave it to the caller to ereport
             * it
             */
            set_errno(ECONNRESET);
            n = -1;
        }
        _ => {
            let _ = errcode(ERRCODE_PROTOCOL_VIOLATION);
            ereport!(
                COMMERROR,
                errmsg!("unrecognized SSL error code: {}", err)
            );
            set_errno(ECONNRESET);
            n = -1;
        }
    }

    n
}

// ------------------------------------------------------------
//   Internal functions
// ------------------------------------------------------------

/*
 * Private substitute BIO: this does the sending and receiving using send() and
 * recv() instead. This is so that we can enable and disable interrupts just
 * while calling recv(). We cannot have interrupts occurring while the bulk of
 * OpenSSL runs, because it uses malloc() and possibly other non-reentrant libc
 * facilities. We also need to call send() and recv() directly so it gets
 * passed through the socket/signals layer on Win32.
 *
 * These functions are closely modelled on the standard socket BIO in OpenSSL;
 * see sock_read() and sock_write() in OpenSSL's crypto/bio/bss_sock.c.
 */

static mut port_bio_method_ptr: *mut BIO_METHOD = null_mut();

unsafe extern "C" fn port_bio_read(h: *mut BIO, buf: *mut c_char, size: c_int) -> c_int {
    let mut res: c_int = 0;
    let port: *mut Port = BIO_get_data(h) as *mut Port;

    if !buf.is_null() {
        res = secure_raw_read(port, buf as *mut c_void, size as Size) as c_int;
        BIO_clear_retry_flags(h);
        (*port).last_read_was_eof = res == 0;
        if res <= 0 {
            /* If we were interrupted, tell caller to retry */
            if errno() == EINTR || errno() == EWOULDBLOCK || errno() == EAGAIN {
                BIO_set_retry_read(h);
            }
        }
    }

    res
}

unsafe extern "C" fn port_bio_write(h: *mut BIO, buf: *const c_char, size: c_int) -> c_int {
    let res: c_int;

    res = secure_raw_write(BIO_get_data(h) as *mut Port, buf as *const c_void, size as Size)
        as c_int;
    BIO_clear_retry_flags(h);
    if res <= 0 {
        /* If we were interrupted, tell caller to retry */
        if errno() == EINTR || errno() == EWOULDBLOCK || errno() == EAGAIN {
            BIO_set_retry_write(h);
        }
    }

    res
}

unsafe extern "C" fn port_bio_ctrl(h: *mut BIO, cmd: c_int, _num: c_long, _ptr: *mut c_void) -> c_long {
    let res: c_long;
    let port: *mut Port = BIO_get_data(h) as *mut Port;

    match cmd {
        BIO_CTRL_EOF => {
            /*
             * This should not be needed. port_bio_read already has a way to
             * signal EOF to OpenSSL. However, OpenSSL made an undocumented,
             * backwards-incompatible change and now expects EOF via BIO_ctrl.
             * See https://github.com/openssl/openssl/issues/8208
             */
            res = (*port).last_read_was_eof as c_long;
        }
        BIO_CTRL_FLUSH => {
            /* libssl expects all BIOs to support BIO_flush. */
            res = 1;
        }
        _ => {
            res = 0;
        }
    }

    res
}

unsafe fn port_bio_method() -> *mut BIO_METHOD {
    if port_bio_method_ptr.is_null() {
        let mut my_bio_index: c_int;

        my_bio_index = BIO_get_new_index();
        if my_bio_index == -1 {
            return null_mut();
        }
        my_bio_index |= BIO_TYPE_SOURCE_SINK;
        port_bio_method_ptr =
            BIO_meth_new(my_bio_index, c"PostgreSQL backend socket".as_ptr());
        if port_bio_method_ptr.is_null() {
            return null_mut();
        }
        if BIO_meth_set_write(port_bio_method_ptr, Some(port_bio_write)) == 0
            || BIO_meth_set_read(port_bio_method_ptr, Some(port_bio_read)) == 0
            || BIO_meth_set_ctrl(port_bio_method_ptr, Some(port_bio_ctrl)) == 0
        {
            BIO_meth_free(port_bio_method_ptr);
            port_bio_method_ptr = null_mut();
            return null_mut();
        }
    }
    port_bio_method_ptr
}

unsafe fn ssl_set_port_bio(port: *mut Port) -> c_int {
    let bio: *mut BIO;
    let bio_method: *mut BIO_METHOD;

    bio_method = port_bio_method();
    if bio_method.is_null() {
        return 0;
    }

    bio = BIO_new(bio_method);
    if bio.is_null() {
        return 0;
    }

    BIO_set_data(bio, port as *mut c_void);
    BIO_set_init(bio, 1);

    SSL_set_bio((*port).ssl, bio, bio);
    1
}

/*
 *	Load precomputed DH parameters.
 *
 *	To prevent "downgrade" attacks, we perform a number of checks
 *	to verify that the DBA-generated DH parameters file contains
 *	what we expect it to contain.
 */
unsafe fn load_dh_file(filename: *mut c_char, isServerStart: bool) -> *mut DH {
    let fp: *mut c_void;
    let mut dh: *mut DH;
    let mut codes: c_int = 0;

    /* attempt to open file.  It's not an error if it doesn't exist. */
    fp = AllocateFile(filename, c"r".as_ptr());
    if fp.is_null() {
        let _ = errcode_for_file_access();
        ereport!(
            if isServerStart { FATAL } else { LOG },
            errmsg!(
                "could not open DH parameters file \"{}\": %m",
                CStr::from_ptr(filename).to_string_lossy()
            )
        );
        return null_mut();
    }

    dh = PEM_read_DHparams(fp, null_mut(), None, null_mut());
    FreeFile(fp);

    if dh.is_null() {
        let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
        ereport!(
            if isServerStart { FATAL } else { LOG },
            errmsg!(
                "could not load DH parameters file: {}",
                CStr::from_ptr(SSLerrmessage(ERR_get_error())).to_string_lossy()
            )
        );
        return null_mut();
    }

    /* make sure the DH parameters are usable */
    if DH_check(dh, &mut codes) == 0 {
        let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
        ereport!(
            if isServerStart { FATAL } else { LOG },
            errmsg!(
                "invalid DH parameters: {}",
                CStr::from_ptr(SSLerrmessage(ERR_get_error())).to_string_lossy()
            )
        );
        DH_free(dh);
        return null_mut();
    }
    if codes & DH_CHECK_P_NOT_PRIME != 0 {
        let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
        ereport!(
            if isServerStart { FATAL } else { LOG },
            errmsg!("invalid DH parameters: p is not prime")
        );
        DH_free(dh);
        return null_mut();
    }
    if (codes & DH_NOT_SUITABLE_GENERATOR != 0) && (codes & DH_CHECK_P_NOT_SAFE_PRIME != 0) {
        let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
        ereport!(
            if isServerStart { FATAL } else { LOG },
            errmsg!("invalid DH parameters: neither suitable generator or safe prime")
        );
        DH_free(dh);
        return null_mut();
    }

    dh
}

/*
 *	Load hardcoded DH parameters.
 *
 *	If DH parameters cannot be loaded from a specified file, we can load
 *	the hardcoded DH parameters supplied with the backend to prevent
 *	problems.
 */
unsafe fn load_dh_buffer(buffer: *const c_char, len: Size) -> *mut DH {
    let bio: *mut BIO;
    let mut dh: *mut DH = null_mut();

    bio = BIO_new_mem_buf(buffer as *const c_void, len as c_int);
    if bio.is_null() {
        return null_mut();
    }
    dh = PEM_read_bio_DHparams(bio, null_mut(), None, null_mut());
    if dh.is_null() {
        ereport!(
            DEBUG2,
            errmsg!(
                "DH load buffer: {}",
                CStr::from_ptr(SSLerrmessage(ERR_get_error())).to_string_lossy()
            )
        );
    }
    BIO_free(bio);

    dh
}

/*
 *	Passphrase collection callback using ssl_passphrase_command
 */
unsafe extern "C" fn ssl_external_passwd_cb(
    buf: *mut c_char,
    size: c_int,
    rwflag: c_int,
    _userdata: *mut c_void,
) -> c_int {
    /* same prompt as OpenSSL uses internally */
    let prompt: &CStr = c"Enter PEM pass phrase:";

    Assert!(rwflag == 0);

    run_ssl_passphrase_command(prompt.as_ptr(), ssl_is_server_start, buf, size)
}

/*
 * Dummy passphrase callback
 *
 * If OpenSSL is told to use a passphrase-protected server key, by default it
 * will issue a prompt on /dev/tty and try to read a key from there.  That's no
 * good during a postmaster SIGHUP cycle, not to mention SSL context reload in
 * an EXEC_BACKEND postmaster child.  So override it with this dummy function
 * that just returns an empty passphrase, guaranteeing failure.
 */
unsafe extern "C" fn dummy_ssl_passwd_cb(
    buf: *mut c_char,
    size: c_int,
    _rwflag: c_int,
    _userdata: *mut c_void,
) -> c_int {
    /* Set flag to change the error message we'll report */
    dummy_ssl_passwd_cb_called = true;
    /* And return empty string */
    Assert!(size > 0);
    *buf.add(0) = b'\0' as c_char;
    0
}

// utils/wait_event.h: WAIT_EVENT_SSL_OPEN_SERVER (enum WaitEventIO).
const WAIT_EVENT_SSL_OPEN_SERVER: uint32 = 0x0b000000 | 50;

/*
 * Examines the provided certificate name, and if it's too long to log or
 * contains unprintable ASCII, escapes and truncates it. The return value is
 * always a new palloc'd string. (The input string is still modified in place,
 * for ease of implementation.)
 */
unsafe fn prepare_cert_name(name: *mut c_char) -> *mut c_char {
    let mut namelen: usize = strlen(name);
    let mut truncated: *mut c_char = name;

    /*
     * Common Names are 64 chars max, so for a common case where the CN is the
     * last field, we can still print the longest possible CN with a 7-character
     * prefix (".../CN=[64 chars]"), for a reasonable limit of 71 characters.
     */
    const MAXLEN: usize = 71;

    if namelen > MAXLEN {
        /*
         * Keep the end of the name, not the beginning, since the most specific
         * field is likely to give users the most information.
         */
        truncated = name.add(namelen - MAXLEN);
        *truncated.add(0) = b'.' as c_char;
        *truncated.add(1) = b'.' as c_char;
        *truncated.add(2) = b'.' as c_char;
        namelen = MAXLEN;
    }
    let _ = namelen;

    pg_clean_ascii(truncated, 0)
}

/*
 *	Certificate verification callback
 *
 *	This callback allows us to examine intermediate problems during
 *	verification, for later logging.
 *
 *	This callback also allows us to override the default acceptance
 *	criteria (e.g., accepting self-signed or expired certs), but
 *	for now we accept the default checks.
 */
unsafe extern "C" fn verify_cb(ok: c_int, ctx: *mut X509_STORE_CTX) -> c_int {
    let depth: c_int;
    let errcode_v: c_int;
    let errstring: *const c_char;
    let mut str: StringInfoData = core::mem::zeroed();
    let cert: *mut X509;

    if ok != 0 {
        /* Nothing to do for the successful case. */
        return ok;
    }

    /* Pull all the information we have on the verification failure. */
    depth = X509_STORE_CTX_get_error_depth(ctx);
    errcode_v = X509_STORE_CTX_get_error(ctx);
    errstring = X509_verify_cert_error_string(errcode_v as c_long);

    initStringInfo(&mut str);
    let msg0 = std::ffi::CString::new(format!(
        "Client certificate verification failed at depth {}: {}.",
        depth,
        CStr::from_ptr(errstring).to_string_lossy()
    ))
    .unwrap();
    appendStringInfoString(&mut str, msg0.as_ptr());

    cert = X509_STORE_CTX_get_current_cert(ctx);
    if !cert.is_null() {
        let subject: *mut c_char;
        let issuer: *mut c_char;
        let sub_prepared: *mut c_char;
        let iss_prepared: *mut c_char;
        let serialno: *mut c_char;
        let sn: *mut ASN1_INTEGER;
        let b: *mut BIGNUM;

        /*
         * Get the Subject and Issuer for logging, but don't let maliciously
         * huge certs flood the logs, and don't reflect non-ASCII bytes into it
         * either.
         */
        subject = X509_NAME_to_cstring(X509_get_subject_name(cert));
        sub_prepared = prepare_cert_name(subject);
        pfree(subject as *mut c_void);

        issuer = X509_NAME_to_cstring(X509_get_issuer_name(cert));
        iss_prepared = prepare_cert_name(issuer);
        pfree(issuer as *mut c_void);

        /*
         * Pull the serial number, too, in case a Subject is still ambiguous.
         * This mirrors be_tls_get_peer_serial().
         */
        sn = X509_get_serialNumber(cert);
        b = ASN1_INTEGER_to_BN(sn, null_mut());
        serialno = BN_bn2dec(b);

        appendStringInfoChar(&mut str, b'\n' as c_char);
        let serialno_str = if !serialno.is_null() {
            CStr::from_ptr(serialno).to_string_lossy().into_owned()
        } else {
            CStr::from_ptr(gettext(c"unknown".as_ptr()))
                .to_string_lossy()
                .into_owned()
        };
        let msg1 = std::ffi::CString::new(format!(
            "Failed certificate data (unverified): subject \"{}\", serial number {}, issuer \"{}\".",
            CStr::from_ptr(sub_prepared).to_string_lossy(),
            serialno_str,
            CStr::from_ptr(iss_prepared).to_string_lossy()
        ))
        .unwrap();
        appendStringInfoString(&mut str, msg1.as_ptr());

        BN_free(b);
        OPENSSL_free(serialno as *mut c_void);
        pfree(iss_prepared as *mut c_void);
        pfree(sub_prepared as *mut c_void);
    }

    /* Store our detail message to be logged later. */
    cert_errdetail = str.data;

    ok
}

/*
 *	This callback is used to copy SSL information messages
 *	into the PostgreSQL log.
 */
unsafe extern "C" fn info_cb(ssl: *const SSL, r#type: c_int, args: c_int) {
    let desc: *const c_char;

    desc = SSL_state_string_long(ssl);
    let desc_s = CStr::from_ptr(desc).to_string_lossy();

    match r#type {
        SSL_CB_HANDSHAKE_START => {
            ereport!(DEBUG4, errmsg!("SSL: handshake start: \"{}\"", desc_s));
        }
        SSL_CB_HANDSHAKE_DONE => {
            ereport!(DEBUG4, errmsg!("SSL: handshake done: \"{}\"", desc_s));
        }
        SSL_CB_ACCEPT_LOOP => {
            ereport!(DEBUG4, errmsg!("SSL: accept loop: \"{}\"", desc_s));
        }
        SSL_CB_ACCEPT_EXIT => {
            ereport!(DEBUG4, errmsg!("SSL: accept exit ({}): \"{}\"", args, desc_s));
        }
        SSL_CB_CONNECT_LOOP => {
            ereport!(DEBUG4, errmsg!("SSL: connect loop: \"{}\"", desc_s));
        }
        SSL_CB_CONNECT_EXIT => {
            ereport!(DEBUG4, errmsg!("SSL: connect exit ({}): \"{}\"", args, desc_s));
        }
        SSL_CB_READ_ALERT => {
            ereport!(
                DEBUG4,
                errmsg!("SSL: read alert (0x{:04x}): \"{}\"", args, desc_s)
            );
        }
        SSL_CB_WRITE_ALERT => {
            ereport!(
                DEBUG4,
                errmsg!("SSL: write alert (0x{:04x}): \"{}\"", args, desc_s)
            );
        }
        _ => {}
    }
}

/* See pqcomm.h comments on OpenSSL implementation of ALPN (RFC 7301) */
static alpn_protos: &[u8] = PG_ALPN_PROTOCOL_VECTOR;

/*
 * Server callback for ALPN negotiation. We use the standard "helper" function
 * even though currently we only accept one value.
 */
unsafe extern "C" fn alpn_cb(
    _ssl: *mut SSL,
    out: *mut *const c_uchar,
    outlen: *mut c_uchar,
    r#in: *const c_uchar,
    inlen: c_uint,
    userdata: *mut c_void,
) -> c_int {
    /*
     * Why does OpenSSL provide a helper function that requires a nonconst
     * vector when the callback is declared to take a const vector? What are we
     * to do with that?
     */
    let retval: c_int;

    Assert!(!userdata.is_null());
    Assert!(!out.is_null());
    Assert!(!outlen.is_null());
    Assert!(!r#in.is_null());

    retval = SSL_select_next_proto(
        out as *mut *mut c_uchar,
        outlen,
        alpn_protos.as_ptr(),
        alpn_protos.len() as c_uint,
        r#in,
        inlen,
    );
    if (*out).is_null() || *outlen as usize > alpn_protos.len() || *outlen == 0 {
        return SSL_TLSEXT_ERR_NOACK; /* can't happen */
    }

    if retval == OPENSSL_NPN_NEGOTIATED {
        SSL_TLSEXT_ERR_OK
    } else {
        /*
         * The client doesn't support our protocol.  Reject the connection with
         * TLS "no_application_protocol" alert, per RFC 7301.
         */
        SSL_TLSEXT_ERR_ALERT_FATAL
    }
}

/*
 * Set DH parameters for generating ephemeral DH keys.  The DH parameters can
 * take a long time to compute, so they must be precomputed.
 *
 * Since few sites will bother to create a parameter file, we also provide a
 * fallback to the parameters provided by the OpenSSL project.
 *
 * These values can be static (once loaded or computed) since the OpenSSL
 * library can efficiently generate random keys from the information provided.
 */
unsafe fn initialize_dh(context: *mut SSL_CTX, isServerStart: bool) -> bool {
    let mut dh: *mut DH = null_mut();

    SSL_CTX_set_options(context, SSL_OP_SINGLE_DH_USE);

    if *ssl_dh_params_file.add(0) != 0 {
        dh = load_dh_file(ssl_dh_params_file, isServerStart);
    }
    if dh.is_null() {
        let buf = FILE_DH2048.as_ptr() as *const c_char;
        dh = load_dh_buffer(buf, FILE_DH2048.len() as Size);
    }
    if dh.is_null() {
        let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
        ereport!(
            if isServerStart { FATAL } else { LOG },
            errmsg!("DH: could not load DH parameters")
        );
        return false;
    }

    if SSL_CTX_set_tmp_dh(context, dh) != 1 {
        let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
        ereport!(
            if isServerStart { FATAL } else { LOG },
            errmsg!(
                "DH: could not set DH parameters: {}",
                CStr::from_ptr(SSLerrmessage(ERR_get_error())).to_string_lossy()
            )
        );
        DH_free(dh);
        return false;
    }

    DH_free(dh);
    true
}

/*
 * Set ECDH parameters for generating ephemeral Elliptic Curve DH keys.  This is
 * much simpler than the DH parameters, as we just need to provide the name of
 * the curve to OpenSSL.
 */
unsafe fn initialize_ecdh(context: *mut SSL_CTX, isServerStart: bool) -> bool {
    // #ifndef OPENSSL_NO_ECDH
    if SSL_CTX_set1_groups_list(context, SSLECDHCurve) != 1 {
        /*
         * OpenSSL 3.3.0 introduced proper error messages for group parsing
         * errors, earlier versions returns "no SSL error reported" which is far
         * from helpful. For older versions, we replace with a better error
         * message. Injecting the error into the OpenSSL error queue need APIs
         * from OpenSSL 3.0.
         */
        let _ = errcode(ERRCODE_CONFIG_FILE_ERROR);
        // C also: errhint("Ensure that each group name is spelled correctly and
        // supported by the installed version of OpenSSL.")
        ereport!(
            if isServerStart { FATAL } else { LOG },
            errmsg!(
                "could not set group names specified in ssl_groups: {}",
                CStr::from_ptr(SSLerrmessageExt(
                    ERR_get_error(),
                    gettext(c"No valid groups found".as_ptr())
                ))
                .to_string_lossy()
            )
        );
        return false;
    }
    // #endif

    true
}

/*
 * Obtain reason string for passed SSL errcode with replacement
 *
 * The error message supplied in replacement will be used in case the error code
 * from OpenSSL is 0, else the error message from SSLerrmessage() will be
 * returned.
 *
 * Not all versions of OpenSSL place an error on the queue even for failing
 * operations, which will yield "no SSL error reported" by SSLerrmessage. This
 * function can be used to ensure that a proper error message is displayed for
 * versions reporting no error, while using the OpenSSL error via SSLerrmessage
 * for versions where there is one.
 */
unsafe fn SSLerrmessageExt(ecode: c_ulong, replacement: *const c_char) -> *const c_char {
    if ecode == 0 {
        replacement
    } else {
        SSLerrmessage(ecode)
    }
}

/*
 * Obtain reason string for passed SSL errcode
 *
 * ERR_get_error() is used by caller to get errcode to pass here.
 *
 * Some caution is needed here since ERR_reason_error_string will return NULL if
 * it doesn't recognize the error code, or (in OpenSSL >= 3) if the code
 * represents a system errno value.  We don't want to return NULL ever.
 */
unsafe fn SSLerrmessage(ecode: c_ulong) -> *const c_char {
    let errreason: *const c_char;
    static mut errbuf: [c_char; 36] = [0; 36];

    if ecode == 0 {
        return gettext(c"no SSL error reported".as_ptr());
    }
    errreason = ERR_reason_error_string(ecode);
    if !errreason.is_null() {
        return errreason;
    }

    /*
     * In OpenSSL 3.0.0 and later, ERR_reason_error_string does not map system
     * errno values anymore.  (See OpenSSL source code for the explanation.)  We
     * can cover that shortcoming with this bit of code.  Older OpenSSL versions
     * don't have the ERR_SYSTEM_ERROR macro, but that's okay because they don't
     * have the shortcoming either.
     */
    // #ifdef ERR_SYSTEM_ERROR
    if ERR_SYSTEM_ERROR(ecode) {
        return strerror(ERR_GET_REASON(ecode));
    }
    // #endif

    /* No choice but to report the numeric ecode */
    snprintf(
        errbuf.as_mut_ptr(),
        core::mem::size_of_val(&errbuf),
        gettext(c"SSL error code %lu".as_ptr()),
        ecode,
    );
    errbuf.as_ptr()
}

#[inline]
unsafe fn ERR_SYSTEM_ERROR(errcode: c_ulong) -> bool {
    // ERR_SYSTEM_FLAG == ((unsigned long)INT_MAX + 1)
    (errcode & 0x80000000) != 0
}

pub unsafe fn be_tls_get_cipher_bits(port: *mut Port) -> c_int {
    let mut bits: c_int = 0;

    if !(*port).ssl.is_null() {
        SSL_get_cipher_bits((*port).ssl, &mut bits);
        bits
    } else {
        0
    }
}

pub unsafe fn be_tls_get_version(port: *mut Port) -> *const c_char {
    if !(*port).ssl.is_null() {
        SSL_get_version((*port).ssl)
    } else {
        null()
    }
}

pub unsafe fn be_tls_get_cipher(port: *mut Port) -> *const c_char {
    if !(*port).ssl.is_null() {
        SSL_get_cipher((*port).ssl)
    } else {
        null()
    }
}

pub unsafe fn be_tls_get_peer_subject_name(port: *mut Port, ptr: *mut c_char, len: Size) {
    if !(*port).peer.is_null() {
        strlcpy(
            ptr,
            X509_NAME_to_cstring(X509_get_subject_name((*port).peer)),
            len,
        );
    } else {
        *ptr.add(0) = b'\0' as c_char;
    }
}

pub unsafe fn be_tls_get_peer_issuer_name(port: *mut Port, ptr: *mut c_char, len: Size) {
    if !(*port).peer.is_null() {
        strlcpy(
            ptr,
            X509_NAME_to_cstring(X509_get_issuer_name((*port).peer)),
            len,
        );
    } else {
        *ptr.add(0) = b'\0' as c_char;
    }
}

pub unsafe fn be_tls_get_peer_serial(port: *mut Port, ptr: *mut c_char, len: Size) {
    if !(*port).peer.is_null() {
        let serial: *mut ASN1_INTEGER;
        let b: *mut BIGNUM;
        let decimal: *mut c_char;

        serial = X509_get_serialNumber((*port).peer);
        b = ASN1_INTEGER_to_BN(serial, null_mut());
        decimal = BN_bn2dec(b);

        BN_free(b);
        strlcpy(ptr, decimal, len);
        OPENSSL_free(decimal as *mut c_void);
    } else {
        *ptr.add(0) = b'\0' as c_char;
    }
}

pub unsafe fn be_tls_get_certificate_hash(port: *mut Port, len: *mut Size) -> *mut c_char {
    let server_cert: *mut X509;
    let cert_hash: *mut c_char;
    let mut algo_type: *const EVP_MD = null();
    let mut hash: [c_uchar; EVP_MAX_MD_SIZE] = [0; EVP_MAX_MD_SIZE]; /* size for SHA-512 */
    let mut hash_size: c_uint = 0;
    let mut algo_nid: c_int = 0;

    *len = 0;
    server_cert = SSL_get_certificate((*port).ssl);
    if server_cert.is_null() {
        return null_mut();
    }

    /*
     * Get the signature algorithm of the certificate to determine the hash
     * algorithm to use for the result.  Prefer X509_get_signature_info(),
     * introduced in OpenSSL 1.1.1, which can handle RSA-PSS signatures.
     */
    // #if HAVE_X509_GET_SIGNATURE_INFO ... #else
    if OBJ_find_sigid_algs(
        X509_get_signature_nid(server_cert),
        &mut algo_nid,
        null_mut(),
    ) == 0
    {
        // #endif
        elog!(
            ERROR,
            "could not determine server certificate signature algorithm"
        );
    }

    /*
     * The TLS server's certificate bytes need to be hashed with SHA-256 if its
     * signature algorithm is MD5 or SHA-1 as per RFC 5929
     * (https://tools.ietf.org/html/rfc5929#section-4.1).  If something else is
     * used, the same hash as the signature algorithm is used.
     */
    match algo_nid {
        NID_md5 | NID_sha1 => {
            algo_type = EVP_sha256();
        }
        _ => {
            algo_type = EVP_get_digestbynid(algo_nid);
            if algo_type.is_null() {
                elog!(
                    ERROR,
                    "could not find digest for NID {}",
                    CStr::from_ptr(OBJ_nid2sn(algo_nid)).to_string_lossy()
                );
            }
        }
    }

    /* generate and save the certificate hash */
    if X509_digest(server_cert, algo_type, hash.as_mut_ptr(), &mut hash_size) == 0 {
        elog!(ERROR, "could not generate server certificate hash");
    }

    cert_hash = palloc(hash_size as Size) as *mut c_char;
    memcpy(
        cert_hash as *mut c_void,
        hash.as_ptr() as *const c_void,
        hash_size as usize,
    );
    *len = hash_size as Size;

    cert_hash
}

/*
 * Convert an X509 subject name to a cstring.
 *
 */
unsafe fn X509_NAME_to_cstring(name: *mut X509_NAME) -> *mut c_char {
    let membuf: *mut BIO = BIO_new(BIO_s_mem());
    let mut i: c_int;
    let mut nid: c_int;
    let count: c_int = X509_NAME_entry_count(name);
    let mut e: *mut X509_NAME_ENTRY;
    let mut v: *mut ASN1_STRING;
    let mut field_name: *const c_char;
    let size: Size;
    let nullterm: c_char;
    let mut sp: *mut c_char = null_mut();
    let dp: *mut c_char;
    let result: *mut c_char;

    if membuf.is_null() {
        let _ = errcode(ERRCODE_OUT_OF_MEMORY);
        ereport!(ERROR, errmsg!("could not create BIO"));
    }

    BIO_set_close(membuf, BIO_CLOSE);
    i = 0;
    while i < count {
        e = X509_NAME_get_entry(name, i);
        nid = OBJ_obj2nid(X509_NAME_ENTRY_get_object(e));
        if nid == NID_undef {
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            ereport!(ERROR, errmsg!("could not get NID for ASN1_OBJECT object"));
        }
        v = X509_NAME_ENTRY_get_data(e);
        field_name = OBJ_nid2sn(nid);
        if field_name.is_null() {
            field_name = OBJ_nid2ln(nid);
        }
        if field_name.is_null() {
            let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
            ereport!(
                ERROR,
                errmsg!("could not convert NID {} to an ASN1_OBJECT structure", nid)
            );
        }
        BIO_printf(membuf, c"/%s=".as_ptr(), field_name);
        ASN1_STRING_print_ex(
            membuf,
            v,
            (ASN1_STRFLGS_RFC2253 & !ASN1_STRFLGS_ESC_MSB) | ASN1_STRFLGS_UTF8_CONVERT,
        );
        i += 1;
    }

    /* ensure null termination of the BIO's content */
    nullterm = b'\0' as c_char;
    BIO_write(membuf, &nullterm as *const c_char as *const c_void, 1);
    size = BIO_get_mem_data(membuf, &mut sp) as Size;
    dp = pg_any_to_server(sp, (size - 1) as c_int, PG_UTF8 as c_int);

    result = pstrdup(dp);
    if dp != sp {
        pfree(dp as *mut c_void);
    }
    if BIO_free(membuf) != 1 {
        elog!(ERROR, "could not free OpenSSL BIO structure");
    }

    result
}

/*
 * Convert TLS protocol version GUC enum to OpenSSL values
 *
 * This is a straightforward one-to-one mapping, but doing it this way makes the
 * definitions of ssl_min_protocol_version and ssl_max_protocol_version
 * independent of OpenSSL availability and version.
 *
 * If a version is passed that is not supported by the current OpenSSL version,
 * then we return -1.  If a nonnegative value is returned, subsequent code can
 * assume it's working with a supported version.
 *
 * Note: this is rather similar to libpq's routine in fe-secure-openssl.c, so
 * make sure to update both routines if changing this one.
 */
unsafe fn ssl_protocol_version_to_openssl(v: c_int) -> c_int {
    match v {
        PG_TLS_ANY => return 0,
        PG_TLS1_VERSION => return TLS1_VERSION,
        PG_TLS1_1_VERSION => {
            // #ifdef TLS1_1_VERSION
            return TLS1_1_VERSION;
            // #endif
        }
        PG_TLS1_2_VERSION => {
            // #ifdef TLS1_2_VERSION
            return TLS1_2_VERSION;
            // #endif
        }
        PG_TLS1_3_VERSION => {
            // #ifdef TLS1_3_VERSION
            return TLS1_3_VERSION;
            // #endif
        }
        _ => {}
    }

    -1
}

/*
 * Likewise provide a mapping to strings.
 */
unsafe fn ssl_protocol_version_to_string(v: c_int) -> *const c_char {
    match v {
        PG_TLS_ANY => c"any".as_ptr(),
        PG_TLS1_VERSION => c"TLSv1".as_ptr(),
        PG_TLS1_1_VERSION => c"TLSv1.1".as_ptr(),
        PG_TLS1_2_VERSION => c"TLSv1.2".as_ptr(),
        PG_TLS1_3_VERSION => c"TLSv1.3".as_ptr(),
        _ => c"(unrecognized)".as_ptr(),
    }
}

pub unsafe extern "C" fn default_openssl_tls_init(context: *mut SSL_CTX, isServerStart: bool) {
    if isServerStart {
        if *ssl_passphrase_command.add(0) != 0 {
            SSL_CTX_set_default_passwd_cb(context, Some(ssl_external_passwd_cb));
        }
    } else {
        if *ssl_passphrase_command.add(0) != 0 && ssl_passphrase_command_supports_reload {
            SSL_CTX_set_default_passwd_cb(context, Some(ssl_external_passwd_cb));
        } else {
            /*
             * If reloading and no external command is configured, override
             * OpenSSL's default handling of passphrase-protected files, because
             * we don't want to prompt for a passphrase in an already-running
             * server.
             */
            SSL_CTX_set_default_passwd_cb(context, Some(dummy_ssl_passwd_cb));
        }
    }
}
