//! Translated from PostgreSQL src/include/libpq/hba.h
//! Interface to hba.c (pg_hba.conf / pg_ident.conf parsing). In-memory config
//! types: idiomatic Rust (no on-disk layout).

use crate::regex::regex::pg_regex_t;

/// Authentication methods supported by PostgreSQL.
/// Keep in sync with the UserAuthName array in hba.c.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UserAuth {
    Reject,
    ImplicitReject, // not a user-visible option
    Trust,
    Ident,
    Password,
    Md5,
    Scram,
    Gss,
    Sspi,
    Pam,
    Bsd,
    Ldap,
    Cert,
    Radius,
    Peer,
    OAuth,
}

/// Must be the last value of the UserAuth enum.
pub const USER_AUTH_LAST: UserAuth = UserAuth::OAuth;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IPCompareMethod {
    Mask,
    SameHost,
    SameNet,
    All,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnType {
    Local,
    Host,
    HostSSL,
    HostNoSSL,
    HostGSS,
    HostNoGSS,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientCertMode {
    Off,
    CA,
    Full,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientCertName {
    CN,
    DN,
}

/// A single string token lexed from an auth config file, plus whether it was
/// quoted. A leading-slash string may carry a compiled regex.
pub struct AuthToken {
    pub string: String,
    pub quoted: bool,
    pub regex: Option<Box<pg_regex_t>>,
}

/// One parsed pg_hba.conf entry.
pub struct HbaLine {
    pub sourcefile: String,
    pub linenumber: i32,
    pub rawline: String,
    pub conntype: ConnType,
    pub databases: Vec<AuthToken>,
    pub roles: Vec<AuthToken>,
    pub addr: std::net::SocketAddr, // sockaddr_storage
    pub addrlen: i32,               // zero if no valid addr
    pub mask: std::net::SocketAddr, // sockaddr_storage
    pub masklen: i32,               // zero if no valid mask
    pub ip_cmp_method: IPCompareMethod,
    pub hostname: Option<String>,
    pub auth_method: UserAuth,
    pub usermap: Option<String>,
    pub pamservice: Option<String>,
    pub pam_use_hostname: bool,
    pub ldaptls: bool,
    pub ldapscheme: Option<String>,
    pub ldapserver: Option<String>,
    pub ldapport: i32,
    pub ldapbinddn: Option<String>,
    pub ldapbindpasswd: Option<String>,
    pub ldapsearchattribute: Option<String>,
    pub ldapsearchfilter: Option<String>,
    pub ldapbasedn: Option<String>,
    pub ldapscope: i32,
    pub ldapprefix: Option<String>,
    pub ldapsuffix: Option<String>,
    pub clientcert: ClientCertMode,
    pub clientcertname: ClientCertName,
    pub krb_realm: Option<String>,
    pub include_realm: bool,
    pub compat_realm: bool,
    pub upn_username: bool,
    pub radiusservers: Vec<AuthToken>,
    pub radiusservers_s: Option<String>,
    pub radiussecrets: Vec<AuthToken>,
    pub radiussecrets_s: Option<String>,
    pub radiusidentifiers: Vec<AuthToken>,
    pub radiusidentifiers_s: Option<String>,
    pub radiusports: Vec<AuthToken>,
    pub radiusports_s: Option<String>,
    pub oauth_issuer: Option<String>,
    pub oauth_scope: Option<String>,
    pub oauth_validator: Option<String>,
    pub oauth_skip_usermap: bool,
}

/// One parsed pg_ident.conf entry.
pub struct IdentLine {
    pub linenumber: i32,
    pub usermap: String,
    pub system_user: AuthToken,
    pub pg_user: AuthToken,
}

/// One line lexed from an auth config file. `fields` is a list of lists of
/// AuthTokens. On a tokenization error, `fields` is empty and `err_msg` is set.
pub struct TokenizedAuthLine {
    pub fields: Vec<Vec<AuthToken>>,
    pub file_name: String,
    pub line_num: i32,
    pub raw_line: String,
    pub err_msg: Option<String>,
}

// TODO(struct-forward): Port is defined in libpq/libpq-be.h (the kluged hbaPort
// typedef); repoint to crate::libpq::libpq_be in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::libpq::libpq_be::Port in Phase 2")]
pub struct Port;

/// `hbaPort` alias.
#[allow(deprecated)]
pub type HbaPort = Port;

/// Returns bool success in C -> Result later; skeleton keeps bool.
pub fn load_hba() -> bool {
    unimplemented!()
}

pub fn load_ident() -> bool {
    unimplemented!()
}

pub fn hba_authname(_auth_method: UserAuth) -> &'static str {
    unimplemented!()
}

#[allow(deprecated)]
pub fn hba_getauthmethod(_port: &mut HbaPort) {
    unimplemented!()
}

/// C returns an int status code -> Result later; skeleton keeps i32.
pub fn check_usermap(
    _usermap_name: &str,
    _pg_user: &str,
    _system_user: &str,
    _case_insensitive: bool,
) -> i32 {
    unimplemented!()
}

/// NULL on failure -> Option (the elevel decides whether C also ereports).
pub fn parse_hba_line(_tok_line: &TokenizedAuthLine, _elevel: i32) -> Option<HbaLine> {
    unimplemented!()
}

pub fn parse_ident_line(_tok_line: &TokenizedAuthLine, _elevel: i32) -> Option<IdentLine> {
    unimplemented!()
}

pub fn pg_isblank(_c: char) -> bool {
    unimplemented!()
}

/// `FILE *` + `char **err_msg` out-param -> Option of the file plus message.
pub fn open_auth_file(
    _filename: &str,
    _elevel: i32,
    _depth: i32,
) -> Result<std::fs::File, Option<String>> {
    unimplemented!()
}

pub fn free_auth_file(_file: std::fs::File, _depth: i32) {
    unimplemented!()
}

/// Appends parsed lines into `tok_lines`.
pub fn tokenize_auth_file(
    _filename: &str,
    _file: &std::fs::File,
    _tok_lines: &mut Vec<TokenizedAuthLine>,
    _elevel: i32,
    _depth: i32,
) {
    unimplemented!()
}
