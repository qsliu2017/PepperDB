//! Data and routines for command tag names and enumeration.
//!
//! Source: postgres/src/backend/tcop/cmdtag.c
//! Merged headers:
//!   - postgres/src/include/tcop/cmdtag.h      (CommandTag enum, QueryCompletion,
//!                                               function declarations)
//!   - postgres/src/include/tcop/cmdtaglist.h  (PG_CMDTAG X-macro table, expanded
//!                                               below into the enum + behavior table)
//!
//! FULLY REAL: the X-macro table has been expanded by hand into a `CommandTag`
//! enum (repr C, CMDTAG_UNKNOWN = 0, remaining variants in list order) and a
//! parallel static `tag_behavior` table of `CommandTagBehavior`.
//!
//! `BuildQueryCompletionString` is included; it depends on `pg_ulltoa_n`
//! (utils/adt/numutils.c) and `MAXINT8LEN` (postgres_ext / c.h). Those are
//! STUBBED at finest granularity below (see notes near the function).

use crate::prelude::*;

extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

/// Buffer size required for command completion tags (cmdtag.h).
pub const COMPLETION_TAG_BUFSIZE: usize = 64;

// ---------------------------------------------------------------------------
// CommandTag enum (expanded from cmdtaglist.h via the PG_CMDTAG X-macro).
//
// CMDTAG_UNKNOWN is 0; the remaining variants follow in the exact order of the
// list, so a CommandTag value can index directly into `tag_behavior`.
// ---------------------------------------------------------------------------
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
#[repr(C)]
pub enum CommandTag {
    CMDTAG_UNKNOWN = 0,
    CMDTAG_ALTER_ACCESS_METHOD,
    CMDTAG_ALTER_AGGREGATE,
    CMDTAG_ALTER_CAST,
    CMDTAG_ALTER_COLLATION,
    CMDTAG_ALTER_CONSTRAINT,
    CMDTAG_ALTER_CONVERSION,
    CMDTAG_ALTER_DATABASE,
    CMDTAG_ALTER_DEFAULT_PRIVILEGES,
    CMDTAG_ALTER_DOMAIN,
    CMDTAG_ALTER_EVENT_TRIGGER,
    CMDTAG_ALTER_EXTENSION,
    CMDTAG_ALTER_FOREIGN_DATA_WRAPPER,
    CMDTAG_ALTER_FOREIGN_TABLE,
    CMDTAG_ALTER_FUNCTION,
    CMDTAG_ALTER_INDEX,
    CMDTAG_ALTER_LANGUAGE,
    CMDTAG_ALTER_LARGE_OBJECT,
    CMDTAG_ALTER_MATERIALIZED_VIEW,
    CMDTAG_ALTER_OPERATOR,
    CMDTAG_ALTER_OPERATOR_CLASS,
    CMDTAG_ALTER_OPERATOR_FAMILY,
    CMDTAG_ALTER_POLICY,
    CMDTAG_ALTER_PROCEDURE,
    CMDTAG_ALTER_PUBLICATION,
    CMDTAG_ALTER_ROLE,
    CMDTAG_ALTER_ROUTINE,
    CMDTAG_ALTER_RULE,
    CMDTAG_ALTER_SCHEMA,
    CMDTAG_ALTER_SEQUENCE,
    CMDTAG_ALTER_SERVER,
    CMDTAG_ALTER_STATISTICS,
    CMDTAG_ALTER_SUBSCRIPTION,
    CMDTAG_ALTER_SYSTEM,
    CMDTAG_ALTER_TABLE,
    CMDTAG_ALTER_TABLESPACE,
    CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION,
    CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY,
    CMDTAG_ALTER_TEXT_SEARCH_PARSER,
    CMDTAG_ALTER_TEXT_SEARCH_TEMPLATE,
    CMDTAG_ALTER_TRANSFORM,
    CMDTAG_ALTER_TRIGGER,
    CMDTAG_ALTER_TYPE,
    CMDTAG_ALTER_USER_MAPPING,
    CMDTAG_ALTER_VIEW,
    CMDTAG_ANALYZE,
    CMDTAG_BEGIN,
    CMDTAG_CALL,
    CMDTAG_CHECKPOINT,
    CMDTAG_CLOSE,
    CMDTAG_CLOSE_CURSOR,
    CMDTAG_CLOSE_CURSOR_ALL,
    CMDTAG_CLUSTER,
    CMDTAG_COMMENT,
    CMDTAG_COMMIT,
    CMDTAG_COMMIT_PREPARED,
    CMDTAG_COPY,
    CMDTAG_COPY_FROM,
    CMDTAG_CREATE_ACCESS_METHOD,
    CMDTAG_CREATE_AGGREGATE,
    CMDTAG_CREATE_CAST,
    CMDTAG_CREATE_COLLATION,
    CMDTAG_CREATE_CONSTRAINT,
    CMDTAG_CREATE_CONVERSION,
    CMDTAG_CREATE_DATABASE,
    CMDTAG_CREATE_DOMAIN,
    CMDTAG_CREATE_EVENT_TRIGGER,
    CMDTAG_CREATE_EXTENSION,
    CMDTAG_CREATE_FOREIGN_DATA_WRAPPER,
    CMDTAG_CREATE_FOREIGN_TABLE,
    CMDTAG_CREATE_FUNCTION,
    CMDTAG_CREATE_INDEX,
    CMDTAG_CREATE_LANGUAGE,
    CMDTAG_CREATE_MATERIALIZED_VIEW,
    CMDTAG_CREATE_OPERATOR,
    CMDTAG_CREATE_OPERATOR_CLASS,
    CMDTAG_CREATE_OPERATOR_FAMILY,
    CMDTAG_CREATE_POLICY,
    CMDTAG_CREATE_PROCEDURE,
    CMDTAG_CREATE_PUBLICATION,
    CMDTAG_CREATE_ROLE,
    CMDTAG_CREATE_ROUTINE,
    CMDTAG_CREATE_RULE,
    CMDTAG_CREATE_SCHEMA,
    CMDTAG_CREATE_SEQUENCE,
    CMDTAG_CREATE_SERVER,
    CMDTAG_CREATE_STATISTICS,
    CMDTAG_CREATE_SUBSCRIPTION,
    CMDTAG_CREATE_TABLE,
    CMDTAG_CREATE_TABLE_AS,
    CMDTAG_CREATE_TABLESPACE,
    CMDTAG_CREATE_TEXT_SEARCH_CONFIGURATION,
    CMDTAG_CREATE_TEXT_SEARCH_DICTIONARY,
    CMDTAG_CREATE_TEXT_SEARCH_PARSER,
    CMDTAG_CREATE_TEXT_SEARCH_TEMPLATE,
    CMDTAG_CREATE_TRANSFORM,
    CMDTAG_CREATE_TRIGGER,
    CMDTAG_CREATE_TYPE,
    CMDTAG_CREATE_USER_MAPPING,
    CMDTAG_CREATE_VIEW,
    CMDTAG_DEALLOCATE,
    CMDTAG_DEALLOCATE_ALL,
    CMDTAG_DECLARE_CURSOR,
    CMDTAG_DELETE,
    CMDTAG_DISCARD,
    CMDTAG_DISCARD_ALL,
    CMDTAG_DISCARD_PLANS,
    CMDTAG_DISCARD_SEQUENCES,
    CMDTAG_DISCARD_TEMP,
    CMDTAG_DO,
    CMDTAG_DROP_ACCESS_METHOD,
    CMDTAG_DROP_AGGREGATE,
    CMDTAG_DROP_CAST,
    CMDTAG_DROP_COLLATION,
    CMDTAG_DROP_CONSTRAINT,
    CMDTAG_DROP_CONVERSION,
    CMDTAG_DROP_DATABASE,
    CMDTAG_DROP_DOMAIN,
    CMDTAG_DROP_EVENT_TRIGGER,
    CMDTAG_DROP_EXTENSION,
    CMDTAG_DROP_FOREIGN_DATA_WRAPPER,
    CMDTAG_DROP_FOREIGN_TABLE,
    CMDTAG_DROP_FUNCTION,
    CMDTAG_DROP_INDEX,
    CMDTAG_DROP_LANGUAGE,
    CMDTAG_DROP_MATERIALIZED_VIEW,
    CMDTAG_DROP_OPERATOR,
    CMDTAG_DROP_OPERATOR_CLASS,
    CMDTAG_DROP_OPERATOR_FAMILY,
    CMDTAG_DROP_OWNED,
    CMDTAG_DROP_POLICY,
    CMDTAG_DROP_PROCEDURE,
    CMDTAG_DROP_PUBLICATION,
    CMDTAG_DROP_ROLE,
    CMDTAG_DROP_ROUTINE,
    CMDTAG_DROP_RULE,
    CMDTAG_DROP_SCHEMA,
    CMDTAG_DROP_SEQUENCE,
    CMDTAG_DROP_SERVER,
    CMDTAG_DROP_STATISTICS,
    CMDTAG_DROP_SUBSCRIPTION,
    CMDTAG_DROP_TABLE,
    CMDTAG_DROP_TABLESPACE,
    CMDTAG_DROP_TEXT_SEARCH_CONFIGURATION,
    CMDTAG_DROP_TEXT_SEARCH_DICTIONARY,
    CMDTAG_DROP_TEXT_SEARCH_PARSER,
    CMDTAG_DROP_TEXT_SEARCH_TEMPLATE,
    CMDTAG_DROP_TRANSFORM,
    CMDTAG_DROP_TRIGGER,
    CMDTAG_DROP_TYPE,
    CMDTAG_DROP_USER_MAPPING,
    CMDTAG_DROP_VIEW,
    CMDTAG_EXECUTE,
    CMDTAG_EXPLAIN,
    CMDTAG_FETCH,
    CMDTAG_GRANT,
    CMDTAG_GRANT_ROLE,
    CMDTAG_IMPORT_FOREIGN_SCHEMA,
    CMDTAG_INSERT,
    CMDTAG_LISTEN,
    CMDTAG_LOAD,
    CMDTAG_LOCK_TABLE,
    CMDTAG_LOGIN,
    CMDTAG_MERGE,
    CMDTAG_MOVE,
    CMDTAG_NOTIFY,
    CMDTAG_PREPARE,
    CMDTAG_PREPARE_TRANSACTION,
    CMDTAG_REASSIGN_OWNED,
    CMDTAG_REFRESH_MATERIALIZED_VIEW,
    CMDTAG_REINDEX,
    CMDTAG_RELEASE,
    CMDTAG_RESET,
    CMDTAG_REVOKE,
    CMDTAG_REVOKE_ROLE,
    CMDTAG_ROLLBACK,
    CMDTAG_ROLLBACK_PREPARED,
    CMDTAG_SAVEPOINT,
    CMDTAG_SECURITY_LABEL,
    CMDTAG_SELECT,
    CMDTAG_SELECT_FOR_KEY_SHARE,
    CMDTAG_SELECT_FOR_NO_KEY_UPDATE,
    CMDTAG_SELECT_FOR_SHARE,
    CMDTAG_SELECT_FOR_UPDATE,
    CMDTAG_SELECT_INTO,
    CMDTAG_SET,
    CMDTAG_SET_CONSTRAINTS,
    CMDTAG_SHOW,
    CMDTAG_START_TRANSACTION,
    CMDTAG_TRUNCATE_TABLE,
    CMDTAG_UNLISTEN,
    CMDTAG_UPDATE,
    CMDTAG_VACUUM,
}

/// QueryCompletion (cmdtag.h).
#[derive(Clone, Copy)]
#[repr(C)]
pub struct QueryCompletion {
    pub commandTag: CommandTag,
    pub nprocessed: uint64,
}

/// Inline helper from cmdtag.h.
#[inline]
pub fn SetQueryCompletion(qc: &mut QueryCompletion, commandTag: CommandTag, nprocessed: uint64) {
    qc.commandTag = commandTag;
    qc.nprocessed = nprocessed;
}

/// Inline helper from cmdtag.h.
#[inline]
pub fn CopyQueryCompletion(dst: &mut QueryCompletion, src: &QueryCompletion) {
    dst.commandTag = src.commandTag;
    dst.nprocessed = src.nprocessed;
}

// ---------------------------------------------------------------------------
// CommandTagBehavior table (expanded from cmdtaglist.h).
//
// In C, `name` is a `const char *` and `namelen` is precomputed. Here `name`
// holds a NUL-terminated byte slice (the C string literal including its
// trailing NUL) so we can hand out a `*const c_char` that matches C's storage.
// `namelen` is computed at build time from the literal length.
// ---------------------------------------------------------------------------
struct CommandTagBehavior {
    name: &'static [u8], // NUL-terminated, mirrors C string literal storage
    namelen: u8,
    event_trigger_ok: bool,
    table_rewrite_ok: bool,
    display_rowcount: bool,
}

// Build one table entry. `$name` is the textual tag; we append a NUL so the
// stored bytes are a valid C string and `namelen` excludes the terminator
// (matching `sizeof(name) - 1` in the C macro).
macro_rules! cmdtag_entry {
    ($name:literal, $evtrgok:expr, $rwrok:expr, $rowcnt:expr) => {
        CommandTagBehavior {
            name: concat!($name, "\0").as_bytes(),
            namelen: ($name.len()) as u8,
            event_trigger_ok: $evtrgok,
            table_rewrite_ok: $rwrok,
            display_rowcount: $rowcnt,
        }
    };
}

static TAG_BEHAVIOR: &[CommandTagBehavior] = &[
    cmdtag_entry!("???", false, false, false),
    cmdtag_entry!("ALTER ACCESS METHOD", true, false, false),
    cmdtag_entry!("ALTER AGGREGATE", true, false, false),
    cmdtag_entry!("ALTER CAST", true, false, false),
    cmdtag_entry!("ALTER COLLATION", true, false, false),
    cmdtag_entry!("ALTER CONSTRAINT", true, false, false),
    cmdtag_entry!("ALTER CONVERSION", true, false, false),
    cmdtag_entry!("ALTER DATABASE", false, false, false),
    cmdtag_entry!("ALTER DEFAULT PRIVILEGES", true, false, false),
    cmdtag_entry!("ALTER DOMAIN", true, false, false),
    cmdtag_entry!("ALTER EVENT TRIGGER", false, false, false),
    cmdtag_entry!("ALTER EXTENSION", true, false, false),
    cmdtag_entry!("ALTER FOREIGN DATA WRAPPER", true, false, false),
    cmdtag_entry!("ALTER FOREIGN TABLE", true, false, false),
    cmdtag_entry!("ALTER FUNCTION", true, false, false),
    cmdtag_entry!("ALTER INDEX", true, false, false),
    cmdtag_entry!("ALTER LANGUAGE", true, false, false),
    cmdtag_entry!("ALTER LARGE OBJECT", true, false, false),
    cmdtag_entry!("ALTER MATERIALIZED VIEW", true, true, false),
    cmdtag_entry!("ALTER OPERATOR", true, false, false),
    cmdtag_entry!("ALTER OPERATOR CLASS", true, false, false),
    cmdtag_entry!("ALTER OPERATOR FAMILY", true, false, false),
    cmdtag_entry!("ALTER POLICY", true, false, false),
    cmdtag_entry!("ALTER PROCEDURE", true, false, false),
    cmdtag_entry!("ALTER PUBLICATION", true, false, false),
    cmdtag_entry!("ALTER ROLE", false, false, false),
    cmdtag_entry!("ALTER ROUTINE", true, false, false),
    cmdtag_entry!("ALTER RULE", true, false, false),
    cmdtag_entry!("ALTER SCHEMA", true, false, false),
    cmdtag_entry!("ALTER SEQUENCE", true, false, false),
    cmdtag_entry!("ALTER SERVER", true, false, false),
    cmdtag_entry!("ALTER STATISTICS", true, false, false),
    cmdtag_entry!("ALTER SUBSCRIPTION", true, false, false),
    cmdtag_entry!("ALTER SYSTEM", false, false, false),
    cmdtag_entry!("ALTER TABLE", true, true, false),
    cmdtag_entry!("ALTER TABLESPACE", false, false, false),
    cmdtag_entry!("ALTER TEXT SEARCH CONFIGURATION", true, false, false),
    cmdtag_entry!("ALTER TEXT SEARCH DICTIONARY", true, false, false),
    cmdtag_entry!("ALTER TEXT SEARCH PARSER", true, false, false),
    cmdtag_entry!("ALTER TEXT SEARCH TEMPLATE", true, false, false),
    cmdtag_entry!("ALTER TRANSFORM", true, false, false),
    cmdtag_entry!("ALTER TRIGGER", true, false, false),
    cmdtag_entry!("ALTER TYPE", true, true, false),
    cmdtag_entry!("ALTER USER MAPPING", true, false, false),
    cmdtag_entry!("ALTER VIEW", true, false, false),
    cmdtag_entry!("ANALYZE", false, false, false),
    cmdtag_entry!("BEGIN", false, false, false),
    cmdtag_entry!("CALL", false, false, false),
    cmdtag_entry!("CHECKPOINT", false, false, false),
    cmdtag_entry!("CLOSE", false, false, false),
    cmdtag_entry!("CLOSE CURSOR", false, false, false),
    cmdtag_entry!("CLOSE CURSOR ALL", false, false, false),
    cmdtag_entry!("CLUSTER", false, false, false),
    cmdtag_entry!("COMMENT", true, false, false),
    cmdtag_entry!("COMMIT", false, false, false),
    cmdtag_entry!("COMMIT PREPARED", false, false, false),
    cmdtag_entry!("COPY", false, false, true),
    cmdtag_entry!("COPY FROM", false, false, false),
    cmdtag_entry!("CREATE ACCESS METHOD", true, false, false),
    cmdtag_entry!("CREATE AGGREGATE", true, false, false),
    cmdtag_entry!("CREATE CAST", true, false, false),
    cmdtag_entry!("CREATE COLLATION", true, false, false),
    cmdtag_entry!("CREATE CONSTRAINT", true, false, false),
    cmdtag_entry!("CREATE CONVERSION", true, false, false),
    cmdtag_entry!("CREATE DATABASE", false, false, false),
    cmdtag_entry!("CREATE DOMAIN", true, false, false),
    cmdtag_entry!("CREATE EVENT TRIGGER", false, false, false),
    cmdtag_entry!("CREATE EXTENSION", true, false, false),
    cmdtag_entry!("CREATE FOREIGN DATA WRAPPER", true, false, false),
    cmdtag_entry!("CREATE FOREIGN TABLE", true, false, false),
    cmdtag_entry!("CREATE FUNCTION", true, false, false),
    cmdtag_entry!("CREATE INDEX", true, false, false),
    cmdtag_entry!("CREATE LANGUAGE", true, false, false),
    cmdtag_entry!("CREATE MATERIALIZED VIEW", true, false, false),
    cmdtag_entry!("CREATE OPERATOR", true, false, false),
    cmdtag_entry!("CREATE OPERATOR CLASS", true, false, false),
    cmdtag_entry!("CREATE OPERATOR FAMILY", true, false, false),
    cmdtag_entry!("CREATE POLICY", true, false, false),
    cmdtag_entry!("CREATE PROCEDURE", true, false, false),
    cmdtag_entry!("CREATE PUBLICATION", true, false, false),
    cmdtag_entry!("CREATE ROLE", false, false, false),
    cmdtag_entry!("CREATE ROUTINE", true, false, false),
    cmdtag_entry!("CREATE RULE", true, false, false),
    cmdtag_entry!("CREATE SCHEMA", true, false, false),
    cmdtag_entry!("CREATE SEQUENCE", true, false, false),
    cmdtag_entry!("CREATE SERVER", true, false, false),
    cmdtag_entry!("CREATE STATISTICS", true, false, false),
    cmdtag_entry!("CREATE SUBSCRIPTION", true, false, false),
    cmdtag_entry!("CREATE TABLE", true, false, false),
    cmdtag_entry!("CREATE TABLE AS", true, false, false),
    cmdtag_entry!("CREATE TABLESPACE", false, false, false),
    cmdtag_entry!("CREATE TEXT SEARCH CONFIGURATION", true, false, false),
    cmdtag_entry!("CREATE TEXT SEARCH DICTIONARY", true, false, false),
    cmdtag_entry!("CREATE TEXT SEARCH PARSER", true, false, false),
    cmdtag_entry!("CREATE TEXT SEARCH TEMPLATE", true, false, false),
    cmdtag_entry!("CREATE TRANSFORM", true, false, false),
    cmdtag_entry!("CREATE TRIGGER", true, false, false),
    cmdtag_entry!("CREATE TYPE", true, false, false),
    cmdtag_entry!("CREATE USER MAPPING", true, false, false),
    cmdtag_entry!("CREATE VIEW", true, false, false),
    cmdtag_entry!("DEALLOCATE", false, false, false),
    cmdtag_entry!("DEALLOCATE ALL", false, false, false),
    cmdtag_entry!("DECLARE CURSOR", false, false, false),
    cmdtag_entry!("DELETE", false, false, true),
    cmdtag_entry!("DISCARD", false, false, false),
    cmdtag_entry!("DISCARD ALL", false, false, false),
    cmdtag_entry!("DISCARD PLANS", false, false, false),
    cmdtag_entry!("DISCARD SEQUENCES", false, false, false),
    cmdtag_entry!("DISCARD TEMP", false, false, false),
    cmdtag_entry!("DO", false, false, false),
    cmdtag_entry!("DROP ACCESS METHOD", true, false, false),
    cmdtag_entry!("DROP AGGREGATE", true, false, false),
    cmdtag_entry!("DROP CAST", true, false, false),
    cmdtag_entry!("DROP COLLATION", true, false, false),
    cmdtag_entry!("DROP CONSTRAINT", true, false, false),
    cmdtag_entry!("DROP CONVERSION", true, false, false),
    cmdtag_entry!("DROP DATABASE", false, false, false),
    cmdtag_entry!("DROP DOMAIN", true, false, false),
    cmdtag_entry!("DROP EVENT TRIGGER", false, false, false),
    cmdtag_entry!("DROP EXTENSION", true, false, false),
    cmdtag_entry!("DROP FOREIGN DATA WRAPPER", true, false, false),
    cmdtag_entry!("DROP FOREIGN TABLE", true, false, false),
    cmdtag_entry!("DROP FUNCTION", true, false, false),
    cmdtag_entry!("DROP INDEX", true, false, false),
    cmdtag_entry!("DROP LANGUAGE", true, false, false),
    cmdtag_entry!("DROP MATERIALIZED VIEW", true, false, false),
    cmdtag_entry!("DROP OPERATOR", true, false, false),
    cmdtag_entry!("DROP OPERATOR CLASS", true, false, false),
    cmdtag_entry!("DROP OPERATOR FAMILY", true, false, false),
    cmdtag_entry!("DROP OWNED", true, false, false),
    cmdtag_entry!("DROP POLICY", true, false, false),
    cmdtag_entry!("DROP PROCEDURE", true, false, false),
    cmdtag_entry!("DROP PUBLICATION", true, false, false),
    cmdtag_entry!("DROP ROLE", false, false, false),
    cmdtag_entry!("DROP ROUTINE", true, false, false),
    cmdtag_entry!("DROP RULE", true, false, false),
    cmdtag_entry!("DROP SCHEMA", true, false, false),
    cmdtag_entry!("DROP SEQUENCE", true, false, false),
    cmdtag_entry!("DROP SERVER", true, false, false),
    cmdtag_entry!("DROP STATISTICS", true, false, false),
    cmdtag_entry!("DROP SUBSCRIPTION", true, false, false),
    cmdtag_entry!("DROP TABLE", true, false, false),
    cmdtag_entry!("DROP TABLESPACE", false, false, false),
    cmdtag_entry!("DROP TEXT SEARCH CONFIGURATION", true, false, false),
    cmdtag_entry!("DROP TEXT SEARCH DICTIONARY", true, false, false),
    cmdtag_entry!("DROP TEXT SEARCH PARSER", true, false, false),
    cmdtag_entry!("DROP TEXT SEARCH TEMPLATE", true, false, false),
    cmdtag_entry!("DROP TRANSFORM", true, false, false),
    cmdtag_entry!("DROP TRIGGER", true, false, false),
    cmdtag_entry!("DROP TYPE", true, false, false),
    cmdtag_entry!("DROP USER MAPPING", true, false, false),
    cmdtag_entry!("DROP VIEW", true, false, false),
    cmdtag_entry!("EXECUTE", false, false, false),
    cmdtag_entry!("EXPLAIN", false, false, false),
    cmdtag_entry!("FETCH", false, false, true),
    cmdtag_entry!("GRANT", true, false, false),
    cmdtag_entry!("GRANT ROLE", false, false, false),
    cmdtag_entry!("IMPORT FOREIGN SCHEMA", true, false, false),
    cmdtag_entry!("INSERT", false, false, true),
    cmdtag_entry!("LISTEN", false, false, false),
    cmdtag_entry!("LOAD", false, false, false),
    cmdtag_entry!("LOCK TABLE", false, false, false),
    cmdtag_entry!("LOGIN", true, false, false),
    cmdtag_entry!("MERGE", false, false, true),
    cmdtag_entry!("MOVE", false, false, true),
    cmdtag_entry!("NOTIFY", false, false, false),
    cmdtag_entry!("PREPARE", false, false, false),
    cmdtag_entry!("PREPARE TRANSACTION", false, false, false),
    cmdtag_entry!("REASSIGN OWNED", false, false, false),
    cmdtag_entry!("REFRESH MATERIALIZED VIEW", true, false, false),
    cmdtag_entry!("REINDEX", true, false, false),
    cmdtag_entry!("RELEASE", false, false, false),
    cmdtag_entry!("RESET", false, false, false),
    cmdtag_entry!("REVOKE", true, false, false),
    cmdtag_entry!("REVOKE ROLE", false, false, false),
    cmdtag_entry!("ROLLBACK", false, false, false),
    cmdtag_entry!("ROLLBACK PREPARED", false, false, false),
    cmdtag_entry!("SAVEPOINT", false, false, false),
    cmdtag_entry!("SECURITY LABEL", true, false, false),
    cmdtag_entry!("SELECT", false, false, true),
    cmdtag_entry!("SELECT FOR KEY SHARE", false, false, false),
    cmdtag_entry!("SELECT FOR NO KEY UPDATE", false, false, false),
    cmdtag_entry!("SELECT FOR SHARE", false, false, false),
    cmdtag_entry!("SELECT FOR UPDATE", false, false, false),
    cmdtag_entry!("SELECT INTO", true, false, false),
    cmdtag_entry!("SET", false, false, false),
    cmdtag_entry!("SET CONSTRAINTS", false, false, false),
    cmdtag_entry!("SHOW", false, false, false),
    cmdtag_entry!("START TRANSACTION", false, false, false),
    cmdtag_entry!("TRUNCATE TABLE", false, false, false),
    cmdtag_entry!("UNLISTEN", false, false, false),
    cmdtag_entry!("UPDATE", false, false, true),
    cmdtag_entry!("VACUUM", false, false, false),
];

#[inline]
fn tag_index(tag: CommandTag) -> usize {
    tag as usize
}

/// InitializeQueryCompletion (cmdtag.c).
pub fn InitializeQueryCompletion(qc: &mut QueryCompletion) {
    qc.commandTag = CommandTag::CMDTAG_UNKNOWN;
    qc.nprocessed = 0;
}

/// GetCommandTagName (cmdtag.c). Returns the C string for the tag name.
#[no_mangle]
pub fn GetCommandTagName(commandTag: CommandTag) -> *const c_char {
    TAG_BEHAVIOR[tag_index(commandTag)].name.as_ptr() as *const c_char
}

/// GetCommandTagNameAndLen (cmdtag.c). Writes the name length into `len` and
/// returns the C string for the tag name.
pub fn GetCommandTagNameAndLen(commandTag: CommandTag, len: *mut Size) -> *const c_char {
    let b = &TAG_BEHAVIOR[tag_index(commandTag)];
    unsafe {
        *len = b.namelen as Size;
    }
    b.name.as_ptr() as *const c_char
}

/// command_tag_display_rowcount (cmdtag.c).
pub fn command_tag_display_rowcount(commandTag: CommandTag) -> bool {
    TAG_BEHAVIOR[tag_index(commandTag)].display_rowcount
}

/// command_tag_event_trigger_ok (cmdtag.c).
pub fn command_tag_event_trigger_ok(commandTag: CommandTag) -> bool {
    TAG_BEHAVIOR[tag_index(commandTag)].event_trigger_ok
}

/// command_tag_table_rewrite_ok (cmdtag.c).
pub fn command_tag_table_rewrite_ok(commandTag: CommandTag) -> bool {
    TAG_BEHAVIOR[tag_index(commandTag)].table_rewrite_ok
}

/// CommandTag value for a given list index. Inverse of `tag as usize`.
#[inline]
fn command_tag_from_index(idx: usize) -> CommandTag {
    // SAFETY: idx is always within 0..TAG_BEHAVIOR.len() (the enum and the table
    // are generated from the same list, in the same order), and CommandTag is a
    // contiguous repr(C) enum starting at 0.
    unsafe { core::mem::transmute::<u32, CommandTag>(idx as u32) }
}

/// ASCII case-insensitive comparison of a C string against a known-ASCII table
/// entry. Mirrors pg_strcasecmp's behavior for the ASCII tag names used here
/// (avoids pulling in the full utils/adt/pg_locale machinery). Returns the
/// sign-style result of comparing `a` (caller string) against `b` (table name).
unsafe fn pg_ascii_strcasecmp(a: *const c_char, b: &[u8]) -> c_int {
    let mut i = 0usize;
    loop {
        let ca = *a.add(i) as u8;
        // `b` includes its trailing NUL, so indexing is in range.
        let cb = b[i];
        let la = ca.to_ascii_uppercase();
        let lb = cb.to_ascii_uppercase();
        if la != lb {
            return la as c_int - lb as c_int;
        }
        if ca == 0 {
            return 0;
        }
        i += 1;
    }
}

/// GetCommandTagEnum (cmdtag.c).
///
/// Returns the CommandTag matching `commandname`, or CMDTAG_UNKNOWN if not
/// recognized. The C original binary-searches the alphabetically sorted table
/// using pg_strcasecmp; here we perform a case-insensitive linear scan over the
/// same table, which yields identical results (the names are unique).
pub fn GetCommandTagEnum(commandname: *const c_char) -> CommandTag {
    if commandname.is_null() || unsafe { *commandname } == 0 {
        return CommandTag::CMDTAG_UNKNOWN;
    }
    for (idx, b) in TAG_BEHAVIOR.iter().enumerate() {
        if unsafe { pg_ascii_strcasecmp(commandname, b.name) } == 0 {
            return command_tag_from_index(idx);
        }
    }
    CommandTag::CMDTAG_UNKNOWN
}

// MAXINT8LEN: maximum length of the decimal text of a 64-bit integer (from
// c.h / "INT64_MIN" = "-9223372036854775808"), excluding the NUL. Used to size
// the assertion in BuildQueryCompletionString.
const MAXINT8LEN: usize = 20;

// STUB: pg_ulltoa_n (utils/adt/numutils.c). The real routine writes the decimal
// text of `value` into `str` WITHOUT a NUL terminator and returns the number of
// bytes written. Not yet ported; minimally implemented here so
// BuildQueryCompletionString is fully functional. TODO: replace with the ported
// numutils.c version (which uses a fast lookup-table conversion).
unsafe fn pg_ulltoa_n(value: uint64, s: *mut c_char) -> c_int {
    // Render decimal digits MSB-first into a temporary, then copy out.
    let mut tmp = [0u8; 20];
    let mut v = value;
    let mut n = 0usize;
    if v == 0 {
        tmp[0] = b'0';
        n = 1;
    } else {
        // Fill from the end.
        let mut buf = [0u8; 20];
        let mut len = 0usize;
        while v > 0 {
            buf[len] = b'0' + (v % 10) as u8;
            v /= 10;
            len += 1;
        }
        for i in 0..len {
            tmp[i] = buf[len - 1 - i];
        }
        n = len;
    }
    for i in 0..n {
        *s.add(i) = tmp[i] as c_char;
    }
    n as c_int
}

/// BuildQueryCompletionString (cmdtag.c).
///
/// Builds a string containing the command tag name plus, for tags with
/// `display_rowcount`, the QueryCompletion's `nprocessed`. Returns the strlen of
/// the constructed string. The caller must ensure `buff` is at least
/// COMPLETION_TAG_BUFSIZE bytes.
///
/// Depends on the STUBBED `pg_ulltoa_n` above.
pub unsafe fn BuildQueryCompletionString(
    buff: *mut c_char,
    qc: &QueryCompletion,
    nameonly: bool,
) -> Size {
    let tag = qc.commandTag;
    let mut taglen: Size = 0;
    let tagname = GetCommandTagNameAndLen(tag, &mut taglen as *mut Size);

    // tagname is plain ASCII; no encoding conversion required.
    memcpy(buff as *mut c_void, tagname as *const c_void, taglen as usize);
    let mut bufp = buff.add(taglen as usize);

    // Ensure the tagname isn't long enough to overrun the buffer.
    Assert!(taglen as usize <= COMPLETION_TAG_BUFSIZE - MAXINT8LEN - 4);

    // For wire-protocol compatibility, INSERT writes a "0" (legacy Oid slot)
    // before the row count.
    if command_tag_display_rowcount(tag) && !nameonly {
        if tag == CommandTag::CMDTAG_INSERT {
            *bufp = b' ' as c_char;
            bufp = bufp.add(1);
            *bufp = b'0' as c_char;
            bufp = bufp.add(1);
        }
        *bufp = b' ' as c_char;
        bufp = bufp.add(1);
        bufp = bufp.add(pg_ulltoa_n(qc.nprocessed, bufp) as usize);
    }

    // NUL terminate.
    *bufp = 0;

    Assert!((bufp as usize - buff as usize) == strlen(buff));

    (bufp as usize - buff as usize) as Size
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::ffi::CStr;

    fn name_str(tag: CommandTag) -> &'static str {
        let p = GetCommandTagName(tag);
        unsafe { CStr::from_ptr(p) }.to_str().unwrap()
    }

    #[test]
    fn select_name() {
        assert_eq!(name_str(CommandTag::CMDTAG_SELECT), "SELECT");
    }

    #[test]
    fn unknown_name() {
        assert_eq!(name_str(CommandTag::CMDTAG_UNKNOWN), "???");
    }

    #[test]
    fn round_trip_insert() {
        let s = b"INSERT\0";
        let tag = GetCommandTagEnum(s.as_ptr() as *const c_char);
        assert_eq!(tag, CommandTag::CMDTAG_INSERT);
    }

    #[test]
    fn round_trip_case_insensitive() {
        let s = b"insert\0";
        assert_eq!(
            GetCommandTagEnum(s.as_ptr() as *const c_char),
            CommandTag::CMDTAG_INSERT
        );
    }

    #[test]
    fn enum_unknown_for_garbage() {
        let s = b"NOT A REAL TAG\0";
        assert_eq!(
            GetCommandTagEnum(s.as_ptr() as *const c_char),
            CommandTag::CMDTAG_UNKNOWN
        );
        assert_eq!(
            GetCommandTagEnum(b"\0".as_ptr() as *const c_char),
            CommandTag::CMDTAG_UNKNOWN
        );
        assert_eq!(GetCommandTagEnum(null()), CommandTag::CMDTAG_UNKNOWN);
    }

    #[test]
    fn display_rowcount_flags() {
        assert!(command_tag_display_rowcount(CommandTag::CMDTAG_SELECT));
        assert!(command_tag_display_rowcount(CommandTag::CMDTAG_INSERT));
        assert!(!command_tag_display_rowcount(CommandTag::CMDTAG_VACUUM));
    }

    #[test]
    fn behavior_flags() {
        assert!(command_tag_table_rewrite_ok(CommandTag::CMDTAG_ALTER_TABLE));
        assert!(!command_tag_table_rewrite_ok(CommandTag::CMDTAG_SELECT));
        assert!(command_tag_event_trigger_ok(CommandTag::CMDTAG_COMMENT));
        assert!(!command_tag_event_trigger_ok(CommandTag::CMDTAG_SELECT));
    }

    #[test]
    fn name_and_len() {
        let mut len: Size = 0;
        let p = GetCommandTagNameAndLen(CommandTag::CMDTAG_SELECT, &mut len as *mut Size);
        assert_eq!(len, 6);
        let s = unsafe { CStr::from_ptr(p) }.to_str().unwrap();
        assert_eq!(s, "SELECT");
    }

    #[test]
    fn table_len_matches_enum() {
        // CMDTAG_VACUUM is the last variant; its index must be table.len()-1.
        assert_eq!(
            CommandTag::CMDTAG_VACUUM as usize,
            TAG_BEHAVIOR.len() - 1
        );
    }

    #[test]
    fn build_completion_insert() {
        let mut buff = [0i8; COMPLETION_TAG_BUFSIZE];
        let qc = QueryCompletion {
            commandTag: CommandTag::CMDTAG_INSERT,
            nprocessed: 5,
        };
        let n = unsafe { BuildQueryCompletionString(buff.as_mut_ptr(), &qc, false) };
        let s = unsafe { CStr::from_ptr(buff.as_ptr()) }.to_str().unwrap();
        assert_eq!(s, "INSERT 0 5");
        assert_eq!(n as usize, s.len());
    }

    #[test]
    fn build_completion_select_nameonly() {
        let mut buff = [0i8; COMPLETION_TAG_BUFSIZE];
        let qc = QueryCompletion {
            commandTag: CommandTag::CMDTAG_SELECT,
            nprocessed: 42,
        };
        let n = unsafe { BuildQueryCompletionString(buff.as_mut_ptr(), &qc, true) };
        let s = unsafe { CStr::from_ptr(buff.as_ptr()) }.to_str().unwrap();
        assert_eq!(s, "SELECT");
        assert_eq!(n as usize, 6);
    }

    #[test]
    fn build_completion_select_rowcount() {
        let mut buff = [0i8; COMPLETION_TAG_BUFSIZE];
        let qc = QueryCompletion {
            commandTag: CommandTag::CMDTAG_SELECT,
            nprocessed: 42,
        };
        unsafe { BuildQueryCompletionString(buff.as_mut_ptr(), &qc, false) };
        let s = unsafe { CStr::from_ptr(buff.as_ptr()) }.to_str().unwrap();
        assert_eq!(s, "SELECT 42");
    }
}
