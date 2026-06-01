//! tcop/cmdtaglist.h - the master list of command tags (X-macro database).
//!
//! In C this header is a pure X-macro "list" file with deliberately no include
//! guard.  It contains a sequence of
//!   `PG_CMDTAG(symname, name, event_trigger_ok, table_rewrite_ok, rowcount)`
//! invocations; the caller defines the `PG_CMDTAG` macro before `#include`-ing
//! this file to extract whatever projection it needs.  cmdtag.h builds the
//! `CommandTag` enum out of `symname`, and cmdtag.c builds the
//! `tag_behavior[]` (`commandTags`) table out of all five fields.
//!
//! Rust has no textual-include / X-macro mechanism, so this module materializes
//! the two projections that the rest of the tree actually consumes:
//!   * the `CMDTAG_*` integer constants (their values are defined by the *order*
//!     of the entries below - the C enum assigns consecutive values starting at
//!     0 for CMDTAG_UNKNOWN),
//!   * a `CommandTagBehavior` const table carrying the textual name and the
//!     three boolean flags of each tag, for cmdtag.c / tooling use.
//!
//! IMPORTANT (from the C header): the entries must be sorted alphabetically on
//! their textual name so that `GetCommandTagEnum()` can bsearch on it.  Do not
//! reorder.

/// cmdtag.h: command tag identifier.  Re-declared locally to keep this module
/// self-contained; the canonical definition is the `CommandTag` C enum that
/// cmdtag.h builds from this list.  As with all ported C enums this is a plain
/// integer alias with `pub const` variants, not a Rust enum.
// TODO: dedup CommandTag with tcop/cmdtag.rs (the cmdtag.h projection) once
// that header is wired up.
pub type CommandTag = std::ffi::c_int;

//
// Command tag identifiers.
//
// These mirror the `CommandTag` enum that cmdtag.h builds out of this list via
// the `PG_CMDTAG(symname, ...)` X-macro: each `symname` becomes the next
// consecutive enum value, starting at 0 for CMDTAG_UNKNOWN.
//

pub const CMDTAG_UNKNOWN: CommandTag = 0;
pub const CMDTAG_ALTER_ACCESS_METHOD: CommandTag = 1;
pub const CMDTAG_ALTER_AGGREGATE: CommandTag = 2;
pub const CMDTAG_ALTER_CAST: CommandTag = 3;
pub const CMDTAG_ALTER_COLLATION: CommandTag = 4;
pub const CMDTAG_ALTER_CONSTRAINT: CommandTag = 5;
pub const CMDTAG_ALTER_CONVERSION: CommandTag = 6;
pub const CMDTAG_ALTER_DATABASE: CommandTag = 7;
pub const CMDTAG_ALTER_DEFAULT_PRIVILEGES: CommandTag = 8;
pub const CMDTAG_ALTER_DOMAIN: CommandTag = 9;
pub const CMDTAG_ALTER_EVENT_TRIGGER: CommandTag = 10;
pub const CMDTAG_ALTER_EXTENSION: CommandTag = 11;
pub const CMDTAG_ALTER_FOREIGN_DATA_WRAPPER: CommandTag = 12;
pub const CMDTAG_ALTER_FOREIGN_TABLE: CommandTag = 13;
pub const CMDTAG_ALTER_FUNCTION: CommandTag = 14;
pub const CMDTAG_ALTER_INDEX: CommandTag = 15;
pub const CMDTAG_ALTER_LANGUAGE: CommandTag = 16;
pub const CMDTAG_ALTER_LARGE_OBJECT: CommandTag = 17;
pub const CMDTAG_ALTER_MATERIALIZED_VIEW: CommandTag = 18;
pub const CMDTAG_ALTER_OPERATOR: CommandTag = 19;
pub const CMDTAG_ALTER_OPERATOR_CLASS: CommandTag = 20;
pub const CMDTAG_ALTER_OPERATOR_FAMILY: CommandTag = 21;
pub const CMDTAG_ALTER_POLICY: CommandTag = 22;
pub const CMDTAG_ALTER_PROCEDURE: CommandTag = 23;
pub const CMDTAG_ALTER_PUBLICATION: CommandTag = 24;
pub const CMDTAG_ALTER_ROLE: CommandTag = 25;
pub const CMDTAG_ALTER_ROUTINE: CommandTag = 26;
pub const CMDTAG_ALTER_RULE: CommandTag = 27;
pub const CMDTAG_ALTER_SCHEMA: CommandTag = 28;
pub const CMDTAG_ALTER_SEQUENCE: CommandTag = 29;
pub const CMDTAG_ALTER_SERVER: CommandTag = 30;
pub const CMDTAG_ALTER_STATISTICS: CommandTag = 31;
pub const CMDTAG_ALTER_SUBSCRIPTION: CommandTag = 32;
pub const CMDTAG_ALTER_SYSTEM: CommandTag = 33;
pub const CMDTAG_ALTER_TABLE: CommandTag = 34;
pub const CMDTAG_ALTER_TABLESPACE: CommandTag = 35;
pub const CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION: CommandTag = 36;
pub const CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY: CommandTag = 37;
pub const CMDTAG_ALTER_TEXT_SEARCH_PARSER: CommandTag = 38;
pub const CMDTAG_ALTER_TEXT_SEARCH_TEMPLATE: CommandTag = 39;
pub const CMDTAG_ALTER_TRANSFORM: CommandTag = 40;
pub const CMDTAG_ALTER_TRIGGER: CommandTag = 41;
pub const CMDTAG_ALTER_TYPE: CommandTag = 42;
pub const CMDTAG_ALTER_USER_MAPPING: CommandTag = 43;
pub const CMDTAG_ALTER_VIEW: CommandTag = 44;
pub const CMDTAG_ANALYZE: CommandTag = 45;
pub const CMDTAG_BEGIN: CommandTag = 46;
pub const CMDTAG_CALL: CommandTag = 47;
pub const CMDTAG_CHECKPOINT: CommandTag = 48;
pub const CMDTAG_CLOSE: CommandTag = 49;
pub const CMDTAG_CLOSE_CURSOR: CommandTag = 50;
pub const CMDTAG_CLOSE_CURSOR_ALL: CommandTag = 51;
pub const CMDTAG_CLUSTER: CommandTag = 52;
pub const CMDTAG_COMMENT: CommandTag = 53;
pub const CMDTAG_COMMIT: CommandTag = 54;
pub const CMDTAG_COMMIT_PREPARED: CommandTag = 55;
pub const CMDTAG_COPY: CommandTag = 56;
pub const CMDTAG_COPY_FROM: CommandTag = 57;
pub const CMDTAG_CREATE_ACCESS_METHOD: CommandTag = 58;
pub const CMDTAG_CREATE_AGGREGATE: CommandTag = 59;
pub const CMDTAG_CREATE_CAST: CommandTag = 60;
pub const CMDTAG_CREATE_COLLATION: CommandTag = 61;
pub const CMDTAG_CREATE_CONSTRAINT: CommandTag = 62;
pub const CMDTAG_CREATE_CONVERSION: CommandTag = 63;
pub const CMDTAG_CREATE_DATABASE: CommandTag = 64;
pub const CMDTAG_CREATE_DOMAIN: CommandTag = 65;
pub const CMDTAG_CREATE_EVENT_TRIGGER: CommandTag = 66;
pub const CMDTAG_CREATE_EXTENSION: CommandTag = 67;
pub const CMDTAG_CREATE_FOREIGN_DATA_WRAPPER: CommandTag = 68;
pub const CMDTAG_CREATE_FOREIGN_TABLE: CommandTag = 69;
pub const CMDTAG_CREATE_FUNCTION: CommandTag = 70;
pub const CMDTAG_CREATE_INDEX: CommandTag = 71;
pub const CMDTAG_CREATE_LANGUAGE: CommandTag = 72;
pub const CMDTAG_CREATE_MATERIALIZED_VIEW: CommandTag = 73;
pub const CMDTAG_CREATE_OPERATOR: CommandTag = 74;
pub const CMDTAG_CREATE_OPERATOR_CLASS: CommandTag = 75;
pub const CMDTAG_CREATE_OPERATOR_FAMILY: CommandTag = 76;
pub const CMDTAG_CREATE_POLICY: CommandTag = 77;
pub const CMDTAG_CREATE_PROCEDURE: CommandTag = 78;
pub const CMDTAG_CREATE_PUBLICATION: CommandTag = 79;
pub const CMDTAG_CREATE_ROLE: CommandTag = 80;
pub const CMDTAG_CREATE_ROUTINE: CommandTag = 81;
pub const CMDTAG_CREATE_RULE: CommandTag = 82;
pub const CMDTAG_CREATE_SCHEMA: CommandTag = 83;
pub const CMDTAG_CREATE_SEQUENCE: CommandTag = 84;
pub const CMDTAG_CREATE_SERVER: CommandTag = 85;
pub const CMDTAG_CREATE_STATISTICS: CommandTag = 86;
pub const CMDTAG_CREATE_SUBSCRIPTION: CommandTag = 87;
pub const CMDTAG_CREATE_TABLE: CommandTag = 88;
pub const CMDTAG_CREATE_TABLE_AS: CommandTag = 89;
pub const CMDTAG_CREATE_TABLESPACE: CommandTag = 90;
pub const CMDTAG_CREATE_TEXT_SEARCH_CONFIGURATION: CommandTag = 91;
pub const CMDTAG_CREATE_TEXT_SEARCH_DICTIONARY: CommandTag = 92;
pub const CMDTAG_CREATE_TEXT_SEARCH_PARSER: CommandTag = 93;
pub const CMDTAG_CREATE_TEXT_SEARCH_TEMPLATE: CommandTag = 94;
pub const CMDTAG_CREATE_TRANSFORM: CommandTag = 95;
pub const CMDTAG_CREATE_TRIGGER: CommandTag = 96;
pub const CMDTAG_CREATE_TYPE: CommandTag = 97;
pub const CMDTAG_CREATE_USER_MAPPING: CommandTag = 98;
pub const CMDTAG_CREATE_VIEW: CommandTag = 99;
pub const CMDTAG_DEALLOCATE: CommandTag = 100;
pub const CMDTAG_DEALLOCATE_ALL: CommandTag = 101;
pub const CMDTAG_DECLARE_CURSOR: CommandTag = 102;
pub const CMDTAG_DELETE: CommandTag = 103;
pub const CMDTAG_DISCARD: CommandTag = 104;
pub const CMDTAG_DISCARD_ALL: CommandTag = 105;
pub const CMDTAG_DISCARD_PLANS: CommandTag = 106;
pub const CMDTAG_DISCARD_SEQUENCES: CommandTag = 107;
pub const CMDTAG_DISCARD_TEMP: CommandTag = 108;
pub const CMDTAG_DO: CommandTag = 109;
pub const CMDTAG_DROP_ACCESS_METHOD: CommandTag = 110;
pub const CMDTAG_DROP_AGGREGATE: CommandTag = 111;
pub const CMDTAG_DROP_CAST: CommandTag = 112;
pub const CMDTAG_DROP_COLLATION: CommandTag = 113;
pub const CMDTAG_DROP_CONSTRAINT: CommandTag = 114;
pub const CMDTAG_DROP_CONVERSION: CommandTag = 115;
pub const CMDTAG_DROP_DATABASE: CommandTag = 116;
pub const CMDTAG_DROP_DOMAIN: CommandTag = 117;
pub const CMDTAG_DROP_EVENT_TRIGGER: CommandTag = 118;
pub const CMDTAG_DROP_EXTENSION: CommandTag = 119;
pub const CMDTAG_DROP_FOREIGN_DATA_WRAPPER: CommandTag = 120;
pub const CMDTAG_DROP_FOREIGN_TABLE: CommandTag = 121;
pub const CMDTAG_DROP_FUNCTION: CommandTag = 122;
pub const CMDTAG_DROP_INDEX: CommandTag = 123;
pub const CMDTAG_DROP_LANGUAGE: CommandTag = 124;
pub const CMDTAG_DROP_MATERIALIZED_VIEW: CommandTag = 125;
pub const CMDTAG_DROP_OPERATOR: CommandTag = 126;
pub const CMDTAG_DROP_OPERATOR_CLASS: CommandTag = 127;
pub const CMDTAG_DROP_OPERATOR_FAMILY: CommandTag = 128;
pub const CMDTAG_DROP_OWNED: CommandTag = 129;
pub const CMDTAG_DROP_POLICY: CommandTag = 130;
pub const CMDTAG_DROP_PROCEDURE: CommandTag = 131;
pub const CMDTAG_DROP_PUBLICATION: CommandTag = 132;
pub const CMDTAG_DROP_ROLE: CommandTag = 133;
pub const CMDTAG_DROP_ROUTINE: CommandTag = 134;
pub const CMDTAG_DROP_RULE: CommandTag = 135;
pub const CMDTAG_DROP_SCHEMA: CommandTag = 136;
pub const CMDTAG_DROP_SEQUENCE: CommandTag = 137;
pub const CMDTAG_DROP_SERVER: CommandTag = 138;
pub const CMDTAG_DROP_STATISTICS: CommandTag = 139;
pub const CMDTAG_DROP_SUBSCRIPTION: CommandTag = 140;
pub const CMDTAG_DROP_TABLE: CommandTag = 141;
pub const CMDTAG_DROP_TABLESPACE: CommandTag = 142;
pub const CMDTAG_DROP_TEXT_SEARCH_CONFIGURATION: CommandTag = 143;
pub const CMDTAG_DROP_TEXT_SEARCH_DICTIONARY: CommandTag = 144;
pub const CMDTAG_DROP_TEXT_SEARCH_PARSER: CommandTag = 145;
pub const CMDTAG_DROP_TEXT_SEARCH_TEMPLATE: CommandTag = 146;
pub const CMDTAG_DROP_TRANSFORM: CommandTag = 147;
pub const CMDTAG_DROP_TRIGGER: CommandTag = 148;
pub const CMDTAG_DROP_TYPE: CommandTag = 149;
pub const CMDTAG_DROP_USER_MAPPING: CommandTag = 150;
pub const CMDTAG_DROP_VIEW: CommandTag = 151;
pub const CMDTAG_EXECUTE: CommandTag = 152;
pub const CMDTAG_EXPLAIN: CommandTag = 153;
pub const CMDTAG_FETCH: CommandTag = 154;
pub const CMDTAG_GRANT: CommandTag = 155;
pub const CMDTAG_GRANT_ROLE: CommandTag = 156;
pub const CMDTAG_IMPORT_FOREIGN_SCHEMA: CommandTag = 157;
pub const CMDTAG_INSERT: CommandTag = 158;
pub const CMDTAG_LISTEN: CommandTag = 159;
pub const CMDTAG_LOAD: CommandTag = 160;
pub const CMDTAG_LOCK_TABLE: CommandTag = 161;
pub const CMDTAG_LOGIN: CommandTag = 162;
pub const CMDTAG_MERGE: CommandTag = 163;
pub const CMDTAG_MOVE: CommandTag = 164;
pub const CMDTAG_NOTIFY: CommandTag = 165;
pub const CMDTAG_PREPARE: CommandTag = 166;
pub const CMDTAG_PREPARE_TRANSACTION: CommandTag = 167;
pub const CMDTAG_REASSIGN_OWNED: CommandTag = 168;
pub const CMDTAG_REFRESH_MATERIALIZED_VIEW: CommandTag = 169;
pub const CMDTAG_REINDEX: CommandTag = 170;
pub const CMDTAG_RELEASE: CommandTag = 171;
pub const CMDTAG_RESET: CommandTag = 172;
pub const CMDTAG_REVOKE: CommandTag = 173;
pub const CMDTAG_REVOKE_ROLE: CommandTag = 174;
pub const CMDTAG_ROLLBACK: CommandTag = 175;
pub const CMDTAG_ROLLBACK_PREPARED: CommandTag = 176;
pub const CMDTAG_SAVEPOINT: CommandTag = 177;
pub const CMDTAG_SECURITY_LABEL: CommandTag = 178;
pub const CMDTAG_SELECT: CommandTag = 179;
pub const CMDTAG_SELECT_FOR_KEY_SHARE: CommandTag = 180;
pub const CMDTAG_SELECT_FOR_NO_KEY_UPDATE: CommandTag = 181;
pub const CMDTAG_SELECT_FOR_SHARE: CommandTag = 182;
pub const CMDTAG_SELECT_FOR_UPDATE: CommandTag = 183;
pub const CMDTAG_SELECT_INTO: CommandTag = 184;
pub const CMDTAG_SET: CommandTag = 185;
pub const CMDTAG_SET_CONSTRAINTS: CommandTag = 186;
pub const CMDTAG_SHOW: CommandTag = 187;
pub const CMDTAG_START_TRANSACTION: CommandTag = 188;
pub const CMDTAG_TRUNCATE_TABLE: CommandTag = 189;
pub const CMDTAG_UNLISTEN: CommandTag = 190;
pub const CMDTAG_UPDATE: CommandTag = 191;
pub const CMDTAG_VACUUM: CommandTag = 192;

/// One past the last command tag (the `COMMAND_TAG_NEXTTAG` of cmdtag.h's
/// `CommandTag` enum).  With the 193 entries above, the next id is 193.
pub const COMMAND_TAG_NEXTTAG: CommandTag = 193;

/// A single row of the C `cmdtaglist.h` X-macro table: the arguments of one
/// `PG_CMDTAG(symname, name, event_trigger_ok, table_rewrite_ok, rowcount)`.
/// This is the `CommandTagBehavior` struct that cmdtag.c builds the
/// `tag_behavior[]` (`commandTags`) array from.
#[derive(Clone, Copy, Debug)]
pub struct CommandTagBehavior {
    /// The `CommandTag` enum value (equal to this entry's position in the list).
    pub tag: CommandTag,
    /// The textual name of the command tag (e.g. "ALTER TABLE").
    pub name: &'static str,
    /// True if this command can be used as an event trigger filter (the C
    /// `event_trigger_ok` field).
    pub event_trigger_ok: bool,
    /// True if this command can be a table-rewrite event trigger target (the C
    /// `table_rewrite_ok` field).
    pub table_rewrite_ok: bool,
    /// True if this command reports a row count in its completion tag (the C
    /// `display_rowcount` / `rowcount` field).
    pub display_rowcount: bool,
}

/// The command tag list, verbatim from tcop/cmdtaglist.h.  Indexed by
/// `CommandTag` (each entry's `tag` equals its index).  Entries are sorted
/// alphabetically by `name` so cmdtag.c can bsearch on the textual name; do not
/// reorder.  This is the Rust projection of the X-macro sequence; cmdtag.c's
/// `commandTags[]` is built from the same data.
pub static COMMAND_TAGS: [CommandTagBehavior; COMMAND_TAG_NEXTTAG as usize] = [
    // tag, name, event_trigger_ok, table_rewrite_ok, display_rowcount
    CommandTagBehavior { tag: CMDTAG_UNKNOWN, name: "???", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_ACCESS_METHOD, name: "ALTER ACCESS METHOD", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_AGGREGATE, name: "ALTER AGGREGATE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_CAST, name: "ALTER CAST", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_COLLATION, name: "ALTER COLLATION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_CONSTRAINT, name: "ALTER CONSTRAINT", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_CONVERSION, name: "ALTER CONVERSION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_DATABASE, name: "ALTER DATABASE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_DEFAULT_PRIVILEGES, name: "ALTER DEFAULT PRIVILEGES", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_DOMAIN, name: "ALTER DOMAIN", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_EVENT_TRIGGER, name: "ALTER EVENT TRIGGER", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_EXTENSION, name: "ALTER EXTENSION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_FOREIGN_DATA_WRAPPER, name: "ALTER FOREIGN DATA WRAPPER", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_FOREIGN_TABLE, name: "ALTER FOREIGN TABLE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_FUNCTION, name: "ALTER FUNCTION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_INDEX, name: "ALTER INDEX", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_LANGUAGE, name: "ALTER LANGUAGE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_LARGE_OBJECT, name: "ALTER LARGE OBJECT", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_MATERIALIZED_VIEW, name: "ALTER MATERIALIZED VIEW", event_trigger_ok: true, table_rewrite_ok: true, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_OPERATOR, name: "ALTER OPERATOR", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_OPERATOR_CLASS, name: "ALTER OPERATOR CLASS", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_OPERATOR_FAMILY, name: "ALTER OPERATOR FAMILY", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_POLICY, name: "ALTER POLICY", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_PROCEDURE, name: "ALTER PROCEDURE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_PUBLICATION, name: "ALTER PUBLICATION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_ROLE, name: "ALTER ROLE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_ROUTINE, name: "ALTER ROUTINE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_RULE, name: "ALTER RULE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_SCHEMA, name: "ALTER SCHEMA", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_SEQUENCE, name: "ALTER SEQUENCE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_SERVER, name: "ALTER SERVER", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_STATISTICS, name: "ALTER STATISTICS", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_SUBSCRIPTION, name: "ALTER SUBSCRIPTION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_SYSTEM, name: "ALTER SYSTEM", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_TABLE, name: "ALTER TABLE", event_trigger_ok: true, table_rewrite_ok: true, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_TABLESPACE, name: "ALTER TABLESPACE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_TEXT_SEARCH_CONFIGURATION, name: "ALTER TEXT SEARCH CONFIGURATION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_TEXT_SEARCH_DICTIONARY, name: "ALTER TEXT SEARCH DICTIONARY", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_TEXT_SEARCH_PARSER, name: "ALTER TEXT SEARCH PARSER", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_TEXT_SEARCH_TEMPLATE, name: "ALTER TEXT SEARCH TEMPLATE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_TRANSFORM, name: "ALTER TRANSFORM", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_TRIGGER, name: "ALTER TRIGGER", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_TYPE, name: "ALTER TYPE", event_trigger_ok: true, table_rewrite_ok: true, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_USER_MAPPING, name: "ALTER USER MAPPING", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ALTER_VIEW, name: "ALTER VIEW", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ANALYZE, name: "ANALYZE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_BEGIN, name: "BEGIN", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CALL, name: "CALL", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CHECKPOINT, name: "CHECKPOINT", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CLOSE, name: "CLOSE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CLOSE_CURSOR, name: "CLOSE CURSOR", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CLOSE_CURSOR_ALL, name: "CLOSE CURSOR ALL", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CLUSTER, name: "CLUSTER", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_COMMENT, name: "COMMENT", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_COMMIT, name: "COMMIT", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_COMMIT_PREPARED, name: "COMMIT PREPARED", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_COPY, name: "COPY", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: true },
    CommandTagBehavior { tag: CMDTAG_COPY_FROM, name: "COPY FROM", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_ACCESS_METHOD, name: "CREATE ACCESS METHOD", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_AGGREGATE, name: "CREATE AGGREGATE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_CAST, name: "CREATE CAST", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_COLLATION, name: "CREATE COLLATION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_CONSTRAINT, name: "CREATE CONSTRAINT", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_CONVERSION, name: "CREATE CONVERSION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_DATABASE, name: "CREATE DATABASE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_DOMAIN, name: "CREATE DOMAIN", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_EVENT_TRIGGER, name: "CREATE EVENT TRIGGER", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_EXTENSION, name: "CREATE EXTENSION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_FOREIGN_DATA_WRAPPER, name: "CREATE FOREIGN DATA WRAPPER", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_FOREIGN_TABLE, name: "CREATE FOREIGN TABLE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_FUNCTION, name: "CREATE FUNCTION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_INDEX, name: "CREATE INDEX", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_LANGUAGE, name: "CREATE LANGUAGE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_MATERIALIZED_VIEW, name: "CREATE MATERIALIZED VIEW", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_OPERATOR, name: "CREATE OPERATOR", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_OPERATOR_CLASS, name: "CREATE OPERATOR CLASS", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_OPERATOR_FAMILY, name: "CREATE OPERATOR FAMILY", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_POLICY, name: "CREATE POLICY", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_PROCEDURE, name: "CREATE PROCEDURE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_PUBLICATION, name: "CREATE PUBLICATION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_ROLE, name: "CREATE ROLE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_ROUTINE, name: "CREATE ROUTINE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_RULE, name: "CREATE RULE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_SCHEMA, name: "CREATE SCHEMA", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_SEQUENCE, name: "CREATE SEQUENCE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_SERVER, name: "CREATE SERVER", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_STATISTICS, name: "CREATE STATISTICS", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_SUBSCRIPTION, name: "CREATE SUBSCRIPTION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_TABLE, name: "CREATE TABLE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_TABLE_AS, name: "CREATE TABLE AS", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_TABLESPACE, name: "CREATE TABLESPACE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_TEXT_SEARCH_CONFIGURATION, name: "CREATE TEXT SEARCH CONFIGURATION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_TEXT_SEARCH_DICTIONARY, name: "CREATE TEXT SEARCH DICTIONARY", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_TEXT_SEARCH_PARSER, name: "CREATE TEXT SEARCH PARSER", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_TEXT_SEARCH_TEMPLATE, name: "CREATE TEXT SEARCH TEMPLATE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_TRANSFORM, name: "CREATE TRANSFORM", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_TRIGGER, name: "CREATE TRIGGER", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_TYPE, name: "CREATE TYPE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_USER_MAPPING, name: "CREATE USER MAPPING", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_CREATE_VIEW, name: "CREATE VIEW", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DEALLOCATE, name: "DEALLOCATE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DEALLOCATE_ALL, name: "DEALLOCATE ALL", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DECLARE_CURSOR, name: "DECLARE CURSOR", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DELETE, name: "DELETE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: true },
    CommandTagBehavior { tag: CMDTAG_DISCARD, name: "DISCARD", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DISCARD_ALL, name: "DISCARD ALL", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DISCARD_PLANS, name: "DISCARD PLANS", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DISCARD_SEQUENCES, name: "DISCARD SEQUENCES", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DISCARD_TEMP, name: "DISCARD TEMP", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DO, name: "DO", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_ACCESS_METHOD, name: "DROP ACCESS METHOD", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_AGGREGATE, name: "DROP AGGREGATE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_CAST, name: "DROP CAST", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_COLLATION, name: "DROP COLLATION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_CONSTRAINT, name: "DROP CONSTRAINT", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_CONVERSION, name: "DROP CONVERSION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_DATABASE, name: "DROP DATABASE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_DOMAIN, name: "DROP DOMAIN", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_EVENT_TRIGGER, name: "DROP EVENT TRIGGER", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_EXTENSION, name: "DROP EXTENSION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_FOREIGN_DATA_WRAPPER, name: "DROP FOREIGN DATA WRAPPER", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_FOREIGN_TABLE, name: "DROP FOREIGN TABLE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_FUNCTION, name: "DROP FUNCTION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_INDEX, name: "DROP INDEX", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_LANGUAGE, name: "DROP LANGUAGE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_MATERIALIZED_VIEW, name: "DROP MATERIALIZED VIEW", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_OPERATOR, name: "DROP OPERATOR", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_OPERATOR_CLASS, name: "DROP OPERATOR CLASS", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_OPERATOR_FAMILY, name: "DROP OPERATOR FAMILY", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_OWNED, name: "DROP OWNED", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_POLICY, name: "DROP POLICY", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_PROCEDURE, name: "DROP PROCEDURE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_PUBLICATION, name: "DROP PUBLICATION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_ROLE, name: "DROP ROLE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_ROUTINE, name: "DROP ROUTINE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_RULE, name: "DROP RULE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_SCHEMA, name: "DROP SCHEMA", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_SEQUENCE, name: "DROP SEQUENCE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_SERVER, name: "DROP SERVER", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_STATISTICS, name: "DROP STATISTICS", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_SUBSCRIPTION, name: "DROP SUBSCRIPTION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_TABLE, name: "DROP TABLE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_TABLESPACE, name: "DROP TABLESPACE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_TEXT_SEARCH_CONFIGURATION, name: "DROP TEXT SEARCH CONFIGURATION", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_TEXT_SEARCH_DICTIONARY, name: "DROP TEXT SEARCH DICTIONARY", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_TEXT_SEARCH_PARSER, name: "DROP TEXT SEARCH PARSER", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_TEXT_SEARCH_TEMPLATE, name: "DROP TEXT SEARCH TEMPLATE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_TRANSFORM, name: "DROP TRANSFORM", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_TRIGGER, name: "DROP TRIGGER", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_TYPE, name: "DROP TYPE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_USER_MAPPING, name: "DROP USER MAPPING", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_DROP_VIEW, name: "DROP VIEW", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_EXECUTE, name: "EXECUTE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_EXPLAIN, name: "EXPLAIN", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_FETCH, name: "FETCH", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: true },
    CommandTagBehavior { tag: CMDTAG_GRANT, name: "GRANT", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_GRANT_ROLE, name: "GRANT ROLE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_IMPORT_FOREIGN_SCHEMA, name: "IMPORT FOREIGN SCHEMA", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_INSERT, name: "INSERT", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: true },
    CommandTagBehavior { tag: CMDTAG_LISTEN, name: "LISTEN", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_LOAD, name: "LOAD", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_LOCK_TABLE, name: "LOCK TABLE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_LOGIN, name: "LOGIN", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_MERGE, name: "MERGE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: true },
    CommandTagBehavior { tag: CMDTAG_MOVE, name: "MOVE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: true },
    CommandTagBehavior { tag: CMDTAG_NOTIFY, name: "NOTIFY", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_PREPARE, name: "PREPARE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_PREPARE_TRANSACTION, name: "PREPARE TRANSACTION", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_REASSIGN_OWNED, name: "REASSIGN OWNED", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_REFRESH_MATERIALIZED_VIEW, name: "REFRESH MATERIALIZED VIEW", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_REINDEX, name: "REINDEX", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_RELEASE, name: "RELEASE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_RESET, name: "RESET", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_REVOKE, name: "REVOKE", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_REVOKE_ROLE, name: "REVOKE ROLE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ROLLBACK, name: "ROLLBACK", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_ROLLBACK_PREPARED, name: "ROLLBACK PREPARED", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_SAVEPOINT, name: "SAVEPOINT", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_SECURITY_LABEL, name: "SECURITY LABEL", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_SELECT, name: "SELECT", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: true },
    CommandTagBehavior { tag: CMDTAG_SELECT_FOR_KEY_SHARE, name: "SELECT FOR KEY SHARE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_SELECT_FOR_NO_KEY_UPDATE, name: "SELECT FOR NO KEY UPDATE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_SELECT_FOR_SHARE, name: "SELECT FOR SHARE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_SELECT_FOR_UPDATE, name: "SELECT FOR UPDATE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_SELECT_INTO, name: "SELECT INTO", event_trigger_ok: true, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_SET, name: "SET", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_SET_CONSTRAINTS, name: "SET CONSTRAINTS", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_SHOW, name: "SHOW", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_START_TRANSACTION, name: "START TRANSACTION", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_TRUNCATE_TABLE, name: "TRUNCATE TABLE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_UNLISTEN, name: "UNLISTEN", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
    CommandTagBehavior { tag: CMDTAG_UPDATE, name: "UPDATE", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: true },
    CommandTagBehavior { tag: CMDTAG_VACUUM, name: "VACUUM", event_trigger_ok: false, table_rewrite_ok: false, display_rowcount: false },
];
