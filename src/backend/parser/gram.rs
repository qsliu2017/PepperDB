//! The lalrpop-generated parser for gram.lalrpop (the gram.y analog).
//!
//! lalrpop emits the parser into OUT_DIR at build time (see build.rs,
//! `lalrpop::process_root`); this module includes it via `lalrpop_mod!`. The
//! generated code does not follow the project's idiomatic/clippy rules, so it is
//! wrapped in the single sanctioned broad clippy allow, scoped to THIS generated
//! module only - the hand-written parser code (scan.rs, parser.rs, scansup.rs)
//! stays 0/0 under the normal policy.
#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::unwrap_used,
    clippy::expect_used,
    unused_qualifications,
    reason = "lalrpop-generated parser; not subject to the project lint policy"
)]

lalrpop_util::lalrpop_mod!(gram_generated, "/backend/parser/gram.rs");

pub use gram_generated::*;
