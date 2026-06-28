//! PG `src/backend/utils/misc/guc.c` -- Grand Unified Configuration.
//!
//! SHIM(step35): defaults-only GUC store; replaced by the real guc.c at step 35.
//!
//! The M1 SELECT path (raw_parser -> parse_analyze -> QueryRewrite ->
//! standard_planner -> Portal/ExecutorRun -> printtup -> pqcomm wire) reads NO
//! GUC, so this shim intentionally carries no state and no accessors yet. The
//! file exists so the `utils/misc/` tree is scaffolded for the real guc.c body,
//! and so a later reachable GUC read has an obvious home. When the first M1+
//! path needs a GUC, add a defaults-returning getter HERE behind the same
//! SHIM(step35) marker; do not build the real GUC system until step 35.
