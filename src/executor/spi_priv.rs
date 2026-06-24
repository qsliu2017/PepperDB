//! Translated from PostgreSQL src/include/executor/spi_priv.h
//!
//! Server Programming Interface private declarations. In-memory structs (per-
//! connection SPI state and the real `_SPI_plan` body). This RESOLVES the
//! opaque `SPIPlanPtr`/`_SPI_plan` placeholder in `spi.rs`.

use crate::c::SubTransactionId;
use crate::nodes::params::ParserSetupHook;
use crate::parser::parser::RawParseMode;
use crate::postgres_ext::Oid;
use crate::utils::memutils::MemoryContext;
use crate::utils::plancache::CachedPlanSource;
use crate::utils::queryenvironment::QueryEnvironment;
use crate::executor::spi::SPITupleTable;

/// `_SPI_PLAN_MAGIC` -- sanity sentinel stored in `_SPI_plan.magic`.
pub const _SPI_PLAN_MAGIC: i32 = 569278163;

/// Per-connection SPI state (one stack entry per SPI_connect nesting level).
/// In-memory: the `slist_head tuptables` intrusive list of live SPITupleTables
/// becomes an owned `Vec`; the `QueryEnvironment *` becomes an owned `Option`.
pub struct _SPI_connection {
    /// rows processed by the Executor for the current call
    pub processed: u64,
    /// tuptable currently being built (NULL when none) -- TODO(ptr)
    pub tuptable: Option<Box<SPITupleTable>>,

    /// subtransaction in which the current Executor call was started
    pub execSubid: SubTransactionId,

    /// all live SPITupleTables (was an intrusive slist_head) -- TODO(ptr)
    pub tuptables: Vec<Box<SPITupleTable>>,
    /// procedure context
    pub procCxt: MemoryContext,
    /// executor context
    pub execCxt: MemoryContext,
    /// context of SPI_connect's caller
    pub savedcxt: MemoryContext,
    /// ID of connecting subtransaction
    pub connectSubid: SubTransactionId,
    /// query environment setup for this SPI level
    pub queryEnv: Option<Box<QueryEnvironment>>,

    /// atomic execution context, does not allow transactions
    pub atomic: bool,
    /// SPI-managed transaction boundary, skip cleanup
    pub internal_xact: bool,

    /// saved values of API globals for previous nesting level
    pub outer_processed: u64,
    pub outer_tuptable: Option<Box<SPITupleTable>>,
    pub outer_result: i32,
}

/// The real body of an SPI plan (resolves the opaque `SPIPlanPtr` in spi.rs).
///
/// Plans have three states: saved, unsaved, or temporary. `plancxt == NULL`
/// identifies a temporary plan; `saved`/`oneshot` distinguish the others. The
/// `List *plancache_list` (one CachedPlanSource per parsetree) -> `Vec`; the
/// `Oid *argtypes` array -> `Vec<Oid>` (empty when nargs is 0); the
/// `void *parserSetupArg` -> a closure capture, dropped here. TODO(ptr)
pub struct _SPI_plan {
    /// should equal `_SPI_PLAN_MAGIC`
    pub magic: i32,
    /// saved or unsaved plan?
    pub saved: bool,
    /// one-shot plan?
    pub oneshot: bool,
    /// one CachedPlanSource per parsetree -- TODO(ptr)
    pub plancache_list: Vec<Box<CachedPlanSource>>,
    /// context containing this _SPI_plan and its data (None for temporary plans)
    pub plancxt: Option<MemoryContext>,
    /// raw_parser() mode
    pub parse_mode: RawParseMode,
    /// cursor options used for planning
    pub cursor_options: i32,
    /// number of plan arguments
    pub nargs: i32,
    /// argument types (empty when nargs is 0)
    pub argtypes: Vec<Oid>,
    /// alternative parameter-spec method
    pub parserSetup: Option<ParserSetupHook>,
    // parserSetupArg (void *) -> closure capture; dropped here. TODO(ptr)
}

/// Error-context callback argument used by spi.c's internal call paths.
/// (Defined in spi.c, not the header; surfaced here as the SPI-private struct.)
/// `void *arg` callback context -> a captured closure per function-mapping 6.3.
pub struct SPICallbackArg {
    /// query string in flight when the error is reported
    pub query: String,
    /// raw_parser() mode for that query
    pub mode: RawParseMode,
}
