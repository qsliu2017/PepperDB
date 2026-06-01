//! executor/spi_priv.rs - Server Programming Interface private declarations.
//!
//! 1:1 translation of postgres/src/include/executor/spi_priv.h (PG 18.3).

use std::ffi::c_int;
use std::ffi::c_void;

use crate::c::uint64;
use crate::c::SubTransactionId;
use crate::lib::ilist::slist_head;
use crate::nodes::pg_list::List;
use crate::postgres_ext::Oid;
use crate::utils::palloc::MemoryContext;

// QueryEnvironment is defined in utils/misc/queryenvironment.rs (queryenvironment.h).
use crate::utils::misc::queryenvironment::QueryEnvironment;

// ParserSetupHook is defined in nodes/params.rs (params.h).
use crate::nodes::params::ParserSetupHook;

// ---------------------------------------------------------------------------
// Types pulled in from executor/spi.h (not yet translated as its own module).
// Defined locally here so spi_priv translates standalone; dedup when spi.h lands.
// ---------------------------------------------------------------------------

// TODO: dedup when spi.h lands - SPITupleTable lives in executor/spi.h.
pub type SPITupleTable = *mut c_void;

// TODO: dedup when parser/parser.h lands - RawParseMode is a C enum there.
pub type RawParseMode = c_int;

pub const _SPI_PLAN_MAGIC: c_int = 569278163;

/// _SPI_connection (anonymous struct typedef'd to _SPI_connection in C).
#[repr(C)]
pub struct _SPI_connection {
    /* current results */
    /// by Executor
    pub processed: uint64,
    /// tuptable currently being built
    pub tuptable: *mut SPITupleTable,

    /// subtransaction in which current Executor call was started
    pub execSubid: SubTransactionId,

    /* resources of this execution context */
    /// list of all live SPITupleTables
    pub tuptables: slist_head,
    /// procedure context
    pub procCxt: MemoryContext,
    /// executor context
    pub execCxt: MemoryContext,
    /// context of SPI_connect's caller
    pub savedcxt: MemoryContext,
    /// ID of connecting subtransaction
    pub connectSubid: SubTransactionId,
    /// query environment setup for SPI level
    pub queryEnv: *mut QueryEnvironment,

    /* transaction management support */
    /// atomic execution context, does not allow transactions
    pub atomic: bool,
    /// SPI-managed transaction boundary, skip cleanup
    pub internal_xact: bool,

    /* saved values of API global variables for previous nesting level */
    pub outer_processed: uint64,
    pub outer_tuptable: *mut SPITupleTable,
    pub outer_result: c_int,
}

/// _SPI_plan
#[repr(C)]
pub struct _SPI_plan {
    /// should equal _SPI_PLAN_MAGIC
    pub magic: c_int,
    /// saved or unsaved plan?
    pub saved: bool,
    /// one-shot plan?
    pub oneshot: bool,
    /// one CachedPlanSource per parsetree
    pub plancache_list: *mut List,
    /// Context containing _SPI_plan and data
    pub plancxt: MemoryContext,
    /// raw_parser() mode
    pub parse_mode: RawParseMode,
    /// Cursor options used for planning
    pub cursor_options: c_int,
    /// number of plan arguments
    pub nargs: c_int,
    /// Argument types (NULL if nargs is 0)
    pub argtypes: *mut Oid,
    /// alternative parameter spec method
    pub parserSetup: ParserSetupHook,
    pub parserSetupArg: *mut c_void,
}
