//! Translated from PostgreSQL src/include/executor/instrument.h
//! Run-time statistics collection.

use crate::portability::instr_time::InstrTime;
use bitflags::bitflags;

/// BufferUsage counters; incremented indefinitely, never reset. In-memory.
#[derive(Debug, Clone, Copy, Default)]
pub struct BufferUsage {
    pub shared_blks_hit: i64,
    pub shared_blks_read: i64,
    pub shared_blks_dirtied: i64,
    pub shared_blks_written: i64,
    pub local_blks_hit: i64,
    pub local_blks_read: i64,
    pub local_blks_dirtied: i64,
    pub local_blks_written: i64,
    pub temp_blks_read: i64,
    pub temp_blks_written: i64,
    pub shared_blk_read_time: InstrTime,
    pub shared_blk_write_time: InstrTime,
    pub local_blk_read_time: InstrTime,
    pub local_blk_write_time: InstrTime,
    pub temp_blk_read_time: InstrTime,
    pub temp_blk_write_time: InstrTime,
}

/// WalUsage tracks per-query WAL activity shown by EXPLAIN etc. In-memory.
#[derive(Debug, Clone, Copy, Default)]
pub struct WalUsage {
    pub records: i64,
    pub fpi: i64,
    pub bytes: u64,
    pub buffers_full: i64,
}

bitflags! {
    /// InstrAlloc's instrument_options bitmask. (C: enum InstrumentOption,
    /// each member a single bit; INSTRUMENT_ALL = all bits = PG_INT32_MAX.)
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct InstrumentOption: i32 {
        const TIMER   = 1 << 0;  // needs timer (and row counts)
        const BUFFERS = 1 << 1;  // needs buffer usage
        const ROWS    = 1 << 2;  // needs row count
        const WAL     = 1 << 3;  // needs WAL usage
    }
}

impl InstrumentOption {
    /// C: `INSTRUMENT_ALL = PG_INT32_MAX`. The real flag bits, not a stored bit.
    pub const ALL: Self = Self::all();
}

/// Per-node run-time instrumentation. In-memory.
#[derive(Debug, Clone, Copy)]
pub struct Instrumentation {
    // Parameters set at node creation:
    pub need_timer: bool,
    pub need_bufusage: bool,
    pub need_walusage: bool,
    pub async_mode: bool,
    // Info about current plan cycle:
    pub running: bool,
    pub starttime: InstrTime,
    pub counter: InstrTime,
    pub firsttuple: f64,
    pub tuplecount: f64,
    pub bufusage_start: BufferUsage,
    pub walusage_start: WalUsage,
    // Accumulated statistics across all completed cycles:
    pub startup: f64,
    pub total: f64,
    pub ntuples: f64,
    pub ntuples2: f64,
    pub nloops: f64,
    pub nfiltered1: f64,
    pub nfiltered2: f64,
    pub bufusage: BufferUsage,
    pub walusage: WalUsage,
}

/// C: WorkerInstrumentation with a FLEXIBLE_ARRAY_MEMBER -> Vec. In-memory.
pub struct WorkerInstrumentation {
    pub instrument: Vec<Instrumentation>, // num_workers = instrument.len()
}

// Process-global accumulators. TODO(global): thread through Session context.
pub static mut pgBufferUsage: BufferUsage = BufferUsage {
    shared_blks_hit: 0,
    shared_blks_read: 0,
    shared_blks_dirtied: 0,
    shared_blks_written: 0,
    local_blks_hit: 0,
    local_blks_read: 0,
    local_blks_dirtied: 0,
    local_blks_written: 0,
    temp_blks_read: 0,
    temp_blks_written: 0,
    shared_blk_read_time: InstrTime::zero(),
    shared_blk_write_time: InstrTime::zero(),
    local_blk_read_time: InstrTime::zero(),
    local_blk_write_time: InstrTime::zero(),
    temp_blk_read_time: InstrTime::zero(),
    temp_blk_write_time: InstrTime::zero(),
};
// TODO(global)
pub static mut pgWalUsage: WalUsage = WalUsage {
    records: 0,
    fpi: 0,
    bytes: 0,
    buffers_full: 0,
};

pub fn InstrAlloc(n: i32, instrument_options: InstrumentOption, async_mode: bool) -> Vec<Instrumentation> {
    unimplemented!()
}
pub fn InstrInit(instr: &mut Instrumentation, instrument_options: InstrumentOption) {
    unimplemented!()
}
pub fn InstrStartNode(instr: &mut Instrumentation) {
    unimplemented!()
}
pub fn InstrStopNode(instr: &mut Instrumentation, n_tuples: f64) {
    unimplemented!()
}
pub fn InstrUpdateTupleCount(instr: &mut Instrumentation, n_tuples: f64) {
    unimplemented!()
}
pub fn InstrEndLoop(instr: &mut Instrumentation) {
    unimplemented!()
}
pub fn InstrAggNode(dst: &mut Instrumentation, add: &Instrumentation) {
    unimplemented!()
}
pub fn InstrStartParallelQuery() {
    unimplemented!()
}
pub fn InstrEndParallelQuery(bufusage: &mut BufferUsage, walusage: &mut WalUsage) {
    unimplemented!()
}
pub fn InstrAccumParallelQuery(bufusage: &mut BufferUsage, walusage: &mut WalUsage) {
    unimplemented!()
}
pub fn BufferUsageAccumDiff(dst: &mut BufferUsage, add: &BufferUsage, sub: &BufferUsage) {
    unimplemented!()
}
pub fn WalUsageAccumDiff(dst: &mut WalUsage, add: &WalUsage, sub: &WalUsage) {
    unimplemented!()
}
