//! Materialized temporary tuple storage (Material nodes, cursors, ...).
//! Translated from backend/utils/sort/tuplestore.c (disposition: full; the
//! spill-to-disk path stub-calls the still-hollow `storage::buffile`, rules.md
//! s4 -- the in-memory store + read-pointer machinery is complete).
//!
//! Memory model (rules.md s10): PG stores `void **memtuples` (palloc'd
//! MinimalTuple chunks). MinimalTuple is an opaque raw stub in this port and the
//! `TupleTableSlot` already carries owned `Vec<Datum>`/`Vec<bool>`, so a stored
//! tuple is an owned [`StoredTuple`] (the deformed row), not serialized bytes.
//! The `memtuples` array becomes an owned `Vec<StoredTuple>`; the read-pointer
//! array a `Vec<TSReadPointer>`. No raw pointers, genuinely `Send`.

use crate::access::tupdesc::TupleDesc;
use crate::backend::executor::execTuples::exec_store_virtual_tuple;
use crate::executor::tuptable::{slot_getallattrs, ExecClearTuple, TupleTableSlot};
use crate::postgres::Datum;
use crate::utils::elog::ERROR;

// EXEC_FLAG_* eflags relevant to the store (executor.h is not yet translated;
// these are the read-capability flags the store understands).
pub const EXEC_FLAG_REWIND: i32 = 0x0004;
pub const EXEC_FLAG_BACKWARD: i32 = 0x0008;
pub const EXEC_FLAG_MARK: i32 = 0x0010;

/// One stored tuple: the deformed row (PG keeps a flat MinimalTuple chunk).
/// By-reference values are deep-copied into `owned` at store time: PG's
/// MinimalTuple is self-contained, so the owned-row model must own the by-ref
/// datum bytes too (the source slot's memory is recycled after the put).
pub struct StoredTuple {
    pub values: Vec<Datum>,
    pub isnull: Vec<bool>,
    /// (attr index, payload) backing each by-ref value in `values`.
    owned: Vec<(usize, Box<[u8]>)>,
}

impl StoredTuple {
    /// Deep-copy a deformed row: by-ref non-null datums (per `tdesc`) are copied
    /// into owned buffers and `values` re-pointed at them.
    fn from_row(values: &[Datum], isnull: &[bool], tdesc: &TupleDesc) -> Self {
        let mut t = Self {
            values: values.to_vec(),
            isnull: isnull.to_vec(),
            owned: Vec::new(),
        };
        let natts = (tdesc.natts as usize).min(t.values.len());
        for i in 0..natts {
            let attr = tdesc.compact_attr(i);
            if t.isnull[i] || attr.attbyval {
                continue;
            }
            // cstring (-2): datum_copy_owned only takes -1/positive; pass strlen+1.
            let typ_len = if attr.attlen == -2 {
                let p = crate::postgres::DatumGetPointer(t.values[i]).cast::<u8>();
                let mut len = 0usize;
                // SAFETY: a non-null cstring datum is NUL-terminated.
                while unsafe { *p.add(len) } != 0 {
                    len += 1;
                }
                i32::try_from(len + 1).unwrap_or(i32::MAX)
            } else {
                i32::from(attr.attlen)
            };
            let (d, buf) = crate::utils::datum::datum_copy_owned(t.values[i], false, typ_len);
            if let Some(b) = buf {
                t.values[i] = d;
                t.owned.push((i, b));
            }
        }
        t
    }
}

/// Persisted state of a Tuplestore (C `TupStoreStatus`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TupStoreStatus {
    InMem,     // tuples still fit in memory
    WriteFile, // writing to temp file
    ReadFile,  // reading from temp file
}

/// State of one read pointer. In InMem state `current` is the next array index
/// to read; the file/offset fields back the on-disk states.
#[derive(Clone, Copy)]
struct TSReadPointer {
    eflags: i32,
    eof_reached: bool,
    current: usize, // next array index to read
    file: i32,      // temp file# (disk states)
    offset: i64,    // byte offset in file (disk states)
}

/// Private state of a tuplestore operation.
pub struct Tuplestorestate {
    status: TupStoreStatus,
    eflags: i32,        // OR of pointers' eflags
    backward: bool,     // store extra length words in file?
    #[allow(dead_code, reason = "interXact governs temp-file persistence; only the disk path reads it")]
    inter_xact: bool,
    truncated: bool,    // tuplestore_trim has removed tuples?
    used_disk: bool,    // for tuplestore_get_stats
    max_space: i64,     // for tuplestore_get_stats
    avail_mem: i64,     // remaining memory available, bytes
    allowed_mem: i64,   // total memory allowed, bytes
    tuples: i64,        // number of tuples added

    /// In-memory tuples. The first `memtupdeleted` entries are released by trim.
    memtuples: Vec<StoredTuple>,
    memtupdeleted: usize,

    readptrs: Vec<TSReadPointer>,
    activeptr: usize,

    /// Tuple descriptor (the C `void *arg` for the heap variant).
    tupdesc: Option<TupleDesc>,
}

/// Rough per-tuple memory charge (PG counts GetMemoryChunkSpace; we approximate
/// from the deformed-row footprint, which drives the in-memory-vs-spill cutover).
fn stored_tuple_space(t: &StoredTuple) -> i64 {
    let datum_bytes = t.values.len() * core::mem::size_of::<Datum>();
    let null_bytes = t.isnull.len();
    let owned_bytes: usize = t.owned.iter().map(|(_, b)| b.len()).sum();
    (datum_bytes + null_bytes + owned_bytes + core::mem::size_of::<StoredTuple>()) as i64
}

/// C `tuplestore_begin_common`.
fn tuplestore_begin_common(eflags: i32, inter_xact: bool, max_kbytes: i32) -> Tuplestorestate {
    let backward = (eflags & EXEC_FLAG_BACKWARD) != 0;
    Tuplestorestate {
        status: TupStoreStatus::InMem,
        eflags,
        backward,
        inter_xact,
        truncated: false,
        used_disk: false,
        max_space: 0,
        avail_mem: i64::from(max_kbytes) * 1024,
        allowed_mem: i64::from(max_kbytes) * 1024,
        tuples: 0,
        memtuples: Vec::new(),
        memtupdeleted: 0,
        readptrs: vec![TSReadPointer {
            eflags,
            eof_reached: false,
            current: 0,
            file: 0,
            offset: 0,
        }],
        activeptr: 0,
        tupdesc: None,
    }
}

/// C `tuplestore_begin_heap`: a store for heap tuples.
///
/// `random_access` enables both forward and backward reads; `inter_xact` keeps
/// any temp files open across transactions; `max_kbytes` is the in-memory limit.
#[allow(
    clippy::unnecessary_box_returns,
    reason = "PG returns a Tuplestorestate*; callers (Portal/trigger/walreceiver) store Box<Tuplestorestate>"
)]
pub fn tuplestore_begin_heap(
    random_access: bool,
    inter_xact: bool,
    max_kbytes: i32,
) -> Box<Tuplestorestate> {
    let eflags = if random_access {
        EXEC_FLAG_BACKWARD | EXEC_FLAG_REWIND | EXEC_FLAG_MARK
    } else {
        EXEC_FLAG_REWIND
    };
    Box::new(tuplestore_begin_common(eflags, inter_xact, max_kbytes))
}

/// Set the tuple descriptor used to deform stored slots (the C heap variant's
/// `arg`). Not in PG's public API but required by the owned store, which keeps
/// deformed rows rather than opaque MinimalTuple chunks.
pub fn tuplestore_set_tupdesc(state: &mut Tuplestorestate, tupdesc: TupleDesc) {
    state.tupdesc = Some(tupdesc);
}

/// C `tuplestore_set_eflags`: set the capability flags before any tuples go in.
pub fn tuplestore_set_eflags(state: &mut Tuplestorestate, eflags: i32) {
    if state.status != TupStoreStatus::InMem || !state.memtuples.is_empty() {
        crate::elog!(ERROR, "too late to call tuplestore_set_eflags");
    }
    state.readptrs[0].eflags = eflags;
    state.eflags = eflags;
    state.backward = (eflags & EXEC_FLAG_BACKWARD) != 0;
}

/// C `tuplestore_alloc_read_pointer`: allocate another read pointer, returning
/// its index. It initially copies read pointer 0's position.
pub fn tuplestore_alloc_read_pointer(state: &mut Tuplestorestate, eflags: i32) -> i32 {
    if (state.status != TupStoreStatus::InMem || !state.memtuples.is_empty())
        && (state.eflags | eflags) != state.eflags
    {
        crate::elog!(ERROR, "too late to require new tuplestore eflags");
    }
    let mut newptr = state.readptrs[0];
    newptr.eflags = eflags;
    state.readptrs.push(newptr);
    state.eflags |= eflags;
    i32::try_from(state.readptrs.len() - 1).unwrap_or(0)
}

/// C `tuplestore_select_read_pointer`: make the given read pointer active.
pub fn tuplestore_select_read_pointer(state: &mut Tuplestorestate, ptr: i32) {
    let ptr = ptr as usize;
    crate::assert!(ptr < state.readptrs.len());
    if ptr == state.activeptr {
        return;
    }
    // The disk (READFILE) seek/tell handoff stub-calls BufFile and is staged;
    // in-memory the active pointer index is all that changes.
    state.activeptr = ptr;
}

/// C `tuplestore_copy_read_pointer`: copy one read pointer's state to another.
pub fn tuplestore_copy_read_pointer(state: &mut Tuplestorestate, srcptr: i32, destptr: i32) {
    let srcptr = srcptr as usize;
    let destptr = destptr as usize;
    crate::assert!(srcptr < state.readptrs.len());
    crate::assert!(destptr < state.readptrs.len());
    if srcptr == destptr {
        return;
    }
    let src = state.readptrs[srcptr];
    let recompute = state.readptrs[destptr].eflags != src.eflags;
    state.readptrs[destptr] = src;
    if recompute {
        let mut eflags = state.readptrs[0].eflags;
        for rp in &state.readptrs[1..] {
            eflags |= rp.eflags;
        }
        state.eflags = eflags;
    }
}

/// C `tuplestore_tuple_count`: tuples added since creation or the last clear.
pub fn tuplestore_tuple_count(state: &Tuplestorestate) -> i64 {
    state.tuples
}

/// C `tuplestore_ateof`: the active read pointer's eof_reached state.
pub fn tuplestore_ateof(state: &Tuplestorestate) -> bool {
    state.readptrs[state.activeptr].eof_reached
}

/// C `tuplestore_puttupleslot`: deform `slot` and store the row (deep-copied,
/// as PG's ExecCopySlotMinimalTuple does).
pub fn tuplestore_puttupleslot(state: &mut Tuplestorestate, slot: &mut TupleTableSlot) {
    slot_getallattrs(slot);
    let n = slot.nvalid.max(0) as usize;
    let tdesc = slot
        .tupleDescriptor
        .clone()
        .unwrap_or_else(|| unreachable!("tuplestore_puttupleslot: slot has a descriptor"));
    let tuple = StoredTuple::from_row(&slot.values[..n], &slot.isnull[..n], &tdesc);
    tuplestore_puttuple_common(state, tuple);
}

/// C `tuplestore_putvalues`: store a row from raw value/isnull arrays.
pub fn tuplestore_putvalues(
    state: &mut Tuplestorestate,
    tdesc: &TupleDesc,
    values: &[Datum],
    isnull: &[bool],
) {
    let tuple = StoredTuple::from_row(values, isnull, tdesc);
    tuplestore_puttuple_common(state, tuple);
}

/// C `tuplestore_puttuple_common`: the shared put path.
fn tuplestore_puttuple_common(state: &mut Tuplestorestate, tuple: StoredTuple) {
    state.tuples += 1;

    match state.status {
        TupStoreStatus::InMem => {
            // Any non-active read pointer at EOF must be released to track the
            // newly added tuple (see API spec in tuplestore.c).
            let memtupcount = state.memtuples.len();
            for (i, readptr) in state.readptrs.iter_mut().enumerate() {
                if readptr.eof_reached && i != state.activeptr {
                    readptr.eof_reached = false;
                    readptr.current = memtupcount;
                }
            }

            state.avail_mem -= stored_tuple_space(&tuple);
            state.memtuples.push(tuple);

            if state.avail_mem < 0 {
                // Time to spill: the temp-file path stub-calls BufFile (staged).
                dumptuples(state);
            }
        }
        TupStoreStatus::WriteFile => {
            writetup(state, &tuple);
        }
        TupStoreStatus::ReadFile => {
            crate::elog!(ERROR, "invalid tuplestore state");
        }
    }
}

/// C `dumptuples`: switch from the in-memory array to tape-based operation.
/// The BufFile temp-file leaves are hollow, so the real spill is staged here.
fn dumptuples(state: &mut Tuplestorestate) {
    state.used_disk = true;
    state.status = TupStoreStatus::WriteFile;
    unimplemented!("tuplestore spill-to-disk: BufFile temp-file path not yet translated");
}

/// C `writetup_heap`: append a stored tuple to the temp file (staged).
fn writetup(_state: &mut Tuplestorestate, _tuple: &StoredTuple) {
    unimplemented!("tuplestore writetup: BufFile temp-file path not yet translated");
}

/// Internal: advance the active read pointer and return the next tuple (by
/// value -- the owned-row model copies on read, where PG returned a pointer into
/// its internal copy). `None` at end of data. In-memory path is complete; the
/// disk states stub-call BufFile.
/// Advance the active read pointer and return the INDEX of the fetched tuple in
/// `memtuples`. PG's `tuplestore_gettuple` returns a pointer into the store's
/// own memory (`should_free=false`); returning the index keeps that semantic --
/// the caller borrows `state.memtuples[idx]`, so by-ref datums copied out of it
/// stay valid as long as the store does (a returned CLONE would free its owned
/// payloads at drop and leave the slot's datums dangling).
fn tuplestore_gettuple(state: &mut Tuplestorestate, forward: bool) -> Option<usize> {
    let activeptr = state.activeptr;
    crate::assert!(forward || (state.readptrs[activeptr].eflags & EXEC_FLAG_BACKWARD) != 0);

    match state.status {
        TupStoreStatus::InMem => {
            let memtupcount = state.memtuples.len();
            let memtupdeleted = state.memtupdeleted;
            let readptr = &mut state.readptrs[activeptr];
            if forward {
                if readptr.eof_reached {
                    return None;
                }
                if readptr.current < memtupcount {
                    let idx = readptr.current;
                    readptr.current += 1;
                    return Some(idx);
                }
                readptr.eof_reached = true;
                None
            } else {
                if readptr.eof_reached {
                    readptr.current = memtupcount;
                    readptr.eof_reached = false;
                } else {
                    if readptr.current <= memtupdeleted {
                        crate::assert!(!state.truncated);
                        return None;
                    }
                    readptr.current -= 1; // last returned tuple
                }
                if readptr.current <= memtupdeleted {
                    crate::assert!(!state.truncated);
                    return None;
                }
                Some(readptr.current - 1)
            }
        }
        TupStoreStatus::WriteFile | TupStoreStatus::ReadFile => {
            unimplemented!("tuplestore gettuple from disk: BufFile path not yet translated");
        }
    }
}

/// C `tuplestore_gettupleslot`: fetch the next tuple into `slot`. Returns false
/// at end of data. The slot's by-ref datums point into the store's owned row
/// (PG `copy=false` semantics: valid until the store is modified or dropped;
/// `copy` is not needed by any current caller).
pub fn tuplestore_gettupleslot(
    state: &mut Tuplestorestate,
    forward: bool,
    _copy: bool,
    slot: &mut TupleTableSlot,
) -> bool {
    ExecClearTuple(slot);
    if let Some(idx) = tuplestore_gettuple(state, forward) {
        let tuple = &state.memtuples[idx];
        let n = tuple.values.len();
        slot.values[..n].copy_from_slice(&tuple.values);
        slot.isnull[..n].copy_from_slice(&tuple.isnull);
        exec_store_virtual_tuple(slot);
        true
    } else {
        false
    }
}

/// C `tuplestore_advance`: skip one tuple in the given direction.
pub fn tuplestore_advance(state: &mut Tuplestorestate, forward: bool) -> bool {
    tuplestore_gettuple(state, forward).is_some()
}

/// C `tuplestore_skiptuples`: skip `ntuples` in the given direction.
pub fn tuplestore_skiptuples(state: &mut Tuplestorestate, ntuples: i64, forward: bool) -> bool {
    crate::assert!(ntuples >= 0);
    for _ in 0..ntuples {
        if tuplestore_gettuple(state, forward).is_none() {
            return false;
        }
    }
    true
}

/// C `tuplestore_rescan`: rewind the active read pointer to the start.
pub fn tuplestore_rescan(state: &mut Tuplestorestate) {
    let activeptr = state.activeptr;
    crate::assert!((state.readptrs[activeptr].eflags & EXEC_FLAG_REWIND) != 0);
    crate::assert!(!state.truncated);
    match state.status {
        TupStoreStatus::InMem => {
            let readptr = &mut state.readptrs[activeptr];
            readptr.eof_reached = false;
            readptr.current = 0;
        }
        TupStoreStatus::WriteFile => {
            let readptr = &mut state.readptrs[activeptr];
            readptr.eof_reached = false;
            readptr.file = 0;
            readptr.offset = 0;
        }
        TupStoreStatus::ReadFile => {
            unimplemented!("tuplestore_rescan on disk: BufFile path not yet translated");
        }
    }
}

/// C `tuplestore_markpos`: remember the active read pointer's current position.
/// In the owned model this copies the active pointer into a hidden mark slot
/// allocated alongside; PG stores `markpos_*` per active pointer. We mirror PG's
/// scheme: the caller pairs `markpos`/`restorepos` on the active pointer, so we
/// snapshot the active pointer's position into its own backup fields.
pub fn tuplestore_markpos(state: &mut Tuplestorestate) {
    crate::assert!((state.readptrs[state.activeptr].eflags & EXEC_FLAG_MARK) != 0);
    match state.status {
        TupStoreStatus::InMem => {
            let activeptr = state.activeptr;
            let cur = state.readptrs[activeptr].current;
            let eof = state.readptrs[activeptr].eof_reached;
            // stash into the unused file/offset fields (InMem doesn't use them)
            state.readptrs[activeptr].file = i32::try_from(cur).unwrap_or(i32::MAX);
            state.readptrs[activeptr].offset = i64::from(eof);
        }
        TupStoreStatus::WriteFile | TupStoreStatus::ReadFile => {
            unimplemented!("tuplestore_markpos on disk: BufFile path not yet translated");
        }
    }
}

/// C `tuplestore_restorepos`: restore the active read pointer to the last mark.
pub fn tuplestore_restorepos(state: &mut Tuplestorestate) {
    crate::assert!((state.readptrs[state.activeptr].eflags & EXEC_FLAG_MARK) != 0);
    match state.status {
        TupStoreStatus::InMem => {
            let activeptr = state.activeptr;
            let cur = state.readptrs[activeptr].file as usize;
            let eof = state.readptrs[activeptr].offset != 0;
            state.readptrs[activeptr].current = cur;
            state.readptrs[activeptr].eof_reached = eof;
        }
        TupStoreStatus::WriteFile | TupStoreStatus::ReadFile => {
            unimplemented!("tuplestore_restorepos on disk: BufFile path not yet translated");
        }
    }
}

/// C `tuplestore_trim`: drop tuples before the oldest read pointer when no
/// pointer needs REWIND. In-memory only; the slide-down of `memtuples` happens
/// when enough leading entries are dead.
pub fn tuplestore_trim(state: &mut Tuplestorestate) {
    // Only safe to trim when no read pointer requires rewind capability.
    if (state.eflags & EXEC_FLAG_REWIND) != 0 {
        return;
    }
    if state.status != TupStoreStatus::InMem {
        return;
    }
    let oldest = state
        .readptrs
        .iter()
        .map(|rp| if rp.eof_reached { state.memtuples.len() } else { rp.current })
        .min()
        .unwrap_or(0);
    let nremove = oldest.saturating_sub(state.memtupdeleted);
    if nremove == 0 {
        return;
    }
    state.memtuples.drain(0..nremove);
    for rp in &mut state.readptrs {
        if rp.current >= nremove {
            rp.current -= nremove;
        } else {
            rp.current = 0;
        }
    }
    state.truncated = true;
    state.memtupdeleted = 0;
}

/// C `tuplestore_in_memory`: is the store still entirely in memory?
pub fn tuplestore_in_memory(state: &Tuplestorestate) -> bool {
    state.status == TupStoreStatus::InMem
}

/// C `tuplestore_get_stats`: (space-type name, peak space in KB).
pub fn tuplestore_get_stats(state: &Tuplestorestate) -> (&'static str, i64) {
    if state.used_disk {
        ("Disk", (state.max_space + 1023) / 1024)
    } else {
        let used = state.allowed_mem - state.avail_mem;
        ("Memory", (used + 1023) / 1024)
    }
}

/// C `tuplestore_clear`: discard all tuples, reset read pointers to start.
pub fn tuplestore_clear(state: &mut Tuplestorestate) {
    state.memtuples.clear();
    state.memtupdeleted = 0;
    state.tuples = 0;
    state.truncated = false;
    state.avail_mem = state.allowed_mem;
    state.status = TupStoreStatus::InMem;
    for rp in &mut state.readptrs {
        rp.eof_reached = false;
        rp.current = 0;
        rp.file = 0;
        rp.offset = 0;
    }
}

/// C `tuplestore_end`: release the store. Owned `Box` drop frees everything; the
/// temp-file cleanup grows when BufFile lands.
#[allow(
    clippy::boxed_local,
    reason = "consumes the owned Box<Tuplestorestate> callers hold (PG frees the pointer); drop releases it"
)]
pub fn tuplestore_end(_state: Box<Tuplestorestate>) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::tupdesc::TupleDescData;
    use crate::backend::executor::execTuples::make_single_tuple_table_slot;
    use crate::catalog::genbki::INT4OID;
    use crate::executor::tuptable::{tts_empty, TTSOpsVirtual};
    use crate::postgres::{DatumGetInt32, Int32GetDatum};
    use std::sync::Arc;

    fn int4_desc() -> TupleDesc {
        let mut td = TupleDescData::create_template(1);
        td.init_builtin_entry(1, "a", INT4OID, -1, 0);
        td.init_entry_collation(1, crate::postgres_ext::InvalidOid);
        Arc::new(td)
    }

    fn put_int(state: &mut Tuplestorestate, v: i32) {
        let desc = int4_desc();
        tuplestore_putvalues(state, &desc, &[Int32GetDatum(v)], &[false]);
    }

    fn read_all(state: &mut Tuplestorestate) -> Vec<i32> {
        let desc = int4_desc();
        let mut slot = make_single_tuple_table_slot(Some(desc), &TTSOpsVirtual);
        let mut out = Vec::new();
        while tuplestore_gettupleslot(state, true, false, &mut slot) {
            out.push(DatumGetInt32(slot.values[0]));
        }
        out
    }

    #[test]
    fn put_then_read_back_in_order() {
        let mut st = tuplestore_begin_heap(true, false, 1024);
        for v in [10, 20, 30, 40] {
            put_int(&mut st, v);
        }
        assert_eq!(tuplestore_tuple_count(&st), 4);
        assert_eq!(read_all(&mut st), vec![10, 20, 30, 40]);
        assert!(tuplestore_ateof(&st));
        assert!(tuplestore_in_memory(&st));
    }

    #[test]
    fn rescan_re_reads_from_start() {
        let mut st = tuplestore_begin_heap(true, false, 1024);
        for v in [1, 2, 3] {
            put_int(&mut st, v);
        }
        assert_eq!(read_all(&mut st), vec![1, 2, 3]);
        tuplestore_rescan(&mut st);
        assert_eq!(read_all(&mut st), vec![1, 2, 3]);
    }

    #[test]
    fn independent_read_pointers() {
        let mut st = tuplestore_begin_heap(true, false, 1024);
        // Allocate a second read pointer up front (as Material nodes do), so both
        // start at the beginning; the two then advance independently.
        let p1 = tuplestore_alloc_read_pointer(&mut st, EXEC_FLAG_REWIND);
        for v in [5, 6, 7] {
            put_int(&mut st, v);
        }
        let desc = int4_desc();
        let mut slot = make_single_tuple_table_slot(Some(desc), &TTSOpsVirtual);

        // pointer 0 reads the first tuple
        assert!(tuplestore_gettupleslot(&mut st, true, false, &mut slot));
        assert_eq!(DatumGetInt32(slot.values[0]), 5);

        // pointer 1 is independent and still at the start
        tuplestore_select_read_pointer(&mut st, p1);
        assert!(tuplestore_gettupleslot(&mut st, true, false, &mut slot));
        assert_eq!(DatumGetInt32(slot.values[0]), 5);

        // back to pointer 0: it resumes at the second tuple
        tuplestore_select_read_pointer(&mut st, 0);
        assert!(tuplestore_gettupleslot(&mut st, true, false, &mut slot));
        assert_eq!(DatumGetInt32(slot.values[0]), 6);
    }

    #[test]
    fn markpos_restorepos_roundtrip() {
        let mut st = tuplestore_begin_heap(true, false, 1024);
        for v in [100, 200, 300] {
            put_int(&mut st, v);
        }
        let desc = int4_desc();
        let mut slot = make_single_tuple_table_slot(Some(desc), &TTSOpsVirtual);

        assert!(tuplestore_gettupleslot(&mut st, true, false, &mut slot));
        assert_eq!(DatumGetInt32(slot.values[0]), 100);

        // mark here, read one more, then restore and re-read it
        tuplestore_markpos(&mut st);
        assert!(tuplestore_gettupleslot(&mut st, true, false, &mut slot));
        assert_eq!(DatumGetInt32(slot.values[0]), 200);

        tuplestore_restorepos(&mut st);
        assert!(tuplestore_gettupleslot(&mut st, true, false, &mut slot));
        assert_eq!(DatumGetInt32(slot.values[0]), 200);
    }

    #[test]
    fn backward_scan_reads_in_reverse() {
        let mut st = tuplestore_begin_heap(true, false, 1024);
        for v in [1, 2, 3] {
            put_int(&mut st, v);
        }
        let desc = int4_desc();
        let mut slot = make_single_tuple_table_slot(Some(desc), &TTSOpsVirtual);

        // read forward to EOF
        while tuplestore_gettupleslot(&mut st, true, false, &mut slot) {}
        assert!(tuplestore_ateof(&st));

        // now walk backward
        let mut back = Vec::new();
        while tuplestore_gettupleslot(&mut st, false, false, &mut slot) {
            back.push(DatumGetInt32(slot.values[0]));
        }
        assert_eq!(back, vec![3, 2, 1]);
        assert!(tts_empty(&slot));
    }

    #[test]
    fn clear_resets_the_store() {
        let mut st = tuplestore_begin_heap(true, false, 1024);
        for v in [9, 8, 7] {
            put_int(&mut st, v);
        }
        tuplestore_clear(&mut st);
        assert_eq!(tuplestore_tuple_count(&st), 0);
        assert_eq!(read_all(&mut st), Vec::<i32>::new());
    }
}
