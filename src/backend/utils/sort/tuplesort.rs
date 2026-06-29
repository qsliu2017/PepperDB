//! Generalized tuple sorting (in-memory qsort + bounded top-N heap + the
//! external-merge state machine). Translated from backend/utils/sort/tuplesort.c
//! and the heap/datum variants from tuplesortvariants.c (PG 14+ split). The
//! in-memory sort and the bounded (LIMIT) heap are COMPLETE; the external-merge
//! run building + final merge are structurally translated but stub-call the
//! still-hollow logtape/BufFile spill leaves (rules.md s4). Parallel/shared sort
//! is staged. Abbreviated keys are staged (a perf optimization).
//!
//! Memory model (rules.md s10): PG's `SortTuple *memtuples` array of palloc'd
//! MinimalTuple/IndexTuple chunks becomes an owned `Vec<SortTuple>`, where each
//! [`SortTuple`] owns its row ([`SortTupleBody`]) instead of a raw `void *tuple`.
//! The per-key comparators are held in an owned, `Send` [`SortKey`] (the full
//! `SortSupportData` carries raw pointers and is not `Send`; the comparator fn is
//! a plain `fn`). No raw pointers; the whole state is genuinely `Send`.
//!
//! The comparator is resolved from the SortSupport / btree cmp function via the
//! opclass (PG `PrepareSortSupportFromOrderingOp`). That resolver needs the
//! syscache/pg_amop machinery (not yet translated), so `begin_heap`/`begin_datum`
//! stub-call it per s4; tests wire the int4 SortSupport comparator directly.

use crate::access::tupdesc::TupleDesc;
use crate::backend::executor::execTuples::exec_store_virtual_tuple;
use crate::executor::tuptable::{slot_getallattrs, ExecClearTuple, TupleTableSlot};
use crate::postgres::Datum;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::utils::elog::ERROR;
use crate::utils::sortsupport::{SortComparator, SortSupportData};

pub type AttrNumber = i16;

/// Owned, `Send` per-key sort metadata. PG threads a full `SortSupportData` (which
/// carries raw `MemoryContext`/`ssup_extra` pointers and is therefore NOT `Send`);
/// the sort only needs these fields, so we keep an owned copy. The comparator is a
/// plain `fn` (resolved from the opclass / btree cmp via SortSupport), hence `Send`
/// (rules.md s10: per-backend state must be genuinely `Send` without `unsafe`).
#[derive(Clone, Copy)]
pub struct SortKey {
    pub comparator: Option<SortComparator>,
    pub reverse: bool,
    pub nulls_first: bool,
    pub attno: AttrNumber,
    pub collation: Oid,
}

impl SortKey {
    fn empty() -> Self {
        Self {
            comparator: None,
            reverse: false,
            nulls_first: false,
            attno: 0,
            collation: InvalidOid,
        }
    }

    /// C `ApplySortComparator`: 3-way compare honoring NULL ordering + reverse.
    /// The comparator fn wants a `&SortSupportData`; build a throwaway one
    /// carrying the collation it may read (builtin int cmps ignore it).
    fn apply(&self, datum1: Datum, isnull1: bool, datum2: Datum, isnull2: bool) -> i32 {
        if isnull1 {
            return if isnull2 {
                0
            } else if self.nulls_first {
                -1
            } else {
                1
            };
        }
        if isnull2 {
            return if self.nulls_first { 1 } else { -1 };
        }
        let cmp_fn = self
            .comparator
            .unwrap_or_else(|| unreachable!("sort key has a comparator before performsort"));
        let ssup = ssup_for_call(self.collation);
        let mut compare = cmp_fn(datum1, datum2, &ssup);
        if self.reverse {
            compare = crate::c::INVERT_COMPARE_RESULT(compare);
        }
        compare
    }
}

/// A minimal `SortSupportData` for invoking a comparator fn. Only `ssup_collation`
/// is read by collation-sensitive comparators; the builtin int cmps ignore it.
fn ssup_for_call(collation: Oid) -> SortSupportData {
    SortSupportData {
        ssup_cxt: crate::utils::palloc::MemoryContext::default(),
        ssup_collation: collation,
        ssup_reverse: false,
        ssup_nulls_first: false,
        ssup_attno: 0,
        ssup_extra: core::ptr::null_mut(),
        comparator: None,
        abbreviate: false,
        abbrev_converter: None,
        abbrev_abort: None,
        abbrev_full_comparator: None,
    }
}

/// Sort options (C `sortopt`). `TUPLESORT_NONE` == empty.
pub mod sortopt {
    pub const NONE: i32 = 0;
    pub const RANDOMACCESS: i32 = 1 << 0;
    pub const ALLOWBOUNDED: i32 = 1 << 1;
}

/// Persisted sort state (C `TupSortStatus`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TupSortStatus {
    Initial,      // loading tuples; still within memory limit
    Bounded,      // loading tuples into a bounded-size heap
    BuildRuns,    // loading tuples; writing to tape
    SortedInMem,  // sort completed entirely in memory
    SortedOnTape, // sort completed, final run is on tape
    FinalMerge,   // performing final merge on-the-fly
}

/// The owned body of a sortable object (PG's `void *tuple`). A heap sort keeps
/// the deformed row; a datum sort keeps nothing extra (the value lives in
/// `datum1`).
#[derive(Clone)]
pub enum SortTupleBody {
    /// Heap tuple: the full deformed row (used for tie-break columns).
    Heap { values: Vec<Datum>, isnull: Vec<bool> },
    /// Datum sort: no separate storage (single column lives in `datum1`).
    Datum,
}

/// The objects we sort. `datum1`/`isnull1` hold the leading sort key.
#[derive(Clone)]
pub struct SortTuple {
    pub body: SortTupleBody,
    pub datum1: Datum,
    pub isnull1: bool,
    pub srctape: i32,
}

/// Which variant's comparison/serialization rules apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Variant {
    Heap,
    Datum,
}

/// Tuplesort state. The C `TuplesortPublic` vtable + private fields are folded
/// into one owned struct (the variant is an enum, not fn pointers).
pub struct Tuplesortstate {
    status: TupSortStatus,
    variant: Variant,
    sortopt: i32,
    bounded: bool,
    bound: i64,
    bound_used: bool,

    n_keys: usize,
    /// Per-key sort metadata (owned, `Send`). The leading key drives datum1.
    sort_keys: Vec<SortKey>,

    /// In-memory tuples (PG `SortTuple *memtuples`). Owned, no raw pointers.
    memtuples: Vec<SortTuple>,

    avail_mem: i64,
    allowed_mem: i64,
    tuple_mem: i64,

    /// Read cursor for SortedInMem (PG `current`/`eof_reached`).
    current: usize,
    eof_reached: bool,
    markpos: usize,
    markpos_eof: bool,

    /// Heap variant `arg`: the tuple descriptor (for tie-break attrs).
    tupdesc: Option<TupleDesc>,
    /// Datum variant: the sorted column's type length / by-value flag.
    datum_typbyval: bool,
}

// --- comparison ---------------------------------------------------------------

impl Tuplesortstate {
    /// C `COMPARETUP`: leading key via datum1, then the variant's tie-break.
    fn comparetup(&self, a: &SortTuple, b: &SortTuple) -> i32 {
        let key = &self.sort_keys[0];
        let compare = key.apply(a.datum1, a.isnull1, b.datum1, b.isnull1);
        if compare != 0 {
            return compare;
        }
        match self.variant {
            Variant::Datum => 0, // single column; datum1 was authoritative
            Variant::Heap => self.comparetup_heap_tiebreak(a, b),
        }
    }

    /// C `comparetup_heap_tiebreak`: compare the 2nd..nth sort keys from the row.
    fn comparetup_heap_tiebreak(&self, a: &SortTuple, b: &SortTuple) -> i32 {
        let (SortTupleBody::Heap { values: av, isnull: an }, SortTupleBody::Heap { values: bv, isnull: bn }) =
            (&a.body, &b.body)
        else {
            return 0;
        };
        for key in &self.sort_keys[1..] {
            let idx = (key.attno - 1) as usize;
            let compare = key.apply(av[idx], an[idx], bv[idx], bn[idx]);
            if compare != 0 {
                return compare;
            }
        }
        0
    }

    /// C `reversedirection`: flip every key's reverse + nulls-first (bounded heap).
    fn reversedirection(&mut self) {
        for key in &mut self.sort_keys {
            key.reverse = !key.reverse;
            key.nulls_first = !key.nulls_first;
        }
    }
}

/// `LACKMEM`: over the memory budget.
fn lackmem(state: &Tuplesortstate) -> bool {
    state.avail_mem < 0
}

/// Rough per-tuple memory charge (PG counts GetMemoryChunkSpace).
fn sort_tuple_space(t: &SortTuple) -> i64 {
    let body = match &t.body {
        SortTupleBody::Heap { values, isnull } => {
            values.len() * core::mem::size_of::<Datum>() + isnull.len()
        }
        SortTupleBody::Datum => 0,
    };
    (body + core::mem::size_of::<SortTuple>()) as i64
}

// --- begin / common -----------------------------------------------------------

/// C `tuplesort_begin_common`.
fn tuplesort_begin_common(work_mem: i32, sortopt: i32) -> Tuplestorestate_init {
    let allowed = i64::from(work_mem) * 1024;
    Tuplestorestate_init {
        status: TupSortStatus::Initial,
        sortopt,
        avail_mem: allowed,
        allowed_mem: allowed,
    }
}

/// Helper holding the fields `tuplesort_begin_common` sets (avoids a half-built
/// `Tuplesortstate` -- the variant fns fill the rest).
#[allow(non_camel_case_types)]
struct Tuplestorestate_init {
    status: TupSortStatus,
    sortopt: i32,
    avail_mem: i64,
    allowed_mem: i64,
}

/// C `tuplesort_begin_heap`: sort heap rows by `nkeys` sort columns.
///
/// `att_nums[i]`/`sort_operators[i]`/`sort_collations[i]`/`nulls_first[i]` define
/// key `i`. The comparator for each key is resolved from `sort_operators[i]` via
/// `PrepareSortSupportFromOrderingOp` (opclass lookup; staged -- see below).
#[allow(clippy::too_many_arguments, reason = "1:1 with PG tuplesort_begin_heap signature")]
#[allow(
    clippy::unnecessary_box_returns,
    reason = "PG returns a Tuplesortstate*; callers (nodeAgg/index) store Box<Tuplesortstate>"
)]
pub fn tuplesort_begin_heap(
    tup_desc: TupleDesc,
    nkeys: i32,
    att_nums: &[AttrNumber],
    sort_operators: &[Oid],
    sort_collations: &[Oid],
    nulls_first_flags: &[bool],
    work_mem: i32,
    sortopt: i32,
) -> Box<Tuplesortstate> {
    crate::assert!(nkeys > 0);
    let init = tuplesort_begin_common(work_mem, sortopt);
    let nkeys = nkeys as usize;

    let mut sort_keys = Vec::with_capacity(nkeys);
    for i in 0..nkeys {
        sort_keys.push(resolve_sort_key(
            sort_operators[i],
            sort_collations[i],
            nulls_first_flags[i],
            att_nums[i],
        ));
    }

    Box::new(Tuplesortstate {
        status: init.status,
        variant: Variant::Heap,
        sortopt: init.sortopt,
        bounded: false,
        bound: 0,
        bound_used: false,
        n_keys: nkeys,
        sort_keys,
        memtuples: Vec::new(),
        avail_mem: init.avail_mem,
        allowed_mem: init.allowed_mem,
        tuple_mem: 0,
        current: 0,
        eof_reached: false,
        markpos: 0,
        markpos_eof: false,
        tupdesc: Some(tup_desc),
        datum_typbyval: true,
    })
}

/// C `tuplesort_begin_datum`: sort a single column of `datum_type`.
#[allow(
    clippy::unnecessary_box_returns,
    reason = "PG returns a Tuplesortstate*; callers store Box<Tuplesortstate>"
)]
pub fn tuplesort_begin_datum(
    datum_type: Oid,
    sort_operator: Oid,
    sort_collation: Oid,
    nulls_first_flag: bool,
    work_mem: i32,
    sortopt: i32,
) -> Box<Tuplesortstate> {
    let init = tuplesort_begin_common(work_mem, sortopt);
    let (_typlen, typbyval) = crate::backend::utils::cache::lsyscache::get_typlenbyval(datum_type);

    let key = resolve_sort_key(sort_operator, sort_collation, nulls_first_flag, 1);

    Box::new(Tuplesortstate {
        status: init.status,
        variant: Variant::Datum,
        sortopt: init.sortopt,
        bounded: false,
        bound: 0,
        bound_used: false,
        n_keys: 1,
        sort_keys: vec![key],
        memtuples: Vec::new(),
        avail_mem: init.avail_mem,
        allowed_mem: init.allowed_mem,
        tuple_mem: 0,
        current: 0,
        eof_reached: false,
        markpos: 0,
        markpos_eof: false,
        tupdesc: None,
        datum_typbyval: typbyval,
    })
}

/// Resolve a [`SortKey`] for an ordering operator (C `PrepareSortSupportFrom-
/// OrderingOp` + the key field setup). The resolver needs the syscache/pg_amop
/// opclass machinery (not yet translated), so it stub-calls per s4; the resolved
/// `comparator`/`reverse` are read back into the owned `SortKey`. Callers that
/// need a working sort before that machinery lands set the comparator directly.
fn resolve_sort_key(
    ordering_op: Oid,
    collation: Oid,
    nulls_first: bool,
    attno: AttrNumber,
) -> SortKey {
    let mut ssup = ssup_for_call(collation);
    ssup.ssup_nulls_first = nulls_first;
    ssup.ssup_attno = attno;
    crate::utils::sortsupport::PrepareSortSupportFromOrderingOp(ordering_op, &mut ssup);
    SortKey {
        comparator: ssup.comparator,
        reverse: ssup.ssup_reverse,
        nulls_first,
        attno,
        collation,
    }
}

// --- bound (top-N) ------------------------------------------------------------

/// C `tuplesort_set_bound`: switch to a bounded sort returning the N smallest.
pub fn tuplesort_set_bound(state: &mut Tuplesortstate, bound: i64) {
    crate::assert!(state.status == TupSortStatus::Initial);
    crate::assert!(state.memtuples.is_empty());
    crate::assert!(!state.bounded);
    crate::assert!((state.sortopt & sortopt::ALLOWBOUNDED) != 0);
    crate::assert!(bound > 0);
    state.bounded = true;
    state.bound = bound;
}

/// C `tuplesort_used_bound`: did a bounded sort actually use the bound?
pub fn tuplesort_used_bound(state: &Tuplesortstate) -> bool {
    state.bound_used
}

// --- put ----------------------------------------------------------------------

/// Build a `SortTuple` from a deformed row, setting the leading-key datum1.
fn make_heap_sorttuple(state: &Tuplesortstate, values: Vec<Datum>, isnull: Vec<bool>) -> SortTuple {
    let idx = (state.sort_keys[0].attno - 1) as usize;
    let datum1 = values[idx];
    let isnull1 = isnull[idx];
    SortTuple {
        body: SortTupleBody::Heap { values, isnull },
        datum1,
        isnull1,
        srctape: 0,
    }
}

/// C `tuplesort_puttupleslot`: deform `slot` and feed it to the sort.
pub fn tuplesort_puttupleslot(state: &mut Tuplesortstate, slot: &mut TupleTableSlot) {
    slot_getallattrs(slot);
    let n = slot.nvalid.max(0) as usize;
    let stup = make_heap_sorttuple(state, slot.values[..n].to_vec(), slot.isnull[..n].to_vec());
    let tuplen = sort_tuple_space(&stup);
    tuplesort_puttuple_common(state, stup, tuplen);
}

/// C `tuplesort_putdatum`: feed a single value to a datum sort.
pub fn tuplesort_putdatum(state: &mut Tuplesortstate, val: Datum, is_null: bool) {
    let stup = SortTuple {
        body: SortTupleBody::Datum,
        datum1: if is_null { Datum(0) } else { val },
        isnull1: is_null,
        srctape: 0,
    };
    let tuplen = sort_tuple_space(&stup);
    tuplesort_puttuple_common(state, stup, tuplen);
}

/// C `tuplesort_puttuple_common`: the shared accumulation + state machine.
fn tuplesort_puttuple_common(state: &mut Tuplesortstate, tuple: SortTuple, tuplen: i64) {
    state.avail_mem -= tuplen;
    state.tuple_mem += tuplen;

    match state.status {
        TupSortStatus::Initial => {
            state.memtuples.push(tuple);
            let memtupcount = state.memtuples.len() as i64;

            // Switch to a bounded heapsort when we have clearly more input than
            // the bound, or workMem is full and we already have enough.
            if state.bounded
                && (memtupcount > state.bound * 2 || (memtupcount > state.bound && lackmem(state)))
            {
                make_bounded_heap(state);
                return;
            }

            if !lackmem(state) {
                return;
            }
            // Out of memory: switch to tape-based operation (staged spill).
            inittapes(state);
            dumptuples(state, false);
        }
        TupSortStatus::Bounded => {
            // Discard the new tuple unless it beats the current heap max (root).
            if state.comparetup(&tuple, &state.memtuples[0]) <= 0 {
                // new tuple <= top of heap; drop it
            } else {
                state.memtuples[0] = tuple;
                tuplesort_heap_replace_top(state);
            }
        }
        TupSortStatus::BuildRuns => {
            state.memtuples.push(tuple);
            dumptuples(state, false);
        }
        _ => crate::elog!(ERROR, "invalid tuplesort state"),
    }
}

// --- bounded heap (top-N) -----------------------------------------------------

/// C `make_bounded_heap`: convert the unsorted array to a bound-sized max-heap
/// (the direction is reversed so the largest sits at the root for easy eviction).
fn make_bounded_heap(state: &mut Tuplesortstate) {
    crate::assert!(state.status == TupSortStatus::Initial);
    crate::assert!(state.bounded);
    let tupcount = state.memtuples.len();
    crate::assert!(tupcount as i64 >= state.bound);

    state.reversedirection();

    let src = core::mem::take(&mut state.memtuples);
    let bound = state.bound as usize;
    for stup in src {
        if state.memtuples.len() < bound {
            tuplesort_heap_insert(state, stup);
        } else if state.comparetup(&stup, &state.memtuples[0]) <= 0 {
            // larger than the heap max (reversed): discard
        } else {
            state.memtuples[0] = stup;
            tuplesort_heap_replace_top(state);
        }
    }
    crate::assert!(state.memtuples.len() == bound);
    state.status = TupSortStatus::Bounded;
}

/// C `sort_bounded_heap`: drain the bound-sized max-heap (under the reversed
/// direction the root is the largest original value) into ascending order.
///
/// PG unheapifies in place, parking each extracted max in the array slot just
/// freed at the tail; here the owned `Vec` shrinks as we delete the top, so we
/// collect the extracted maxima (largest-first) and reverse to ascending.
fn sort_bounded_heap(state: &mut Tuplesortstate) {
    crate::assert!(state.status == TupSortStatus::Bounded);
    let tupcount = state.memtuples.len();

    let mut desc = Vec::with_capacity(tupcount);
    while !state.memtuples.is_empty() {
        // The root is the current max (reversed order); move it out.
        let last = state.memtuples.len() - 1;
        state.memtuples.swap(0, last);
        let top = state.memtuples.pop().unwrap_or_else(|| unreachable!("heap nonempty"));
        desc.push(top);
        if !state.memtuples.is_empty() {
            // The element now at the root must be sifted down (delete-top).
            tuplesort_heap_replace_top(state);
        }
    }
    desc.reverse();
    state.memtuples = desc;
    crate::assert!(state.memtuples.len() == tupcount);

    state.reversedirection();
    state.status = TupSortStatus::SortedInMem;
    state.bound_used = true;
}

/// C `tuplesort_heap_insert`: sift up a new entry (Knuth 5.2.3 ex. 16).
fn tuplesort_heap_insert(state: &mut Tuplesortstate, tuple: SortTuple) {
    state.memtuples.push(tuple);
    let mut j = state.memtuples.len() - 1;
    while j > 0 {
        let i = (j - 1) >> 1;
        if state.comparetup(&state.memtuples[j], &state.memtuples[i]) >= 0 {
            break;
        }
        state.memtuples.swap(j, i);
        j = i;
    }
}

/// C `tuplesort_heap_replace_top`: the root has been overwritten; sift it down.
fn tuplesort_heap_replace_top(state: &mut Tuplesortstate) {
    let n = state.memtuples.len();
    crate::assert!(n >= 1);
    let mut i = 0usize;
    loop {
        let mut j = 2 * i + 1;
        if j >= n {
            break;
        }
        if j + 1 < n && state.comparetup(&state.memtuples[j], &state.memtuples[j + 1]) > 0 {
            j += 1;
        }
        if state.comparetup(&state.memtuples[i], &state.memtuples[j]) <= 0 {
            break;
        }
        state.memtuples.swap(i, j);
        i = j;
    }
}

// --- external-merge structure (staged) ---------------------------------------

/// C `inittapes`: prepare the logical-tape set for run building. The logtape /
/// BufFile spill leaves are still hollow, so the disk path is staged (s4).
fn inittapes(_state: &mut Tuplesortstate) {
    unimplemented!("tuplesort external sort (inittapes): logtape/BufFile spill not yet translated");
}

/// C `dumptuples`: flush in-memory tuples out to a run on tape (staged).
fn dumptuples(_state: &mut Tuplesortstate, _all: bool) {
    unimplemented!("tuplesort external sort (dumptuples): logtape/BufFile spill not yet translated");
}

/// C `mergeruns`: merge the runs on tape down to the final result (staged).
fn mergeruns(_state: &mut Tuplesortstate) {
    unimplemented!("tuplesort external sort (mergeruns): logtape/BufFile spill not yet translated");
}

// --- performsort --------------------------------------------------------------

/// C `tuplesort_sort_memtuples`: qsort the in-memory array by the comparator.
fn tuplesort_sort_memtuples(state: &mut Tuplesortstate) {
    if state.memtuples.len() > 1 {
        // PG uses a hand-written qsort over SortTuple; the slice sort is the
        // idiomatic owned equivalent (stable is fine -- the tie-break already
        // makes the order total over the sort keys).
        let mut tuples = core::mem::take(&mut state.memtuples);
        tuples.sort_by(|a, b| match state.comparetup(a, b) {
            n if n < 0 => core::cmp::Ordering::Less,
            n if n > 0 => core::cmp::Ordering::Greater,
            _ => core::cmp::Ordering::Equal,
        });
        state.memtuples = tuples;
    }
}

/// C `tuplesort_performsort`: finish accumulating and produce sorted output.
pub fn tuplesort_performsort(state: &mut Tuplesortstate) {
    match state.status {
        TupSortStatus::Initial => {
            // Everything fit in memory: just sort it.
            tuplesort_sort_memtuples(state);
            state.status = TupSortStatus::SortedInMem;
            state.current = 0;
            state.eof_reached = false;
            state.markpos = 0;
            state.markpos_eof = false;
        }
        TupSortStatus::Bounded => {
            // Bounded heap -> sorted array.
            sort_bounded_heap(state);
            state.current = 0;
            state.eof_reached = false;
            state.markpos = 0;
            state.markpos_eof = false;
        }
        TupSortStatus::BuildRuns => {
            dumptuples(state, true);
            mergeruns(state);
            state.eof_reached = false;
            state.markpos = 0;
            state.markpos_eof = false;
        }
        _ => crate::elog!(ERROR, "invalid tuplesort state"),
    }
}

// --- get ----------------------------------------------------------------------

/// C `tuplesort_gettuple_common`: fetch the next sorted tuple, or None at end.
/// The in-memory (SortedInMem) path is complete; the on-tape paths are staged.
fn tuplesort_gettuple_common(state: &mut Tuplesortstate, forward: bool) -> Option<SortTuple> {
    match state.status {
        TupSortStatus::SortedInMem => {
            crate::assert!(forward || (state.sortopt & sortopt::RANDOMACCESS) != 0);
            if forward {
                if state.current < state.memtuples.len() {
                    let t = state.memtuples[state.current].clone();
                    state.current += 1;
                    return Some(t);
                }
                state.eof_reached = true;
                if state.bounded && state.current as i64 >= state.bound {
                    crate::elog!(ERROR, "retrieved too many tuples in a bounded sort");
                }
                None
            } else {
                if state.current == 0 {
                    return None;
                }
                if state.eof_reached {
                    state.eof_reached = false;
                } else {
                    state.current -= 1;
                    if state.current == 0 {
                        return None;
                    }
                }
                Some(state.memtuples[state.current - 1].clone())
            }
        }
        TupSortStatus::SortedOnTape | TupSortStatus::FinalMerge => {
            unimplemented!("tuplesort gettuple from tape: logtape/BufFile path not yet translated");
        }
        _ => {
            crate::elog!(ERROR, "invalid tuplesort state");
            unreachable!()
        }
    }
}

/// C `tuplesort_gettupleslot`: fetch the next sorted row into `slot`.
pub fn tuplesort_gettupleslot(
    state: &mut Tuplesortstate,
    forward: bool,
    _copy: bool,
    slot: &mut TupleTableSlot,
) -> bool {
    match tuplesort_gettuple_common(state, forward) {
        Some(SortTuple { body: SortTupleBody::Heap { values, isnull }, .. }) => {
            ExecClearTuple(slot);
            let n = values.len();
            slot.values[..n].copy_from_slice(&values);
            slot.isnull[..n].copy_from_slice(&isnull);
            exec_store_virtual_tuple(slot);
            true
        }
        Some(SortTuple { body: SortTupleBody::Datum, .. }) => {
            crate::elog!(ERROR, "tuplesort_gettupleslot called on a datum sort");
            unreachable!()
        }
        None => {
            ExecClearTuple(slot);
            false
        }
    }
}

/// C `tuplesort_getdatum`: fetch the next sorted (value, isnull), or None at end.
pub fn tuplesort_getdatum(state: &mut Tuplesortstate, forward: bool) -> Option<(Datum, bool)> {
    let stup = tuplesort_gettuple_common(state, forward)?;
    let _ = state.datum_typbyval; // by-ref copy handling grows with varlena
    Some((stup.datum1, stup.isnull1))
}

/// C `tuplesort_skiptuples`: skip `ntuples` in the given direction.
pub fn tuplesort_skiptuples(state: &mut Tuplesortstate, ntuples: i64, forward: bool) -> bool {
    crate::assert!(ntuples >= 0);
    for _ in 0..ntuples {
        if tuplesort_gettuple_common(state, forward).is_none() {
            return false;
        }
    }
    true
}

/// C `tuplesort_rescan`: rewind the (random-access) sort to the start.
pub fn tuplesort_rescan(state: &mut Tuplesortstate) {
    crate::assert!((state.sortopt & sortopt::RANDOMACCESS) != 0);
    match state.status {
        TupSortStatus::SortedInMem => {
            state.current = 0;
            state.eof_reached = false;
            state.markpos = 0;
            state.markpos_eof = false;
        }
        _ => unimplemented!("tuplesort_rescan on tape: logtape path not yet translated"),
    }
}

/// C `tuplesort_markpos`: remember the current read position.
pub fn tuplesort_markpos(state: &mut Tuplesortstate) {
    crate::assert!((state.sortopt & sortopt::RANDOMACCESS) != 0);
    match state.status {
        TupSortStatus::SortedInMem => {
            state.markpos = state.current;
            state.markpos_eof = state.eof_reached;
        }
        _ => unimplemented!("tuplesort_markpos on tape: logtape path not yet translated"),
    }
}

/// C `tuplesort_restorepos`: restore to the last marked position.
pub fn tuplesort_restorepos(state: &mut Tuplesortstate) {
    crate::assert!((state.sortopt & sortopt::RANDOMACCESS) != 0);
    match state.status {
        TupSortStatus::SortedInMem => {
            state.current = state.markpos;
            state.eof_reached = state.markpos_eof;
        }
        _ => unimplemented!("tuplesort_restorepos on tape: logtape path not yet translated"),
    }
}

/// C `tuplesort_end`: release the sort. Owned `Box` drop frees the rows.
#[allow(
    clippy::boxed_local,
    reason = "consumes the owned Box<Tuplesortstate> callers hold (PG frees the pointer); drop releases it"
)]
pub fn tuplesort_end(_state: Box<Tuplesortstate>) {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::tupdesc::TupleDescData;
    use crate::backend::executor::execTuples::make_single_tuple_table_slot;
    use crate::catalog::genbki::INT4OID;
    use crate::backend::executor::execTuples::exec_store_virtual_tuple;
    use crate::executor::tuptable::TTSOpsVirtual;
    use crate::postgres::{DatumGetInt32, Int32GetDatum};
    use crate::postgres_ext::InvalidOid;
    use crate::utils::sortsupport::ssup_datum_int32_cmp;
    use std::sync::Arc;

    fn int4_desc(natts: i32) -> TupleDesc {
        let mut td = TupleDescData::create_template(natts);
        for i in 1..=natts {
            td.init_builtin_entry(i as i16, "c", INT4OID, -1, 0);
            td.init_entry_collation(i as i16, InvalidOid);
        }
        Arc::new(td)
    }

    /// Wire the int4 btree SortSupport comparator directly (the opclass resolver
    /// `PrepareSortSupportFromOrderingOp` is staged; M5 uses the int4 cmp).
    fn int4_key(attno: i16, reverse: bool, nulls_first: bool) -> SortKey {
        SortKey {
            comparator: Some(ssup_datum_int32_cmp),
            reverse,
            nulls_first,
            attno,
            collation: InvalidOid,
        }
    }

    fn set_int4_key(
        state: &mut Tuplesortstate,
        keyidx: usize,
        attno: i16,
        reverse: bool,
        nulls_first: bool,
    ) {
        state.sort_keys[keyidx] = int4_key(attno, reverse, nulls_first);
    }

    #[allow(clippy::unnecessary_box_returns, reason = "mirrors the boxed public begin_* API")]
    fn begin_int4_heap(reverse: bool, nulls_first: bool, sortopt: i32) -> Box<Tuplesortstate> {
        // Build the state, then overwrite the (staged) comparator with the real
        // int4 one rather than calling the not-yet-translated opclass resolver.
        let desc = int4_desc(1);
        let mut state = Box::new(Tuplesortstate {
            status: TupSortStatus::Initial,
            variant: Variant::Heap,
            sortopt,
            bounded: false,
            bound: 0,
            bound_used: false,
            n_keys: 1,
            sort_keys: vec![SortKey::empty()],
            memtuples: Vec::new(),
            avail_mem: 4096 * 1024,
            allowed_mem: 4096 * 1024,
            tuple_mem: 0,
            current: 0,
            eof_reached: false,
            markpos: 0,
            markpos_eof: false,
            tupdesc: Some(desc),
            datum_typbyval: true,
        });
        set_int4_key(&mut state, 0, 1, reverse, nulls_first);
        state
    }

    fn put_int(state: &mut Tuplesortstate, v: i32, isnull: bool) {
        let desc = int4_desc(1);
        let mut slot = make_single_tuple_table_slot(Some(desc), &TTSOpsVirtual);
        slot.values[0] = Int32GetDatum(v);
        slot.isnull[0] = isnull;
        exec_store_virtual_tuple(&mut slot);
        tuplesort_puttupleslot(state, &mut slot);
    }

    fn drain(state: &mut Tuplesortstate) -> Vec<Option<i32>> {
        let desc = int4_desc(1);
        let mut slot = make_single_tuple_table_slot(Some(desc), &TTSOpsVirtual);
        let mut out = Vec::new();
        while tuplesort_gettupleslot(state, true, false, &mut slot) {
            if slot.isnull[0] {
                out.push(None);
            } else {
                out.push(Some(DatumGetInt32(slot.values[0])));
            }
        }
        out
    }

    #[test]
    fn sort_int4_ascending() {
        let mut st = begin_int4_heap(false, false, sortopt::NONE);
        for v in [5, 1, 4, 2, 3] {
            put_int(&mut st, v, false);
        }
        tuplesort_performsort(&mut st);
        assert_eq!(drain(&mut st), vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
    }

    #[test]
    fn sort_int4_descending() {
        let mut st = begin_int4_heap(true, false, sortopt::NONE);
        for v in [5, 1, 4, 2, 3] {
            put_int(&mut st, v, false);
        }
        tuplesort_performsort(&mut st);
        assert_eq!(drain(&mut st), vec![Some(5), Some(4), Some(3), Some(2), Some(1)]);
    }

    #[test]
    fn nulls_last_default_ascending() {
        let mut st = begin_int4_heap(false, false, sortopt::NONE);
        put_int(&mut st, 2, false);
        put_int(&mut st, 0, true); // NULL
        put_int(&mut st, 1, false);
        tuplesort_performsort(&mut st);
        assert_eq!(drain(&mut st), vec![Some(1), Some(2), None]);
    }

    #[test]
    fn nulls_first() {
        let mut st = begin_int4_heap(false, true, sortopt::NONE);
        put_int(&mut st, 2, false);
        put_int(&mut st, 0, true); // NULL
        put_int(&mut st, 1, false);
        tuplesort_performsort(&mut st);
        assert_eq!(drain(&mut st), vec![None, Some(1), Some(2)]);
    }

    #[test]
    fn bounded_top_n_returns_n_smallest() {
        let mut st = begin_int4_heap(false, false, sortopt::ALLOWBOUNDED);
        tuplesort_set_bound(&mut st, 3);
        for v in [9, 3, 7, 1, 8, 2, 5, 4, 6] {
            put_int(&mut st, v, false);
        }
        tuplesort_performsort(&mut st);
        assert!(tuplesort_used_bound(&st));
        // A bounded sort is consumed by reading exactly `bound` tuples (the LIMIT
        // node never over-fetches); fetching past the bound is an error in PG.
        let desc = int4_desc(1);
        let mut slot = make_single_tuple_table_slot(Some(desc), &TTSOpsVirtual);
        let mut out = Vec::new();
        for _ in 0..3 {
            assert!(tuplesort_gettupleslot(&mut st, true, false, &mut slot));
            out.push(DatumGetInt32(slot.values[0]));
        }
        assert_eq!(out, vec![1, 2, 3]);
    }

    #[test]
    fn two_key_sort_with_tiebreak() {
        // Sort by col1 asc, then col2 asc; col1 has ties.
        let desc = int4_desc(2);
        let mut st = Box::new(Tuplesortstate {
            status: TupSortStatus::Initial,
            variant: Variant::Heap,
            sortopt: sortopt::NONE,
            bounded: false,
            bound: 0,
            bound_used: false,
            n_keys: 2,
            sort_keys: vec![SortKey::empty(), SortKey::empty()],
            memtuples: Vec::new(),
            avail_mem: 4096 * 1024,
            allowed_mem: 4096 * 1024,
            tuple_mem: 0,
            current: 0,
            eof_reached: false,
            markpos: 0,
            markpos_eof: false,
            tupdesc: Some(desc.clone()),
            datum_typbyval: true,
        });
        set_int4_key(&mut st, 0, 1, false, false);
        set_int4_key(&mut st, 1, 2, false, false);

        let rows = [(1, 30), (1, 10), (2, 5), (1, 20)];
        for (a, b) in rows {
            let mut slot = make_single_tuple_table_slot(Some(desc.clone()), &TTSOpsVirtual);
            slot.values[0] = Int32GetDatum(a);
            slot.values[1] = Int32GetDatum(b);
            slot.isnull[0] = false;
            slot.isnull[1] = false;
            exec_store_virtual_tuple(&mut slot);
            tuplesort_puttupleslot(&mut st, &mut slot);
        }
        tuplesort_performsort(&mut st);

        let mut slot = make_single_tuple_table_slot(Some(desc), &TTSOpsVirtual);
        let mut got = Vec::new();
        while tuplesort_gettupleslot(&mut st, true, false, &mut slot) {
            got.push((DatumGetInt32(slot.values[0]), DatumGetInt32(slot.values[1])));
        }
        assert_eq!(got, vec![(1, 10), (1, 20), (1, 30), (2, 5)]);
    }

    #[test]
    fn datum_sort_ascending() {
        let mut st = Box::new(Tuplesortstate {
            status: TupSortStatus::Initial,
            variant: Variant::Datum,
            sortopt: sortopt::NONE,
            bounded: false,
            bound: 0,
            bound_used: false,
            n_keys: 1,
            sort_keys: vec![SortKey::empty()],
            memtuples: Vec::new(),
            avail_mem: 4096 * 1024,
            allowed_mem: 4096 * 1024,
            tuple_mem: 0,
            current: 0,
            eof_reached: false,
            markpos: 0,
            markpos_eof: false,
            tupdesc: None,
            datum_typbyval: true,
        });
        st.sort_keys[0].comparator = Some(ssup_datum_int32_cmp);

        for v in [30, 10, 20] {
            tuplesort_putdatum(&mut st, Int32GetDatum(v), false);
        }
        tuplesort_performsort(&mut st);
        let mut out = Vec::new();
        while let Some((d, isnull)) = tuplesort_getdatum(&mut st, true) {
            assert!(!isnull);
            out.push(DatumGetInt32(d));
        }
        assert_eq!(out, vec![10, 20, 30]);
    }

    #[test]
    fn many_tuples_sorted_in_memory() {
        let mut st = begin_int4_heap(false, false, sortopt::NONE);
        for v in (0..1000).rev() {
            put_int(&mut st, v, false);
        }
        tuplesort_performsort(&mut st);
        let out = drain(&mut st);
        assert_eq!(out.len(), 1000);
        assert_eq!(out[0], Some(0));
        assert_eq!(out[999], Some(999));
        assert!(out.windows(2).all(|w| w[0] <= w[1]));
    }

    #[test]
    fn rescan_markpos_restorepos() {
        let mut st = begin_int4_heap(false, false, sortopt::RANDOMACCESS);
        for v in [3, 1, 2] {
            put_int(&mut st, v, false);
        }
        tuplesort_performsort(&mut st);
        let desc = int4_desc(1);
        let mut slot = make_single_tuple_table_slot(Some(desc), &TTSOpsVirtual);

        assert!(tuplesort_gettupleslot(&mut st, true, false, &mut slot));
        assert_eq!(DatumGetInt32(slot.values[0]), 1);
        tuplesort_markpos(&mut st);
        assert!(tuplesort_gettupleslot(&mut st, true, false, &mut slot));
        assert_eq!(DatumGetInt32(slot.values[0]), 2);
        tuplesort_restorepos(&mut st);
        assert!(tuplesort_gettupleslot(&mut st, true, false, &mut slot));
        assert_eq!(DatumGetInt32(slot.values[0]), 2);

        tuplesort_rescan(&mut st);
        assert!(tuplesort_gettupleslot(&mut st, true, false, &mut slot));
        assert_eq!(DatumGetInt32(slot.values[0]), 1);
    }

    /// The sort state is genuinely `Send` (the executor runs on the multi-thread
    /// runtime; rules.md s10).
    #[test]
    fn state_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<Tuplesortstate>();
        assert_send::<Box<Tuplesortstate>>();
    }
}
