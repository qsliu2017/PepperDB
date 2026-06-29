//! TID bitmap package. Translated from backend/nodes/tidbitmap.c (disposition:
//! full for the owned, single-process bitmap the bitmap-scan executor drives;
//! the DSA shared-bitmap + parallel iteration path is staged).
//!
//! A `TIDBitmap` is a set of heap TIDs, page-organized: each [`PagetableEntry`]
//! holds either an EXACT per-page bitmap of tuple offsets or, once the bitmap
//! grows past its memory budget, a LOSSY chunk header standing for a run of whole
//! pages. PG stores both in one hash table keyed by block number; we do the same
//! with an owned `HashMap<BlockNumber, PagetableEntry>` (no raw alloc, no shmem).
//! Page entries are emitted in ascending block order by iteration (PG sorts the
//! page/chunk arrays in `tbm_begin_iterate`).
//!
//! Owned vs PG (rules.md s8): the C module avoids a hashtable for a single page
//! (the `TBM_ONE_PAGE` fast path) and lazily creates it. We always use the
//! `HashMap` -- the allocation it saves is a micro-optimization that doesn't carry
//! to an owned `HashMap` whose empty form is cheap; the lossy/exact semantics and
//! the memory-budget lossify behavior are reproduced faithfully.
//!
//! GROW/STAGE (rules.md s4): the DSA shared bitmap (`tbm_prepare_shared_iterate`,
//! `tbm_attach_shared_iterate`, the shared iterator + its LWLock) is the parallel
//! bitmap-heap-scan path; under the single-process spine the shared variant
//! collapses and is a clean `not_yet_reachable`. The recheck/lossy machinery the
//! serial scan needs is COMPLETE.

use std::collections::HashMap;

use crate::storage::block::{BlockNumber, INVALID_BLOCK_NUMBER};
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::OffsetNumber;

/// Per-page tuple-offset capacity of an exact entry (PG `TBM_MAX_TUPLES_PER_PAGE`,
/// = the heap's `MaxHeapTuplesPerPage`). An exact page's bitmap has one bit per
/// possible heap tuple offset (1-based, bit k = offset k+1).
pub const TBM_MAX_TUPLES_PER_PAGE: i32 = crate::access::htup_details::MaxHeapTuplesPerPage;

/// Bits per bitmap word (`BITS_PER_BITMAPWORD`); we use 64-bit words.
const BITS_PER_BITMAPWORD: usize = 64;
type Bitmapword = u64;

/// Pages aggregated into one lossy chunk (`PAGES_PER_CHUNK = BLCKSZ / 32`). A chunk
/// header's bitmap has one bit per page in `[blockno, blockno + PAGES_PER_CHUNK)`.
const PAGES_PER_CHUNK: u32 = crate::pg_config::BLCKSZ / 32;

/// Active words for an exact page's offset bitmap.
const WORDS_PER_PAGE: usize =
    ((TBM_MAX_TUPLES_PER_PAGE as usize - 1) / BITS_PER_BITMAPWORD) + 1;
/// Active words for a lossy chunk's page bitmap.
const WORDS_PER_CHUNK: usize = ((PAGES_PER_CHUNK as usize - 1) / BITS_PER_BITMAPWORD) + 1;
/// Word count of an entry (max of the two, so one struct serves both roles).
const WORDS_PER_ENTRY: usize = if WORDS_PER_PAGE > WORDS_PER_CHUNK {
    WORDS_PER_PAGE
} else {
    WORDS_PER_CHUNK
};

#[inline]
const fn wordnum(x: u32) -> usize {
    (x as usize) / BITS_PER_BITMAPWORD
}
#[inline]
const fn bitnum(x: u32) -> usize {
    (x as usize) % BITS_PER_BITMAPWORD
}

/// A hash-table entry: the bitmap for one heap page (exact) or one lossy chunk.
///
/// For an EXACT page, `blockno` is the page number and bit k of `words` represents
/// tuple offset k+1. For a LOSSY chunk, `blockno` is the first page in the chunk
/// (a multiple of `PAGES_PER_CHUNK`) and bit k represents page `blockno + k`.
/// `recheck` (exact pages only) flags candidate matches: the indicated tuples must
/// have the full qual re-evaluated.
#[derive(Clone)]
pub struct PagetableEntry {
    pub blockno: BlockNumber,
    pub ischunk: bool,
    pub recheck: bool,
    words: [Bitmapword; WORDS_PER_ENTRY],
}

impl PagetableEntry {
    fn new(blockno: BlockNumber) -> Self {
        Self {
            blockno,
            ischunk: false,
            recheck: false,
            words: [0; WORDS_PER_ENTRY],
        }
    }

    /// Extract the set tuple offsets of an exact page (`tbm_extract_page_tuple`).
    /// Offsets are 1-based and returned in ascending order.
    #[must_use]
    pub fn offsets(&self) -> Vec<OffsetNumber> {
        let mut out = Vec::new();
        for (wn, &w) in self.words.iter().enumerate().take(WORDS_PER_PAGE) {
            if w == 0 {
                continue;
            }
            let mut bits = w;
            let mut off = (wn * BITS_PER_BITMAPWORD) + 1;
            while bits != 0 {
                if bits & 1 != 0 {
                    out.push(off as OffsetNumber);
                }
                off += 1;
                bits >>= 1;
            }
        }
        out
    }
}

/// A whole TID bitmap: an owned set of heap TIDs (PG `TIDBitmap`). The C struct's
/// `TBM_ONE_PAGE`/`TBM_HASH` status + DSA fields collapse to one owned `HashMap`.
pub struct TIDBitmap {
    pagetable: HashMap<BlockNumber, PagetableEntry>,
    nentries: i32,
    maxentries: i32,
    npages: i32,
    nchunks: i32,
    iterating: bool,
}

impl TIDBitmap {
    fn page_is_lossy(&self, pageno: BlockNumber) -> bool {
        if self.nchunks == 0 {
            return false;
        }
        let bitno = pageno % PAGES_PER_CHUNK;
        let chunk_pageno = pageno - bitno;
        self.pagetable.get(&chunk_pageno).is_some_and(|page| {
            page.ischunk && (page.words[wordnum(bitno)] & (1 << bitnum(bitno))) != 0
        })
    }

    /// Find an exact (non-lossy) entry for `pageno` (`tbm_find_pageentry`).
    fn find_pageentry(&self, pageno: BlockNumber) -> Option<&PagetableEntry> {
        if self.nentries == 0 {
            return None;
        }
        let page = self.pagetable.get(&pageno)?;
        if page.ischunk {
            return None;
        }
        Some(page)
    }

    /// Find or create an exact entry for `pageno` (`tbm_get_pageentry`). May push
    /// `nentries` past `maxentries`; the caller lossifies at the next safe point.
    fn get_pageentry(&mut self, pageno: BlockNumber) -> &mut PagetableEntry {
        let found = self.pagetable.contains_key(&pageno);
        if !found {
            self.pagetable.insert(pageno, PagetableEntry::new(pageno));
            self.nentries += 1;
            self.npages += 1;
        }
        self.pagetable
            .get_mut(&pageno)
            .unwrap_or_else(|| unreachable!("entry just inserted"))
    }

    /// Mark `pageno` lossy (`tbm_mark_page_lossy`): set its bit in the chunk header,
    /// removing any extant exact entry for the page.
    fn mark_page_lossy(&mut self, pageno: BlockNumber) {
        let bitno = pageno % PAGES_PER_CHUNK;
        let chunk_pageno = pageno - bitno;

        // Remove any extant non-lossy entry for the page (unless it IS the header).
        if bitno != 0 && self.pagetable.remove(&pageno).is_some() {
            self.nentries -= 1;
            self.npages -= 1;
        }

        match self.pagetable.get(&chunk_pageno) {
            None => {
                let mut page = PagetableEntry::new(chunk_pageno);
                page.ischunk = true;
                self.pagetable.insert(chunk_pageno, page);
                self.nentries += 1;
                self.nchunks += 1;
            }
            Some(p) if !p.ischunk => {
                // Header page was exact; convert to lossy (it had some tuple bit set).
                let mut page = PagetableEntry::new(chunk_pageno);
                page.ischunk = true;
                page.words[0] = 1;
                self.pagetable.insert(chunk_pageno, page);
                self.nchunks += 1;
                self.npages -= 1;
            }
            Some(_) => {}
        }

        let page = self
            .pagetable
            .get_mut(&chunk_pageno)
            .unwrap_or_else(|| unreachable!("chunk header just ensured"));
        page.words[wordnum(bitno)] |= 1 << bitnum(bitno);
    }

    /// Lose information to get back under the memory limit (`tbm_lossify`). Converts
    /// exact pages to lossy chunks until `nentries <= maxentries/2`.
    fn lossify(&mut self) {
        // Collect candidate exact-page block numbers (not chunk headers themselves).
        let candidates: Vec<BlockNumber> = self
            .pagetable
            .values()
            .filter(|p| !p.ischunk && (p.blockno % PAGES_PER_CHUNK) != 0)
            .map(|p| p.blockno)
            .collect();

        for blk in candidates {
            // The page may already have been folded into a chunk by a prior step.
            if self.pagetable.get(&blk).is_none_or(|p| p.ischunk) {
                continue;
            }
            self.mark_page_lossy(blk);
            if self.nentries <= self.maxentries / 2 {
                break;
            }
        }

        if self.nentries > self.maxentries / 2 {
            self.maxentries = self.nentries.min((i32::MAX - 1) / 2) * 2;
        }
    }
}

/// `tbm_create`: an initially empty bitmap limited to ~`maxbytes` of memory. The
/// DSA argument (parallel) is dropped under the single-process spine.
#[must_use]
pub fn tbm_create(maxbytes: usize) -> Box<TIDBitmap> {
    Box::new(TIDBitmap {
        pagetable: HashMap::new(),
        nentries: 0,
        maxentries: tbm_calculate_entries(maxbytes),
        npages: 0,
        nchunks: 0,
        iterating: false,
    })
}

/// `tbm_free`: owned drop (no-op; the box drops at the call site).
#[allow(
    clippy::boxed_local,
    reason = "mirrors the C tbm_free(TIDBitmap *): takes ownership so the bitmap frees here"
)]
pub fn tbm_free(_tbm: Box<TIDBitmap>) {}

/// `tbm_add_tuples`: add heap TIDs to the bitmap. `recheck` flags candidate matches
/// (the heap scan must re-evaluate the qual for the reported tuples).
pub fn tbm_add_tuples(tbm: &mut TIDBitmap, tids: &[ItemPointerData], recheck: bool) {
    crate::assert!(!tbm.iterating, "tbm_add_tuples after iteration started");
    let mut currblk = INVALID_BLOCK_NUMBER;
    let mut page_is_lossy = false;

    for tid in tids {
        let blk = tid.block_number();
        let off = tid.offset_number();
        if off < 1 || i32::from(off) > TBM_MAX_TUPLES_PER_PAGE {
            crate::elog!(crate::utils::elog::ERROR, format!("tuple offset out of range: {off}"));
        }

        if blk != currblk {
            page_is_lossy = tbm.page_is_lossy(blk);
            currblk = blk;
        }
        if page_is_lossy {
            continue; // whole page already marked lossy
        }

        {
            let page = tbm.get_pageentry(blk);
            // Exact page: set the bit for the individual tuple offset.
            let pos = u32::from(off) - 1;
            page.words[wordnum(pos)] |= 1 << bitnum(pos);
            page.recheck |= recheck;
        }

        if tbm.nentries > tbm.maxentries {
            tbm.lossify();
            currblk = INVALID_BLOCK_NUMBER; // page may have become lossy
        }
    }
}

/// `tbm_add_page`: mark a whole page for reporting (always lossy/recheck).
pub fn tbm_add_page(tbm: &mut TIDBitmap, pageno: BlockNumber) {
    tbm.mark_page_lossy(pageno);
    if tbm.nentries > tbm.maxentries {
        tbm.lossify();
    }
}

/// `tbm_union`: set union, `a |= b` in place (`b` unchanged).
pub fn tbm_union(a: &mut TIDBitmap, b: &TIDBitmap) {
    crate::assert!(!a.iterating, "tbm_union after iteration started");
    if b.nentries == 0 {
        return;
    }
    let bpages: Vec<PagetableEntry> = b.pagetable.values().cloned().collect();
    for bpage in &bpages {
        tbm_union_page(a, bpage);
    }
}

/// Merge one page of `b` into `a` during a union.
fn tbm_union_page(a: &mut TIDBitmap, bpage: &PagetableEntry) {
    if bpage.ischunk {
        // Mark each page indicated by b's chunk lossy in a.
        for wn in 0..WORDS_PER_CHUNK {
            let mut w = bpage.words[wn];
            if w == 0 {
                continue;
            }
            let mut pg = bpage.blockno + (wn * BITS_PER_BITMAPWORD) as u32;
            while w != 0 {
                if w & 1 != 0 {
                    a.mark_page_lossy(pg);
                }
                pg += 1;
                w >>= 1;
            }
        }
    } else if a.page_is_lossy(bpage.blockno) {
        // already lossy in a; nothing to do
    } else {
        let apage = a.get_pageentry(bpage.blockno);
        if apage.ischunk {
            apage.words[0] |= 1;
        } else {
            for wn in 0..WORDS_PER_PAGE {
                apage.words[wn] |= bpage.words[wn];
            }
            apage.recheck |= bpage.recheck;
        }
    }
    if a.nentries > a.maxentries {
        a.lossify();
    }
}

/// `tbm_intersect`: set intersection, `a &= b` in place (`b` unchanged).
pub fn tbm_intersect(a: &mut TIDBitmap, b: &TIDBitmap) {
    crate::assert!(!a.iterating, "tbm_intersect after iteration started");
    if a.nentries == 0 {
        return;
    }
    let ablocks: Vec<BlockNumber> = a.pagetable.keys().copied().collect();
    for blk in ablocks {
        let Some(mut apage) = a.pagetable.remove(&blk) else { continue };
        let empty = tbm_intersect_page(&mut apage, b);
        if empty {
            if apage.ischunk {
                a.nchunks -= 1;
            } else {
                a.npages -= 1;
            }
            a.nentries -= 1;
            // entry already removed above
        } else {
            a.pagetable.insert(blk, apage);
        }
    }
}

/// Intersect one page of `a` against `b`; returns true if `apage` is now empty.
fn tbm_intersect_page(apage: &mut PagetableEntry, b: &TIDBitmap) -> bool {
    if apage.ischunk {
        let mut candelete = true;
        for wn in 0..WORDS_PER_CHUNK {
            let w = apage.words[wn];
            if w == 0 {
                continue;
            }
            let mut neww = w;
            let mut pg = apage.blockno + (wn * BITS_PER_BITMAPWORD) as u32;
            let mut bit = 0u32;
            let mut bits = w;
            while bits != 0 {
                if bits & 1 != 0
                    && !b.page_is_lossy(pg)
                    && b.find_pageentry(pg).is_none()
                {
                    neww &= !(1 << bit);
                }
                pg += 1;
                bit += 1;
                bits >>= 1;
            }
            apage.words[wn] = neww;
            if neww != 0 {
                candelete = false;
            }
        }
        candelete
    } else if b.page_is_lossy(apage.blockno) {
        // b is lossy here: keep a's bits but require recheck.
        apage.recheck = true;
        false
    } else {
        let mut candelete = true;
        if let Some(bpage) = b.find_pageentry(apage.blockno) {
            for wn in 0..WORDS_PER_PAGE {
                apage.words[wn] &= bpage.words[wn];
                if apage.words[wn] != 0 {
                    candelete = false;
                }
            }
            apage.recheck |= bpage.recheck;
        }
        candelete
    }
}

/// `tbm_is_empty`: true if the bitmap holds no TIDs.
#[must_use]
pub fn tbm_is_empty(tbm: &TIDBitmap) -> bool {
    tbm.nentries == 0
}

/// `tbm_calculate_entries`: estimate the entry budget for `maxbytes`.
#[must_use]
pub fn tbm_calculate_entries(maxbytes: usize) -> i32 {
    let per = std::mem::size_of::<PagetableEntry>() + 2 * std::mem::size_of::<usize>();
    let n = (maxbytes / per).min((i32::MAX - 1) as usize).max(16);
    n as i32
}

/// One page of an iteration (PG `TBMIterateResult`). For an exact page,
/// `offsets` holds the set tuple offsets (1-based, ascending) and `lossy` is false.
/// For a lossy page, `offsets` is empty, `lossy`/`recheck` are true, and the caller
/// must examine all tuples on the page.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct TBMIterateResult {
    pub blockno: BlockNumber,
    pub lossy: bool,
    pub recheck: bool,
    pub offsets: Vec<OffsetNumber>,
}

/// A private (single-process) iterator over a `TIDBitmap` (PG `TBMPrivateIterator`).
/// Holds the sorted ascending list of (page-or-chunk) entries to emit. The bitmap
/// is logically read-only while an iterator exists.
pub struct TBMPrivateIterator {
    /// Sorted result rows, materialized at `tbm_begin_iterate`, emitted front-to-back.
    queue: std::collections::VecDeque<TBMIterateResult>,
}

/// Unified iterator (PG `TBMIterator`): a private iterator under the single-process
/// spine. The shared (DSA) variant is staged; `Shared` is held for the relscan
/// union's shape but is never constructed on this path.
pub enum TBMIterator {
    Private(Option<Box<TBMPrivateIterator>>),
    /// Staged parallel/shared iterator (DSA). Not reachable on the serial spine.
    Shared(Option<Box<TBMSharedIterator>>),
}

/// Staged shared iterator (DSA-backed parallel iteration). Not yet reachable.
pub struct TBMSharedIterator {
    _private: (),
}

/// `tbm_begin_private_iterate`: materialize the sorted page/chunk emission queue.
/// Pages are delivered in ascending block order; lossy chunks expand to one result
/// per set page bit, merged into the same order as the exact pages.
#[must_use]
pub fn tbm_begin_private_iterate(tbm: &mut TIDBitmap) -> Box<TBMPrivateIterator> {
    tbm.iterating = true;

    let mut rows: Vec<TBMIterateResult> = Vec::with_capacity(tbm.nentries.max(0) as usize);
    for entry in tbm.pagetable.values() {
        if entry.ischunk {
            for wn in 0..WORDS_PER_CHUNK {
                let mut w = entry.words[wn];
                if w == 0 {
                    continue;
                }
                let mut pg = entry.blockno + (wn * BITS_PER_BITMAPWORD) as u32;
                while w != 0 {
                    if w & 1 != 0 {
                        rows.push(TBMIterateResult {
                            blockno: pg,
                            lossy: true,
                            recheck: true,
                            offsets: Vec::new(),
                        });
                    }
                    pg += 1;
                    w >>= 1;
                }
            }
        } else {
            rows.push(TBMIterateResult {
                blockno: entry.blockno,
                lossy: false,
                recheck: entry.recheck,
                offsets: entry.offsets(),
            });
        }
    }
    rows.sort_by_key(|r| r.blockno);

    Box::new(TBMPrivateIterator { queue: rows.into() })
}

/// `tbm_private_iterate`: the next page of the bitmap, or `None` when exhausted.
pub fn tbm_private_iterate(iterator: &mut TBMPrivateIterator) -> Option<TBMIterateResult> {
    iterator.queue.pop_front()
}

/// `tbm_end_private_iterate`: owned drop (no-op).
#[allow(
    clippy::boxed_local,
    reason = "mirrors the C tbm_end_private_iterate: takes ownership so the iterator frees here"
)]
pub fn tbm_end_private_iterate(_iterator: Box<TBMPrivateIterator>) {}

/// `tbm_begin_iterate`: start a (private) iteration over the bitmap.
#[must_use]
pub fn tbm_begin_iterate(tbm: &mut TIDBitmap) -> TBMIterator {
    TBMIterator::Private(Some(tbm_begin_private_iterate(tbm)))
}

/// `tbm_iterate`: the next page via a unified iterator, or `None` when exhausted.
pub fn tbm_iterate(iterator: &mut TBMIterator) -> Option<TBMIterateResult> {
    match iterator {
        TBMIterator::Private(Some(it)) => tbm_private_iterate(it),
        TBMIterator::Private(None) => None,
        TBMIterator::Shared(_) => {
            unimplemented!("tbm_iterate: shared (DSA/parallel) iteration is staged")
        }
    }
}

/// `tbm_exhausted`: whether the iterator has been ended/cleared.
#[must_use]
pub fn tbm_exhausted(iterator: &TBMIterator) -> bool {
    match iterator {
        TBMIterator::Private(it) => it.is_none(),
        TBMIterator::Shared(it) => it.is_none(),
    }
}

/// `tbm_end_iterate`: clean up an iterator.
pub fn tbm_end_iterate(iterator: &mut TBMIterator) {
    match iterator {
        TBMIterator::Private(it) => {
            if let Some(it) = it.take() {
                tbm_end_private_iterate(it);
            }
        }
        TBMIterator::Shared(it) => {
            *it = None;
        }
    }
}

#[cfg(test)]
#[path = "tidbitmap_tests.rs"]
mod tests;
