//! Logical tape abstraction over a temporary BufFile, for external sort/merge.
//! Translated from backend/utils/sort/logtape.c (disposition: full; the BufFile
//! block I/O leaves stub-call the still-hollow `storage::buffile`, rules.md s4).
//!
//! A LogicalTapeSet stores many independent "logical tapes" in one underlying
//! temp file. Each tape is a singly/doubly-linked chain of BLCKSZ blocks; the
//! last `sizeof(TapeBlockTrailer)` bytes of every block hold the prev/next block
//! links (and, on the last block, the negated valid-byte count). Free blocks are
//! recycled through a min-heap so the file stays compact.
//!
//! Memory model (rules.md s10): the C `int64 *freeBlocks` min-heap and the
//! per-tape `char *buffer`/`int64 *prealloc` become owned `Vec`s; the
//! `LogicalTapeSet *tapeSet` back-pointer on each tape is dropped (the tape set
//! owns its tapes and is threaded as `&mut` to the tape methods). The block
//! trailer is read/written through byte offsets with `i64::{from,to}_le_bytes`
//! rather than forming a reference over a possibly-misaligned address, so the
//! owned `Vec<u8>` buffer is alignment-sound.

use crate::pg_config::BLCKSZ;
use crate::storage::buffile::{
    BufFile, BufFileCreateFileSet, BufFileCreateTemp, BufFileReadExact, BufFileSeekBlock,
    BufFileWrite,
};
use crate::storage::sharedfileset::SharedFileSet;
use crate::utils::elog::ERROR;

/// `sizeof(TapeBlockTrailer)` -- two `int64`s (prev, next).
const TAPE_BLOCK_TRAILER_SIZE: usize = 2 * core::mem::size_of::<i64>();

/// `TapeBlockPayloadSize` -- usable bytes per block, excluding the trailer.
const TAPE_BLOCK_PAYLOAD_SIZE: usize = BLCKSZ as usize - TAPE_BLOCK_TRAILER_SIZE;

/// `MaxAllocSize` (utils/memutils.h): 1 GB - 1, the largest single palloc.
const MAX_ALLOC_SIZE: usize = 0x3fff_ffff;

const TAPE_WRITE_PREALLOC_MIN: i32 = 8;
const TAPE_WRITE_PREALLOC_MAX: i32 = 128;

/// `TapeShare` metadata exported when freezing a worker's materialized tape.
#[derive(Debug, Clone, Copy)]
pub struct TapeShare {
    /// Location of the materialized tape's first block.
    pub firstblocknumber: i64,
}

// --- TapeBlockTrailer accessors (byte-offset, alignment-sound) ---------------

/// Offset of the trailer within a block buffer.
const fn trailer_off() -> usize {
    TAPE_BLOCK_PAYLOAD_SIZE
}

fn trailer_prev(buf: &[u8]) -> i64 {
    let o = trailer_off();
    i64::from_le_bytes(buf[o..o + 8].try_into().unwrap_or([0; 8]))
}
fn trailer_next(buf: &[u8]) -> i64 {
    let o = trailer_off() + 8;
    i64::from_le_bytes(buf[o..o + 8].try_into().unwrap_or([0; 8]))
}
fn set_trailer_prev(buf: &mut [u8], v: i64) {
    let o = trailer_off();
    buf[o..o + 8].copy_from_slice(&v.to_le_bytes());
}
fn set_trailer_next(buf: &mut [u8], v: i64) {
    let o = trailer_off() + 8;
    buf[o..o + 8].copy_from_slice(&v.to_le_bytes());
}
/// `TapeBlockIsLast`: the last block stores a negative `next`.
fn block_is_last(buf: &[u8]) -> bool {
    trailer_next(buf) < 0
}
/// `TapeBlockGetNBytes`: valid payload bytes in `buf`.
fn block_nbytes(buf: &[u8]) -> usize {
    if block_is_last(buf) {
        usize::try_from(-trailer_next(buf)).unwrap_or(0)
    } else {
        TAPE_BLOCK_PAYLOAD_SIZE
    }
}
/// `TapeBlockSetNBytes`: stamp the valid-byte count on the last block.
fn set_block_nbytes(buf: &mut [u8], nbytes: usize) {
    set_trailer_next(buf, -(nbytes as i64));
}

// --- min-heap helpers (C left_offset/right_offset/parent_offset) -------------

const fn left_offset(i: u64) -> u64 {
    2 * i + 1
}
const fn right_offset(i: u64) -> u64 {
    2 * i + 2
}
const fn parent_offset(i: u64) -> u64 {
    (i - 1) / 2
}

/// A single logical tape within a [`LogicalTapeSet`].
pub struct LogicalTape {
    pub writing: bool,             // T while in write phase
    pub frozen: bool,              // T if blocks should not be freed when read
    pub dirty: bool,               // does buffer need to be written?
    pub first_block_number: i64,
    pub cur_block_number: i64,
    pub next_block_number: i64,
    pub offset_block_number: i64,
    pub buffer: Vec<u8>,           // physical buffer (empty == not yet allocated)
    pub buffer_size: usize,
    pub max_size: usize,           // highest useful, safe buffer_size
    pub pos: usize,                // next read/write position in buffer
    pub nbytes: usize,             // total # of valid bytes in buffer
    pub prealloc: Vec<i64>,        // preallocated block numbers, descending
    pub prealloc_size: i32,
}

/// A set of related logical tapes sharing one underlying temp file.
pub struct LogicalTapeSet {
    pub pfile: Option<Box<BufFile>>, // underlying file, None until imported (leader)
    /// Whether this set is backed by a shared fileset (parallel sort). The
    /// `SharedFileSet` itself is owned by the caller (it holds a `Mutex` and is
    /// not `Clone`); we only need to know it exists for the leader-tape rule.
    pub has_fileset: bool,
    pub worker: i32,                 // worker #, or -1 for leader/serial

    pub n_blocks_allocated: i64,
    pub n_blocks_written: i64,
    pub n_hole_blocks: i64,

    pub forget_free_space: bool,
    pub free_blocks: Vec<i64>,       // min-heap of recycled block numbers
    pub enable_prealloc: bool,
}

impl LogicalTapeSet {
    /// C `ltsWriteBlock`: write a block-sized buffer to `blocknum`. BufFile has no
    /// holes, so any gap before `blocknum` is back-filled with zero blocks.
    fn write_block(&mut self, blocknum: i64, buffer: &[u8]) {
        while blocknum > self.n_blocks_written {
            let zero = [0u8; BLCKSZ as usize];
            let nbw = self.n_blocks_written;
            self.write_block(nbw, &zero);
        }
        let pfile = self
            .pfile
            .as_mut()
            .unwrap_or_else(|| unreachable!("tape set has a backing BufFile when writing"));
        // BufFile seek+write are the disk leaves; hollow until buffile lands.
        BufFileSeekBlock(pfile, blocknum);
        BufFileWrite(pfile, buffer);

        if blocknum == self.n_blocks_written {
            self.n_blocks_written += 1;
        }
    }

    /// C `ltsReadBlock`: read block `blocknum` into `buffer` (BLCKSZ bytes).
    fn read_block(&mut self, blocknum: i64, buffer: &mut [u8]) {
        let pfile = self
            .pfile
            .as_mut()
            .unwrap_or_else(|| unreachable!("tape set has a backing BufFile when reading"));
        if BufFileSeekBlock(pfile, blocknum) != 0 {
            crate::elog!(ERROR, "could not seek to block {blocknum} of temporary file");
        }
        BufFileReadExact(pfile, buffer);
    }

    /// C `ltsGetFreeBlock`: pop the lowest recycled block, or extend the file.
    fn get_free_block(&mut self) -> i64 {
        let n = self.free_blocks.len();
        if n == 0 {
            let b = self.n_blocks_allocated;
            self.n_blocks_allocated += 1;
            return b;
        }
        if n == 1 {
            return self.free_blocks.pop().unwrap_or(0);
        }
        let blocknum = self.free_blocks[0];
        let holeval = self.free_blocks.pop().unwrap_or(0);
        let heap = &mut self.free_blocks;
        let heapsize = heap.len() as u64;
        let mut holepos: u64 = 0;
        loop {
            let left = left_offset(holepos);
            let right = right_offset(holepos);
            let min_child = if left < heapsize && right < heapsize {
                if heap[left as usize] < heap[right as usize] {
                    left
                } else {
                    right
                }
            } else if left < heapsize {
                left
            } else if right < heapsize {
                right
            } else {
                break;
            };
            if heap[min_child as usize] >= holeval {
                break;
            }
            heap[holepos as usize] = heap[min_child as usize];
            holepos = min_child;
        }
        heap[holepos as usize] = holeval;
        blocknum
    }

    /// C `ltsReleaseBlock`: return `blocknum` to the free-block min-heap.
    fn release_block(&mut self, blocknum: i64) {
        if self.forget_free_space {
            return;
        }
        // Leak the block rather than grow the freelist past MaxAllocSize.
        if (self.free_blocks.len() + 1) * core::mem::size_of::<i64>() > MAX_ALLOC_SIZE {
            return;
        }
        let heap = &mut self.free_blocks;
        heap.push(0); // grow; the real value is sifted into place below
        let mut holepos = (heap.len() - 1) as u64;
        while holepos != 0 {
            let parent = parent_offset(holepos);
            if heap[parent as usize] < blocknum {
                break;
            }
            heap[holepos as usize] = heap[parent as usize];
            holepos = parent;
        }
        heap[holepos as usize] = blocknum;
    }

    /// C `ltsGetPreallocBlock`: hand out a preallocated block for `lt`, refilling
    /// the per-tape prealloc list (descending) from the free heap when empty.
    fn get_prealloc_block(&mut self, lt: &mut LogicalTape) -> i64 {
        if lt.prealloc.is_empty() {
            if lt.prealloc_size == 0 {
                lt.prealloc_size = TAPE_WRITE_PREALLOC_MIN;
            } else if lt.prealloc_size < TAPE_WRITE_PREALLOC_MAX {
                lt.prealloc_size *= 2;
            }
            for _ in 0..lt.prealloc_size {
                lt.prealloc.push(self.get_free_block());
            }
            // sort descending so blocks are consumed lowest-first from the end
            lt.prealloc.sort_unstable_by(|a, b| b.cmp(a));
        }
        lt.prealloc.pop().unwrap_or_else(|| self.get_free_block())
    }

    /// C `ltsGetBlock`: prealloc path when enabled, else the plain free list.
    fn get_block(&mut self, lt: &mut LogicalTape) -> i64 {
        if self.enable_prealloc {
            self.get_prealloc_block(lt)
        } else {
            self.get_free_block()
        }
    }
}

/// C `LogicalTapeSetCreate`: build a tape set, creating its temp file unless this
/// is the leader of a shared sort (which hijacks an imported tape's BufFile).
#[allow(
    clippy::unnecessary_box_returns,
    reason = "PG returns a LogicalTapeSet*; tuplesort holds an owned Box<LogicalTapeSet>"
)]
pub fn logical_tape_set_create(
    preallocate: bool,
    fileset: Option<&mut SharedFileSet>,
    worker: i32,
) -> Box<LogicalTapeSet> {
    let has_fileset = fileset.is_some();
    let pfile = match fileset {
        Some(_) if worker == -1 => None,
        Some(fs) => Some(BufFileCreateFileSet(&mut fs.fs, &worker.to_string())),
        None => Some(BufFileCreateTemp(false)),
    };
    Box::new(LogicalTapeSet {
        pfile,
        has_fileset,
        worker,
        n_blocks_allocated: 0,
        n_blocks_written: 0,
        n_hole_blocks: 0,
        forget_free_space: false,
        free_blocks: Vec::with_capacity(32),
        enable_prealloc: preallocate,
    })
}

/// C `ltsCreateTape`: a fresh write-phase tape (its buffer is allocated lazily).
fn lts_create_tape() -> LogicalTape {
    LogicalTape {
        writing: true,
        frozen: false,
        dirty: false,
        first_block_number: -1,
        cur_block_number: -1,
        next_block_number: -1,
        offset_block_number: 0,
        buffer: Vec::new(),
        buffer_size: 0,
        max_size: MAX_ALLOC_SIZE,
        pos: 0,
        nbytes: 0,
        prealloc: Vec::new(),
        prealloc_size: 0,
    }
}

/// C `LogicalTapeCreate`: a new write tape (forbidden on a shared-set leader).
#[allow(
    clippy::unnecessary_box_returns,
    reason = "PG returns a LogicalTape*; tuplesort holds owned Box<LogicalTape> handles per tape"
)]
pub fn logical_tape_create(lts: &LogicalTapeSet) -> Box<LogicalTape> {
    if lts.has_fileset && lts.worker == -1 {
        crate::elog!(ERROR, "cannot create new tapes in leader process");
    }
    Box::new(lts_create_tape())
}

/// C `LogicalTapeWrite`: append `data` to the write tape, dumping full blocks.
pub fn logical_tape_write(lts: &mut LogicalTapeSet, lt: &mut LogicalTape, data: &[u8]) {
    crate::assert!(lt.writing);
    crate::assert!(lt.offset_block_number == 0);

    if lt.buffer.is_empty() {
        lt.buffer = vec![0u8; BLCKSZ as usize];
        lt.buffer_size = BLCKSZ as usize;
    }
    if lt.cur_block_number == -1 {
        lt.cur_block_number = lts.get_block(lt);
        lt.first_block_number = lt.cur_block_number;
        set_trailer_prev(&mut lt.buffer, -1);
    }

    let mut off = 0usize;
    let mut size = data.len();
    while size > 0 {
        if lt.pos >= TAPE_BLOCK_PAYLOAD_SIZE {
            if !lt.dirty {
                crate::elog!(ERROR, "invalid logtape state: should be dirty");
            }
            let next_block_number = lts.get_block(lt);
            set_trailer_next(&mut lt.buffer, next_block_number);
            lts.write_block(lt.cur_block_number, &lt.buffer);
            set_trailer_prev(&mut lt.buffer, lt.cur_block_number);
            lt.cur_block_number = next_block_number;
            lt.pos = 0;
            lt.nbytes = 0;
        }
        let nthistime = (TAPE_BLOCK_PAYLOAD_SIZE - lt.pos).min(size);
        lt.buffer[lt.pos..lt.pos + nthistime].copy_from_slice(&data[off..off + nthistime]);
        lt.dirty = true;
        lt.pos += nthistime;
        if lt.nbytes < lt.pos {
            lt.nbytes = lt.pos;
        }
        off += nthistime;
        size -= nthistime;
    }
}

/// C `ltsReadFillBuffer`: refill `lt.buffer` from its next on-tape block(s).
/// Returns the number of bytes read into the buffer.
fn lts_read_fill_buffer(lts: &mut LogicalTapeSet, lt: &mut LogicalTape) -> usize {
    lt.pos = 0;
    lt.nbytes = 0;
    loop {
        if lt.next_block_number == -1 {
            break;
        }
        if lt.buffer_size - lt.nbytes < BLCKSZ as usize {
            break;
        }
        // Read one block worth into a scratch, then copy its payload in.
        let mut block = vec![0u8; BLCKSZ as usize];
        lts.read_block(lt.next_block_number, &mut block);
        let nbytes = block_nbytes(&block);
        lt.buffer[lt.nbytes..lt.nbytes + nbytes].copy_from_slice(&block[..nbytes]);
        lt.nbytes += nbytes;

        let this_block = lt.next_block_number;
        if block_is_last(&block) {
            lt.next_block_number = -1;
        } else {
            lt.next_block_number = trailer_next(&block);
        }
        if !lt.frozen {
            lts.release_block(this_block);
        }
        lt.cur_block_number = this_block;
        if nbytes < TAPE_BLOCK_PAYLOAD_SIZE {
            break;
        }
    }
    lt.nbytes
}

/// C `ltsInitReadBuffer`: allocate the read buffer and load the first block.
fn lts_init_read_buffer(lts: &mut LogicalTapeSet, lt: &mut LogicalTape) {
    crate::assert!(lt.buffer_size > 0);
    lt.buffer = vec![0u8; lt.buffer_size];
    lt.next_block_number = lt.first_block_number;
    lt.pos = 0;
    lt.nbytes = 0;
    lts_read_fill_buffer(lts, lt);
}

/// C `LogicalTapeRewindForRead`: flush the final write block, then switch the
/// tape to read mode with a `buffer_size` read buffer.
pub fn logical_tape_rewind_for_read(
    lts: &mut LogicalTapeSet,
    lt: &mut LogicalTape,
    buffer_size: usize,
) {
    // Round the requested buffer to a block multiple, at least one block.
    let blk = BLCKSZ as usize;
    let mut buffer_size = (buffer_size / blk) * blk;
    if buffer_size < blk {
        buffer_size = blk;
    }
    if buffer_size > lt.max_size {
        buffer_size = lt.max_size;
    }

    if lt.writing {
        // Flush the last partial block, then switch to read mode.
        if lt.dirty {
            set_block_nbytes(&mut lt.buffer, lt.nbytes);
            let cur = lt.cur_block_number;
            lts.write_block(cur, &lt.buffer);
        }
        lt.writing = false;
    }
    // Either path then (re)initializes the read buffer from the first block.
    lt.buffer_size = buffer_size;
    lts_init_read_buffer(lts, lt);
}

/// C `LogicalTapeRead`: read up to `size` bytes from the read tape into `dst`,
/// refilling the buffer from tape as needed. Returns the bytes actually read.
pub fn logical_tape_read(
    lts: &mut LogicalTapeSet,
    lt: &mut LogicalTape,
    dst: &mut [u8],
) -> usize {
    crate::assert!(!lt.writing);
    let size = dst.len();
    let mut nread = 0usize;
    while nread < size {
        if lt.pos >= lt.nbytes && lts_read_fill_buffer(lts, lt) == 0 {
            break; // EOF
        }
        let nthistime = (lt.nbytes - lt.pos).min(size - nread);
        if nthistime == 0 {
            break;
        }
        dst[nread..nread + nthistime].copy_from_slice(&lt.buffer[lt.pos..lt.pos + nthistime]);
        lt.pos += nthistime;
        nread += nthistime;
    }
    nread
}

/// C `LogicalTapeFreeze`: finalize a written tape for repeated read access and,
/// for a shared sort, export its first-block location through `share`.
pub fn logical_tape_freeze(
    lts: &mut LogicalTapeSet,
    lt: &mut LogicalTape,
    share: Option<&mut TapeShare>,
) {
    crate::assert!(lt.writing);
    crate::assert!(lt.offset_block_number == 0);

    if lt.dirty {
        set_block_nbytes(&mut lt.buffer, lt.nbytes);
        let cur = lt.cur_block_number;
        lts.write_block(cur, &lt.buffer);
    }
    lt.writing = false;
    lt.frozen = true;

    if lt.buffer.is_empty() {
        lt.buffer = vec![0u8; BLCKSZ as usize];
        lt.buffer_size = BLCKSZ as usize;
    }
    lt.next_block_number = lt.first_block_number;
    lt.pos = 0;
    lt.nbytes = 0;
    if lt.first_block_number != -1 {
        lts_read_fill_buffer(lts, lt);
    }

    if let Some(s) = share {
        s.firstblocknumber = lt.first_block_number;
    }
}

/// C `LogicalTapeSetForgetFreeSpace`: stop recycling freed blocks.
pub fn logical_tape_set_forget_free_space(lts: &mut LogicalTapeSet) {
    lts.forget_free_space = true;
}

/// C `LogicalTapeSetBlocks`: blocks used by the underlying file (minus holes).
pub fn logical_tape_set_blocks(lts: &LogicalTapeSet) -> i64 {
    lts.n_blocks_written - lts.n_hole_blocks
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A tape set with no backing BufFile, for exercising the pure block
    /// bookkeeping (the free-block min-heap) without touching the hollow BufFile
    /// leaves. The real `logical_tape_set_create` eagerly creates the temp file.
    fn bookkeeping_only() -> LogicalTapeSet {
        LogicalTapeSet {
            pfile: None,
            has_fileset: false,
            worker: -1,
            n_blocks_allocated: 0,
            n_blocks_written: 0,
            n_hole_blocks: 0,
            forget_free_space: false,
            free_blocks: Vec::with_capacity(32),
            enable_prealloc: false,
        }
    }

    #[test]
    fn trailer_roundtrip_is_alignment_sound() {
        let mut buf = vec![0u8; BLCKSZ as usize];
        set_trailer_prev(&mut buf, 42);
        set_trailer_next(&mut buf, 7);
        assert_eq!(trailer_prev(&buf), 42);
        assert_eq!(trailer_next(&buf), 7);
        assert!(!block_is_last(&buf));

        set_block_nbytes(&mut buf, 123);
        assert!(block_is_last(&buf));
        assert_eq!(block_nbytes(&buf), 123);
    }

    #[test]
    fn free_block_heap_returns_lowest_first() {
        let mut lts = bookkeeping_only();
        // No backing BufFile is touched by the block bookkeeping.
        for b in [5i64, 2, 9, 1, 7] {
            lts.release_block(b);
        }
        let mut out = Vec::new();
        for _ in 0..5 {
            out.push(lts.get_free_block());
        }
        assert_eq!(out, vec![1, 2, 5, 7, 9]);
    }

    #[test]
    fn empty_freelist_extends_file() {
        let mut lts = bookkeeping_only();
        assert_eq!(lts.get_free_block(), 0);
        assert_eq!(lts.get_free_block(), 1);
        assert_eq!(lts.n_blocks_allocated, 2);
    }
}
