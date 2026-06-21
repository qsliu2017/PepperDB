//! Translation of postgres/src/common/blkreftable.c
//!   + merged public types from postgres/src/include/common/blkreftable.h
//!
//! #include mapping:
//!   "common/blkreftable.h"  -> public types merged here (BlockRefTable,
//!                              BlockRefTableEntry, BlockRefTableReader,
//!                              BlockRefTableWriter, io_callback_fn,
//!                              report_error_fn, BLOCKREFTABLE_MAGIC).
//!   "common/hashfn.h"       -> crate::common::hashfn::hash_bytes
//!   "port/pg_crc32c.h"      -> crate::port::pg_crc32c::{INIT/COMP/FIN/EQ_CRC32C}
//!   "storage/block.h"       -> crate::storage::block::{BlockNumber, InvalidBlockNumber}
//!   "storage/relfilelocator.h" -> RelFileLocator defined here (STUB: not yet a
//!                              ported module) using crate::common::relpath types.
//!   "lib/simplehash.h"      -> crate::lib::simplehash generic.
//!
//! This is the incremental-backup block-reference table: a hash of
//! (relation,fork) -> modified-block sets, plus serialize/deserialize via
//! caller-supplied I/O callbacks and a CRC.  Backend (#ifndef FRONTEND) path.

use crate::prelude::*;

use crate::common::hashfn::hash_bytes;
use crate::common::relpath::{ForkNumber, RelFileNumber};
use crate::lib::simplehash::{
    SimpleHash, SimpleHashIterator, SimpleHashOps, SH_STATUS_EMPTY, SH_STATUS_IN_USE,
};
use crate::port::pg_crc32c::{
    pg_crc32c, COMP_CRC32C, EQ_CRC32C, FIN_CRC32C, INIT_CRC32C,
};
use crate::port::qsort::pg_qsort;
use crate::storage::block::{BlockNumber, InvalidBlockNumber};

extern "C" {
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn memcmp(s1: *const c_void, s2: *const c_void, n: usize) -> c_int;
}

/* c.h: BITS_PER_BYTE. */
const BITS_PER_BYTE: usize = 8;

/* ----------------------------------------------------------------------------
 * Merged public types from blkreftable.h
 * ------------------------------------------------------------------------- */

/* Magic number for serialization file format. */
pub const BLOCKREFTABLE_MAGIC: uint32 = 0x652b137b;

/*
 * storage/relfilelocator.h (STUB: not yet ported as its own module).
 * RelFileNumber comes from common/relpath.
 */
pub use crate::storage::relfilelocator::RelFileLocator;

/*
 * The return value of io_callback_fn should be the number of bytes read or
 * written.  If an error occurs, the functions should report it and not return.
 *
 * report_error_fn should not return.  The C declares it variadic
 * (printf-style); here the file always passes a single already-formatted
 * message string built with Rust formatting, so the callback takes one &str.
 */
pub type io_callback_fn =
    unsafe fn(callback_arg: *mut c_void, data: *mut c_void, length: c_int) -> c_int;
pub type report_error_fn = unsafe fn(callback_arg: *mut c_void, msg: &str) -> !;

/* ----------------------------------------------------------------------------
 * Internal representational constants
 * ------------------------------------------------------------------------- */

const BLOCKS_PER_CHUNK: usize = 1 << 16;
const BLOCKS_PER_ENTRY: usize = BITS_PER_BYTE * core::mem::size_of::<uint16>();
const MAX_ENTRIES_PER_CHUNK: usize = BLOCKS_PER_CHUNK / BLOCKS_PER_ENTRY;
const INITIAL_ENTRIES_PER_CHUNK: usize = 16;

/* BlockRefTableChunk == uint16 * */
type BlockRefTableChunk = *mut uint16;

/* Buffer size, so that we avoid doing many small I/Os. */
const BUFSIZE: usize = 65536;

/* ----------------------------------------------------------------------------
 * Hash key + entry (the simplehash SH_ELEMENT_TYPE / SH_KEY_TYPE).
 * ------------------------------------------------------------------------- */

/*
 * A block reference table keeps track of the status of each relation fork
 * individually.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BlockRefTableKey {
    pub rlocator: RelFileLocator,
    pub forknum: ForkNumber,
}

impl BlockRefTableKey {
    /* C uses `BlockRefTableKey key = {0}` to ensure padding is zero before the
     * memcpy of rlocator + forknum, so the raw-byte hash/compare is stable. */
    fn zeroed() -> Self {
        unsafe { core::mem::zeroed() }
    }
}

/*
 * State for one relation fork.  Mirrors the C struct BlockRefTableEntry, which
 * embeds the key plus separately-palloc'd variable chunk arrays.  We keep those
 * as raw pointers and per-entry palloc, exactly as the C does.  A `status` byte
 * is added for the simplehash slot bookkeeping (the C template stores it inline
 * as `char status`).
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BlockRefTableEntry {
    pub key: BlockRefTableKey,
    pub limit_block: BlockNumber,
    pub status: u8,
    pub nchunks: uint32,
    pub chunk_size: *mut uint16,
    pub chunk_usage: *mut uint16,
    pub chunk_data: *mut BlockRefTableChunk,
}

/*
 * On-disk serialization format for block reference table entries.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct BlockRefTableSerializedEntry {
    pub rlocator: RelFileLocator,
    pub forknum: ForkNumber,
    pub limit_block: BlockNumber,
    pub nchunks: uint32,
}

/* ----------------------------------------------------------------------------
 * simplehash instantiation: `blockreftable`
 * ------------------------------------------------------------------------- */

pub struct BlockRefTableOps;

impl SimpleHashOps for BlockRefTableOps {
    type Elem = BlockRefTableEntry;
    type Key = BlockRefTableKey;

    fn empty_elem() -> BlockRefTableEntry {
        let mut e: BlockRefTableEntry = unsafe { core::mem::zeroed() };
        e.status = SH_STATUS_EMPTY;
        e
    }
    fn status(e: &BlockRefTableEntry) -> u8 {
        e.status
    }
    fn set_status(e: &mut BlockRefTableEntry, s: u8) {
        e.status = s;
    }
    fn hash_key(key: BlockRefTableKey) -> u32 {
        /* SH_HASH_KEY: hash_bytes((unsigned char *) &key, sizeof(key)). */
        unsafe {
            hash_bytes(
                &key as *const BlockRefTableKey as *const c_uchar,
                core::mem::size_of::<BlockRefTableKey>() as c_int,
            )
        }
    }
    fn entry_hash(e: &BlockRefTableEntry) -> u32 {
        Self::hash_key(e.key)
    }
    fn set_key(e: &mut BlockRefTableEntry, key: BlockRefTableKey) {
        e.key = key;
    }
    fn keys_equal(e: &BlockRefTableEntry, key: BlockRefTableKey) -> bool {
        /* SH_EQUAL: memcmp(&a, &b, sizeof(key)) == 0. */
        unsafe {
            memcmp(
                &e.key as *const BlockRefTableKey as *const c_void,
                &key as *const BlockRefTableKey as *const c_void,
                core::mem::size_of::<BlockRefTableKey>(),
            ) == 0
        }
    }
}

type BlockRefTableHash = SimpleHash<BlockRefTableOps>;

/*
 * A block reference table is basically just the hash table, but we don't want
 * to expose that to outside callers.  We keep track of the memory context in
 * use too (#ifndef FRONTEND), so allocations land in the same context.
 */
pub struct BlockRefTable {
    pub hash: BlockRefTableHash,
    pub mcxt: MemoryContext,
}

/*
 * Ad-hoc buffer for file I/O.
 */
#[repr(C)]
pub struct BlockRefTableBuffer {
    pub io_callback: io_callback_fn,
    pub io_callback_arg: *mut c_void,
    pub data: [c_char; BUFSIZE],
    pub used: c_int,
    pub cursor: c_int,
    pub crc: pg_crc32c,
}

/*
 * State for keeping track of progress while incrementally reading a block
 * table reference file from disk.
 */
pub struct BlockRefTableReader {
    pub buffer: BlockRefTableBuffer,
    pub error_filename: *mut c_char,
    pub error_callback: report_error_fn,
    pub error_callback_arg: *mut c_void,
    pub total_chunks: uint32,
    pub consumed_chunks: uint32,
    pub chunk_size: *mut uint16,
    pub chunk_data: [uint16; MAX_ENTRIES_PER_CHUNK],
    pub chunk_position: uint32,
}

/*
 * State for keeping track of progress while incrementally writing a block
 * reference table file to disk.
 */
pub struct BlockRefTableWriter {
    pub buffer: BlockRefTableBuffer,
}

/* ----------------------------------------------------------------------------
 * Entire in-memory block reference table.
 * ------------------------------------------------------------------------- */

/*
 * Create an empty block reference table.
 */
pub unsafe fn CreateEmptyBlockRefTable() -> *mut BlockRefTable {
    let brtab = palloc(core::mem::size_of::<BlockRefTable>()) as *mut BlockRefTable;

    /*
     * Even completely empty database has a few hundred relation forks, so it
     * seems best to size the hash on the assumption that we're going to have at
     * least a few thousand entries.
     */
    (*brtab).mcxt = CurrentMemoryContext;
    core::ptr::write(&mut (*brtab).hash, BlockRefTableHash::create(4096));

    brtab
}

/*
 * Set the "limit block" for a relation fork and forget any modified blocks with
 * equal or higher block numbers.
 */
pub unsafe fn BlockRefTableSetLimitBlock(
    brtab: *mut BlockRefTable,
    rlocator: *const RelFileLocator,
    forknum: ForkNumber,
    limit_block: BlockNumber,
) {
    let mut key = BlockRefTableKey::zeroed();
    memcpy(
        &mut key.rlocator as *mut RelFileLocator as *mut c_void,
        rlocator as *const c_void,
        core::mem::size_of::<RelFileLocator>(),
    );
    key.forknum = forknum;

    let (idx, found) = (*brtab).hash.insert(key);
    let brtentry = (*brtab).hash.entry_mut(idx) as *mut BlockRefTableEntry;

    if !found {
        /*
         * We have no existing data about this relation fork, so just record the
         * limit_block value supplied by the caller, and make sure other parts of
         * the entry are properly initialized.
         */
        (*brtentry).limit_block = limit_block;
        (*brtentry).nchunks = 0;
        (*brtentry).chunk_size = null_mut();
        (*brtentry).chunk_usage = null_mut();
        (*brtentry).chunk_data = null_mut();
        return;
    }

    BlockRefTableEntrySetLimitBlock(brtentry, limit_block);
}

/*
 * Mark a block in a given relation fork as known to have been modified.
 */
pub unsafe fn BlockRefTableMarkBlockModified(
    brtab: *mut BlockRefTable,
    rlocator: *const RelFileLocator,
    forknum: ForkNumber,
    blknum: BlockNumber,
) {
    let mut key = BlockRefTableKey::zeroed();
    let oldcontext = MemoryContextSwitchTo((*brtab).mcxt);

    memcpy(
        &mut key.rlocator as *mut RelFileLocator as *mut c_void,
        rlocator as *const c_void,
        core::mem::size_of::<RelFileLocator>(),
    );
    key.forknum = forknum;

    let (idx, found) = (*brtab).hash.insert(key);
    let brtentry = (*brtab).hash.entry_mut(idx) as *mut BlockRefTableEntry;

    if !found {
        /*
         * We want to set the initial limit block value to something higher than
         * any legal block number.  InvalidBlockNumber fits the bill.
         */
        (*brtentry).limit_block = InvalidBlockNumber;
        (*brtentry).nchunks = 0;
        (*brtentry).chunk_size = null_mut();
        (*brtentry).chunk_usage = null_mut();
        (*brtentry).chunk_data = null_mut();
    }

    BlockRefTableEntryMarkBlockModified(brtentry, forknum, blknum);

    MemoryContextSwitchTo(oldcontext);
}

/*
 * Get an entry from a block reference table.
 *
 * If the entry does not exist, this function returns NULL.  Otherwise, it
 * returns the entry and sets *limit_block to the value from the entry.
 */
pub unsafe fn BlockRefTableGetEntry(
    brtab: *mut BlockRefTable,
    rlocator: *const RelFileLocator,
    forknum: ForkNumber,
    limit_block: *mut BlockNumber,
) -> *mut BlockRefTableEntry {
    let mut key = BlockRefTableKey::zeroed();

    Assert!(!limit_block.is_null());

    memcpy(
        &mut key.rlocator as *mut RelFileLocator as *mut c_void,
        rlocator as *const c_void,
        core::mem::size_of::<RelFileLocator>(),
    );
    key.forknum = forknum;

    match (*brtab).hash.lookup(key) {
        Some(idx) => {
            let entry = (*brtab).hash.entry_mut(idx) as *mut BlockRefTableEntry;
            *limit_block = (*entry).limit_block;
            entry
        }
        None => null_mut(),
    }
}

/*
 * Get block numbers from a table entry.
 *
 * 'blocks' must point to enough space to hold at least 'nblocks' block numbers,
 * and any block numbers we manage to get will be written there.  The return
 * value is the number of block numbers actually written.
 *
 * We do not return block numbers unless they are greater than or equal to
 * start_blkno and strictly less than stop_blkno.
 */
pub unsafe fn BlockRefTableEntryGetBlocks(
    entry: *mut BlockRefTableEntry,
    start_blkno: BlockNumber,
    stop_blkno: BlockNumber,
    blocks: *mut BlockNumber,
    nblocks: c_int,
) -> c_int {
    Assert!(!entry.is_null());

    /*
     * Figure out which chunks could potentially contain blocks of interest.
     * We need to be careful about overflow here, because stop_blkno could be
     * InvalidBlockNumber or something very close to it.
     */
    let start_chunkno: uint32 = start_blkno / BLOCKS_PER_CHUNK as uint32;
    let mut stop_chunkno: uint32 = stop_blkno / BLOCKS_PER_CHUNK as uint32;
    if (stop_blkno % BLOCKS_PER_CHUNK as uint32) != 0 {
        stop_chunkno += 1;
    }
    if stop_chunkno > (*entry).nchunks {
        stop_chunkno = (*entry).nchunks;
    }

    let mut nresults: c_int = 0;

    /* Loop over chunks. */
    let mut chunkno = start_chunkno;
    while chunkno < stop_chunkno {
        let chunk_usage = *(*entry).chunk_usage.add(chunkno as usize);
        let chunk_data = *(*entry).chunk_data.add(chunkno as usize);
        let mut start_offset: usize = 0;
        let mut stop_offset: usize = BLOCKS_PER_CHUNK;

        /*
         * If the start and/or stop block number falls within this chunk, the
         * whole chunk may not be of interest.
         */
        if chunkno == start_chunkno {
            start_offset = (start_blkno % BLOCKS_PER_CHUNK as uint32) as usize;
        }
        if chunkno == stop_chunkno - 1 {
            Assert!(stop_blkno > chunkno * BLOCKS_PER_CHUNK as uint32);
            stop_offset = (stop_blkno - (chunkno * BLOCKS_PER_CHUNK as uint32)) as usize;
            Assert!(stop_offset <= BLOCKS_PER_CHUNK);
        }

        if chunk_usage as usize == MAX_ENTRIES_PER_CHUNK {
            /* It's a bitmap, so test every relevant bit. */
            let mut i = start_offset;
            while i < stop_offset {
                let w = *chunk_data.add(i / BLOCKS_PER_ENTRY);
                if (w & (1u16 << (i % BLOCKS_PER_ENTRY))) != 0 {
                    let blkno = chunkno * BLOCKS_PER_CHUNK as uint32 + i as uint32;
                    *blocks.add(nresults as usize) = blkno;
                    nresults += 1;
                    if nresults == nblocks {
                        return nresults;
                    }
                }
                i += 1;
            }
        } else {
            /* It's an array of offsets, so check each one. */
            let mut i: usize = 0;
            while i < chunk_usage as usize {
                let offset = *chunk_data.add(i) as usize;
                if offset >= start_offset && offset < stop_offset {
                    let blkno = chunkno * BLOCKS_PER_CHUNK as uint32 + offset as uint32;
                    *blocks.add(nresults as usize) = blkno;
                    nresults += 1;
                    if nresults == nblocks {
                        return nresults;
                    }
                }
                i += 1;
            }
        }

        chunkno += 1;
    }

    nresults
}

/*
 * Serialize a block reference table to a file.
 */
pub unsafe fn WriteBlockRefTable(
    brtab: *mut BlockRefTable,
    write_callback: io_callback_fn,
    write_callback_arg: *mut c_void,
) {
    // Build the buffer with the callback set (can't core::mem::zeroed() a struct
    // holding a bare fn pointer).
    let mut buffer = BlockRefTableBuffer {
        io_callback: write_callback,
        io_callback_arg: write_callback_arg,
        data: [0; BUFSIZE],
        used: 0,
        cursor: 0,
        crc: INIT_CRC32C(),
    };
    let magic: uint32 = BLOCKREFTABLE_MAGIC;

    /* Write magic number. */
    BlockRefTableWrite(
        &mut buffer,
        &magic as *const uint32 as *mut c_void,
        core::mem::size_of::<uint32>() as c_int,
    );

    let members = (*brtab).hash.members();

    /* Write the entries, assuming there are some. */
    if members > 0 {
        /* Extract entries into serializable format and sort them. */
        let sdata = palloc(members as usize * core::mem::size_of::<BlockRefTableSerializedEntry>())
            as *mut BlockRefTableSerializedEntry;

        let mut it: SimpleHashIterator = (*brtab).hash.start_iterate();
        let mut i: usize = 0;
        while let Some(idx) = (*brtab).hash.iterate(&mut it) {
            let brtentry = (*brtab).hash.entry(idx) as *const BlockRefTableEntry;
            let sentry = sdata.add(i);
            i += 1;

            (*sentry).rlocator = (*brtentry).key.rlocator;
            (*sentry).forknum = (*brtentry).key.forknum;
            (*sentry).limit_block = (*brtentry).limit_block;
            (*sentry).nchunks = (*brtentry).nchunks;

            /* trim trailing zero entries */
            while (*sentry).nchunks > 0
                && *(*brtentry).chunk_usage.add(((*sentry).nchunks - 1) as usize) == 0
            {
                (*sentry).nchunks -= 1;
            }
        }
        Assert!(i == members as usize);
        pg_qsort(
            sdata as *mut c_void,
            i,
            core::mem::size_of::<BlockRefTableSerializedEntry>(),
            BlockRefTableComparator,
        );

        /* Loop over entries in sorted order and serialize each one. */
        for i in 0..members as usize {
            let sentry = sdata.add(i);
            let mut key = BlockRefTableKey::zeroed();

            /* Write the serialized entry itself. */
            BlockRefTableWrite(
                &mut buffer,
                sentry as *mut c_void,
                core::mem::size_of::<BlockRefTableSerializedEntry>() as c_int,
            );

            /* Look up the original entry so we can access the chunks. */
            memcpy(
                &mut key.rlocator as *mut RelFileLocator as *mut c_void,
                &(*sentry).rlocator as *const RelFileLocator as *const c_void,
                core::mem::size_of::<RelFileLocator>(),
            );
            key.forknum = (*sentry).forknum;
            let idx = (*brtab).hash.lookup(key).expect("entry must exist");
            let brtentry = (*brtab).hash.entry(idx) as *const BlockRefTableEntry;

            /* Write the untruncated portion of the chunk length array. */
            if (*sentry).nchunks != 0 {
                BlockRefTableWrite(
                    &mut buffer,
                    (*brtentry).chunk_usage as *mut c_void,
                    ((*sentry).nchunks as usize * core::mem::size_of::<uint16>()) as c_int,
                );
            }

            /* Write the contents of each chunk. */
            for j in 0..(*brtentry).nchunks as usize {
                let usage = *(*brtentry).chunk_usage.add(j);
                if usage == 0 {
                    continue;
                }
                BlockRefTableWrite(
                    &mut buffer,
                    *(*brtentry).chunk_data.add(j) as *mut c_void,
                    (usage as usize * core::mem::size_of::<uint16>()) as c_int,
                );
            }
        }
    }

    /* Write out appropriate terminator and CRC and flush buffer. */
    BlockRefTableFileTerminate(&mut buffer);
}

/* ----------------------------------------------------------------------------
 * Incremental reader.
 * ------------------------------------------------------------------------- */

/*
 * Prepare to incrementally read a block reference table file.
 */
pub unsafe fn CreateBlockRefTableReader(
    read_callback: io_callback_fn,
    read_callback_arg: *mut c_void,
    error_filename: *mut c_char,
    error_callback: report_error_fn,
    error_callback_arg: *mut c_void,
) -> *mut BlockRefTableReader {
    let reader = palloc0(core::mem::size_of::<BlockRefTableReader>()) as *mut BlockRefTableReader;
    (*reader).buffer.io_callback = read_callback;
    (*reader).buffer.io_callback_arg = read_callback_arg;
    (*reader).error_filename = error_filename;
    (*reader).error_callback = error_callback;
    (*reader).error_callback_arg = error_callback_arg;
    (*reader).buffer.crc = INIT_CRC32C();

    /* Verify magic number. */
    let mut magic: uint32 = 0;
    BlockRefTableRead(
        reader,
        &mut magic as *mut uint32 as *mut c_void,
        core::mem::size_of::<uint32>() as c_int,
    );
    if magic != BLOCKREFTABLE_MAGIC {
        (error_callback)(
            error_callback_arg,
            &format!(
                "file \"{}\" has wrong magic number: expected {}, found {}",
                cstr_to_string(error_filename),
                BLOCKREFTABLE_MAGIC,
                magic
            ),
        );
    }

    reader
}

/*
 * Read next relation fork covered by this block reference table file.
 *
 * After calling this function, you must call BlockRefTableReaderGetBlocks until
 * it returns 0 before calling it again.
 */
pub unsafe fn BlockRefTableReaderNextRelation(
    reader: *mut BlockRefTableReader,
    rlocator: *mut RelFileLocator,
    forknum: *mut ForkNumber,
    limit_block: *mut BlockNumber,
) -> bool {
    let mut sentry: BlockRefTableSerializedEntry = core::mem::zeroed();
    let zentry: BlockRefTableSerializedEntry = core::mem::zeroed();

    /*
     * Sanity check: caller must read all blocks from all chunks before moving on
     * to the next relation.
     */
    Assert!((*reader).total_chunks == (*reader).consumed_chunks);

    /* Read serialized entry. */
    BlockRefTableRead(
        reader,
        &mut sentry as *mut BlockRefTableSerializedEntry as *mut c_void,
        core::mem::size_of::<BlockRefTableSerializedEntry>() as c_int,
    );

    /*
     * If we just read the sentinel entry indicating that we've reached the end,
     * read and check the CRC.
     */
    if memcmp(
        &sentry as *const BlockRefTableSerializedEntry as *const c_void,
        &zentry as *const BlockRefTableSerializedEntry as *const c_void,
        core::mem::size_of::<BlockRefTableSerializedEntry>(),
    ) == 0
    {
        /*
         * We want to know the CRC of the file excluding the 4-byte CRC itself,
         * so copy the current value of the CRC accumulator before reading those
         * bytes, and use the copy to finalize the calculation.
         */
        let mut expected_crc = (*reader).buffer.crc;
        expected_crc = FIN_CRC32C(expected_crc);

        /* Now we can read the actual value. */
        let mut actual_crc: pg_crc32c = 0;
        BlockRefTableRead(
            reader,
            &mut actual_crc as *mut pg_crc32c as *mut c_void,
            core::mem::size_of::<pg_crc32c>() as c_int,
        );

        /* Throw an error if there is a mismatch. */
        if !EQ_CRC32C(expected_crc, actual_crc) {
            ((*reader).error_callback)(
                (*reader).error_callback_arg,
                &format!(
                    "file \"{}\" has wrong checksum: expected {:08X}, found {:08X}",
                    cstr_to_string((*reader).error_filename),
                    expected_crc,
                    actual_crc
                ),
            );
        }

        return false;
    }

    /* Read chunk size array. */
    if !(*reader).chunk_size.is_null() {
        pfree((*reader).chunk_size as *mut c_void);
    }
    (*reader).chunk_size =
        palloc(sentry.nchunks as usize * core::mem::size_of::<uint16>()) as *mut uint16;
    BlockRefTableRead(
        reader,
        (*reader).chunk_size as *mut c_void,
        (sentry.nchunks as usize * core::mem::size_of::<uint16>()) as c_int,
    );

    /* Set up for chunk scan. */
    (*reader).total_chunks = sentry.nchunks;
    (*reader).consumed_chunks = 0;

    /* Return data to caller. */
    memcpy(
        rlocator as *mut c_void,
        &sentry.rlocator as *const RelFileLocator as *const c_void,
        core::mem::size_of::<RelFileLocator>(),
    );
    *forknum = sentry.forknum;
    *limit_block = sentry.limit_block;
    true
}

/*
 * Get modified blocks associated with the relation fork returned by the most
 * recent call to BlockRefTableReaderNextRelation.
 */
pub unsafe fn BlockRefTableReaderGetBlocks(
    reader: *mut BlockRefTableReader,
    blocks: *mut BlockNumber,
    nblocks: c_int,
) -> c_uint {
    let mut blocks_found: c_uint = 0;

    /* Must provide space for at least one block number to be returned. */
    Assert!(nblocks > 0);

    /* Loop collecting blocks to return to caller. */
    loop {
        /*
         * If we've read at least one chunk, maybe it contains some block numbers
         * that could satisfy caller's request.
         */
        if (*reader).consumed_chunks > 0 {
            let chunkno = (*reader).consumed_chunks - 1;
            let chunk_size = *(*reader).chunk_size.add(chunkno as usize);

            if chunk_size as usize == MAX_ENTRIES_PER_CHUNK {
                /* Bitmap format, so search for bits that are set. */
                while ((*reader).chunk_position as usize) < BLOCKS_PER_CHUNK
                    && (blocks_found as c_int) < nblocks
                {
                    let chunkoffset = (*reader).chunk_position as u16;
                    let w = (*reader).chunk_data[chunkoffset as usize / BLOCKS_PER_ENTRY];
                    if (w & (1u16 << (chunkoffset as usize % BLOCKS_PER_ENTRY))) != 0 {
                        *blocks.add(blocks_found as usize) =
                            chunkno * BLOCKS_PER_CHUNK as uint32 + chunkoffset as uint32;
                        blocks_found += 1;
                    }
                    (*reader).chunk_position += 1;
                }
            } else {
                /* Not in bitmap format, so each entry is a 2-byte offset. */
                while (*reader).chunk_position < chunk_size as uint32
                    && (blocks_found as c_int) < nblocks
                {
                    *blocks.add(blocks_found as usize) = chunkno * BLOCKS_PER_CHUNK as uint32
                        + (*reader).chunk_data[(*reader).chunk_position as usize] as uint32;
                    blocks_found += 1;
                    (*reader).chunk_position += 1;
                }
            }
        }

        /* We found enough blocks, so we're done. */
        if (blocks_found as c_int) >= nblocks {
            break;
        }

        /*
         * We didn't find enough blocks, so we must need the next chunk.  If there
         * are none left, though, then we're done anyway.
         */
        if (*reader).consumed_chunks == (*reader).total_chunks {
            break;
        }

        /*
         * Read data for next chunk and reset scan position to beginning of chunk.
         * Note that the next chunk might be empty, in which case we consume the
         * chunk without actually consuming any bytes from the underlying file.
         */
        let next_chunk_size = *(*reader).chunk_size.add((*reader).consumed_chunks as usize);
        if next_chunk_size > 0 {
            let ptr = (*reader).chunk_data.as_mut_ptr();
            BlockRefTableRead(
                reader,
                ptr as *mut c_void,
                (next_chunk_size as usize * core::mem::size_of::<uint16>()) as c_int,
            );
        }
        (*reader).consumed_chunks += 1;
        (*reader).chunk_position = 0;
    }

    blocks_found
}

/*
 * Release memory used while reading a block reference table from a file.
 */
pub unsafe fn DestroyBlockRefTableReader(reader: *mut BlockRefTableReader) {
    if !(*reader).chunk_size.is_null() {
        pfree((*reader).chunk_size as *mut c_void);
        (*reader).chunk_size = null_mut();
    }
    pfree(reader as *mut c_void);
}

/* ----------------------------------------------------------------------------
 * Incremental writer.
 * ------------------------------------------------------------------------- */

/*
 * Prepare to write a block reference table file incrementally.
 */
pub unsafe fn CreateBlockRefTableWriter(
    write_callback: io_callback_fn,
    write_callback_arg: *mut c_void,
) -> *mut BlockRefTableWriter {
    let magic: uint32 = BLOCKREFTABLE_MAGIC;

    /* Prepare buffer and CRC check and save callbacks. */
    let writer = palloc0(core::mem::size_of::<BlockRefTableWriter>()) as *mut BlockRefTableWriter;
    (*writer).buffer.io_callback = write_callback;
    (*writer).buffer.io_callback_arg = write_callback_arg;
    (*writer).buffer.crc = INIT_CRC32C();

    /* Write magic number. */
    BlockRefTableWrite(
        &mut (*writer).buffer,
        &magic as *const uint32 as *mut c_void,
        core::mem::size_of::<uint32>() as c_int,
    );

    writer
}

/*
 * Append one entry to a block reference table file.
 *
 * Note that entries must be written in the proper order, that is, sorted by
 * tablespace, then database, then relfilenumber, then fork number.  Caller is
 * responsible for supplying data in the correct order.
 */
pub unsafe fn BlockRefTableWriteEntry(
    writer: *mut BlockRefTableWriter,
    entry: *mut BlockRefTableEntry,
) {
    let mut sentry: BlockRefTableSerializedEntry = core::mem::zeroed();

    /* Convert to serialized entry format. */
    sentry.rlocator = (*entry).key.rlocator;
    sentry.forknum = (*entry).key.forknum;
    sentry.limit_block = (*entry).limit_block;
    sentry.nchunks = (*entry).nchunks;

    /* Trim trailing zero entries. */
    while sentry.nchunks > 0 && *(*entry).chunk_usage.add((sentry.nchunks - 1) as usize) == 0 {
        sentry.nchunks -= 1;
    }

    /* Write the serialized entry itself. */
    BlockRefTableWrite(
        &mut (*writer).buffer,
        &mut sentry as *mut BlockRefTableSerializedEntry as *mut c_void,
        core::mem::size_of::<BlockRefTableSerializedEntry>() as c_int,
    );

    /* Write the untruncated portion of the chunk length array. */
    if sentry.nchunks != 0 {
        BlockRefTableWrite(
            &mut (*writer).buffer,
            (*entry).chunk_usage as *mut c_void,
            (sentry.nchunks as usize * core::mem::size_of::<uint16>()) as c_int,
        );
    }

    /* Write the contents of each chunk. */
    for j in 0..(*entry).nchunks as usize {
        let usage = *(*entry).chunk_usage.add(j);
        if usage == 0 {
            continue;
        }
        BlockRefTableWrite(
            &mut (*writer).buffer,
            *(*entry).chunk_data.add(j) as *mut c_void,
            (usage as usize * core::mem::size_of::<uint16>()) as c_int,
        );
    }
}

/*
 * Finalize an incremental write of a block reference table file.
 */
pub unsafe fn DestroyBlockRefTableWriter(writer: *mut BlockRefTableWriter) {
    BlockRefTableFileTerminate(&mut (*writer).buffer);
    pfree(writer as *mut c_void);
}

/* ----------------------------------------------------------------------------
 * Standalone entry manipulation.
 * ------------------------------------------------------------------------- */

/*
 * Allocate a standalone BlockRefTableEntry.
 */
pub unsafe fn CreateBlockRefTableEntry(
    rlocator: RelFileLocator,
    forknum: ForkNumber,
) -> *mut BlockRefTableEntry {
    let entry = palloc0(core::mem::size_of::<BlockRefTableEntry>()) as *mut BlockRefTableEntry;

    memcpy(
        &mut (*entry).key.rlocator as *mut RelFileLocator as *mut c_void,
        &rlocator as *const RelFileLocator as *const c_void,
        core::mem::size_of::<RelFileLocator>(),
    );
    (*entry).key.forknum = forknum;
    (*entry).limit_block = InvalidBlockNumber;

    entry
}

/*
 * Update a BlockRefTableEntry with a new value for the "limit block" and forget
 * any equal-or-higher-numbered modified blocks.
 */
pub unsafe fn BlockRefTableEntrySetLimitBlock(
    entry: *mut BlockRefTableEntry,
    limit_block: BlockNumber,
) {
    /* If we already have an equal or lower limit block, do nothing. */
    if limit_block >= (*entry).limit_block {
        return;
    }

    /* Record the new limit block value. */
    (*entry).limit_block = limit_block;

    /*
     * Figure out which chunk would store the state of the new limit block, and
     * which offset within that chunk.
     */
    let limit_chunkno = (limit_block / BLOCKS_PER_CHUNK as uint32) as usize;
    let limit_chunkoffset = (limit_block % BLOCKS_PER_CHUNK as uint32) as usize;

    /*
     * If the number of chunks is not large enough for any blocks with equal or
     * higher block numbers to exist, then there is nothing further to do.
     */
    if limit_chunkno >= (*entry).nchunks as usize {
        return;
    }

    /* Discard entire contents of any higher-numbered chunks. */
    for chunkno in (limit_chunkno + 1)..(*entry).nchunks as usize {
        *(*entry).chunk_usage.add(chunkno) = 0;
    }

    /*
     * Next, we need to discard any offsets within the chunk that would contain
     * the limit_block.
     */
    let limit_chunk = *(*entry).chunk_data.add(limit_chunkno);
    if *(*entry).chunk_usage.add(limit_chunkno) as usize == MAX_ENTRIES_PER_CHUNK {
        /* It's a bitmap.  Unset bits. */
        for chunkoffset in limit_chunkoffset..BLOCKS_PER_CHUNK {
            *limit_chunk.add(chunkoffset / BLOCKS_PER_ENTRY) &=
                !(1u16 << (chunkoffset % BLOCKS_PER_ENTRY));
        }
    } else {
        /* It's an offset array.  Filter out large offsets. */
        let mut j: usize = 0;
        let usage = *(*entry).chunk_usage.add(limit_chunkno) as usize;
        for i in 0..usage {
            Assert!(j <= i);
            let v = *limit_chunk.add(i);
            if (v as usize) < limit_chunkoffset {
                *limit_chunk.add(j) = v;
                j += 1;
            }
        }
        Assert!(j <= usage);
        *(*entry).chunk_usage.add(limit_chunkno) = j as uint16;
    }
}

/*
 * Mark a block in a given BlockRefTableEntry as known to have been modified.
 */
pub unsafe fn BlockRefTableEntryMarkBlockModified(
    entry: *mut BlockRefTableEntry,
    _forknum: ForkNumber,
    blknum: BlockNumber,
) {
    /*
     * Which chunk should store the state of this block?  And what is the offset
     * of this block relative to the start of that chunk?
     */
    let chunkno = (blknum / BLOCKS_PER_CHUNK as uint32) as usize;
    let chunkoffset = (blknum % BLOCKS_PER_CHUNK as uint32) as usize;

    /*
     * If 'nchunks' isn't big enough for us to be able to represent the state of
     * this block, we need to enlarge our arrays.
     */
    if chunkno >= (*entry).nchunks as usize {
        /*
         * New array size is a power of 2, at least 16, big enough so that chunkno
         * will be a valid array index.
         */
        let mut max_chunks = Max(16usize, (*entry).nchunks as usize);
        while max_chunks < chunkno + 1 {
            max_chunks *= 2;
        }
        let extra_chunks = max_chunks - (*entry).nchunks as usize;

        if (*entry).nchunks == 0 {
            (*entry).chunk_size =
                palloc0(core::mem::size_of::<uint16>() * max_chunks) as *mut uint16;
            (*entry).chunk_usage =
                palloc0(core::mem::size_of::<uint16>() * max_chunks) as *mut uint16;
            (*entry).chunk_data = palloc0(
                core::mem::size_of::<BlockRefTableChunk>() * max_chunks,
            ) as *mut BlockRefTableChunk;
        } else {
            (*entry).chunk_size = repalloc(
                (*entry).chunk_size as *mut c_void,
                core::mem::size_of::<uint16>() * max_chunks,
            ) as *mut uint16;
            memset(
                (*entry).chunk_size.add((*entry).nchunks as usize) as *mut c_void,
                0,
                extra_chunks * core::mem::size_of::<uint16>(),
            );
            (*entry).chunk_usage = repalloc(
                (*entry).chunk_usage as *mut c_void,
                core::mem::size_of::<uint16>() * max_chunks,
            ) as *mut uint16;
            memset(
                (*entry).chunk_usage.add((*entry).nchunks as usize) as *mut c_void,
                0,
                extra_chunks * core::mem::size_of::<uint16>(),
            );
            (*entry).chunk_data = repalloc(
                (*entry).chunk_data as *mut c_void,
                core::mem::size_of::<BlockRefTableChunk>() * max_chunks,
            ) as *mut BlockRefTableChunk;
            memset(
                (*entry).chunk_data.add((*entry).nchunks as usize) as *mut c_void,
                0,
                extra_chunks * core::mem::size_of::<BlockRefTableChunk>(),
            );
        }
        (*entry).nchunks = max_chunks as uint32;
    }

    /*
     * If the chunk that covers this block number doesn't exist yet, create it as
     * an array and add the appropriate offset to it.
     */
    if *(*entry).chunk_size.add(chunkno) == 0 {
        let data = palloc(core::mem::size_of::<uint16>() * INITIAL_ENTRIES_PER_CHUNK) as *mut uint16;
        *(*entry).chunk_data.add(chunkno) = data;
        *(*entry).chunk_size.add(chunkno) = INITIAL_ENTRIES_PER_CHUNK as uint16;
        *data.add(0) = chunkoffset as uint16;
        *(*entry).chunk_usage.add(chunkno) = 1;
        return;
    }

    /*
     * If the number of entries in this chunk is already maximum, it must be a
     * bitmap.  Just set the appropriate bit.
     */
    if *(*entry).chunk_usage.add(chunkno) as usize == MAX_ENTRIES_PER_CHUNK {
        let chunk = *(*entry).chunk_data.add(chunkno);
        *chunk.add(chunkoffset / BLOCKS_PER_ENTRY) |= 1u16 << (chunkoffset % BLOCKS_PER_ENTRY);
        return;
    }

    /*
     * There is an existing chunk and it's in array format.  Let's find out
     * whether it already has an entry for this block.  If so, do nothing.
     */
    {
        let chunk = *(*entry).chunk_data.add(chunkno);
        let usage = *(*entry).chunk_usage.add(chunkno) as usize;
        for i in 0..usage {
            if *chunk.add(i) as usize == chunkoffset {
                return;
            }
        }
    }

    /*
     * If the number of entries currently used is one less than the maximum, it's
     * time to convert to bitmap format.
     */
    if *(*entry).chunk_usage.add(chunkno) as usize == MAX_ENTRIES_PER_CHUNK - 1 {
        /* Allocate a new chunk. */
        let newchunk = palloc0(MAX_ENTRIES_PER_CHUNK * core::mem::size_of::<uint16>()) as *mut uint16;

        let oldchunk = *(*entry).chunk_data.add(chunkno);
        let usage = *(*entry).chunk_usage.add(chunkno) as usize;

        /* Set the bit for each existing entry. */
        for j in 0..usage {
            let coff = *oldchunk.add(j) as usize;
            *newchunk.add(coff / BLOCKS_PER_ENTRY) |= 1u16 << (coff % BLOCKS_PER_ENTRY);
        }

        /* Set the bit for the new entry. */
        *newchunk.add(chunkoffset / BLOCKS_PER_ENTRY) |= 1u16 << (chunkoffset % BLOCKS_PER_ENTRY);

        /* Swap the new chunk into place and update metadata. */
        pfree(oldchunk as *mut c_void);
        *(*entry).chunk_data.add(chunkno) = newchunk;
        *(*entry).chunk_size.add(chunkno) = MAX_ENTRIES_PER_CHUNK as uint16;
        *(*entry).chunk_usage.add(chunkno) = MAX_ENTRIES_PER_CHUNK as uint16;
        return;
    }

    /*
     * OK, we currently have an array, and we don't need to convert to a bitmap,
     * but we do need to add a new element.  If there's not enough room, we'll
     * have to expand the array.
     */
    if *(*entry).chunk_usage.add(chunkno) == *(*entry).chunk_size.add(chunkno) {
        let newsize = *(*entry).chunk_size.add(chunkno) as usize * 2;
        Assert!(newsize <= MAX_ENTRIES_PER_CHUNK);
        *(*entry).chunk_data.add(chunkno) = repalloc(
            *(*entry).chunk_data.add(chunkno) as *mut c_void,
            newsize * core::mem::size_of::<uint16>(),
        ) as *mut uint16;
        *(*entry).chunk_size.add(chunkno) = newsize as uint16;
    }

    /* Now we can add the new entry. */
    let chunk = *(*entry).chunk_data.add(chunkno);
    let usage = *(*entry).chunk_usage.add(chunkno) as usize;
    *chunk.add(usage) = chunkoffset as uint16;
    *(*entry).chunk_usage.add(chunkno) += 1;
}

/*
 * Release memory for a BlockRefTableEntry that was created by
 * CreateBlockRefTableEntry.
 */
pub unsafe fn BlockRefTableFreeEntry(entry: *mut BlockRefTableEntry) {
    if !(*entry).chunk_size.is_null() {
        pfree((*entry).chunk_size as *mut c_void);
        (*entry).chunk_size = null_mut();
    }
    if !(*entry).chunk_usage.is_null() {
        pfree((*entry).chunk_usage as *mut c_void);
        (*entry).chunk_usage = null_mut();
    }
    if !(*entry).chunk_data.is_null() {
        pfree((*entry).chunk_data as *mut c_void);
        (*entry).chunk_data = null_mut();
    }
    pfree(entry as *mut c_void);
}

/* ----------------------------------------------------------------------------
 * Internal helpers.
 * ------------------------------------------------------------------------- */

/*
 * Comparator for BlockRefTableSerializedEntry objects.
 *
 * We make the tablespace OID the first column of the sort key to match the
 * on-disk tree structure.
 */
unsafe fn BlockRefTableComparator(a: *const c_void, b: *const c_void) -> c_int {
    let sa = a as *const BlockRefTableSerializedEntry;
    let sb = b as *const BlockRefTableSerializedEntry;

    if (*sa).rlocator.spcOid > (*sb).rlocator.spcOid {
        return 1;
    }
    if (*sa).rlocator.spcOid < (*sb).rlocator.spcOid {
        return -1;
    }

    if (*sa).rlocator.dbOid > (*sb).rlocator.dbOid {
        return 1;
    }
    if (*sa).rlocator.dbOid < (*sb).rlocator.dbOid {
        return -1;
    }

    if (*sa).rlocator.relNumber > (*sb).rlocator.relNumber {
        return 1;
    }
    if (*sa).rlocator.relNumber < (*sb).rlocator.relNumber {
        return -1;
    }

    if (*sa).forknum > (*sb).forknum {
        return 1;
    }
    if (*sa).forknum < (*sb).forknum {
        return -1;
    }

    0
}

/*
 * Flush any buffered data out of a BlockRefTableBuffer.
 */
unsafe fn BlockRefTableFlush(buffer: *mut BlockRefTableBuffer) {
    ((*buffer).io_callback)(
        (*buffer).io_callback_arg,
        (*buffer).data.as_mut_ptr() as *mut c_void,
        (*buffer).used,
    );
    (*buffer).used = 0;
}

/*
 * Read data from a BlockRefTableBuffer, and update the running CRC calculation
 * for the returned data (but not any data that we may have buffered but not yet
 * actually returned).
 */
unsafe fn BlockRefTableRead(reader: *mut BlockRefTableReader, mut data: *mut c_void, mut length: c_int) {
    let buffer = &mut (*reader).buffer as *mut BlockRefTableBuffer;

    /* Loop until read is fully satisfied. */
    while length > 0 {
        if (*buffer).cursor < (*buffer).used {
            /*
             * If any buffered data is available, use that to satisfy as much of
             * the request as possible.
             */
            let bytes_to_copy = Min(length, (*buffer).used - (*buffer).cursor);
            let src = (*buffer).data.as_ptr().add((*buffer).cursor as usize) as *const c_void;
            memcpy(data, src, bytes_to_copy as usize);
            (*buffer).crc = COMP_CRC32C((*buffer).crc, src, bytes_to_copy as usize);
            (*buffer).cursor += bytes_to_copy;
            data = (data as *mut c_char).add(bytes_to_copy as usize) as *mut c_void;
            length -= bytes_to_copy;
        } else if length as usize >= BUFSIZE {
            /*
             * If the request length is long, read directly into caller's buffer.
             */
            let bytes_read = ((*buffer).io_callback)((*buffer).io_callback_arg, data, length);
            (*buffer).crc = COMP_CRC32C((*buffer).crc, data, bytes_read as usize);
            data = (data as *mut c_char).add(bytes_read as usize) as *mut c_void;
            length -= bytes_read;

            /* If we didn't get anything, that's bad. */
            if bytes_read == 0 {
                ((*reader).error_callback)(
                    (*reader).error_callback_arg,
                    &format!(
                        "file \"{}\" ends unexpectedly",
                        cstr_to_string((*reader).error_filename)
                    ),
                );
            }
        } else {
            /* Refill our buffer. */
            (*buffer).used = ((*buffer).io_callback)(
                (*buffer).io_callback_arg,
                (*buffer).data.as_mut_ptr() as *mut c_void,
                BUFSIZE as c_int,
            );
            (*buffer).cursor = 0;

            /* If we didn't get anything, that's bad. */
            if (*buffer).used == 0 {
                ((*reader).error_callback)(
                    (*reader).error_callback_arg,
                    &format!(
                        "file \"{}\" ends unexpectedly",
                        cstr_to_string((*reader).error_filename)
                    ),
                );
            }
        }
    }
}

/*
 * Supply data to a BlockRefTableBuffer for write to the underlying File, and
 * update the running CRC calculation for that data.
 */
unsafe fn BlockRefTableWrite(buffer: *mut BlockRefTableBuffer, data: *mut c_void, length: c_int) {
    /* Update running CRC calculation. */
    (*buffer).crc = COMP_CRC32C((*buffer).crc, data, length as usize);

    /* If the new data can't fit into the buffer, flush the buffer. */
    if (*buffer).used + length > BUFSIZE as c_int {
        ((*buffer).io_callback)(
            (*buffer).io_callback_arg,
            (*buffer).data.as_mut_ptr() as *mut c_void,
            (*buffer).used,
        );
        (*buffer).used = 0;
    }

    /* If the new data would fill the buffer, or more, write it directly. */
    if length as usize >= BUFSIZE {
        ((*buffer).io_callback)((*buffer).io_callback_arg, data, length);
        return;
    }

    /* Otherwise, copy the new data into the buffer. */
    memcpy(
        (*buffer).data.as_mut_ptr().add((*buffer).used as usize) as *mut c_void,
        data,
        length as usize,
    );
    (*buffer).used += length;
    Assert!((*buffer).used <= BUFSIZE as c_int);
}

/*
 * Generate the sentinel and CRC required at the end of a block reference table
 * file and flush them out of our internal buffer.
 */
unsafe fn BlockRefTableFileTerminate(buffer: *mut BlockRefTableBuffer) {
    let mut zentry: BlockRefTableSerializedEntry = core::mem::zeroed();

    /* Write a sentinel indicating that there are no more entries. */
    BlockRefTableWrite(
        buffer,
        &mut zentry as *mut BlockRefTableSerializedEntry as *mut c_void,
        core::mem::size_of::<BlockRefTableSerializedEntry>() as c_int,
    );

    /*
     * Writing the checksum will perturb the ongoing checksum calculation, so
     * copy the state first and finalize the computation using the copy.
     */
    let mut crc = (*buffer).crc;
    crc = FIN_CRC32C(crc);
    BlockRefTableWrite(
        buffer,
        &mut crc as *mut pg_crc32c as *mut c_void,
        core::mem::size_of::<pg_crc32c>() as c_int,
    );

    /* Flush any leftover data out of our buffer. */
    BlockRefTableFlush(buffer);
}

/* Helper: render a possibly-null C string for error messages. */
unsafe fn cstr_to_string(s: *const c_char) -> String {
    if s.is_null() {
        return String::from("(null)");
    }
    let mut len = 0usize;
    while *s.add(len) != 0 {
        len += 1;
    }
    let bytes = core::slice::from_raw_parts(s as *const u8, len);
    String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn loc(spc: Oid, db: Oid, rel: RelFileNumber) -> RelFileLocator {
        RelFileLocator {
            spcOid: spc,
            dbOid: db,
            relNumber: rel,
        }
    }

    #[test]
    fn mark_and_get_blocks_two_relations() {
        unsafe {
            let brtab = CreateEmptyBlockRefTable();

            let r1 = loc(1, 100, 200);
            let r2 = loc(1, 100, 201);

            /* Mark a few blocks across two relations / forks. */
            BlockRefTableMarkBlockModified(brtab, &r1, 0, 5);
            BlockRefTableMarkBlockModified(brtab, &r1, 0, 9);
            BlockRefTableMarkBlockModified(brtab, &r1, 0, 5); /* dup ignored */
            BlockRefTableMarkBlockModified(brtab, &r2, 1, 70000); /* spans chunk 1 */

            /* r1/fork0 should report blocks 5 and 9. */
            let mut limit: BlockNumber = 0;
            let e1 = BlockRefTableGetEntry(brtab, &r1, 0, &mut limit);
            assert!(!e1.is_null());
            assert_eq!(limit, InvalidBlockNumber);
            let mut out = [0u32; 16];
            let n = BlockRefTableEntryGetBlocks(e1, 0, (*e1).nchunks * BLOCKS_PER_CHUNK as u32, out.as_mut_ptr(), 16);
            let mut got: Vec<u32> = out[..n as usize].to_vec();
            got.sort();
            assert_eq!(got, vec![5, 9]);

            /* r2/fork1 should report block 70000. */
            let e2 = BlockRefTableGetEntry(brtab, &r2, 1, &mut limit);
            assert!(!e2.is_null());
            let n2 = BlockRefTableEntryGetBlocks(e2, 0, (*e2).nchunks * BLOCKS_PER_CHUNK as u32, out.as_mut_ptr(), 16);
            assert_eq!(n2, 1);
            assert_eq!(out[0], 70000);

            /* A relation we never touched is absent. */
            let r3 = loc(2, 100, 999);
            let e3 = BlockRefTableGetEntry(brtab, &r3, 0, &mut limit);
            assert!(e3.is_null());
        }
    }

    #[test]
    fn set_limit_block_forgets_higher_blocks() {
        unsafe {
            let brtab = CreateEmptyBlockRefTable();
            let r = loc(1, 1, 1);

            BlockRefTableMarkBlockModified(brtab, &r, 0, 3);
            BlockRefTableMarkBlockModified(brtab, &r, 0, 7);
            BlockRefTableMarkBlockModified(brtab, &r, 0, 12);

            /* Truncate to 8 blocks: blocks >= 8 must be forgotten. */
            BlockRefTableSetLimitBlock(brtab, &r, 0, 8);

            let mut limit: BlockNumber = 0;
            let e = BlockRefTableGetEntry(brtab, &r, 0, &mut limit);
            assert!(!e.is_null());
            assert_eq!(limit, 8);
            let mut out = [0u32; 16];
            let n = BlockRefTableEntryGetBlocks(e, 0, (*e).nchunks * BLOCKS_PER_CHUNK as u32, out.as_mut_ptr(), 16);
            let mut got: Vec<u32> = out[..n as usize].to_vec();
            got.sort();
            assert_eq!(got, vec![3, 7]);
        }
    }

    /* In-memory Vec-backed I/O callbacks for the Write/Read round-trip. */
    struct IoBuf {
        data: Vec<u8>,
        pos: usize,
    }

    unsafe fn write_cb(arg: *mut c_void, data: *mut c_void, length: c_int) -> c_int {
        let buf = &mut *(arg as *mut IoBuf);
        let slice = core::slice::from_raw_parts(data as *const u8, length as usize);
        buf.data.extend_from_slice(slice);
        length
    }

    unsafe fn read_cb(arg: *mut c_void, data: *mut c_void, length: c_int) -> c_int {
        let buf = &mut *(arg as *mut IoBuf);
        let avail = buf.data.len() - buf.pos;
        let n = core::cmp::min(avail, length as usize);
        if n > 0 {
            core::ptr::copy_nonoverlapping(
                buf.data.as_ptr().add(buf.pos),
                data as *mut u8,
                n,
            );
            buf.pos += n;
        }
        n as c_int
    }

    unsafe fn err_cb(_arg: *mut c_void, msg: &str) -> ! {
        panic!("blkreftable read error: {}", msg);
    }

    #[test]
    fn write_read_round_trip() {
        unsafe {
            let brtab = CreateEmptyBlockRefTable();
            let r1 = loc(1, 100, 200);
            let r2 = loc(1, 100, 201);

            BlockRefTableMarkBlockModified(brtab, &r1, 0, 5);
            BlockRefTableMarkBlockModified(brtab, &r1, 0, 9);
            BlockRefTableMarkBlockModified(brtab, &r2, 0, 1);
            BlockRefTableMarkBlockModified(brtab, &r2, 0, 70000);

            /* Serialize into an in-memory buffer. */
            let mut wbuf = IoBuf { data: Vec::new(), pos: 0 };
            WriteBlockRefTable(brtab, write_cb, &mut wbuf as *mut IoBuf as *mut c_void);
            assert!(wbuf.data.len() > 4);

            /* Read it back. */
            let mut rbuf = IoBuf { data: wbuf.data.clone(), pos: 0 };
            let fname = b"test\0".as_ptr() as *mut c_char;
            let reader = CreateBlockRefTableReader(
                read_cb,
                &mut rbuf as *mut IoBuf as *mut c_void,
                fname,
                err_cb,
                core::ptr::null_mut(),
            );

            /* Collect all (rlocator, forknum) -> sorted blocks. */
            let mut seen: Vec<(Oid, Oid, RelFileNumber, ForkNumber, Vec<u32>)> = Vec::new();
            loop {
                let mut rl: RelFileLocator = core::mem::zeroed();
                let mut fk: ForkNumber = 0;
                let mut lim: BlockNumber = 0;
                if !BlockRefTableReaderNextRelation(reader, &mut rl, &mut fk, &mut lim) {
                    break;
                }
                let mut blocks: Vec<u32> = Vec::new();
                let mut out = [0u32; 4];
                loop {
                    let n = BlockRefTableReaderGetBlocks(reader, out.as_mut_ptr(), 4);
                    if n == 0 {
                        break;
                    }
                    blocks.extend_from_slice(&out[..n as usize]);
                }
                blocks.sort();
                seen.push((rl.spcOid, rl.dbOid, rl.relNumber, fk, blocks));
            }
            DestroyBlockRefTableReader(reader);

            seen.sort();
            assert_eq!(seen.len(), 2);
            assert_eq!(seen[0], (1, 100, 200, 0, vec![5, 9]));
            assert_eq!(seen[1], (1, 100, 201, 0, vec![1, 70000]));
        }
    }
}
