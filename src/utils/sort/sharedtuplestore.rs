//! src/backend/utils/sort/sharedtuplestore.c
//!
//! sharedtuplestore.c
//!   Simple mechanism for sharing tuples between backends.
//!
//! This module contains a shared temporary tuple storage mechanism providing
//! a parallel-aware subset of the features of tuplestore.c.  Multiple backends
//! can write to a SharedTuplestore, and then multiple backends can later scan
//! the stored tuples.  Currently, the only scan type supported is a parallel
//! scan where each backend reads an arbitrary subset of the tuples that were
//! written.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/sort/sharedtuplestore.c

use crate::prelude::*;

use crate::c::{uint32, FLEXIBLE_ARRAY_MEMBER};
use crate::pg_config::BLCKSZ;
use crate::pg_config_manual::{MAXPGPATH, NAMEDATALEN};
use crate::storage::block::BlockNumber;
use std::ffi::{c_char, c_int, c_void};

// ----- header file: src/include/utils/sharedtuplestore.h -----

/*
 * A flag indicating that the tuplestore will only be scanned once, so backing
 * files can be unlinked early.
 */
pub const SHARED_TUPLESTORE_SINGLE_PASS: c_int = 0x01;

// ----- end header -----

/*
 * The size of chunks, in pages.  This is somewhat arbitrarily set to match
 * the size of HASH_CHUNK, so that Parallel Hash obtains new chunks of tuples
 * at approximately the same rate as it allocates new chunks of memory to
 * insert them into.
 */
const STS_CHUNK_PAGES: usize = 4;
const STS_CHUNK_HEADER_SIZE: usize = core::mem::offset_of!(SharedTuplestoreChunk, data);
const STS_CHUNK_DATA_SIZE: usize = STS_CHUNK_PAGES * BLCKSZ as usize - STS_CHUNK_HEADER_SIZE;

/* Chunk written to disk. */
#[repr(C)]
pub struct SharedTuplestoreChunk {
    pub ntuples: c_int,  /* Number of tuples in this chunk. */
    pub overflow: c_int, /* If overflow, how many including this one? */
    pub data: [c_char; FLEXIBLE_ARRAY_MEMBER],
}

/* Per-participant shared state. */
#[repr(C)]
pub struct SharedTuplestoreParticipant {
    pub lock: LWLock,
    pub read_page: BlockNumber, /* Page number for next read. */
    pub npages: BlockNumber,    /* Number of pages written. */
    pub writing: bool,          /* Used only for assertions. */
}

/* The control object that lives in shared memory. */
#[repr(C)]
pub struct SharedTuplestore {
    pub nparticipants: c_int, /* Number of participants that can write. */
    pub flags: c_int,         /* Flag bits from SHARED_TUPLESTORE_XXX */
    pub meta_data_size: Size, /* Size of per-tuple header. */
    pub name: [c_char; NAMEDATALEN], /* A name for this tuplestore. */

    /* Followed by per-participant shared state. */
    pub participants: [SharedTuplestoreParticipant; FLEXIBLE_ARRAY_MEMBER],
}

/* Per-participant state that lives in backend-local memory. */
#[repr(C)]
pub struct SharedTuplestoreAccessor {
    pub participant: c_int,           /* My participant number. */
    pub sts: *mut SharedTuplestore,   /* The shared state. */
    pub fileset: *mut SharedFileSet,  /* The SharedFileSet holding files. */
    pub context: MemoryContext,       /* Memory context for buffers. */

    /* State for reading. */
    pub read_participant: c_int,        /* The current participant to read from. */
    pub read_file: *mut BufFile,        /* The current file to read from. */
    pub read_ntuples_available: c_int,  /* The number of tuples in chunk. */
    pub read_ntuples: c_int,            /* How many tuples have we read from chunk? */
    pub read_bytes: Size,               /* How many bytes have we read from chunk? */
    pub read_buffer: *mut c_char,       /* A buffer for loading tuples. */
    pub read_buffer_size: Size,
    pub read_next_page: BlockNumber,    /* Lowest block we'll consider reading. */

    /* State for writing. */
    pub write_chunk: *mut SharedTuplestoreChunk, /* Buffer for writing. */
    pub write_file: *mut BufFile,                /* The current file to write to. */
    pub write_page: BlockNumber,                 /* The next page to write to. */
    pub write_pointer: *mut c_char,              /* Current write pointer within chunk. */
    pub write_end: *mut c_char,                  /* One past the end of the current chunk. */
}

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char;
    fn memcpy(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
}

/*
 * Return the amount of shared memory required to hold SharedTuplestore for a
 * given number of participants.
 */
pub unsafe fn sts_estimate(participants: c_int) -> Size {
    core::mem::offset_of!(SharedTuplestore, participants)
        + core::mem::size_of::<SharedTuplestoreParticipant>() * participants as usize
}

/*
 * Initialize a SharedTuplestore in existing shared memory.  There must be
 * space for sts_estimate(participants) bytes.  If flags includes the value
 * SHARED_TUPLESTORE_SINGLE_PASS, the files may in future be removed more
 * eagerly (but this isn't yet implemented).
 *
 * Tuples that are stored may optionally carry a piece of fixed sized
 * meta-data which will be retrieved along with the tuple.  This is useful for
 * the hash values used in multi-batch hash joins, but could have other
 * applications.
 *
 * The caller must supply a SharedFileSet, which is essentially a directory
 * that will be cleaned up automatically, and a name which must be unique
 * across all SharedTuplestores created in the same SharedFileSet.
 */
pub unsafe fn sts_initialize(
    sts: *mut SharedTuplestore,
    participants: c_int,
    my_participant_number: c_int,
    meta_data_size: Size,
    flags: c_int,
    fileset: *mut SharedFileSet,
    name: *const c_char,
) -> *mut SharedTuplestoreAccessor {
    let accessor: *mut SharedTuplestoreAccessor;
    let mut i: c_int;

    assert!(my_participant_number < participants);

    (*sts).nparticipants = participants;
    (*sts).meta_data_size = meta_data_size;
    (*sts).flags = flags;

    if strlen(name) > core::mem::size_of_val(&(*sts).name) - 1 {
        elog!(ERROR, "SharedTuplestore name too long");
    }
    strcpy((*sts).name.as_mut_ptr(), name);

    /*
     * Limit meta-data so it + tuple size always fits into a single chunk.
     * sts_puttuple() and sts_read_tuple() could be made to support scenarios
     * where that's not the case, but it's not currently required. If so,
     * meta-data size probably should be made variable, too.
     */
    if meta_data_size + core::mem::size_of::<uint32>() >= STS_CHUNK_DATA_SIZE {
        elog!(ERROR, "meta-data too long");
    }

    i = 0;
    while i < participants {
        let p = sts_participant(sts, i as usize);
        LWLockInitialize(&mut (*p).lock, LWTRANCHE_SHARED_TUPLESTORE);
        (*p).read_page = 0;
        (*p).npages = 0;
        (*p).writing = false;
        i += 1;
    }

    accessor = palloc0(core::mem::size_of::<SharedTuplestoreAccessor>()) as *mut SharedTuplestoreAccessor;
    (*accessor).participant = my_participant_number;
    (*accessor).sts = sts;
    (*accessor).fileset = fileset;
    (*accessor).context = CurrentMemoryContext;

    accessor
}

/*
 * Attach to a SharedTuplestore that has been initialized by another backend,
 * so that this backend can read and write tuples.
 */
pub unsafe fn sts_attach(
    sts: *mut SharedTuplestore,
    my_participant_number: c_int,
    fileset: *mut SharedFileSet,
) -> *mut SharedTuplestoreAccessor {
    let accessor: *mut SharedTuplestoreAccessor;

    assert!(my_participant_number < (*sts).nparticipants);

    accessor = palloc0(core::mem::size_of::<SharedTuplestoreAccessor>()) as *mut SharedTuplestoreAccessor;
    (*accessor).participant = my_participant_number;
    (*accessor).sts = sts;
    (*accessor).fileset = fileset;
    (*accessor).context = CurrentMemoryContext;

    accessor
}

unsafe fn sts_flush_chunk(accessor: *mut SharedTuplestoreAccessor) {
    let size: Size;

    size = STS_CHUNK_PAGES * BLCKSZ as usize;
    BufFileWrite((*accessor).write_file, (*accessor).write_chunk as *mut c_void, size);
    memset((*accessor).write_chunk as *mut c_void, 0, size);
    (*accessor).write_pointer = (*(*accessor).write_chunk).data.as_mut_ptr();
    let p = sts_participant((*accessor).sts, (*accessor).participant as usize);
    (*p).npages += STS_CHUNK_PAGES as BlockNumber;
}

/*
 * Finish writing tuples.  This must be called by all backends that have
 * written data before any backend begins reading it.
 */
pub unsafe fn sts_end_write(accessor: *mut SharedTuplestoreAccessor) {
    if !(*accessor).write_file.is_null() {
        sts_flush_chunk(accessor);
        BufFileClose((*accessor).write_file);
        pfree((*accessor).write_chunk as *mut c_void);
        (*accessor).write_chunk = std::ptr::null_mut();
        (*accessor).write_file = std::ptr::null_mut();
        let p = sts_participant((*accessor).sts, (*accessor).participant as usize);
        (*p).writing = false;
    }
}

/*
 * Prepare to rescan.  Only one participant must call this.  After it returns,
 * all participants may call sts_begin_parallel_scan() and then loop over
 * sts_parallel_scan_next().  This function must not be called concurrently
 * with a scan, and synchronization to avoid that is the caller's
 * responsibility.
 */
pub unsafe fn sts_reinitialize(accessor: *mut SharedTuplestoreAccessor) {
    let mut i: c_int;

    /*
     * Reset the shared read head for all participants' files.  Also set the
     * initial chunk size to the minimum (any increases from that size will be
     * recorded in chunk_expansion_log).
     */
    i = 0;
    while i < (*(*accessor).sts).nparticipants {
        let p = sts_participant((*accessor).sts, i as usize);
        (*p).read_page = 0;
        i += 1;
    }
}

/*
 * Begin scanning the contents in parallel.
 */
pub unsafe fn sts_begin_parallel_scan(accessor: *mut SharedTuplestoreAccessor) {
    /* End any existing scan that was in progress. */
    sts_end_parallel_scan(accessor);

    /*
     * Any backend that might have written into this shared tuplestore must
     * have called sts_end_write(), so that all buffers are flushed and the
     * files have stopped growing.
     */
    let mut i: c_int = 0;
    while i < (*(*accessor).sts).nparticipants {
        let p = sts_participant((*accessor).sts, i as usize);
        assert!(!(*p).writing);
        i += 1;
    }

    /*
     * We will start out reading the file that THIS backend wrote.  There may
     * be some caching locality advantage to that.
     */
    (*accessor).read_participant = (*accessor).participant;
    (*accessor).read_file = std::ptr::null_mut();
    (*accessor).read_next_page = 0;
}

/*
 * Finish a parallel scan, freeing associated backend-local resources.
 */
pub unsafe fn sts_end_parallel_scan(accessor: *mut SharedTuplestoreAccessor) {
    /*
     * Here we could delete all files if SHARED_TUPLESTORE_SINGLE_PASS, but
     * we'd probably need a reference count of current parallel scanners so we
     * could safely do it only when the reference count reaches zero.
     */
    if !(*accessor).read_file.is_null() {
        BufFileClose((*accessor).read_file);
        (*accessor).read_file = std::ptr::null_mut();
    }
}

/*
 * Write a tuple.  If a meta-data size was provided to sts_initialize, then a
 * pointer to meta data of that size must be provided.
 */
pub unsafe fn sts_puttuple(
    accessor: *mut SharedTuplestoreAccessor,
    meta_data: *mut c_void,
    tuple: MinimalTuple,
) {
    let mut size: Size;

    /* Do we have our own file yet? */
    if (*accessor).write_file.is_null() {
        let participant: *mut SharedTuplestoreParticipant;
        let mut name: [c_char; MAXPGPATH] = [0; MAXPGPATH];
        let oldcxt: MemoryContext;

        /* Create one.  Only this backend will write into it. */
        sts_filename(name.as_mut_ptr(), accessor, (*accessor).participant);

        oldcxt = MemoryContextSwitchTo((*accessor).context);
        (*accessor).write_file =
            BufFileCreateFileSet(&mut (*(*accessor).fileset).fs, name.as_ptr());
        MemoryContextSwitchTo(oldcxt);

        /* Set up the shared state for this backend's file. */
        participant = sts_participant((*accessor).sts, (*accessor).participant as usize);
        (*participant).writing = true; /* for assertions only */
    }

    /* Do we have space? */
    size = (*(*accessor).sts).meta_data_size + (*tuple).t_len as Size;
    if (*accessor).write_pointer.is_null()
        || (*accessor).write_pointer.add(size) > (*accessor).write_end
    {
        if (*accessor).write_chunk.is_null() {
            /* First time through.  Allocate chunk. */
            (*accessor).write_chunk = MemoryContextAllocZero(
                (*accessor).context,
                STS_CHUNK_PAGES * BLCKSZ as usize,
            ) as *mut SharedTuplestoreChunk;
            (*(*accessor).write_chunk).ntuples = 0;
            (*accessor).write_pointer = (*(*accessor).write_chunk).data.as_mut_ptr();
            (*accessor).write_end =
                ((*accessor).write_chunk as *mut c_char).add(STS_CHUNK_PAGES * BLCKSZ as usize);
        } else {
            /* See if flushing helps. */
            sts_flush_chunk(accessor);
        }

        /* It may still not be enough in the case of a gigantic tuple. */
        if (*accessor).write_pointer.add(size) > (*accessor).write_end {
            let mut written: Size;

            /*
             * We'll write the beginning of the oversized tuple, and then
             * write the rest in some number of 'overflow' chunks.
             *
             * sts_initialize() verifies that the size of the tuple +
             * meta-data always fits into a chunk. Because the chunk has been
             * flushed above, we can be sure to have all of a chunk's usable
             * space available.
             */
            assert!(
                (*accessor)
                    .write_pointer
                    .add((*(*accessor).sts).meta_data_size + core::mem::size_of::<uint32>())
                    < (*accessor).write_end
            );

            /* Write the meta-data as one chunk. */
            if (*(*accessor).sts).meta_data_size > 0 {
                memcpy(
                    (*accessor).write_pointer as *mut c_void,
                    meta_data,
                    (*(*accessor).sts).meta_data_size,
                );
            }

            /*
             * Write as much of the tuple as we can fit. This includes the
             * tuple's size at the start.
             */
            written = (*accessor).write_end as usize
                - (*accessor).write_pointer as usize
                - (*(*accessor).sts).meta_data_size;
            memcpy(
                (*accessor)
                    .write_pointer
                    .add((*(*accessor).sts).meta_data_size) as *mut c_void,
                tuple as *const c_void,
                written,
            );
            (*(*accessor).write_chunk).ntuples += 1;
            size -= (*(*accessor).sts).meta_data_size;
            size -= written;
            /* Now write as many overflow chunks as we need for the rest. */
            while size > 0 {
                let written_this_chunk: Size;

                sts_flush_chunk(accessor);

                /*
                 * How many overflow chunks to go?  This will allow readers to
                 * skip all of them at once instead of reading each one.
                 */
                (*(*accessor).write_chunk).overflow =
                    ((size + STS_CHUNK_DATA_SIZE - 1) / STS_CHUNK_DATA_SIZE) as c_int;
                written_this_chunk = Min(
                    (*accessor).write_end as usize - (*accessor).write_pointer as usize,
                    size,
                );
                memcpy(
                    (*accessor).write_pointer as *mut c_void,
                    (tuple as *const c_char).add(written) as *const c_void,
                    written_this_chunk,
                );
                (*accessor).write_pointer = (*accessor).write_pointer.add(written_this_chunk);
                size -= written_this_chunk;
                written += written_this_chunk;
            }
            return;
        }
    }

    /* Copy meta-data and tuple into buffer. */
    if (*(*accessor).sts).meta_data_size > 0 {
        memcpy(
            (*accessor).write_pointer as *mut c_void,
            meta_data,
            (*(*accessor).sts).meta_data_size,
        );
    }
    memcpy(
        (*accessor)
            .write_pointer
            .add((*(*accessor).sts).meta_data_size) as *mut c_void,
        tuple as *const c_void,
        (*tuple).t_len as usize,
    );
    (*accessor).write_pointer = (*accessor).write_pointer.add(size);
    (*(*accessor).write_chunk).ntuples += 1;
}

unsafe fn sts_read_tuple(
    accessor: *mut SharedTuplestoreAccessor,
    meta_data: *mut c_void,
) -> MinimalTuple {
    let tuple: MinimalTuple;
    let mut size: uint32 = 0;
    let mut remaining_size: Size;
    let mut this_chunk_size: Size;
    let mut destination: *mut c_char;

    /*
     * We'll keep track of bytes read from this chunk so that we can detect an
     * overflowing tuple and switch to reading overflow pages.
     */
    if (*(*accessor).sts).meta_data_size > 0 {
        BufFileReadExact(
            (*accessor).read_file,
            meta_data,
            (*(*accessor).sts).meta_data_size,
        );
        (*accessor).read_bytes += (*(*accessor).sts).meta_data_size;
    }
    BufFileReadExact(
        (*accessor).read_file,
        &mut size as *mut uint32 as *mut c_void,
        core::mem::size_of_val(&size),
    );
    (*accessor).read_bytes += core::mem::size_of_val(&size);
    if size as Size > (*accessor).read_buffer_size {
        let new_read_buffer_size: Size;

        if !(*accessor).read_buffer.is_null() {
            pfree((*accessor).read_buffer as *mut c_void);
        }
        new_read_buffer_size = Max(size as Size, (*accessor).read_buffer_size * 2);
        (*accessor).read_buffer =
            MemoryContextAlloc((*accessor).context, new_read_buffer_size) as *mut c_char;
        (*accessor).read_buffer_size = new_read_buffer_size;
    }
    remaining_size = size as Size - core::mem::size_of::<uint32>();
    this_chunk_size = Min(
        remaining_size,
        BLCKSZ as usize * STS_CHUNK_PAGES - (*accessor).read_bytes,
    );
    destination = (*accessor).read_buffer.add(core::mem::size_of::<uint32>());
    BufFileReadExact((*accessor).read_file, destination as *mut c_void, this_chunk_size);
    (*accessor).read_bytes += this_chunk_size;
    remaining_size -= this_chunk_size;
    destination = destination.add(this_chunk_size);
    (*accessor).read_ntuples += 1;

    /* Check if we need to read any overflow chunks. */
    while remaining_size > 0 {
        /* We are now positioned at the start of an overflow chunk. */
        let mut chunk_header: SharedTuplestoreChunk = core::mem::zeroed();

        BufFileReadExact(
            (*accessor).read_file,
            &mut chunk_header as *mut SharedTuplestoreChunk as *mut c_void,
            STS_CHUNK_HEADER_SIZE,
        );
        (*accessor).read_bytes = STS_CHUNK_HEADER_SIZE;
        if chunk_header.overflow == 0 {
            elog!(
                ERROR,
                "unexpected chunk in shared tuplestore temporary file"
            );
        }
        (*accessor).read_next_page += STS_CHUNK_PAGES as BlockNumber;
        this_chunk_size = Min(
            remaining_size,
            BLCKSZ as usize * STS_CHUNK_PAGES - STS_CHUNK_HEADER_SIZE,
        );
        BufFileReadExact((*accessor).read_file, destination as *mut c_void, this_chunk_size);
        (*accessor).read_bytes += this_chunk_size;
        remaining_size -= this_chunk_size;
        destination = destination.add(this_chunk_size);

        /*
         * These will be used to count regular tuples following the oversized
         * tuple that spilled into this overflow chunk.
         */
        (*accessor).read_ntuples = 0;
        (*accessor).read_ntuples_available = chunk_header.ntuples;
    }

    tuple = (*accessor).read_buffer as MinimalTuple;
    (*tuple).t_len = size;

    tuple
}

/*
 * Get the next tuple in the current parallel scan.
 */
pub unsafe fn sts_parallel_scan_next(
    accessor: *mut SharedTuplestoreAccessor,
    meta_data: *mut c_void,
) -> MinimalTuple {
    let mut p: *mut SharedTuplestoreParticipant;
    let mut read_page: BlockNumber = 0;
    let mut eof: bool;

    loop {
        /* Can we read more tuples from the current chunk? */
        if (*accessor).read_ntuples < (*accessor).read_ntuples_available {
            return sts_read_tuple(accessor, meta_data);
        }

        /* Find the location of a new chunk to read. */
        p = sts_participant((*accessor).sts, (*accessor).read_participant as usize);

        LWLockAcquire(&mut (*p).lock, LW_EXCLUSIVE);
        /* We can skip directly past overflow pages we know about. */
        if (*p).read_page < (*accessor).read_next_page {
            (*p).read_page = (*accessor).read_next_page;
        }
        eof = (*p).read_page >= (*p).npages;
        if !eof {
            /* Claim the next chunk. */
            read_page = (*p).read_page;
            /* Advance the read head for the next reader. */
            (*p).read_page += STS_CHUNK_PAGES as BlockNumber;
            (*accessor).read_next_page = (*p).read_page;
        }
        LWLockRelease(&mut (*p).lock);

        if !eof {
            let mut chunk_header: SharedTuplestoreChunk = core::mem::zeroed();

            /* Make sure we have the file open. */
            if (*accessor).read_file.is_null() {
                let mut name: [c_char; MAXPGPATH] = [0; MAXPGPATH];
                let oldcxt: MemoryContext;

                sts_filename(name.as_mut_ptr(), accessor, (*accessor).read_participant);

                oldcxt = MemoryContextSwitchTo((*accessor).context);
                (*accessor).read_file = BufFileOpenFileSet(
                    &mut (*(*accessor).fileset).fs,
                    name.as_ptr(),
                    O_RDONLY,
                    false,
                );
                MemoryContextSwitchTo(oldcxt);
            }

            /* Seek and load the chunk header. */
            if BufFileSeekBlock((*accessor).read_file, read_page) != 0 {
                elog!(
                    ERROR,
                    "could not seek to block {} in shared tuplestore temporary file",
                    read_page
                );
            }
            BufFileReadExact(
                (*accessor).read_file,
                &mut chunk_header as *mut SharedTuplestoreChunk as *mut c_void,
                STS_CHUNK_HEADER_SIZE,
            );

            /*
             * If this is an overflow chunk, we skip it and any following
             * overflow chunks all at once.
             */
            if chunk_header.overflow > 0 {
                (*accessor).read_next_page =
                    read_page + chunk_header.overflow as BlockNumber * STS_CHUNK_PAGES as BlockNumber;
                continue;
            }

            (*accessor).read_ntuples = 0;
            (*accessor).read_ntuples_available = chunk_header.ntuples;
            (*accessor).read_bytes = STS_CHUNK_HEADER_SIZE;

            /* Go around again, so we can get a tuple from this chunk. */
        } else {
            if !(*accessor).read_file.is_null() {
                BufFileClose((*accessor).read_file);
                (*accessor).read_file = std::ptr::null_mut();
            }

            /*
             * Try the next participant's file.  If we've gone full circle,
             * we're done.
             */
            (*accessor).read_participant =
                ((*accessor).read_participant + 1) % (*(*accessor).sts).nparticipants;
            if (*accessor).read_participant == (*accessor).participant {
                break;
            }
            (*accessor).read_next_page = 0;

            /* Go around again, so we can get a chunk from this file. */
        }
    }

    std::ptr::null_mut()
}

/*
 * Create the name used for the BufFile that a given participant will write.
 */
unsafe fn sts_filename(
    name: *mut c_char,
    accessor: *mut SharedTuplestoreAccessor,
    participant: c_int,
) {
    snprintf(
        name,
        MAXPGPATH,
        c"%s.p%d".as_ptr(),
        (*(*accessor).sts).name.as_ptr(),
        participant,
    );
}

/*
 * Helper to obtain a pointer to the i'th participant in the flexible array
 * member of SharedTuplestore.
 */
#[inline]
unsafe fn sts_participant(
    sts: *mut SharedTuplestore,
    i: usize,
) -> *mut SharedTuplestoreParticipant {
    let base = (sts as *mut c_char).add(core::mem::offset_of!(SharedTuplestore, participants))
        as *mut SharedTuplestoreParticipant;
    base.add(i)
}

// ----- Stubs for unported dependencies -----

pub type MinimalTuple = *mut MinimalTupleData;

#[repr(C)]
pub struct MinimalTupleData {
    pub t_len: uint32,
}

#[repr(C)]
pub struct LWLock {
    _private: [u8; 0],
}

#[repr(C)]
pub struct BufFile {
    _private: [u8; 0],
}

#[repr(C)]
pub struct SharedFileSet {
    pub fs: FileSet,
}

#[repr(C)]
pub struct FileSet {
    _private: [u8; 0],
}

pub const LWTRANCHE_SHARED_TUPLESTORE: c_int = 0;
pub const LW_EXCLUSIVE: c_int = 0;
pub const O_RDONLY: c_int = 0;

#[inline]
unsafe fn Min(a: Size, b: Size) -> Size {
    if a < b {
        a
    } else {
        b
    }
}

#[inline]
unsafe fn Max(a: Size, b: Size) -> Size {
    if a > b {
        a
    } else {
        b
    }
}

unsafe fn LWLockInitialize(_lock: *mut LWLock, _tranche_id: c_int) {
    crate::storage::lmgr::lwlock::LWLockInitialize(_lock as _, _tranche_id)
}

unsafe fn LWLockAcquire(_lock: *mut LWLock, _mode: c_int) -> bool {
    crate::storage::lmgr::lwlock::LWLockAcquire(_lock as _, if _mode == 1 { crate::storage::lmgr::lwlock::LWLockMode::LW_SHARED } else { crate::storage::lmgr::lwlock::LWLockMode::LW_EXCLUSIVE })
}

unsafe fn LWLockRelease(_lock: *mut LWLock) {
    crate::storage::lmgr::lwlock::LWLockRelease(_lock as _)
}

unsafe fn BufFileWrite(_file: *mut BufFile, _ptr: *mut c_void, _size: Size) {
    unimplemented!() // TODO: storage/buffile.c
}

unsafe fn BufFileClose(_file: *mut BufFile) {
    unimplemented!() // TODO: storage/buffile.c
}

unsafe fn BufFileReadExact(_file: *mut BufFile, _ptr: *mut c_void, _size: Size) {
    unimplemented!() // TODO: storage/buffile.c
}

unsafe fn BufFileCreateFileSet(_fileset: *mut FileSet, _name: *const c_char) -> *mut BufFile {
    unimplemented!() // TODO: storage/buffile.c
}

unsafe fn BufFileOpenFileSet(
    _fileset: *mut FileSet,
    _name: *const c_char,
    _mode: c_int,
    _missing_ok: bool,
) -> *mut BufFile {
    unimplemented!() // TODO: storage/buffile.c
}

unsafe fn BufFileSeekBlock(_file: *mut BufFile, _blknum: BlockNumber) -> c_int {
    unimplemented!() // TODO: storage/buffile.c
}
