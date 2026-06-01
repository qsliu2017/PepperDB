//! sync.rs
//!   File synchronization management code.
//! Translated 1:1 from postgres/src/backend/storage/sync/sync.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/storage/sync/sync.c

use crate::prelude::*;

use crate::nodes::pg_list::{
    lappend, lfirst, list_cell_number, list_delete_first_n, list_free_deep, list_nth, List,
    ListCell, NIL,
};
use crate::portability::instr_time::{
    instr_time, INSTR_TIME_GET_MICROSEC, INSTR_TIME_SET_CURRENT, INSTR_TIME_SUBTRACT,
};
use crate::storage::relfilelocator::RelFileLocator;
use crate::utils::hash::dynahash::{
    hash_create, hash_search, hash_seq_init, hash_seq_search, HASHACTION, HASHCTL, HASH_BLOBS,
    HASH_CONTEXT, HASH_ELEM, HASH_SEQ_STATUS, HTAB,
};
use crate::{current_cell, foreach, AllocSetContextCreate};

/* postgres.h pulls in miscadmin.h-equivalent globals below */
use crate::miscadmin::{AmCheckpointerProcess, IsUnderPostmaster};

/*
 * ---------------------------------------------------------------------------
 * Declarations merged from src/include/storage/sync.h
 * ---------------------------------------------------------------------------
 */

/*
 * Type of sync request.  These are used to manage the set of pending
 * requests to call a sync handler's sync or unlink functions at the next
 * checkpoint.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SyncRequestType {
    SYNC_REQUEST,        /* schedule a call of sync function */
    SYNC_UNLINK_REQUEST, /* schedule a call of unlink function */
    SYNC_FORGET_REQUEST, /* forget all calls for a tag */
    SYNC_FILTER_REQUEST, /* forget all calls satisfying match fn */
}
pub use SyncRequestType::*;

/*
 * Which set of functions to use to handle a given request.  The values of
 * the enumerators must match the indexes of the function table in sync.c.
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SyncRequestHandler {
    SYNC_HANDLER_MD = 0,
    SYNC_HANDLER_CLOG,
    SYNC_HANDLER_COMMIT_TS,
    SYNC_HANDLER_MULTIXACT_OFFSET,
    SYNC_HANDLER_MULTIXACT_MEMBER,
    SYNC_HANDLER_NONE,
}
pub use SyncRequestHandler::*;

/* Integer values of the SyncRequestHandler enum, used as table indexes. */
pub const SYNC_HANDLER_MD: c_int = SyncRequestHandler::SYNC_HANDLER_MD as c_int;
pub const SYNC_HANDLER_CLOG: c_int = SyncRequestHandler::SYNC_HANDLER_CLOG as c_int;
pub const SYNC_HANDLER_COMMIT_TS: c_int = SyncRequestHandler::SYNC_HANDLER_COMMIT_TS as c_int;
pub const SYNC_HANDLER_MULTIXACT_OFFSET: c_int =
    SyncRequestHandler::SYNC_HANDLER_MULTIXACT_OFFSET as c_int;
pub const SYNC_HANDLER_MULTIXACT_MEMBER: c_int =
    SyncRequestHandler::SYNC_HANDLER_MULTIXACT_MEMBER as c_int;
pub const SYNC_HANDLER_NONE: c_int = SyncRequestHandler::SYNC_HANDLER_NONE as c_int;

/*
 * A tag identifying a file.  Currently it has the members required for md.c's
 * usage, but sync.c has no knowledge of the internal structure, and it is
 * liable to change as required by future handlers.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FileTag {
    pub handler: int16, /* SyncRequestHandler value, saving space */
    pub forknum: int16, /* ForkNumber, saving space */
    pub rlocator: RelFileLocator,
    pub segno: uint64,
}

/*
 * ---------------------------------------------------------------------------
 * sync.c body
 * ---------------------------------------------------------------------------
 */

/*
 * In some contexts (currently, standalone backends and the checkpointer)
 * we keep track of pending fsync operations: we need to remember all relation
 * segments that have been written since the last checkpoint, so that we can
 * fsync them down to disk before completing the next checkpoint.  This hash
 * table remembers the pending operations.  We use a hash table mostly as
 * a convenient way of merging duplicate requests.
 *
 * We use a similar mechanism to remember no-longer-needed files that can
 * be deleted after the next checkpoint, but we use a linked list instead of
 * a hash table, because we don't expect there to be any duplicate requests.
 *
 * These mechanisms are only used for non-temp relations; we never fsync
 * temp rels, nor do we need to postpone their deletion (see comments in
 * mdunlink).
 *
 * (Regular backends do not track pending operations locally, but forward
 * them to the checkpointer.)
 */
type CycleCtr = uint16; /* can be any convenient integer size */

#[repr(C)]
struct PendingFsyncEntry {
    tag: FileTag,        /* identifies handler and file */
    cycle_ctr: CycleCtr, /* sync_cycle_ctr of oldest request */
    canceled: bool,      /* canceled is true if we canceled "recently" */
}

#[repr(C)]
struct PendingUnlinkEntry {
    tag: FileTag,        /* identifies handler and file */
    cycle_ctr: CycleCtr, /* checkpoint_cycle_ctr when request was made */
    canceled: bool,      /* true if request has been canceled */
}

static mut pendingOps: *mut HTAB = std::ptr::null_mut();
static mut pendingUnlinks: *mut List = NIL;
static mut pendingOpsCxt: MemoryContext = std::ptr::null_mut(); /* context for the above  */

static mut sync_cycle_ctr: CycleCtr = 0;
static mut checkpoint_cycle_ctr: CycleCtr = 0;

/* Intervals for calling AbsorbSyncRequests */
const FSYNCS_PER_ABSORB: c_int = 10;
const UNLINKS_PER_ABSORB: c_int = 10;

/*
 * Function pointers for handling sync and unlink requests.
 */
#[repr(C)]
struct SyncOps {
    sync_syncfiletag: Option<unsafe fn(ftag: *const FileTag, path: *mut c_char) -> c_int>,
    sync_unlinkfiletag: Option<unsafe fn(ftag: *const FileTag, path: *mut c_char) -> c_int>,
    sync_filetagmatches: Option<unsafe fn(ftag: *const FileTag, candidate: *const FileTag) -> bool>,
}

/*
 * These indexes must correspond to the values of the SyncRequestHandler enum.
 */
static syncsw: [SyncOps; 5] = [
    /* magnetic disk */
    /* [SYNC_HANDLER_MD] */
    SyncOps {
        sync_syncfiletag: Some(mdsyncfiletag),
        sync_unlinkfiletag: Some(mdunlinkfiletag),
        sync_filetagmatches: Some(mdfiletagmatches),
    },
    /* pg_xact */
    /* [SYNC_HANDLER_CLOG] */
    SyncOps {
        sync_syncfiletag: Some(clogsyncfiletag),
        sync_unlinkfiletag: None,
        sync_filetagmatches: None,
    },
    /* pg_commit_ts */
    /* [SYNC_HANDLER_COMMIT_TS] */
    SyncOps {
        sync_syncfiletag: Some(committssyncfiletag),
        sync_unlinkfiletag: None,
        sync_filetagmatches: None,
    },
    /* pg_multixact/offsets */
    /* [SYNC_HANDLER_MULTIXACT_OFFSET] */
    SyncOps {
        sync_syncfiletag: Some(multixactoffsetssyncfiletag),
        sync_unlinkfiletag: None,
        sync_filetagmatches: None,
    },
    /* pg_multixact/members */
    /* [SYNC_HANDLER_MULTIXACT_MEMBER] */
    SyncOps {
        sync_syncfiletag: Some(multixactmemberssyncfiletag),
        sync_unlinkfiletag: None,
        sync_filetagmatches: None,
    },
];

/*
 * Initialize data structures for the file sync tracking.
 */
pub unsafe fn InitSync() {
    /*
     * Create pending-operations hashtable if we need it.  Currently, we need
     * it if we are standalone (not under a postmaster) or if we are a
     * checkpointer auxiliary process.
     */
    if !IsUnderPostmaster || AmCheckpointerProcess() {
        let mut hash_ctl: HASHCTL = std::mem::zeroed();

        /*
         * XXX: The checkpointer needs to add entries to the pending ops table
         * when absorbing fsync requests.  That is done within a critical
         * section, which isn't usually allowed, but we make an exception. It
         * means that there's a theoretical possibility that you run out of
         * memory while absorbing fsync requests, which leads to a PANIC.
         * Fortunately the hash table is small so that's unlikely to happen in
         * practice.
         */
        pendingOpsCxt = AllocSetContextCreate!(
            TopMemoryContext,
            c"Pending ops context".as_ptr(),
            ALLOCSET_DEFAULT_SIZES
        );
        MemoryContextAllowInCriticalSection(pendingOpsCxt as crate::utils::mmgr::memnodes::MemoryContext, true);

        hash_ctl.keysize = std::mem::size_of::<FileTag>();
        hash_ctl.entrysize = std::mem::size_of::<PendingFsyncEntry>();
        hash_ctl.hcxt = pendingOpsCxt as crate::utils::palloc::MemoryContext;
        pendingOps = hash_create(
            c"Pending Ops Table".as_ptr(),
            100,
            &hash_ctl,
            HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
        );
        pendingUnlinks = NIL;
    }
}

/*
 * SyncPreCheckpoint() -- Do pre-checkpoint work
 *
 * To distinguish unlink requests that arrived before this checkpoint
 * started from those that arrived during the checkpoint, we use a cycle
 * counter similar to the one we use for fsync requests. That cycle
 * counter is incremented here.
 *
 * This must be called *before* the checkpoint REDO point is determined.
 * That ensures that we won't delete files too soon.  Since this calls
 * AbsorbSyncRequests(), which performs memory allocations, it cannot be
 * called within a critical section.
 *
 * Note that we can't do anything here that depends on the assumption
 * that the checkpoint will be completed.
 */
pub unsafe fn SyncPreCheckpoint() {
    /*
     * Operations such as DROP TABLESPACE assume that the next checkpoint will
     * process all recently forwarded unlink requests, but if they aren't
     * absorbed prior to advancing the cycle counter, they won't be processed
     * until a future checkpoint.  The following absorb ensures that any
     * unlink requests forwarded before the checkpoint began will be processed
     * in the current checkpoint.
     */
    AbsorbSyncRequests();

    /*
     * Any unlink requests arriving after this point will be assigned the next
     * cycle counter, and won't be unlinked until next checkpoint.
     */
    checkpoint_cycle_ctr += 1;
}

/*
 * SyncPostCheckpoint() -- Do post-checkpoint work
 *
 * Remove any lingering files that can now be safely removed.
 */
pub unsafe fn SyncPostCheckpoint() {
    let mut absorb_counter: c_int;
    /*
     * The C source uses a `ListCell *lc` that is NULL iff the foreach loop ran
     * to completion.  The Rust `foreach!` macro iterates a `ForEachState` and
     * does not expose that cell, so we run the loop manually and capture `lc`.
     */
    let mut lc: *mut ListCell = std::ptr::null_mut();

    absorb_counter = UNLINKS_PER_ABSORB;
    {
        let mut __i: c_int = 0;
        loop {
            if !(!pendingUnlinks.is_null() && __i < (*pendingUnlinks).length) {
                /* reached the end of the list */
                lc = std::ptr::null_mut();
                break;
            }
            lc = (*pendingUnlinks).elements.add(__i as usize);

            let entry: *mut PendingUnlinkEntry = lfirst(lc) as *mut PendingUnlinkEntry;
            let mut path: [c_char; MAXPGPATH as usize] = [0; MAXPGPATH as usize];

            /* Skip over any canceled entries */
            if (*entry).canceled {
                __i += 1;
                continue;
            }

            /*
             * New entries are appended to the end, so if the entry is new we've
             * reached the end of old entries.
             *
             * Note: if just the right number of consecutive checkpoints fail, we
             * could be fooled here by cycle_ctr wraparound.  However, the only
             * consequence is that we'd delay unlinking for one more checkpoint,
             * which is perfectly tolerable.
             */
            if (*entry).cycle_ctr == checkpoint_cycle_ctr {
                break;
            }

            /* Unlink the file */
            if (syncsw[(*entry).tag.handler as usize]
                .sync_unlinkfiletag
                .unwrap())(&(*entry).tag, path.as_mut_ptr())
                < 0
            {
                /*
                 * There's a race condition, when the database is dropped at the
                 * same time that we process the pending unlink requests. If the
                 * DROP DATABASE deletes the file before we do, we will get ENOENT
                 * here. rmtree() also has to ignore ENOENT errors, to deal with
                 * the possibility that we delete the file first.
                 */
                if get_errno() != ENOENT {
                    ereport!(
                        WARNING,
                        errmsg!(
                            "could not remove file \"{}\": %m",
                            std::ffi::CStr::from_ptr(path.as_ptr()).to_string_lossy()
                        )
                    );
                }
            }

            /* Mark the list entry as canceled, just in case */
            (*entry).canceled = true;

            /*
             * As in ProcessSyncRequests, we don't want to stop absorbing fsync
             * requests for a long time when there are many deletions to be done.
             * We can safely call AbsorbSyncRequests() at this point in the loop.
             */
            absorb_counter -= 1;
            if absorb_counter <= 0 {
                AbsorbSyncRequests();
                absorb_counter = UNLINKS_PER_ABSORB;
            }

            __i += 1;
        }
    }

    /*
     * If we reached the end of the list, we can just remove the whole list
     * (remembering to pfree all the PendingUnlinkEntry objects).  Otherwise,
     * we must keep the entries at or after "lc".
     */
    if lc.is_null() {
        list_free_deep(pendingUnlinks);
        pendingUnlinks = NIL;
    } else {
        let ntodelete: c_int = list_cell_number(pendingUnlinks, lc);

        let mut i: c_int = 0;
        while i < ntodelete {
            pfree(list_nth(pendingUnlinks, i));
            i += 1;
        }

        pendingUnlinks = list_delete_first_n(pendingUnlinks, ntodelete);
    }
}

/*
 *	ProcessSyncRequests() -- Process queued fsync requests.
 */
pub unsafe fn ProcessSyncRequests() {
    static mut sync_in_progress: bool = false;

    let mut hstat: HASH_SEQ_STATUS = std::mem::zeroed();
    let mut entry: *mut PendingFsyncEntry;
    let mut absorb_counter: c_int;

    /* Statistics on sync times */
    let mut processed: c_int = 0;
    let mut sync_start: instr_time = unsafe { core::mem::zeroed() };
    let mut sync_end: instr_time = unsafe { core::mem::zeroed() };
    let mut sync_diff: instr_time = unsafe { core::mem::zeroed() };
    let mut elapsed: uint64;
    let mut longest: uint64 = 0;
    let mut total_elapsed: uint64 = 0;

    /*
     * This is only called during checkpoints, and checkpoints should only
     * occur in processes that have created a pendingOps.
     */
    if pendingOps.is_null() {
        elog!(ERROR, "cannot sync without a pendingOps table");
    }

    /*
     * If we are in the checkpointer, the sync had better include all fsync
     * requests that were queued by backends up to this point.  The tightest
     * race condition that could occur is that a buffer that must be written
     * and fsync'd for the checkpoint could have been dumped by a backend just
     * before it was visited by BufferSync().  We know the backend will have
     * queued an fsync request before clearing the buffer's dirtybit, so we
     * are safe as long as we do an Absorb after completing BufferSync().
     */
    AbsorbSyncRequests();

    /*
     * To avoid excess fsync'ing (in the worst case, maybe a never-terminating
     * checkpoint), we want to ignore fsync requests that are entered into the
     * hashtable after this point --- they should be processed next time,
     * instead.  We use sync_cycle_ctr to tell old entries apart from new
     * ones: new ones will have cycle_ctr equal to the incremented value of
     * sync_cycle_ctr.
     *
     * In normal circumstances, all entries present in the table at this point
     * will have cycle_ctr exactly equal to the current (about to be old)
     * value of sync_cycle_ctr.  However, if we fail partway through the
     * fsync'ing loop, then older values of cycle_ctr might remain when we
     * come back here to try again.  Repeated checkpoint failures would
     * eventually wrap the counter around to the point where an old entry
     * might appear new, causing us to skip it, possibly allowing a checkpoint
     * to succeed that should not have.  To forestall wraparound, any time the
     * previous ProcessSyncRequests() failed to complete, run through the
     * table and forcibly set cycle_ctr = sync_cycle_ctr.
     *
     * Think not to merge this loop with the main loop, as the problem is
     * exactly that that loop may fail before having visited all the entries.
     * From a performance point of view it doesn't matter anyway, as this path
     * will never be taken in a system that's functioning normally.
     */
    if sync_in_progress {
        /* prior try failed, so update any stale cycle_ctr values */
        hash_seq_init(&raw mut hstat, pendingOps);
        loop {
            entry = hash_seq_search(&raw mut hstat) as *mut PendingFsyncEntry;
            if entry.is_null() {
                break;
            }
            (*entry).cycle_ctr = sync_cycle_ctr;
        }
    }

    /* Advance counter so that new hashtable entries are distinguishable */
    sync_cycle_ctr += 1;

    /* Set flag to detect failure if we don't reach the end of the loop */
    sync_in_progress = true;

    /* Now scan the hashtable for fsync requests to process */
    absorb_counter = FSYNCS_PER_ABSORB;
    hash_seq_init(&raw mut hstat, pendingOps);
    loop {
        entry = hash_seq_search(&raw mut hstat) as *mut PendingFsyncEntry;
        if entry.is_null() {
            break;
        }

        let mut failures: c_int;

        /*
         * If the entry is new then don't process it this time; it is new.
         * Note "continue" bypasses the hash-remove call at the bottom of the
         * loop.
         */
        if (*entry).cycle_ctr == sync_cycle_ctr {
            continue;
        }

        /* Else assert we haven't missed it */
        Assert!(((*entry).cycle_ctr.wrapping_add(1)) as CycleCtr == sync_cycle_ctr);

        /*
         * If fsync is off then we don't have to bother opening the file at
         * all.  (We delay checking until this point so that changing fsync on
         * the fly behaves sensibly.)
         */
        if enableFsync {
            /*
             * If in checkpointer, we want to absorb pending requests every so
             * often to prevent overflow of the fsync request queue.  It is
             * unspecified whether newly-added entries will be visited by
             * hash_seq_search, but we don't care since we don't need to
             * process them anyway.
             */
            absorb_counter -= 1;
            if absorb_counter <= 0 {
                AbsorbSyncRequests();
                absorb_counter = FSYNCS_PER_ABSORB;
            }

            /*
             * The fsync table could contain requests to fsync segments that
             * have been deleted (unlinked) by the time we get to them. Rather
             * than just hoping an ENOENT (or EACCES on Windows) error can be
             * ignored, what we do on error is absorb pending requests and
             * then retry. Since mdunlink() queues a "cancel" message before
             * actually unlinking, the fsync request is guaranteed to be
             * marked canceled after the absorb if it really was this case.
             * DROP DATABASE likewise has to tell us to forget fsync requests
             * before it starts deletions.
             */
            failures = 0;
            while !(*entry).canceled {
                let mut path: [c_char; MAXPGPATH as usize] = [0; MAXPGPATH as usize];

                INSTR_TIME_SET_CURRENT(&mut sync_start);
                if (syncsw[(*entry).tag.handler as usize]
                    .sync_syncfiletag
                    .unwrap())(&(*entry).tag, path.as_mut_ptr())
                    == 0
                {
                    /* Success; update statistics about sync timing */
                    INSTR_TIME_SET_CURRENT(&mut sync_end);
                    sync_diff = sync_end;
                    INSTR_TIME_SUBTRACT(&mut sync_diff, sync_start);
                    elapsed = INSTR_TIME_GET_MICROSEC(sync_diff) as uint64;
                    if elapsed > longest {
                        longest = elapsed;
                    }
                    total_elapsed += elapsed;
                    processed += 1;

                    if log_checkpoints {
                        elog!(
                            DEBUG1,
                            "checkpoint sync: number={} file={} time={:.3} ms",
                            processed,
                            std::ffi::CStr::from_ptr(path.as_ptr()).to_string_lossy(),
                            elapsed as f64 / 1000.0
                        );
                    }

                    break; /* out of retry loop */
                }

                /*
                 * It is possible that the relation has been dropped or
                 * truncated since the fsync request was entered. Therefore,
                 * allow ENOENT, but only if we didn't fail already on this
                 * file.
                 */
                if !FILE_POSSIBLY_DELETED(get_errno()) || failures > 0 {
                    ereport!(
                        data_sync_elevel(ERROR),
                        errmsg!(
                            "could not fsync file \"{}\": %m",
                            std::ffi::CStr::from_ptr(path.as_ptr()).to_string_lossy()
                        )
                    );
                } else {
                    ereport!(
                        DEBUG1,
                        errmsg!(
                            "could not fsync file \"{}\" but retrying: %m",
                            std::ffi::CStr::from_ptr(path.as_ptr()).to_string_lossy()
                        )
                    );
                }

                /*
                 * Absorb incoming requests and check to see if a cancel
                 * arrived for this relation fork.
                 */
                AbsorbSyncRequests();
                absorb_counter = FSYNCS_PER_ABSORB; /* might as well... */

                failures += 1;
            } /* end retry loop */
        }

        /* We are done with this entry, remove it */
        if hash_search(
            pendingOps,
            &raw const (*entry).tag as *const c_void,
            HASHACTION::HASH_REMOVE,
            std::ptr::null_mut(),
        )
        .is_null()
        {
            elog!(ERROR, "pendingOps corrupted");
        }
    } /* end loop over hashtable entries */

    /* Return sync performance metrics for report at checkpoint end */
    CheckpointStats.ckpt_sync_rels = processed;
    CheckpointStats.ckpt_longest_sync = longest;
    CheckpointStats.ckpt_agg_sync_time = total_elapsed;

    /* Flag successful completion of ProcessSyncRequests */
    sync_in_progress = false;
}

/*
 * RememberSyncRequest() -- callback from checkpointer side of sync request
 *
 * We stuff fsync requests into the local hash table for execution
 * during the checkpointer's next checkpoint.  UNLINK requests go into a
 * separate linked list, however, because they get processed separately.
 *
 * See sync.h for more information on the types of sync requests supported.
 */
pub unsafe fn RememberSyncRequest(ftag: *const FileTag, type_: SyncRequestType) {
    Assert!(!pendingOps.is_null());

    if type_ == SYNC_FORGET_REQUEST {
        let entry: *mut PendingFsyncEntry;

        /* Cancel previously entered request */
        entry = hash_search(
            pendingOps,
            ftag as *const c_void,
            HASHACTION::HASH_FIND,
            std::ptr::null_mut(),
        ) as *mut PendingFsyncEntry;
        if !entry.is_null() {
            (*entry).canceled = true;
        }
    } else if type_ == SYNC_FILTER_REQUEST {
        let mut hstat: HASH_SEQ_STATUS = std::mem::zeroed();
        let mut pfe: *mut PendingFsyncEntry;

        /* Cancel matching fsync requests */
        hash_seq_init(&raw mut hstat, pendingOps);
        loop {
            pfe = hash_seq_search(&raw mut hstat) as *mut PendingFsyncEntry;
            if pfe.is_null() {
                break;
            }
            if (*pfe).tag.handler == (*ftag).handler
                && (syncsw[(*ftag).handler as usize]
                    .sync_filetagmatches
                    .unwrap())(ftag, &(*pfe).tag)
            {
                (*pfe).canceled = true;
            }
        }

        /* Cancel matching unlink requests */
        foreach!(cell, pendingUnlinks, {
            let pue: *mut PendingUnlinkEntry =
                lfirst(current_cell!(cell)) as *mut PendingUnlinkEntry;

            if (*pue).tag.handler == (*ftag).handler
                && (syncsw[(*ftag).handler as usize]
                    .sync_filetagmatches
                    .unwrap())(ftag, &(*pue).tag)
            {
                (*pue).canceled = true;
            }
        });
    } else if type_ == SYNC_UNLINK_REQUEST {
        /* Unlink request: put it in the linked list */
        let oldcxt: MemoryContext = MemoryContextSwitchTo(pendingOpsCxt);
        let entry: *mut PendingUnlinkEntry;

        entry = palloc(std::mem::size_of::<PendingUnlinkEntry>()) as *mut PendingUnlinkEntry;
        (*entry).tag = *ftag;
        (*entry).cycle_ctr = checkpoint_cycle_ctr;
        (*entry).canceled = false;

        pendingUnlinks = lappend(pendingUnlinks, entry as *mut c_void);

        MemoryContextSwitchTo(oldcxt);
    } else {
        /* Normal case: enter a request to fsync this segment */
        let oldcxt: MemoryContext = MemoryContextSwitchTo(pendingOpsCxt);
        let entry: *mut PendingFsyncEntry;
        let mut found: bool = false;

        Assert!(type_ == SYNC_REQUEST);

        entry = hash_search(
            pendingOps,
            ftag as *const c_void,
            HASHACTION::HASH_ENTER,
            &raw mut found,
        ) as *mut PendingFsyncEntry;
        /* if new entry, or was previously canceled, initialize it */
        if !found || (*entry).canceled {
            (*entry).cycle_ctr = sync_cycle_ctr;
            (*entry).canceled = false;
        }

        /*
         * NB: it's intentional that we don't change cycle_ctr if the entry
         * already exists.  The cycle_ctr must represent the oldest fsync
         * request that could be in the entry.
         */

        MemoryContextSwitchTo(oldcxt);
    }
}

/*
 * Register the sync request locally, or forward it to the checkpointer.
 *
 * If retryOnError is true, we'll keep trying if there is no space in the
 * queue.  Return true if we succeeded, or false if there wasn't space.
 */
pub unsafe fn RegisterSyncRequest(
    ftag: *const FileTag,
    type_: SyncRequestType,
    retryOnError: bool,
) -> bool {
    let mut ret: bool;

    if !pendingOps.is_null() {
        /* standalone backend or startup process: fsync state is local */
        RememberSyncRequest(ftag, type_);
        return true;
    }

    loop {
        /*
         * Notify the checkpointer about it.  If we fail to queue a message in
         * retryOnError mode, we have to sleep and try again ... ugly, but
         * hopefully won't happen often.
         *
         * XXX should we CHECK_FOR_INTERRUPTS in this loop?  Escaping with an
         * error in the case of SYNC_UNLINK_REQUEST would leave the
         * no-longer-used file still present on disk, which would be bad, so
         * I'm inclined to assume that the checkpointer will always empty the
         * queue soon.
         */
        ret = ForwardSyncRequest(ftag, type_);

        /*
         * If we are successful in queueing the request, or we failed and were
         * instructed not to retry on error, break.
         */
        if ret || (!ret && !retryOnError) {
            break;
        }

        WaitLatch(
            std::ptr::null_mut(),
            WL_EXIT_ON_PM_DEATH | WL_TIMEOUT,
            10,
            WAIT_EVENT_REGISTER_SYNC_REQUEST,
        );
    }

    ret
}

/*
 * ---------------------------------------------------------------------------
 * Port-port stubs for symbols that do not yet have a home in the Rust tree.
 * ---------------------------------------------------------------------------
 */

// errno helpers (darwin: errno lives behind __error()).
extern "C" {
    fn __error() -> *mut c_int;
}
#[inline]
unsafe fn get_errno() -> c_int {
    *__error()
}

// from errno.h
const ENOENT: c_int = 2;

// TODO(pg-port): real MAXPGPATH lives in pg_config_manual.h.
const MAXPGPATH: c_int = 1024;

// TODO(pg-port): real enableFsync lives in utils/init/globals.rs (re-exported
// via miscadmin.h); referenced here as the bootstrap global.
use crate::utils::init::globals::enableFsync;

// TODO(pg-port): real log_checkpoints GUC lives in access/xlog.c.
static mut log_checkpoints: bool = false;

// TODO(pg-port): real WaitLatch / wait-event constants live in storage/ipc/latch.rs.
use crate::storage::ipc::latch::{WaitLatch, WL_EXIT_ON_PM_DEATH, WL_TIMEOUT};

// TODO(pg-port): real WAIT_EVENT_REGISTER_SYNC_REQUEST lives in
// utils/activity/wait_event.h (generated).
const WAIT_EVENT_REGISTER_SYNC_REQUEST: u32 = 0x09000000;

// TODO(pg-port): real MemoryContextAllowInCriticalSection lives in
// utils/mmgr/mcxt.rs.
use crate::utils::mmgr::mcxt::MemoryContextAllowInCriticalSection;

// TODO(pg-port): real CheckpointStatsData / CheckpointStats lives in access/xlog.c.
#[repr(C)]
struct CheckpointStatsData {
    ckpt_sync_rels: c_int,
    ckpt_longest_sync: uint64,
    ckpt_agg_sync_time: uint64,
}
static mut CheckpointStats: CheckpointStatsData = CheckpointStatsData {
    ckpt_sync_rels: 0,
    ckpt_longest_sync: 0,
    ckpt_agg_sync_time: 0,
};

// TODO(pg-port): real data_sync_elevel lives in storage/file/fd.c.
unsafe fn data_sync_elevel(elevel: c_int) -> c_int {
    if enableFsync {
        elevel
    } else {
        WARNING
    }
}

// TODO(pg-port): real FILE_POSSIBLY_DELETED lives in storage/fd.h.
#[inline]
fn FILE_POSSIBLY_DELETED(err: c_int) -> bool {
    err == ENOENT
}

// TODO(pg-port): real AbsorbSyncRequests lives in postmaster/checkpointer.c.
unsafe fn AbsorbSyncRequests() {
    unimplemented!("AbsorbSyncRequests not yet ported (postmaster/checkpointer.c)")
}

// TODO(pg-port): real ForwardSyncRequest lives in postmaster/checkpointer.c.
unsafe fn ForwardSyncRequest(_ftag: *const FileTag, _type: SyncRequestType) -> bool {
    unimplemented!("ForwardSyncRequest not yet ported (postmaster/checkpointer.c)")
}

// TODO(pg-port): real md sync handlers live in storage/smgr/md.c.
unsafe fn mdsyncfiletag(_ftag: *const FileTag, _path: *mut c_char) -> c_int {
    unimplemented!("mdsyncfiletag not yet ported (storage/smgr/md.c)")
}
unsafe fn mdunlinkfiletag(_ftag: *const FileTag, _path: *mut c_char) -> c_int {
    unimplemented!("mdunlinkfiletag not yet ported (storage/smgr/md.c)")
}
unsafe fn mdfiletagmatches(_ftag: *const FileTag, _candidate: *const FileTag) -> bool {
    unimplemented!("mdfiletagmatches not yet ported (storage/smgr/md.c)")
}

// TODO(pg-port): real clog sync handler lives in access/transam/clog.c.
unsafe fn clogsyncfiletag(_ftag: *const FileTag, _path: *mut c_char) -> c_int {
    unimplemented!("clogsyncfiletag not yet ported (access/transam/clog.c)")
}

// TODO(pg-port): real commit_ts sync handler lives in access/transam/commit_ts.c.
unsafe fn committssyncfiletag(_ftag: *const FileTag, _path: *mut c_char) -> c_int {
    unimplemented!("committssyncfiletag not yet ported (access/transam/commit_ts.c)")
}

// TODO(pg-port): real multixact sync handlers live in access/transam/multixact.c.
unsafe fn multixactoffsetssyncfiletag(_ftag: *const FileTag, _path: *mut c_char) -> c_int {
    unimplemented!("multixactoffsetssyncfiletag not yet ported (access/transam/multixact.c)")
}
unsafe fn multixactmemberssyncfiletag(_ftag: *const FileTag, _path: *mut c_char) -> c_int {
    unimplemented!("multixactmemberssyncfiletag not yet ported (access/transam/multixact.c)")
}
