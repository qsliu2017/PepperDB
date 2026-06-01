//! src/backend/access/hash/hashsearch.c
//!
//! search code for postgres hash tables
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use crate::access::common::indextuple::{IndexTuple, IndexTupleData};
use crate::access::relscan::{IndexScanDescData, IndexScanInstrumentation, SnapshotData};
use crate::access::sdir::{ScanDirection, ScanDirectionIsBackward, ScanDirectionIsForward};
use crate::access::common::scankey::{ScanKey, ScanKeyData, SK_ISNULL};
use crate::access::stratnum::HTEqualStrategyNumber;
use crate::access::spgist::spgist_private::MaxIndexTuplesPerPage;
use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::storage::block::{BlockNumber, BlockNumberIsValid};
use crate::storage::buf::Buffer;
use crate::storage::bufpage::{
    Page, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
};
use crate::storage::itemid::ItemIdIsDead;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::off::{
    FirstOffsetNumber, OffsetNumber, OffsetNumberNext, OffsetNumberPrev,
};
use crate::utils::rel::Relation;

// IndexScanDesc: the canonical pointer typedef (relscan.h defines only the
// struct here).
pub type IndexScanDesc = *mut IndexScanDescData;

// ---------------------------------------------------------------------------
// access/hash.h - types, constants and helpers not yet ported.
// ---------------------------------------------------------------------------

pub type Bucket = uint32;

type HashPageOpaque = *mut HashPageOpaqueData;
type HashScanOpaque = *mut HashScanOpaqueData;

#[repr(C)]
pub struct HashPageOpaqueData {
    pub hasho_prevblkno: BlockNumber, /* see hash.h */
    pub hasho_nextblkno: BlockNumber, /* next ovfl page, or InvalidBlockNumber if none */
    pub hasho_bucket: Bucket,         /* bucket number this pg belongs to */
    pub hasho_flag: uint16,           /* page type code + flag bits, see hash.h */
    pub hasho_page_id: uint16,        /* for identification of hash indexes */
}

/* what we remember about each match */
#[repr(C)]
pub struct HashScanPosItem {
    pub heapTid: ItemPointerData,   /* TID of referenced heap item */
    pub indexOffset: OffsetNumber,  /* index item's location within page */
}

#[repr(C)]
pub struct HashScanPosData {
    pub buf: Buffer,            /* if valid, the buffer is pinned */
    pub currPage: BlockNumber,  /* current hash index page */
    pub nextPage: BlockNumber,  /* next overflow page */
    pub prevPage: BlockNumber,  /* prev overflow or bucket page */

    /*
     * The items array is always ordered in index order (ie, increasing
     * indexoffset).  When scanning backwards it is convenient to fill the
     * array back-to-front, so we start at the last slot and fill downwards.
     * Hence we need both a first-valid-entry and a last-valid-entry counter.
     * itemIndex is a cursor showing which entry was last returned to caller.
     */
    pub firstItem: c_int,       /* first valid index in items[] */
    pub lastItem: c_int,        /* last valid index in items[] */
    pub itemIndex: c_int,       /* current index in items[] */

    pub items: [HashScanPosItem; MaxIndexTuplesPerPage], /* MUST BE LAST */
}

/*
 *	HashScanOpaqueData is private state for a hash index scan.
 */
#[repr(C)]
pub struct HashScanOpaqueData {
    /* Hash value of the scan key, ie, the hash key we seek */
    pub hashso_sk_hash: uint32,

    /* remember the buffer associated with primary bucket */
    pub hashso_bucket_buf: Buffer,

    /*
     * remember the buffer associated with primary bucket page of bucket being
     * split.  it is required during the scan of the bucket which is being
     * populated during split operation.
     */
    pub hashso_split_bucket_buf: Buffer,

    /* Whether scan starts on bucket being populated due to split */
    pub hashso_buc_populated: bool,

    /*
     * Whether scanning bucket being split?  The value of this parameter is
     * referred only when hashso_buc_populated is true.
     */
    pub hashso_buc_split: bool,
    /* info about killed items if any (killedItems is NULL if never used) */
    pub killedItems: *mut c_int, /* currPos.items indexes of killed items */
    pub numKilled: c_int,        /* number of currently stored items */

    /*
     * Identify all the matching items on a page and save them in
     * HashScanPosData
     */
    pub currPos: HashScanPosData, /* current position data */
}

// page-type flags (hash.h)
const LH_OVERFLOW_PAGE: uint16 = 1 << 0;
const LH_BUCKET_PAGE: uint16 = 1 << 1;
const LH_BUCKET_BEING_POPULATED: uint16 = 1 << 4;

// _hash_getbuf access modes (hash.h)
const HASH_READ: c_int = 0; /* BUFFER_LOCK_SHARE */

// buffer lock modes (bufmgr.h)
const BUFFER_LOCK_UNLOCK: c_int = 0;
const BUFFER_LOCK_SHARE: c_int = 1;

const InvalidBuffer: Buffer = 0;
const InvalidBlockNumber: BlockNumber = 0xFFFFFFFF;

// INDEX_MOVED_BY_SPLIT_MASK == INDEX_AM_RESERVED_BIT (itup.h)
const INDEX_MOVED_BY_SPLIT_MASK: u16 = 0x2000;

unsafe fn HashPageGetOpaque(page: Page) -> HashPageOpaque {
    // PageGetSpecialPointer(page)
    let phdr = page as *mut u8;
    let special = *(phdr.add(core::mem::offset_of!(PageHeaderShim, pd_special)) as *const u16);
    phdr.add(special as usize) as HashPageOpaque
}

// Minimal page header shim to locate pd_special (storage/bufpage.h).
#[repr(C)]
struct PageHeaderShim {
    pd_lsn: u64,
    pd_checksum: u16,
    pd_flags: u16,
    pd_lower: u16,
    pd_upper: u16,
    pd_special: u16,
}

unsafe fn H_BUCKET_BEING_POPULATED(opaque: HashPageOpaque) -> bool {
    ((*opaque).hasho_flag & LH_BUCKET_BEING_POPULATED) != 0
}

unsafe fn _hash_get_indextuple_hashkey(itup: IndexTuple) -> uint32 {
    // The hash key is stored as the (single) attribute of the index tuple,
    // immediately after the IndexTupleData header.
    let attp = (itup as *mut c_char).add(core::mem::size_of::<IndexTupleData>()) as *const uint32;
    *attp
}

// HashScanPosInvalidate(scanpos) (hash.h)
unsafe fn HashScanPosInvalidate(scanpos: &mut HashScanPosData) {
    scanpos.buf = InvalidBuffer;
    scanpos.currPage = InvalidBlockNumber;
    scanpos.nextPage = InvalidBlockNumber;
    scanpos.prevPage = InvalidBlockNumber;
    scanpos.firstItem = 0;
    scanpos.lastItem = 0;
    scanpos.itemIndex = 0;
}

// ---------------------------------------------------------------------------
// IndexScanInstrumentation is currently an opaque type; access nsearches via a
// shim layout (executor/instrument.h).
// ---------------------------------------------------------------------------
#[repr(C)]
struct IndexScanInstrumentationShim {
    nsearches: u64,
}

// ---------------------------------------------------------------------------
// Stubbed callees from other (not-yet-ported) translation units.
// ---------------------------------------------------------------------------

unsafe fn _hash_getbuf(
    _rel: Relation,
    _blkno: BlockNumber,
    _access: c_int,
    _flags: c_int,
) -> Buffer {
    unimplemented!() // TODO: access/hashpage.c
}

unsafe fn _hash_getbucketbuf_from_hashkey(
    _rel: Relation,
    _hashkey: uint32,
    _access: c_int,
    _cachedmetap: *mut c_void,
) -> Buffer {
    unimplemented!() // TODO: access/hashpage.c
}

unsafe fn _hash_relbuf(_rel: Relation, _buf: Buffer) {
    unimplemented!() // TODO: access/hashpage.c
}

unsafe fn _hash_dropbuf(_rel: Relation, _buf: Buffer) {
    unimplemented!() // TODO: access/hashpage.c
}

unsafe fn _hash_dropscanbuf(_rel: Relation, _so: HashScanOpaque) {
    unimplemented!() // TODO: access/hashpage.c
}

unsafe fn _hash_get_oldblock_from_newbucket(_rel: Relation, _new_bucket: Bucket) -> BlockNumber {
    unimplemented!() // TODO: access/hashpage.c
}

unsafe fn _hash_checkpage(_rel: Relation, _buf: Buffer, _flags: c_int) {
    unimplemented!() // TODO: access/hashutil.c
}

unsafe fn _hash_binsearch(_page: Page, _hash_value: uint32) -> OffsetNumber {
    unimplemented!() // TODO: access/hashutil.c
}

unsafe fn _hash_binsearch_last(_page: Page, _hash_value: uint32) -> OffsetNumber {
    unimplemented!() // TODO: access/hashutil.c
}

unsafe fn _hash_kill_items(_scan: IndexScanDesc) {
    unimplemented!() // TODO: access/hashutil.c
}

unsafe fn _hash_checkqual(_scan: IndexScanDesc, _itup: IndexTuple) -> bool {
    unimplemented!() // TODO: access/hashutil.c
}

unsafe fn _hash_datum2hashkey(_rel: Relation, _key: Datum) -> uint32 {
    unimplemented!() // TODO: access/hashutil.c
}

unsafe fn _hash_datum2hashkey_type(_rel: Relation, _key: Datum, _keytype: Oid) -> uint32 {
    unimplemented!() // TODO: access/hashutil.c
}

unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/bufmgr.h
}

unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn BufferIsValid(_buffer: Buffer) -> bool {
    unimplemented!() // TODO: storage/bufmgr.h
}

unsafe fn BufferIsInvalid(_buffer: Buffer) -> bool {
    unimplemented!() // TODO: storage/bufmgr.h
}

unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn PredicateLockPage(_rel: Relation, _blkno: BlockNumber, _snapshot: *mut SnapshotData) {
    unimplemented!() // TODO: storage/predicate.c
}

unsafe fn pgstat_count_index_scan(_rel: Relation) {
    unimplemented!() // TODO: pgstat.h
}

// ---------------------------------------------------------------------------

/*
 *	_hash_next() -- Get the next item in a scan.
 *
 *		On entry, so->currPos describes the current page, which may
 *		be pinned but not locked, and so->currPos.itemIndex identifies
 *		which item was previously returned.
 *
 *		On successful exit, scan->xs_heaptid is set to the TID of the next
 *		heap tuple.  so->currPos is updated as needed.
 *
 *		On failure exit (no more tuples), we return false with pin
 *		held on bucket page but no pins or locks held on overflow
 *		page.
 */
pub unsafe fn _hash_next(scan: IndexScanDesc, dir: ScanDirection) -> bool {
    let rel: Relation = (*scan).indexRelation;
    let so: HashScanOpaque = (*scan).opaque as HashScanOpaque;
    let currItem: *mut HashScanPosItem;
    let blkno: BlockNumber;
    let mut buf: Buffer;
    let mut end_of_scan: bool = false;

    /*
     * Advance to the next tuple on the current page; or if done, try to read
     * data from the next or previous page based on the scan direction. Before
     * moving to the next or previous page make sure that we deal with all the
     * killed items.
     */
    if ScanDirectionIsForward(dir) {
        (*so).currPos.itemIndex += 1;
        if (*so).currPos.itemIndex > (*so).currPos.lastItem {
            if (*so).numKilled > 0 {
                _hash_kill_items(scan);
            }

            blkno = (*so).currPos.nextPage;
            if BlockNumberIsValid(blkno) {
                buf = _hash_getbuf(rel, blkno, HASH_READ, LH_OVERFLOW_PAGE as c_int);
                if !_hash_readpage(scan, &mut buf, dir) {
                    end_of_scan = true;
                }
            } else {
                end_of_scan = true;
            }
        }
    } else {
        (*so).currPos.itemIndex -= 1;
        if (*so).currPos.itemIndex < (*so).currPos.firstItem {
            if (*so).numKilled > 0 {
                _hash_kill_items(scan);
            }

            blkno = (*so).currPos.prevPage;
            if BlockNumberIsValid(blkno) {
                buf = _hash_getbuf(
                    rel,
                    blkno,
                    HASH_READ,
                    (LH_BUCKET_PAGE | LH_OVERFLOW_PAGE) as c_int,
                );

                /*
                 * We always maintain the pin on bucket page for whole scan
                 * operation, so releasing the additional pin we have acquired
                 * here.
                 */
                if buf == (*so).hashso_bucket_buf || buf == (*so).hashso_split_bucket_buf {
                    _hash_dropbuf(rel, buf);
                }

                if !_hash_readpage(scan, &mut buf, dir) {
                    end_of_scan = true;
                }
            } else {
                end_of_scan = true;
            }
        }
    }

    if end_of_scan {
        _hash_dropscanbuf(rel, so);
        HashScanPosInvalidate(&mut (*so).currPos);
        return false;
    }

    /* OK, itemIndex says what to return */
    currItem = &mut (*so).currPos.items[(*so).currPos.itemIndex as usize];
    (*scan).xs_heaptid = (*currItem).heapTid;

    true
}

/*
 * Advance to next page in a bucket, if any.  If we are scanning the bucket
 * being populated during split operation then this function advances to the
 * bucket being split after the last bucket page of bucket being populated.
 */
unsafe fn _hash_readnext(
    scan: IndexScanDesc,
    bufp: *mut Buffer,
    pagep: *mut Page,
    opaquep: *mut HashPageOpaque,
) {
    let blkno: BlockNumber;
    let rel: Relation = (*scan).indexRelation;
    let so: HashScanOpaque = (*scan).opaque as HashScanOpaque;
    let mut block_found: bool = false;

    blkno = (**opaquep).hasho_nextblkno;

    /*
     * Retain the pin on primary bucket page till the end of scan.  Refer the
     * comments in _hash_first to know the reason of retaining pin.
     */
    if *bufp == (*so).hashso_bucket_buf || *bufp == (*so).hashso_split_bucket_buf {
        LockBuffer(*bufp, BUFFER_LOCK_UNLOCK);
    } else {
        _hash_relbuf(rel, *bufp);
    }

    *bufp = InvalidBuffer;
    /* check for interrupts while we're not holding any buffer lock */
    CHECK_FOR_INTERRUPTS();
    if BlockNumberIsValid(blkno) {
        *bufp = _hash_getbuf(rel, blkno, HASH_READ, LH_OVERFLOW_PAGE as c_int);
        block_found = true;
    } else if (*so).hashso_buc_populated && !(*so).hashso_buc_split {
        /*
         * end of bucket, scan bucket being split if there was a split in
         * progress at the start of scan.
         */
        *bufp = (*so).hashso_split_bucket_buf;

        /*
         * buffer for bucket being split must be valid as we acquire the pin
         * on it before the start of scan and retain it till end of scan.
         */
        Assert!(BufferIsValid(*bufp));

        LockBuffer(*bufp, BUFFER_LOCK_SHARE);
        PredicateLockPage(rel, BufferGetBlockNumber(*bufp), (*scan).xs_snapshot);

        /*
         * setting hashso_buc_split to true indicates that we are scanning
         * bucket being split.
         */
        (*so).hashso_buc_split = true;

        block_found = true;
    }

    if block_found {
        *pagep = BufferGetPage(*bufp);
        *opaquep = HashPageGetOpaque(*pagep);
    }
}

/*
 * Advance to previous page in a bucket, if any.  If the current scan has
 * started during split operation then this function advances to bucket
 * being populated after the first bucket page of bucket being split.
 */
unsafe fn _hash_readprev(
    scan: IndexScanDesc,
    bufp: *mut Buffer,
    pagep: *mut Page,
    opaquep: *mut HashPageOpaque,
) {
    let blkno: BlockNumber;
    let rel: Relation = (*scan).indexRelation;
    let so: HashScanOpaque = (*scan).opaque as HashScanOpaque;
    let haveprevblk: bool;

    blkno = (**opaquep).hasho_prevblkno;

    /*
     * Retain the pin on primary bucket page till the end of scan.  Refer the
     * comments in _hash_first to know the reason of retaining pin.
     */
    if *bufp == (*so).hashso_bucket_buf || *bufp == (*so).hashso_split_bucket_buf {
        LockBuffer(*bufp, BUFFER_LOCK_UNLOCK);
        haveprevblk = false;
    } else {
        _hash_relbuf(rel, *bufp);
        haveprevblk = true;
    }

    *bufp = InvalidBuffer;
    /* check for interrupts while we're not holding any buffer lock */
    CHECK_FOR_INTERRUPTS();

    if haveprevblk {
        Assert!(BlockNumberIsValid(blkno));
        *bufp = _hash_getbuf(
            rel,
            blkno,
            HASH_READ,
            (LH_BUCKET_PAGE | LH_OVERFLOW_PAGE) as c_int,
        );
        *pagep = BufferGetPage(*bufp);
        *opaquep = HashPageGetOpaque(*pagep);

        /*
         * We always maintain the pin on bucket page for whole scan operation,
         * so releasing the additional pin we have acquired here.
         */
        if *bufp == (*so).hashso_bucket_buf || *bufp == (*so).hashso_split_bucket_buf {
            _hash_dropbuf(rel, *bufp);
        }
    } else if (*so).hashso_buc_populated && (*so).hashso_buc_split {
        /*
         * end of bucket, scan bucket being populated if there was a split in
         * progress at the start of scan.
         */
        *bufp = (*so).hashso_bucket_buf;

        /*
         * buffer for bucket being populated must be valid as we acquire the
         * pin on it before the start of scan and retain it till end of scan.
         */
        Assert!(BufferIsValid(*bufp));

        LockBuffer(*bufp, BUFFER_LOCK_SHARE);
        *pagep = BufferGetPage(*bufp);
        *opaquep = HashPageGetOpaque(*pagep);

        /* move to the end of bucket chain */
        while BlockNumberIsValid((**opaquep).hasho_nextblkno) {
            _hash_readnext(scan, bufp, pagep, opaquep);
        }

        /*
         * setting hashso_buc_split to false indicates that we are scanning
         * bucket being populated.
         */
        (*so).hashso_buc_split = false;
    }
}

/*
 *	_hash_first() -- Find the first item in a scan.
 *
 *		We find the first item (or, if backward scan, the last item) in the
 *		index that satisfies the qualification associated with the scan
 *		descriptor.
 *
 *		On successful exit, if the page containing current index tuple is an
 *		overflow page, both pin and lock are released whereas if it is a bucket
 *		page then it is pinned but not locked and data about the matching
 *		tuple(s) on the page has been loaded into so->currPos,
 *		scan->xs_heaptid is set to the heap TID of the current tuple.
 *
 *		On failure exit (no more tuples), we return false, with pin held on
 *		bucket page but no pins or locks held on overflow page.
 */
pub unsafe fn _hash_first(scan: IndexScanDesc, dir: ScanDirection) -> bool {
    let rel: Relation = (*scan).indexRelation;
    let so: HashScanOpaque = (*scan).opaque as HashScanOpaque;
    let cur: ScanKey;
    let hashkey: uint32;
    let bucket: Bucket;
    let mut buf: Buffer;
    let mut page: Page;
    let mut opaque: HashPageOpaque;
    let currItem: *mut HashScanPosItem;

    pgstat_count_index_scan(rel);
    if !(*scan).instrument.is_null() {
        (*((*scan).instrument as *mut IndexScanInstrumentationShim)).nsearches += 1;
    }

    /*
     * We do not support hash scans with no index qualification, because we
     * would have to read the whole index rather than just one bucket. That
     * creates a whole raft of problems, since we haven't got a practical way
     * to lock all the buckets against splits or compactions.
     */
    if (*scan).numberOfKeys < 1 {
        ereport!(
            ERROR,
            "hash indexes do not support whole-index scans"
        );
    }

    /* There may be more than one index qual, but we hash only the first */
    cur = &mut *((*scan).keyData as *mut ScanKeyData).add(0);

    /* We support only single-column hash indexes */
    Assert!((*cur).sk_attno == 1);
    /* And there's only one operator strategy, too */
    Assert!((*cur).sk_strategy == HTEqualStrategyNumber);

    /*
     * If the constant in the index qual is NULL, assume it cannot match any
     * items in the index.
     */
    if (*cur).sk_flags & SK_ISNULL != 0 {
        return false;
    }

    /*
     * Okay to compute the hash key.  We want to do this before acquiring any
     * locks, in case a user-defined hash function happens to be slow.
     *
     * If scankey operator is not a cross-type comparison, we can use the
     * cached hash function; otherwise gotta look it up in the catalogs.
     *
     * We support the convention that sk_subtype == InvalidOid means the
     * opclass input type; this is a hack to simplify life for ScanKeyInit().
     */
    if (*cur).sk_subtype == *(*rel).rd_opcintype.add(0) || (*cur).sk_subtype == InvalidOid {
        hashkey = _hash_datum2hashkey(rel, (*cur).sk_argument);
    } else {
        hashkey = _hash_datum2hashkey_type(rel, (*cur).sk_argument, (*cur).sk_subtype);
    }

    (*so).hashso_sk_hash = hashkey;

    buf = _hash_getbucketbuf_from_hashkey(rel, hashkey, HASH_READ, null_mut());
    PredicateLockPage(rel, BufferGetBlockNumber(buf), (*scan).xs_snapshot);
    page = BufferGetPage(buf);
    opaque = HashPageGetOpaque(page);
    bucket = (*opaque).hasho_bucket;

    (*so).hashso_bucket_buf = buf;

    /*
     * If a bucket split is in progress, then while scanning the bucket being
     * populated, we need to skip tuples that were copied from bucket being
     * split.  We also need to maintain a pin on the bucket being split to
     * ensure that split-cleanup work done by vacuum doesn't remove tuples
     * from it till this scan is done.  We need to maintain a pin on the
     * bucket being populated to ensure that vacuum doesn't squeeze that
     * bucket till this scan is complete; otherwise, the ordering of tuples
     * can't be maintained during forward and backward scans.  Here, we have
     * to be cautious about locking order: first, acquire the lock on bucket
     * being split; then, release the lock on it but not the pin; then,
     * acquire a lock on bucket being populated and again re-verify whether
     * the bucket split is still in progress.  Acquiring the lock on bucket
     * being split first ensures that the vacuum waits for this scan to
     * finish.
     */
    if H_BUCKET_BEING_POPULATED(opaque) {
        let old_blkno: BlockNumber;
        let old_buf: Buffer;

        old_blkno = _hash_get_oldblock_from_newbucket(rel, bucket);

        /*
         * release the lock on new bucket and re-acquire it after acquiring
         * the lock on old bucket.
         */
        LockBuffer(buf, BUFFER_LOCK_UNLOCK);

        old_buf = _hash_getbuf(rel, old_blkno, HASH_READ, LH_BUCKET_PAGE as c_int);

        /*
         * remember the split bucket buffer so as to use it later for
         * scanning.
         */
        (*so).hashso_split_bucket_buf = old_buf;
        LockBuffer(old_buf, BUFFER_LOCK_UNLOCK);

        LockBuffer(buf, BUFFER_LOCK_SHARE);
        page = BufferGetPage(buf);
        opaque = HashPageGetOpaque(page);
        Assert!((*opaque).hasho_bucket == bucket);

        if H_BUCKET_BEING_POPULATED(opaque) {
            (*so).hashso_buc_populated = true;
        } else {
            _hash_dropbuf(rel, (*so).hashso_split_bucket_buf);
            (*so).hashso_split_bucket_buf = InvalidBuffer;
        }
    }

    /* If a backwards scan is requested, move to the end of the chain */
    if ScanDirectionIsBackward(dir) {
        /*
         * Backward scans that start during split needs to start from end of
         * bucket being split.
         */
        while BlockNumberIsValid((*opaque).hasho_nextblkno)
            || ((*so).hashso_buc_populated && !(*so).hashso_buc_split)
        {
            _hash_readnext(scan, &mut buf, &mut page, &mut opaque);
        }
    }

    /* remember which buffer we have pinned, if any */
    Assert!(BufferIsInvalid((*so).currPos.buf));
    (*so).currPos.buf = buf;

    /* Now find all the tuples satisfying the qualification from a page */
    if !_hash_readpage(scan, &mut buf, dir) {
        return false;
    }

    /* OK, itemIndex says what to return */
    currItem = &mut (*so).currPos.items[(*so).currPos.itemIndex as usize];
    (*scan).xs_heaptid = (*currItem).heapTid;

    /* if we're here, _hash_readpage found a valid tuples */
    true
}

/*
 *	_hash_readpage() -- Load data from current index page into so->currPos
 *
 *	We scan all the items in the current index page and save them into
 *	so->currPos if it satisfies the qualification. If no matching items
 *	are found in the current page, we move to the next or previous page
 *	in a bucket chain as indicated by the direction.
 *
 *	Return true if any matching items are found else return false.
 */
unsafe fn _hash_readpage(scan: IndexScanDesc, bufP: *mut Buffer, dir: ScanDirection) -> bool {
    let rel: Relation = (*scan).indexRelation;
    let so: HashScanOpaque = (*scan).opaque as HashScanOpaque;
    let mut buf: Buffer;
    let mut page: Page;
    let mut opaque: HashPageOpaque;
    let mut offnum: OffsetNumber;
    let mut itemIndex: uint16;

    buf = *bufP;
    Assert!(BufferIsValid(buf));
    _hash_checkpage(rel, buf, (LH_BUCKET_PAGE | LH_OVERFLOW_PAGE) as c_int);
    page = BufferGetPage(buf);
    opaque = HashPageGetOpaque(page);

    (*so).currPos.buf = buf;
    (*so).currPos.currPage = BufferGetBlockNumber(buf);

    if ScanDirectionIsForward(dir) {
        let mut prev_blkno: BlockNumber;

        loop {
            /* new page, locate starting position by binary search */
            offnum = _hash_binsearch(page, (*so).hashso_sk_hash);

            itemIndex = _hash_load_qualified_items(scan, page, offnum, dir) as uint16;

            if itemIndex != 0 {
                break;
            }

            /*
             * Could not find any matching tuples in the current page, move to
             * the next page. Before leaving the current page, deal with any
             * killed items.
             */
            if (*so).numKilled > 0 {
                _hash_kill_items(scan);
            }

            /*
             * If this is a primary bucket page, hasho_prevblkno is not a real
             * block number.
             */
            if (*so).currPos.buf == (*so).hashso_bucket_buf
                || (*so).currPos.buf == (*so).hashso_split_bucket_buf
            {
                prev_blkno = InvalidBlockNumber;
            } else {
                prev_blkno = (*opaque).hasho_prevblkno;
            }

            _hash_readnext(scan, &mut buf, &mut page, &mut opaque);
            if BufferIsValid(buf) {
                (*so).currPos.buf = buf;
                (*so).currPos.currPage = BufferGetBlockNumber(buf);
            } else {
                /*
                 * Remember next and previous block numbers for scrollable
                 * cursors to know the start position and return false
                 * indicating that no more matching tuples were found. Also,
                 * don't reset currPage or lsn, because we expect
                 * _hash_kill_items to be called for the old page after this
                 * function returns.
                 */
                let _ = prev_blkno; // silence unused warning in else branch path
                (*so).currPos.prevPage = prev_blkno;
                (*so).currPos.nextPage = InvalidBlockNumber;
                (*so).currPos.buf = buf;
                return false;
            }
        }

        (*so).currPos.firstItem = 0;
        (*so).currPos.lastItem = itemIndex as c_int - 1;
        (*so).currPos.itemIndex = 0;
    } else {
        let mut next_blkno: BlockNumber;

        loop {
            /* new page, locate starting position by binary search */
            offnum = _hash_binsearch_last(page, (*so).hashso_sk_hash);

            itemIndex = _hash_load_qualified_items(scan, page, offnum, dir) as uint16;

            if itemIndex != MaxIndexTuplesPerPage as uint16 {
                break;
            }

            /*
             * Could not find any matching tuples in the current page, move to
             * the previous page. Before leaving the current page, deal with
             * any killed items.
             */
            if (*so).numKilled > 0 {
                _hash_kill_items(scan);
            }

            if (*so).currPos.buf == (*so).hashso_bucket_buf
                || (*so).currPos.buf == (*so).hashso_split_bucket_buf
            {
                next_blkno = (*opaque).hasho_nextblkno;
            } else {
                next_blkno = InvalidBlockNumber;
            }

            _hash_readprev(scan, &mut buf, &mut page, &mut opaque);
            if BufferIsValid(buf) {
                (*so).currPos.buf = buf;
                (*so).currPos.currPage = BufferGetBlockNumber(buf);
            } else {
                /*
                 * Remember next and previous block numbers for scrollable
                 * cursors to know the start position and return false
                 * indicating that no more matching tuples were found. Also,
                 * don't reset currPage or lsn, because we expect
                 * _hash_kill_items to be called for the old page after this
                 * function returns.
                 */
                let _ = next_blkno; // silence unused warning in else branch path
                (*so).currPos.prevPage = InvalidBlockNumber;
                (*so).currPos.nextPage = next_blkno;
                (*so).currPos.buf = buf;
                return false;
            }
        }

        (*so).currPos.firstItem = itemIndex as c_int;
        (*so).currPos.lastItem = MaxIndexTuplesPerPage as c_int - 1;
        (*so).currPos.itemIndex = MaxIndexTuplesPerPage as c_int - 1;
    }

    if (*so).currPos.buf == (*so).hashso_bucket_buf
        || (*so).currPos.buf == (*so).hashso_split_bucket_buf
    {
        (*so).currPos.prevPage = InvalidBlockNumber;
        (*so).currPos.nextPage = (*opaque).hasho_nextblkno;
        LockBuffer((*so).currPos.buf, BUFFER_LOCK_UNLOCK);
    } else {
        (*so).currPos.prevPage = (*opaque).hasho_prevblkno;
        (*so).currPos.nextPage = (*opaque).hasho_nextblkno;
        _hash_relbuf(rel, (*so).currPos.buf);
        (*so).currPos.buf = InvalidBuffer;
    }

    Assert!((*so).currPos.firstItem <= (*so).currPos.lastItem);
    true
}

/*
 * Load all the qualified items from a current index page
 * into so->currPos. Helper function for _hash_readpage.
 */
unsafe fn _hash_load_qualified_items(
    scan: IndexScanDesc,
    page: Page,
    mut offnum: OffsetNumber,
    dir: ScanDirection,
) -> c_int {
    let so: HashScanOpaque = (*scan).opaque as HashScanOpaque;
    let mut itup: IndexTuple;
    let mut itemIndex: c_int;
    let maxoff: OffsetNumber;

    maxoff = PageGetMaxOffsetNumber(page);

    if ScanDirectionIsForward(dir) {
        /* load items[] in ascending order */
        itemIndex = 0;

        while offnum <= maxoff {
            Assert!(offnum >= FirstOffsetNumber);
            itup = PageGetItem(page, PageGetItemId(page, offnum)) as IndexTuple;

            /*
             * skip the tuples that are moved by split operation for the scan
             * that has started when split was in progress. Also, skip the
             * tuples that are marked as dead.
             */
            if ((*so).hashso_buc_populated
                && !(*so).hashso_buc_split
                && ((*itup).t_info & INDEX_MOVED_BY_SPLIT_MASK) != 0)
                || ((*scan).ignore_killed_tuples && ItemIdIsDead(PageGetItemId(page, offnum)))
            {
                offnum = OffsetNumberNext(offnum); /* move forward */
                continue;
            }

            if (*so).hashso_sk_hash == _hash_get_indextuple_hashkey(itup)
                && _hash_checkqual(scan, itup)
            {
                /* tuple is qualified, so remember it */
                _hash_saveitem(so, itemIndex, offnum, itup);
                itemIndex += 1;
            } else {
                /*
                 * No more matching tuples exist in this page. so, exit while
                 * loop.
                 */
                break;
            }

            offnum = OffsetNumberNext(offnum);
        }

        Assert!(itemIndex <= MaxIndexTuplesPerPage as c_int);
        itemIndex
    } else {
        /* load items[] in descending order */
        itemIndex = MaxIndexTuplesPerPage as c_int;

        while offnum >= FirstOffsetNumber {
            Assert!(offnum <= maxoff);
            itup = PageGetItem(page, PageGetItemId(page, offnum)) as IndexTuple;

            /*
             * skip the tuples that are moved by split operation for the scan
             * that has started when split was in progress. Also, skip the
             * tuples that are marked as dead.
             */
            if ((*so).hashso_buc_populated
                && !(*so).hashso_buc_split
                && ((*itup).t_info & INDEX_MOVED_BY_SPLIT_MASK) != 0)
                || ((*scan).ignore_killed_tuples && ItemIdIsDead(PageGetItemId(page, offnum)))
            {
                offnum = OffsetNumberPrev(offnum); /* move back */
                continue;
            }

            if (*so).hashso_sk_hash == _hash_get_indextuple_hashkey(itup)
                && _hash_checkqual(scan, itup)
            {
                itemIndex -= 1;
                /* tuple is qualified, so remember it */
                _hash_saveitem(so, itemIndex, offnum, itup);
            } else {
                /*
                 * No more matching tuples exist in this page. so, exit while
                 * loop.
                 */
                break;
            }

            offnum = OffsetNumberPrev(offnum);
        }

        Assert!(itemIndex >= 0);
        itemIndex
    }
}

/* Save an index item into so->currPos.items[itemIndex] */
#[inline]
unsafe fn _hash_saveitem(
    so: HashScanOpaque,
    itemIndex: c_int,
    offnum: OffsetNumber,
    itup: IndexTuple,
) {
    let currItem: *mut HashScanPosItem = &mut (*so).currPos.items[itemIndex as usize];

    (*currItem).heapTid = (*itup).t_tid;
    (*currItem).indexOffset = offnum;
}
