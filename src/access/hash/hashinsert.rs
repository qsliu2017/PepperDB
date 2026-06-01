//! access/hash/hashinsert.c - Item insertion in hash tables for Postgres.

use crate::prelude::*;

use crate::access::common::indextuple::{IndexTuple, IndexTupleData, IndexTupleSize};
use crate::access::rmgrdesc::hashdesc::{
    xl_hash_insert, xl_hash_vacuum_one_page, SizeOfHashInsert, SizeOfHashVacuumOnePage,
    XLOG_HASH_INSERT, XLOG_HASH_VACUUM_ONE_PAGE,
};
use crate::access::rmgrlist::RM_HASH_ID;
use crate::access::transam::xlogdefs::XLogRecPtr;
use crate::elog;
use crate::miscadmin::{END_CRIT_SECTION, START_CRIT_SECTION};
use crate::pg_config::BLCKSZ;
use crate::storage::block::{BlockNumber, BlockNumberIsValid};
use crate::storage::buf::Buffer;
use crate::storage::bufpage::{
    Item, Page, PageAddItem, PageGetFreeSpace, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
    PageIndexMultiDelete, PageSetLSN,
};
use crate::storage::itemid::{ItemId, ItemIdIsDead};
use crate::storage::off::{
    OffsetNumber, OffsetNumberNext, FirstOffsetNumber, InvalidOffsetNumber, MaxOffsetNumber,
};
use crate::utils::elog::ERROR;
use crate::utils::rel::{Relation, RelationGetRelationName};

// ---------------------------------------------------------------------------
// access/hash.h - types, constants and helpers not yet ported.
// ---------------------------------------------------------------------------

pub type Bucket = uint32;

type HashMetaPage = *mut HashMetaPageData;
type HashPageOpaque = *mut HashPageOpaqueData;

#[repr(C)]
pub struct HashMetaPageData {
    pub hashm_ntuples: f64,
    pub hashm_ffactor: uint16,
    pub hashm_maxbucket: uint32,
    pub hashm_highmask: uint32,
    pub hashm_lowmask: uint32,
}

#[repr(C)]
pub struct HashPageOpaqueData {
    pub hasho_nextblkno: BlockNumber,
    pub hasho_bucket: Bucket,
    pub hasho_flag: uint16,
}

// page-type flags (hash.h)
const LH_OVERFLOW_PAGE: uint16 = 1 << 0;
const LH_BUCKET_PAGE: uint16 = 1 << 1;
const LH_META_PAGE: uint16 = 1 << 3;
const LH_PAGE_HAS_DEAD_TUPLES: uint16 = 1 << 5;
const LH_PAGE_TYPE: uint16 = LH_OVERFLOW_PAGE | LH_BUCKET_PAGE | LH_META_PAGE;

// special block number for the metapage (hash.h)
const HASH_METAPAGE: BlockNumber = 0;

// _hash_getbuf flags (hash.h)
const HASH_NOLOCK: c_int = -1;
const HASH_WRITE: c_int = 0x2000;

// buffer lock modes (bufmgr.h)
const BUFFER_LOCK_UNLOCK: c_int = 0;
const BUFFER_LOCK_EXCLUSIVE: c_int = 2;

// REGBUF flag (xloginsert.h)
const REGBUF_STANDARD: uint8 = 0x04;

const InvalidBuffer: Buffer = 0;

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

unsafe fn HashPageGetMeta(page: Page) -> HashMetaPage {
    // (HashMetaPage) PageGetContents(page)
    let phdr = page as *mut u8;
    // PageGetContents skips the page header (MAXALIGN(SizeOfPageHeaderData)).
    phdr.add(MAXALIGN(core::mem::size_of::<PageHeaderShim>())) as HashMetaPage
}

unsafe fn H_BUCKET_BEING_SPLIT(opaque: HashPageOpaque) -> bool {
    const LH_BUCKET_BEING_SPLIT: uint16 = 1 << 6;
    ((*opaque).hasho_flag & LH_BUCKET_BEING_SPLIT) != 0
}

unsafe fn H_HAS_DEAD_TUPLES(opaque: HashPageOpaque) -> bool {
    ((*opaque).hasho_flag & LH_PAGE_HAS_DEAD_TUPLES) != 0
}

unsafe fn _hash_get_indextuple_hashkey(itup: IndexTuple) -> uint32 {
    // The hash key is stored as the (single) attribute of the index tuple,
    // immediately after the IndexTupleData header.
    let attp = (itup as *mut c_char).add(core::mem::size_of::<IndexTupleData>()) as *const uint32;
    *attp
}

// HashMaxItemSize() (hash.h): largest tuple that fits on a hash page.
unsafe fn HashMaxItemSize(_page: Page) -> Size {
    // MAXALIGN_DOWN(BLCKSZ - SizeOfPageHeaderData
    //   - sizeof(ItemIdData) - MAXALIGN(sizeof(HashPageOpaqueData)))
    let header = MAXALIGN(core::mem::size_of::<PageHeaderShim>());
    crate::c::MAXALIGN_DOWN(
        BLCKSZ
            - header
            - core::mem::size_of::<ItemIdShim>()
            - MAXALIGN(core::mem::size_of::<HashPageOpaqueData>()),
    )
}

#[repr(C)]
struct ItemIdShim {
    _bits: u32,
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
    _cachedmetap: *mut HashMetaPage,
) -> Buffer {
    unimplemented!() // TODO: access/hashpage.c
}

unsafe fn _hash_relbuf(_rel: Relation, _buf: Buffer) {
    unimplemented!() // TODO: access/hashpage.c
}

unsafe fn _hash_dropbuf(_rel: Relation, _buf: Buffer) {
    unimplemented!() // TODO: access/hashpage.c
}

unsafe fn _hash_addovflpage(
    _rel: Relation,
    _metabuf: Buffer,
    _buf: Buffer,
    _retain_pin: bool,
) -> Buffer {
    unimplemented!() // TODO: access/hashovfl.c
}

unsafe fn _hash_finish_split(
    _rel: Relation,
    _metabuf: Buffer,
    _obuf: Buffer,
    _obucket: Bucket,
    _maxbucket: uint32,
    _highmask: uint32,
    _lowmask: uint32,
) {
    unimplemented!() // TODO: access/hashpage.c
}

unsafe fn _hash_expandtable(_rel: Relation, _metabuf: Buffer) {
    unimplemented!() // TODO: access/hashpage.c
}

unsafe fn _hash_checkpage(_rel: Relation, _buf: Buffer, _flags: c_int) {
    unimplemented!() // TODO: access/hashutil.c
}

unsafe fn _hash_binsearch(_page: Page, _hash_value: uint32) -> OffsetNumber {
    unimplemented!() // TODO: access/hashutil.c
}

unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/bufmgr.h
}

unsafe fn BufferGetBlockNumber(_buffer: Buffer) -> BlockNumber {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn MarkBufferDirty(_buffer: Buffer) {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn IsBufferCleanupOK(_buffer: Buffer) -> bool {
    unimplemented!() // TODO: storage/bufmgr.c
}

unsafe fn RelationNeedsWAL(_relation: Relation) -> bool {
    unimplemented!() // TODO: utils/rel.h
}

unsafe fn RelationIsAccessibleInLogicalDecoding(_relation: Relation) -> bool {
    unimplemented!() // TODO: utils/rel.h
}

unsafe fn CheckForSerializableConflictIn(
    _relation: Relation,
    _tid: *mut c_void,
    _blkno: BlockNumber,
) {
    unimplemented!() // TODO: storage/predicate.c
}

unsafe fn index_compute_xid_horizon_for_tuples(
    _irel: Relation,
    _hrel: Relation,
    _ibuf: Buffer,
    _itemnos: *mut OffsetNumber,
    _nitems: c_int,
) -> TransactionId {
    unimplemented!() // TODO: access/genam.c
}

unsafe fn XLogBeginInsert() {
    unimplemented!() // TODO: access/xloginsert.c
}

unsafe fn XLogRegisterData(_data: *mut c_void, _len: c_int) {
    unimplemented!() // TODO: access/xloginsert.c
}

unsafe fn XLogRegisterBuffer(_block_id: uint8, _buffer: Buffer, _flags: uint8) {
    unimplemented!() // TODO: access/xloginsert.c
}

unsafe fn XLogRegisterBufData(_block_id: uint8, _data: *mut c_void, _len: c_int) {
    unimplemented!() // TODO: access/xloginsert.c
}

unsafe fn XLogInsert(_rmid: u8, _info: uint8) -> u64 {
    unimplemented!() // TODO: access/xloginsert.c
}

// ---------------------------------------------------------------------------

/*
 *	_hash_doinsert() -- Handle insertion of a single index tuple.
 *
 *		This routine is called by the public interface routines, hashbuild
 *		and hashinsert.  By here, itup is completely filled in.
 *
 * 'sorted' must only be passed as 'true' when inserts are done in hashkey
 * order.
 */
pub unsafe fn _hash_doinsert(rel: Relation, itup: IndexTuple, heapRel: Relation, sorted: bool) {
    let mut buf: Buffer = InvalidBuffer;
    let mut bucket_buf: Buffer;
    let mut metabuf: Buffer;
    let metap: HashMetaPage;
    let mut usedmetap: HashMetaPage = null_mut();
    let mut metapage: Page;
    let mut page: Page;
    let mut pageopaque: HashPageOpaque;
    let mut itemsz: Size;
    let do_expand: bool;
    let hashkey: uint32;
    let mut bucket: Bucket;
    let itup_off: OffsetNumber;

    /*
     * Get the hash key for the item (it's stored in the index tuple itself).
     */
    hashkey = _hash_get_indextuple_hashkey(itup);

    /* compute item size too */
    itemsz = IndexTupleSize(itup as *const IndexTupleData);
    itemsz = MAXALIGN(itemsz); /* be safe, PageAddItem will do this but we
                                * need to be consistent */

    'restart_insert: loop {
        /*
         * Read the metapage.  We don't lock it yet; HashMaxItemSize() will
         * examine pd_pagesize_version, but that can't change so we can examine
         * it without a lock.
         */
        metabuf = _hash_getbuf(rel, HASH_METAPAGE, HASH_NOLOCK, LH_META_PAGE as c_int);
        metapage = BufferGetPage(metabuf);

        /*
         * Check whether the item can fit on a hash page at all. (Eventually,
         * we ought to try to apply TOAST methods if not.)  Note that at this
         * point, itemsz doesn't include the ItemId.
         *
         * XXX this is useless code if we are only storing hash keys.
         */
        if itemsz > HashMaxItemSize(metapage) {
            ereport!(
                ERROR,
                "index row size exceeds hash maximum; values larger than a buffer page cannot be indexed"
            );
        }

        /* Lock the primary bucket page for the target bucket. */
        buf = _hash_getbucketbuf_from_hashkey(rel, hashkey, HASH_WRITE, &mut usedmetap);
        Assert!(!usedmetap.is_null());

        CheckForSerializableConflictIn(rel, null_mut(), BufferGetBlockNumber(buf));

        /* remember the primary bucket buffer to release the pin on it at end. */
        bucket_buf = buf;

        page = BufferGetPage(buf);
        pageopaque = HashPageGetOpaque(page);
        bucket = (*pageopaque).hasho_bucket;

        /*
         * If this bucket is in the process of being split, try to finish the
         * split before inserting, because that might create room for the
         * insertion to proceed without allocating an additional overflow page.
         * It's only interesting to finish the split if we're trying to insert
         * into the bucket from which we're removing tuples (the "old" bucket),
         * not if we're trying to insert into the bucket into which tuples are
         * being moved (the "new" bucket).
         */
        if H_BUCKET_BEING_SPLIT(pageopaque) && IsBufferCleanupOK(buf) {
            /* release the lock on bucket buffer, before completing the split. */
            LockBuffer(buf, BUFFER_LOCK_UNLOCK);

            _hash_finish_split(
                rel,
                metabuf,
                buf,
                bucket,
                (*usedmetap).hashm_maxbucket,
                (*usedmetap).hashm_highmask,
                (*usedmetap).hashm_lowmask,
            );

            /* release the pin on old and meta buffer.  retry for insert. */
            _hash_dropbuf(rel, buf);
            _hash_dropbuf(rel, metabuf);
            continue 'restart_insert;
        }

        /* Do the insertion */
        while PageGetFreeSpace(page) < itemsz {
            let nextblkno: BlockNumber;

            /*
             * Check if current page has any DEAD tuples. If yes, delete these
             * tuples and see if we can get a space for the new item to be
             * inserted before moving to the next page in the bucket chain.
             */
            if H_HAS_DEAD_TUPLES(pageopaque) {
                if IsBufferCleanupOK(buf) {
                    _hash_vacuum_one_page(rel, heapRel, metabuf, buf);

                    if PageGetFreeSpace(page) >= itemsz {
                        break; /* OK, now we have enough space */
                    }
                }
            }

            /*
             * no space on this page; check for an overflow page
             */
            nextblkno = (*pageopaque).hasho_nextblkno;

            if BlockNumberIsValid(nextblkno) {
                /*
                 * ovfl page exists; go get it.  if it doesn't have room, we'll
                 * find out next pass through the loop test above.  we always
                 * release both the lock and pin if this is an overflow page,
                 * but only the lock if this is the primary bucket page, since
                 * the pin on the primary bucket must be retained throughout the
                 * scan.
                 */
                if buf != bucket_buf {
                    _hash_relbuf(rel, buf);
                } else {
                    LockBuffer(buf, BUFFER_LOCK_UNLOCK);
                }
                buf = _hash_getbuf(rel, nextblkno, HASH_WRITE, LH_OVERFLOW_PAGE as c_int);
                page = BufferGetPage(buf);
            } else {
                /*
                 * we're at the end of the bucket chain and we haven't found a
                 * page with enough room.  allocate a new overflow page.
                 */

                /* release our write lock without modifying buffer */
                LockBuffer(buf, BUFFER_LOCK_UNLOCK);

                /* chain to a new overflow page */
                buf = _hash_addovflpage(rel, metabuf, buf, buf == bucket_buf);
                page = BufferGetPage(buf);

                /* should fit now, given test above */
                Assert!(PageGetFreeSpace(page) >= itemsz);
            }
            pageopaque = HashPageGetOpaque(page);
            Assert!(((*pageopaque).hasho_flag & LH_PAGE_TYPE) == LH_OVERFLOW_PAGE);
            Assert!((*pageopaque).hasho_bucket == bucket);
        }

        /*
         * Write-lock the metapage so we can increment the tuple count. After
         * incrementing it, check to see if it's time for a split.
         */
        LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

        /* Do the update.  No ereport(ERROR) until changes are logged */
        START_CRIT_SECTION();

        /* found page with enough space, so add the item here */
        itup_off = _hash_pgaddtup(rel, buf, itemsz, itup, sorted);
        MarkBufferDirty(buf);

        /* metapage operations */
        metap = HashPageGetMeta(metapage);
        (*metap).hashm_ntuples += 1.0;

        /* Make sure this stays in sync with _hash_expandtable() */
        do_expand = (*metap).hashm_ntuples
            > (*metap).hashm_ffactor as f64 * ((*metap).hashm_maxbucket + 1) as f64;

        MarkBufferDirty(metabuf);

        /* XLOG stuff */
        if RelationNeedsWAL(rel) {
            let mut xlrec: xl_hash_insert = core::mem::zeroed();
            let recptr: XLogRecPtr;

            xlrec.offnum = itup_off;

            XLogBeginInsert();
            XLogRegisterData(
                &mut xlrec as *mut _ as *mut c_void,
                SizeOfHashInsert as c_int,
            );

            XLogRegisterBuffer(1, metabuf, REGBUF_STANDARD);

            XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
            XLogRegisterBufData(
                0,
                itup as *mut c_void,
                IndexTupleSize(itup as *const IndexTupleData) as c_int,
            );

            recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_INSERT);

            PageSetLSN(BufferGetPage(buf), recptr);
            PageSetLSN(BufferGetPage(metabuf), recptr);
        }

        END_CRIT_SECTION();

        /* drop lock on metapage, but keep pin */
        LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);

        /*
         * Release the modified page and ensure to release the pin on primary
         * page.
         */
        _hash_relbuf(rel, buf);
        if buf != bucket_buf {
            _hash_dropbuf(rel, bucket_buf);
        }

        /* Attempt to split if a split is needed */
        if do_expand {
            _hash_expandtable(rel, metabuf);
        }

        /* Finally drop our pin on the metapage */
        _hash_dropbuf(rel, metabuf);

        break;
    }
}

/*
 *	_hash_pgaddtup() -- add a tuple to a particular page in the index.
 *
 * This routine adds the tuple to the page as requested; it does not write out
 * the page.  It is an error to call this function without pin and write lock
 * on the target buffer.
 *
 * Returns the offset number at which the tuple was inserted.  This function
 * is responsible for preserving the condition that tuples in a hash index
 * page are sorted by hashkey value, however, if the caller is certain that
 * the hashkey for the tuple being added is >= the hashkeys of all existing
 * tuples on the page, then the 'appendtup' flag may be passed as true.  This
 * saves from having to binary search for the correct location to insert the
 * tuple.
 */
pub unsafe fn _hash_pgaddtup(
    rel: Relation,
    buf: Buffer,
    itemsize: Size,
    itup: IndexTuple,
    appendtup: bool,
) -> OffsetNumber {
    let itup_off: OffsetNumber;
    let page: Page;

    _hash_checkpage(rel, buf, (LH_BUCKET_PAGE | LH_OVERFLOW_PAGE) as c_int);
    page = BufferGetPage(buf);

    /*
     * Find where to insert the tuple (preserving page's hashkey ordering). If
     * 'appendtup' is true then we just insert it at the end.
     */
    if appendtup {
        itup_off = PageGetMaxOffsetNumber(page) + 1;

        /* ensure this tuple's hashkey is >= the final existing tuple */
        if cfg!(debug_assertions) && PageGetMaxOffsetNumber(page) > 0 {
            let lasttup: IndexTuple;
            let itemid: ItemId;

            itemid = PageGetItemId(page, PageGetMaxOffsetNumber(page));
            lasttup = PageGetItem(page, itemid) as IndexTuple;

            Assert!(
                _hash_get_indextuple_hashkey(lasttup) <= _hash_get_indextuple_hashkey(itup)
            );
        }
    } else {
        let hashkey: uint32 = _hash_get_indextuple_hashkey(itup);

        itup_off = _hash_binsearch(page, hashkey);
    }

    if PageAddItem(page, itup as Item, itemsize, itup_off, false, false) == InvalidOffsetNumber {
        elog!(ERROR, "failed to add index item to \"{}\"", "?");
        let _ = RelationGetRelationName(rel);
    }

    itup_off
}

/*
 *	_hash_pgaddmultitup() -- add a tuple vector to a particular page in the
 *							 index.
 *
 * This routine has same requirements for locking and tuple ordering as
 * _hash_pgaddtup().
 *
 * Returns the offset number array at which the tuples were inserted.
 */
pub unsafe fn _hash_pgaddmultitup(
    rel: Relation,
    buf: Buffer,
    itups: *mut IndexTuple,
    itup_offsets: *mut OffsetNumber,
    nitups: uint16,
) {
    let mut itup_off: OffsetNumber;
    let page: Page;
    let mut hashkey: uint32;
    let mut i: c_int;

    _hash_checkpage(rel, buf, (LH_BUCKET_PAGE | LH_OVERFLOW_PAGE) as c_int);
    page = BufferGetPage(buf);

    i = 0;
    while i < nitups as c_int {
        let mut itemsize: Size;

        itemsize = IndexTupleSize(*itups.add(i as usize) as *const IndexTupleData);
        itemsize = MAXALIGN(itemsize);

        /* Find where to insert the tuple (preserving page's hashkey ordering) */
        hashkey = _hash_get_indextuple_hashkey(*itups.add(i as usize));
        itup_off = _hash_binsearch(page, hashkey);

        *itup_offsets.add(i as usize) = itup_off;

        if PageAddItem(
            page,
            *itups.add(i as usize) as Item,
            itemsize,
            itup_off,
            false,
            false,
        ) == InvalidOffsetNumber
        {
            elog!(ERROR, "failed to add index item to \"{}\"", "?");
            let _ = RelationGetRelationName(rel);
        }

        i += 1;
    }
}

/*
 * _hash_vacuum_one_page - vacuum just one index page.
 *
 * Try to remove LP_DEAD items from the given page. We must acquire cleanup
 * lock on the page being modified before calling this function.
 */
unsafe fn _hash_vacuum_one_page(rel: Relation, hrel: Relation, metabuf: Buffer, buf: Buffer) {
    let mut deletable: [OffsetNumber; MaxOffsetNumber as usize] =
        [0; MaxOffsetNumber as usize];
    let mut ndeletable: c_int = 0;
    let mut offnum: OffsetNumber;
    let maxoff: OffsetNumber;
    let page: Page = BufferGetPage(buf);
    let pageopaque: HashPageOpaque;
    let metap: HashMetaPage;

    /* Scan each tuple in page to see if it is marked as LP_DEAD */
    maxoff = PageGetMaxOffsetNumber(page);
    offnum = FirstOffsetNumber;
    while offnum <= maxoff {
        let itemId: ItemId = PageGetItemId(page, offnum);

        if ItemIdIsDead(itemId) {
            deletable[ndeletable as usize] = offnum;
            ndeletable += 1;
        }
        offnum = OffsetNumberNext(offnum);
    }

    if ndeletable > 0 {
        let snapshotConflictHorizon: TransactionId;

        snapshotConflictHorizon = index_compute_xid_horizon_for_tuples(
            rel,
            hrel,
            buf,
            deletable.as_mut_ptr(),
            ndeletable,
        );

        /*
         * Write-lock the meta page so that we can decrement tuple count.
         */
        LockBuffer(metabuf, BUFFER_LOCK_EXCLUSIVE);

        /* No ereport(ERROR) until changes are logged */
        START_CRIT_SECTION();

        PageIndexMultiDelete(page, deletable.as_mut_ptr(), ndeletable);

        /*
         * Mark the page as not containing any LP_DEAD items. This is not
         * certainly true (there might be some that have recently been marked,
         * but weren't included in our target-item list), but it will almost
         * always be true and it doesn't seem worth an additional page scan to
         * check it. Remember that LH_PAGE_HAS_DEAD_TUPLES is only a hint
         * anyway.
         */
        pageopaque = HashPageGetOpaque(page);
        (*pageopaque).hasho_flag &= !LH_PAGE_HAS_DEAD_TUPLES;

        metap = HashPageGetMeta(BufferGetPage(metabuf));
        (*metap).hashm_ntuples -= ndeletable as f64;

        MarkBufferDirty(buf);
        MarkBufferDirty(metabuf);

        /* XLOG stuff */
        if RelationNeedsWAL(rel) {
            let mut xlrec: xl_hash_vacuum_one_page = core::mem::zeroed();
            let recptr: XLogRecPtr;

            xlrec.isCatalogRel = RelationIsAccessibleInLogicalDecoding(hrel);
            xlrec.snapshotConflictHorizon = snapshotConflictHorizon;
            xlrec.ntuples = ndeletable as uint16;

            XLogBeginInsert();
            XLogRegisterBuffer(0, buf, REGBUF_STANDARD);
            XLogRegisterData(
                &mut xlrec as *mut _ as *mut c_void,
                SizeOfHashVacuumOnePage as c_int,
            );

            /*
             * We need the target-offsets array whether or not we store the
             * whole buffer, to allow us to find the snapshotConflictHorizon on
             * a standby server.
             */
            XLogRegisterData(
                deletable.as_mut_ptr() as *mut c_void,
                ndeletable * core::mem::size_of::<OffsetNumber>() as c_int,
            );

            XLogRegisterBuffer(1, metabuf, REGBUF_STANDARD);

            recptr = XLogInsert(RM_HASH_ID, XLOG_HASH_VACUUM_ONE_PAGE);

            PageSetLSN(BufferGetPage(buf), recptr);
            PageSetLSN(BufferGetPage(metabuf), recptr);
        }

        END_CRIT_SECTION();

        /*
         * Releasing write lock on meta page as we have updated the tuple count.
         */
        LockBuffer(metabuf, BUFFER_LOCK_UNLOCK);
    }
}
