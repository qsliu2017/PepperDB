//! src/backend/access/hash/hashutil.c
//!
//! Utility code for Postgres hash implementation.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_ushort, c_void};
use crate::pg_config::BLCKSZ;

use crate::access::attnum::AttrNumber;
use crate::access::common::indextuple::{IndexInfoFindDataOffset, IndexTuple};
use crate::access::relscan::IndexScanDescData;
use crate::port::pg_bitutils::{pg_ceil_log2_32, pg_leftmost_one_pos32};
use crate::postgres_ext::Oid;
use crate::storage::block::BlockNumber;
use crate::storage::bufpage::{
    Page, PageGetContents, PageGetItem, PageGetItemId, PageGetMaxOffsetNumber,
    PageGetSpecialPointer, PageGetSpecialSize, PageIsNew,
};
use crate::storage::buf::Buffer;
use crate::storage::itemid::{ItemId, ItemIdMarkDead};
use crate::storage::itemptr::{ItemPointerData, ItemPointerEquals};
use crate::storage::off::{
    FirstOffsetNumber, OffsetNumber, OffsetNumberIsValid, OffsetNumberNext,
};
use crate::storage::block::BlockNumberIsValid;
use crate::utils::fmgr::{FmgrInfo, FunctionCall1Coll, OidFunctionCall1Coll};
use crate::utils::rel::{Relation, RelationGetRelationName};
use crate::access::spgist::spgist_private::MaxIndexTuplesPerPage;

// IndexScanDesc: relscan.rs only defines IndexScanDescData; the canonical
// pointer typedef is needed because this file dereferences scan fields.
pub type IndexScanDesc = *mut IndexScanDescData;

// ---------------------------------------------------------------------------
// Buffer-manager helpers (bufmgr.h). No canonical Rust port exists yet; these
// are local stubs matching the C signatures, mirroring sibling hash files.
// ---------------------------------------------------------------------------
const BUFFER_LOCK_UNLOCK: c_int = 0;
const BUFFER_LOCK_SHARE: c_int = 1;
const BUFFER_LOCK_EXCLUSIVE: c_int = 2;

unsafe fn BufferGetPage(_buffer: Buffer) -> Page {
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}

unsafe fn BufferIsValid(_buffer: Buffer) -> bool { crate::access::nbtree::nbtpage::BufferIsValid(_buffer) }

unsafe fn LockBuffer(_buffer: Buffer, _mode: c_int) {
    unimplemented!() // TODO: storage/buffer/bufmgr.c
}

unsafe fn MarkBufferDirtyHint(_buffer: Buffer, _buffer_std: bool) { crate::storage::buffer::bufmgr::MarkBufferDirtyHint(_buffer, _buffer_std) }

// ---------------------------------------------------------------------------
// Declarations merged from access/hash/hash.h that are needed by hashutil.c
// ---------------------------------------------------------------------------

/*
 * Mapping from hash bucket number to physical block number of bucket's
 * starting page.  Beware of multiple evaluations of argument!
 */
pub type Bucket = uint32;

pub const InvalidBucket: Bucket = 0xFFFFFFFF as Bucket;

/* BUCKET_TO_BLKNO(metap,B) */
#[inline]
pub unsafe fn BUCKET_TO_BLKNO(metap: HashMetaPage, B: Bucket) -> BlockNumber {
	((B as BlockNumber)
		+ (if B != 0 {
			(*metap).hashm_spares[(_hash_spareindex(B + 1) - 1) as usize]
		} else {
			0
		})) + 1
}

/*
 * Special space for hash index pages.
 */
pub const LH_UNUSED_PAGE: c_int = 0;
pub const LH_OVERFLOW_PAGE: c_int = 1 << 0;
pub const LH_BUCKET_PAGE: c_int = 1 << 1;
pub const LH_BITMAP_PAGE: c_int = 1 << 2;
pub const LH_META_PAGE: c_int = 1 << 3;
pub const LH_BUCKET_BEING_POPULATED: c_int = 1 << 4;
pub const LH_BUCKET_BEING_SPLIT: c_int = 1 << 5;
pub const LH_BUCKET_NEEDS_SPLIT_CLEANUP: c_int = 1 << 6;
pub const LH_PAGE_HAS_DEAD_TUPLES: c_int = 1 << 7;

pub const LH_PAGE_TYPE: c_int =
	LH_OVERFLOW_PAGE | LH_BUCKET_PAGE | LH_BITMAP_PAGE | LH_META_PAGE;

#[repr(C)]
pub struct HashPageOpaqueData {
	pub hasho_prevblkno: BlockNumber, /* see above */
	pub hasho_nextblkno: BlockNumber, /* see above */
	pub hasho_bucket: Bucket,         /* bucket number this pg belongs to */
	pub hasho_flag: uint16,           /* page type code + flag bits, see above */
	pub hasho_page_id: uint16,        /* for identification of hash indexes */
}

pub type HashPageOpaque = *mut HashPageOpaqueData;

#[inline]
pub unsafe fn HashPageGetOpaque(page: Page) -> HashPageOpaque {
	PageGetSpecialPointer(page) as HashPageOpaque
}

pub const HASHO_PAGE_ID: c_int = 0xFF80;

#[repr(C)]
pub struct HashScanPosItem {
	/* what we remember about each match */
	pub heapTid: ItemPointerData, /* TID of referenced heap item */
	pub indexOffset: OffsetNumber, /* index item's location within page */
}

#[repr(C)]
pub struct HashScanPosData {
	pub buf: Buffer,           /* if valid, the buffer is pinned */
	pub currPage: BlockNumber, /* current hash index page */
	pub nextPage: BlockNumber, /* next overflow page */
	pub prevPage: BlockNumber, /* prev overflow or bucket page */

	pub firstItem: c_int, /* first valid index in items[] */
	pub lastItem: c_int,  /* last valid index in items[] */
	pub itemIndex: c_int, /* current index in items[] */

	pub items: [HashScanPosItem; MaxIndexTuplesPerPage as usize], /* MUST BE LAST */
}

/* HashScanPosIsPinned(scanpos) */
#[inline]
pub unsafe fn HashScanPosIsPinned(scanpos: &HashScanPosData) -> bool {
	AssertMacro!(
		BlockNumberIsValid(scanpos.currPage) || !BufferIsValid(scanpos.buf)
	);
	BufferIsValid(scanpos.buf)
}

/* HashScanPosIsValid(scanpos) */
#[inline]
pub unsafe fn HashScanPosIsValid(scanpos: &HashScanPosData) -> bool {
	AssertMacro!(
		BlockNumberIsValid(scanpos.currPage) || !BufferIsValid(scanpos.buf)
	);
	BlockNumberIsValid(scanpos.currPage)
}

#[repr(C)]
pub struct HashScanOpaqueData {
	/* Hash value of the scan key, ie, the hash key we seek */
	pub hashso_sk_hash: uint32,

	/* remember the buffer associated with primary bucket */
	pub hashso_bucket_buf: Buffer,

	/*
	 * remember the buffer associated with primary bucket page of bucket being
	 * split.
	 */
	pub hashso_split_bucket_buf: Buffer,

	/* Whether scan starts on bucket being populated due to split */
	pub hashso_buc_populated: bool,

	/* Whether scanning bucket being split? */
	pub hashso_buc_split: bool,
	/* info about killed items if any (killedItems is NULL if never used) */
	pub killedItems: *mut c_int, /* currPos.items indexes of killed items */
	pub numKilled: c_int,        /* number of currently stored items */

	/* Identify all the matching items on a page and save them */
	pub currPos: HashScanPosData, /* current position data */
}

pub type HashScanOpaque = *mut HashScanOpaqueData;

/*
 * Definitions for metapage.
 */
pub const HASH_METAPAGE: BlockNumber = 0; /* metapage is always block 0 */

pub const HASH_MAGIC: uint32 = 0x6440640;
pub const HASH_VERSION: uint32 = 4;

pub const HASH_SPLITPOINT_PHASE_BITS: uint32 = 2;
pub const HASH_SPLITPOINT_PHASES_PER_GRP: uint32 = 1 << HASH_SPLITPOINT_PHASE_BITS;
pub const HASH_SPLITPOINT_PHASE_MASK: uint32 = HASH_SPLITPOINT_PHASES_PER_GRP - 1;
pub const HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE: uint32 = 10;

/* defines max number of splitpoint phases a hash index can have */
pub const HASH_MAX_SPLITPOINT_GROUP: uint32 = 32;
pub const HASH_MAX_SPLITPOINTS: usize = (((HASH_MAX_SPLITPOINT_GROUP
	- HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE)
	* HASH_SPLITPOINT_PHASES_PER_GRP)
	+ HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE) as usize;

/* HASH_MAX_BITMAPS = Min(BLCKSZ / 8, 1024) */
pub const HASH_MAX_BITMAPS: usize = if (BLCKSZ / 8) < 1024 {
	(BLCKSZ / 8) as usize
} else {
	1024
};

#[repr(C)]
pub struct HashMetaPageData {
	pub hashm_magic: uint32,   /* magic no. for hash tables */
	pub hashm_version: uint32, /* version ID */
	pub hashm_ntuples: f64,    /* number of tuples stored in the table */
	pub hashm_ffactor: uint16, /* target fill factor (tuples/bucket) */
	pub hashm_bsize: uint16,   /* index page size (bytes) */
	pub hashm_bmsize: uint16,  /* bitmap array size (bytes) - power of 2 */
	pub hashm_bmshift: uint16, /* log2(bitmap array size in BITS) */
	pub hashm_maxbucket: uint32, /* ID of maximum bucket in use */
	pub hashm_highmask: uint32, /* mask to modulo into entire table */
	pub hashm_lowmask: uint32, /* mask to modulo into lower half of table */
	pub hashm_ovflpoint: uint32, /* splitpoint from which ovflpage allocated */
	pub hashm_firstfree: uint32, /* lowest-number free ovflpage (bit#) */
	pub hashm_nmaps: uint32,   /* number of bitmap pages */
	pub hashm_procid: RegProcedure, /* hash function id from pg_proc */
	pub hashm_spares: [uint32; HASH_MAX_SPLITPOINTS], /* spare pages before each splitpoint */
	pub hashm_mapp: [BlockNumber; HASH_MAX_BITMAPS], /* blknos of ovfl bitmaps */
}

pub type HashMetaPage = *mut HashMetaPageData;

#[repr(C)]
pub struct HashOptions {
	pub varlena_header_: int32, /* varlena header (do not touch directly!) */
	pub fillfactor: c_int,      /* page fill factor in percent (0..100) */
}

#[inline]
pub unsafe fn HashPageGetMeta(page: Page) -> HashMetaPage {
	PageGetContents(page) as HashMetaPage
}

/*
 * page-level and high-level locking modes (see README)
 */
pub const HASH_READ: c_int = BUFFER_LOCK_SHARE;
pub const HASH_WRITE: c_int = BUFFER_LOCK_EXCLUSIVE;
pub const HASH_NOLOCK: c_int = -1;

pub const HASHSTANDARD_PROC: uint16 = 1;
pub const HASHEXTENDED_PROC: uint16 = 2;
pub const HASHOPTIONS_PROC: uint16 = 3;
pub const HASHNProcs: uint16 = 3;

/* CALC_NEW_BUCKET(old_bucket, lowmask) */
#[inline]
pub fn CALC_NEW_BUCKET(old_bucket: Bucket, lowmask: uint32) -> Bucket {
	old_bucket | (lowmask + 1)
}

// ---------------------------------------------------------------------------
// hashutil.c
// ---------------------------------------------------------------------------

/*
 * _hash_checkqual -- does the index tuple satisfy the scan conditions?
 */
pub unsafe fn _hash_checkqual(scan: IndexScanDesc, itup: IndexTuple) -> bool {
	/*
	 * Currently, we can't check any of the scan conditions since we do not
	 * have the original index entry value to supply to the sk_func. Always
	 * return true; we expect that hashgettuple already set the recheck flag
	 * to make the main indexscan code do it.
	 */
	let _ = (scan, itup);

	true
}

/*
 * _hash_datum2hashkey -- given a Datum, call the index's hash function
 *
 * The Datum is assumed to be of the index's column type, so we can use the
 * "primary" hash function that's tracked for us by the generic index code.
 */
pub unsafe fn _hash_datum2hashkey(rel: Relation, key: Datum) -> uint32 {
	let procinfo: *mut FmgrInfo;
	let collation: Oid;

	/* XXX assumes index has only one attribute */
	procinfo = index_getprocinfo(rel, 1, HASHSTANDARD_PROC);
	collation = *(*rel).rd_indcollation.offset(0);

	DatumGetUInt32(FunctionCall1Coll(procinfo, collation, key))
}

/*
 * _hash_datum2hashkey_type -- given a Datum of a specified type,
 *			hash it in a fashion compatible with this index
 *
 * This is much more expensive than _hash_datum2hashkey, so use it only in
 * cross-type situations.
 */
pub unsafe fn _hash_datum2hashkey_type(rel: Relation, key: Datum, keytype: Oid) -> uint32 {
	let hash_proc: RegProcedure;
	let collation: Oid;

	/* XXX assumes index has only one attribute */
	hash_proc = get_opfamily_proc(
		*(*rel).rd_opfamily.offset(0),
		keytype,
		keytype,
		HASHSTANDARD_PROC as c_int,
	);
	if !RegProcedureIsValid(hash_proc) {
		elog!(
			ERROR,
			"missing support function {}({},{}) for index \"{}\"",
			HASHSTANDARD_PROC,
			keytype,
			keytype,
			std::ffi::CStr::from_ptr(RelationGetRelationName(rel)).to_string_lossy()
		);
	}
	collation = *(*rel).rd_indcollation.offset(0);

	DatumGetUInt32(OidFunctionCall1Coll(hash_proc, collation, key))
}

/*
 * _hash_hashkey2bucket -- determine which bucket the hashkey maps to.
 */
pub unsafe fn _hash_hashkey2bucket(
	hashkey: uint32,
	maxbucket: uint32,
	highmask: uint32,
	lowmask: uint32,
) -> Bucket {
	let mut bucket: Bucket;

	bucket = hashkey & highmask;
	if bucket > maxbucket {
		bucket = bucket & lowmask;
	}

	bucket
}

/*
 * _hash_spareindex -- returns spare index / global splitpoint phase of the
 *					   bucket
 */
pub unsafe fn _hash_spareindex(num_bucket: uint32) -> uint32 {
	let splitpoint_group: uint32;
	let mut splitpoint_phases: uint32;

	splitpoint_group = pg_ceil_log2_32(num_bucket);

	if splitpoint_group < HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE {
		return splitpoint_group;
	}

	/* account for single-phase groups */
	splitpoint_phases = HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE;

	/* account for multi-phase groups before splitpoint_group */
	splitpoint_phases += (splitpoint_group - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE)
		<< HASH_SPLITPOINT_PHASE_BITS;

	/* account for phases within current group */
	splitpoint_phases += ((num_bucket - 1)
		>> (splitpoint_group - (HASH_SPLITPOINT_PHASE_BITS + 1)))
		& HASH_SPLITPOINT_PHASE_MASK; /* to 0-based value. */

	splitpoint_phases
}

/*
 *	_hash_get_totalbuckets -- returns total number of buckets allocated till
 *							the given splitpoint phase.
 */
pub unsafe fn _hash_get_totalbuckets(splitpoint_phase: uint32) -> uint32 {
	let mut splitpoint_group: uint32;
	let mut total_buckets: uint32;
	let phases_within_splitpoint_group: uint32;

	if splitpoint_phase < HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE {
		return 1 << splitpoint_phase;
	}

	/* get splitpoint's group */
	splitpoint_group = HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE;
	splitpoint_group += (splitpoint_phase - HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE)
		>> HASH_SPLITPOINT_PHASE_BITS;

	/* account for buckets before splitpoint_group */
	total_buckets = 1 << (splitpoint_group - 1);

	/* account for buckets within splitpoint_group */
	phases_within_splitpoint_group = ((splitpoint_phase
		- HASH_SPLITPOINT_GROUPS_WITH_ONE_PHASE)
		& HASH_SPLITPOINT_PHASE_MASK)
		+ 1; /* from 0-based to 1-based */
	total_buckets += ((1 << (splitpoint_group - 1)) >> HASH_SPLITPOINT_PHASE_BITS)
		* phases_within_splitpoint_group;

	total_buckets
}

/*
 * _hash_checkpage -- sanity checks on the format of all hash pages
 *
 * If flags is not zero, it is a bitwise OR of the acceptable page types
 * (values of hasho_flag & LH_PAGE_TYPE).
 */
pub unsafe fn _hash_checkpage(rel: Relation, buf: Buffer, flags: c_int) {
	let page: Page = BufferGetPage(buf);

	/*
	 * ReadBuffer verifies that every newly-read page passes
	 * PageHeaderIsValid, which means it either contains a reasonably sane
	 * page header or is all-zero.  We have to defend against the all-zero
	 * case, however.
	 */
	if PageIsNew(page) {
		ereport!(
			ERROR,
			"index contains unexpected zero page"
		);
		// errcode(ERRCODE_INDEX_CORRUPTED),
		// errmsg("index \"%s\" contains unexpected zero page at block %u",
		//        RelationGetRelationName(rel), BufferGetBlockNumber(buf)),
		// errhint("Please REINDEX it.")
	}

	/*
	 * Additionally check that the special area looks sane.
	 */
	if PageGetSpecialSize(page) as Size != MAXALIGN(size_of::<HashPageOpaqueData>()) {
		ereport!(
			ERROR,
			"index contains corrupted page"
		);
		// errcode(ERRCODE_INDEX_CORRUPTED),
		// errmsg("index \"%s\" contains corrupted page at block %u", ...),
		// errhint("Please REINDEX it.")
	}

	if flags != 0 {
		let opaque: HashPageOpaque = HashPageGetOpaque(page);

		if ((*opaque).hasho_flag as c_int & flags) == 0 {
			ereport!(
				ERROR,
				"index contains corrupted page"
			);
			// errcode(ERRCODE_INDEX_CORRUPTED),
			// errmsg("index \"%s\" contains corrupted page at block %u", ...),
			// errhint("Please REINDEX it.")
		}
	}

	/*
	 * When checking the metapage, also verify magic number and version.
	 */
	if flags == LH_META_PAGE {
		let metap: HashMetaPage = HashPageGetMeta(page);

		if (*metap).hashm_magic != HASH_MAGIC {
			ereport!(
				ERROR,
				"index is not a hash index"
			);
			// errcode(ERRCODE_INDEX_CORRUPTED),
			// errmsg("index \"%s\" is not a hash index", ...)
		}

		if (*metap).hashm_version != HASH_VERSION {
			ereport!(
				ERROR,
				"index has wrong hash version"
			);
			// errcode(ERRCODE_INDEX_CORRUPTED),
			// errmsg("index \"%s\" has wrong hash version", ...),
			// errhint("Please REINDEX it.")
		}
	}
}

pub unsafe fn hashoptions(reloptions: Datum, validate: bool) -> *mut bytea {
	const tab: [relopt_parse_elt; 1] = [relopt_parse_elt {
		optname: c"fillfactor".as_ptr(),
		opttype: RELOPT_TYPE_INT,
		offset: core::mem::offset_of!(HashOptions, fillfactor) as c_int,
	}];

	build_reloptions(
		reloptions,
		validate,
		RELOPT_KIND_HASH,
		size_of::<HashOptions>(),
		tab.as_ptr(),
		lengthof!(tab) as c_int,
	) as *mut bytea
}

/*
 * _hash_get_indextuple_hashkey - get the hash index tuple's hash key value
 */
pub unsafe fn _hash_get_indextuple_hashkey(itup: IndexTuple) -> uint32 {
	let attp: *mut c_char;

	/*
	 * We assume the hash key is the first attribute and can't be null, so
	 * this can be done crudely but very very cheaply ...
	 */
	attp = (itup as *mut c_char).offset(IndexInfoFindDataOffset((*itup).t_info) as isize);
	*(attp as *mut uint32)
}

/*
 * _hash_convert_tuple - convert raw index data to hash key
 *
 * Inputs: values and isnull arrays for the user data column(s)
 * Outputs: values and isnull arrays for the index tuple, suitable for
 *		passing to index_form_tuple().
 *
 * Returns true if successful, false if not (because there are null values).
 * On a false result, the given data need not be indexed.
 *
 * Note: callers know that the index-column arrays are always of length 1.
 * In principle, there could be more than one input column, though we do not
 * currently support that.
 */
pub unsafe fn _hash_convert_tuple(
	index: Relation,
	user_values: *mut Datum,
	user_isnull: *mut bool,
	index_values: *mut Datum,
	index_isnull: *mut bool,
) -> bool {
	let hashkey: uint32;

	/*
	 * We do not insert null values into hash indexes.  This is okay because
	 * the only supported search operator is '=', and we assume it is strict.
	 */
	if *user_isnull.offset(0) {
		return false;
	}

	hashkey = _hash_datum2hashkey(index, *user_values.offset(0));
	*index_values.offset(0) = UInt32GetDatum(hashkey);
	*index_isnull.offset(0) = false;
	true
}

/*
 * _hash_binsearch - Return the offset number in the page where the
 *					 specified hash value should be sought or inserted.
 *
 * We use binary search, relying on the assumption that the existing entries
 * are ordered by hash key.
 */
pub unsafe fn _hash_binsearch(page: Page, hash_value: uint32) -> OffsetNumber {
	let mut upper: OffsetNumber;
	let mut lower: OffsetNumber;

	/* Loop invariant: lower <= desired place <= upper */
	upper = PageGetMaxOffsetNumber(page) + 1;
	lower = FirstOffsetNumber;

	while upper > lower {
		let off: OffsetNumber;
		let itup: IndexTuple;
		let hashkey: uint32;

		off = (upper + lower) / 2;
		Assert!(OffsetNumberIsValid(off));

		itup = PageGetItem(page, PageGetItemId(page, off)) as IndexTuple;
		hashkey = _hash_get_indextuple_hashkey(itup);
		if hashkey < hash_value {
			lower = off + 1;
		} else {
			upper = off;
		}
	}

	lower
}

/*
 * _hash_binsearch_last
 *
 * Same as above, except that if there are multiple matching items in the
 * page, we return the offset of the last one instead of the first one,
 * and the possible range of outputs is 0..maxoffset not 1..maxoffset+1.
 * This is handy for starting a new page in a backwards scan.
 */
pub unsafe fn _hash_binsearch_last(page: Page, hash_value: uint32) -> OffsetNumber {
	let mut upper: OffsetNumber;
	let mut lower: OffsetNumber;

	/* Loop invariant: lower <= desired place <= upper */
	upper = PageGetMaxOffsetNumber(page);
	lower = FirstOffsetNumber - 1;

	while upper > lower {
		let itup: IndexTuple;
		let off: OffsetNumber;
		let hashkey: uint32;

		off = (upper + lower + 1) / 2;
		Assert!(OffsetNumberIsValid(off));

		itup = PageGetItem(page, PageGetItemId(page, off)) as IndexTuple;
		hashkey = _hash_get_indextuple_hashkey(itup);
		if hashkey > hash_value {
			upper = off - 1;
		} else {
			lower = off;
		}
	}

	lower
}

/*
 *	_hash_get_oldblock_from_newbucket() -- get the block number of a bucket
 *			from which current (new) bucket is being split.
 */
pub unsafe fn _hash_get_oldblock_from_newbucket(rel: Relation, new_bucket: Bucket) -> BlockNumber {
	let old_bucket: Bucket;
	let mask: uint32;
	let metabuf: Buffer;
	let metap: HashMetaPage;
	let blkno: BlockNumber;

	/*
	 * To get the old bucket from the current bucket, we need a mask to modulo
	 * into lower half of table.  This mask is stored in meta page as
	 * hashm_lowmask, but here we can't rely on the same, because we need a
	 * value of lowmask that was prevalent at the time when bucket split was
	 * started.  Masking the most significant bit of new bucket would give us
	 * old bucket.
	 */
	mask = ((1 as uint32) << pg_leftmost_one_pos32(new_bucket)) - 1;
	old_bucket = new_bucket & mask;

	metabuf = _hash_getbuf(rel, HASH_METAPAGE, HASH_READ, LH_META_PAGE);
	metap = HashPageGetMeta(BufferGetPage(metabuf));

	blkno = BUCKET_TO_BLKNO(metap, old_bucket);

	_hash_relbuf(rel, metabuf);

	blkno
}

/*
 *	_hash_get_newblock_from_oldbucket() -- get the block number of a bucket
 *			that will be generated after split from old bucket.
 */
pub unsafe fn _hash_get_newblock_from_oldbucket(rel: Relation, old_bucket: Bucket) -> BlockNumber {
	let new_bucket: Bucket;
	let metabuf: Buffer;
	let metap: HashMetaPage;
	let blkno: BlockNumber;

	metabuf = _hash_getbuf(rel, HASH_METAPAGE, HASH_READ, LH_META_PAGE);
	metap = HashPageGetMeta(BufferGetPage(metabuf));

	new_bucket = _hash_get_newbucket_from_oldbucket(
		rel,
		old_bucket,
		(*metap).hashm_lowmask,
		(*metap).hashm_maxbucket,
	);
	blkno = BUCKET_TO_BLKNO(metap, new_bucket);

	_hash_relbuf(rel, metabuf);

	blkno
}

/*
 *	_hash_get_newbucket_from_oldbucket() -- get the new bucket that will be
 *			generated after split from current (old) bucket.
 */
pub unsafe fn _hash_get_newbucket_from_oldbucket(
	rel: Relation,
	old_bucket: Bucket,
	mut lowmask: uint32,
	maxbucket: uint32,
) -> Bucket {
	let mut new_bucket: Bucket;
	let _ = rel;

	new_bucket = CALC_NEW_BUCKET(old_bucket, lowmask);
	if new_bucket > maxbucket {
		lowmask = lowmask >> 1;
		new_bucket = CALC_NEW_BUCKET(old_bucket, lowmask);
	}

	new_bucket
}

/*
 * _hash_kill_items - set LP_DEAD state for items an indexscan caller has
 * told us were killed.
 */
pub unsafe fn _hash_kill_items(scan: IndexScanDesc) {
	let so: HashScanOpaque = (*scan).opaque as HashScanOpaque;
	let rel: Relation = (*scan).indexRelation;
	let blkno: BlockNumber;
	let buf: Buffer;
	let page: Page;
	let opaque: HashPageOpaque;
	let mut offnum: OffsetNumber;
	let maxoff: OffsetNumber;
	let numKilled: c_int = (*so).numKilled;
	let mut i: c_int;
	let mut killedsomething: bool = false;
	let mut havePin: bool = false;

	Assert!((*so).numKilled > 0);
	Assert!(!(*so).killedItems.is_null());
	Assert!(HashScanPosIsValid(&(*so).currPos));

	/*
	 * Always reset the scan state, so we don't look for same items on other
	 * pages.
	 */
	(*so).numKilled = 0;

	blkno = (*so).currPos.currPage;
	if HashScanPosIsPinned(&(*so).currPos) {
		/*
		 * We already have pin on this buffer, so, all we need to do is
		 * acquire lock on it.
		 */
		havePin = true;
		buf = (*so).currPos.buf;
		LockBuffer(buf, BUFFER_LOCK_SHARE);
	} else {
		buf = _hash_getbuf(rel, blkno, HASH_READ, LH_OVERFLOW_PAGE);
	}

	page = BufferGetPage(buf);
	opaque = HashPageGetOpaque(page);
	maxoff = PageGetMaxOffsetNumber(page);

	i = 0;
	while i < numKilled {
		let itemIndex: c_int = *(*so).killedItems.offset(i as isize);
		let currItem: *mut HashScanPosItem = &mut (*so).currPos.items[itemIndex as usize];

		offnum = (*currItem).indexOffset;

		Assert!(
			itemIndex >= (*so).currPos.firstItem && itemIndex <= (*so).currPos.lastItem
		);

		while offnum <= maxoff {
			let iid: ItemId = PageGetItemId(page, offnum);
			let ituple: IndexTuple = PageGetItem(page, iid) as IndexTuple;

			if ItemPointerEquals(&mut (*ituple).t_tid, &mut (*currItem).heapTid) {
				/* found the item */
				ItemIdMarkDead(iid);
				killedsomething = true;
				break; /* out of inner search loop */
			}
			offnum = OffsetNumberNext(offnum);
		}

		i += 1;
	}

	/*
	 * Since this can be redone later if needed, mark as dirty hint. Whenever
	 * we mark anything LP_DEAD, we also set the page's
	 * LH_PAGE_HAS_DEAD_TUPLES flag, which is likewise just a hint.
	 */
	if killedsomething {
		(*opaque).hasho_flag |= LH_PAGE_HAS_DEAD_TUPLES as uint16;
		MarkBufferDirtyHint(buf, true);
	}

	if (*so).hashso_bucket_buf == (*so).currPos.buf || havePin {
		LockBuffer((*so).currPos.buf, BUFFER_LOCK_UNLOCK);
	} else {
		_hash_relbuf(rel, buf);
	}
}

// ---------------------------------------------------------------------------
// Local stubs for unported helpers
// ---------------------------------------------------------------------------

// reloptions.h types/values - no Rust port of access/common/reloptions.c yet.
pub type relopt_kind = c_int;
pub const RELOPT_KIND_HASH: relopt_kind = 1 << 4;
pub type relopt_type = c_int;
pub const RELOPT_TYPE_INT: relopt_type = 1;

#[repr(C)]
pub struct relopt_parse_elt {
	pub optname: *const c_char,  /* option's name */
	pub opttype: relopt_type,    /* option's datatype */
	pub offset: c_int,           /* offset of field in result struct */
}

unsafe fn index_getprocinfo(irel: Relation, attnum: AttrNumber, procnum: uint16) -> *mut FmgrInfo { crate::access::index::indexam::index_getprocinfo(irel, attnum, procnum) }

unsafe fn get_opfamily_proc(opfamily: Oid, lefttype: Oid, righttype: Oid, procnum: c_int) -> RegProcedure {
	unimplemented!() // TODO: utils/cache/lsyscache.c
}

unsafe fn build_reloptions(
	reloptions: Datum,
	validate: bool,
	kind: relopt_kind,
	relopt_struct_size: Size,
	relopt_elems: *const relopt_parse_elt,
	num_relopt_elems: c_int,
) -> *mut c_void {
	crate::access::common::reloptions::build_reloptions(
		reloptions, validate, kind as _, relopt_struct_size, relopt_elems as _, num_relopt_elems,
	)
}

unsafe fn _hash_getbuf(rel: Relation, blkno: BlockNumber, access: c_int, flags: c_int) -> Buffer { unimplemented!() }

unsafe fn _hash_relbuf(rel: Relation, buf: Buffer) { unimplemented!() }
