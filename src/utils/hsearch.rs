//! Translated from PostgreSQL src/include/utils/hsearch.h
//! dynahash API mapped HashMap-style: find/enter/remove become methods;
//! the foundPtr out-param and HASHACTION selector disappear (see function-mapping).

use bitflags::bitflags;

/// Hash function signature: (key bytes) -> hash.
pub type HashValueFunc = fn(&[u8]) -> u32;
/// Key comparison: zero for match, nonzero for no match.
pub type HashCompareFunc = fn(&[u8], &[u8]) -> i32;
/// Key copying: dest, src -> dest (return value unused in C).
pub type HashCopyFunc = fn(&mut [u8], &[u8]);
/// Allocator matching malloc().
pub type HashAllocFunc = fn(usize) -> *mut u8;

/// Parameter data structure for hash_create; only fields named by flags are set.
pub struct HASHCTL {
    pub num_partitions: i64,
    pub ssize: i64,
    pub dsize: i64,
    pub max_dsize: i64,
    pub keysize: usize,
    pub entrysize: usize,
    pub hash: Option<HashValueFunc>,
    pub r#match: Option<HashCompareFunc>,
    pub keycopy: Option<HashCopyFunc>,
    pub alloc: Option<HashAllocFunc>,
    // hcxt (MemoryContext) and hctl (shared-mem header) drop under single-process model.
}

bitflags! {
    /// Flag bits for hash_create; most indicate which parameters are supplied.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct HashFlags: i32 {
        const PARTITION  = 0x0001;
        const SEGMENT    = 0x0002;
        const DIRSIZE    = 0x0004;
        const ELEM       = 0x0008;
        const STRINGS    = 0x0010;
        const BLOBS      = 0x0020;
        const FUNCTION   = 0x0040;
        const COMPARE    = 0x0080;
        const KEYCOPY    = 0x0100;
        const ALLOC      = 0x0200;
        const CONTEXT    = 0x0400;
        const SHARED_MEM = 0x0800;
        const ATTACH     = 0x1000;
        const FIXED_SIZE = 0x2000;
    }
}

/// max_dsize value to indicate expansible directory.
pub const NO_MAX_DSIZE: i64 = -1;

/// Hash table control struct; opaque in C, a HashMap-backed type here.
pub struct HTAB;

impl HTAB {
    /// hash_create.
    pub fn hash_create(tabname: &str, nelem: i64, info: &HASHCTL, flags: HashFlags) -> Box<Self> {
        let _ = (tabname, nelem, info, flags);
        unimplemented!()
    }

    pub fn hash_stats(&self, whence: &str) {
        let _ = whence;
        unimplemented!()
    }

    /// HASH_FIND.
    pub fn get(&self, key: &[u8]) -> Option<*const u8> {
        let _ = key;
        unimplemented!()
    }

    /// HASH_ENTER (insert-or-get).
    pub fn entry(&mut self, key: &[u8]) -> *mut u8 {
        let _ = key;
        unimplemented!()
    }

    /// HASH_ENTER_NULL: None if allocation fails.
    pub fn entry_null(&mut self, key: &[u8]) -> Option<*mut u8> {
        let _ = key;
        unimplemented!()
    }

    /// HASH_REMOVE.
    pub fn remove(&mut self, key: &[u8]) -> Option<*mut u8> {
        let _ = key;
        unimplemented!()
    }

    pub fn get_hash_value(&self, key: &[u8]) -> u32 {
        let _ = key;
        unimplemented!()
    }

    pub fn get_with_hash_value(&self, key: &[u8], hashvalue: u32) -> Option<*const u8> {
        let _ = (key, hashvalue);
        unimplemented!()
    }

    pub fn entry_with_hash_value(&mut self, key: &[u8], hashvalue: u32) -> *mut u8 {
        let _ = (key, hashvalue);
        unimplemented!()
    }

    pub fn update_hash_key(&mut self, existing_entry: *mut u8, new_key: &[u8]) -> bool {
        let _ = (existing_entry, new_key);
        unimplemented!()
    }

    pub fn get_num_entries(&self) -> i64 {
        unimplemented!()
    }

    pub fn freeze(&mut self) {
        unimplemented!()
    }
}

/// hash_seq scan status (opaque to callers).
pub struct HashSeqStatus;

impl HashSeqStatus {
    pub fn init(hashp: &HTAB) -> Self {
        let _ = hashp;
        unimplemented!()
    }

    pub fn init_with_hash_value(hashp: &HTAB, hashvalue: u32) -> Self {
        let _ = (hashp, hashvalue);
        unimplemented!()
    }

    /// Returns the next entry, or None at end of scan.
    #[allow(clippy::should_implement_trait, reason = "inherent method mirrors PG hash_search API name")]
    pub fn next(&mut self) -> Option<*mut u8> {
        unimplemented!()
    }

    pub fn term(&mut self) {
        unimplemented!()
    }
}

pub fn hash_estimate_size(num_entries: i64, entrysize: usize) -> usize {
    let _ = (num_entries, entrysize);
    unimplemented!()
}

pub fn hash_select_dirsize(num_entries: i64) -> i64 {
    let _ = num_entries;
    unimplemented!()
}

pub fn hash_get_shared_size(info: &HASHCTL, flags: HashFlags) -> usize {
    let _ = (info, flags);
    unimplemented!()
}

pub fn at_eo_xact_hash_tables(is_commit: bool) {
    let _ = is_commit;
    unimplemented!()
}

pub fn at_eo_sub_xact_hash_tables(is_commit: bool, nest_depth: i32) {
    let _ = (is_commit, nest_depth);
    unimplemented!()
}
