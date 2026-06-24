//! Translated from PostgreSQL src/include/tsearch/ts_cache.h
//! Tsearch related object caches.

use crate::fmgr::FmgrInfo;
use crate::postgres_ext::Oid;
use crate::utils::palloc::MemoryContext;

/// Common header shared by all TS*CacheEntry structs. In-memory.
pub struct TSAnyCacheEntry {
    pub obj_id: Oid,
    pub isvalid: bool,
}

/// Parser cache entry. (`prsId` is the hash key.) In-memory.
pub struct TSParserCacheEntry {
    pub prs_id: Oid,
    pub isvalid: bool,
    pub start_oid: Oid,
    pub token_oid: Oid,
    pub end_oid: Oid,
    pub headline_oid: Oid,
    pub lextype_oid: Oid,
    pub prsstart: FmgrInfo,
    pub prstoken: FmgrInfo,
    pub prsend: FmgrInfo,
    pub prsheadline: FmgrInfo,
}

/// Dictionary cache entry. (`dictId` is the hash key.) In-memory.
pub struct TSDictionaryCacheEntry {
    pub dict_id: Oid,
    pub isvalid: bool,
    pub lexize_oid: Oid,
    pub lexize: FmgrInfo,
    pub dict_ctx: MemoryContext,                  // private-data memory context
    pub dict_data: Option<Box<dyn core::any::Any>>, // C void* private data
}

/// C: anonymous struct - a dictionary-id list. The `Oid *dictIds`/`int len`
/// pair becomes a `Vec`.
pub struct ListDictionary {
    pub dict_ids: Vec<Oid>,
}

/// Configuration cache entry. (`cfgId` is the hash key.) In-memory.
pub struct TSConfigCacheEntry {
    pub cfg_id: Oid,
    pub isvalid: bool,
    pub prs_id: Oid,
    pub map: Vec<ListDictionary>, // lenmap = map.len()
}

// GUC for current configuration. TODO(global)
pub static mut TSCurrentConfig: Option<String> = None;

pub fn lookup_ts_parser_cache(prs_id: Oid) -> Option<Box<TSParserCacheEntry>> {
    unimplemented!()
}
pub fn lookup_ts_dictionary_cache(dict_id: Oid) -> Option<Box<TSDictionaryCacheEntry>> {
    unimplemented!()
}
pub fn lookup_ts_config_cache(cfg_id: Oid) -> Option<Box<TSConfigCacheEntry>> {
    unimplemented!()
}

/// C: `Oid getTSCurrentConfig(bool emitError)` - InvalidOid sentinel -> None.
pub fn getTSCurrentConfig(emit_error: bool) -> Option<Oid> {
    unimplemented!()
}
