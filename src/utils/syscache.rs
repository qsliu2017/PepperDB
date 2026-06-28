//! Translated from PostgreSQL src/include/utils/syscache.h
//! System catalog cache definitions.
//! See also lsyscache.rs for convenience cache-lookup routines.

use crate::access::attnum::AttrNumber;
use crate::access::htup::HeapTupleData;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::catcache::CatCList;

// HeapTuple-family pointer alias (htup.h does not export it yet). An invalid
// (NULL) HeapTuple is the "not found" sentinel, modeled as Option<HeapTuple>::None.
pub type HeapTuple = *mut HeapTupleData; // TODO(ptr)

/// System cache identifiers (the generated `catalog/syscache_ids.h`): the
/// alphabetically-sorted set of `MAKE_SYSCACHE` declarations across the catalog
/// headers. Hand-maintained here because those annotations live in the catalog
/// `.h` headers (not a `.dat`); the discriminant VALUES MATTER -- they index the
/// cache array, so keep the order and start at 0.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SysCacheIdentifier {
    AGGFNOID = 0,
    AMNAME,
    AMOID,
    AMOPOPID,
    AMOPSTRATEGY,
    AMPROCNUM,
    ATTNAME,
    ATTNUM,
    AUTHMEMMEMROLE,
    AUTHMEMROLEMEM,
    AUTHNAME,
    AUTHOID,
    CASTSOURCETARGET,
    CLAAMNAMENSP,
    CLAOID,
    COLLNAMEENCNSP,
    COLLOID,
    CONDEFAULT,
    CONNAMENSP,
    CONSTROID,
    CONVOID,
    DATABASEOID,
    DEFACLROLENSPOBJ,
    ENUMOID,
    ENUMTYPOIDNAME,
    EVENTTRIGGERNAME,
    EVENTTRIGGEROID,
    EXTENSIONNAME,
    EXTENSIONOID,
    FOREIGNDATAWRAPPERNAME,
    FOREIGNDATAWRAPPEROID,
    FOREIGNSERVERNAME,
    FOREIGNSERVEROID,
    FOREIGNTABLEREL,
    INDEXRELID,
    LANGNAME,
    LANGOID,
    NAMESPACENAME,
    NAMESPACEOID,
    OPERNAMENSP,
    OPEROID,
    OPFAMILYAMNAMENSP,
    OPFAMILYOID,
    PARAMETERACLNAME,
    PARAMETERACLOID,
    PARTRELID,
    PROCNAMEARGSNSP,
    PROCOID,
    PUBLICATIONNAME,
    PUBLICATIONNAMESPACE,
    PUBLICATIONNAMESPACEMAP,
    PUBLICATIONOID,
    PUBLICATIONREL,
    PUBLICATIONRELMAP,
    RANGEMULTIRANGE,
    RANGETYPE,
    RELNAMENSP,
    RELOID,
    REPLORIGIDENT,
    REPLORIGNAME,
    RULERELNAME,
    SEQRELID,
    STATEXTDATASTXOID,
    STATEXTNAMENSP,
    STATEXTOID,
    STATRELATTINH,
    SUBSCRIPTIONNAME,
    SUBSCRIPTIONOID,
    SUBSCRIPTIONRELMAP,
    TABLESPACEOID,
    TRFOID,
    TRFTYPELANG,
    TSCONFIGMAP,
    TSCONFIGNAMENSP,
    TSCONFIGOID,
    TSDICTNAMENSP,
    TSDICTOID,
    TSPARSERNAMENSP,
    TSPARSEROID,
    TSTEMPLATENAMENSP,
    TSTEMPLATEOID,
    TYPENAMENSP,
    TYPEOID,
    USERMAPPINGOID,
    USERMAPPINGUSERSERVER,
}

/// C: `#define SysCacheSize (lastcache + 1)`.
pub const SYSCACHE_SIZE: usize = SysCacheIdentifier::USERMAPPINGUSERSERVER as usize + 1;

pub fn InitCatalogCache() {
    unimplemented!()
}

pub fn InitCatalogCachePhase2() {
    unimplemented!()
}

// Search routines. SYNC (hit-only) bodies in
// crate::backend::utils::cache::syscache (step 14); the async warm path is
// search_sys_cache_populate. An invalid HeapTuple ("not found") -> None.
pub fn SearchSysCache(
    cache_id: SysCacheIdentifier,
    key1: Datum,
    key2: Datum,
    key3: Datum,
    key4: Datum,
) -> Option<HeapTuple> {
    crate::backend::utils::cache::syscache::search_sys_cache(cache_id, &[key1, key2, key3, key4])
}

// Argument-specific variants are preferred (faster, key-count insulated).
pub fn SearchSysCache1(cache_id: SysCacheIdentifier, key1: Datum) -> Option<HeapTuple> {
    crate::backend::utils::cache::syscache::search_sys_cache(cache_id, &[key1])
}

pub fn SearchSysCache2(cache_id: SysCacheIdentifier, key1: Datum, key2: Datum) -> Option<HeapTuple> {
    crate::backend::utils::cache::syscache::search_sys_cache(cache_id, &[key1, key2])
}

pub fn SearchSysCache3(
    cache_id: SysCacheIdentifier,
    key1: Datum,
    key2: Datum,
    key3: Datum,
) -> Option<HeapTuple> {
    crate::backend::utils::cache::syscache::search_sys_cache(cache_id, &[key1, key2, key3])
}

pub fn SearchSysCache4(
    cache_id: SysCacheIdentifier,
    key1: Datum,
    key2: Datum,
    key3: Datum,
    key4: Datum,
) -> Option<HeapTuple> {
    crate::backend::utils::cache::syscache::search_sys_cache(cache_id, &[key1, key2, key3, key4])
}

pub fn ReleaseSysCache(tuple: HeapTuple) {
    crate::backend::utils::cache::syscache::release_sys_cache(tuple);
}

pub fn SearchSysCacheLocked1(_cache_id: SysCacheIdentifier, _key1: Datum) -> Option<HeapTuple> {
    unimplemented!()
}

// Convenience routines.
pub fn SearchSysCacheCopy(
    _cache_id: SysCacheIdentifier,
    _key1: Datum,
    _key2: Datum,
    _key3: Datum,
    _key4: Datum,
) -> Option<HeapTuple> {
    unimplemented!()
}

pub fn SearchSysCacheLockedCopy1(
    _cache_id: SysCacheIdentifier,
    _key1: Datum,
) -> Option<HeapTuple> {
    unimplemented!()
}

pub fn SearchSysCacheExists(
    cache_id: SysCacheIdentifier,
    key1: Datum,
    key2: Datum,
    key3: Datum,
    key4: Datum,
) -> bool {
    crate::backend::utils::cache::syscache::search_sys_cache_exists(
        cache_id,
        &[key1, key2, key3, key4],
    )
}

/// OID lookup. InvalidOid sentinel -> None.
pub fn GetSysCacheOid(
    cache_id: SysCacheIdentifier,
    oidcol: AttrNumber,
    key1: Datum,
    key2: Datum,
    key3: Datum,
    key4: Datum,
) -> Option<Oid> {
    crate::backend::utils::cache::syscache::get_sys_cache_oid(
        cache_id,
        oidcol,
        &[key1, key2, key3, key4],
    )
}

pub fn SearchSysCacheAttName(_relid: Oid, _attname: &str) -> Option<HeapTuple> {
    unimplemented!()
}

pub fn SearchSysCacheCopyAttName(_relid: Oid, _attname: &str) -> Option<HeapTuple> {
    unimplemented!()
}

pub fn SearchSysCacheExistsAttName(_relid: Oid, _attname: &str) -> bool {
    unimplemented!()
}

pub fn SearchSysCacheAttNum(_relid: Oid, _attnum: i16) -> Option<HeapTuple> {
    unimplemented!()
}

pub fn SearchSysCacheCopyAttNum(_relid: Oid, _attnum: i16) -> Option<HeapTuple> {
    unimplemented!()
}

/// Get a cached tuple attribute. The `bool *isNull` out-param folds into Option.
pub fn SysCacheGetAttr(
    _cache_id: SysCacheIdentifier,
    _tup: HeapTuple,
    _attribute_number: AttrNumber,
) -> Option<Datum> {
    unimplemented!()
}

pub fn SysCacheGetAttrNotNull(
    _cache_id: SysCacheIdentifier,
    _tup: HeapTuple,
    _attribute_number: AttrNumber,
) -> Datum {
    unimplemented!()
}

pub fn GetSysCacheHashValue(
    _cache_id: SysCacheIdentifier,
    _key1: Datum,
    _key2: Datum,
    _key3: Datum,
    _key4: Datum,
) -> u32 {
    unimplemented!()
}

/// List-search interface. Callers must also use catcache.h.
pub fn SearchSysCacheList(
    _cache_id: SysCacheIdentifier,
    _nkeys: i32,
    _key1: Datum,
    _key2: Datum,
    _key3: Datum,
) -> *mut CatCList {
    unimplemented!()
}

pub fn SysCacheInvalidate(_cache_id: SysCacheIdentifier, _hash_value: u32) {
    unimplemented!()
}

pub fn RelationInvalidatesSnapshotsOnly(_relid: Oid) -> bool {
    unimplemented!()
}

pub fn RelationHasSysCache(_relid: Oid) -> bool {
    unimplemented!()
}

pub fn RelationSupportsSysCache(_relid: Oid) -> bool {
    unimplemented!()
}

// The C key-count-specific macros (SearchSysCacheCopyN / SearchSysCacheExistsN /
// GetSysCacheOidN / GetSysCacheHashValueN) just pad missing keys with 0 and call
// the base fn. Provide the small-arity wrappers directly.

pub fn SearchSysCacheCopy1(cache_id: SysCacheIdentifier, key1: Datum) -> Option<HeapTuple> {
    SearchSysCacheCopy(cache_id, key1, Datum(0), Datum(0), Datum(0))
}

pub fn SearchSysCacheCopy2(
    cache_id: SysCacheIdentifier,
    key1: Datum,
    key2: Datum,
) -> Option<HeapTuple> {
    SearchSysCacheCopy(cache_id, key1, key2, Datum(0), Datum(0))
}

pub fn SearchSysCacheCopy3(
    cache_id: SysCacheIdentifier,
    key1: Datum,
    key2: Datum,
    key3: Datum,
) -> Option<HeapTuple> {
    SearchSysCacheCopy(cache_id, key1, key2, key3, Datum(0))
}

pub fn SearchSysCacheCopy4(
    cache_id: SysCacheIdentifier,
    key1: Datum,
    key2: Datum,
    key3: Datum,
    key4: Datum,
) -> Option<HeapTuple> {
    SearchSysCacheCopy(cache_id, key1, key2, key3, key4)
}

pub fn SearchSysCacheExists1(cache_id: SysCacheIdentifier, key1: Datum) -> bool {
    SearchSysCacheExists(cache_id, key1, Datum(0), Datum(0), Datum(0))
}

pub fn SearchSysCacheExists2(cache_id: SysCacheIdentifier, key1: Datum, key2: Datum) -> bool {
    SearchSysCacheExists(cache_id, key1, key2, Datum(0), Datum(0))
}

pub fn SearchSysCacheExists3(
    cache_id: SysCacheIdentifier,
    key1: Datum,
    key2: Datum,
    key3: Datum,
) -> bool {
    SearchSysCacheExists(cache_id, key1, key2, key3, Datum(0))
}

pub fn SearchSysCacheExists4(
    cache_id: SysCacheIdentifier,
    key1: Datum,
    key2: Datum,
    key3: Datum,
    key4: Datum,
) -> bool {
    SearchSysCacheExists(cache_id, key1, key2, key3, key4)
}

pub fn GetSysCacheOid1(
    cache_id: SysCacheIdentifier,
    oidcol: AttrNumber,
    key1: Datum,
) -> Option<Oid> {
    GetSysCacheOid(cache_id, oidcol, key1, Datum(0), Datum(0), Datum(0))
}

pub fn GetSysCacheOid2(
    cache_id: SysCacheIdentifier,
    oidcol: AttrNumber,
    key1: Datum,
    key2: Datum,
) -> Option<Oid> {
    GetSysCacheOid(cache_id, oidcol, key1, key2, Datum(0), Datum(0))
}

pub fn GetSysCacheOid3(
    cache_id: SysCacheIdentifier,
    oidcol: AttrNumber,
    key1: Datum,
    key2: Datum,
    key3: Datum,
) -> Option<Oid> {
    GetSysCacheOid(cache_id, oidcol, key1, key2, key3, Datum(0))
}

pub fn GetSysCacheOid4(
    cache_id: SysCacheIdentifier,
    oidcol: AttrNumber,
    key1: Datum,
    key2: Datum,
    key3: Datum,
    key4: Datum,
) -> Option<Oid> {
    GetSysCacheOid(cache_id, oidcol, key1, key2, key3, key4)
}

pub fn GetSysCacheHashValue1(cache_id: SysCacheIdentifier, key1: Datum) -> u32 {
    GetSysCacheHashValue(cache_id, key1, Datum(0), Datum(0), Datum(0))
}

pub fn GetSysCacheHashValue2(cache_id: SysCacheIdentifier, key1: Datum, key2: Datum) -> u32 {
    GetSysCacheHashValue(cache_id, key1, key2, Datum(0), Datum(0))
}

pub fn GetSysCacheHashValue3(
    cache_id: SysCacheIdentifier,
    key1: Datum,
    key2: Datum,
    key3: Datum,
) -> u32 {
    GetSysCacheHashValue(cache_id, key1, key2, key3, Datum(0))
}

pub fn GetSysCacheHashValue4(
    cache_id: SysCacheIdentifier,
    key1: Datum,
    key2: Datum,
    key3: Datum,
    key4: Datum,
) -> u32 {
    GetSysCacheHashValue(cache_id, key1, key2, key3, key4)
}

// SearchSysCacheListN -> SearchSysCacheList stubs (require catcache.h too).
pub fn SearchSysCacheList1(cache_id: SysCacheIdentifier, key1: Datum) -> *mut CatCList {
    SearchSysCacheList(cache_id, 1, key1, Datum(0), Datum(0))
}

pub fn SearchSysCacheList2(
    cache_id: SysCacheIdentifier,
    key1: Datum,
    key2: Datum,
) -> *mut CatCList {
    SearchSysCacheList(cache_id, 2, key1, key2, Datum(0))
}

pub fn SearchSysCacheList3(
    cache_id: SysCacheIdentifier,
    key1: Datum,
    key2: Datum,
    key3: Datum,
) -> *mut CatCList {
    SearchSysCacheList(cache_id, 3, key1, key2, key3)
}
