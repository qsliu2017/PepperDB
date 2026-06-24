//! Translated from PostgreSQL src/include/utils/syscache.h
//! System catalog cache definitions.
//! See also lsyscache.rs for convenience cache-lookup routines.

use crate::access::attnum::AttrNumber;
use crate::access::htup::HeapTupleData;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

// HeapTuple-family pointer alias (htup.h does not export it yet). An invalid
// (NULL) HeapTuple is the "not found" sentinel, modeled as Option<HeapTuple>::None.
pub type HeapTuple = *mut HeapTupleData; // TODO(ptr)

/// System cache identifiers. Hand-maintained list mirroring the generated
/// `enum SysCacheIdentifier` (catalog/syscache_ids.h): the alphabetically-sorted
/// set of MAKE_SYSCACHE declarations across the catalog headers. The discriminant
/// VALUES MATTER -- they index the cache array, so keep order and start at 0.
/// TODO(catalog-derive): regenerate from MAKE_SYSCACHE via build.rs.
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

// Search routines. An invalid HeapTuple ("not found") -> None per function-mapping.
pub fn SearchSysCache(
    _cache_id: SysCacheIdentifier,
    _key1: Datum,
    _key2: Datum,
    _key3: Datum,
    _key4: Datum,
) -> Option<HeapTuple> {
    unimplemented!()
}

// Argument-specific variants are preferred (faster, key-count insulated).
pub fn SearchSysCache1(_cache_id: SysCacheIdentifier, _key1: Datum) -> Option<HeapTuple> {
    unimplemented!()
}

pub fn SearchSysCache2(
    _cache_id: SysCacheIdentifier,
    _key1: Datum,
    _key2: Datum,
) -> Option<HeapTuple> {
    unimplemented!()
}

pub fn SearchSysCache3(
    _cache_id: SysCacheIdentifier,
    _key1: Datum,
    _key2: Datum,
    _key3: Datum,
) -> Option<HeapTuple> {
    unimplemented!()
}

pub fn SearchSysCache4(
    _cache_id: SysCacheIdentifier,
    _key1: Datum,
    _key2: Datum,
    _key3: Datum,
    _key4: Datum,
) -> Option<HeapTuple> {
    unimplemented!()
}

pub fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!()
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
    _cache_id: SysCacheIdentifier,
    _key1: Datum,
    _key2: Datum,
    _key3: Datum,
    _key4: Datum,
) -> bool {
    unimplemented!()
}

/// OID lookup. InvalidOid sentinel -> None.
pub fn GetSysCacheOid(
    _cache_id: SysCacheIdentifier,
    _oidcol: AttrNumber,
    _key1: Datum,
    _key2: Datum,
    _key3: Datum,
    _key4: Datum,
) -> Option<Oid> {
    unimplemented!()
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

// catclist's definition lives in utils/catcache.h (intentionally not included
// here). Rule 7: opaque local placeholder, repointed in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::utils::catcache::CatCList in Phase 2")]
pub struct CatCList {
    _private: [u8; 0],
}

/// List-search interface. Callers must also use catcache.h.
#[allow(deprecated)]
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
#[allow(deprecated)]
pub fn SearchSysCacheList1(cache_id: SysCacheIdentifier, key1: Datum) -> *mut CatCList {
    SearchSysCacheList(cache_id, 1, key1, Datum(0), Datum(0))
}

#[allow(deprecated)]
pub fn SearchSysCacheList2(
    cache_id: SysCacheIdentifier,
    key1: Datum,
    key2: Datum,
) -> *mut CatCList {
    SearchSysCacheList(cache_id, 2, key1, key2, Datum(0))
}

#[allow(deprecated)]
pub fn SearchSysCacheList3(
    cache_id: SysCacheIdentifier,
    key1: Datum,
    key2: Datum,
    key3: Datum,
) -> *mut CatCList {
    SearchSysCacheList(cache_id, 3, key1, key2, key3)
}
