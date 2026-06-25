//! Translated from PostgreSQL src/include/catalog/pg_publication.h

use crate::c::NameData;
use crate::catalog::objectaddress::ObjectAddress;
use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::nodes::Node;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;
use crate::utils::palloc::MemoryContext;
use crate::utils::rel::Relation;

// pg_list tombstoned; these `List *` values are node/relation lists.
type List = Vec<Box<Node>>;

pub const PublicationRelationId: Oid = Oid(6104);

#[repr(C)]
#[derive(pepperdb_derive::Catalog)]
pub struct FormData_pg_publication {
    pub oid: Oid,
    pub pubname: NameData,
    pub pubowner: Oid, // BKI_LOOKUP(pg_authid)
    pub puballtables: bool,
    pub pubinsert: bool,
    pub pubupdate: bool,
    pub pubdelete: bool,
    pub pubtruncate: bool,
    pub pubviaroot: bool,
    pub pubgencols: i8, // 'n' none / 's' stored
}

pub type Form_pg_publication = *mut FormData_pg_publication; // TODO(ptr)

// DECLARE_UNIQUE_INDEX_PKEY(pg_publication_oid_index, 6110, PublicationObjectIndexId, ...)
// DECLARE_UNIQUE_INDEX(pg_publication_pubname_index, 6111, PublicationNameIndexId, ...)
// MAKE_SYSCACHE(PUBLICATIONOID, pg_publication_oid_index, 8)
// MAKE_SYSCACHE(PUBLICATIONNAME, pg_publication_pubname_index, 8)

// In-memory structs (not on-disk).
pub struct PublicationActions {
    pub pubinsert: bool,
    pub pubupdate: bool,
    pub pubdelete: bool,
    pub pubtruncate: bool,
}

pub struct PublicationDesc {
    pub pubactions: PublicationActions,
    pub rf_valid_for_update: bool,
    pub rf_valid_for_delete: bool,
    pub cols_valid_for_update: bool,
    pub cols_valid_for_delete: bool,
    pub gencols_valid_for_update: bool,
    pub gencols_valid_for_delete: bool,
}

// char-valued enum (EXPOSE_TO_CLIENT_CODE)
#[repr(i8)]
pub enum PublishGencolsType {
    None = b'n' as i8,
    Stored = b's' as i8,
}

pub struct Publication {
    pub oid: Oid,
    pub name: String,
    pub alltables: bool,
    pub pubviaroot: bool,
    pub pubgencols_type: PublishGencolsType,
    pub pubactions: PublicationActions,
}

pub struct PublicationRelInfo {
    pub relation: Relation,
    pub where_clause: Node, // whereClause
    pub columns: List,
}

// ROOT/LEAF/ALL selector for GetRelationPublications()
pub enum PublicationPartOpt {
    Root,
    Leaf,
    All,
}

pub fn GetPublication(_pubid: Oid) -> Publication {
    unimplemented!()
}

// missing_ok -> Option
pub fn GetPublicationByName(_pubname: &str, _missing_ok: bool) -> Option<Publication> {
    unimplemented!()
}

pub fn GetRelationPublications(_relid: Oid) -> List {
    unimplemented!()
}

pub fn GetPublicationRelations(_pubid: Oid, _pub_partopt: PublicationPartOpt) -> List {
    unimplemented!()
}

pub fn GetAllTablesPublications() -> List {
    unimplemented!()
}

pub fn GetAllTablesPublicationRelations(_pubviaroot: bool) -> List {
    unimplemented!()
}

pub fn GetPublicationSchemas(_pubid: Oid) -> List {
    unimplemented!()
}

pub fn GetSchemaPublications(_schemaid: Oid) -> List {
    unimplemented!()
}

pub fn GetSchemaPublicationRelations(_schemaid: Oid, _pub_partopt: PublicationPartOpt) -> List {
    unimplemented!()
}

pub fn GetAllSchemaPublicationRelations(_pubid: Oid, _pub_partopt: PublicationPartOpt) -> List {
    unimplemented!()
}

pub fn GetPubPartitionOptionRelations(
    _result: List,
    _pub_partopt: PublicationPartOpt,
    _relid: Oid,
) -> List {
    unimplemented!()
}

// returns top ancestor + ancestor_level out-param
pub fn GetTopMostAncestorInPublication(_puboid: Oid, _ancestors: &List) -> (Oid, i32) {
    unimplemented!()
}

pub fn is_publishable_relation(_rel: &Relation) -> bool {
    unimplemented!()
}

pub fn is_schema_publication(_pubid: Oid) -> bool {
    unimplemented!()
}

// returns bool + cols out-param -> Option<Bitmapset>
pub fn check_and_fetch_column_list(_pub: &Publication, _relid: Oid, _mcxt: &MemoryContext) -> Option<Bitmapset> {
    unimplemented!()
}

pub fn publication_add_relation(_pubid: Oid, _pri: &PublicationRelInfo, _if_not_exists: bool) -> ObjectAddress {
    unimplemented!()
}

pub fn pub_collist_validate(_targetrel: &Relation, _columns: &List) -> Bitmapset {
    unimplemented!()
}

pub fn publication_add_schema(_pubid: Oid, _schemaid: Oid, _if_not_exists: bool) -> ObjectAddress {
    unimplemented!()
}

pub fn pub_collist_to_bitmapset(_columns: &Bitmapset, _pubcols: Datum, _mcxt: &MemoryContext) -> Bitmapset {
    unimplemented!()
}

pub fn pub_form_cols_map(_relation: &Relation, _include_gencols_type: PublishGencolsType) -> Bitmapset {
    unimplemented!()
}
