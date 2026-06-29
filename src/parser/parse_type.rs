//! Translated from PostgreSQL src/include/parser/parse_type.h

use crate::access::htup::HeapTuple;
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{ColumnDef, TypeName};
use crate::parser::parse_node::ParseState;
use crate::postgres::Datum;
use crate::postgres_ext::{InvalidOid, Oid};

pub type Type = HeapTuple;

/// `typmod_p` out-param folded in; missing-tuple sentinel -> `Option`.
pub fn LookupTypeName(
    _pstate: &mut ParseState,
    _type_name: &TypeName,
    _typmod_p: &mut i32,
    _missing_ok: bool,
) -> Option<Type> {
    unimplemented!()
}

pub fn LookupTypeNameExtended(
    _pstate: &mut ParseState,
    _type_name: &TypeName,
    _typmod_p: &mut i32,
    _temp_ok: bool,
    _missing_ok: bool,
) -> Option<Type> {
    unimplemented!()
}

/// `InvalidOid` sentinel (when `missing_ok`) -> `Option`.
pub fn LookupTypeNameOid(
    _pstate: &mut ParseState,
    _type_name: &TypeName,
    _missing_ok: bool,
) -> Option<Oid> {
    unimplemented!()
}

/// `typmod_p` out-param folded in -> `(Type, typmod)`.
pub fn typenameType(
    _pstate: &mut ParseState,
    _type_name: &TypeName,
) -> (Type, i32) {
    unimplemented!()
}

pub fn typenameTypeId(pstate: &mut ParseState, type_name: &TypeName) -> Oid {
    typenameTypeIdAndMod(pstate, type_name).0
}

/// `typeid_p`/`typmod_p` out-params -> returned tuple. M4 (step 23): resolve the
/// `TypeName` to `(typeOid, typmod)` synchronously via the (warm) TYPEOID /
/// TYPENAMENSP syscache -- the cast transform runs in sync context, and
/// `warm_expr_caches` pre-warms the type-name caches over the wire. typmod-bearing
/// types (numeric(p,s)) grow with the opt_type_modifiers machinery; M4 passes -1.
pub fn typenameTypeIdAndMod(_pstate: &mut ParseState, type_name: &TypeName) -> (Oid, i32) {
    use crate::backend::utils::cache::syscache::{get_sys_cache_oid, search_sys_cache, release_sys_cache};
    use crate::catalog::pg_namespace::PG_CATALOG_NAMESPACE;
    use crate::catalog::pg_type::Anum_pg_type_oid;
    use crate::utils::syscache::SysCacheIdentifier;

    // An internally generated TypeName carries the OID directly (names == NIL).
    if type_name.names.is_empty() {
        if type_name.typeOid == InvalidOid {
            unimplemented!("typenameTypeIdAndMod: OID-less internal TypeName");
        }
        return (type_name.typeOid, type_name.typemod);
    }
    if !type_name.typmods.is_empty() {
        unimplemented!("typenameTypeIdAndMod: type modifiers");
    }

    let names: Vec<&str> = type_name.names.iter().map(|s| s.sval.as_str()).collect();
    let typname = match names.as_slice() {
        // A pg_catalog-qualified (`pg_catalog.int4`) or bare (`numeric`) name. M4
        // resolves both against pg_catalog (the only namespace seeded base types
        // live in); the general search-path / explicit-schema lookup grows later.
        [n] | ["pg_catalog", n] => *n,
        _ => unimplemented!("typenameTypeIdAndMod: schema-qualified / 3+-part type name"),
    };

    // TYPENAMENSP (typname, typnamespace) -> the row; read its oid column.
    let nd = crate::backend::catalog::heap::name_data(typname);
    let keys = [
        crate::postgres::NameGetDatum(&nd),
        crate::postgres::ObjectIdGetDatum(PG_CATALOG_NAMESPACE),
    ];
    if let Some(oid) =
        get_sys_cache_oid(SysCacheIdentifier::TYPENAMENSP, Anum_pg_type_oid as i16, &keys)
    {
        return (oid, -1);
    }
    // Negative-cache / cold miss: release any held tuple and raise.
    if let Some(t) = search_sys_cache(SysCacheIdentifier::TYPENAMENSP, &keys) {
        release_sys_cache(t);
    }
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_UNDEFINED_OBJECT)
            .errmsg(format!("type \"{}\" does not exist", names.join(".")));
    });
    unreachable!("ereport(ERROR) diverges");
}

pub fn TypeNameToString(_type_name: &TypeName) -> String {
    unimplemented!()
}

pub fn TypeNameListToString(_typenames: &[Node]) -> String {
    unimplemented!()
}

pub fn LookupCollation(_pstate: &mut ParseState, _collnames: Vec<Node>, _location: i32) -> Oid {
    unimplemented!()
}

pub fn GetColumnDefCollation(
    _pstate: &mut ParseState,
    _coldef: &ColumnDef,
    _type_oid: Oid,
) -> Oid {
    unimplemented!()
}

pub fn typeidType(_id: Oid) -> Type {
    unimplemented!()
}

pub fn typeTypeId(_tp: Type) -> Oid {
    unimplemented!()
}

pub fn typeLen(_t: Type) -> i16 {
    unimplemented!()
}

pub fn typeByVal(_t: Type) -> bool {
    unimplemented!()
}

pub fn typeTypeName(_t: Type) -> String {
    unimplemented!()
}

pub fn typeTypeRelid(_typ: Type) -> Oid {
    unimplemented!()
}

pub fn typeTypeCollation(_typ: Type) -> Oid {
    unimplemented!()
}

pub fn stringTypeDatum(_tp: Type, _string: &str, _atttypmod: i32) -> Datum {
    unimplemented!()
}

pub fn typeidTypeRelid(_type_id: Oid) -> Oid {
    unimplemented!()
}

pub fn typeOrDomainTypeRelid(_type_id: Oid) -> Oid {
    unimplemented!()
}

pub fn typeStringToTypeName(_str: &str, _escontext: Option<&mut Node>) -> Box<TypeName> {
    unimplemented!()
}

/// `typeid_p`/`typmod_p` out-params -> returned tuple on success.
pub fn parseTypeString(
    _str: &str,
    _escontext: Option<&mut Node>,
) -> Option<(Oid, i32)> {
    unimplemented!()
}

/// true if typeid is composite, or domain over composite, but not RECORD
pub fn ISCOMPLEX(typeid: Oid) -> bool {
    typeOrDomainTypeRelid(typeid) != InvalidOid
}
