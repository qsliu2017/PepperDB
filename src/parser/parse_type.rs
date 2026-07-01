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

/// PG `parseTypeString`: parse a type-name string to `(typeOid, typmod)`.
///
/// PG runs the raw parser in type-name mode (`typeStringToTypeName`) then
/// `typenameTypeIdAndMod`. The raw parser's type-name mode is not wired yet, so
/// this handles the common form the current callers need -- a bare or
/// `pg_catalog`-qualified type name with no typmod, array bounds, or `%TYPE` --
/// by building the `TypeName` directly and resolving it through the (warm)
/// TYPENAMENSP syscache. Forms needing the grammar (typmods like `numeric(10,2)`,
/// arrays like `int[]`, `SETOF`) are deferred to the type-name grammar mode.
///
/// The `_escontext` out-param folds to the returned `Option`: `None` on a lookup
/// miss (mirroring C's soft-error return). typeOid/typmod out-params -> tuple.
pub fn parseTypeString(str: &str, _escontext: Option<&mut Node>) -> Option<(Oid, i32)> {
    use crate::nodes::value::String_;

    let name = str.trim();
    // Reject anything the bare-name fast path cannot faithfully handle; those need
    // the type-name grammar (typmod, arrays, SETOF, %TYPE, quoted identifiers).
    if name.is_empty()
        || name.contains(['(', ')', '[', ']', '"', '%', ' ', '\t', '\n'])
        || name.to_ascii_lowercase().starts_with("setof")
    {
        unimplemented!("parseTypeString: type-name grammar forms (typmod/array/SETOF) deferred");
    }

    // A bare or `schema.type` dotted name -> the TypeName.names parts.
    let parts: Vec<String_> = name
        .split('.')
        .map(|p| String_ { sval: p.to_owned() })
        .collect();
    let type_name = TypeName {
        names: parts,
        typeOid: InvalidOid,
        setof: false,
        pct_type: false,
        typmods: Vec::new(),
        typemod: -1,
        arrayBounds: Vec::new(),
        location: -1,
    };
    // typenameTypeIdAndMod raises ERROR on an unknown type (hard error); here that
    // is the intended behavior for the `pg_input_is_valid` typname argument (a bad
    // TYPE NAME is a hard error, distinct from a bad VALUE which is the soft one).
    let mut pstate = crate::parser::parse_node::make_parsestate(None);
    Some(typenameTypeIdAndMod(&mut pstate, &type_name))
}

/// true if typeid is composite, or domain over composite, but not RECORD
pub fn ISCOMPLEX(typeid: Oid) -> bool {
    typeOrDomainTypeRelid(typeid) != InvalidOid
}
