//! src/backend/commands/collationcmds.c
//!
//! collation-related commands support code
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;
use crate::{foreach, current_cell, lfirst_node};
use crate::nodes::pg_list::{List, lfirst};
use crate::miscadmin::MyDatabaseId;

use std::ffi::{c_char, c_int, c_void};

// ---------------------------------------------------------------------------
// Local type aliases / stubs for not-yet-ported dependencies
// ---------------------------------------------------------------------------

type ParseState = c_void;
type DefElem = c_void;
type AlterCollationStmt = c_void;
type ObjectAddress = c_void; // placeholder; see ObjectAddressSet usage
type Relation = *mut c_void;
type HeapTuple = *mut c_void;
type Form_pg_collation = *mut c_void;
type Form_pg_database = *mut c_void;

// Provider constants (catalog/pg_collation.h)
const COLLPROVIDER_DEFAULT: c_char = b'd' as c_char;
const COLLPROVIDER_BUILTIN: c_char = b'b' as c_char;
const COLLPROVIDER_LIBC: c_char = b'c' as c_char;
const COLLPROVIDER_ICU: c_char = b'i' as c_char;

extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strlen(s: *const c_char) -> usize;
}

/// matches struct in collationcmds.c
#[repr(C)]
struct CollAliasData {
    localename: *mut c_char, // name of locale, as per "locale -a"
    alias: *mut c_char,      // shortened alias for same
    enc: c_int,              // encoding
}

/*
 * CREATE COLLATION
 */
pub unsafe fn DefineCollation(
    pstate: *mut ParseState,
    names: *mut List,
    parameters: *mut List,
    if_not_exists: bool,
) -> ObjectAddress {
    let collName: *mut c_char;
    let collNamespace: Oid;
    let aclresult: AclResult;
    let mut fromEl: *mut DefElem = std::ptr::null_mut();
    let mut localeEl: *mut DefElem = std::ptr::null_mut();
    let mut lccollateEl: *mut DefElem = std::ptr::null_mut();
    let mut lcctypeEl: *mut DefElem = std::ptr::null_mut();
    let mut providerEl: *mut DefElem = std::ptr::null_mut();
    let mut deterministicEl: *mut DefElem = std::ptr::null_mut();
    let mut rulesEl: *mut DefElem = std::ptr::null_mut();
    let mut versionEl: *mut DefElem = std::ptr::null_mut();
    let mut collcollate: *mut c_char;
    let mut collctype: *mut c_char;
    let mut colllocale: *const c_char;
    let mut collicurules: *mut c_char;
    let collisdeterministic: bool;
    let collencoding: c_int;
    let collprovider: c_char;
    let mut collversion: *mut c_char = std::ptr::null_mut();
    let newoid: Oid;
    let mut address: ObjectAddress = std::mem::zeroed();

    let mut collName_local: *mut c_char = std::ptr::null_mut();
    collNamespace = QualifiedNameGetCreationNamespace(names, &mut collName_local);
    collName = collName_local;

    aclresult = object_aclcheck(
        NamespaceRelationId,
        collNamespace,
        GetUserId(),
        ACL_CREATE,
    );
    if aclresult != ACLCHECK_OK {
        aclcheck_error(
            aclresult,
            OBJECT_SCHEMA,
            get_namespace_name(collNamespace),
        );
    }

    foreach!(pl, parameters, {
        let defel: *mut DefElem = lfirst_node!(DefElem, T_DefElem, current_cell!(pl));
        let defelp: *mut *mut DefElem;

        if strcmp(defGetDefname(defel), c"from".as_ptr()) == 0 {
            defelp = &mut fromEl;
        } else if strcmp(defGetDefname(defel), c"locale".as_ptr()) == 0 {
            defelp = &mut localeEl;
        } else if strcmp(defGetDefname(defel), c"lc_collate".as_ptr()) == 0 {
            defelp = &mut lccollateEl;
        } else if strcmp(defGetDefname(defel), c"lc_ctype".as_ptr()) == 0 {
            defelp = &mut lcctypeEl;
        } else if strcmp(defGetDefname(defel), c"provider".as_ptr()) == 0 {
            defelp = &mut providerEl;
        } else if strcmp(defGetDefname(defel), c"deterministic".as_ptr()) == 0 {
            defelp = &mut deterministicEl;
        } else if strcmp(defGetDefname(defel), c"rules".as_ptr()) == 0 {
            defelp = &mut rulesEl;
        } else if strcmp(defGetDefname(defel), c"version".as_ptr()) == 0 {
            defelp = &mut versionEl;
        } else {
            elog!(
                ERROR,
                "collation attribute \"{}\" not recognized",
                cstr_to_string(defGetDefname(defel))
            );
            unreachable!();
        }
        if !(*defelp).is_null() {
            errorConflictingDefElem(defel, pstate);
        }
        *defelp = defel;
    });

    if !localeEl.is_null() && (!lccollateEl.is_null() || !lcctypeEl.is_null()) {
        ereport!(
            ERROR,
            "conflicting or redundant options"
        );
    }

    if !fromEl.is_null() && list_length(parameters) != 1 {
        ereport!(
            ERROR,
            "conflicting or redundant options"
        );
    }

    if !fromEl.is_null() {
        let collid: Oid;
        let tp: HeapTuple;
        let mut datum: Datum;
        let mut isnull: bool = false;

        collid = get_collation_oid(defGetQualifiedName(fromEl), false);
        tp = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));
        if !HeapTupleIsValid(tp) {
            elog!(ERROR, "cache lookup failed for collation {}", collid);
            unreachable!();
        }

        collprovider = (*(GETSTRUCT(tp) as Form_pg_collation_ptr)).collprovider;
        collisdeterministic = (*(GETSTRUCT(tp) as Form_pg_collation_ptr)).collisdeterministic;
        collencoding = (*(GETSTRUCT(tp) as Form_pg_collation_ptr)).collencoding;

        datum = SysCacheGetAttr(COLLOID, tp, Anum_pg_collation_collcollate, &mut isnull);
        if !isnull {
            collcollate = TextDatumGetCString(datum);
        } else {
            collcollate = std::ptr::null_mut();
        }

        datum = SysCacheGetAttr(COLLOID, tp, Anum_pg_collation_collctype, &mut isnull);
        if !isnull {
            collctype = TextDatumGetCString(datum);
        } else {
            collctype = std::ptr::null_mut();
        }

        datum = SysCacheGetAttr(COLLOID, tp, Anum_pg_collation_colllocale, &mut isnull);
        if !isnull {
            colllocale = TextDatumGetCString(datum);
        } else {
            colllocale = std::ptr::null();
        }

        /*
         * When the ICU locale comes from an existing collation, do not
         * canonicalize to a language tag.
         */

        datum = SysCacheGetAttr(COLLOID, tp, Anum_pg_collation_collicurules, &mut isnull);
        if !isnull {
            collicurules = TextDatumGetCString(datum);
        } else {
            collicurules = std::ptr::null_mut();
        }

        ReleaseSysCache(tp);

        /*
         * Copying the "default" collation is not allowed because most code
         * checks for DEFAULT_COLLATION_OID instead of COLLPROVIDER_DEFAULT,
         * and so having a second collation with COLLPROVIDER_DEFAULT would
         * not work and potentially confuse or crash some code.  This could be
         * fixed with some legwork.
         */
        if collprovider == COLLPROVIDER_DEFAULT {
            ereport!(
                ERROR,
                "collation \"default\" cannot be copied"
            );
        }
    } else {
        let mut collproviderstr: *mut c_char = std::ptr::null_mut();

        collcollate = std::ptr::null_mut();
        collctype = std::ptr::null_mut();
        colllocale = std::ptr::null();
        collicurules = std::ptr::null_mut();

        if !providerEl.is_null() {
            collproviderstr = defGetString(providerEl);
        }

        if !deterministicEl.is_null() {
            collisdeterministic = defGetBoolean(deterministicEl);
        } else {
            collisdeterministic = true;
        }

        if !rulesEl.is_null() {
            collicurules = defGetString(rulesEl);
        }

        if !versionEl.is_null() {
            collversion = defGetString(versionEl);
        }

        if !collproviderstr.is_null() {
            if pg_strcasecmp(collproviderstr, c"builtin".as_ptr()) == 0 {
                collprovider = COLLPROVIDER_BUILTIN;
            } else if pg_strcasecmp(collproviderstr, c"icu".as_ptr()) == 0 {
                collprovider = COLLPROVIDER_ICU;
            } else if pg_strcasecmp(collproviderstr, c"libc".as_ptr()) == 0 {
                collprovider = COLLPROVIDER_LIBC;
            } else {
                elog!(
                    ERROR,
                    "unrecognized collation provider: {}",
                    cstr_to_string(collproviderstr)
                );
                unreachable!();
            }
        } else {
            collprovider = COLLPROVIDER_LIBC;
        }

        if !localeEl.is_null() {
            if collprovider == COLLPROVIDER_LIBC {
                collcollate = defGetString(localeEl);
                collctype = defGetString(localeEl);
            } else {
                colllocale = defGetString(localeEl);
            }
        }

        if !lccollateEl.is_null() {
            collcollate = defGetString(lccollateEl);
        }

        if !lcctypeEl.is_null() {
            collctype = defGetString(lcctypeEl);
        }

        if collprovider == COLLPROVIDER_BUILTIN {
            if colllocale.is_null() {
                elog!(ERROR, "parameter \"{}\" must be specified", "locale");
                unreachable!();
            }

            colllocale = builtin_validate_locale(GetDatabaseEncoding(), colllocale);
        } else if collprovider == COLLPROVIDER_LIBC {
            if collcollate.is_null() {
                elog!(ERROR, "parameter \"{}\" must be specified", "lc_collate");
                unreachable!();
            }

            if collctype.is_null() {
                elog!(ERROR, "parameter \"{}\" must be specified", "lc_ctype");
                unreachable!();
            }
        } else if collprovider == COLLPROVIDER_ICU {
            if colllocale.is_null() {
                elog!(ERROR, "parameter \"{}\" must be specified", "locale");
                unreachable!();
            }

            /*
             * During binary upgrade, preserve the locale string. Otherwise,
             * canonicalize to a language tag.
             */
            if !IsBinaryUpgrade {
                let langtag: *mut c_char = icu_language_tag(colllocale, icu_validation_level);

                if !langtag.is_null() && strcmp(colllocale, langtag) != 0 {
                    elog!(
                        NOTICE,
                        "using standard form \"{}\" for ICU locale \"{}\"",
                        cstr_to_string(langtag),
                        cstr_to_string(colllocale)
                    );

                    colllocale = langtag;
                }
            }

            icu_validate_locale(colllocale);
        }

        /*
         * Nondeterministic collations are currently only supported with ICU
         * because that's the only case where it can actually make a
         * difference. So we can save writing the code for the other
         * providers.
         */
        if !collisdeterministic && collprovider != COLLPROVIDER_ICU {
            ereport!(
                ERROR,
                "nondeterministic collations not supported with this provider"
            );
        }

        if !collicurules.is_null() && collprovider != COLLPROVIDER_ICU {
            ereport!(
                ERROR,
                "ICU rules cannot be specified unless locale provider is ICU"
            );
        }

        if collprovider == COLLPROVIDER_BUILTIN {
            collencoding = builtin_locale_encoding(colllocale);
        } else if collprovider == COLLPROVIDER_ICU {
            // #ifdef USE_ICU not compiled
            collencoding = -1;
        } else {
            collencoding = GetDatabaseEncoding();
            check_encoding_locale_matches(collencoding, collcollate, collctype);
        }
    }

    if collversion.is_null() {
        let locale: *const c_char;

        if collprovider == COLLPROVIDER_LIBC {
            locale = collcollate;
        } else {
            locale = colllocale;
        }

        collversion = get_collation_actual_version(collprovider, locale);
    }

    newoid = CollationCreate(
        collName,
        collNamespace,
        GetUserId(),
        collprovider,
        collisdeterministic,
        collencoding,
        collcollate,
        collctype,
        colllocale,
        collicurules,
        collversion,
        if_not_exists,
        false, /* not quiet */
    );

    if !OidIsValid(newoid) {
        return InvalidObjectAddress();
    }

    /* Check that the locales can be loaded. */
    CommandCounterIncrement();
    let _ = pg_newlocale_from_collation(newoid);

    ObjectAddressSet(&mut address, CollationRelationId, newoid);

    address
}

/*
 * Subroutine for ALTER COLLATION SET SCHEMA and RENAME
 *
 * Is there a collation with the same name of the given collation already in
 * the given namespace?  If so, raise an appropriate error message.
 */
pub unsafe fn IsThereCollationInNamespace(collname: *const c_char, nspOid: Oid) {
    /* make sure the name doesn't already exist in new schema */
    if SearchSysCacheExists3(
        COLLNAMEENCNSP,
        CStringGetDatum(collname),
        Int32GetDatum(GetDatabaseEncoding()),
        ObjectIdGetDatum(nspOid),
    ) {
        elog!(
            ERROR,
            "collation \"{}\" for encoding \"{}\" already exists in schema \"{}\"",
            cstr_to_string(collname),
            cstr_to_string(GetDatabaseEncodingName()),
            cstr_to_string(get_namespace_name(nspOid))
        );
    }

    /* mustn't match an any-encoding entry, either */
    if SearchSysCacheExists3(
        COLLNAMEENCNSP,
        CStringGetDatum(collname),
        Int32GetDatum(-1),
        ObjectIdGetDatum(nspOid),
    ) {
        elog!(
            ERROR,
            "collation \"{}\" already exists in schema \"{}\"",
            cstr_to_string(collname),
            cstr_to_string(get_namespace_name(nspOid))
        );
    }
}

/*
 * ALTER COLLATION
 */
pub unsafe fn AlterCollation(stmt: *mut AlterCollationStmt) -> ObjectAddress {
    let rel: Relation;
    let collOid: Oid;
    let mut tup: HeapTuple;
    let collForm: Form_pg_collation_ptr;
    let mut datum: Datum;
    let mut isnull: bool = false;
    let oldversion: *mut c_char;
    let newversion: *mut c_char;
    let mut address: ObjectAddress = std::mem::zeroed();

    rel = table_open(CollationRelationId, RowExclusiveLock);
    collOid = get_collation_oid(stmt_collname(stmt), false);

    if collOid == DEFAULT_COLLATION_OID {
        ereport!(
            ERROR,
            "cannot refresh version of default collation"
        );
    }

    if !object_ownercheck(CollationRelationId, collOid, GetUserId()) {
        aclcheck_error(
            ACLCHECK_NOT_OWNER,
            OBJECT_COLLATION,
            NameListToString(stmt_collname(stmt)),
        );
    }

    tup = SearchSysCacheCopy1(COLLOID, ObjectIdGetDatum(collOid));
    if !HeapTupleIsValid(tup) {
        elog!(ERROR, "cache lookup failed for collation {}", collOid);
        unreachable!();
    }

    collForm = GETSTRUCT(tup) as Form_pg_collation_ptr;
    datum = SysCacheGetAttr(COLLOID, tup, Anum_pg_collation_collversion, &mut isnull);
    oldversion = if isnull {
        std::ptr::null_mut()
    } else {
        TextDatumGetCString(datum)
    };

    if (*collForm).collprovider == COLLPROVIDER_LIBC {
        datum = SysCacheGetAttrNotNull(COLLOID, tup, Anum_pg_collation_collcollate);
    } else {
        datum = SysCacheGetAttrNotNull(COLLOID, tup, Anum_pg_collation_colllocale);
    }

    newversion = get_collation_actual_version(
        (*collForm).collprovider,
        TextDatumGetCString(datum),
    );

    /* cannot change from NULL to non-NULL or vice versa */
    if (oldversion.is_null() && !newversion.is_null())
        || (!oldversion.is_null() && newversion.is_null())
    {
        elog!(ERROR, "invalid collation version change");
        unreachable!();
    } else if !oldversion.is_null()
        && !newversion.is_null()
        && strcmp(newversion, oldversion) != 0
    {
        let mut nulls: [bool; Natts_pg_collation] = [false; Natts_pg_collation];
        let mut replaces: [bool; Natts_pg_collation] = [false; Natts_pg_collation];
        let mut values: [Datum; Natts_pg_collation] = [0; Natts_pg_collation];

        elog!(
            NOTICE,
            "changing version from {} to {}",
            cstr_to_string(oldversion),
            cstr_to_string(newversion)
        );

        // memset(values, 0) / memset(nulls, false) / memset(replaces, false) handled by initializers above.
        let _ = &mut nulls;

        values[(Anum_pg_collation_collversion - 1) as usize] = CStringGetTextDatum(newversion);
        replaces[(Anum_pg_collation_collversion - 1) as usize] = true;

        tup = heap_modify_tuple(
            tup,
            RelationGetDescr(rel),
            values.as_mut_ptr(),
            nulls.as_mut_ptr(),
            replaces.as_mut_ptr(),
        );
    } else {
        ereport!(NOTICE, "version has not changed");
    }

    CatalogTupleUpdate(rel, t_self_ptr(tup), tup);

    InvokeObjectPostAlterHook(CollationRelationId, collOid, 0);

    ObjectAddressSet(&mut address, CollationRelationId, collOid);

    heap_freetuple(tup);
    table_close(rel, NoLock);

    address
}

#[no_mangle]
pub unsafe extern "C" fn pg_collation_actual_version(fcinfo: FunctionCallInfo) -> Datum {
    let collid: Oid = PG_GETARG_OID(fcinfo, 0);
    let provider: c_char;
    let locale: *mut c_char;
    let version: *mut c_char;
    let datum: Datum;

    if collid == DEFAULT_COLLATION_OID {
        /* retrieve from pg_database */

        let dbtup: HeapTuple = SearchSysCache1(DATABASEOID, ObjectIdGetDatum(MyDatabaseId));

        if !HeapTupleIsValid(dbtup) {
            elog!(ERROR, "database with OID {} does not exist", MyDatabaseId);
            unreachable!();
        }

        provider = (*(GETSTRUCT(dbtup) as Form_pg_database_ptr)).datlocprovider;

        if provider == COLLPROVIDER_LIBC {
            datum = SysCacheGetAttrNotNull(DATABASEOID, dbtup, Anum_pg_database_datcollate);
            locale = TextDatumGetCString(datum);
        } else {
            datum = SysCacheGetAttrNotNull(DATABASEOID, dbtup, Anum_pg_database_datlocale);
            locale = TextDatumGetCString(datum);
        }

        ReleaseSysCache(dbtup);
    } else {
        /* retrieve from pg_collation */

        let colltp: HeapTuple = SearchSysCache1(COLLOID, ObjectIdGetDatum(collid));

        if !HeapTupleIsValid(colltp) {
            elog!(ERROR, "collation with OID {} does not exist", collid);
            unreachable!();
        }

        provider = (*(GETSTRUCT(colltp) as Form_pg_collation_ptr)).collprovider;
        Assert!(provider != COLLPROVIDER_DEFAULT);

        if provider == COLLPROVIDER_LIBC {
            datum = SysCacheGetAttrNotNull(COLLOID, colltp, Anum_pg_collation_collcollate);
            locale = TextDatumGetCString(datum);
        } else {
            datum = SysCacheGetAttrNotNull(COLLOID, colltp, Anum_pg_collation_colllocale);
            locale = TextDatumGetCString(datum);
        }

        ReleaseSysCache(colltp);
    }

    version = get_collation_actual_version(provider, locale);
    if !version.is_null() {
        PG_RETURN_TEXT_P(cstring_to_text(version))
    } else {
        PG_RETURN_NULL()
    }
}

/* will we use "locale -a" in pg_import_system_collations? */
/* #if !defined(WIN32) -> READ_LOCALE_A_OUTPUT */

/*
 * "Normalize" a libc locale name, stripping off encoding tags such as
 * ".utf8" (e.g., "en_US.utf8" -> "en_US", but "br_FR.iso885915@euro"
 * -> "br_FR@euro").  Return true if a new, different name was
 * generated.
 */
unsafe fn normalize_libc_locale_name(new: *mut c_char, old: *const c_char) -> bool {
    let mut n = new;
    let mut o = old;
    let mut changed = false;

    while *o != 0 {
        if *o == b'.' as c_char {
            /* skip over encoding tag such as ".utf8" or ".UTF-8" */
            o = o.add(1);
            while (*o >= b'A' as c_char && *o <= b'Z' as c_char)
                || (*o >= b'a' as c_char && *o <= b'z' as c_char)
                || (*o >= b'0' as c_char && *o <= b'9' as c_char)
                || (*o == b'-' as c_char)
            {
                o = o.add(1);
            }
            changed = true;
        } else {
            *n = *o;
            n = n.add(1);
            o = o.add(1);
        }
    }
    *n = 0;

    changed
}

/*
 * qsort comparator for CollAliasData items
 */
unsafe extern "C" fn cmpaliases(a: *const c_void, b: *const c_void) -> c_int {
    let ca = a as *const CollAliasData;
    let cb = b as *const CollAliasData;

    /* comparing localename is enough because other fields are derived */
    strcmp((*ca).localename, (*cb).localename)
}

/*
 * Create a new collation using the input locale 'locale'. (subroutine for
 * pg_import_system_collations())
 *
 * 'nspid' is the namespace id where the collation will be created.
 *
 * 'nvalidp' is incremented if the locale has a valid encoding.
 *
 * 'ncreatedp' is incremented if the collation is actually created.  If the
 * collation already exists it will quietly do nothing.
 *
 * The returned value is the encoding of the locale, -1 if the locale is not
 * valid for creating a collation.
 */
unsafe fn create_collation_from_locale(
    locale: *const c_char,
    nspid: c_int,
    nvalidp: *mut c_int,
    ncreatedp: *mut c_int,
) -> c_int {
    let enc: c_int;
    let collid: Oid;

    /*
     * Some systems have locale names that don't consist entirely of ASCII
     * letters (such as "bokmal" or "francais"). This is pretty silly, since we
     * need the locale itself to interpret the non-ASCII characters. We can't
     * do much with those, so we filter them out.
     */
    if !pg_is_ascii(locale) {
        elog!(
            DEBUG1,
            "skipping locale with non-ASCII name: \"{}\"",
            cstr_to_string(locale)
        );
        return -1;
    }

    enc = pg_get_encoding_from_locale(locale, false);
    if enc < 0 {
        elog!(
            DEBUG1,
            "skipping locale with unrecognized encoding: \"{}\"",
            cstr_to_string(locale)
        );
        return -1;
    }
    if !PG_VALID_BE_ENCODING(enc) {
        elog!(
            DEBUG1,
            "skipping locale with client-only encoding: \"{}\"",
            cstr_to_string(locale)
        );
        return -1;
    }
    if enc == PG_SQL_ASCII {
        return -1; /* C/POSIX are already in the catalog */
    }

    /* count valid locales found in operating system */
    *nvalidp += 1;

    /*
     * Create a collation named the same as the locale, but quietly doing
     * nothing if it already exists.  This is the behavior we need even at
     * initdb time, because some versions of "locale -a" can report the same
     * locale name more than once.  And it's convenient for later import runs,
     * too, since you just about always want to add on new locales without a
     * lot of chatter about existing ones.
     */
    collid = CollationCreate(
        locale,
        nspid as Oid,
        GetUserId(),
        COLLPROVIDER_LIBC,
        true,
        enc,
        locale,
        locale,
        std::ptr::null(),
        std::ptr::null_mut(),
        get_collation_actual_version(COLLPROVIDER_LIBC, locale),
        true,
        true,
    );
    if OidIsValid(collid) {
        *ncreatedp += 1;

        /* Must do CCI between inserts to handle duplicates correctly */
        CommandCounterIncrement();
    }

    enc
}

/*
 * pg_import_system_collations: add known system collations to pg_collation
 */
#[no_mangle]
pub unsafe extern "C" fn pg_import_system_collations(fcinfo: FunctionCallInfo) -> Datum {
    let nspid: Oid = PG_GETARG_OID(fcinfo, 0);
    let mut ncreated: c_int = 0;

    if !superuser() {
        ereport!(
            ERROR,
            "must be superuser to import system collations"
        );
    }

    if !SearchSysCacheExists1(NAMESPACEOID, ObjectIdGetDatum(nspid)) {
        elog!(ERROR, "schema with OID {} does not exist", nspid);
        unreachable!();
    }

    /* Load collations known to libc, using "locale -a" to enumerate them */
    /* #ifdef READ_LOCALE_A_OUTPUT */
    {
        let locale_a_handle: *mut FILE;
        let mut localebuf: [c_char; LOCALE_NAME_BUFLEN] = [0; LOCALE_NAME_BUFLEN];
        let mut nvalid: c_int = 0;
        let mut collid: Oid;
        let mut aliases: *mut CollAliasData;
        let mut naliases: c_int;
        let mut maxaliases: c_int;
        let mut i: c_int;

        /* expansible array of aliases */
        maxaliases = 100;
        aliases = palloc((maxaliases as usize) * std::mem::size_of::<CollAliasData>())
            as *mut CollAliasData;
        naliases = 0;

        locale_a_handle = OpenPipeStream(c"locale -a".as_ptr(), c"r".as_ptr());
        if locale_a_handle.is_null() {
            elog!(
                ERROR,
                "could not execute command \"{}\": %m",
                "locale -a"
            );
            unreachable!();
        }

        while !fgets(
            localebuf.as_mut_ptr(),
            std::mem::size_of::<[c_char; LOCALE_NAME_BUFLEN]>() as c_int,
            locale_a_handle,
        )
        .is_null()
        {
            let len: usize;
            let enc: c_int;
            let mut alias: [c_char; LOCALE_NAME_BUFLEN] = [0; LOCALE_NAME_BUFLEN];

            len = strlen(localebuf.as_ptr());

            if len == 0 || localebuf[len - 1] != b'\n' as c_char {
                elog!(
                    DEBUG1,
                    "skipping locale with too-long name: \"{}\"",
                    cstr_to_string(localebuf.as_ptr())
                );
                continue;
            }
            localebuf[len - 1] = 0;

            enc = create_collation_from_locale(
                localebuf.as_ptr(),
                nspid as c_int,
                &mut nvalid,
                &mut ncreated,
            );
            if enc < 0 {
                continue;
            }

            /*
             * Generate aliases such as "en_US" in addition to "en_US.utf8"
             * for ease of use.  Note that collation names are unique per
             * encoding only, so this doesn't clash with "en_US" for LATIN1,
             * say.
             *
             * However, it might conflict with a name we'll see later in the
             * "locale -a" output.  So save up the aliases and try to add them
             * after we've read all the output.
             */
            if normalize_libc_locale_name(alias.as_mut_ptr(), localebuf.as_ptr()) {
                if naliases >= maxaliases {
                    maxaliases *= 2;
                    aliases = repalloc(
                        aliases as *mut c_void,
                        (maxaliases as usize) * std::mem::size_of::<CollAliasData>(),
                    ) as *mut CollAliasData;
                }
                (*aliases.add(naliases as usize)).localename = pstrdup(localebuf.as_ptr());
                (*aliases.add(naliases as usize)).alias = pstrdup(alias.as_ptr());
                (*aliases.add(naliases as usize)).enc = enc;
                naliases += 1;
            }
        }

        /*
         * We don't check the return value of this, because we want to support
         * the case where there "locale" command does not exist.  (This is
         * unusual but can happen on minimalized Linux distributions, for
         * example.)  We will warn below if no locales could be found.
         */
        ClosePipeStream(locale_a_handle);

        /*
         * Before processing the aliases, sort them by locale name.  The point
         * here is that if "locale -a" gives us multiple locale names with the
         * same encoding and base name, say "en_US.utf8" and "en_US.utf-8", we
         * want to pick a deterministic one of them.  First in ASCII sort
         * order is a good enough rule.  (Before PG 10, the code corresponding
         * to this logic in initdb.c had an additional ordering rule, to
         * prefer the locale name exactly matching the alias, if any.  We
         * don't need to consider that here, because we would have already
         * created such a pg_collation entry above, and that one will win.)
         */
        if naliases > 1 {
            qsort(
                aliases as *mut c_void,
                naliases as usize,
                std::mem::size_of::<CollAliasData>(),
                cmpaliases,
            );
        }

        /* Now add aliases, ignoring any that match pre-existing entries */
        i = 0;
        while i < naliases {
            let locale: *mut c_char = (*aliases.add(i as usize)).localename;
            let alias: *mut c_char = (*aliases.add(i as usize)).alias;
            let enc: c_int = (*aliases.add(i as usize)).enc;

            collid = CollationCreate(
                alias,
                nspid,
                GetUserId(),
                COLLPROVIDER_LIBC,
                true,
                enc,
                locale,
                locale,
                std::ptr::null(),
                std::ptr::null_mut(),
                get_collation_actual_version(COLLPROVIDER_LIBC, locale),
                true,
                true,
            );
            if OidIsValid(collid) {
                ncreated += 1;

                CommandCounterIncrement();
            }

            i += 1;
        }

        /* Give a warning if "locale -a" seems to be malfunctioning */
        if nvalid == 0 {
            ereport!(WARNING, "no usable system locales were found");
        }
    }
    /* #endif READ_LOCALE_A_OUTPUT */

    /*
     * Load collations known to ICU
     *
     * (#ifdef USE_ICU not compiled in this build)
     */

    /* Load collations known to WIN32 (#ifdef ENUM_SYSTEM_LOCALE not compiled) */

    PG_RETURN_INT32(ncreated)
}

// ---------------------------------------------------------------------------
// Helper aliases / stubs for not-yet-ported dependencies
// ---------------------------------------------------------------------------

type FILE = c_void;
type FunctionCallInfo = *mut c_void;
type AclResult = c_int;
type Form_pg_collation_ptr = *mut PgFormCollation;
type Form_pg_database_ptr = *mut PgFormDatabase;

/// minimal layout for Form_pg_collation fields accessed here
#[repr(C)]
struct PgFormCollation {
    collprovider: c_char,
    collisdeterministic: bool,
    collencoding: c_int,
}

/// minimal layout for Form_pg_database fields accessed here
#[repr(C)]
struct PgFormDatabase {
    datlocprovider: c_char,
}

const ACLCHECK_OK: AclResult = 0;
const ACLCHECK_NOT_OWNER: AclResult = 1;

const LOCALE_NAME_BUFLEN: usize = 128;

// Catalog OIDs / attribute numbers / constants (stubs)
const NamespaceRelationId: Oid = 2615;
const CollationRelationId: Oid = 3456;
const ACL_CREATE: u32 = 0;
const OBJECT_SCHEMA: c_int = 0;
const OBJECT_COLLATION: c_int = 0;
const COLLOID: c_int = 0;
const COLLNAMEENCNSP: c_int = 0;
const DATABASEOID: c_int = 0;
const NAMESPACEOID: c_int = 0;
const DEFAULT_COLLATION_OID: Oid = 100;
const RowExclusiveLock: c_int = 3;
const NoLock: c_int = 0;
const Natts_pg_collation: usize = 12;
const Anum_pg_collation_collcollate: c_int = 8;
const Anum_pg_collation_collctype: c_int = 9;
const Anum_pg_collation_colllocale: c_int = 10;
const Anum_pg_collation_collicurules: c_int = 11;
const Anum_pg_collation_collversion: c_int = 12;
const Anum_pg_database_datcollate: c_int = 0;
const Anum_pg_database_datlocale: c_int = 0;
const PG_SQL_ASCII: c_int = 0;

unsafe fn cstr_to_string(s: *const c_char) -> std::string::String {
    if s.is_null() {
        return std::string::String::new();
    }
    std::ffi::CStr::from_ptr(s).to_string_lossy().into_owned()
}

// stubs for unported helper fns -----------------------------------------------

unsafe fn QualifiedNameGetCreationNamespace(
    _names: *mut List,
    _objname_p: *mut *mut c_char,
) -> Oid {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn object_aclcheck(_classid: Oid, _objectid: Oid, _roleid: Oid, _mode: u32) -> AclResult {
    unimplemented!() // TODO: catalog/aclchk.c
}
unsafe fn GetUserId() -> Oid {
    unimplemented!() // TODO: utils/init/miscinit.c
}
unsafe fn aclcheck_error(_aclerr: AclResult, _objtype: c_int, _objectname: *const c_char) {
    unimplemented!() // TODO: catalog/aclchk.c
}
unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn defGetDefname(_def: *mut DefElem) -> *const c_char {
    unimplemented!() // TODO: nodes/parsenodes.h (defel->defname)
}
unsafe fn errorConflictingDefElem(_defel: *mut DefElem, _pstate: *mut ParseState) {
    unimplemented!() // TODO: commands/define.c
}
unsafe fn list_length(_l: *mut List) -> c_int {
    unimplemented!() // TODO: nodes/list.c
}
unsafe fn get_collation_oid(_name: *mut List, _missing_ok: bool) -> Oid {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn defGetQualifiedName(_def: *mut DefElem) -> *mut List {
    unimplemented!() // TODO: commands/define.c
}
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn HeapTupleIsValid(tup: HeapTuple) -> bool {
    !tup.is_null()
}
unsafe fn GETSTRUCT(_tup: HeapTuple) -> *mut c_void {
    unimplemented!() // TODO: access/htup_details.h
}
unsafe fn SysCacheGetAttr(
    _cacheId: c_int,
    _tup: HeapTuple,
    _attributeNumber: c_int,
    _isNull: *mut bool,
) -> Datum {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn SysCacheGetAttrNotNull(_cacheId: c_int, _tup: HeapTuple, _attributeNumber: c_int) -> Datum {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn TextDatumGetCString(_d: Datum) -> *mut c_char {
    unimplemented!() // TODO: utils/builtins.h
}
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn defGetString(_def: *mut DefElem) -> *mut c_char {
    unimplemented!() // TODO: commands/define.c
}
unsafe fn defGetBoolean(_def: *mut DefElem) -> bool {
    unimplemented!() // TODO: commands/define.c
}
unsafe fn pg_strcasecmp(_s1: *const c_char, _s2: *const c_char) -> c_int {
    unimplemented!() // TODO: port/pgstrcasecmp.c
}
unsafe fn builtin_validate_locale(_encoding: c_int, _locale: *const c_char) -> *const c_char {
    unimplemented!() // TODO: utils/adt/pg_locale.c
}
unsafe fn GetDatabaseEncoding() -> c_int {
    unimplemented!() // TODO: mb/mbutils.c
}
unsafe fn GetDatabaseEncodingName() -> *const c_char {
    unimplemented!() // TODO: mb/mbutils.c
}
unsafe fn icu_language_tag(_loc_str: *const c_char, _elevel: c_int) -> *mut c_char {
    unimplemented!() // TODO: utils/adt/pg_locale.c
}
unsafe fn icu_validate_locale(_loc_str: *const c_char) {
    unimplemented!() // TODO: utils/adt/pg_locale.c
}
unsafe fn builtin_locale_encoding(_locale: *const c_char) -> c_int {
    unimplemented!() // TODO: utils/adt/pg_locale.c
}
unsafe fn check_encoding_locale_matches(
    _encoding: c_int,
    _collate: *const c_char,
    _ctype: *const c_char,
) {
    unimplemented!() // TODO: commands/dbcommands.c
}
unsafe fn get_collation_actual_version(_collprovider: c_char, _collcollate: *const c_char) -> *mut c_char {
    unimplemented!() // TODO: utils/adt/pg_locale.c
}
unsafe fn CollationCreate(
    _collname: *const c_char,
    _collnamespace: Oid,
    _collowner: Oid,
    _collprovider: c_char,
    _collisdeterministic: bool,
    _collencoding: c_int,
    _collcollate: *const c_char,
    _collctype: *const c_char,
    _colllocale: *const c_char,
    _collicurules: *const c_char,
    _collversion: *const c_char,
    _if_not_exists: bool,
    _quiet: bool,
) -> Oid {
    unimplemented!() // TODO: catalog/pg_collation.c
}
unsafe fn OidIsValid(objectId: Oid) -> bool {
    objectId != 0 // InvalidOid == 0
}
unsafe fn InvalidObjectAddress() -> ObjectAddress {
    std::mem::zeroed() // TODO: catalog/objectaddress.h
}
unsafe fn CommandCounterIncrement() {
    unimplemented!() // TODO: access/transam/xact.c
}
unsafe fn pg_newlocale_from_collation(_collid: Oid) -> *mut c_void {
    unimplemented!() // TODO: utils/adt/pg_locale.c
}
unsafe fn ObjectAddressSet(_addr: *mut ObjectAddress, _class_id: Oid, _object_id: Oid) {
    unimplemented!() // TODO: catalog/objectaddress.h
}
unsafe fn SearchSysCacheExists3(_cacheId: c_int, _key1: Datum, _key2: Datum, _key3: Datum) -> bool {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn SearchSysCacheExists1(_cacheId: c_int, _key1: Datum) -> bool {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn CStringGetDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn Int32GetDatum(_i: c_int) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn ObjectIdGetDatum(_oid: Oid) -> Datum {
    unimplemented!() // TODO: postgres.h
}
unsafe fn table_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    unimplemented!() // TODO: access/table.c
}
unsafe fn table_close(_relation: Relation, _lockmode: c_int) {
    unimplemented!() // TODO: access/table.c
}
unsafe fn stmt_collname(_stmt: *mut AlterCollationStmt) -> *mut List {
    unimplemented!() // TODO: nodes/parsenodes.h (stmt->collname)
}
unsafe fn object_ownercheck(_classid: Oid, _objectid: Oid, _roleid: Oid) -> bool {
    unimplemented!() // TODO: catalog/aclchk.c
}
unsafe fn NameListToString(_names: *mut List) -> *const c_char {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn SearchSysCacheCopy1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO: utils/builtins.h
}
unsafe fn heap_modify_tuple(
    _tuple: HeapTuple,
    _tupleDesc: *mut c_void,
    _replValues: *mut Datum,
    _replIsnull: *mut bool,
    _doReplace: *mut bool,
) -> HeapTuple {
    unimplemented!() // TODO: access/common/heaptuple.c
}
unsafe fn RelationGetDescr(_rel: Relation) -> *mut c_void {
    unimplemented!() // TODO: utils/rel.h
}
unsafe fn t_self_ptr(_tup: HeapTuple) -> *mut c_void {
    unimplemented!() // TODO: access/htup.h (&tup->t_self)
}
unsafe fn CatalogTupleUpdate(_heapRel: Relation, _otid: *mut c_void, _tup: HeapTuple) {
    unimplemented!() // TODO: catalog/indexing.c
}
unsafe fn InvokeObjectPostAlterHook(_classId: Oid, _objectId: Oid, _subId: c_int) {
    unimplemented!() // TODO: catalog/objectaccess.h
}
unsafe fn heap_freetuple(_htup: HeapTuple) {
    unimplemented!() // TODO: access/common/heaptuple.c
}
unsafe fn PG_GETARG_OID(_fcinfo: FunctionCallInfo, _n: c_int) -> Oid {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn cstring_to_text(_s: *const c_char) -> *mut c_void {
    unimplemented!() // TODO: utils/adt/varlena.c
}
unsafe fn PG_RETURN_TEXT_P(_x: *mut c_void) -> Datum {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn PG_RETURN_NULL() -> Datum {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn PG_RETURN_INT32(_x: c_int) -> Datum {
    unimplemented!() // TODO: fmgr.h
}
unsafe fn pg_is_ascii(_str: *const c_char) -> bool {
    unimplemented!() // TODO: common/string.c
}
unsafe fn pg_get_encoding_from_locale(_ctype: *const c_char, _write_message: bool) -> c_int {
    unimplemented!() // TODO: port/chklocale.c
}
unsafe fn PG_VALID_BE_ENCODING(_encoding: c_int) -> bool {
    unimplemented!() // TODO: mb/pg_wchar.h
}
unsafe fn superuser() -> bool {
    unimplemented!() // TODO: utils/misc/superuser.c
}
unsafe fn OpenPipeStream(_command: *const c_char, _mode: *const c_char) -> *mut FILE {
    unimplemented!() // TODO: storage/file/fd.c
}
unsafe fn ClosePipeStream(_file: *mut FILE) -> c_int {
    unimplemented!() // TODO: storage/file/fd.c
}
unsafe fn fgets(_s: *mut c_char, _size: c_int, _stream: *mut FILE) -> *mut c_char {
    unimplemented!() // TODO: libc stdio
}
unsafe fn palloc(_size: usize) -> *mut c_void {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}
unsafe fn repalloc(_pointer: *mut c_void, _size: usize) -> *mut c_void {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}
unsafe fn pstrdup(_in: *const c_char) -> *mut c_char {
    unimplemented!() // TODO: utils/mmgr/mcxt.c
}
unsafe fn qsort(
    _base: *mut c_void,
    _nmemb: usize,
    _size: usize,
    _compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
) {
    unimplemented!() // TODO: libc qsort
}

// Globals referenced (stubs) --------------------------------------------------
static IsBinaryUpgrade: bool = false; // TODO: utils/init/globals.c
static icu_validation_level: c_int = 0; // TODO: utils/adt/pg_locale.c
const ERROR: c_int = 21; // elevel; matches elog.h ERROR
const NOTICE: c_int = 18;
const WARNING: c_int = 19;
const DEBUG1: c_int = 14;
