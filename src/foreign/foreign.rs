//! src/backend/foreign/foreign.c
//!
//! support for foreign-data wrappers, servers and user mappings.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group

use crate::prelude::*;

use std::ffi::{c_char, c_int};

use crate::access::attnum::AttrNumber;
use crate::c::{bits16, uint16};
use crate::nodes::nodes::NodeTag;
use crate::nodes::nodes::NodeTag::{T_HashJoin, T_MergeJoin, T_NestLoop};
use crate::nodes::pg_list::{lfirst, List, ListCell, NIL};
use crate::storage::block::BlockNumber;
use crate::utils::mmgr::mcxt::CacheMemoryContext;
use crate::{current_cell, foreach, makeNode, IsA};

// ============================================================================
// foreign.h
// ============================================================================

/* Helper for obtaining username for user mapping */
// #define MappingUserName(userid)
//     (OidIsValid(userid) ? GetUserNameFromId(userid, false) : "public")
#[inline]
unsafe fn MappingUserName(userid: Oid) -> *const c_char {
    if OidIsValid(userid) {
        GetUserNameFromId(userid, false)
    } else {
        c"public".as_ptr()
    }
}

#[repr(C)]
pub struct ForeignDataWrapper {
    pub fdwid: Oid,             /* FDW Oid */
    pub owner: Oid,             /* FDW owner user Oid */
    pub fdwname: *mut c_char,   /* Name of the FDW */
    pub fdwhandler: Oid,        /* Oid of handler function, or 0 */
    pub fdwvalidator: Oid,      /* Oid of validator function, or 0 */
    pub options: *mut List,     /* fdwoptions as DefElem list */
}

#[repr(C)]
pub struct ForeignServer {
    pub serverid: Oid,             /* server Oid */
    pub fdwid: Oid,                /* foreign-data wrapper */
    pub owner: Oid,                /* server owner user Oid */
    pub servername: *mut c_char,   /* name of the server */
    pub servertype: *mut c_char,   /* server type, optional */
    pub serverversion: *mut c_char,/* server version, optional */
    pub options: *mut List,        /* srvoptions as DefElem list */
}

#[repr(C)]
pub struct UserMapping {
    pub umid: Oid,           /* Oid of user mapping */
    pub userid: Oid,         /* local user Oid */
    pub serverid: Oid,       /* server Oid */
    pub options: *mut List,  /* useoptions as DefElem list */
}

#[repr(C)]
pub struct ForeignTable {
    pub relid: Oid,          /* relation Oid */
    pub serverid: Oid,       /* server Oid */
    pub options: *mut List,  /* ftoptions as DefElem list */
}

/* Flags for GetForeignServerExtended */
pub const FSV_MISSING_OK: bits16 = 0x01;

/* Flags for GetForeignDataWrapperExtended */
pub const FDW_MISSING_OK: bits16 = 0x01;

// ============================================================================
// foreign.c
// ============================================================================

/*
 * GetForeignDataWrapper -	look up the foreign-data wrapper by OID.
 */
pub unsafe fn GetForeignDataWrapper(fdwid: Oid) -> *mut ForeignDataWrapper {
    GetForeignDataWrapperExtended(fdwid, 0)
}


/*
 * GetForeignDataWrapperExtended -	look up the foreign-data wrapper
 * by OID. If flags uses FDW_MISSING_OK, return NULL if the object cannot
 * be found instead of raising an error.
 */
pub unsafe fn GetForeignDataWrapperExtended(fdwid: Oid, flags: bits16) -> *mut ForeignDataWrapper {
    let fdwform: Form_pg_foreign_data_wrapper;
    let fdw: *mut ForeignDataWrapper;
    let datum: Datum;
    let tp: HeapTuple;
    let mut isnull: bool = false;

    tp = SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(fdwid));

    if !HeapTupleIsValid(tp) {
        if (flags & FDW_MISSING_OK) == 0 {
            elog!(ERROR, "cache lookup failed for foreign-data wrapper {}", fdwid);
        }
        return std::ptr::null_mut();
    }

    fdwform = GETSTRUCT(tp) as Form_pg_foreign_data_wrapper;

    fdw = palloc(std::mem::size_of::<ForeignDataWrapper>()) as *mut ForeignDataWrapper;
    (*fdw).fdwid = fdwid;
    (*fdw).owner = (*fdwform).fdwowner;
    (*fdw).fdwname = pstrdup(NameStr(&mut (*fdwform).fdwname));
    (*fdw).fdwhandler = (*fdwform).fdwhandler;
    (*fdw).fdwvalidator = (*fdwform).fdwvalidator;

    /* Extract the fdwoptions */
    datum = SysCacheGetAttr(FOREIGNDATAWRAPPEROID,
                            tp,
                            Anum_pg_foreign_data_wrapper_fdwoptions,
                            &mut isnull);
    if isnull {
        (*fdw).options = NIL;
    } else {
        (*fdw).options = untransformRelOptions(datum);
    }

    ReleaseSysCache(tp);

    fdw
}


/*
 * GetForeignDataWrapperByName - look up the foreign-data wrapper
 * definition by name.
 */
pub unsafe fn GetForeignDataWrapperByName(fdwname: *const c_char, missing_ok: bool) -> *mut ForeignDataWrapper {
    let fdwId: Oid = get_foreign_data_wrapper_oid(fdwname, missing_ok);

    if !OidIsValid(fdwId) {
        return std::ptr::null_mut();
    }

    GetForeignDataWrapper(fdwId)
}


/*
 * GetForeignServer - look up the foreign server definition.
 */
pub unsafe fn GetForeignServer(serverid: Oid) -> *mut ForeignServer {
    GetForeignServerExtended(serverid, 0)
}


/*
 * GetForeignServerExtended - look up the foreign server definition. If
 * flags uses FSV_MISSING_OK, return NULL if the object cannot be found
 * instead of raising an error.
 */
pub unsafe fn GetForeignServerExtended(serverid: Oid, flags: bits16) -> *mut ForeignServer {
    let serverform: Form_pg_foreign_server;
    let server: *mut ForeignServer;
    let tp: HeapTuple;
    let mut datum: Datum;
    let mut isnull: bool = false;

    tp = SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(serverid));

    if !HeapTupleIsValid(tp) {
        if (flags & FSV_MISSING_OK) == 0 {
            elog!(ERROR, "cache lookup failed for foreign server {}", serverid);
        }
        return std::ptr::null_mut();
    }

    serverform = GETSTRUCT(tp) as Form_pg_foreign_server;

    server = palloc(std::mem::size_of::<ForeignServer>()) as *mut ForeignServer;
    (*server).serverid = serverid;
    (*server).servername = pstrdup(NameStr(&mut (*serverform).srvname));
    (*server).owner = (*serverform).srvowner;
    (*server).fdwid = (*serverform).srvfdw;

    /* Extract server type */
    datum = SysCacheGetAttr(FOREIGNSERVEROID,
                            tp,
                            Anum_pg_foreign_server_srvtype,
                            &mut isnull);
    (*server).servertype = if isnull { std::ptr::null_mut() } else { TextDatumGetCString(datum) };

    /* Extract server version */
    datum = SysCacheGetAttr(FOREIGNSERVEROID,
                            tp,
                            Anum_pg_foreign_server_srvversion,
                            &mut isnull);
    (*server).serverversion = if isnull { std::ptr::null_mut() } else { TextDatumGetCString(datum) };

    /* Extract the srvoptions */
    datum = SysCacheGetAttr(FOREIGNSERVEROID,
                            tp,
                            Anum_pg_foreign_server_srvoptions,
                            &mut isnull);
    if isnull {
        (*server).options = NIL;
    } else {
        (*server).options = untransformRelOptions(datum);
    }

    ReleaseSysCache(tp);

    server
}


/*
 * GetForeignServerByName - look up the foreign server definition by name.
 */
pub unsafe fn GetForeignServerByName(srvname: *const c_char, missing_ok: bool) -> *mut ForeignServer {
    let serverid: Oid = get_foreign_server_oid(srvname, missing_ok);

    if !OidIsValid(serverid) {
        return std::ptr::null_mut();
    }

    GetForeignServer(serverid)
}


/*
 * GetUserMapping - look up the user mapping.
 *
 * If no mapping is found for the supplied user, we also look for
 * PUBLIC mappings (userid == InvalidOid).
 */
pub unsafe fn GetUserMapping(userid: Oid, serverid: Oid) -> *mut UserMapping {
    let datum: Datum;
    let mut tp: HeapTuple;
    let mut isnull: bool = false;
    let um: *mut UserMapping;

    tp = SearchSysCache2(USERMAPPINGUSERSERVER,
                         ObjectIdGetDatum(userid),
                         ObjectIdGetDatum(serverid));

    if !HeapTupleIsValid(tp) {
        /* Not found for the specific user -- try PUBLIC */
        tp = SearchSysCache2(USERMAPPINGUSERSERVER,
                             ObjectIdGetDatum(InvalidOid),
                             ObjectIdGetDatum(serverid));
    }

    if !HeapTupleIsValid(tp) {
        let server: *mut ForeignServer = GetForeignServer(serverid);

        elog!(ERROR, "user mapping not found for user \"{}\", server \"{}\"",
              MappingUserName(userid) as usize, (*server).servername as usize);
        unreachable!();
    }

    um = palloc(std::mem::size_of::<UserMapping>()) as *mut UserMapping;
    (*um).umid = (*(GETSTRUCT(tp) as Form_pg_user_mapping)).oid;
    (*um).userid = userid;
    (*um).serverid = serverid;

    /* Extract the umoptions */
    datum = SysCacheGetAttr(USERMAPPINGUSERSERVER,
                            tp,
                            Anum_pg_user_mapping_umoptions,
                            &mut isnull);
    if isnull {
        (*um).options = NIL;
    } else {
        (*um).options = untransformRelOptions(datum);
    }

    ReleaseSysCache(tp);

    um
}


/*
 * GetForeignTable - look up the foreign table definition by relation oid.
 */
pub unsafe fn GetForeignTable(relid: Oid) -> *mut ForeignTable {
    let tableform: Form_pg_foreign_table;
    let ft: *mut ForeignTable;
    let tp: HeapTuple;
    let datum: Datum;
    let mut isnull: bool = false;

    tp = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for foreign table {}", relid);
    }
    tableform = GETSTRUCT(tp) as Form_pg_foreign_table;

    ft = palloc(std::mem::size_of::<ForeignTable>()) as *mut ForeignTable;
    (*ft).relid = relid;
    (*ft).serverid = (*tableform).ftserver;

    /* Extract the ftoptions */
    datum = SysCacheGetAttr(FOREIGNTABLEREL,
                            tp,
                            Anum_pg_foreign_table_ftoptions,
                            &mut isnull);
    if isnull {
        (*ft).options = NIL;
    } else {
        (*ft).options = untransformRelOptions(datum);
    }

    ReleaseSysCache(tp);

    ft
}


/*
 * GetForeignColumnOptions - Get attfdwoptions of given relation/attnum
 * as list of DefElem.
 */
pub unsafe fn GetForeignColumnOptions(relid: Oid, attnum: AttrNumber) -> *mut List {
    let options: *mut List;
    let tp: HeapTuple;
    let datum: Datum;
    let mut isnull: bool = false;

    tp = SearchSysCache2(ATTNUM,
                         ObjectIdGetDatum(relid),
                         Int16GetDatum(attnum));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for attribute {} of relation {}",
              attnum, relid);
    }
    datum = SysCacheGetAttr(ATTNUM,
                            tp,
                            Anum_pg_attribute_attfdwoptions,
                            &mut isnull);
    if isnull {
        options = NIL;
    } else {
        options = untransformRelOptions(datum);
    }

    ReleaseSysCache(tp);

    options
}


/*
 * GetFdwRoutine - call the specified foreign-data wrapper handler routine
 * to get its FdwRoutine struct.
 */
pub unsafe fn GetFdwRoutine(fdwhandler: Oid) -> *mut FdwRoutine {
    let datum: Datum;
    let routine: *mut FdwRoutine;

    /* Check if the access to foreign tables is restricted */
    if unlikely((restrict_nonsystem_relation_kind & RESTRICT_RELKIND_FOREIGN_TABLE) != 0) {
        /* there must not be built-in FDW handler  */
        ereport!(ERROR, "access to non-system foreign table is restricted");
    }

    datum = OidFunctionCall0(fdwhandler);
    routine = DatumGetPointer(datum) as *mut FdwRoutine;

    if routine.is_null() || !IsA_FdwRoutine(routine) {
        elog!(ERROR, "foreign-data wrapper handler function {} did not return an FdwRoutine struct",
              fdwhandler);
    }

    routine
}


/*
 * GetForeignServerIdByRelId - look up the foreign server
 * for the given foreign table, and return its OID.
 */
pub unsafe fn GetForeignServerIdByRelId(relid: Oid) -> Oid {
    let tp: HeapTuple;
    let tableform: Form_pg_foreign_table;
    let serverid: Oid;

    tp = SearchSysCache1(FOREIGNTABLEREL, ObjectIdGetDatum(relid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for foreign table {}", relid);
    }
    tableform = GETSTRUCT(tp) as Form_pg_foreign_table;
    serverid = (*tableform).ftserver;
    ReleaseSysCache(tp);

    serverid
}


/*
 * GetFdwRoutineByServerId - look up the handler of the foreign-data wrapper
 * for the given foreign server, and retrieve its FdwRoutine struct.
 */
pub unsafe fn GetFdwRoutineByServerId(serverid: Oid) -> *mut FdwRoutine {
    let mut tp: HeapTuple;
    let fdwform: Form_pg_foreign_data_wrapper;
    let serverform: Form_pg_foreign_server;
    let fdwid: Oid;
    let fdwhandler: Oid;

    /* Get foreign-data wrapper OID for the server. */
    tp = SearchSysCache1(FOREIGNSERVEROID, ObjectIdGetDatum(serverid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for foreign server {}", serverid);
    }
    serverform = GETSTRUCT(tp) as Form_pg_foreign_server;
    fdwid = (*serverform).srvfdw;
    ReleaseSysCache(tp);

    /* Get handler function OID for the FDW. */
    tp = SearchSysCache1(FOREIGNDATAWRAPPEROID, ObjectIdGetDatum(fdwid));
    if !HeapTupleIsValid(tp) {
        elog!(ERROR, "cache lookup failed for foreign-data wrapper {}", fdwid);
    }
    fdwform = GETSTRUCT(tp) as Form_pg_foreign_data_wrapper;
    fdwhandler = (*fdwform).fdwhandler;

    /* Complain if FDW has been set to NO HANDLER. */
    if !OidIsValid(fdwhandler) {
        elog!(ERROR, "foreign-data wrapper \"{}\" has no handler",
              NameStr(&mut (*fdwform).fdwname) as usize);
    }

    ReleaseSysCache(tp);

    /* And finally, call the handler function. */
    GetFdwRoutine(fdwhandler)
}


/*
 * GetFdwRoutineByRelId - look up the handler of the foreign-data wrapper
 * for the given foreign table, and retrieve its FdwRoutine struct.
 */
pub unsafe fn GetFdwRoutineByRelId(relid: Oid) -> *mut FdwRoutine {
    let serverid: Oid;

    /* Get server OID for the foreign table. */
    serverid = GetForeignServerIdByRelId(relid);

    /* Now retrieve server's FdwRoutine struct. */
    GetFdwRoutineByServerId(serverid)
}

/*
 * GetFdwRoutineForRelation - look up the handler of the foreign-data wrapper
 * for the given foreign table, and retrieve its FdwRoutine struct.
 *
 * This function is preferred over GetFdwRoutineByRelId because it caches
 * the data in the relcache entry, saving a number of catalog lookups.
 *
 * If makecopy is true then the returned data is freshly palloc'd in the
 * caller's memory context.  Otherwise, it's a pointer to the relcache data,
 * which will be lost in any relcache reset --- so don't rely on it long.
 */
pub unsafe fn GetFdwRoutineForRelation(relation: Relation, makecopy: bool) -> *mut FdwRoutine {
    let fdwroutine: *mut FdwRoutine;
    let cfdwroutine: *mut FdwRoutine;

    if (*relation).rd_fdwroutine.is_null() {
        /* Get the info by consulting the catalogs and the FDW code */
        fdwroutine = GetFdwRoutineByRelId(RelationGetRelid(relation));

        /* Save the data for later reuse in CacheMemoryContext */
        cfdwroutine = MemoryContextAlloc(CacheMemoryContext as *mut _,
                                         std::mem::size_of::<FdwRoutine>()) as *mut FdwRoutine;
        memcpy(cfdwroutine as *mut _, fdwroutine as *const _, std::mem::size_of::<FdwRoutine>());
        (*relation).rd_fdwroutine = cfdwroutine as *mut _;

        /* Give back the locally palloc'd copy regardless of makecopy */
        return fdwroutine;
    }

    /* We have valid cached data --- does the caller want a copy? */
    if makecopy {
        let fdwroutine: *mut FdwRoutine = palloc(std::mem::size_of::<FdwRoutine>()) as *mut FdwRoutine;
        memcpy(fdwroutine as *mut _, (*relation).rd_fdwroutine as *const _, std::mem::size_of::<FdwRoutine>());
        return fdwroutine;
    }

    /* Only a short-lived reference is needed, so just hand back cached copy */
    (*relation).rd_fdwroutine as *mut FdwRoutine
}


/*
 * IsImportableForeignTable - filter table names for IMPORT FOREIGN SCHEMA
 *
 * Returns true if given table name should be imported according to the
 * statement's import filter options.
 */
pub unsafe fn IsImportableForeignTable(tablename: *const c_char,
                                       stmt: *mut ImportForeignSchemaStmt) -> bool {
    match (*stmt).list_type {
        FDW_IMPORT_SCHEMA_ALL => {
            return true;
        }

        FDW_IMPORT_SCHEMA_LIMIT_TO => {
            foreach!(lc, (*stmt).table_list, {
                let rv: *mut RangeVar = lfirst(current_cell!(lc)) as *mut RangeVar;

                if strcmp(tablename, (*rv).relname) == 0 {
                    return true;
                }
            });
            return false;
        }

        FDW_IMPORT_SCHEMA_EXCEPT => {
            foreach!(lc, (*stmt).table_list, {
                let rv: *mut RangeVar = lfirst(current_cell!(lc)) as *mut RangeVar;

                if strcmp(tablename, (*rv).relname) == 0 {
                    return false;
                }
            });
            return true;
        }

        _ => {}
    }
    false /* shouldn't get here */
}


/*
 * pg_options_to_table - Convert options array to name/value table
 *
 * This is useful to provide details for information_schema and pg_dump.
 */
pub unsafe fn pg_options_to_table(fcinfo: FunctionCallInfo) -> Datum {
    let array: Datum = PG_GETARG_DATUM(fcinfo, 0);
    let options: *mut List;
    let rsinfo: *mut ReturnSetInfo;

    options = untransformRelOptions(array);
    rsinfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;

    /* prepare the result set */
    InitMaterializedSRF(fcinfo, MAT_SRF_USE_EXPECTED_DESC);

    foreach!(cell, options, {
        let def: *mut DefElem = lfirst(current_cell!(cell)) as *mut DefElem;
        let mut values: [Datum; 2] = [0; 2];
        let mut nulls: [bool; 2] = [false; 2];

        values[0] = CStringGetTextDatum((*def).defname);
        nulls[0] = false;
        if !(*def).arg.is_null() {
            values[1] = CStringGetTextDatum(strVal((*def).arg));
            nulls[1] = false;
        } else {
            values[1] = 0 as Datum;
            nulls[1] = true;
        }
        tuplestore_putvalues((*rsinfo).setResult, (*rsinfo).setDesc,
                             values.as_mut_ptr(), nulls.as_mut_ptr());
    });

    0 as Datum
}


/*
 * Describes the valid options for postgresql FDW, server, and user mapping.
 */
#[repr(C)]
struct ConnectionOption {
    optname: *const c_char,
    optcontext: Oid,        /* Oid of catalog in which option may appear */
}

/*
 * Copied from fe-connect.c PQconninfoOptions.
 *
 * The list is small - don't bother with bsearch if it stays so.
 */
static libpq_conninfo_options: [ConnectionOption; 16] = [
    ConnectionOption { optname: c"authtype".as_ptr(),       optcontext: ForeignServerRelationId },
    ConnectionOption { optname: c"service".as_ptr(),        optcontext: ForeignServerRelationId },
    ConnectionOption { optname: c"user".as_ptr(),           optcontext: UserMappingRelationId },
    ConnectionOption { optname: c"password".as_ptr(),       optcontext: UserMappingRelationId },
    ConnectionOption { optname: c"connect_timeout".as_ptr(),optcontext: ForeignServerRelationId },
    ConnectionOption { optname: c"dbname".as_ptr(),         optcontext: ForeignServerRelationId },
    ConnectionOption { optname: c"host".as_ptr(),           optcontext: ForeignServerRelationId },
    ConnectionOption { optname: c"hostaddr".as_ptr(),       optcontext: ForeignServerRelationId },
    ConnectionOption { optname: c"port".as_ptr(),           optcontext: ForeignServerRelationId },
    ConnectionOption { optname: c"tty".as_ptr(),            optcontext: ForeignServerRelationId },
    ConnectionOption { optname: c"options".as_ptr(),        optcontext: ForeignServerRelationId },
    ConnectionOption { optname: c"requiressl".as_ptr(),     optcontext: ForeignServerRelationId },
    ConnectionOption { optname: c"sslmode".as_ptr(),        optcontext: ForeignServerRelationId },
    ConnectionOption { optname: c"gsslib".as_ptr(),         optcontext: ForeignServerRelationId },
    ConnectionOption { optname: c"gssdelegation".as_ptr(),  optcontext: ForeignServerRelationId },
    ConnectionOption { optname: std::ptr::null(),           optcontext: InvalidOid },
];

// SAFETY: raw pointers in static; crate-wide allows static_mut_refs etc.
unsafe impl Sync for ConnectionOption {}


/*
 * Check if the provided option is one of libpq conninfo options.
 * context is the Oid of the catalog the option came from, or 0 if we
 * don't care.
 */
unsafe fn is_conninfo_option(option: *const c_char, context: Oid) -> bool {
    let mut i = 0;
    while !libpq_conninfo_options[i].optname.is_null() {
        let opt = &libpq_conninfo_options[i];
        if context == opt.optcontext && strcmp(opt.optname, option) == 0 {
            return true;
        }
        i += 1;
    }
    false
}


/*
 * Validate the generic option given to SERVER or USER MAPPING.
 * Raise an ERROR if the option or its value is considered invalid.
 *
 * Valid server options are all libpq conninfo options except
 * user and password -- these may only appear in USER MAPPING options.
 *
 * Caution: this function is deprecated, and is now meant only for testing
 * purposes, because the list of options it knows about doesn't necessarily
 * square with those known to whichever libpq instance you might be using.
 * Inquire of libpq itself, instead.
 */
pub unsafe fn postgresql_fdw_validator(fcinfo: FunctionCallInfo) -> Datum {
    let options_list: *mut List = untransformRelOptions(PG_GETARG_DATUM(fcinfo, 0));
    let catalog: Oid = PG_GETARG_OID(fcinfo, 1);

    foreach!(cell, options_list, {
        let def: *mut DefElem = lfirst(current_cell!(cell)) as *mut DefElem;

        if !is_conninfo_option((*def).defname, catalog) {
            let mut closest_match: *const c_char;
            let mut match_state: ClosestMatchState = std::mem::zeroed();
            let mut has_valid_options: bool = false;

            /*
             * Unknown option specified, complain about it. Provide a hint
             * with a valid option that looks similar, if there is one.
             */
            initClosestMatch(&mut match_state, (*def).defname, 4);
            let mut i = 0;
            while !libpq_conninfo_options[i].optname.is_null() {
                let opt = &libpq_conninfo_options[i];
                if catalog == opt.optcontext {
                    has_valid_options = true;
                    updateClosestMatch(&mut match_state, opt.optname);
                }
                i += 1;
            }

            closest_match = getClosestMatch(&mut match_state);
            let _ = &mut closest_match;
            let _ = has_valid_options;
            elog!(ERROR, "invalid option \"{}\"", (*def).defname as usize);

            #[allow(unreachable_code)]
            {
                PG_RETURN_BOOL(false);
            }
        }
    });

    PG_RETURN_BOOL(true)
}


/*
 * get_foreign_data_wrapper_oid - given a FDW name, look up the OID
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return InvalidOid.
 */
pub unsafe fn get_foreign_data_wrapper_oid(fdwname: *const c_char, missing_ok: bool) -> Oid {
    let oid: Oid;

    oid = GetSysCacheOid1(FOREIGNDATAWRAPPERNAME,
                          Anum_pg_foreign_data_wrapper_oid,
                          CStringGetDatum(fdwname));
    if !OidIsValid(oid) && !missing_ok {
        elog!(ERROR, "foreign-data wrapper \"{}\" does not exist", fdwname as usize);
    }
    oid
}


/*
 * get_foreign_server_oid - given a server name, look up the OID
 *
 * If missing_ok is false, throw an error if name not found.  If true, just
 * return InvalidOid.
 */
pub unsafe fn get_foreign_server_oid(servername: *const c_char, missing_ok: bool) -> Oid {
    let oid: Oid;

    oid = GetSysCacheOid1(FOREIGNSERVERNAME, Anum_pg_foreign_server_oid,
                          CStringGetDatum(servername));
    if !OidIsValid(oid) && !missing_ok {
        elog!(ERROR, "server \"{}\" does not exist", servername as usize);
    }
    oid
}

/*
 * Get a copy of an existing local path for a given join relation.
 *
 * This function is usually helpful to obtain an alternate local path for EPQ
 * checks.
 *
 * Right now, this function only supports unparameterized foreign joins, so we
 * only search for unparameterized path in the given list of paths. Since we
 * are searching for a path which can be used to construct an alternative local
 * plan for a foreign join, we look for only MergeJoin, HashJoin or NestLoop
 * paths.
 *
 * If the inner or outer subpath of the chosen path is a ForeignScan, we
 * replace it with its outer subpath.  For this reason, and also because the
 * planner might free the original path later, the path returned by this
 * function is a shallow copy of the original.  There's no need to copy
 * the substructure, so we don't.
 *
 * Since the plan created using this path will presumably only be used to
 * execute EPQ checks, efficiency of the path is not a concern. But since the
 * path list in RelOptInfo is anyway sorted by total cost we are likely to
 * choose the most efficient path, which is all for the best.
 */
#[allow(unreachable_code)]
pub unsafe fn GetExistingLocalJoinPath(joinrel: *mut RelOptInfo) -> *mut Path {
    Assert!(IS_JOIN_REL(joinrel));

    foreach!(lc, (*joinrel).pathlist, {
        let path: *mut Path = lfirst(current_cell!(lc)) as *mut Path;
        let mut joinpath: *mut JoinPath = std::ptr::null_mut();

        /* Skip parameterized paths. */
        if !(*path).param_info.is_null() {
            continue;
        }

        match (*path).pathtype {
            T_HashJoin => {
                let hash_path: *mut HashPath = makeNode!(HashPath, T_HashPath);

                memcpy(hash_path as *mut _, path as *const _, std::mem::size_of::<HashPath>());
                joinpath = hash_path as *mut JoinPath;
            }

            T_NestLoop => {
                let nest_path: *mut NestPath = makeNode!(NestPath, T_NestPath);

                memcpy(nest_path as *mut _, path as *const _, std::mem::size_of::<NestPath>());
                joinpath = nest_path as *mut JoinPath;
            }

            T_MergeJoin => {
                let merge_path: *mut MergePath = makeNode!(MergePath, T_MergePath);

                memcpy(merge_path as *mut _, path as *const _, std::mem::size_of::<MergePath>());
                joinpath = merge_path as *mut JoinPath;
            }

            _ => {
                /*
                 * Just skip anything else. We don't know if corresponding
                 * plan would build the output row from whole-row references
                 * of base relations and execute the EPQ checks.
                 */
            }
        }

        /* This path isn't good for us, check next. */
        if joinpath.is_null() {
            continue;
        }

        /*
         * If either inner or outer path is a ForeignPath corresponding to a
         * pushed down join, replace it with the fdw_outerpath, so that we
         * maintain path for EPQ checks built entirely of local join
         * strategies.
         */
        if IsA!((*joinpath).outerjoinpath, T_ForeignPath) {
            let foreign_path: *mut ForeignPath;

            foreign_path = (*joinpath).outerjoinpath as *mut ForeignPath;
            if IS_JOIN_REL((*foreign_path).path.parent) {
                (*joinpath).outerjoinpath = (*foreign_path).fdw_outerpath;

                if (*joinpath).path.pathtype == T_MergeJoin {
                    let merge_path: *mut MergePath = joinpath as *mut MergePath;

                    /*
                     * If the new outer path is already well enough ordered
                     * for the mergejoin, we can skip doing an explicit sort.
                     */
                    if !(*merge_path).outersortkeys.is_null() &&
                        pathkeys_count_contained_in((*merge_path).outersortkeys,
                                                    (*(*joinpath).outerjoinpath).pathkeys,
                                                    &mut (*merge_path).outer_presorted_keys) {
                        (*merge_path).outersortkeys = NIL;
                    }
                }
            }
        }

        if IsA!((*joinpath).innerjoinpath, T_ForeignPath) {
            let foreign_path: *mut ForeignPath;

            foreign_path = (*joinpath).innerjoinpath as *mut ForeignPath;
            if IS_JOIN_REL((*foreign_path).path.parent) {
                (*joinpath).innerjoinpath = (*foreign_path).fdw_outerpath;

                if (*joinpath).path.pathtype == T_MergeJoin {
                    let merge_path: *mut MergePath = joinpath as *mut MergePath;

                    /*
                     * If the new inner path is already well enough ordered
                     * for the mergejoin, we can skip doing an explicit sort.
                     */
                    if !(*merge_path).innersortkeys.is_null() &&
                        pathkeys_contained_in((*merge_path).innersortkeys,
                                              (*(*joinpath).innerjoinpath).pathkeys) {
                        (*merge_path).innersortkeys = NIL;
                    }
                }
            }
        }

        return joinpath as *mut Path;
    });
    std::ptr::null_mut()
}


// ============================================================================
// Local stubs for unported dependencies
// ============================================================================

// Catalog Oid constants
const ForeignServerRelationId: Oid = 1417; // TODO: catalog/pg_foreign_server.h
const UserMappingRelationId: Oid = 1418;   // TODO: catalog/pg_user_mapping.h

// SysCache enum ids (treated as opaque c_int)
const FOREIGNDATAWRAPPEROID: c_int = 0; // TODO: utils/syscache.h
const FOREIGNDATAWRAPPERNAME: c_int = 0; // TODO: utils/syscache.h
const FOREIGNSERVEROID: c_int = 0;      // TODO: utils/syscache.h
const FOREIGNSERVERNAME: c_int = 0;     // TODO: utils/syscache.h
const FOREIGNTABLEREL: c_int = 0;       // TODO: utils/syscache.h
const USERMAPPINGUSERSERVER: c_int = 0; // TODO: utils/syscache.h
const ATTNUM: c_int = 0;                // TODO: utils/syscache.h

// Catalog attribute number constants
const Anum_pg_foreign_data_wrapper_fdwoptions: c_int = 0; // TODO: catalog/pg_foreign_data_wrapper.h
const Anum_pg_foreign_data_wrapper_oid: c_int = 0;        // TODO: catalog/pg_foreign_data_wrapper.h
const Anum_pg_foreign_server_srvtype: c_int = 0;          // TODO: catalog/pg_foreign_server.h
const Anum_pg_foreign_server_srvversion: c_int = 0;       // TODO: catalog/pg_foreign_server.h
const Anum_pg_foreign_server_srvoptions: c_int = 0;       // TODO: catalog/pg_foreign_server.h
const Anum_pg_foreign_server_oid: c_int = 0;              // TODO: catalog/pg_foreign_server.h
const Anum_pg_foreign_table_ftoptions: c_int = 0;         // TODO: catalog/pg_foreign_table.h
const Anum_pg_user_mapping_umoptions: c_int = 0;          // TODO: catalog/pg_user_mapping.h
const Anum_pg_attribute_attfdwoptions: c_int = 0;         // TODO: catalog/pg_attribute.h

// restrict_nonsystem_relation_kind bits (from tcop/tcopprot.h)
const RESTRICT_RELKIND_FOREIGN_TABLE: c_int = 0x02; // TODO: tcop/tcopprot.h
static mut restrict_nonsystem_relation_kind: c_int = 0; // TODO: tcop/tcopprot.h

// ImportForeignSchemaType values (parsenodes.h)
const FDW_IMPORT_SCHEMA_ALL: c_int = 0;      // TODO: nodes/parsenodes.h
const FDW_IMPORT_SCHEMA_LIMIT_TO: c_int = 1; // TODO: nodes/parsenodes.h
const FDW_IMPORT_SCHEMA_EXCEPT: c_int = 2;   // TODO: nodes/parsenodes.h

// MAT_SRF flags (funcapi.h)
const MAT_SRF_USE_EXPECTED_DESC: c_int = 0x01; // TODO: funcapi.h

// ---- Opaque catalog Form pointer types ----
#[repr(C)]
pub struct FormData_pg_foreign_data_wrapper {
    pub oid: Oid,
    pub fdwname: NameData,
    pub fdwowner: Oid,
    pub fdwhandler: Oid,
    pub fdwvalidator: Oid,
}
pub type Form_pg_foreign_data_wrapper = *mut FormData_pg_foreign_data_wrapper;

#[repr(C)]
pub struct FormData_pg_foreign_server {
    pub oid: Oid,
    pub srvname: NameData,
    pub srvowner: Oid,
    pub srvfdw: Oid,
}
pub type Form_pg_foreign_server = *mut FormData_pg_foreign_server;

#[repr(C)]
pub struct FormData_pg_foreign_table {
    pub oid: Oid,
    pub ftrelid: Oid,
    pub ftserver: Oid,
}
pub type Form_pg_foreign_table = *mut FormData_pg_foreign_table;

#[repr(C)]
pub struct FormData_pg_user_mapping {
    pub oid: Oid,
    pub umuser: Oid,
    pub umserver: Oid,
}
pub type Form_pg_user_mapping = *mut FormData_pg_user_mapping;

#[repr(C)]
pub struct NameData {
    pub data: [c_char; 64], // NAMEDATALEN
}

// FdwRoutine struct (foreign/fdwapi.h)
#[repr(C)]
pub struct FdwRoutine {
    pub _type: NodeTag,
    // ... remaining fields omitted; only size/tag is used here. TODO: foreign/fdwapi.h
}

// Planner/path node types (nodes/pathnodes.h)
#[repr(C)]
pub struct RelOptInfo {
    pub pathlist: *mut List,
    // TODO: nodes/pathnodes.h
}

#[repr(C)]
pub struct Path {
    pub _type: NodeTag,
    pub pathtype: NodeTag,
    pub parent: *mut RelOptInfo,
    pub param_info: *mut ParamPathInfo,
    pub pathkeys: *mut List,
    // TODO: nodes/pathnodes.h
}

#[repr(C)]
pub struct ParamPathInfo {
    pub _type: NodeTag,
    // TODO: nodes/pathnodes.h
}

#[repr(C)]
pub struct JoinPath {
    pub path: Path,
    pub outerjoinpath: *mut Path,
    pub innerjoinpath: *mut Path,
    // TODO: nodes/pathnodes.h
}

#[repr(C)]
pub struct HashPath {
    pub jpath: JoinPath,
    // TODO: nodes/pathnodes.h
}

#[repr(C)]
pub struct NestPath {
    pub jpath: JoinPath,
    // TODO: nodes/pathnodes.h
}

#[repr(C)]
pub struct MergePath {
    pub jpath: JoinPath,
    pub outersortkeys: *mut List,
    pub innersortkeys: *mut List,
    pub outer_presorted_keys: c_int,
    // TODO: nodes/pathnodes.h
}

#[repr(C)]
pub struct ForeignPath {
    pub path: Path,
    pub fdw_outerpath: *mut Path,
    // TODO: nodes/pathnodes.h
}

// parsenodes.h node types
#[repr(C)]
pub struct ImportForeignSchemaStmt {
    pub _type: NodeTag,
    pub list_type: c_int,
    pub table_list: *mut List,
    // TODO: nodes/parsenodes.h
}

#[repr(C)]
pub struct RangeVar {
    pub _type: NodeTag,
    pub relname: *mut c_char,
    // TODO: nodes/parsenodes.h
}

#[repr(C)]
pub struct DefElem {
    pub _type: NodeTag,
    pub defname: *mut c_char,
    pub arg: *mut Node,
    // TODO: nodes/parsenodes.h
}

// ReturnSetInfo / FunctionCallInfo (fmgr.h, funcapi.h)
#[repr(C)]
pub struct ReturnSetInfo {
    pub _type: NodeTag,
    pub setResult: *mut Tuplestorestate,
    pub setDesc: TupleDesc,
    // TODO: funcapi.h / fmgr.h
}

pub type FunctionCallInfo = *mut FunctionCallInfoBaseData;

#[repr(C)]
pub struct FunctionCallInfoBaseData {
    pub resultinfo: *mut fmNodePtr,
    // TODO: fmgr.h
}

pub enum fmNodePtr {}
pub enum Tuplestorestate {}
pub enum Node {}
pub type TupleDesc = *mut TupleDescData;
pub enum TupleDescData {}
pub type Relation = *mut RelationData;
#[repr(C)]
pub struct RelationData {
    pub rd_fdwroutine: *mut FdwRoutineData,
    // TODO: utils/rel.h
}
pub enum FdwRoutineData {}

pub enum HeapTupleData {}
pub type HeapTuple = *mut HeapTupleData;

// ClosestMatch (utils/varlena.h)
#[repr(C)]
pub struct ClosestMatchState {
    pub source: *const c_char,
    pub min_d: c_int,
    pub max_d: c_int,
    pub match_: *const c_char,
}

// ---- Function stubs ----
unsafe fn GetUserNameFromId(_userid: Oid, _noerr: bool) -> *const c_char { unimplemented!() /* TODO: utils/acl.c */ }
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple { unimplemented!() /* TODO: utils/cache/syscache.c */ }
unsafe fn SearchSysCache2(_cacheId: c_int, _key1: Datum, _key2: Datum) -> HeapTuple { unimplemented!() /* TODO: utils/cache/syscache.c */ }
unsafe fn ReleaseSysCache(_tuple: HeapTuple) { unimplemented!() /* TODO: utils/cache/syscache.c */ }
unsafe fn SysCacheGetAttr(_cacheId: c_int, _tup: HeapTuple, _attributeNumber: c_int, _isNull: *mut bool) -> Datum { unimplemented!() /* TODO: utils/cache/syscache.c */ }
unsafe fn GetSysCacheOid1(_cacheId: c_int, _oidcol: c_int, _key1: Datum) -> Oid { unimplemented!() /* TODO: utils/cache/syscache.c */ }
unsafe fn HeapTupleIsValid(tup: HeapTuple) -> bool { !tup.is_null() /* TODO: access/htup.h */ }
unsafe fn GETSTRUCT(_tup: HeapTuple) -> *mut std::ffi::c_void { unimplemented!() /* TODO: access/htup_details.h */ }
unsafe fn NameStr(name: *mut NameData) -> *mut c_char { (*name).data.as_mut_ptr() /* TODO: c.h */ }
unsafe fn pstrdup(_s: *const c_char) -> *mut c_char { unimplemented!() /* TODO: utils/mmgr/mcxt.c */ }
unsafe fn untransformRelOptions(_options: Datum) -> *mut List { unimplemented!() /* TODO: access/common/reloptions.c */ }
unsafe fn TextDatumGetCString(_d: Datum) -> *mut c_char { unimplemented!() /* TODO: utils/builtins.h */ }
unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum { unimplemented!() /* TODO: utils/builtins.h */ }
unsafe fn CStringGetDatum(_s: *const c_char) -> Datum { unimplemented!() /* TODO: postgres.h */ }
unsafe fn Int16GetDatum(_i: AttrNumber) -> Datum { unimplemented!() /* TODO: postgres.h */ }
unsafe fn OidFunctionCall0(_functionId: Oid) -> Datum { unimplemented!() /* TODO: utils/fmgr.c */ }
unsafe fn RelationGetRelid(_relation: Relation) -> Oid { unimplemented!() /* TODO: utils/rel.h */ }
unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: c_int) { unimplemented!() /* TODO: utils/fmgr/funcapi.c */ }
unsafe fn tuplestore_putvalues(_state: *mut Tuplestorestate, _tdesc: TupleDesc, _values: *mut Datum, _isnull: *mut bool) { unimplemented!() /* TODO: utils/sort/tuplestore.c */ }
unsafe fn strVal(_v: *mut Node) -> *const c_char { unimplemented!() /* TODO: nodes/value.h */ }
unsafe fn initClosestMatch(_state: *mut ClosestMatchState, _source: *const c_char, _max_d: c_int) { unimplemented!() /* TODO: utils/adt/varlena.c */ }
unsafe fn updateClosestMatch(_state: *mut ClosestMatchState, _candidate: *const c_char) { unimplemented!() /* TODO: utils/adt/varlena.c */ }
unsafe fn getClosestMatch(_state: *mut ClosestMatchState) -> *const c_char { unimplemented!() /* TODO: utils/adt/varlena.c */ }
unsafe fn pathkeys_count_contained_in(_keys1: *mut List, _keys2: *mut List, _n_common: *mut c_int) -> bool { unimplemented!() /* TODO: optimizer/path/pathkeys.c */ }
unsafe fn pathkeys_contained_in(_keys1: *mut List, _keys2: *mut List) -> bool { unimplemented!() /* TODO: optimizer/path/pathkeys.c */ }
unsafe fn IS_JOIN_REL(_rel: *mut RelOptInfo) -> bool { unimplemented!() /* TODO: nodes/pathnodes.h */ }
unsafe fn PG_GETARG_DATUM(_fcinfo: FunctionCallInfo, _n: c_int) -> Datum { unimplemented!() /* TODO: fmgr.h */ }
unsafe fn PG_GETARG_OID(_fcinfo: FunctionCallInfo, _n: c_int) -> Oid { unimplemented!() /* TODO: fmgr.h */ }
unsafe fn PG_RETURN_BOOL(_b: bool) -> Datum { unimplemented!() /* TODO: fmgr.h */ }
// `unlikely` is provided by crate::c (glob-imported via the prelude); no local copy.
// IsA(node, FdwRoutine): this port's NodeTag enum has no T_FdwRoutine variant yet,
// so the IsA! macro cannot be used here. Stub the predicate. TODO: add T_FdwRoutine.
unsafe fn IsA_FdwRoutine(_node: *mut FdwRoutine) -> bool { unimplemented!() /* TODO: nodes/nodes.h T_FdwRoutine */ }

extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn memcpy(dest: *mut std::ffi::c_void, src: *const std::ffi::c_void, n: usize) -> *mut std::ffi::c_void;
}
