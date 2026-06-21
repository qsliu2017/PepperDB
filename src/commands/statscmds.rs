//! src/backend/commands/statscmds.c
//!
//! Commands for creating and altering extended statistics objects
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

// Function-like macros live at the crate root (#[macro_export]).
use crate::{IsA, foreach, current_cell, lfirst_node};

// Node/list infrastructure.
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::pg_list::{List, NIL, list_length, lfirst};

// Parse/primitive node structs used by the statistics commands.
use crate::nodes::parsenodes::{
    AlterStatsStmt, CreateStatsStmt, StatsElem, AclMode, ACL_CREATE, ObjectType,
};
use crate::nodes::parsenodes::ObjectType::{OBJECT_SCHEMA, OBJECT_STATISTIC_EXT};
use crate::nodes::primnodes::{RangeVar, Var};
use crate::nodes::bitmapset::Bitmapset;

// Relation/relcache helpers.
use crate::utils::rel::{
    Relation, RelationGetRelid, RelationGetRelationName, RelationGetNamespace, RelationGetDescr,
};

// Tuple/descriptor types.
use crate::access::htup_details::{HeapTuple, HeapTupleData, HeapTupleIsValid, GETSTRUCT};
use crate::access::common::tupdesc::TupleDesc;
use crate::access::attnum::AttrNumber;
use crate::access::sysattr::FirstLowInvalidHeapAttributeNumber;

// Catalog OIDs and column/attribute metadata.
use crate::catalog::catalog_oids::{
    RelationRelationId, NamespaceRelationId, StatisticExtRelationId, StatisticExtDataRelationId,
};
use crate::catalog::pg_class::{
    RELKIND_RELATION, RELKIND_MATVIEW, RELKIND_FOREIGN_TABLE, RELKIND_PARTITIONED_TABLE,
};
use crate::catalog::pg_attribute::{Form_pg_attribute, ATTRIBUTE_GENERATED_VIRTUAL};
use crate::catalog::pg_statistic_ext::Form_pg_statistic_ext;
use crate::catalog::pg_type_d::CHAROID;

// Lock modes.
use crate::storage::lockdefs::{NoLock, RowExclusiveLock, ShareUpdateExclusiveLock};

// Item pointer (CatalogTuple* helpers take an ItemPointer).
use crate::storage::itemptr::ItemPointerData;

// Array type for the stxkind catalog column.
use crate::utils::array::ArrayType;

// Statistics limits/constants.
use crate::statistics::statistics::{STATS_MAX_DIMENSIONS, MAX_STATISTICS_TARGET};

// Name length limit (pg_config_manual.h / pg_config.h).
use crate::pg_config::NAMEDATALEN;

// Owner/misc globals.
use crate::miscadmin::{GetUserId, allowSystemTableMods};

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn qsort(
        base: *mut c_void,
        nmemb: usize,
        size: usize,
        compar: Option<unsafe extern "C" fn(*const c_void, *const c_void) -> c_int>,
    );
}

/* qsort comparator for the attnums in CreateStatistics */
unsafe extern "C" fn compare_int16(a: *const c_void, b: *const c_void) -> c_int {
    let av: c_int = *(a as *const int16) as c_int;
    let bv: c_int = *(b as *const int16) as c_int;

    /* this can't overflow if int is wider than int16 */
    av - bv
}

/*
 *		CREATE STATISTICS
 */
pub unsafe fn CreateStatistics(stmt: *mut CreateStatsStmt, check_rights: bool) -> ObjectAddress {
    let mut attnums: [int16; STATS_MAX_DIMENSIONS as usize] =
        [0; STATS_MAX_DIMENSIONS as usize];
    let mut nattnums: c_int = 0;
    let numcols: c_int;
    let namestr: *mut c_char;
    let mut stxname: NameData = std::mem::zeroed();
    let statoid: Oid;
    let namespaceId: Oid;
    let stxowner: Oid = GetUserId();
    let htup: HeapTuple;
    let mut values: [Datum; Natts_pg_statistic_ext as usize] =
        [0; Natts_pg_statistic_ext as usize];
    let mut nulls: [bool; Natts_pg_statistic_ext as usize] =
        [false; Natts_pg_statistic_ext as usize];
    let stxkeys: *mut int2vector;
    let mut stxexprs: *mut List = NIL as *mut List;
    let exprsDatum: Datum;
    let statrel: Relation;
    let mut rel: Relation = std::ptr::null_mut();
    let relid: Oid;
    let mut parentobject: ObjectAddress = std::mem::zeroed();
    let mut myself: ObjectAddress = std::mem::zeroed();
    let mut types: [Datum; 4] = [0; 4]; /* one for each possible type of statistic */
    let mut ntypes: c_int;
    let stxkind: *mut ArrayType;
    let mut build_ndistinct: bool;
    let mut build_dependencies: bool;
    let mut build_mcv: bool;
    let build_expressions: bool;
    let mut requested_type: bool = false;
    let mut i: c_int;

    Assert!(IsA!(stmt, T_CreateStatsStmt));

    /*
     * Examine the FROM clause.  Currently, we only allow it to be a single
     * simple table, but later we'll probably allow multiple tables and JOIN
     * syntax.  The grammar is already prepared for that, so we have to check
     * here that what we got is what we can support.
     */
    if list_length((*stmt).relations) != 1 {
        ereport!(
            ERROR,
            "only a single relation is allowed in CREATE STATISTICS"
        );
    }

    foreach!(cell, (*stmt).relations, {
        let rln = lfirst(current_cell!(cell)) as *mut Node;

        if !IsA!(rln, T_RangeVar) {
            ereport!(
                ERROR,
                "only a single relation is allowed in CREATE STATISTICS"
            );
        }

        /*
         * CREATE STATISTICS will influence future execution plans but does
         * not interfere with currently executing plans.  So it should be
         * enough to take only ShareUpdateExclusiveLock on relation,
         * conflicting with ANALYZE and other DDL that sets statistical
         * information, but not with normal queries.
         */
        rel = relation_openrv(rln as *mut RangeVar, ShareUpdateExclusiveLock as c_int);

        /* Restrict to allowed relation types */
        if (*(*rel).rd_rel).relkind != RELKIND_RELATION
            && (*(*rel).rd_rel).relkind != RELKIND_MATVIEW
            && (*(*rel).rd_rel).relkind != RELKIND_FOREIGN_TABLE
            && (*(*rel).rd_rel).relkind != RELKIND_PARTITIONED_TABLE
        {
            elog!(
                ERROR,
                "cannot define statistics for relation \"{}\"",
                CStr_to_str(RelationGetRelationName(rel))
            );
        }

        /*
         * You must own the relation to create stats on it.
         *
         * NB: Concurrent changes could cause this function's lookup to find a
         * different relation than a previous lookup by the caller, so we must
         * perform this check even when check_rights == false.
         */
        if !object_ownercheck(RelationRelationId, RelationGetRelid(rel), stxowner) {
            aclcheck_error(
                ACLCHECK_NOT_OWNER,
                get_relkind_objtype((*(*rel).rd_rel).relkind),
                RelationGetRelationName(rel),
            );
        }

        /* Creating statistics on system catalogs is not allowed */
        if !allowSystemTableMods && IsSystemRelation(rel) {
            elog!(
                ERROR,
                "permission denied: \"{}\" is a system catalog",
                CStr_to_str(RelationGetRelationName(rel))
            );
        }
    });

    Assert!(!rel.is_null());
    relid = RelationGetRelid(rel);

    /*
     * If the node has a name, split it up and determine creation namespace.
     * If not, put the object in the same namespace as the relation, and cons
     * up a name for it.  (This can happen either via "CREATE STATISTICS ..."
     * or via "CREATE TABLE ... (LIKE)".)
     */
    if !(*stmt).defnames.is_null() {
        let mut namestr_out: *mut c_char = std::ptr::null_mut();
        namespaceId = QualifiedNameGetCreationNamespace((*stmt).defnames, &mut namestr_out);
        namestr = namestr_out;
    } else {
        namespaceId = RelationGetNamespace(rel);
        namestr = ChooseExtendedStatisticName(
            RelationGetRelationName(rel),
            ChooseExtendedStatisticNameAddition((*stmt).exprs),
            c"stat".as_ptr(),
            namespaceId,
        );
    }
    namestrcpy(&mut stxname, namestr);

    /*
     * Check we have creation rights in target namespace.  Skip check if
     * caller doesn't want it.
     */
    if check_rights {
        let aclresult: AclResult;

        aclresult = object_aclcheck(
            NamespaceRelationId,
            namespaceId,
            GetUserId(),
            ACL_CREATE as AclMode,
        );
        if aclresult != ACLCHECK_OK {
            aclcheck_error(
                aclresult,
                OBJECT_SCHEMA,
                get_namespace_name(namespaceId),
            );
        }
    }

    /*
     * Deal with the possibility that the statistics object already exists.
     */
    if SearchSysCacheExists2(
        STATEXTNAMENSP as c_int,
        CStringGetDatum(namestr),
        ObjectIdGetDatum(namespaceId),
    ) {
        if (*stmt).if_not_exists {
            /*
             * Since stats objects aren't members of extensions (see comments
             * below), no need for checkMembershipInCurrentExtension here.
             */
            elog!(
                NOTICE,
                "statistics object \"{}\" already exists, skipping",
                CStr_to_str(namestr)
            );
            relation_close(rel, NoLock as c_int);
            return InvalidObjectAddress;
        }

        elog!(
            ERROR,
            "statistics object \"{}\" already exists",
            CStr_to_str(namestr)
        );
    }

    /*
     * Make sure no more than STATS_MAX_DIMENSIONS columns are used. There
     * might be duplicates and so on, but we'll deal with those later.
     */
    numcols = list_length((*stmt).exprs);
    if numcols > STATS_MAX_DIMENSIONS as c_int {
        elog!(
            ERROR,
            "cannot have more than {} columns in statistics",
            STATS_MAX_DIMENSIONS
        );
    }

    /*
     * Convert the expression list to a simple array of attnums, but also keep
     * a list of more complex expressions.  While at it, enforce some
     * constraints - we don't allow extended statistics on system attributes,
     * and we require the data type to have a less-than operator.
     *
     * There are many ways to "mask" a simple attribute reference as an
     * expression, for example "(a+0)" etc. We can't possibly detect all of
     * them, but we handle at least the simple case with the attribute in
     * parens. There'll always be a way around this, if the user is determined
     * (like the "(a+0)" example), but this makes it somewhat consistent with
     * how indexes treat attributes/expressions.
     */
    foreach!(cell, (*stmt).exprs, {
        let selem = lfirst_node!(StatsElem, T_StatsElem, current_cell!(cell));

        if !(*selem).name.is_null() {
            /* column reference */
            let attname: *mut c_char;
            let atttuple: HeapTuple;
            let attForm: Form_pg_attribute;
            let r#type: *mut TypeCacheEntry;

            attname = (*selem).name;

            atttuple = SearchSysCacheAttName(relid, attname);
            if !HeapTupleIsValid(atttuple) {
                elog!(
                    ERROR,
                    "column \"{}\" does not exist",
                    CStr_to_str(attname)
                );
            }
            attForm = GETSTRUCT(atttuple) as Form_pg_attribute;

            /* Disallow use of system attributes in extended stats */
            if (*attForm).attnum <= 0 {
                ereport!(
                    ERROR,
                    "statistics creation on system columns is not supported"
                );
            }

            /* Disallow use of virtual generated columns in extended stats */
            if (*attForm).attgenerated == ATTRIBUTE_GENERATED_VIRTUAL as c_char {
                ereport!(
                    ERROR,
                    "statistics creation on virtual generated columns is not supported"
                );
            }

            /* Disallow data types without a less-than operator */
            r#type = lookup_type_cache((*attForm).atttypid, TYPECACHE_LT_OPR as c_int);
            if (*r#type).lt_opr == InvalidOid {
                elog!(
                    ERROR,
                    "column \"{}\" cannot be used in statistics because its type {} has no default btree operator class",
                    CStr_to_str(attname),
                    CStr_to_str(format_type_be((*attForm).atttypid))
                );
            }

            attnums[nattnums as usize] = (*attForm).attnum;
            nattnums += 1;
            ReleaseSysCache(atttuple);
        } else if IsA!((*selem).expr, T_Var) {
            /* column reference in parens */
            let var = (*selem).expr as *mut Var;
            let r#type: *mut TypeCacheEntry;

            /* Disallow use of system attributes in extended stats */
            if (*var).varattno <= 0 {
                ereport!(
                    ERROR,
                    "statistics creation on system columns is not supported"
                );
            }

            /* Disallow use of virtual generated columns in extended stats */
            if get_attgenerated(relid, (*var).varattno) == ATTRIBUTE_GENERATED_VIRTUAL as c_char {
                ereport!(
                    ERROR,
                    "statistics creation on virtual generated columns is not supported"
                );
            }

            /* Disallow data types without a less-than operator */
            r#type = lookup_type_cache((*var).vartype, TYPECACHE_LT_OPR as c_int);
            if (*r#type).lt_opr == InvalidOid {
                elog!(
                    ERROR,
                    "column \"{}\" cannot be used in statistics because its type {} has no default btree operator class",
                    CStr_to_str(get_attname(relid, (*var).varattno, false)),
                    CStr_to_str(format_type_be((*var).vartype))
                );
            }

            attnums[nattnums as usize] = (*var).varattno;
            nattnums += 1;
        } else {
            /* expression */
            let expr = (*selem).expr;
            let atttype: Oid;
            let r#type: *mut TypeCacheEntry;
            let mut attnums_bms: *mut Bitmapset = std::ptr::null_mut();
            let mut k: c_int;

            Assert!(!expr.is_null());

            pull_varattnos(expr, 1, &mut attnums_bms);

            k = -1;
            loop {
                k = bms_next_member(attnums_bms, k);
                if k < 0 {
                    break;
                }
                let attnum: AttrNumber = (k + FirstLowInvalidHeapAttributeNumber as i32) as AttrNumber;

                /* Disallow expressions referencing system attributes. */
                if attnum <= 0 {
                    ereport!(
                        ERROR,
                        "statistics creation on system columns is not supported"
                    );
                }

                /* Disallow use of virtual generated columns in extended stats */
                if get_attgenerated(relid, attnum) == ATTRIBUTE_GENERATED_VIRTUAL as c_char {
                    ereport!(
                        ERROR,
                        "statistics creation on virtual generated columns is not supported"
                    );
                }
            }

            /*
             * Disallow data types without a less-than operator.
             *
             * We ignore this for statistics on a single expression, in which
             * case we'll build the regular statistics only (and that code can
             * deal with such data types).
             */
            if list_length((*stmt).exprs) > 1 {
                atttype = exprType(expr);
                r#type = lookup_type_cache(atttype, TYPECACHE_LT_OPR as c_int);
                if (*r#type).lt_opr == InvalidOid {
                    elog!(
                        ERROR,
                        "expression cannot be used in multivariate statistics because its type {} has no default btree operator class",
                        CStr_to_str(format_type_be(atttype))
                    );
                }
            }

            stxexprs = lappend(stxexprs, expr as *mut c_void);
        }
    });

    /*
     * Parse the statistics kinds.
     *
     * First check that if this is the case with a single expression, there
     * are no statistics kinds specified (we don't allow that for the simple
     * CREATE STATISTICS form).
     */
    if (list_length((*stmt).exprs) == 1) && (list_length(stxexprs) == 1) {
        /* statistics kinds not specified */
        if (*stmt).stat_types != NIL as *mut List {
            ereport!(
                ERROR,
                "when building statistics on a single expression, statistics kinds may not be specified"
            );
        }
    }

    /* OK, let's check that we recognize the statistics kinds. */
    build_ndistinct = false;
    build_dependencies = false;
    build_mcv = false;
    foreach!(cell, (*stmt).stat_types, {
        let r#type = strVal(lfirst(current_cell!(cell)));

        if strcmp(r#type, c"ndistinct".as_ptr()) == 0 {
            build_ndistinct = true;
            requested_type = true;
        } else if strcmp(r#type, c"dependencies".as_ptr()) == 0 {
            build_dependencies = true;
            requested_type = true;
        } else if strcmp(r#type, c"mcv".as_ptr()) == 0 {
            build_mcv = true;
            requested_type = true;
        } else {
            elog!(
                ERROR,
                "unrecognized statistics kind \"{}\"",
                CStr_to_str(r#type)
            );
        }
    });

    /*
     * If no statistic type was specified, build them all (but only when the
     * statistics is defined on more than one column/expression).
     */
    if (!requested_type) && (numcols >= 2) {
        build_ndistinct = true;
        build_dependencies = true;
        build_mcv = true;
    }

    /*
     * When there are non-trivial expressions, build the expression stats
     * automatically. This allows calculating good estimates for stats that
     * consider per-clause estimates (e.g. functional dependencies).
     */
    build_expressions = stxexprs != NIL as *mut List;

    /*
     * Check that at least two columns were specified in the statement, or
     * that we're building statistics on a single expression.
     */
    if (numcols < 2) && (list_length(stxexprs) != 1) {
        ereport!(
            ERROR,
            "extended statistics require at least 2 columns"
        );
    }

    /*
     * Sort the attnums, which makes detecting duplicates somewhat easier, and
     * it does not hurt (it does not matter for the contents, unlike for
     * indexes, for example).
     */
    qsort(
        attnums.as_mut_ptr() as *mut c_void,
        nattnums as usize,
        std::mem::size_of::<int16>(),
        Some(compare_int16),
    );

    /*
     * Check for duplicates in the list of columns. The attnums are sorted so
     * just check consecutive elements.
     */
    i = 1;
    while i < nattnums {
        if attnums[i as usize] == attnums[(i - 1) as usize] {
            ereport!(
                ERROR,
                "duplicate column name in statistics definition"
            );
        }
        i += 1;
    }

    /*
     * Check for duplicate expressions. We do two loops, counting the
     * occurrences of each expression. This is O(N^2) but we only allow small
     * number of expressions and it's not executed often.
     *
     * XXX We don't cross-check attributes and expressions, because it does
     * not seem worth it. In principle we could check that expressions don't
     * contain trivial attribute references like "(a)", but the reasoning is
     * similar to why we don't bother with extracting columns from
     * expressions. It's either expensive or very easy to defeat for
     * determined user, and there's no risk if we allow such statistics (the
     * statistics is useless, but harmless).
     */
    foreach!(cell, stxexprs, {
        let expr1 = lfirst(current_cell!(cell)) as *mut Node;
        let mut cnt: c_int = 0;

        foreach!(cell2, stxexprs, {
            let expr2 = lfirst(current_cell!(cell2)) as *mut Node;

            if equal(expr1 as *const c_void, expr2 as *const c_void) {
                cnt += 1;
            }
        });

        /* every expression should find at least itself */
        Assert!(cnt >= 1);

        if cnt > 1 {
            ereport!(
                ERROR,
                "duplicate expression in statistics definition"
            );
        }
    });

    /* Form an int2vector representation of the sorted column list */
    stxkeys = buildint2vector(attnums.as_ptr(), nattnums);

    /* construct the char array of enabled statistic types */
    ntypes = 0;
    if build_ndistinct {
        types[ntypes as usize] = CharGetDatum(STATS_EXT_NDISTINCT as c_char);
        ntypes += 1;
    }
    if build_dependencies {
        types[ntypes as usize] = CharGetDatum(STATS_EXT_DEPENDENCIES as c_char);
        ntypes += 1;
    }
    if build_mcv {
        types[ntypes as usize] = CharGetDatum(STATS_EXT_MCV as c_char);
        ntypes += 1;
    }
    if build_expressions {
        types[ntypes as usize] = CharGetDatum(STATS_EXT_EXPRESSIONS as c_char);
        ntypes += 1;
    }
    Assert!(ntypes > 0 && ntypes <= types.len() as c_int);
    stxkind = construct_array_builtin(types.as_ptr(), ntypes, CHAROID);

    /* convert the expressions (if any) to a text datum */
    if stxexprs != NIL as *mut List {
        let exprsString: *mut c_char;

        exprsString = nodeToString(stxexprs as *const c_void);
        exprsDatum = CStringGetTextDatum(exprsString);
        pfree(exprsString as *mut c_void);
    } else {
        exprsDatum = 0 as Datum;
    }

    statrel = table_open(StatisticExtRelationId, RowExclusiveLock as c_int);

    /*
     * Everything seems fine, so let's build the pg_statistic_ext tuple.
     */
    memset(
        values.as_mut_ptr() as *mut c_void,
        0,
        std::mem::size_of_val(&values),
    );
    memset(
        nulls.as_mut_ptr() as *mut c_void,
        0,
        std::mem::size_of_val(&nulls),
    );

    statoid = GetNewOidWithIndex(
        statrel,
        StatisticExtOidIndexId,
        Anum_pg_statistic_ext_oid as AttrNumber,
    );
    values[(Anum_pg_statistic_ext_oid - 1) as usize] = ObjectIdGetDatum(statoid);
    values[(Anum_pg_statistic_ext_stxrelid - 1) as usize] = ObjectIdGetDatum(relid);
    values[(Anum_pg_statistic_ext_stxname - 1) as usize] = NameGetDatum(&stxname);
    values[(Anum_pg_statistic_ext_stxnamespace - 1) as usize] = ObjectIdGetDatum(namespaceId);
    values[(Anum_pg_statistic_ext_stxowner - 1) as usize] = ObjectIdGetDatum(stxowner);
    values[(Anum_pg_statistic_ext_stxkeys - 1) as usize] = PointerGetDatum(stxkeys as *mut c_void);
    nulls[(Anum_pg_statistic_ext_stxstattarget - 1) as usize] = true;
    values[(Anum_pg_statistic_ext_stxkind - 1) as usize] = PointerGetDatum(stxkind as *mut c_void);

    values[(Anum_pg_statistic_ext_stxexprs - 1) as usize] = exprsDatum;
    if exprsDatum == 0 as Datum {
        nulls[(Anum_pg_statistic_ext_stxexprs - 1) as usize] = true;
    }

    /* insert it into pg_statistic_ext */
    htup = heap_form_tuple((*statrel).rd_att, values.as_ptr(), nulls.as_ptr());
    CatalogTupleInsert(statrel, htup);
    heap_freetuple(htup);

    relation_close(statrel, RowExclusiveLock as c_int);

    /*
     * We used to create the pg_statistic_ext_data tuple too, but it's not
     * clear what value should the stxdinherit flag have (it depends on
     * whether the rel is partitioned, contains data, etc.)
     */

    InvokeObjectPostCreateHook(StatisticExtRelationId, statoid, 0);

    /*
     * Invalidate relcache so that others see the new statistics object.
     */
    CacheInvalidateRelcache(rel);

    relation_close(rel, NoLock as c_int);

    /*
     * Add an AUTO dependency on each column used in the stats, so that the
     * stats object goes away if any or all of them get dropped.
     */
    ObjectAddressSet(&mut myself, StatisticExtRelationId, statoid);

    /* add dependencies for plain column references */
    i = 0;
    while i < nattnums {
        ObjectAddressSubSet(
            &mut parentobject,
            RelationRelationId,
            relid,
            attnums[i as usize] as int32,
        );
        recordDependencyOn(&myself, &parentobject, DEPENDENCY_AUTO);
        i += 1;
    }

    /*
     * If there are no dependencies on a column, give the statistics object an
     * auto dependency on the whole table.  In most cases, this will be
     * redundant, but it might not be if the statistics expressions contain no
     * Vars (which might seem strange but possible). This is consistent with
     * what we do for indexes in index_create.
     *
     * XXX We intentionally don't consider the expressions before adding this
     * dependency, because recordDependencyOnSingleRelExpr may not create any
     * dependencies for whole-row Vars.
     */
    if nattnums == 0 {
        ObjectAddressSet(&mut parentobject, RelationRelationId, relid);
        recordDependencyOn(&myself, &parentobject, DEPENDENCY_AUTO);
    }

    /*
     * Store dependencies on anything mentioned in statistics expressions,
     * just like we do for index expressions.
     */
    if !stxexprs.is_null() {
        recordDependencyOnSingleRelExpr(
            &myself,
            stxexprs as *mut Node,
            relid,
            DEPENDENCY_NORMAL,
            DEPENDENCY_AUTO,
            false,
        );
    }

    /*
     * Also add dependencies on namespace and owner.  These are required
     * because the stats object might have a different namespace and/or owner
     * than the underlying table(s).
     */
    ObjectAddressSet(&mut parentobject, NamespaceRelationId, namespaceId);
    recordDependencyOn(&myself, &parentobject, DEPENDENCY_NORMAL);

    recordDependencyOnOwner(StatisticExtRelationId, statoid, stxowner);

    /*
     * XXX probably there should be a recordDependencyOnCurrentExtension call
     * here too, but we'd have to add support for ALTER EXTENSION ADD/DROP
     * STATISTICS, which is more work than it seems worth.
     */

    /* Add any requested comment */
    if !(*stmt).stxcomment.is_null() {
        CreateComments(statoid, StatisticExtRelationId, 0, (*stmt).stxcomment);
    }

    /* Return stats object's address */
    myself
}

/*
 *		ALTER STATISTICS
 */
pub unsafe fn AlterStatistics(stmt: *mut AlterStatsStmt) -> ObjectAddress {
    let rel: Relation;
    let stxoid: Oid;
    let oldtup: HeapTuple;
    let newtup: HeapTuple;
    let mut repl_val: [Datum; Natts_pg_statistic_ext as usize] =
        [0; Natts_pg_statistic_ext as usize];
    let mut repl_null: [bool; Natts_pg_statistic_ext as usize] =
        [false; Natts_pg_statistic_ext as usize];
    let mut repl_repl: [bool; Natts_pg_statistic_ext as usize] =
        [false; Natts_pg_statistic_ext as usize];
    let mut address: ObjectAddress = std::mem::zeroed();
    let mut newtarget: c_int = 0;
    let newtarget_default: bool;

    /* -1 was used in previous versions for the default setting */
    if !(*stmt).stxstattarget.is_null() && intVal((*stmt).stxstattarget as *mut c_void) != -1 {
        newtarget = intVal((*stmt).stxstattarget as *mut c_void);
        newtarget_default = false;
    } else {
        newtarget_default = true;
    }

    if !newtarget_default {
        /* Limit statistics target to a sane range */
        if newtarget < 0 {
            elog!(ERROR, "statistics target {} is too low", newtarget);
        } else if newtarget > MAX_STATISTICS_TARGET as c_int {
            newtarget = MAX_STATISTICS_TARGET as c_int;
            elog!(WARNING, "lowering statistics target to {}", newtarget);
        }
    }

    /* lookup OID of the statistics object */
    stxoid = get_statistics_object_oid((*stmt).defnames, (*stmt).missing_ok);

    /*
     * If we got here and the OID is not valid, it means the statistics object
     * does not exist, but the command specified IF EXISTS. So report this as
     * a simple NOTICE and we're done.
     */
    if !OidIsValid(stxoid) {
        let mut schemaname: *mut c_char = std::ptr::null_mut();
        let mut statname: *mut c_char = std::ptr::null_mut();

        Assert!((*stmt).missing_ok);

        DeconstructQualifiedName((*stmt).defnames, &mut schemaname, &mut statname);

        if !schemaname.is_null() {
            elog!(
                NOTICE,
                "statistics object \"{}.{}\" does not exist, skipping",
                CStr_to_str(schemaname),
                CStr_to_str(statname)
            );
        } else {
            elog!(
                NOTICE,
                "statistics object \"{}\" does not exist, skipping",
                CStr_to_str(statname)
            );
        }

        return InvalidObjectAddress;
    }

    /* Search pg_statistic_ext */
    rel = table_open(StatisticExtRelationId, RowExclusiveLock as c_int);

    oldtup = SearchSysCache1(STATEXTOID as c_int, ObjectIdGetDatum(stxoid));
    if !HeapTupleIsValid(oldtup) {
        elog!(
            ERROR,
            "cache lookup failed for extended statistics object {}",
            stxoid
        );
    }

    /* Must be owner of the existing statistics object */
    if !object_ownercheck(StatisticExtRelationId, stxoid, GetUserId()) {
        aclcheck_error(
            ACLCHECK_NOT_OWNER,
            OBJECT_STATISTIC_EXT,
            NameListToString((*stmt).defnames),
        );
    }

    /* Build new tuple. */
    memset(
        repl_val.as_mut_ptr() as *mut c_void,
        0,
        std::mem::size_of_val(&repl_val),
    );
    memset(
        repl_null.as_mut_ptr() as *mut c_void,
        0,
        std::mem::size_of_val(&repl_null),
    );
    memset(
        repl_repl.as_mut_ptr() as *mut c_void,
        0,
        std::mem::size_of_val(&repl_repl),
    );

    /* replace the stxstattarget column */
    repl_repl[(Anum_pg_statistic_ext_stxstattarget - 1) as usize] = true;
    if !newtarget_default {
        repl_val[(Anum_pg_statistic_ext_stxstattarget - 1) as usize] =
            Int16GetDatum(newtarget as int16);
    } else {
        repl_null[(Anum_pg_statistic_ext_stxstattarget - 1) as usize] = true;
    }

    newtup = heap_modify_tuple(
        oldtup,
        RelationGetDescr(rel),
        repl_val.as_ptr(),
        repl_null.as_ptr(),
        repl_repl.as_ptr(),
    );

    /* Update system catalog. */
    CatalogTupleUpdate(rel, &mut (*newtup).t_self, newtup);

    InvokeObjectPostAlterHook(StatisticExtRelationId, stxoid, 0);

    ObjectAddressSet(&mut address, StatisticExtRelationId, stxoid);

    /*
     * NOTE: because we only support altering the statistics target, not the
     * other fields, there is no need to update dependencies.
     */

    heap_freetuple(newtup);
    ReleaseSysCache(oldtup);

    table_close(rel, RowExclusiveLock as c_int);

    address
}

/*
 * Delete entry in pg_statistic_ext_data catalog. We don't know if the row
 * exists, so don't error out.
 */
pub unsafe fn RemoveStatisticsDataById(statsOid: Oid, inh: bool) {
    let relation: Relation;
    let tup: HeapTuple;

    relation = table_open(StatisticExtDataRelationId, RowExclusiveLock as c_int);

    tup = SearchSysCache2(
        STATEXTDATASTXOID as c_int,
        ObjectIdGetDatum(statsOid),
        BoolGetDatum(inh),
    );

    /* We don't know if the data row for inh value exists. */
    if HeapTupleIsValid(tup) {
        CatalogTupleDelete(relation, &mut (*tup).t_self);

        ReleaseSysCache(tup);
    }

    table_close(relation, RowExclusiveLock as c_int);
}

/*
 * Guts of statistics object deletion.
 */
pub unsafe fn RemoveStatisticsById(statsOid: Oid) {
    let relation: Relation;
    let rel: Relation;
    let tup: HeapTuple;
    let statext: Form_pg_statistic_ext;
    let relid: Oid;

    /*
     * Delete the pg_statistic_ext tuple.  Also send out a cache inval on the
     * associated table, so that dependent plans will be rebuilt.
     */
    relation = table_open(StatisticExtRelationId, RowExclusiveLock as c_int);

    tup = SearchSysCache1(STATEXTOID as c_int, ObjectIdGetDatum(statsOid));

    if !HeapTupleIsValid(tup) {
        /* should not happen */
        elog!(
            ERROR,
            "cache lookup failed for statistics object {}",
            statsOid
        );
    }

    statext = GETSTRUCT(tup) as Form_pg_statistic_ext;
    relid = (*statext).stxrelid;

    /*
     * Delete the pg_statistic_ext_data tuples holding the actual statistical
     * data. There might be data with/without inheritance, so attempt deleting
     * both. We lock the user table first, to prevent other processes (e.g.
     * DROP STATISTICS) from removing the row concurrently.
     */
    rel = table_open(relid, ShareUpdateExclusiveLock as c_int);

    RemoveStatisticsDataById(statsOid, true);
    RemoveStatisticsDataById(statsOid, false);

    CacheInvalidateRelcacheByRelid(relid);

    CatalogTupleDelete(relation, &mut (*tup).t_self);

    ReleaseSysCache(tup);

    /* Keep lock until the end of the transaction. */
    table_close(rel, NoLock as c_int);

    table_close(relation, RowExclusiveLock as c_int);
}

/*
 * Select a nonconflicting name for a new statistics object.
 *
 * name1, name2, and label are used the same way as for makeObjectName(),
 * except that the label can't be NULL; digits will be appended to the label
 * if needed to create a name that is unique within the specified namespace.
 *
 * Returns a palloc'd string.
 *
 * Note: it is theoretically possible to get a collision anyway, if someone
 * else chooses the same name concurrently.  This is fairly unlikely to be
 * a problem in practice, especially if one is holding a share update
 * exclusive lock on the relation identified by name1.  However, if choosing
 * multiple names within a single command, you'd better create the new object
 * and do CommandCounterIncrement before choosing the next one!
 */
unsafe fn ChooseExtendedStatisticName(
    name1: *const c_char,
    name2: *const c_char,
    label: *const c_char,
    namespaceid: Oid,
) -> *mut c_char {
    let mut pass: c_int = 0;
    let mut stxname: *mut c_char;
    let mut modlabel: [c_char; NAMEDATALEN as usize] = [0; NAMEDATALEN as usize];

    /* try the unmodified label first */
    strlcpy(modlabel.as_mut_ptr(), label, std::mem::size_of_val(&modlabel) as Size);

    loop {
        let existingstats: Oid;

        stxname = makeObjectName(name1, name2, modlabel.as_ptr());

        existingstats = GetSysCacheOid2(
            STATEXTNAMENSP as c_int,
            Anum_pg_statistic_ext_oid as AttrNumber,
            PointerGetDatum(stxname as *mut c_void),
            ObjectIdGetDatum(namespaceid),
        );
        if !OidIsValid(existingstats) {
            break;
        }

        /* found a conflict, so try a new name component */
        pfree(stxname as *mut c_void);
        pass += 1;
        snprintf(
            modlabel.as_mut_ptr(),
            std::mem::size_of_val(&modlabel),
            c"%s%d".as_ptr(),
            label,
            pass,
        );
    }

    stxname
}

/*
 * Generate "name2" for a new statistics object given the list of column
 * names for it.  This will be passed to ChooseExtendedStatisticName along
 * with the parent table name and a suitable label.
 *
 * We know that less than NAMEDATALEN characters will actually be used,
 * so we can truncate the result once we've generated that many.
 *
 * XXX see also ChooseForeignKeyConstraintNameAddition and
 * ChooseIndexNameAddition.
 */
unsafe fn ChooseExtendedStatisticNameAddition(exprs: *mut List) -> *mut c_char {
    let mut buf: [c_char; (NAMEDATALEN * 2) as usize] = [0; (NAMEDATALEN * 2) as usize];
    let mut buflen: c_int = 0;

    buf[0] = b'\0' as c_char;
    foreach!(lc, exprs, {
        let selem = lfirst(current_cell!(lc)) as *mut StatsElem;
        let mut name: *const c_char;

        /* It should be one of these, but just skip if it happens not to be */
        if !IsA!(selem, T_StatsElem) {
            continue;
        }

        name = (*selem).name;

        if buflen > 0 {
            buf[buflen as usize] = b'_' as c_char; /* insert _ between names */
            buflen += 1;
        }

        /*
         * We use fixed 'expr' for expressions, which have empty column names.
         * For indexes this is handled in ChooseIndexColumnNames, but we have
         * no such function for stats and it does not seem worth adding. If a
         * better name is needed, the user can specify it explicitly.
         */
        if name.is_null() {
            name = c"expr".as_ptr();
        }

        /*
         * At this point we have buflen <= NAMEDATALEN.  name should be less
         * than NAMEDATALEN already, but use strlcpy for paranoia.
         */
        strlcpy(
            buf.as_mut_ptr().add(buflen as usize),
            name,
            NAMEDATALEN as Size,
        );
        buflen += strlen(buf.as_ptr().add(buflen as usize)) as c_int;
        if buflen >= NAMEDATALEN as c_int {
            break;
        }
    });
    pstrdup(buf.as_ptr())
}

/*
 * StatisticsGetRelation: given a statistics object's OID, get the OID of
 * the relation it is defined on.  Uses the system cache.
 */
pub unsafe fn StatisticsGetRelation(statId: Oid, missing_ok: bool) -> Oid {
    let tuple: HeapTuple;
    let stx: Form_pg_statistic_ext;
    let result: Oid;

    tuple = SearchSysCache1(STATEXTOID as c_int, ObjectIdGetDatum(statId));
    if !HeapTupleIsValid(tuple) {
        if missing_ok {
            return InvalidOid;
        }
        elog!(
            ERROR,
            "cache lookup failed for statistics object {}",
            statId
        );
    }
    stx = GETSTRUCT(tuple) as Form_pg_statistic_ext;
    Assert!((*stx).oid == statId);

    result = (*stx).stxrelid;
    ReleaseSysCache(tuple);
    result
}

// ---------------------------------------------------------------------------
// Local stubs / constants for unported headers
// ---------------------------------------------------------------------------

// catalog/objectaddress.h is not ported yet; reuse the ObjectAddress struct from
// objectaccess and provide the helpers/sentinels this unit needs.
use crate::catalog::objectaccess::ObjectAddress;

const InvalidObjectAddress: ObjectAddress = ObjectAddress {
    classId: InvalidOid,
    objectId: InvalidOid,
    objectSubId: 0,
};

#[allow(non_snake_case)]
unsafe fn ObjectAddressSet(addr: &mut ObjectAddress, class_id: Oid, object_id: Oid) {
    addr.classId = class_id;
    addr.objectId = object_id;
    addr.objectSubId = 0;
}

#[allow(non_snake_case)]
unsafe fn ObjectAddressSubSet(
    addr: &mut ObjectAddress,
    class_id: Oid,
    object_id: Oid,
    object_sub_id: int32,
) {
    addr.classId = class_id;
    addr.objectId = object_id;
    addr.objectSubId = object_sub_id;
}

// catalog/dependency.h (DependencyType) is not ported yet.
pub type DependencyType = c_int;
const DEPENDENCY_NORMAL: DependencyType = b'n' as c_int;
const DEPENDENCY_AUTO: DependencyType = b'a' as c_int;

// utils/acl.h (AclResult) is not ported yet.
pub type AclResult = c_int;
const ACLCHECK_OK: AclResult = 0;
const ACLCHECK_NOT_OWNER: AclResult = 1;

// utils/syscache.h cache identifiers are not ported yet.
const STATEXTNAMENSP: c_int = 63;
const STATEXTOID: c_int = 64;
const STATEXTDATASTXOID: c_int = 62;

// utils/typcache.h flag (lookup_type_cache).
const TYPECACHE_LT_OPR: c_int = 0x0001;

// utils/typcache.h (TypeCacheEntry) is not ported yet; only the lt_opr field is
// referenced by this unit.
#[repr(C)]
pub struct TypeCacheEntry {
    pub lt_opr: Oid,
}

// catalog/pg_statistic_ext.h column/relation metadata not yet generated.
const Natts_pg_statistic_ext: usize = 9;
const Anum_pg_statistic_ext_oid: c_int = 1;
const Anum_pg_statistic_ext_stxrelid: c_int = 2;
const Anum_pg_statistic_ext_stxname: c_int = 3;
const Anum_pg_statistic_ext_stxnamespace: c_int = 4;
const Anum_pg_statistic_ext_stxowner: c_int = 5;
const Anum_pg_statistic_ext_stxstattarget: c_int = 6;
const Anum_pg_statistic_ext_stxkeys: c_int = 7;
const Anum_pg_statistic_ext_stxkind: c_int = 8;
const Anum_pg_statistic_ext_stxexprs: c_int = 9;
const StatisticExtOidIndexId: Oid = 3380;

// pg_statistic_ext.h statistics-kind codes (as c_char, see CharGetDatum below).
const STATS_EXT_NDISTINCT: c_char = b'd' as c_char;
const STATS_EXT_DEPENDENCIES: c_char = b'f' as c_char;
const STATS_EXT_MCV: c_char = b'm' as c_char;
const STATS_EXT_EXPRESSIONS: c_char = b'e' as c_char;

unsafe fn NameGetDatum(name: *const NameData) -> Datum {
    PointerGetDatum(name as *const c_void)
}

unsafe fn CStr_to_str(s: *const c_char) -> &'static str {
    if s.is_null() {
        return "";
    }
    std::ffi::CStr::from_ptr(s).to_str().unwrap_or("")
}

unsafe fn relation_openrv(_relation: *mut RangeVar, _lockmode: c_int) -> Relation {
    unimplemented!() // TODO: access/relation.c
}
unsafe fn relation_close(_relation: Relation, _lockmode: c_int) {
    unimplemented!() // TODO: access/relation.c
}
unsafe fn table_open(_relationId: Oid, _lockmode: c_int) -> Relation {
    unimplemented!() // TODO: access/table/table.c
}
unsafe fn table_close(_relation: Relation, _lockmode: c_int) {
    unimplemented!() // TODO: access/table/table.c
}
unsafe fn object_ownercheck(_classid: Oid, _objectid: Oid, _roleid: Oid) -> bool {
    unimplemented!() // TODO: catalog/aclchk.c
}
unsafe fn aclcheck_error(_aclerr: AclResult, _objtype: ObjectType, _objectname: *const c_char) {
    unimplemented!() // TODO: catalog/aclchk.c
}
unsafe fn object_aclcheck(_classid: Oid, _objectid: Oid, _roleid: Oid, _mode: AclMode) -> AclResult {
    unimplemented!() // TODO: catalog/aclchk.c
}
unsafe fn get_relkind_objtype(_relkind: c_char) -> ObjectType {
    unimplemented!() // TODO: catalog/objectaddress.c
}
unsafe fn IsSystemRelation(_relation: Relation) -> bool {
    unimplemented!() // TODO: catalog/catalog.c
}
unsafe fn QualifiedNameGetCreationNamespace(
    _names: *mut List,
    _objname_p: *mut *mut c_char,
) -> Oid {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn get_namespace_name(_nspid: Oid) -> *mut c_char {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn namestrcpy(_name: *mut NameData, _str: *const c_char) -> c_int {
    unimplemented!() // TODO: backend/utils/adt/name.c
}
unsafe fn SearchSysCacheExists2(_cacheId: c_int, _key1: Datum, _key2: Datum) -> bool {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn SearchSysCacheAttName(_relid: Oid, _attname: *const c_char) -> HeapTuple {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!() // TODO: utils/cache/typcache.c
}
unsafe fn format_type_be(_type_oid: Oid) -> *mut c_char {
    unimplemented!() // TODO: utils/adt/format_type.c
}
unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn get_attgenerated(_relid: Oid, _attnum: AttrNumber) -> c_char {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn get_attname(_relid: Oid, _attnum: AttrNumber, _missing_ok: bool) -> *mut c_char {
    unimplemented!() // TODO: utils/cache/lsyscache.c
}
unsafe fn pull_varattnos(_node: *mut Node, _varno: c_int, _varattnos: *mut *mut Bitmapset) {
    unimplemented!() // TODO: optimizer/util/var.c
}
unsafe fn bms_next_member(_a: *const Bitmapset, _prevbit: c_int) -> c_int {
    unimplemented!() // TODO: nodes/bitmapset.c
}
unsafe fn exprType(_expr: *const Node) -> Oid {
    unimplemented!() // TODO: nodes/nodeFuncs.c
}
unsafe fn lappend(_list: *mut List, _datum: *mut c_void) -> *mut List {
    unimplemented!() // TODO: nodes/list.c
}
unsafe fn strVal(_v: *mut c_void) -> *mut c_char {
    unimplemented!() // TODO: nodes/value.h
}
unsafe fn equal(_a: *const c_void, _b: *const c_void) -> bool {
    unimplemented!() // TODO: nodes/equalfuncs.c
}
unsafe fn buildint2vector(_int2s: *const int16, _n: c_int) -> *mut int2vector {
    unimplemented!() // TODO: utils/adt/int.c
}
unsafe fn construct_array_builtin(_elems: *const Datum, _nelems: c_int, _elmtype: Oid) -> *mut ArrayType {
    unimplemented!() // TODO: utils/adt/arrayfuncs.c
}
unsafe fn nodeToString(_obj: *const c_void) -> *mut c_char {
    unimplemented!() // TODO: nodes/outfuncs.c
}
unsafe fn CStringGetTextDatum(_s: *const c_char) -> Datum {
    unimplemented!() // TODO: utils/fmgr.h
}
unsafe fn GetNewOidWithIndex(_relation: Relation, _indexId: Oid, _oidcolumn: AttrNumber) -> Oid {
    unimplemented!() // TODO: catalog/catalog.c
}
unsafe fn heap_form_tuple(
    _tupleDescriptor: TupleDesc,
    _values: *const Datum,
    _isnull: *const bool,
) -> HeapTuple {
    unimplemented!() // TODO: access/common/heaptuple.c
}
unsafe fn CatalogTupleInsert(_heapRel: Relation, _tup: HeapTuple) -> Oid {
    unimplemented!() // TODO: catalog/indexing.c
}
unsafe fn heap_freetuple(_htup: HeapTuple) {
    unimplemented!() // TODO: access/common/heaptuple.c
}
unsafe fn InvokeObjectPostCreateHook(_classId: Oid, _objectId: Oid, _subId: c_int) {
    unimplemented!() // TODO: catalog/objectaccess.h
}
unsafe fn CacheInvalidateRelcache(_relation: Relation) {
    unimplemented!() // TODO: utils/cache/inval.c
}
unsafe fn recordDependencyOn(
    _depender: *const ObjectAddress,
    _referenced: *const ObjectAddress,
    _behavior: DependencyType,
) {
    unimplemented!() // TODO: catalog/pg_depend.c
}
unsafe fn recordDependencyOnSingleRelExpr(
    _depender: *const ObjectAddress,
    _expr: *mut Node,
    _relId: Oid,
    _behavior: DependencyType,
    _self_behavior: DependencyType,
    _reverse_self: bool,
) {
    unimplemented!() // TODO: catalog/dependency.c
}
unsafe fn recordDependencyOnOwner(_classId: Oid, _objectId: Oid, _owner: Oid) {
    unimplemented!() // TODO: catalog/pg_shdepend.c
}
unsafe fn CreateComments(_oid: Oid, _classoid: Oid, _subid: int32, _comment: *const c_char) {
    unimplemented!() // TODO: commands/comment.c
}
unsafe fn intVal(_v: *mut c_void) -> c_int {
    unimplemented!() // TODO: nodes/value.h
}
unsafe fn get_statistics_object_oid(_names: *mut List, _missing_ok: bool) -> Oid {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn DeconstructQualifiedName(
    _names: *mut List,
    _nspname_p: *mut *mut c_char,
    _objname_p: *mut *mut c_char,
) {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn SearchSysCache1(_cacheId: c_int, _key1: Datum) -> HeapTuple {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn SearchSysCache2(_cacheId: c_int, _key1: Datum, _key2: Datum) -> HeapTuple {
    unimplemented!() // TODO: utils/cache/syscache.c
}
unsafe fn NameListToString(_names: *mut List) -> *mut c_char {
    unimplemented!() // TODO: catalog/namespace.c
}
unsafe fn heap_modify_tuple(
    _tuple: HeapTuple,
    _tupleDesc: TupleDesc,
    _replValues: *const Datum,
    _replIsnull: *const bool,
    _doReplace: *const bool,
) -> HeapTuple {
    unimplemented!() // TODO: access/common/heaptuple.c
}
unsafe fn CatalogTupleUpdate(_heapRel: Relation, _otid: *mut ItemPointerData, _tup: HeapTuple) {
    unimplemented!() // TODO: catalog/indexing.c
}
unsafe fn InvokeObjectPostAlterHook(_classId: Oid, _objectId: Oid, _subId: c_int) {
    unimplemented!() // TODO: catalog/objectaccess.h
}
unsafe fn CatalogTupleDelete(_heapRel: Relation, _tid: *mut ItemPointerData) {
    unimplemented!() // TODO: catalog/indexing.c
}
unsafe fn CacheInvalidateRelcacheByRelid(_relid: Oid) {
    unimplemented!() // TODO: utils/cache/inval.c
}
unsafe fn strlcpy(_dst: *mut c_char, _src: *const c_char, _siz: Size) -> Size {
    unimplemented!() // TODO: port/strlcpy.c
}
unsafe fn makeObjectName(
    _name1: *const c_char,
    _name2: *const c_char,
    _label: *const c_char,
) -> *mut c_char {
    unimplemented!() // TODO: commands/indexcmds.c
}
unsafe fn GetSysCacheOid2(
    _cacheId: c_int,
    _oidcol: AttrNumber,
    _key1: Datum,
    _key2: Datum,
) -> Oid {
    unimplemented!() // TODO: utils/cache/syscache.c
}
// NOTE: pstrdup is provided by the prelude (crate::utils::palloc); no local stub.
