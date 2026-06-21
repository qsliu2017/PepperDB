//! src/backend/statistics/dependencies.c
//!
//! POSTGRES functional dependencies
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/statistics/dependencies.c

use crate::prelude::*;

use core::ffi::{c_char, c_int, c_void};

use crate::access::attnum::{
    AttrNumber, AttrNumberIsForUserDefinedAttr, AttributeNumberIsValid, InvalidAttrNumber,
};
use crate::access::htup_details::{HeapTuple, MaxHeapAttributeNumber};
use crate::nodes::bitmapset::{
    bms_add_member, bms_del_member, bms_free, bms_is_member, bms_member_index, bms_membership,
    bms_next_member, bms_num_members, Bitmapset, BMS_Membership,
};
use crate::nodes::nodes::{JoinType, Node, Selectivity};
use crate::nodes::pathnodes::{
    PlannerInfo, RelOptInfo, RestrictInfo, SpecialJoinInfo, StatisticExtInfo,
};
use crate::nodes::pg_list::{lappend, lfirst, linitial, list_length, list_nth, lsecond, List};
use crate::nodes::primnodes::{
    BoolExpr, OpExpr, RelabelType, ScalarArrayOpExpr, Var,
};
use crate::nodes::parsenodes::RangeTblEntry;
use crate::postgres::Datum;
use crate::postgres_ext::Oid;

use crate::statistics::extended_stats_internal::{
    build_sorted_items, multi_sort_add_dimension, multi_sort_compare_dim, multi_sort_compare_dims,
    multi_sort_init, MultiSortSupport, SortItem, StatsBuildData,
};
use crate::statistics::statistics::{
    MVDependencies, MVDependency, STATS_DEPS_MAGIC, STATS_DEPS_TYPE_BASIC, STATS_MAX_DIMENSIONS,
};

use crate::{current_cell, foreach, IsA};

/* size of the struct header fields (magic, type, ndeps) */
const SizeOfHeader: Size = 3 * core::mem::size_of::<uint32>();

/* size of a serialized dependency (degree, natts, atts) */
#[inline]
const fn SizeOfItem(natts: usize) -> Size {
    core::mem::size_of::<f64>() + core::mem::size_of::<AttrNumber>() * (1 + natts)
}

/* minimal size of a dependency (with two attributes) */
const MinSizeOfItem: Size = SizeOfItem(2);

/* minimal size of dependencies, when all deps are minimal */
#[inline]
const fn MinSizeOfItems(ndeps: usize) -> Size {
    SizeOfHeader + ndeps * MinSizeOfItem
}

/*
 * Internal state for DependencyGenerator of dependencies. Dependencies are similar to
 * k-permutations of n elements, except that the order does not matter for the
 * first (k-1) elements. That is, (a,b=>c) and (b,a=>c) are equivalent.
 */
#[repr(C)]
struct DependencyGeneratorData {
    k: c_int,                  /* size of the dependency */
    n: c_int,                  /* number of possible attributes */
    current: c_int,            /* next dependency to return (index) */
    ndependencies: AttrNumber, /* number of dependencies generated */
    dependencies: *mut AttrNumber, /* array of pre-generated dependencies	*/
}

type DependencyGenerator = *mut DependencyGeneratorData;

unsafe fn generate_dependencies_recurse(
    state: DependencyGenerator,
    index: c_int,
    start: AttrNumber,
    current: *mut AttrNumber,
) {
    /*
     * The generator handles the first (k-1) elements differently from the
     * last element.
     */
    if index < ((*state).k - 1) {
        let mut i: AttrNumber;

        /*
         * The first (k-1) values have to be in ascending order, which we
         * generate recursively.
         */

        i = start;
        while (i as c_int) < (*state).n {
            *current.offset(index as isize) = i;
            generate_dependencies_recurse(state, index + 1, i + 1, current);
            i += 1;
        }
    } else {
        let mut i: c_int;

        /*
         * the last element is the implied value, which does not respect the
         * ascending order. We just need to check that the value is not in the
         * first (k-1) elements.
         */

        i = 0;
        while i < (*state).n {
            let mut j: c_int;
            let mut match_: bool = false;

            *current.offset(index as isize) = i as AttrNumber;

            j = 0;
            while j < index {
                if *current.offset(j as isize) == i as AttrNumber {
                    match_ = true;
                    break;
                }
                j += 1;
            }

            /*
             * If the value is not found in the first part of the dependency,
             * we're done.
             */
            if !match_ {
                (*state).dependencies = repalloc(
                    (*state).dependencies as *mut c_void,
                    (*state).k as usize
                        * ((*state).ndependencies as usize + 1)
                        * core::mem::size_of::<AttrNumber>(),
                ) as *mut AttrNumber;
                libc_memcpy(
                    (*state)
                        .dependencies
                        .offset(((*state).k * (*state).ndependencies as c_int) as isize)
                        as *mut c_void,
                    current as *const c_void,
                    (*state).k as usize * core::mem::size_of::<AttrNumber>(),
                );
                (*state).ndependencies += 1;
            }
            i += 1;
        }
    }
}

/* generate all dependencies (k-permutations of n elements) */
unsafe fn generate_dependencies(state: DependencyGenerator) {
    let current: *mut AttrNumber =
        palloc0(core::mem::size_of::<AttrNumber>() * (*state).k as usize) as *mut AttrNumber;

    generate_dependencies_recurse(state, 0, 0, current);

    pfree(current as *mut c_void);
}

/*
 * initialize the DependencyGenerator of variations, and prebuild the variations
 *
 * This pre-builds all the variations. We could also generate them in
 * DependencyGenerator_next(), but this seems simpler.
 */
unsafe fn DependencyGenerator_init(n: c_int, k: c_int) -> DependencyGenerator {
    let state: DependencyGenerator;

    Assert!((n >= k) && (k > 0));

    /* allocate the DependencyGenerator state */
    state = palloc0(core::mem::size_of::<DependencyGeneratorData>()) as DependencyGenerator;
    (*state).dependencies =
        palloc(k as usize * core::mem::size_of::<AttrNumber>()) as *mut AttrNumber;

    (*state).ndependencies = 0;
    (*state).current = 0;
    (*state).k = k;
    (*state).n = n;

    /* now actually pre-generate all the variations */
    generate_dependencies(state);

    state
}

/* free the DependencyGenerator state */
unsafe fn DependencyGenerator_free(state: DependencyGenerator) {
    pfree((*state).dependencies as *mut c_void);
    pfree(state as *mut c_void);
}

/* generate next combination */
unsafe fn DependencyGenerator_next(state: DependencyGenerator) -> *mut AttrNumber {
    if (*state).current == (*state).ndependencies as c_int {
        return core::ptr::null_mut();
    }

    let ret = (*state)
        .dependencies
        .offset(((*state).k * (*state).current) as isize);
    (*state).current += 1;
    ret
}

/*
 * validates functional dependency on the data
 *
 * An actual work horse of detecting functional dependencies. Given a variation
 * of k attributes, it checks that the first (k-1) are sufficient to determine
 * the last one.
 */
unsafe fn dependency_degree(
    data: *mut StatsBuildData,
    k: c_int,
    dependency: *mut AttrNumber,
) -> f64 {
    let mut i: c_int;
    let mut nitems: c_int = 0;
    let mss: MultiSortSupport;
    let items: *mut SortItem;
    let attnums_dep: *mut AttrNumber;

    /* counters valid within a group */
    let mut group_size: c_int = 0;
    let mut n_violations: c_int = 0;

    /* total number of rows supporting (consistent with) the dependency */
    let mut n_supporting_rows: c_int = 0;

    /* Make sure we have at least two input attributes. */
    Assert!(k >= 2);

    /* sort info for all attributes columns */
    mss = multi_sort_init(k);

    /*
     * Translate the array of indexes to regular attnums for the dependency
     * (we will need this to identify the columns in StatsBuildData).
     */
    attnums_dep = palloc(k as usize * core::mem::size_of::<AttrNumber>()) as *mut AttrNumber;
    i = 0;
    while i < k {
        *attnums_dep.offset(i as isize) =
            *(*data).attnums.offset(*dependency.offset(i as isize) as isize);
        i += 1;
    }

    /*
     * Verify the dependency (a,b,...)->z, using a rather simple algorithm:
     *
     * (a) sort the data lexicographically
     *
     * (b) split the data into groups by first (k-1) columns
     *
     * (c) for each group count different values in the last column
     *
     * We use the column data types' default sort operators and collations;
     * perhaps at some point it'd be worth using column-specific collations?
     */

    /* prepare the sort function for the dimensions */
    i = 0;
    while i < k {
        let colstat: *mut VacAttrStats =
            *(*data).stats.offset(*dependency.offset(i as isize) as isize) as *mut VacAttrStats;
        let r#type: *mut TypeCacheEntry;

        r#type = lookup_type_cache((*colstat).attrtypid, TYPECACHE_LT_OPR);
        if (*r#type).lt_opr == InvalidOid
        /* shouldn't happen */
        {
            elog!(
                ERROR,
                "cache lookup failed for ordering operator for type {}",
                (*colstat).attrtypid
            );
            unreachable!();
        }

        /* prepare the sort function for this dimension */
        multi_sort_add_dimension(mss, i, (*r#type).lt_opr, (*colstat).attrcollid);
        i += 1;
    }

    /*
     * build an array of SortItem(s) sorted using the multi-sort support
     *
     * XXX This relies on all stats entries pointing to the same tuple
     * descriptor.  For now that assumption holds, but it might change in the
     * future for example if we support statistics on multiple tables.
     */
    items = build_sorted_items(data, &mut nitems, mss, k, attnums_dep);

    /*
     * Walk through the sorted array, split it into rows according to the
     * first (k-1) columns. If there's a single value in the last column, we
     * count the group as 'supporting' the functional dependency. Otherwise we
     * count it as contradicting.
     */

    /* start with the first row forming a group */
    group_size = 1;

    /* loop 1 beyond the end of the array so that we count the final group */
    i = 1;
    while i <= nitems {
        /*
         * Check if the group ended, which may be either because we processed
         * all the items (i==nitems), or because the i-th item is not equal to
         * the preceding one.
         */
        if i == nitems
            || multi_sort_compare_dims(
                0,
                k - 2,
                items.offset((i - 1) as isize),
                items.offset(i as isize),
                mss,
            ) != 0
        {
            /*
             * If no violations were found in the group then track the rows of
             * the group as supporting the functional dependency.
             */
            if n_violations == 0 {
                n_supporting_rows += group_size;
            }

            /* Reset counters for the new group */
            n_violations = 0;
            group_size = 1;
            i += 1;
            continue;
        }
        /* first columns match, but the last one does not (so contradicting) */
        else if multi_sort_compare_dim(
            k - 1,
            items.offset((i - 1) as isize),
            items.offset(i as isize),
            mss,
        ) != 0
        {
            n_violations += 1;
        }

        group_size += 1;
        i += 1;
    }

    /* Compute the 'degree of validity' as (supporting/total). */
    n_supporting_rows as f64 * 1.0 / (*data).numrows as f64
}

/*
 * detects functional dependencies between groups of columns
 *
 * Generates all possible subsets of columns (variations) and computes
 * the degree of validity for each one. For example when creating statistics
 * on three columns (a,b,c) there are 9 possible dependencies
 *
 *	   two columns			  three columns
 *	   -----------			  -------------
 *	   (a) -> b				  (a,b) -> c
 *	   (a) -> c				  (a,c) -> b
 *	   (b) -> a				  (b,c) -> a
 *	   (b) -> c
 *	   (c) -> a
 *	   (c) -> b
 */
pub unsafe fn statext_dependencies_build(data: *mut StatsBuildData) -> *mut MVDependencies {
    let mut i: c_int;
    let mut k: c_int;

    /* result */
    let mut dependencies: *mut MVDependencies = core::ptr::null_mut();
    let cxt: MemoryContext;

    Assert!((*data).nattnums >= 2);

    /* tracks memory allocated by dependency_degree calls */
    cxt = AllocSetContextCreate!(
        CurrentMemoryContext,
        c"dependency_degree cxt".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );

    /*
     * We'll try build functional dependencies starting from the smallest ones
     * covering just 2 columns, to the largest ones, covering all columns
     * included in the statistics object.  We start from the smallest ones
     * because we want to be able to skip already implied ones.
     */
    k = 2;
    while k <= (*data).nattnums {
        let mut dependency: *mut AttrNumber; /* array with k elements */

        /* prepare a DependencyGenerator of variation */
        let DependencyGenerator: DependencyGenerator =
            DependencyGenerator_init((*data).nattnums, k);

        /* generate all possible variations of k values (out of n) */
        loop {
            dependency = DependencyGenerator_next(DependencyGenerator);
            if dependency.is_null() {
                break;
            }

            let degree: f64;
            let d: *mut MVDependency;
            let oldcxt: MemoryContext;

            /* release memory used by dependency degree calculation */
            oldcxt = MemoryContextSwitchTo(cxt);

            /* compute how valid the dependency seems */
            degree = dependency_degree(data, k, dependency);

            MemoryContextSwitchTo(oldcxt);
            MemoryContextReset(cxt);

            /*
             * if the dependency seems entirely invalid, don't store it
             */
            if degree == 0.0 {
                continue;
            }

            d = palloc0(
                core::mem::offset_of!(MVDependency, attributes)
                    + k as usize * core::mem::size_of::<AttrNumber>(),
            ) as *mut MVDependency;

            /* copy the dependency (and keep the indexes into stxkeys) */
            (*d).degree = degree;
            (*d).nattributes = k as AttrNumber;
            i = 0;
            while i < k {
                *(*d).attributes.as_mut_ptr().offset(i as isize) =
                    *(*data).attnums.offset(*dependency.offset(i as isize) as isize);
                i += 1;
            }

            /* initialize the list of dependencies */
            if dependencies.is_null() {
                dependencies = palloc0(core::mem::size_of::<MVDependencies>()) as *mut MVDependencies;

                (*dependencies).magic = STATS_DEPS_MAGIC;
                (*dependencies).r#type = STATS_DEPS_TYPE_BASIC;
                (*dependencies).ndeps = 0;
            }

            (*dependencies).ndeps += 1;
            dependencies = repalloc(
                dependencies as *mut c_void,
                core::mem::offset_of!(MVDependencies, deps)
                    + (*dependencies).ndeps as usize * core::mem::size_of::<*mut MVDependency>(),
            ) as *mut MVDependencies;

            *(*dependencies)
                .deps
                .as_mut_ptr()
                .offset(((*dependencies).ndeps - 1) as isize) = d;
        }

        /*
         * we're done with variations of k elements, so free the
         * DependencyGenerator
         */
        DependencyGenerator_free(DependencyGenerator);
        k += 1;
    }

    MemoryContextDelete(cxt);

    dependencies
}

/*
 * Serialize list of dependencies into a bytea value.
 */
pub unsafe fn statext_dependencies_serialize(dependencies: *mut MVDependencies) -> *mut bytea {
    let mut i: c_int;
    let output: *mut bytea;
    let mut tmp: *mut c_char;
    let mut len: Size;

    /* we need to store ndeps, with a number of attributes for each one */
    len = VARHDRSZ as Size + SizeOfHeader;

    /* and also include space for the actual attribute numbers and degrees */
    i = 0;
    while i < (*dependencies).ndeps as c_int {
        len += SizeOfItem((**(*dependencies).deps.as_ptr().offset(i as isize)).nattributes as usize);
        i += 1;
    }

    output = palloc0(len) as *mut bytea;
    SET_VARSIZE(output, len as c_int);

    tmp = VARDATA(output as *mut c_void) as *mut c_char;

    /* Store the base struct values (magic, type, ndeps) */
    libc_memcpy(
        tmp as *mut c_void,
        &(*dependencies).magic as *const uint32 as *const c_void,
        core::mem::size_of::<uint32>(),
    );
    tmp = tmp.add(core::mem::size_of::<uint32>());
    libc_memcpy(
        tmp as *mut c_void,
        &(*dependencies).r#type as *const uint32 as *const c_void,
        core::mem::size_of::<uint32>(),
    );
    tmp = tmp.add(core::mem::size_of::<uint32>());
    libc_memcpy(
        tmp as *mut c_void,
        &(*dependencies).ndeps as *const uint32 as *const c_void,
        core::mem::size_of::<uint32>(),
    );
    tmp = tmp.add(core::mem::size_of::<uint32>());

    /* store number of attributes and attribute numbers for each dependency */
    i = 0;
    while i < (*dependencies).ndeps as c_int {
        let d: *mut MVDependency = *(*dependencies).deps.as_ptr().offset(i as isize);

        libc_memcpy(
            tmp as *mut c_void,
            &(*d).degree as *const f64 as *const c_void,
            core::mem::size_of::<f64>(),
        );
        tmp = tmp.add(core::mem::size_of::<f64>());

        libc_memcpy(
            tmp as *mut c_void,
            &(*d).nattributes as *const AttrNumber as *const c_void,
            core::mem::size_of::<AttrNumber>(),
        );
        tmp = tmp.add(core::mem::size_of::<AttrNumber>());

        libc_memcpy(
            tmp as *mut c_void,
            (*d).attributes.as_ptr() as *const c_void,
            core::mem::size_of::<AttrNumber>() * (*d).nattributes as usize,
        );
        tmp = tmp.add(core::mem::size_of::<AttrNumber>() * (*d).nattributes as usize);

        /* protect against overflow */
        Assert!(tmp <= (output as *mut c_char).add(len));
        i += 1;
    }

    /* make sure we've produced exactly the right amount of data */
    Assert!(tmp == (output as *mut c_char).add(len));

    output
}

/*
 * Reads serialized dependencies into MVDependencies structure.
 */
pub unsafe fn statext_dependencies_deserialize(data: *mut bytea) -> *mut MVDependencies {
    let mut i: c_int;
    let min_expected_size: Size;
    let mut dependencies: *mut MVDependencies;
    let mut tmp: *mut c_char;

    if data.is_null() {
        return core::ptr::null_mut();
    }

    if (VARSIZE_ANY_EXHDR(data) as Size) < SizeOfHeader {
        elog!(
            ERROR,
            "invalid MVDependencies size {} (expected at least {})",
            VARSIZE_ANY_EXHDR(data),
            SizeOfHeader
        );
        unreachable!();
    }

    /* read the MVDependencies header */
    dependencies = palloc0(core::mem::size_of::<MVDependencies>()) as *mut MVDependencies;

    /* initialize pointer to the data part (skip the varlena header) */
    tmp = VARDATA_ANY(data as *mut c_void) as *mut c_char;

    /* read the header fields and perform basic sanity checks */
    libc_memcpy(
        &mut (*dependencies).magic as *mut uint32 as *mut c_void,
        tmp as *const c_void,
        core::mem::size_of::<uint32>(),
    );
    tmp = tmp.add(core::mem::size_of::<uint32>());
    libc_memcpy(
        &mut (*dependencies).r#type as *mut uint32 as *mut c_void,
        tmp as *const c_void,
        core::mem::size_of::<uint32>(),
    );
    tmp = tmp.add(core::mem::size_of::<uint32>());
    libc_memcpy(
        &mut (*dependencies).ndeps as *mut uint32 as *mut c_void,
        tmp as *const c_void,
        core::mem::size_of::<uint32>(),
    );
    tmp = tmp.add(core::mem::size_of::<uint32>());

    if (*dependencies).magic != STATS_DEPS_MAGIC {
        elog!(
            ERROR,
            "invalid dependency magic {} (expected {})",
            (*dependencies).magic,
            STATS_DEPS_MAGIC
        );
        unreachable!();
    }

    if (*dependencies).r#type != STATS_DEPS_TYPE_BASIC {
        elog!(
            ERROR,
            "invalid dependency type {} (expected {})",
            (*dependencies).r#type,
            STATS_DEPS_TYPE_BASIC
        );
        unreachable!();
    }

    if (*dependencies).ndeps == 0 {
        elog!(ERROR, "invalid zero-length item array in MVDependencies");
        unreachable!();
    }

    /* what minimum bytea size do we expect for those parameters */
    min_expected_size = SizeOfItem((*dependencies).ndeps as usize);

    if (VARSIZE_ANY_EXHDR(data) as Size) < min_expected_size {
        elog!(
            ERROR,
            "invalid dependencies size {} (expected at least {})",
            VARSIZE_ANY_EXHDR(data),
            min_expected_size
        );
        unreachable!();
    }

    /* allocate space for the MCV items */
    dependencies = repalloc(
        dependencies as *mut c_void,
        core::mem::offset_of!(MVDependencies, deps)
            + ((*dependencies).ndeps as usize * core::mem::size_of::<*mut MVDependency>()),
    ) as *mut MVDependencies;

    i = 0;
    while i < (*dependencies).ndeps as c_int {
        let mut degree: f64 = 0.0;
        let mut k: AttrNumber = 0;
        let d: *mut MVDependency;

        /* degree of validity */
        libc_memcpy(
            &mut degree as *mut f64 as *mut c_void,
            tmp as *const c_void,
            core::mem::size_of::<f64>(),
        );
        tmp = tmp.add(core::mem::size_of::<f64>());

        /* number of attributes */
        libc_memcpy(
            &mut k as *mut AttrNumber as *mut c_void,
            tmp as *const c_void,
            core::mem::size_of::<AttrNumber>(),
        );
        tmp = tmp.add(core::mem::size_of::<AttrNumber>());

        /* is the number of attributes valid? */
        Assert!((k >= 2) && (k as usize <= STATS_MAX_DIMENSIONS));

        /* now that we know the number of attributes, allocate the dependency */
        d = palloc0(
            core::mem::offset_of!(MVDependency, attributes)
                + (k as usize * core::mem::size_of::<AttrNumber>()),
        ) as *mut MVDependency;

        (*d).degree = degree;
        (*d).nattributes = k;

        /* copy attribute numbers */
        libc_memcpy(
            (*d).attributes.as_mut_ptr() as *mut c_void,
            tmp as *const c_void,
            core::mem::size_of::<AttrNumber>() * (*d).nattributes as usize,
        );
        tmp = tmp.add(core::mem::size_of::<AttrNumber>() * (*d).nattributes as usize);

        *(*dependencies).deps.as_mut_ptr().offset(i as isize) = d;

        /* still within the bytea */
        Assert!(tmp <= (data as *mut c_char).add(VARSIZE_ANY(data) as usize));
        i += 1;
    }

    /* we should have consumed the whole bytea exactly */
    Assert!(tmp == (data as *mut c_char).add(VARSIZE_ANY(data) as usize));

    dependencies
}

/*
 * dependency_is_fully_matched
 *		checks that a functional dependency is fully matched given clauses on
 *		attributes (assuming the clauses are suitable equality clauses)
 */
unsafe fn dependency_is_fully_matched(
    dependency: *mut MVDependency,
    attnums: *mut Bitmapset,
) -> bool {
    let mut j: c_int;

    /*
     * Check that the dependency actually is fully covered by clauses. We have
     * to translate all attribute numbers, as those are referenced
     */
    j = 0;
    while j < (*dependency).nattributes as c_int {
        let attnum: c_int = *(*dependency).attributes.as_ptr().offset(j as isize) as c_int;

        if !bms_is_member(attnum, attnums) {
            return false;
        }
        j += 1;
    }

    true
}

/*
 * statext_dependencies_load
 *		Load the functional dependencies for the indicated pg_statistic_ext tuple
 */
pub unsafe fn statext_dependencies_load(mvoid: Oid, inh: bool) -> *mut MVDependencies {
    let result: *mut MVDependencies;
    let mut isnull: bool = false;
    let deps: Datum;
    let htup: HeapTuple;

    htup = SearchSysCache2(
        STATEXTDATASTXOID,
        ObjectIdGetDatum(mvoid),
        BoolGetDatum(inh),
    );
    if !HeapTupleIsValid(htup) {
        elog!(ERROR, "cache lookup failed for statistics object {}", mvoid);
        unreachable!();
    }

    deps = SysCacheGetAttr(
        STATEXTDATASTXOID,
        htup,
        Anum_pg_statistic_ext_data_stxddependencies,
        &mut isnull,
    );
    if isnull {
        elog!(
            ERROR,
            "requested statistics kind \"{}\" is not yet built for statistics object {}",
            STATS_EXT_DEPENDENCIES as u8 as char,
            mvoid
        );
        unreachable!();
    }

    result = statext_dependencies_deserialize(DatumGetByteaPP(deps));

    ReleaseSysCache(htup);

    result
}

/*
 * pg_dependencies_in		- input routine for type pg_dependencies.
 *
 * pg_dependencies is real enough to be a table column, but it has no operations
 * of its own, and disallows input too
 */
pub unsafe fn pg_dependencies_in(_fcinfo: FunctionCallInfo) -> Datum {
    /*
     * pg_node_list stores the data in binary form and parsing text input is
     * not needed, so disallow this.
     */
    ereport!(
        ERROR,
        errmsg!("cannot accept a value of type {}", "pg_dependencies")
    );
    // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)

    PG_RETURN_VOID() /* keep compiler quiet */
}

/*
 * pg_dependencies		- output routine for type pg_dependencies.
 */
pub unsafe fn pg_dependencies_out(fcinfo: FunctionCallInfo) -> Datum {
    let data: *mut bytea = PG_GETARG_BYTEA_PP(fcinfo, 0);
    let dependencies: *mut MVDependencies = statext_dependencies_deserialize(data);
    let mut i: c_int;
    let mut j: c_int;
    let mut str: StringInfoData = core::mem::zeroed();

    initStringInfo(&mut str);
    appendStringInfoChar(&mut str, b'{' as c_char);

    i = 0;
    while i < (*dependencies).ndeps as c_int {
        let dependency: *mut MVDependency = *(*dependencies).deps.as_ptr().offset(i as isize);

        if i > 0 {
            appendStringInfoString(&mut str, c", ".as_ptr());
        }

        appendStringInfoChar(&mut str, b'"' as c_char);
        j = 0;
        while j < (*dependency).nattributes as c_int {
            if j == (*dependency).nattributes as c_int - 1 {
                appendStringInfoString(&mut str, c" => ".as_ptr());
            } else if j > 0 {
                appendStringInfoString(&mut str, c", ".as_ptr());
            }

            appendStringInfo(
                &mut str,
                c"%d".as_ptr(),
                *(*dependency).attributes.as_ptr().offset(j as isize) as c_int,
            );
            j += 1;
        }
        appendStringInfo(&mut str, c"\": %f".as_ptr(), (*dependency).degree);
        i += 1;
    }

    appendStringInfoChar(&mut str, b'}' as c_char);

    PG_RETURN_CSTRING(str.data)
}

/*
 * pg_dependencies_recv		- binary input routine for type pg_dependencies.
 */
pub unsafe fn pg_dependencies_recv(_fcinfo: FunctionCallInfo) -> Datum {
    ereport!(
        ERROR,
        errmsg!("cannot accept a value of type {}", "pg_dependencies")
    );
    // C also: errcode(ERRCODE_FEATURE_NOT_SUPPORTED)

    PG_RETURN_VOID() /* keep compiler quiet */
}

/*
 * pg_dependencies_send		- binary output routine for type pg_dependencies.
 *
 * Functional dependencies are serialized in a bytea value (although the type
 * is named differently), so let's just send that.
 */
pub unsafe fn pg_dependencies_send(fcinfo: FunctionCallInfo) -> Datum {
    byteasend(fcinfo)
}

/*
 * dependency_is_compatible_clause
 *		Determines if the clause is compatible with functional dependencies
 *
 * Only clauses that have the form of equality to a pseudoconstant, or can be
 * interpreted that way, are currently accepted.  Furthermore the variable
 * part of the clause must be a simple Var belonging to the specified
 * relation, whose attribute number we return in *attnum on success.
 */
unsafe fn dependency_is_compatible_clause(
    mut clause: *mut Node,
    relid: Index,
    attnum: *mut AttrNumber,
) -> bool {
    let var: *mut Var;
    let mut clause_expr: *mut Node;

    if IsA!(clause, T_RestrictInfo) {
        let rinfo: *mut RestrictInfo = clause as *mut RestrictInfo;

        /* Pseudoconstants are not interesting (they couldn't contain a Var) */
        if (*rinfo).pseudoconstant {
            return false;
        }

        /* Clauses referencing multiple, or no, varnos are incompatible */
        if bms_membership((*rinfo).clause_relids) != BMS_Membership::BMS_SINGLETON {
            return false;
        }

        clause = (*rinfo).clause as *mut Node;
    }

    if is_opclause(clause as *const c_void) {
        /* If it's an opclause, check for Var = Const or Const = Var. */
        let expr: *mut OpExpr = clause as *mut OpExpr;

        /* Only expressions with two arguments are candidates. */
        if list_length((*expr).args) != 2 {
            return false;
        }

        /* Make sure non-selected argument is a pseudoconstant. */
        if is_pseudo_constant_clause(lsecond((*expr).args) as *mut Node) {
            clause_expr = linitial((*expr).args) as *mut Node;
        } else if is_pseudo_constant_clause(linitial((*expr).args) as *mut Node) {
            clause_expr = lsecond((*expr).args) as *mut Node;
        } else {
            return false;
        }

        /*
         * If it's not an "=" operator, just ignore the clause, as it's not
         * compatible with functional dependencies.
         *
         * This uses the function for estimating selectivity, not the operator
         * directly (a bit awkward, but well ...).
         *
         * XXX this is pretty dubious; probably it'd be better to check btree
         * or hash opclass membership, so as not to be fooled by custom
         * selectivity functions, and to be more consistent with decisions
         * elsewhere in the planner.
         */
        if get_oprrest((*expr).opno) != F_EQSEL {
            return false;
        }

        /* OK to proceed with checking "var" */
    } else if IsA!(clause, T_ScalarArrayOpExpr) {
        /* If it's a scalar array operator, check for Var IN Const. */
        let expr: *mut ScalarArrayOpExpr = clause as *mut ScalarArrayOpExpr;

        /*
         * Reject ALL() variant, we only care about ANY/IN.
         *
         * XXX Maybe we should check if all the values are the same, and allow
         * ALL in that case? Doesn't seem very practical, though.
         */
        if !(*expr).useOr {
            return false;
        }

        /* Only expressions with two arguments are candidates. */
        if list_length((*expr).args) != 2 {
            return false;
        }

        /*
         * We know it's always (Var IN Const), so we assume the var is the
         * first argument, and pseudoconstant is the second one.
         */
        if !is_pseudo_constant_clause(lsecond((*expr).args) as *mut Node) {
            return false;
        }

        clause_expr = linitial((*expr).args) as *mut Node;

        /*
         * If it's not an "=" operator, just ignore the clause, as it's not
         * compatible with functional dependencies. The operator is identified
         * simply by looking at which function it uses to estimate
         * selectivity. That's a bit strange, but it's what other similar
         * places do.
         */
        if get_oprrest((*expr).opno) != F_EQSEL {
            return false;
        }

        /* OK to proceed with checking "var" */
    } else if is_orclause(clause as *const c_void) {
        let bool_expr: *mut BoolExpr = clause as *mut BoolExpr;
        let lc: *mut c_void;

        /* start with no attribute number */
        *attnum = InvalidAttrNumber;

        foreach!(lc, (*bool_expr).args, {
            let mut clause_attnum: AttrNumber = 0;

            /*
             * Had we found incompatible clause in the arguments, treat the
             * whole clause as incompatible.
             */
            if !dependency_is_compatible_clause(
                lfirst(current_cell!(lc)) as *mut Node,
                relid,
                &mut clause_attnum,
            ) {
                return false;
            }

            if *attnum == InvalidAttrNumber {
                *attnum = clause_attnum;
            }

            /* ensure all the variables are the same (same attnum) */
            if *attnum != clause_attnum {
                return false;
            }
        });

        /* the Var is already checked by the recursive call */
        return true;
    } else if is_notclause(clause as *const c_void) {
        /*
         * "NOT x" can be interpreted as "x = false", so get the argument and
         * proceed with seeing if it's a suitable Var.
         */
        clause_expr = get_notclausearg(clause) as *mut Node;
    } else {
        /*
         * A boolean expression "x" can be interpreted as "x = true", so
         * proceed with seeing if it's a suitable Var.
         */
        clause_expr = clause;
    }

    /*
     * We may ignore any RelabelType node above the operand.  (There won't be
     * more than one, since eval_const_expressions has been applied already.)
     */
    if IsA!(clause_expr, T_RelabelType) {
        clause_expr = (*(clause_expr as *mut RelabelType)).arg as *mut Node;
    }

    /* We only support plain Vars for now */
    if !IsA!(clause_expr, T_Var) {
        return false;
    }

    /* OK, we know we have a Var */
    var = clause_expr as *mut Var;

    /* Ensure Var is from the correct relation */
    if (*var).varno as Index != relid {
        return false;
    }

    /* We also better ensure the Var is from the current level */
    if (*var).varlevelsup != 0 {
        return false;
    }

    /* Also ignore system attributes (we don't allow stats on those) */
    if !AttrNumberIsForUserDefinedAttr((*var).varattno) {
        return false;
    }

    *attnum = (*var).varattno;
    true
}

/*
 * find_strongest_dependency
 *		find the strongest dependency on the attributes
 *
 * When applying functional dependencies, we start with the strongest
 * dependencies. That is, we select the dependency that:
 *
 * (a) has all attributes covered by equality clauses
 *
 * (b) has the most attributes
 *
 * (c) has the highest degree of validity
 *
 * This guarantees that we eliminate the most redundant conditions first
 * (see the comment in dependencies_clauselist_selectivity).
 */
unsafe fn find_strongest_dependency(
    dependencies: *mut *mut MVDependencies,
    ndependencies: c_int,
    attnums: *mut Bitmapset,
) -> *mut MVDependency {
    let mut i: c_int;
    let mut j: c_int;
    let mut strongest: *mut MVDependency = core::ptr::null_mut();

    /* number of attnums in clauses */
    let nattnums: c_int = bms_num_members(attnums);

    /*
     * Iterate over the MVDependency items and find the strongest one from the
     * fully-matched dependencies. We do the cheap checks first, before
     * matching it against the attnums.
     */
    i = 0;
    while i < ndependencies {
        j = 0;
        while j < (**dependencies.offset(i as isize)).ndeps as c_int {
            let dependency: *mut MVDependency =
                *(**dependencies.offset(i as isize)).deps.as_ptr().offset(j as isize);

            /*
             * Skip dependencies referencing more attributes than available
             * clauses, as those can't be fully matched.
             */
            if (*dependency).nattributes as c_int > nattnums {
                j += 1;
                continue;
            }

            if !strongest.is_null() {
                /* skip dependencies on fewer attributes than the strongest. */
                if (*dependency).nattributes < (*strongest).nattributes {
                    j += 1;
                    continue;
                }

                /* also skip weaker dependencies when attribute count matches */
                if (*strongest).nattributes == (*dependency).nattributes
                    && (*strongest).degree > (*dependency).degree
                {
                    j += 1;
                    continue;
                }
            }

            /*
             * this dependency is stronger, but we must still check that it's
             * fully matched to these attnums. We perform this check last as
             * it's slightly more expensive than the previous checks.
             */
            if dependency_is_fully_matched(dependency, attnums) {
                strongest = dependency; /* save new best match */
            }
            j += 1;
        }
        i += 1;
    }

    strongest
}

/*
 * clauselist_apply_dependencies
 *		Apply the specified functional dependencies to a list of clauses and
 *		return the estimated selectivity of the clauses that are compatible
 *		with any of the given dependencies.
 *
 * This will estimate all not-already-estimated clauses that are compatible
 * with functional dependencies, and which have an attribute mentioned by any
 * of the given dependencies (either as an implying or implied attribute).
 *
 * Given (lists of) clauses on attributes (a,b) and a functional dependency
 * (a=>b), the per-column selectivities P(a) and P(b) are notionally combined
 * using the formula
 *
 *		P(a,b) = f * P(a) + (1-f) * P(a) * P(b)
 *
 * where 'f' is the degree of dependency.  This reflects the fact that we
 * expect a fraction f of all rows to be consistent with the dependency
 * (a=>b), and so have a selectivity of P(a), while the remaining rows are
 * treated as independent.
 *
 * In practice, we use a slightly modified version of this formula, which uses
 * a selectivity of Min(P(a), P(b)) for the dependent rows, since the result
 * should obviously not exceed either column's individual selectivity.  I.e.,
 * we actually combine selectivities using the formula
 *
 *		P(a,b) = f * Min(P(a), P(b)) + (1-f) * P(a) * P(b)
 *
 * This can make quite a difference if the specific values matching the
 * clauses are not consistent with the functional dependency.
 */
unsafe fn clauselist_apply_dependencies(
    root: *mut PlannerInfo,
    clauses: *mut List,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    dependencies: *mut *mut MVDependency,
    ndependencies: c_int,
    list_attnums: *mut AttrNumber,
    estimatedclauses: *mut *mut Bitmapset,
) -> Selectivity {
    let mut attnums: *mut Bitmapset;
    let mut i: c_int;
    let mut j: c_int;
    let nattrs: c_int;
    let attr_sel: *mut Selectivity;
    let mut attidx: c_int;
    let mut listidx: c_int;
    let l: *mut c_void;
    let mut s1: Selectivity;

    /*
     * Extract the attnums of all implying and implied attributes from all the
     * given dependencies.  Each of these attributes is expected to have at
     * least 1 not-already-estimated compatible clause that we will estimate
     * here.
     */
    attnums = core::ptr::null_mut();
    i = 0;
    while i < ndependencies {
        j = 0;
        while j < (**dependencies.offset(i as isize)).nattributes as c_int {
            let attnum: AttrNumber =
                *(**dependencies.offset(i as isize)).attributes.as_ptr().offset(j as isize);

            attnums = bms_add_member(attnums, attnum as c_int);
            j += 1;
        }
        i += 1;
    }

    /*
     * Compute per-column selectivity estimates for each of these attributes,
     * and mark all the corresponding clauses as estimated.
     */
    nattrs = bms_num_members(attnums);
    attr_sel = palloc(core::mem::size_of::<Selectivity>() * nattrs as usize) as *mut Selectivity;

    attidx = 0;
    i = -1;
    loop {
        i = bms_next_member(attnums, i);
        if i < 0 {
            break;
        }

        let mut attr_clauses: *mut List = core::ptr::null_mut();
        let simple_sel: Selectivity;

        listidx = -1;
        foreach!(l, clauses, {
            let clause: *mut Node = lfirst(current_cell!(l)) as *mut Node;

            listidx += 1;
            if *list_attnums.offset(listidx as isize) as c_int == i {
                attr_clauses = lappend(attr_clauses, clause as *mut c_void);
                *estimatedclauses = bms_add_member(*estimatedclauses, listidx);
            }
        });

        simple_sel =
            clauselist_selectivity_ext(root, attr_clauses, varRelid, jointype, sjinfo, false);
        *attr_sel.offset(attidx as isize) = simple_sel;
        attidx += 1;
    }

    /*
     * Now combine these selectivities using the dependency information.  For
     * chains of dependencies such as a -> b -> c, the b -> c dependency will
     * come before the a -> b dependency in the array, so we traverse the
     * array backwards to ensure such chains are computed in the right order.
     *
     * As explained above, pairs of selectivities are combined using the
     * formula
     *
     * P(a,b) = f * Min(P(a), P(b)) + (1-f) * P(a) * P(b)
     *
     * to ensure that the combined selectivity is never greater than either
     * individual selectivity.
     *
     * Where multiple dependencies apply (e.g., a -> b -> c), we use
     * conditional probabilities to compute the overall result as follows:
     *
     * P(a,b,c) = P(c|a,b) * P(a,b) = P(c|a,b) * P(b|a) * P(a)
     *
     * so we replace the selectivities of all implied attributes with
     * conditional probabilities, that are conditional on all their implying
     * attributes.  The selectivities of all other non-implied attributes are
     * left as they are.
     */
    i = ndependencies - 1;
    while i >= 0 {
        let dependency: *mut MVDependency = *dependencies.offset(i as isize);
        let mut attnum: AttrNumber;
        let s2: Selectivity;
        let f: f64;

        /* Selectivity of all the implying attributes */
        s1 = 1.0;
        j = 0;
        while j < (*dependency).nattributes as c_int - 1 {
            attnum = *(*dependency).attributes.as_ptr().offset(j as isize);
            attidx = bms_member_index(attnums, attnum as c_int);
            s1 *= *attr_sel.offset(attidx as isize);
            j += 1;
        }

        /* Original selectivity of the implied attribute */
        attnum = *(*dependency).attributes.as_ptr().offset(j as isize);
        attidx = bms_member_index(attnums, attnum as c_int);
        s2 = *attr_sel.offset(attidx as isize);

        /*
         * Replace s2 with the conditional probability s2 given s1, computed
         * using the formula P(b|a) = P(a,b) / P(a), which simplifies to
         *
         * P(b|a) = f * Min(P(a), P(b)) / P(a) + (1-f) * P(b)
         *
         * where P(a) = s1, the selectivity of the implying attributes, and
         * P(b) = s2, the selectivity of the implied attribute.
         */
        f = (*dependency).degree;

        if s1 <= s2 {
            *attr_sel.offset(attidx as isize) = f + (1.0 - f) * s2;
        } else {
            *attr_sel.offset(attidx as isize) = f * s2 / s1 + (1.0 - f) * s2;
        }
        i -= 1;
    }

    /*
     * The overall selectivity of all the clauses on all these attributes is
     * then the product of all the original (non-implied) probabilities and
     * the new conditional (implied) probabilities.
     */
    s1 = 1.0;
    i = 0;
    while i < nattrs {
        s1 *= *attr_sel.offset(i as isize);
        i += 1;
    }

    CLAMP_PROBABILITY(&mut s1);

    pfree(attr_sel as *mut c_void);
    bms_free(attnums);

    s1
}

/*
 * dependency_is_compatible_expression
 *		Determines if the expression is compatible with functional dependencies
 *
 * Similar to dependency_is_compatible_clause, but doesn't enforce that the
 * expression is a simple Var.  On success, return the matching statistics
 * expression into *expr.
 */
unsafe fn dependency_is_compatible_expression(
    mut clause: *mut Node,
    relid: Index,
    statlist: *mut List,
    expr: *mut *mut Node,
) -> bool {
    let lc: *mut c_void;
    let lc2: *mut c_void;
    let mut clause_expr: *mut Node;

    if IsA!(clause, T_RestrictInfo) {
        let rinfo: *mut RestrictInfo = clause as *mut RestrictInfo;

        /* Pseudoconstants are not interesting (they couldn't contain a Var) */
        if (*rinfo).pseudoconstant {
            return false;
        }

        /* Clauses referencing multiple, or no, varnos are incompatible */
        if bms_membership((*rinfo).clause_relids) != BMS_Membership::BMS_SINGLETON {
            return false;
        }

        clause = (*rinfo).clause as *mut Node;
    }

    if is_opclause(clause as *const c_void) {
        /* If it's an opclause, check for Var = Const or Const = Var. */
        let op_expr: *mut OpExpr = clause as *mut OpExpr;

        /* Only expressions with two arguments are candidates. */
        if list_length((*op_expr).args) != 2 {
            return false;
        }

        /* Make sure non-selected argument is a pseudoconstant. */
        if is_pseudo_constant_clause(lsecond((*op_expr).args) as *mut Node) {
            clause_expr = linitial((*op_expr).args) as *mut Node;
        } else if is_pseudo_constant_clause(linitial((*op_expr).args) as *mut Node) {
            clause_expr = lsecond((*op_expr).args) as *mut Node;
        } else {
            return false;
        }

        /*
         * If it's not an "=" operator, just ignore the clause, as it's not
         * compatible with functional dependencies.
         *
         * This uses the function for estimating selectivity, not the operator
         * directly (a bit awkward, but well ...).
         *
         * XXX this is pretty dubious; probably it'd be better to check btree
         * or hash opclass membership, so as not to be fooled by custom
         * selectivity functions, and to be more consistent with decisions
         * elsewhere in the planner.
         */
        if get_oprrest((*op_expr).opno) != F_EQSEL {
            return false;
        }

        /* OK to proceed with checking "var" */
    } else if IsA!(clause, T_ScalarArrayOpExpr) {
        /* If it's a scalar array operator, check for Var IN Const. */
        let saop_expr: *mut ScalarArrayOpExpr = clause as *mut ScalarArrayOpExpr;

        /*
         * Reject ALL() variant, we only care about ANY/IN.
         *
         * FIXME Maybe we should check if all the values are the same, and
         * allow ALL in that case? Doesn't seem very practical, though.
         */
        if !(*saop_expr).useOr {
            return false;
        }

        /* Only expressions with two arguments are candidates. */
        if list_length((*saop_expr).args) != 2 {
            return false;
        }

        /*
         * We know it's always (Var IN Const), so we assume the var is the
         * first argument, and pseudoconstant is the second one.
         */
        if !is_pseudo_constant_clause(lsecond((*saop_expr).args) as *mut Node) {
            return false;
        }

        clause_expr = linitial((*saop_expr).args) as *mut Node;

        /*
         * If it's not an "=" operator, just ignore the clause, as it's not
         * compatible with functional dependencies. The operator is identified
         * simply by looking at which function it uses to estimate
         * selectivity. That's a bit strange, but it's what other similar
         * places do.
         */
        if get_oprrest((*saop_expr).opno) != F_EQSEL {
            return false;
        }

        /* OK to proceed with checking "var" */
    } else if is_orclause(clause as *const c_void) {
        let bool_expr: *mut BoolExpr = clause as *mut BoolExpr;

        /* start with no expression (we'll use the first match) */
        *expr = core::ptr::null_mut();

        foreach!(lc, (*bool_expr).args, {
            let mut or_expr: *mut Node = core::ptr::null_mut();

            /*
             * Had we found incompatible expression in the arguments, treat
             * the whole expression as incompatible.
             */
            if !dependency_is_compatible_expression(
                lfirst(current_cell!(lc)) as *mut Node,
                relid,
                statlist,
                &mut or_expr,
            ) {
                return false;
            }

            if (*expr).is_null() {
                *expr = or_expr;
            }

            /* ensure all the expressions are the same */
            if !equal(or_expr as *const c_void, *expr as *const c_void) {
                return false;
            }
        });

        /* the expression is already checked by the recursive call */
        return true;
    } else if is_notclause(clause as *const c_void) {
        /*
         * "NOT x" can be interpreted as "x = false", so get the argument and
         * proceed with seeing if it's a suitable Var.
         */
        clause_expr = get_notclausearg(clause) as *mut Node;
    } else {
        /*
         * A boolean expression "x" can be interpreted as "x = true", so
         * proceed with seeing if it's a suitable Var.
         */
        clause_expr = clause;
    }

    /*
     * We may ignore any RelabelType node above the operand.  (There won't be
     * more than one, since eval_const_expressions has been applied already.)
     */
    if IsA!(clause_expr, T_RelabelType) {
        clause_expr = (*(clause_expr as *mut RelabelType)).arg as *mut Node;
    }

    /*
     * Search for a matching statistics expression.
     */
    foreach!(lc, statlist, {
        'next_stat: {
            let info: *mut StatisticExtInfo = lfirst(current_cell!(lc)) as *mut StatisticExtInfo;

            /* ignore stats without dependencies */
            if (*info).kind != STATS_EXT_DEPENDENCIES {
                break 'next_stat;
            }

            foreach!(lc2, (*info).exprs, {
                let stat_expr: *mut Node = lfirst(current_cell!(lc2)) as *mut Node;

                if equal(clause_expr as *const c_void, stat_expr as *const c_void) {
                    *expr = stat_expr;
                    return true;
                }
            });
        }
    });

    false
}

/*
 * dependencies_clauselist_selectivity
 *		Return the estimated selectivity of (a subset of) the given clauses
 *		using functional dependency statistics, or 1.0 if no useful functional
 *		dependency statistic exists.
 *
 * 'estimatedclauses' is an input/output argument that gets a bit set
 * corresponding to the (zero-based) list index of each clause that is included
 * in the estimated selectivity.
 *
 * Given equality clauses on attributes (a,b) we find the strongest dependency
 * between them, i.e. either (a=>b) or (b=>a). Assuming (a=>b) is the selected
 * dependency, we then combine the per-clause selectivities using the formula
 *
 *	   P(a,b) = f * P(a) + (1-f) * P(a) * P(b)
 *
 * where 'f' is the degree of the dependency.  (Actually we use a slightly
 * modified version of this formula -- see clauselist_apply_dependencies()).
 *
 * With clauses on more than two attributes, the dependencies are applied
 * recursively, starting with the widest/strongest dependencies. For example
 * P(a,b,c) is first split like this:
 *
 *	   P(a,b,c) = f * P(a,b) + (1-f) * P(a,b) * P(c)
 *
 * assuming (a,b=>c) is the strongest dependency.
 */
pub unsafe fn dependencies_clauselist_selectivity(
    root: *mut PlannerInfo,
    clauses: *mut List,
    varRelid: c_int,
    jointype: JoinType,
    sjinfo: *mut SpecialJoinInfo,
    rel: *mut RelOptInfo,
    estimatedclauses: *mut *mut Bitmapset,
) -> Selectivity {
    let mut s1: Selectivity = 1.0;
    let l: *mut c_void;
    let mut clauses_attnums: *mut Bitmapset = core::ptr::null_mut();
    let list_attnums: *mut AttrNumber;
    let mut listidx: c_int;
    let func_dependencies: *mut *mut MVDependencies;
    let mut nfunc_dependencies: c_int;
    let mut total_ndeps: c_int;
    let dependencies: *mut *mut MVDependency;
    let mut ndependencies: c_int;
    let mut i: c_int;
    let attnum_offset: AttrNumber;
    let rte: *mut RangeTblEntry = planner_rt_fetch((*rel).relid, root);

    /* unique expressions */
    let unique_exprs: *mut *mut Node;
    let mut unique_exprs_cnt: c_int;

    /* check if there's any stats that might be useful for us. */
    if !has_stats_of_kind((*rel).statlist, STATS_EXT_DEPENDENCIES) {
        return 1.0;
    }

    list_attnums = palloc(core::mem::size_of::<AttrNumber>() * list_length(clauses) as usize)
        as *mut AttrNumber;

    /*
     * We allocate space as if every clause was a unique expression, although
     * that's probably overkill. Some will be simple column references that
     * we'll translate to attnums, and there might be duplicates. But it's
     * easier and cheaper to just do one allocation than repalloc later.
     */
    unique_exprs =
        palloc(core::mem::size_of::<*mut Node>() * list_length(clauses) as usize) as *mut *mut Node;
    unique_exprs_cnt = 0;

    /*
     * Pre-process the clauses list to extract the attnums seen in each item.
     * We need to determine if there's any clauses which will be useful for
     * dependency selectivity estimations. Along the way we'll record all of
     * the attnums for each clause in a list which we'll reference later so we
     * don't need to repeat the same work again. We'll also keep track of all
     * attnums seen.
     *
     * We also skip clauses that we already estimated using different types of
     * statistics (we treat them as incompatible).
     *
     * To handle expressions, we assign them negative attnums, as if it was a
     * system attribute (this is fine, as we only allow extended stats on user
     * attributes). And then we offset everything by the number of
     * expressions, so that we can store the values in a bitmapset.
     */
    listidx = 0;
    foreach!(l, clauses, {
        let clause: *mut Node = lfirst(current_cell!(l)) as *mut Node;
        let mut attnum: AttrNumber = 0;
        let mut expr: *mut Node = core::ptr::null_mut();

        /* ignore clause by default */
        *list_attnums.offset(listidx as isize) = InvalidAttrNumber;

        if !bms_is_member(listidx, *estimatedclauses) {
            /*
             * If it's a simple column reference, just extract the attnum. If
             * it's an expression, assign a negative attnum as if it was a
             * system attribute.
             */
            if dependency_is_compatible_clause(clause, (*rel).relid, &mut attnum) {
                *list_attnums.offset(listidx as isize) = attnum;
            } else if dependency_is_compatible_expression(
                clause,
                (*rel).relid,
                (*rel).statlist,
                &mut expr,
            ) {
                /* special attnum assigned to this expression */
                attnum = InvalidAttrNumber;

                Assert!(!expr.is_null());

                /* If the expression is duplicate, use the same attnum. */
                i = 0;
                while i < unique_exprs_cnt {
                    if equal(
                        *unique_exprs.offset(i as isize) as *const c_void,
                        expr as *const c_void,
                    ) {
                        /* negative attribute number to expression */
                        attnum = -(i + 1) as AttrNumber;
                        break;
                    }
                    i += 1;
                }

                /* not found in the list, so add it */
                if attnum == InvalidAttrNumber {
                    *unique_exprs.offset(unique_exprs_cnt as isize) = expr;
                    unique_exprs_cnt += 1;

                    /* after incrementing the value, to get -1, -2, ... */
                    attnum = -unique_exprs_cnt as AttrNumber;
                }

                /* remember which attnum was assigned to this clause */
                *list_attnums.offset(listidx as isize) = attnum;
            }
        }

        listidx += 1;
    });

    Assert!(listidx == list_length(clauses));

    /*
     * How much we need to offset the attnums? If there are no expressions,
     * then no offset is needed. Otherwise we need to offset enough for the
     * lowest value (-unique_exprs_cnt) to become 1.
     */
    if unique_exprs_cnt > 0 {
        attnum_offset = (unique_exprs_cnt + 1) as AttrNumber;
    } else {
        attnum_offset = 0;
    }

    /*
     * Now that we know how many expressions there are, we can offset the
     * values just enough to build the bitmapset.
     */
    i = 0;
    while i < list_length(clauses) {
        let attnum: AttrNumber;

        /* ignore incompatible or already estimated clauses */
        if *list_attnums.offset(i as isize) == InvalidAttrNumber {
            i += 1;
            continue;
        }

        /* make sure the attnum is in the expected range */
        Assert!(*list_attnums.offset(i as isize) >= (-unique_exprs_cnt) as AttrNumber);
        Assert!(*list_attnums.offset(i as isize) as c_int <= MaxHeapAttributeNumber);

        /* make sure the attnum is positive (valid AttrNumber) */
        attnum = *list_attnums.offset(i as isize) + attnum_offset;

        /*
         * Either it's a regular attribute, or it's an expression, in which
         * case we must not have seen it before (expressions are unique).
         *
         * XXX Check whether it's a regular attribute has to be done using the
         * original attnum, while the second check has to use the value with
         * an offset.
         */
        Assert!(
            AttrNumberIsForUserDefinedAttr(*list_attnums.offset(i as isize))
                || !bms_is_member(attnum as c_int, clauses_attnums)
        );

        /*
         * Remember the offset attnum, both for attributes and expressions.
         * We'll pass list_attnums to clauselist_apply_dependencies, which
         * uses it to identify clauses in a bitmap. We could also pass the
         * offset, but this is more convenient.
         */
        *list_attnums.offset(i as isize) = attnum;

        clauses_attnums = bms_add_member(clauses_attnums, attnum as c_int);
        i += 1;
    }

    /*
     * If there's not at least two distinct attnums and expressions, then
     * reject the whole list of clauses. We must return 1.0 so the calling
     * function's selectivity is unaffected.
     */
    if bms_membership(clauses_attnums) != BMS_Membership::BMS_MULTIPLE {
        bms_free(clauses_attnums);
        pfree(list_attnums as *mut c_void);
        return 1.0;
    }

    /*
     * Load all functional dependencies matching at least two parameters. We
     * can simply consider all dependencies at once, without having to search
     * for the best statistics object.
     *
     * To not waste cycles and memory, we deserialize dependencies only for
     * statistics that match at least two attributes. The array is allocated
     * with the assumption that all objects match - we could grow the array to
     * make it just the right size, but it's likely wasteful anyway thanks to
     * moving the freed chunks to freelists etc.
     */
    func_dependencies = palloc(
        core::mem::size_of::<*mut MVDependencies>() * list_length((*rel).statlist) as usize,
    ) as *mut *mut MVDependencies;
    nfunc_dependencies = 0;
    total_ndeps = 0;

    foreach!(l, (*rel).statlist, {
        'stat_continue: {
            let stat: *mut StatisticExtInfo = lfirst(current_cell!(l)) as *mut StatisticExtInfo;
            let mut nmatched: c_int;
            let mut nexprs: c_int;
            let mut k: c_int;
            let deps: *mut MVDependencies;

            /* skip statistics that are not of the correct type */
            if (*stat).kind != STATS_EXT_DEPENDENCIES {
                break 'stat_continue;
            }

            /* skip statistics with mismatching stxdinherit value */
            if (*stat).inherit != (*rte).inh {
                break 'stat_continue;
            }

            /*
             * Count matching attributes - we have to undo the attnum offsets. The
             * input attribute numbers are not offset (expressions are not
             * included in stat->keys, so it's not necessary). But we need to
             * offset it before checking against clauses_attnums.
             */
            nmatched = 0;
            k = -1;
            loop {
                k = bms_next_member((*stat).keys, k);
                if k < 0 {
                    break;
                }
                let mut attnum: AttrNumber = k as AttrNumber;

                /* skip expressions */
                if !AttrNumberIsForUserDefinedAttr(attnum) {
                    continue;
                }

                /* apply the same offset as above */
                attnum += attnum_offset;

                if bms_is_member(attnum as c_int, clauses_attnums) {
                    nmatched += 1;
                }
            }

            /* count matching expressions */
            nexprs = 0;
            i = 0;
            while i < unique_exprs_cnt {
                let lc: *mut c_void;

                foreach!(lc, (*stat).exprs, {
                    let stat_expr: *mut Node = lfirst(current_cell!(lc)) as *mut Node;

                    /* try to match it */
                    if equal(
                        stat_expr as *const c_void,
                        *unique_exprs.offset(i as isize) as *const c_void,
                    ) {
                        nexprs += 1;
                    }
                });
                i += 1;
            }

            /*
             * Skip objects matching fewer than two attributes/expressions from
             * clauses.
             */
            if nmatched + nexprs < 2 {
                break 'stat_continue;
            }

            deps = statext_dependencies_load((*stat).statOid, (*rte).inh);

            /*
             * The expressions may be represented by different attnums in the
             * stats, we need to remap them to be consistent with the clauses.
             * That will make the later steps (e.g. picking the strongest item and
             * so on) much simpler and cheaper, because it won't need to care
             * about the offset at all.
             *
             * When we're at it, we can ignore dependencies that are not fully
             * matched by clauses (i.e. referencing attributes or expressions that
             * are not in the clauses).
             *
             * We have to do this for all statistics, as long as there are any
             * expressions - we need to shift the attnums in all dependencies.
             *
             * XXX Maybe we should do this always, because it also eliminates some
             * of the dependencies early. It might be cheaper than having to walk
             * the longer list in find_strongest_dependency later, especially as
             * we need to do that repeatedly?
             *
             * XXX We have to do this even when there are no expressions in
             * clauses, otherwise find_strongest_dependency may fail for stats
             * with expressions (due to lookup of negative value in bitmap). So we
             * need to at least filter out those dependencies. Maybe we could do
             * it in a cheaper way (if there are no expr clauses, we can just
             * discard all negative attnums without any lookups).
             */
            if unique_exprs_cnt > 0 || !(*stat).exprs.is_null() {
                let mut ndeps: c_int = 0;

                i = 0;
                while i < (*deps).ndeps as c_int {
                    let mut skip: bool = false;
                    let dep: *mut MVDependency = *(*deps).deps.as_ptr().offset(i as isize);
                    let mut j: c_int;

                    j = 0;
                    while j < (*dep).nattributes as c_int {
                        let idx: c_int;
                        let expr: *mut Node;
                        let mut unique_attnum: AttrNumber = InvalidAttrNumber;
                        let mut attnum: AttrNumber;

                        /* undo the per-statistics offset */
                        attnum = *(*dep).attributes.as_ptr().offset(j as isize);

                        /*
                         * For regular attributes we can simply check if it
                         * matches any clause. If there's no matching clause, we
                         * can just ignore it. We need to offset the attnum
                         * though.
                         */
                        if AttrNumberIsForUserDefinedAttr(attnum) {
                            *(*dep).attributes.as_mut_ptr().offset(j as isize) =
                                attnum + attnum_offset;

                            if !bms_is_member(
                                *(*dep).attributes.as_ptr().offset(j as isize) as c_int,
                                clauses_attnums,
                            ) {
                                skip = true;
                                break;
                            }

                            j += 1;
                            continue;
                        }

                        /*
                         * the attnum should be a valid system attnum (-1, -2,
                         * ...)
                         */
                        Assert!(AttributeNumberIsValid(attnum));

                        /*
                         * For expressions, we need to do two translations. First
                         * we have to translate the negative attnum to index in
                         * the list of expressions (in the statistics object).
                         * Then we need to see if there's a matching clause. The
                         * index of the unique expression determines the attnum
                         * (and we offset it).
                         */
                        idx = -(1 + attnum) as c_int;

                        /* Is the expression index is valid? */
                        Assert!((idx >= 0) && (idx < list_length((*stat).exprs)));

                        expr = list_nth((*stat).exprs, idx) as *mut Node;

                        /* try to find the expression in the unique list */
                        let mut m: c_int = 0;
                        while m < unique_exprs_cnt {
                            /*
                             * found a matching unique expression, use the attnum
                             * (derived from index of the unique expression)
                             */
                            if equal(
                                *unique_exprs.offset(m as isize) as *const c_void,
                                expr as *const c_void,
                            ) {
                                unique_attnum = -(m + 1) as AttrNumber + attnum_offset;
                                break;
                            }
                            m += 1;
                        }

                        /*
                         * Found no matching expression, so we can simply skip
                         * this dependency, because there's no chance it will be
                         * fully covered.
                         */
                        if unique_attnum == InvalidAttrNumber {
                            skip = true;
                            break;
                        }

                        /* otherwise remap it to the new attnum */
                        *(*dep).attributes.as_mut_ptr().offset(j as isize) = unique_attnum;
                        j += 1;
                    }

                    /* if found a matching dependency, keep it */
                    if !skip {
                        /* maybe we've skipped something earlier, so move it */
                        if ndeps != i {
                            *(*deps).deps.as_mut_ptr().offset(ndeps as isize) =
                                *(*deps).deps.as_ptr().offset(i as isize);
                        }

                        ndeps += 1;
                    }
                    i += 1;
                }

                (*deps).ndeps = ndeps as uint32;
            }

            /*
             * It's possible we've removed all dependencies, in which case we
             * don't bother adding it to the list.
             */
            if (*deps).ndeps > 0 {
                *func_dependencies.offset(nfunc_dependencies as isize) = deps;
                total_ndeps += (*deps).ndeps as c_int;
                nfunc_dependencies += 1;
            }
        }
    });

    /* if no matching stats could be found then we've nothing to do */
    if nfunc_dependencies == 0 {
        pfree(func_dependencies as *mut c_void);
        bms_free(clauses_attnums);
        pfree(list_attnums as *mut c_void);
        pfree(unique_exprs as *mut c_void);
        return 1.0;
    }

    /*
     * Work out which dependencies we can apply, starting with the
     * widest/strongest ones, and proceeding to smaller/weaker ones.
     */
    dependencies =
        palloc(core::mem::size_of::<*mut MVDependency>() * total_ndeps as usize) as *mut *mut MVDependency;
    ndependencies = 0;

    loop {
        let dependency: *mut MVDependency;
        let attnum: AttrNumber;

        /* the widest/strongest dependency, fully matched by clauses */
        dependency =
            find_strongest_dependency(func_dependencies, nfunc_dependencies, clauses_attnums);
        if dependency.is_null() {
            break;
        }

        *dependencies.offset(ndependencies as isize) = dependency;
        ndependencies += 1;

        /* Ignore dependencies using this implied attribute in later loops */
        attnum = *(*dependency)
            .attributes
            .as_ptr()
            .offset(((*dependency).nattributes - 1) as isize);
        clauses_attnums = bms_del_member(clauses_attnums, attnum as c_int);
    }

    /*
     * If we found applicable dependencies, use them to estimate all
     * compatible clauses on attributes that they refer to.
     */
    if ndependencies != 0 {
        s1 = clauselist_apply_dependencies(
            root,
            clauses,
            varRelid,
            jointype,
            sjinfo,
            dependencies,
            ndependencies,
            list_attnums,
            estimatedclauses,
        );
    }

    /* free deserialized functional dependencies (and then the array) */
    i = 0;
    while i < nfunc_dependencies {
        pfree(*func_dependencies.offset(i as isize) as *mut c_void);
        i += 1;
    }

    pfree(dependencies as *mut c_void);
    pfree(func_dependencies as *mut c_void);
    bms_free(clauses_attnums);
    pfree(list_attnums as *mut c_void);
    pfree(unique_exprs as *mut c_void);

    s1
}

/* ---- memcpy via libc ---- */
extern "C" {
    #[link_name = "memcpy"]
    fn libc_memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

/* ===================== local stubs for unported deps ===================== */

pub type Index = c_uint;

// pg_statistic_ext / pg_statistic_ext_data
const STATS_EXT_DEPENDENCIES: c_char = b'f' as c_char; // catalog/pg_statistic_ext.h
const Anum_pg_statistic_ext_data_stxddependencies: c_int = 5; // catalog/pg_statistic_ext_data.h
const STATEXTDATASTXOID: c_int = 62; // utils/syscache.h

const TYPECACHE_LT_OPR: c_int = 0x0001; // utils/typcache.h

const F_EQSEL: Oid = 101; // utils/fmgroids.h

#[repr(C)]
pub struct TypeCacheEntry {
    pub lt_opr: Oid,
}

// Shadow layout exposing only the VacAttrStats fields this file touches.
// statistics.h types VacAttrStats as c_void, so we cast to this for field access.
// TODO(pg-port): commands/vacuum.h defines the full VacAttrStats.
#[repr(C)]
pub struct VacAttrStats {
    pub attrtypid: Oid,
    pub attrcollid: Oid,
}

#[repr(C)]
pub struct StringInfoData {
    pub data: *mut c_char,
    pub len: c_int,
    pub maxlen: c_int,
    pub cursor: c_int,
}

pub type bytea = c_void;
pub type FunctionCallInfo = *mut c_void;

const VARHDRSZ: c_int = 4; // c.h

unsafe fn lookup_type_cache(_type_id: Oid, _flags: c_int) -> *mut TypeCacheEntry {
    unimplemented!() // TODO(pg-port): src/backend/utils/cache/typcache.c
}

unsafe fn is_opclause(_clause: *const c_void) -> bool {
    unimplemented!() // TODO(pg-port): src/include/nodes/nodeFuncs.h
}

unsafe fn is_orclause(_clause: *const c_void) -> bool {
    unimplemented!() // TODO(pg-port): src/include/optimizer/clauses.h
}

unsafe fn is_notclause(_clause: *const c_void) -> bool {
    unimplemented!() // TODO(pg-port): src/include/optimizer/clauses.h
}

unsafe fn get_notclausearg(_clause: *mut Node) -> *mut crate::nodes::primnodes::Expr {
    unimplemented!() // TODO(pg-port): src/include/optimizer/clauses.h
}

unsafe fn is_pseudo_constant_clause(_clause: *mut Node) -> bool {
    unimplemented!() // TODO(pg-port): src/backend/optimizer/util/clauses.c
}

unsafe fn get_oprrest(_opno: Oid) -> Oid {
    unimplemented!() // TODO(pg-port): src/backend/utils/cache/lsyscache.c
}

unsafe fn clauselist_selectivity_ext(
    _root: *mut PlannerInfo,
    _clauses: *mut List,
    _varRelid: c_int,
    _jointype: JoinType,
    _sjinfo: *mut SpecialJoinInfo,
    _use_extended_stats: bool,
) -> Selectivity {
    unimplemented!() // TODO(pg-port): src/backend/optimizer/path/clausesel.c
}

unsafe fn equal(_a: *const c_void, _b: *const c_void) -> bool {
    unimplemented!() // TODO(pg-port): src/backend/nodes/equalfuncs.c
}

// TODO(pg-port): src/include/postgres.h CLAMP_PROBABILITY macro.
#[inline]
unsafe fn CLAMP_PROBABILITY(p: &mut Selectivity) {
    if *p < 0.0 {
        *p = 0.0;
    } else if *p > 1.0 {
        *p = 1.0;
    }
}

unsafe fn has_stats_of_kind(_stats: *mut List, _requiredkind: c_char) -> bool {
    unimplemented!() // TODO(pg-port): src/backend/statistics/extended_stats.c
}

unsafe fn planner_rt_fetch(_rti: Index, _root: *mut PlannerInfo) -> *mut RangeTblEntry {
    unimplemented!() // TODO(pg-port): src/include/nodes/pathnodes.h (rt_fetch)
}

unsafe fn SearchSysCache2(_cacheId: c_int, _key1: Datum, _key2: Datum) -> HeapTuple {
    unimplemented!() // TODO(pg-port): src/backend/utils/cache/syscache.c
}

unsafe fn SysCacheGetAttr(
    _cacheId: c_int,
    _tup: HeapTuple,
    _attributeNumber: c_int,
    _isNull: *mut bool,
) -> Datum {
    unimplemented!() // TODO(pg-port): src/backend/utils/cache/syscache.c
}

unsafe fn ReleaseSysCache(_tuple: HeapTuple) {
    unimplemented!() // TODO(pg-port): src/backend/utils/cache/syscache.c
}

#[inline]
unsafe fn HeapTupleIsValid(htup: HeapTuple) -> bool {
    !htup.is_null()
}

unsafe fn ObjectIdGetDatum(_oid: Oid) -> Datum {
    unimplemented!() // TODO(pg-port): src/include/postgres.h
}

unsafe fn BoolGetDatum(_b: bool) -> Datum {
    unimplemented!() // TODO(pg-port): src/include/postgres.h
}

unsafe fn DatumGetByteaPP(_d: Datum) -> *mut bytea {
    unimplemented!() // TODO(pg-port): src/include/fmgr.h
}

unsafe fn byteasend(_fcinfo: FunctionCallInfo) -> Datum {
    unimplemented!() // TODO(pg-port): src/backend/utils/adt/varlena.c
}

unsafe fn VARSIZE_ANY(_ptr: *mut bytea) -> u32 {
    unimplemented!() // TODO(pg-port): src/include/varatt.h
}

unsafe fn VARSIZE_ANY_EXHDR(_ptr: *mut bytea) -> u32 {
    unimplemented!() // TODO(pg-port): src/include/varatt.h
}

unsafe fn VARDATA_ANY(_ptr: *mut c_void) -> *mut c_char {
    unimplemented!() // TODO(pg-port): src/include/varatt.h
}

unsafe fn VARDATA(_ptr: *mut c_void) -> *mut c_char {
    unimplemented!() // TODO(pg-port): src/include/varatt.h
}

unsafe fn SET_VARSIZE(_ptr: *mut bytea, _len: c_int) {
    unimplemented!() // TODO(pg-port): src/include/varatt.h
}

unsafe fn initStringInfo(_str: *mut StringInfoData) {
    unimplemented!() // TODO(pg-port): src/common/stringinfo.c
}

unsafe fn appendStringInfoChar(_str: *mut StringInfoData, _ch: c_char) {
    unimplemented!() // TODO(pg-port): src/common/stringinfo.c
}

unsafe fn appendStringInfoString(_str: *mut StringInfoData, _s: *const c_char) {
    unimplemented!() // TODO(pg-port): src/common/stringinfo.c
}

extern "C" {
    fn appendStringInfo(str: *mut StringInfoData, fmt: *const c_char, ...);
}

// PG_RETURN_VOID / PG_RETURN_CSTRING / PG_GETARG_BYTEA_PP - fmgr.h
unsafe fn PG_RETURN_VOID() -> Datum {
    0
}

unsafe fn PG_RETURN_CSTRING(_c: *mut c_char) -> Datum {
    unimplemented!() // TODO(pg-port): src/include/fmgr.h
}

unsafe fn PG_GETARG_BYTEA_PP(_fcinfo: FunctionCallInfo, _n: c_int) -> *mut bytea {
    unimplemented!() // TODO(pg-port): src/include/fmgr.h
}
