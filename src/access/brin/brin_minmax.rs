//! Translation of postgres/src/backend/access/brin/brin_minmax.c
//!
//!     Implementation of Min/Max opclass for BRIN
//!
//! The "minmax" opclass summarizes a heap page range by the smallest and
//! largest indexed value seen in that range.  Each BRIN index tuple therefore
//! stores exactly two Datums per column: [min, max].  Four mandatory BRIN
//! support procedures are implemented here:
//!
//!   brin_minmax_opcinfo    (BRIN_PROCNUM_OPCINFO)    describe the stored shape
//!   brin_minmax_add_value  (BRIN_PROCNUM_ADDVALUE)   fold a new value in
//!   brin_minmax_consistent (BRIN_PROCNUM_CONSISTENT) test a scan key
//!   brin_minmax_union      (BRIN_PROCNUM_UNION)      merge two summaries
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! PORTING NOTES
//! -------------
//! The core add_value / consistent / union logic is translated 1:1 and is
//! REAL: given a valid comparison FmgrInfo it tracks the running [min,max] and
//! answers scan keys exactly like upstream.
//!
//! The catalog-dependent plumbing is NOT yet ported and is stubbed:
//!   * `minmax_get_strategy_procinfo`'s cache-FILL path (the SearchSysCache4 on
//!     pg_amop + get_opcode + fmgr_info_cxt) is `unimplemented!()`.  The cache
//!     LOOKUP path -- reading an already-populated FmgrInfo out of the per-column
//!     `MinmaxOpaque.strategy_procinfos[]` array stored in `oi_opaque` -- is real,
//!     so callers/tests that pre-populate the opaque can exercise the logic.
//!   * `lookup_type_cache` (the TypeCacheEntry build in opcinfo) is stubbed.
//!   * The `bd_tupdesc`/`Form_pg_attribute` attribute lookup (attbyval/attlen/
//!     atttypid) is funneled through `minmax_get_attr`, which is a stub returning
//!     a process-provided `MinmaxAttr` -- the real `TupleDescAttr(bd_tupdesc, ..)`
//!     decode is unported.
//!
//! The locally-defined #[repr(C)] structs (BrinValues, BrinOpcInfo, BrinDesc)
//! mirror only the subset of fields these four functions touch; see
//! access/brin_tuple.h and access/brin_internal.h for the full definitions.

use crate::prelude::*; // Datum, c_int, uint16, Oid, bool, palloc/palloc0, null_mut, DatumGet*/*GetDatum
use crate::utils::fmgr::{FmgrInfo, FunctionCall2Coll, FunctionCallInfo};
use crate::utils::adt::datum::datumCopy;
use crate::{
    PG_GETARG_DATUM, PG_GETARG_OID, PG_GETARG_POINTER, PG_GET_COLLATION, PG_NARGS, PG_RETURN_BOOL,
    PG_RETURN_DATUM, PG_RETURN_POINTER, PG_RETURN_VOID,
};

/* AttrNumber is `int16` (primnodes.h / c.h); mirror it locally for clarity. */
pub type AttrNumber = int16;

/*
 * B-tree strategy numbers, from access/stratnum.h.  brin_minmax only ever uses
 * the five B-tree strategies, so we pull them in by name (they are also
 * available from crate::access::stratnum).
 */
pub const BTLessStrategyNumber: uint16 = 1;
pub const BTLessEqualStrategyNumber: uint16 = 2;
pub const BTEqualStrategyNumber: uint16 = 3;
pub const BTGreaterEqualStrategyNumber: uint16 = 4;
pub const BTGreaterStrategyNumber: uint16 = 5;
pub const BTMaxStrategyNumber: uint16 = 5;

/*
 * ScanKey strategy fields touched by brin_minmax_consistent.  This is the
 * subset of access/skey.h's ScanKeyData that the consistent function reads.
 * (The crate's full ScanKeyData lives in access/common/scankey.rs; we mirror
 * only the read fields here to keep the port self-contained.)
 */
#[repr(C)]
pub struct MinmaxScanKey {
    pub sk_attno: AttrNumber,
    pub sk_strategy: uint16,
    pub sk_subtype: Oid,
    pub sk_argument: Datum,
}

/*
 * BrinValues -- one per indexed column in an in-memory BRIN tuple.
 * From access/brin_tuple.h; the serialize callback is opaque here.
 */
#[repr(C)]
pub struct BrinValues {
    pub bv_attno: AttrNumber,    /* index attribute number */
    pub bv_hasnulls: bool,       /* are there any nulls in the page range? */
    pub bv_allnulls: bool,       /* are all values nulls in the page range? */
    pub bv_values: *mut Datum,   /* current accumulated values ([min,max]) */
    pub bv_mem_value: Datum,     /* expanded accumulated values */
    pub bv_context: *mut c_void, /* MemoryContext */
    pub bv_serialize: *mut c_void, /* brin_serialize_callback_type */
}

/*
 * Number of stored Datums the opclass type-cache flexible array is sized for in
 * this port.  Real BRIN sizes oi_typcache[] to oi_nstored; minmax uses 2.
 */
pub const MINMAX_NSTORED: usize = 2;

/*
 * BrinOpcInfo -- result of the OpcInfo amproc; describes the on-disk shape of
 * one index column.  From access/brin_internal.h.  oi_typcache is a flexible
 * array of TypeCacheEntry* in C; we fix it at MINMAX_NSTORED entries and keep
 * the entries opaque (TypeCacheEntry is unported).
 */
#[repr(C)]
pub struct BrinOpcInfo {
    pub oi_nstored: uint16,                          /* # Datums stored per column */
    pub oi_regular_nulls: bool,                      /* regular NULL handling? */
    pub oi_opaque: *mut c_void,                      /* opclass private (MinmaxOpaque) */
    pub oi_typcache: [*mut c_void; MINMAX_NSTORED],  /* TypeCacheEntry* per column */
}

/*
 * BrinDesc -- decodes BRIN tuples to/from disk.  From access/brin_internal.h.
 * bd_info is the per-column BrinOpcInfo array (natts long); the remaining
 * pointer fields (context/index/tupdescs) are opaque in this port.
 */
#[repr(C)]
pub struct BrinDesc {
    pub bd_context: *mut c_void,        /* MemoryContext */
    pub bd_index: *mut c_void,          /* Relation */
    pub bd_tupdesc: *mut c_void,        /* TupleDesc */
    pub bd_disktdesc: *mut c_void,      /* TupleDesc */
    pub bd_totalstored: c_int,          /* total stored Datums across columns */
    pub bd_info: *mut *mut BrinOpcInfo, /* per-column info; bd_tupdesc->natts long */
}

/*
 * Per-column opclass private area, stashed in BrinOpcInfo.oi_opaque.
 * From the C MinmaxOpaque: a per-subtype cache of comparison procedures.
 */
#[repr(C)]
pub struct MinmaxOpaque {
    pub cached_subtype: Oid,
    pub strategy_procinfos: [FmgrInfo; BTMaxStrategyNumber as usize],
}

/*
 * Stub for the attribute info brin_minmax needs out of bd_tupdesc.  In C this
 * is `TupleDescAttr(bdesc->bd_tupdesc, attno - 1)` yielding a Form_pg_attribute
 * from which attbyval/attlen/atttypid are read.  The tupdesc decode is unported;
 * `minmax_get_attr` returns one of these instead.
 */
#[repr(C)]
pub struct MinmaxAttr {
    pub atttypid: Oid,
    pub attlen: c_int,
    pub attbyval: bool,
}

pub const InvalidOid: Oid = 0;

/* --------------------------------------------------------------------------
 * brin_minmax_opcinfo  (BRIN_PROCNUM_OPCINFO)
 *
 * palloc0 a BrinOpcInfo describing a two-Datum [min,max] column and wire up its
 * opaque area.  In C the allocation packs MinmaxOpaque immediately after the
 * BrinOpcInfo (a single palloc); here we allocate the opaque separately to keep
 * the layout safe in Rust, then point oi_opaque at it.
 * -------------------------------------------------------------------------- */
pub unsafe fn brin_minmax_opcinfo(fcinfo: FunctionCallInfo) -> Datum {
    let typoid: Oid = PG_GETARG_OID!(fcinfo, 0);

    /*
     * opaque->strategy_procinfos is initialized lazily; palloc0 leaves every
     * fn_oid == InvalidOid so minmax_get_strategy_procinfo treats them as empty.
     */
    let result = palloc0(core::mem::size_of::<BrinOpcInfo>()) as *mut BrinOpcInfo;

    (*result).oi_nstored = 2;
    (*result).oi_regular_nulls = true;

    /* MinmaxOpaque (packed after the struct in C); allocate it zeroed. */
    let opaque = palloc0(core::mem::size_of::<MinmaxOpaque>()) as *mut MinmaxOpaque;
    (*result).oi_opaque = opaque as *mut c_void;

    /*
     * oi_typcache[0] = oi_typcache[1] = lookup_type_cache(typoid, 0).
     * STUB: lookup_type_cache / TypeCacheEntry are unported; leave the slots
     * null.  Recorded so callers know the type cache is not populated yet.
     */
    let _ = typoid; // TODO(pg-port): result->oi_typcache[0..2] = lookup_type_cache(typoid, 0)
    (*result).oi_typcache[0] = null_mut();
    (*result).oi_typcache[1] = null_mut();

    PG_RETURN_POINTER!(result);
}

/* --------------------------------------------------------------------------
 * brin_minmax_add_value  (BRIN_PROCNUM_ADDVALUE)
 *
 * Fold a not-null heap value into the page-range summary.  If the range was
 * all-null, the new value becomes both min and max.  Otherwise compare against
 * the stored min (via the "<" proc) and stored max (via the ">" proc) and
 * replace whichever bound it falls outside of.  Returns true iff the summary
 * was updated.
 * -------------------------------------------------------------------------- */
pub unsafe fn brin_minmax_add_value(fcinfo: FunctionCallInfo) -> Datum {
    let bdesc = PG_GETARG_POINTER!(fcinfo, 0) as *mut BrinDesc;
    let column = PG_GETARG_POINTER!(fcinfo, 1) as *mut BrinValues;
    let newval: Datum = PG_GETARG_DATUM!(fcinfo, 2);
    /* isnull (arg 3) is asserted-not-null only; not used here. */
    let colloid: Oid = PG_GET_COLLATION!(fcinfo);
    let mut updated = false;

    let attno: AttrNumber = (*column).bv_attno;
    let attr = minmax_get_attr(bdesc, attno);

    /*
     * If the recorded value is null, store the new value (which we know to be
     * not null) as both minimum and maximum, and we're done.
     */
    if (*column).bv_allnulls {
        *(*column).bv_values.add(0) = datumCopy(newval, (*attr).attbyval, (*attr).attlen);
        *(*column).bv_values.add(1) = datumCopy(newval, (*attr).attbyval, (*attr).attlen);
        (*column).bv_allnulls = false;
        PG_RETURN_BOOL!(true);
    }

    /*
     * Otherwise compare the new value with the existing boundaries and update
     * them accordingly.  First check if it is less than the existing minimum.
     */
    let mut cmp_fn = minmax_get_strategy_procinfo(bdesc, attno as uint16, (*attr).atttypid, BTLessStrategyNumber);
    let mut compar = FunctionCall2Coll(cmp_fn, colloid, newval, *(*column).bv_values.add(0));
    if DatumGetBool(compar) {
        if !(*attr).attbyval {
            pfree(DatumGetPointer(*(*column).bv_values.add(0)) as *mut c_void);
        }
        *(*column).bv_values.add(0) = datumCopy(newval, (*attr).attbyval, (*attr).attlen);
        updated = true;
    }

    /*
     * And now compare it to the existing maximum.
     */
    cmp_fn = minmax_get_strategy_procinfo(bdesc, attno as uint16, (*attr).atttypid, BTGreaterStrategyNumber);
    compar = FunctionCall2Coll(cmp_fn, colloid, newval, *(*column).bv_values.add(1));
    if DatumGetBool(compar) {
        if !(*attr).attbyval {
            pfree(DatumGetPointer(*(*column).bv_values.add(1)) as *mut c_void);
        }
        *(*column).bv_values.add(1) = datumCopy(newval, (*attr).attbyval, (*attr).attlen);
        updated = true;
    }

    PG_RETURN_BOOL!(updated);
}

/* --------------------------------------------------------------------------
 * brin_minmax_consistent  (BRIN_PROCNUM_CONSISTENT)
 *
 * Decide whether a scan key is consistent with the [min,max] summary.  NULL
 * keys and all-NULL ranges are filtered out by the AM before we are reached.
 *   <  / <=        min OP key
 *   =              (min <= key) AND (max >= key)
 *   >= / >         max OP key
 * Each test runs the appropriate B-tree comparison proc via FunctionCall2Coll.
 * -------------------------------------------------------------------------- */
pub unsafe fn brin_minmax_consistent(fcinfo: FunctionCallInfo) -> Datum {
    let bdesc = PG_GETARG_POINTER!(fcinfo, 0) as *mut BrinDesc;
    let column = PG_GETARG_POINTER!(fcinfo, 1) as *mut BrinValues;
    let key = PG_GETARG_POINTER!(fcinfo, 2) as *mut MinmaxScanKey;
    let colloid: Oid = PG_GET_COLLATION!(fcinfo);

    /* This opclass uses the old signature with only three arguments. */
    Assert!(PG_NARGS!(fcinfo) == 3);
    /* Should not be dealing with all-NULL ranges. */
    Assert!(!(*column).bv_allnulls);

    let attno: AttrNumber = (*key).sk_attno;
    let subtype: Oid = (*key).sk_subtype;
    let value: Datum = (*key).sk_argument;
    let matches: Datum;

    match (*key).sk_strategy {
        BTLessStrategyNumber | BTLessEqualStrategyNumber => {
            let finfo = minmax_get_strategy_procinfo(bdesc, attno as uint16, subtype, (*key).sk_strategy);
            matches = FunctionCall2Coll(finfo, colloid, *(*column).bv_values.add(0), value);
        }
        BTEqualStrategyNumber => {
            /*
             * In the equality case (WHERE col = someval) we want the page range
             * if min <= scankey AND max >= scankey.
             */
            let finfo = minmax_get_strategy_procinfo(bdesc, attno as uint16, subtype, BTLessEqualStrategyNumber);
            let lo = FunctionCall2Coll(finfo, colloid, *(*column).bv_values.add(0), value);
            if !DatumGetBool(lo) {
                matches = lo;
            } else {
                /* max() >= scankey */
                let finfo = minmax_get_strategy_procinfo(bdesc, attno as uint16, subtype, BTGreaterEqualStrategyNumber);
                matches = FunctionCall2Coll(finfo, colloid, *(*column).bv_values.add(1), value);
            }
        }
        BTGreaterEqualStrategyNumber | BTGreaterStrategyNumber => {
            let finfo = minmax_get_strategy_procinfo(bdesc, attno as uint16, subtype, (*key).sk_strategy);
            matches = FunctionCall2Coll(finfo, colloid, *(*column).bv_values.add(1), value);
        }
        _ => {
            /* shouldn't happen */
            ereport!(ERROR, errmsg!("invalid strategy number {}", (*key).sk_strategy));
            unreachable!();
        }
    }

    PG_RETURN_DATUM!(matches);
}

/* --------------------------------------------------------------------------
 * brin_minmax_union  (BRIN_PROCNUM_UNION)
 *
 * Update col_a in place to be the union of col_a and col_b: lower its min to
 * B's min if B's is smaller, raise its max to B's max if B's is larger.  col_b
 * is untouched.  Both ranges are guaranteed non-all-null by the AM.
 * -------------------------------------------------------------------------- */
pub unsafe fn brin_minmax_union(fcinfo: FunctionCallInfo) -> Datum {
    let bdesc = PG_GETARG_POINTER!(fcinfo, 0) as *mut BrinDesc;
    let col_a = PG_GETARG_POINTER!(fcinfo, 1) as *mut BrinValues;
    let col_b = PG_GETARG_POINTER!(fcinfo, 2) as *mut BrinValues;
    let colloid: Oid = PG_GET_COLLATION!(fcinfo);

    Assert!((*col_a).bv_attno == (*col_b).bv_attno);
    Assert!(!(*col_a).bv_allnulls && !(*col_b).bv_allnulls);

    let attno: AttrNumber = (*col_a).bv_attno;
    let attr = minmax_get_attr(bdesc, attno);

    /* Adjust minimum, if B's min is less than A's min */
    let mut finfo = minmax_get_strategy_procinfo(bdesc, attno as uint16, (*attr).atttypid, BTLessStrategyNumber);
    let mut needsadj = FunctionCall2Coll(finfo, colloid, *(*col_b).bv_values.add(0), *(*col_a).bv_values.add(0));
    if DatumGetBool(needsadj) {
        if !(*attr).attbyval {
            pfree(DatumGetPointer(*(*col_a).bv_values.add(0)) as *mut c_void);
        }
        *(*col_a).bv_values.add(0) =
            datumCopy(*(*col_b).bv_values.add(0), (*attr).attbyval, (*attr).attlen);
    }

    /* Adjust maximum, if B's max is greater than A's max */
    finfo = minmax_get_strategy_procinfo(bdesc, attno as uint16, (*attr).atttypid, BTGreaterStrategyNumber);
    needsadj = FunctionCall2Coll(finfo, colloid, *(*col_b).bv_values.add(1), *(*col_a).bv_values.add(1));
    if DatumGetBool(needsadj) {
        if !(*attr).attbyval {
            pfree(DatumGetPointer(*(*col_a).bv_values.add(1)) as *mut c_void);
        }
        *(*col_a).bv_values.add(1) =
            datumCopy(*(*col_b).bv_values.add(1), (*attr).attbyval, (*attr).attlen);
    }

    PG_RETURN_VOID!();
}

/* --------------------------------------------------------------------------
 * minmax_get_attr -- STUB for TupleDescAttr(bdesc->bd_tupdesc, attno - 1).
 *
 * brin_minmax reads attbyval/attlen/atttypid off the indexed column.  The
 * tupdesc decode is unported; here the caller is expected to have stashed a
 * `MinmaxAttr` pointer in bd_disktdesc for the (single) column under test.
 * Real port: walk bdesc->bd_tupdesc with TupleDescAttr.
 * -------------------------------------------------------------------------- */
unsafe fn minmax_get_attr(bdesc: *mut BrinDesc, _attno: AttrNumber) -> *mut MinmaxAttr {
    // TODO(pg-port): return TupleDescAttr(bdesc->bd_tupdesc, attno - 1).
    let p = (*bdesc).bd_disktdesc as *mut MinmaxAttr;
    Assert!(!p.is_null());
    p
}

/* --------------------------------------------------------------------------
 * minmax_get_strategy_procinfo
 *
 * Cache and return the comparison procedure for (subtype, strategynum), out of
 * the per-column MinmaxOpaque stored in bdesc->bd_info[attno-1]->oi_opaque.
 *
 * The cache LOOKUP (reading an already-populated FmgrInfo) is real.  The cache
 * FILL path -- the SearchSysCache4(AMOPSTRATEGY, ...) + get_opcode +
 * fmgr_info_cxt that upstream uses to populate a slot on miss -- depends on the
 * unported syscache/opclass machinery and is `unimplemented!()`.  Tests and
 * callers must therefore pre-populate strategy_procinfos[strategynum-1] (and
 * set cached_subtype == subtype) for the strategies they exercise.
 * -------------------------------------------------------------------------- */
unsafe fn minmax_get_strategy_procinfo(
    bdesc: *mut BrinDesc,
    attno: uint16,
    subtype: Oid,
    strategynum: uint16,
) -> *mut FmgrInfo {
    Assert!(strategynum >= 1 && strategynum <= BTMaxStrategyNumber);

    let opaque = (*(*(*bdesc).bd_info.add((attno - 1) as usize))).oi_opaque as *mut MinmaxOpaque;

    /*
     * We cache the procedures for the previous subtype in the opaque struct, to
     * avoid repetitive syscache lookups.  If the subtype changed, invalidate
     * all the cached entries.
     */
    if (*opaque).cached_subtype != subtype {
        let mut i: uint16 = 1;
        while i <= BTMaxStrategyNumber {
            (*opaque).strategy_procinfos[(i - 1) as usize].fn_oid = InvalidOid;
            i += 1;
        }
        (*opaque).cached_subtype = subtype;
    }

    if (*opaque).strategy_procinfos[(strategynum - 1) as usize].fn_oid == InvalidOid {
        /*
         * Cache miss.  Upstream looks up the operator in the opclass'
         * opfamily via SearchSysCache4(AMOPSTRATEGY, opfamily, atttypid,
         * subtype, strategynum), takes its opcode, and fmgr_info_cxt's it into
         * the slot.  The syscache / pg_amop / get_opcode / fmgr_info_cxt path
         * is unported.
         */
        // TODO(pg-port): SearchSysCache4(AMOPSTRATEGY, ...) -> get_opcode ->
        //                fmgr_info_cxt(opcode, &slot, bdesc->bd_context).
        unimplemented!("minmax_get_strategy_procinfo cache fill: pg_amop syscache lookup unported");
    }

    &mut (*opaque).strategy_procinfos[(strategynum - 1) as usize]
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::MaybeUninit;
    use crate::utils::fmgr::PGFunction;

    /*
     * Fake fmgr-V1 "int4 less": returns BoolGetDatum(arg0 < arg1).
     * Reads its two Datum args as i32 (like int4lt) and clears isnull.
     */
    unsafe fn fake_int4_lt(fcinfo: FunctionCallInfo) -> Datum {
        let a = DatumGetInt32(PG_GETARG_DATUM!(fcinfo, 0));
        let b = DatumGetInt32(PG_GETARG_DATUM!(fcinfo, 1));
        (*fcinfo).isnull = false;
        BoolGetDatum(a < b)
    }

    /* Fake fmgr-V1 "int4 less-equal". */
    unsafe fn fake_int4_le(fcinfo: FunctionCallInfo) -> Datum {
        let a = DatumGetInt32(PG_GETARG_DATUM!(fcinfo, 0));
        let b = DatumGetInt32(PG_GETARG_DATUM!(fcinfo, 1));
        (*fcinfo).isnull = false;
        BoolGetDatum(a <= b)
    }

    /* Fake fmgr-V1 "int4 greater-equal". */
    unsafe fn fake_int4_ge(fcinfo: FunctionCallInfo) -> Datum {
        let a = DatumGetInt32(PG_GETARG_DATUM!(fcinfo, 0));
        let b = DatumGetInt32(PG_GETARG_DATUM!(fcinfo, 1));
        (*fcinfo).isnull = false;
        BoolGetDatum(a >= b)
    }

    /* Fake fmgr-V1 "int4 greater". */
    unsafe fn fake_int4_gt(fcinfo: FunctionCallInfo) -> Datum {
        let a = DatumGetInt32(PG_GETARG_DATUM!(fcinfo, 0));
        let b = DatumGetInt32(PG_GETARG_DATUM!(fcinfo, 1));
        (*fcinfo).isnull = false;
        BoolGetDatum(a > b)
    }

    unsafe fn fake_flinfo(proc: PGFunction) -> FmgrInfo {
        let mut fi: FmgrInfo = MaybeUninit::zeroed().assume_init();
        fi.fn_addr = Some(proc);
        fi.fn_oid = 1; /* any valid-looking, non-Invalid oid (cache treats !=Invalid as filled) */
        fi.fn_nargs = 2;
        fi
    }

    /*
     * Build a MinmaxOpaque whose five B-tree strategy slots point at the fake
     * int4 comparison procs, with cached_subtype matching so the cache LOOKUP
     * path is taken (never the unimplemented FILL path).
     */
    unsafe fn make_opaque(subtype: Oid) -> Box<MinmaxOpaque> {
        let mut op: MinmaxOpaque = MaybeUninit::zeroed().assume_init();
        op.cached_subtype = subtype;
        op.strategy_procinfos[(BTLessStrategyNumber - 1) as usize] = fake_flinfo(fake_int4_lt);
        op.strategy_procinfos[(BTLessEqualStrategyNumber - 1) as usize] = fake_flinfo(fake_int4_le);
        op.strategy_procinfos[(BTGreaterEqualStrategyNumber - 1) as usize] = fake_flinfo(fake_int4_ge);
        op.strategy_procinfos[(BTGreaterStrategyNumber - 1) as usize] = fake_flinfo(fake_int4_gt);
        Box::new(op)
    }

    /*
     * Assemble a BrinDesc + per-column BrinOpcInfo wired to `opaque`, plus a
     * MinmaxAttr for int4 (byval, len 4) stashed in bd_disktdesc so
     * minmax_get_attr can find it.
     */
    unsafe fn make_bdesc(opaque: *mut MinmaxOpaque, attr: *mut MinmaxAttr) -> (Box<BrinDesc>, Box<BrinOpcInfo>, Box<*mut BrinOpcInfo>) {
        let mut opc: BrinOpcInfo = MaybeUninit::zeroed().assume_init();
        opc.oi_nstored = 2;
        opc.oi_regular_nulls = true;
        opc.oi_opaque = opaque as *mut c_void;
        let mut opc = Box::new(opc);

        let mut info_slot: Box<*mut BrinOpcInfo> = Box::new(&mut *opc as *mut BrinOpcInfo);

        let mut bd: BrinDesc = MaybeUninit::zeroed().assume_init();
        bd.bd_info = &mut *info_slot as *mut *mut BrinOpcInfo;
        bd.bd_disktdesc = attr as *mut c_void;
        let bd = Box::new(bd);
        (bd, opc, info_slot)
    }

    /* Build a fcinfo with `nargs` slots, all zero, collation Invalid. */
    unsafe fn make_fcinfo(nargs: usize) -> Vec<u8> {
        let sz = core::mem::size_of::<crate::utils::fmgr::FunctionCallInfoBaseData>()
            + nargs * core::mem::size_of::<crate::postgres::NullableDatum>();
        let mut buf = vec![0u8; sz];
        let fc = buf.as_mut_ptr() as FunctionCallInfo;
        (*fc).fncollation = InvalidOid;
        (*fc).nargs = nargs as i16;
        buf
    }

    unsafe fn set_arg(fc: FunctionCallInfo, n: usize, v: Datum) {
        (*(*fc).args.as_mut_ptr().add(n)).value = v;
        (*(*fc).args.as_mut_ptr().add(n)).isnull = false;
    }

    #[test]
    fn add_value_tracks_running_min_and_max() {
        unsafe {
            let mut opaque = make_opaque(23) /* match attr.atttypid; else cache-fill stub */;
            let mut attr = Box::new(MinmaxAttr { atttypid: 23, attlen: 4, attbyval: true });
            let (mut bd, _opc, _slot) = make_bdesc(&mut *opaque, &mut *attr);

            /* [min,max] storage and an all-null column to start. */
            let mut vals: [Datum; 2] = [0, 0];
            let mut col: BrinValues = MaybeUninit::zeroed().assume_init();
            col.bv_attno = 1;
            col.bv_allnulls = true;
            col.bv_values = vals.as_mut_ptr();

            let inputs: [i32; 5] = [50, 20, 70, 10, 30];
            let mut expect_min = i32::MAX;
            let mut expect_max = i32::MIN;

            for &x in inputs.iter() {
                let mut buf = make_fcinfo(4);
                let fc = buf.as_mut_ptr() as FunctionCallInfo;
                set_arg(fc, 0, &mut *bd as *mut BrinDesc as Datum);
                set_arg(fc, 1, &mut col as *mut BrinValues as Datum);
                set_arg(fc, 2, Int32GetDatum(x));
                set_arg(fc, 3, BoolGetDatum(false));

                let _ = brin_minmax_add_value(fc);

                if x < expect_min { expect_min = x; }
                if x > expect_max { expect_max = x; }
                assert_eq!(DatumGetInt32(vals[0]), expect_min, "min after {}", x);
                assert_eq!(DatumGetInt32(vals[1]), expect_max, "max after {}", x);
            }
            assert!(!col.bv_allnulls);
            assert_eq!(DatumGetInt32(vals[0]), 10);
            assert_eq!(DatumGetInt32(vals[1]), 70);
        }
    }

    #[test]
    fn consistent_equal_true_iff_key_in_range() {
        unsafe {
            let mut opaque = make_opaque(InvalidOid);
            let mut attr = Box::new(MinmaxAttr { atttypid: 23, attlen: 4, attbyval: true });
            let (mut bd, _opc, _slot) = make_bdesc(&mut *opaque, &mut *attr);

            /* summary [min=10, max=70], not all-null. */
            let mut vals: [Datum; 2] = [Int32GetDatum(10), Int32GetDatum(70)];
            let mut col: BrinValues = MaybeUninit::zeroed().assume_init();
            col.bv_attno = 1;
            col.bv_allnulls = false;
            col.bv_values = vals.as_mut_ptr();

            /* keys: below, at-min, mid, at-max, above. */
            let cases: [(i32, bool); 5] =
                [(5, false), (10, true), (40, true), (70, true), (90, false)];

            for (k, want) in cases.iter().copied() {
                let mut key: MinmaxScanKey = MaybeUninit::zeroed().assume_init();
                key.sk_attno = 1;
                key.sk_strategy = BTEqualStrategyNumber;
                key.sk_subtype = InvalidOid;
                key.sk_argument = Int32GetDatum(k);

                let mut buf = make_fcinfo(3);
                let fc = buf.as_mut_ptr() as FunctionCallInfo;
                set_arg(fc, 0, &mut *bd as *mut BrinDesc as Datum);
                set_arg(fc, 1, &mut col as *mut BrinValues as Datum);
                set_arg(fc, 2, &mut key as *mut MinmaxScanKey as Datum);

                let got = DatumGetBool(brin_minmax_consistent(fc));
                assert_eq!(got, want, "BTEqual key={}", k);
            }
        }
    }
}
