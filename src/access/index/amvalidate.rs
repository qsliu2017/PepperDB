//! Source: postgres/src/backend/access/index/amvalidate.c
//!
//! MERGED from postgres/src/include/access/amvalidate.h:
//!   - the `OpFamilyOpFuncGroup` struct (returned, in a List, by
//!     identify_opfamily_groups)
//!   - extern decls for identify_opfamily_groups / check_amproc_signature /
//!     check_amoptsproc_signature / check_amop_signature /
//!     opclass_for_family_datatype / opfamily_can_sort_type
//!
//! Support routines for index access methods' amvalidate and amadjustmembers
//! functions.  This is the SHARED opclass-validation helper used by every
//! access method's amvalidate (ginvalidate / spgvalidate / hashvalidate /
//! nbtvalidate / brin_validate).
//!
//! #include mapping:
//!   "postgres.h"                  -> crate::prelude::*
//!   "access/amvalidate.h"         -> this file
//!   "access/htup_details.h"       -> crate::access::htup_details (GETSTRUCT, HeapTupleData)
//!   "catalog/pg_am.h"             -> BTREE_AM_OID (crate::catalog::pg_known_oids)
//!   "catalog/pg_amop.h"           -> crate::catalog::pg_amop (Form_pg_amop)
//!   "catalog/pg_amproc.h"         -> crate::catalog::pg_amproc (Form_pg_amproc)
//!   "catalog/pg_opclass.h"        -> crate::catalog::pg_opclass (Form_pg_opclass)
//!   "catalog/pg_operator.h"       -> crate::catalog::pg_operator (Form_pg_operator)
//!   "catalog/pg_proc.h"           -> crate::catalog::pg_proc (Form_pg_proc)
//!   "catalog/pg_type.h"           -> VOIDOID / INTERNALOID (crate::catalog::pg_type_d)
//!   "parser/parse_coerce.h"       -> IsBinaryCoercible (STUB: parse_coerce.c not ported)
//!   "utils/syscache.h"            -> SearchSysCache1 / SearchSysCacheList1 /
//!                                    ReleaseSysCache / ReleaseCatCacheList (STUB:
//!                                    syscache/catcache not ported)
//!
//! TRANSLATION NOTES (deviations from the 1:1 C source -- read carefully):
//!
//! * `identify_opfamily_groups` is ported 1:1.  It does NOT sort: this PG 18.3
//!   version assumes its two input CatCLists are ALREADY ordered by datatype
//!   (they come straight out of the AMOPSTRATEGY / AMPROCNUM caches, which key
//!   on lefttype/righttype).  It advances the two lists concurrently and detects
//!   group boundaries.  (An older PostgreSQL variant sorted an OpFamilyMember
//!   list with pg_qsort; that code is not in this file, so it is not ported
//!   here.  `OpFamilyMember` itself already lives in access/index/amapi.rs.)
//!   The `CatCList` / `CatCTup` types are NOT yet ported (utils/catcache.h), so
//!   we mirror the two structs MINIMALLY here -- only the fields this function
//!   reads: `ordered`, `n_members`, `members[]`, and each member's `tuple`.
//!
//! * The check_* helpers fetch a catalog row via SearchSysCache1 in C.  The
//!   syscache is not ported, so the syscache fetch is a STUB (`unimplemented!()`
//!   via search_*_syscache).  The pure COMPARISON logic -- the part that is
//!   actually interesting and bug-prone -- is factored into `check_proc_sig`
//!   and `check_op_sig`, which operate on plain mirrored signature structs
//!   (`ProcSig` / `OpSig`) and are fully REAL and unit-tested.  The public
//!   check_amproc_signature / check_amop_signature / check_amoptsproc_signature
//!   wrappers reproduce the C control flow and call the syscache stub.
//!
//! * C variadic `...` (maxargs Oid arguments) becomes a `&[Oid]` slice.
//!
//! * `IsBinaryCoercible(srctype, targettype)` is unported (parse_coerce.c); we
//!   provide a local STUB that returns false (a strict, conservative answer:
//!   only exact matches pass when `exact` is false).  Replace with the real
//!   coercion check once parse_coerce.c is ported.

use crate::prelude::*;

use crate::access::htup_details::{HeapTupleData, GETSTRUCT};
use crate::catalog::pg_amop::Form_pg_amop;
use crate::catalog::pg_amproc::Form_pg_amproc;
use crate::catalog::pg_known_oids::BTREE_AM_OID;
use crate::catalog::pg_opclass::Form_pg_opclass;
use crate::catalog::pg_operator::Form_pg_operator;
use crate::catalog::pg_proc::Form_pg_proc;
use crate::catalog::pg_type_d::{INTERNALOID, VOIDOID};
use crate::nodes::pg_list::{lappend, List, NIL};
use crate::utils::palloc::palloc;

// ===========================================================================
//                       amvalidate.h: public types
// ===========================================================================

/// `struct OpFamilyOpFuncGroup` - returned (in a List) by
/// identify_opfamily_groups().
///
/// One per lefttype/righttype combination present in the family's operator and
/// support-function lists.  If amopstrategy K is present for this datatype
/// combination, bit `1 << K` is set in `operatorset`; likewise for support
/// functions in `functionset`.  With u64 fields we can handle operator and
/// function numbers up to 63, which is plenty for the foreseeable future.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpFamilyOpFuncGroup {
    /// amoplefttype / amproclefttype
    pub lefttype: Oid,
    /// amoprighttype / amprocrighttype
    pub righttype: Oid,
    /// bitmask of operators with these types
    pub operatorset: u64,
    /// bitmask of support funcs with these types
    pub functionset: u64,
}

// ===========================================================================
//   MINIMAL mirror of utils/catcache.h CatCList / CatCTup (NOT yet ported).
//   Only the fields identify_opfamily_groups() actually reads are present.
//   When utils/catcache.rs lands, replace these with the real types.
// ===========================================================================

/// Minimal mirror of `CatCTup` (utils/catcache.h): only the embedded tuple
/// management header is reproduced here.
#[repr(C)]
pub struct CatCTup {
    /// tuple management header
    pub tuple: HeapTupleData,
}

/// Minimal mirror of `CatCList` (utils/catcache.h): only `ordered`,
/// `n_members`, and the `members[]` flexible array are reproduced.
#[repr(C)]
pub struct CatCList {
    /// members listed in index order?
    pub ordered: bool,
    /// number of member tuples
    pub n_members: c_int,
    /// member CatCTup pointers (FLEXIBLE_ARRAY_MEMBER in C)
    pub members: [*mut CatCTup; FLEXIBLE_ARRAY_MEMBER],
}

impl CatCList {
    /// `&list->members[i]->tuple` - the i'th member's HeapTupleData.
    ///
    /// # Safety
    /// `i` must be < `self.n_members`, and the corresponding member pointer
    /// must reference a valid CatCTup.
    #[inline]
    pub unsafe fn member_tuple(&self, i: usize) -> *const HeapTupleData {
        let base = self.members.as_ptr();
        let memb = *base.add(i);
        &(*memb).tuple as *const HeapTupleData
    }
}

// ===========================================================================
//   STUB: parse_coerce.c IsBinaryCoercible (not yet ported).
// ===========================================================================

/// STUB for `IsBinaryCoercible(srctype, targettype)` (parser/parse_coerce.c is
/// not ported).  Conservatively returns true only for the trivial exact-match
/// case, false otherwise.  Replace with the real binary-coercibility check
/// (which consults pg_cast etc.) once parse_coerce.c is available.
fn IsBinaryCoercible(srctype: Oid, targettype: Oid) -> bool {
    // The real function also treats ANY targets, polymorphics, and pg_cast
    // binary-coercible entries as acceptable.  Until ported, only identity
    // passes -- callers that need binary coercibility will reject more than C.
    srctype == targettype
}

// ===========================================================================
//   identify_opfamily_groups (REAL, ported 1:1)
// ===========================================================================

/// `identify_opfamily_groups()` returns a List of OpFamilyOpFuncGroup structs,
/// one for each combination of lefttype/righttype present in the family's
/// operator and support function lists.
///
/// The given CatCLists are expected to represent a single opfamily fetched from
/// the AMOPSTRATEGY and AMPROCNUM caches, so they will be in order by those
/// caches' second and third cache keys, namely the datatypes.
///
/// # Safety
/// `oprlist`/`proclist` must reference valid CatCLists whose members reference
/// valid CatCTups whose tuples carry Form_pg_amop / Form_pg_amproc structs.
pub unsafe fn identify_opfamily_groups(
    oprlist: *const CatCList,
    proclist: *const CatCList,
) -> *mut List {
    let mut result: *mut List = NIL;
    let mut thisgroup: *mut OpFamilyOpFuncGroup;
    let mut oprform: Form_pg_amop;
    let mut procform: Form_pg_amproc;
    let mut io: c_int;
    let mut ip: c_int;

    let oprlist_ref = &*oprlist;
    let proclist_ref = &*proclist;

    // We need the lists to be ordered; should be true in normal operation
    if !oprlist_ref.ordered || !proclist_ref.ordered {
        elog!(ERROR, "cannot validate operator family without ordered data");
    }

    // Advance through the lists concurrently.  Thanks to the ordering, we should
    // see all operators and functions of a given datatype pair consecutively.
    thisgroup = core::ptr::null_mut();
    io = 0;
    ip = 0;
    if io < oprlist_ref.n_members {
        oprform = GETSTRUCT(oprlist_ref.member_tuple(io as usize)) as Form_pg_amop;
        io += 1;
    } else {
        oprform = core::ptr::null_mut();
    }
    if ip < proclist_ref.n_members {
        procform = GETSTRUCT(proclist_ref.member_tuple(ip as usize)) as Form_pg_amproc;
        ip += 1;
    } else {
        procform = core::ptr::null_mut();
    }

    while !oprform.is_null() || !procform.is_null() {
        if !oprform.is_null()
            && !thisgroup.is_null()
            && (*oprform).amoplefttype == (*thisgroup).lefttype
            && (*oprform).amoprighttype == (*thisgroup).righttype
        {
            // Operator belongs to current group; include it and advance

            // Ignore strategy numbers outside supported range
            if (*oprform).amopstrategy > 0 && (*oprform).amopstrategy < 64 {
                (*thisgroup).operatorset |= 1u64 << (*oprform).amopstrategy;
            }

            if io < oprlist_ref.n_members {
                oprform = GETSTRUCT(oprlist_ref.member_tuple(io as usize)) as Form_pg_amop;
                io += 1;
            } else {
                oprform = core::ptr::null_mut();
            }
            continue;
        }

        if !procform.is_null()
            && !thisgroup.is_null()
            && (*procform).amproclefttype == (*thisgroup).lefttype
            && (*procform).amprocrighttype == (*thisgroup).righttype
        {
            // Procedure belongs to current group; include it and advance

            // Ignore function numbers outside supported range
            if (*procform).amprocnum > 0 && (*procform).amprocnum < 64 {
                (*thisgroup).functionset |= 1u64 << (*procform).amprocnum;
            }

            if ip < proclist_ref.n_members {
                procform = GETSTRUCT(proclist_ref.member_tuple(ip as usize)) as Form_pg_amproc;
                ip += 1;
            } else {
                procform = core::ptr::null_mut();
            }
            continue;
        }

        // Time for a new group
        thisgroup =
            palloc(core::mem::size_of::<OpFamilyOpFuncGroup>()) as *mut OpFamilyOpFuncGroup;
        if !oprform.is_null()
            && (procform.is_null()
                || ((*oprform).amoplefttype < (*procform).amproclefttype
                    || ((*oprform).amoplefttype == (*procform).amproclefttype
                        && (*oprform).amoprighttype < (*procform).amprocrighttype)))
        {
            (*thisgroup).lefttype = (*oprform).amoplefttype;
            (*thisgroup).righttype = (*oprform).amoprighttype;
        } else {
            (*thisgroup).lefttype = (*procform).amproclefttype;
            (*thisgroup).righttype = (*procform).amprocrighttype;
        }
        (*thisgroup).operatorset = 0;
        (*thisgroup).functionset = 0;
        result = lappend(result, thisgroup as *mut c_void);
    }

    result
}

// ===========================================================================
//   Signature-check COMPARISON cores (REAL, unit-tested).
//   These operate on plain mirrored signature structs so the comparison logic
//   can be exercised without a live syscache.
// ===========================================================================

/// Plain mirror of the parts of Form_pg_proc that check_amproc_signature reads.
/// (The real Form_pg_proc truncates `proargtypes` at the CATALOG_VARLEN cutoff,
/// so we carry it explicitly here.)
#[derive(Clone, Debug)]
pub struct ProcSig {
    pub prorettype: Oid,
    pub proretset: bool,
    pub pronargs: i16,
    pub proargtypes: Vec<Oid>,
}

/// Plain mirror of the parts of Form_pg_operator that check_amop_signature
/// reads.
#[derive(Clone, Debug)]
pub struct OpSig {
    pub oprresult: Oid,
    pub oprkind: c_char,
    pub oprleft: Oid,
    pub oprright: Oid,
}

/// The pure COMPARISON core of check_amproc_signature (REAL).
///
/// `restype` must match the result type exactly; result must not be a set; the
/// argument count must lie in `[minargs, maxargs]`.  The `argtypes` slice holds
/// up to `maxargs` expected argument-type OIDs.  If `exact`, each compared arg
/// must match exactly; otherwise it must be binary-coercible (via the
/// IsBinaryCoercible stub).  Returns true iff the signature is acceptable.
pub fn check_proc_sig(
    sig: &ProcSig,
    restype: Oid,
    exact: bool,
    minargs: c_int,
    maxargs: c_int,
    argtypes: &[Oid],
) -> bool {
    let mut result = true;

    if sig.prorettype != restype
        || sig.proretset
        || (sig.pronargs as c_int) < minargs
        || (sig.pronargs as c_int) > maxargs
    {
        result = false;
    }

    for i in 0..maxargs {
        let argtype = argtypes[i as usize];
        if i >= sig.pronargs as c_int {
            continue;
        }
        let want = sig.proargtypes[i as usize];
        if if exact {
            argtype != want
        } else {
            !IsBinaryCoercible(argtype, want)
        } {
            result = false;
        }
    }

    result
}

/// The pure COMPARISON core of check_amop_signature (REAL).
///
/// We hard-wire acceptance to binary operators only, with exact type matches:
/// the lefttype/righttype come from pg_amop and should match the operator
/// exactly.
pub fn check_op_sig(sig: &OpSig, restype: Oid, lefttype: Oid, righttype: Oid) -> bool {
    let mut result = true;
    if sig.oprresult != restype
        || sig.oprkind != b'b' as c_char
        || sig.oprleft != lefttype
        || sig.oprright != righttype
    {
        result = false;
    }
    result
}

// ===========================================================================
//   STUB syscache fetches used by the public check_* wrappers.
// ===========================================================================

/// STUB for `SearchSysCache1(PROCOID, funcid)` + GETSTRUCT -> ProcSig.
/// utils/syscache.c is not ported; this panics until it is.
fn search_proc_syscache(_funcid: Oid) -> ProcSig {
    // C: tp = SearchSysCache1(PROCOID, ObjectIdGetDatum(funcid));
    //    if (!HeapTupleIsValid(tp)) elog(ERROR, "cache lookup failed ...");
    //    procform = (Form_pg_proc) GETSTRUCT(tp);
    unimplemented!("STUB: syscache PROCOID lookup (utils/syscache.c not ported)")
}

/// STUB for `SearchSysCache1(OPEROID, opno)` + GETSTRUCT -> OpSig.
fn search_oper_syscache(_opno: Oid) -> OpSig {
    unimplemented!("STUB: syscache OPEROID lookup (utils/syscache.c not ported)")
}

// ===========================================================================
//   Public check_* wrappers (control flow ported; syscache fetch STUBBED).
// ===========================================================================

/// Validate the signature (argument and result types) of an opclass support
/// function.  Return true if OK, false if not.
///
/// `argtypes` represents the C `...` of up to `maxargs` argument-type OIDs.  If
/// `exact` is true they must match the function arg types exactly, else only
/// binary-coercibly.  In any case the function result type must match `restype`
/// exactly.
///
/// STUB: the SearchSysCache1(PROCOID) fetch is `unimplemented!()`; the
/// comparison itself is delegated to the REAL `check_proc_sig`.
pub fn check_amproc_signature(
    funcid: Oid,
    restype: Oid,
    exact: bool,
    minargs: c_int,
    maxargs: c_int,
    argtypes: &[Oid],
) -> bool {
    let sig = search_proc_syscache(funcid);
    check_proc_sig(&sig, restype, exact, minargs, maxargs, argtypes)
    // C: ReleaseSysCache(tp); -- no-op once syscache lands.
}

/// Validate the signature of an opclass options support function, which must be
/// `void(internal)`.
pub fn check_amoptsproc_signature(funcid: Oid) -> bool {
    check_amproc_signature(funcid, VOIDOID, true, 1, 1, &[INTERNALOID])
}

/// Validate the signature (argument and result types) of an opclass operator.
/// Return true if OK, false if not.
///
/// We can hard-wire this as accepting only binary operators, and insist on
/// exact type matches, since the given lefttype/righttype come from pg_amop and
/// should always match the operator exactly.
///
/// STUB: the SearchSysCache1(OPEROID) fetch is `unimplemented!()`; the
/// comparison itself is delegated to the REAL `check_op_sig`.
pub fn check_amop_signature(opno: Oid, restype: Oid, lefttype: Oid, righttype: Oid) -> bool {
    let sig = search_oper_syscache(opno);
    check_op_sig(&sig, restype, lefttype, righttype)
    // C: ReleaseSysCache(tp); -- no-op once syscache lands.
}

// ===========================================================================
//   opclass lookup helpers (control flow ported; syscache list STUBBED).
// ===========================================================================

/// Minimal mirror of `CatCList` carrying pg_opclass member tuples, used by
/// opclass_for_family_datatype.  Reuses the same CatCList layout.
type OpclassCatCList = CatCList;

/// STUB for `SearchSysCacheList1(CLAAMNAMENSP, amoid)`.  catcache.c is not
/// ported; this panics until it is.
fn search_claamnamensp_list(_amoid: Oid) -> *const OpclassCatCList {
    unimplemented!("STUB: catcache CLAAMNAMENSP list (utils/catcache.c not ported)")
}

/// Get the OID of the opclass belonging to an opfamily and accepting the
/// specified type as input type.  Returns InvalidOid if no such opclass.
///
/// If there is more than one such opclass, you get a random one of them.  Since
/// that shouldn't happen, we don't waste cycles checking.
///
/// STUB: the SearchSysCacheList1(CLAAMNAMENSP) fetch is `unimplemented!()`; the
/// scan/comparison over the returned list is the real C logic.
///
/// # Safety
/// Relies on the (stubbed) syscache list returning valid CatCTups carrying
/// Form_pg_opclass tuples.
pub unsafe fn opclass_for_family_datatype(
    amoid: Oid,
    opfamilyoid: Oid,
    datatypeoid: Oid,
) -> Oid {
    let mut result: Oid = InvalidOid;

    // We search through all the AM's opclasses to see if one matches.  This is a
    // bit inefficient but there is no better index available.  It also saves
    // making an explicit check that the opfamily belongs to the AM.
    let opclist = search_claamnamensp_list(amoid);
    let opclist_ref = &*opclist;

    for i in 0..opclist_ref.n_members {
        let classtup = opclist_ref.member_tuple(i as usize);
        let classform = GETSTRUCT(classtup) as Form_pg_opclass;

        if (*classform).opcfamily == opfamilyoid && (*classform).opcintype == datatypeoid {
            result = (*classform).oid;
            break;
        }
    }

    // C: ReleaseCatCacheList(opclist); -- no-op once catcache lands.
    result
}

/// Is the datatype a legitimate input type for the btree opfamily?
///
/// # Safety
/// As `opclass_for_family_datatype`.
pub unsafe fn opfamily_can_sort_type(opfamilyoid: Oid, datatypeoid: Oid) -> bool {
    OidIsValid(opclass_for_family_datatype(
        BTREE_AM_OID,
        opfamilyoid,
        datatypeoid,
    ))
}

// ===========================================================================
//                                 TESTS
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;
    use crate::access::htup_details::HeapTupleHeaderData;
    use crate::catalog::pg_amop::FormData_pg_amop;
    use crate::catalog::pg_amproc::FormData_pg_amproc;
    use crate::nodes::pg_list::{list_length, list_nth};

    // --- helpers to fabricate a CatCList of amop/amproc tuples ----------------
    //
    // GETSTRUCT(tuple) returns t_data + t_data->t_hoff.  We build a HeapTuple
    // whose t_data points at a buffer laid out as [HeapTupleHeader bytes ...]
    // [Form payload at offset t_hoff].  We make t_hoff 0-relative by storing the
    // payload directly after a header whose t_hoff names the payload offset.

    // Boxed backing storage kept alive for the duration of a test.  `_payload`
    // owns the buffer that ctup.tuple.t_data points into.
    struct TupleBox {
        _payload: Box<[u8]>,
        ctup: Box<CatCTup>,
    }

    fn make_tuple<T: Copy>(form: T) -> TupleBox {
        // Lay out: header struct, then payload right after it, with t_hoff =
        // size_of::<HeapTupleHeaderData>() so GETSTRUCT lands on the payload.
        // We allocate the header and payload contiguously in one buffer.
        let hoff = core::mem::size_of::<HeapTupleHeaderData>();
        let total = hoff + core::mem::size_of::<T>();
        let mut buf = vec![0u8; total].into_boxed_slice();

        // Write the form payload at offset hoff.
        unsafe {
            let dst = buf.as_mut_ptr().add(hoff) as *mut T;
            core::ptr::write_unaligned(dst, form);
        }

        // The header lives at offset 0 of the same buffer; set t_hoff.
        unsafe {
            let hdr = buf.as_mut_ptr() as *mut HeapTupleHeaderData;
            (*hdr).t_hoff = hoff as u8;
        }

        // HeapTupleData.t_data must point at the buffer's start (the header).
        let mut ctup = Box::new(CatCTup {
            tuple: unsafe { core::mem::zeroed() },
        });
        ctup.tuple.t_data = buf.as_mut_ptr() as *mut HeapTupleHeaderData;

        TupleBox {
            _payload: buf,
            ctup,
        }
    }

    fn amop(lefttype: Oid, righttype: Oid, strategy: i16) -> FormData_pg_amop {
        let mut f: FormData_pg_amop = unsafe { core::mem::zeroed() };
        f.amoplefttype = lefttype;
        f.amoprighttype = righttype;
        f.amopstrategy = strategy;
        f
    }

    fn amproc(lefttype: Oid, righttype: Oid, num: i16) -> FormData_pg_amproc {
        let mut f: FormData_pg_amproc = unsafe { core::mem::zeroed() };
        f.amproclefttype = lefttype;
        f.amprocrighttype = righttype;
        f.amprocnum = num;
        f
    }

    // Build a CatCList from a set of TupleBoxes. Returns the boxed list plus the
    // owning boxes (kept alive by the caller).
    struct ListBox {
        _boxes: Vec<TupleBox>,
        // backing storage for the variable-length CatCList
        buf: Box<[u8]>,
    }

    impl ListBox {
        fn as_catclist(&self) -> *const CatCList {
            self.buf.as_ptr() as *const CatCList
        }
    }

    fn make_list(boxes: Vec<TupleBox>, ordered: bool) -> ListBox {
        let n = boxes.len();
        // size of CatCList header + n member pointers
        let base = core::mem::size_of::<CatCList>();
        let total = base + n * core::mem::size_of::<*mut CatCTup>();
        let mut buf = vec![0u8; total].into_boxed_slice();
        unsafe {
            let cl = buf.as_mut_ptr() as *mut CatCList;
            (*cl).ordered = ordered;
            (*cl).n_members = n as c_int;
            let members = (*cl).members.as_mut_ptr();
            for (i, b) in boxes.iter().enumerate() {
                let ctup_ptr = b.ctup.as_ref() as *const CatCTup as *mut CatCTup;
                *members.add(i) = ctup_ptr;
            }
        }
        ListBox { _boxes: boxes, buf }
    }

    // Collect (lefttype, righttype, operatorset, functionset) from the result.
    unsafe fn collect(result: *mut List) -> Vec<(Oid, Oid, u64, u64)> {
        let mut out = Vec::new();
        let len = list_length(result);
        for i in 0..len {
            let g = list_nth(result, i) as *const OpFamilyOpFuncGroup;
            out.push((
                (*g).lefttype,
                (*g).righttype,
                (*g).operatorset,
                (*g).functionset,
            ));
        }
        out
    }

    #[test]
    fn groups_three_members_two_groups() {
        // Two operators on (10,10) strategies 1 and 2, one operator on (10,20)
        // strategy 1, and procs: (10,10) num 1 ; (10,20) num 2.
        let oprs = vec![
            make_tuple(amop(10, 10, 1)),
            make_tuple(amop(10, 10, 2)),
            make_tuple(amop(10, 20, 1)),
        ];
        let procs = vec![
            make_tuple(amproc(10, 10, 1)),
            make_tuple(amproc(10, 20, 2)),
        ];
        let oprlist = make_list(oprs, true);
        let proclist = make_list(procs, true);

        unsafe {
            let res = identify_opfamily_groups(oprlist.as_catclist(), proclist.as_catclist());
            let groups = collect(res);

            // Two distinct (lefttype,righttype) groups: (10,10) and (10,20).
            assert_eq!(groups.len(), 2);

            // Group (10,10): operators 1 and 2 set -> bits 1,2 ; function 1.
            let g1010 = groups.iter().find(|g| g.0 == 10 && g.1 == 10).unwrap();
            assert_eq!(g1010.2, (1u64 << 1) | (1u64 << 2));
            assert_eq!(g1010.3, 1u64 << 1);

            // Group (10,20): operator 1 ; function 2.
            let g1020 = groups.iter().find(|g| g.0 == 10 && g.1 == 20).unwrap();
            assert_eq!(g1020.2, 1u64 << 1);
            assert_eq!(g1020.3, 1u64 << 2);
        }
    }

    #[test]
    fn out_of_range_strategy_and_func_ignored() {
        // strategy 0 and 64 must be ignored; strategy 63 must set bit 63.
        let oprs = vec![
            make_tuple(amop(5, 5, 0)),
            make_tuple(amop(5, 5, 63)),
            make_tuple(amop(5, 5, 64)),
        ];
        let procs = vec![make_tuple(amproc(5, 5, 0)), make_tuple(amproc(5, 5, 63))];
        let oprlist = make_list(oprs, true);
        let proclist = make_list(procs, true);

        unsafe {
            let res = identify_opfamily_groups(oprlist.as_catclist(), proclist.as_catclist());
            let groups = collect(res);
            assert_eq!(groups.len(), 1);
            assert_eq!(groups[0].0, 5);
            assert_eq!(groups[0].1, 5);
            // only bit 63 set among operators (0 and 64 ignored)
            assert_eq!(groups[0].2, 1u64 << 63);
            // only bit 63 among functions
            assert_eq!(groups[0].3, 1u64 << 63);
        }
    }

    #[test]
    fn idempotent_grouping_on_already_grouped_input() {
        // Run grouping once, then build a fresh ordered list from the same
        // (lefttype,righttype) ordering and confirm the group key list is
        // identical (idempotence of the boundary detection).
        let mk = || {
            let oprs = vec![
                make_tuple(amop(1, 1, 1)),
                make_tuple(amop(1, 2, 1)),
                make_tuple(amop(2, 2, 1)),
            ];
            let procs = vec![
                make_tuple(amproc(1, 1, 1)),
                make_tuple(amproc(1, 2, 1)),
                make_tuple(amproc(2, 2, 1)),
            ];
            (make_list(oprs, true), make_list(procs, true))
        };

        unsafe {
            let (o1, p1) = mk();
            let r1 = collect(identify_opfamily_groups(o1.as_catclist(), p1.as_catclist()));
            let (o2, p2) = mk();
            let r2 = collect(identify_opfamily_groups(o2.as_catclist(), p2.as_catclist()));

            let keys1: Vec<(Oid, Oid)> = r1.iter().map(|g| (g.0, g.1)).collect();
            let keys2: Vec<(Oid, Oid)> = r2.iter().map(|g| (g.0, g.1)).collect();
            assert_eq!(keys1, keys2);
            // Three distinct datatype pairs -> three groups, in order.
            assert_eq!(keys1, vec![(1, 1), (1, 2), (2, 2)]);
        }
    }

    #[test]
    fn opr_only_list_uses_operator_keys() {
        // No procs: groups come entirely from the operator list.
        let oprs = vec![make_tuple(amop(7, 8, 3)), make_tuple(amop(9, 9, 4))];
        let oprlist = make_list(oprs, true);
        let proclist = make_list(vec![], true);

        unsafe {
            let res = identify_opfamily_groups(oprlist.as_catclist(), proclist.as_catclist());
            let groups = collect(res);
            assert_eq!(groups.len(), 2);
            assert_eq!((groups[0].0, groups[0].1), (7, 8));
            assert_eq!(groups[0].2, 1u64 << 3);
            assert_eq!((groups[1].0, groups[1].1), (9, 9));
            assert_eq!(groups[1].2, 1u64 << 4);
        }
    }

    // --- signature comparison cores (REAL) -----------------------------------

    #[test]
    fn proc_sig_exact_ok() {
        let sig = ProcSig {
            prorettype: 23,
            proretset: false,
            pronargs: 2,
            proargtypes: vec![25, 25],
        };
        assert!(check_proc_sig(&sig, 23, true, 2, 2, &[25, 25]));
    }

    #[test]
    fn proc_sig_rejects_wrong_restype_and_set() {
        let mut sig = ProcSig {
            prorettype: 23,
            proretset: false,
            pronargs: 1,
            proargtypes: vec![25],
        };
        // wrong result type
        assert!(!check_proc_sig(&sig, 16, true, 1, 1, &[25]));
        // returns a set
        sig.proretset = true;
        assert!(!check_proc_sig(&sig, 23, true, 1, 1, &[25]));
    }

    #[test]
    fn proc_sig_nargs_bounds() {
        let sig = ProcSig {
            prorettype: 16,
            proretset: false,
            pronargs: 3,
            proargtypes: vec![25, 25, 25],
        };
        // pronargs above maxargs
        assert!(!check_proc_sig(&sig, 16, true, 1, 2, &[25, 25]));
        // pronargs below minargs
        let sig2 = ProcSig {
            prorettype: 16,
            proretset: false,
            pronargs: 0,
            proargtypes: vec![],
        };
        assert!(!check_proc_sig(&sig2, 16, true, 1, 2, &[25, 25]));
    }

    #[test]
    fn proc_sig_arg_type_mismatch_only_within_pronargs() {
        // maxargs=2 but pronargs=1: the second (extra) expected arg is ignored.
        let sig = ProcSig {
            prorettype: 16,
            proretset: false,
            pronargs: 1,
            proargtypes: vec![25],
        };
        // first arg matches; second arg slot ignored because i >= pronargs.
        assert!(check_proc_sig(&sig, 16, true, 1, 2, &[25, 9999]));
        // first arg mismatch -> reject.
        assert!(!check_proc_sig(&sig, 16, true, 1, 2, &[26, 9999]));
    }

    #[test]
    fn op_sig_exact_binary_only() {
        let sig = OpSig {
            oprresult: 16,
            oprkind: b'b' as c_char,
            oprleft: 23,
            oprright: 23,
        };
        assert!(check_op_sig(&sig, 16, 23, 23));
        // prefix operator ('l') rejected
        let mut prefix = sig.clone();
        prefix.oprkind = b'l' as c_char;
        assert!(!check_op_sig(&prefix, 16, 23, 23));
        // wrong left type rejected
        assert!(!check_op_sig(&sig, 16, 99, 23));
        // wrong result rejected
        assert!(!check_op_sig(&sig, 25, 23, 23));
    }
}
