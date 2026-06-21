//! C-ABI forwarding shims for the linked C parser (scan.c/gram.c/parser.c/
//! kwlookup.c/keywords.c, compiled by build.rs).  Each `#[no_mangle] extern "C"`
//! entry satisfies an undefined symbol the generated parser references, forwarding
//! to the real Rust implementation.  Pointer args are normalized to `c_void`
//! (ABI-equivalent); enum args arrive as `c_int` and are `transmute`d to the
//! callee's enum (the C parser only ever passes in-range values).
//!
//! Symbols already exported elsewhere are intentionally NOT redefined here:
//!   palloc/pfree/pstrdup/pg_strcasecmp/truncate_identifier  -> backend_link_shims.rs
//!   copyObjectImpl                                          -> nodes::copyfuncs (#[no_mangle])
//!   psprintf, errmsg/errmsg_internal/errdetail/errhint      -> C glue (pdb_parser_glue.c)
//!   error_context_stack                                     -> utils::error::elog_impl (#[no_mangle])

use core::ffi::{c_char, c_int, c_uchar, c_void};

type Size = usize;
type Oid = u32;
type PgWchar = u32;
type Index = u32;
type LOCKMODE = c_int;
type AttrNumber = i16;

// ---- nodes/value.c ----
#[no_mangle] pub unsafe extern "C" fn makeString(s: *mut c_void) -> *mut c_void {
    crate::nodes::value::makeString(s as _) as _
}
#[no_mangle] pub unsafe extern "C" fn makeInteger(i: c_int) -> *mut c_void {
    crate::nodes::value::makeInteger(i) as _
}
#[no_mangle] pub unsafe extern "C" fn makeFloat(num: *mut c_void) -> *mut c_void {
    crate::nodes::value::makeFloat(num as _) as _
}
#[no_mangle] pub unsafe extern "C" fn makeBoolean(val: bool) -> *mut c_void {
    crate::nodes::value::makeBoolean(val) as _
}

// ---- nodes/makefuncs.c ----
#[no_mangle] pub unsafe extern "C" fn makeA_Expr(kind: c_int, name: *mut c_void, lexpr: *mut c_void, rexpr: *mut c_void, location: c_int) -> *mut c_void {
    crate::nodes::makefuncs::makeA_Expr(core::mem::transmute(kind), name as _, lexpr as _, rexpr as _, location) as _
}
#[no_mangle] pub unsafe extern "C" fn makeSimpleA_Expr(kind: c_int, name: *mut c_char, lexpr: *mut c_void, rexpr: *mut c_void, location: c_int) -> *mut c_void {
    crate::nodes::makefuncs::makeSimpleA_Expr(core::mem::transmute(kind), name, lexpr as _, rexpr as _, location) as _
}
#[no_mangle] pub unsafe extern "C" fn makeAlias(aliasname: *const c_char, colnames: *mut c_void) -> *mut c_void {
    crate::nodes::makefuncs::makeAlias(aliasname, colnames as _) as _
}
#[no_mangle] pub unsafe extern "C" fn makeBoolExpr(boolop: c_int, args: *mut c_void, location: c_int) -> *mut c_void {
    crate::nodes::makefuncs::makeBoolExpr(core::mem::transmute(boolop), args as _, location) as _
}
#[no_mangle] pub unsafe extern "C" fn makeDefElem(name: *mut c_char, arg: *mut c_void, location: c_int) -> *mut c_void {
    crate::nodes::makefuncs::makeDefElem(name, arg as _, location) as _
}
#[no_mangle] pub unsafe extern "C" fn makeDefElemExtended(name_space: *mut c_char, name: *mut c_char, arg: *mut c_void, defaction: c_int, location: c_int) -> *mut c_void {
    crate::nodes::makefuncs::makeDefElemExtended(name_space, name, arg as _, core::mem::transmute(defaction), location) as _
}
#[no_mangle] pub unsafe extern "C" fn makeFuncCall(name: *mut c_void, args: *mut c_void, funcformat: c_int, location: c_int) -> *mut c_void {
    crate::nodes::makefuncs::makeFuncCall(name as _, args as _, core::mem::transmute(funcformat), location) as _
}
#[no_mangle] pub unsafe extern "C" fn makeGroupingSet(kind: c_int, content: *mut c_void, location: c_int) -> *mut c_void {
    crate::nodes::makefuncs::makeGroupingSet(core::mem::transmute(kind), content as _, location) as _
}
#[no_mangle] pub unsafe extern "C" fn makeRangeVar(schemaname: *mut c_char, relname: *mut c_char, location: c_int) -> *mut c_void {
    crate::nodes::makefuncs::makeRangeVar(schemaname, relname, location) as _
}
#[no_mangle] pub unsafe extern "C" fn makeStringConst(s: *mut c_char, location: c_int) -> *mut c_void {
    crate::nodes::makefuncs::makeStringConst(s, location) as _
}
#[no_mangle] pub unsafe extern "C" fn makeTypeName(typnam: *mut c_char) -> *mut c_void {
    crate::nodes::makefuncs::makeTypeName(typnam) as _
}
#[no_mangle] pub unsafe extern "C" fn makeTypeNameFromNameList(names: *mut c_void) -> *mut c_void {
    crate::nodes::makefuncs::makeTypeNameFromNameList(names as _) as _
}
#[no_mangle] pub unsafe extern "C" fn makeVacuumRelation(relation: *mut c_void, oid: Oid, va_cols: *mut c_void) -> *mut c_void {
    crate::nodes::makefuncs::makeVacuumRelation(relation as _, oid, va_cols as _) as _
}
#[no_mangle] pub unsafe extern "C" fn makeJsonBehavior(btype: c_int, expr: *mut c_void, location: c_int) -> *mut c_void {
    crate::nodes::makefuncs::makeJsonBehavior(core::mem::transmute(btype), expr as _, location) as _
}
#[no_mangle] pub unsafe extern "C" fn makeJsonFormat(typ: c_int, encoding: c_int, location: c_int) -> *mut c_void {
    crate::nodes::makefuncs::makeJsonFormat(core::mem::transmute(typ), core::mem::transmute(encoding), location) as _
}
#[no_mangle] pub unsafe extern "C" fn makeJsonIsPredicate(expr: *mut c_void, format: *mut c_void, item_type: c_int, unique_keys: bool, location: c_int) -> *mut c_void {
    crate::nodes::makefuncs::makeJsonIsPredicate(expr as _, format as _, core::mem::transmute(item_type), unique_keys, location) as _
}
#[no_mangle] pub unsafe extern "C" fn makeJsonKeyValue(key: *mut c_void, value: *mut c_void) -> *mut c_void {
    crate::nodes::makefuncs::makeJsonKeyValue(key as _, value as _) as _
}
#[no_mangle] pub unsafe extern "C" fn makeJsonTablePathSpec(string: *mut c_char, name: *mut c_char, string_location: c_int, name_location: c_int) -> *mut c_void {
    crate::nodes::makefuncs::makeJsonTablePathSpec(string, name, string_location, name_location) as _
}
#[no_mangle] pub unsafe extern "C" fn makeJsonValueExpr(raw_expr: *mut c_void, formatted_expr: *mut c_void, format: *mut c_void) -> *mut c_void {
    crate::nodes::makefuncs::makeJsonValueExpr(raw_expr as _, formatted_expr as _, format as _) as _
}

// ---- nodes/list.c (pg_list home) ----
#[no_mangle] pub unsafe extern "C" fn lappend(list: *mut c_void, datum: *mut c_void) -> *mut c_void {
    crate::nodes::pg_list::lappend(list as _, datum as _) as _
}
#[no_mangle] pub unsafe extern "C" fn lcons(datum: *mut c_void, list: *mut c_void) -> *mut c_void {
    crate::nodes::pg_list::lcons(datum as _, list as _) as _
}
#[no_mangle] pub unsafe extern "C" fn list_concat(list1: *mut c_void, list2: *const c_void) -> *mut c_void {
    crate::nodes::pg_list::list_concat(list1 as _, list2 as _) as _
}
#[no_mangle] pub unsafe extern "C" fn list_copy_tail(oldlist: *const c_void, nskip: c_int) -> *mut c_void {
    crate::nodes::pg_list::list_copy_tail(oldlist as _, nskip) as _
}
#[no_mangle] pub unsafe extern "C" fn list_delete_nth_cell(list: *mut c_void, n: c_int) -> *mut c_void {
    crate::nodes::pg_list::list_delete_nth_cell(list as _, n) as _
}
#[no_mangle] pub unsafe extern "C" fn list_delete_first(list: *mut c_void) -> *mut c_void {
    crate::nodes::pg_list::list_delete_first(list as _) as _
}
#[no_mangle] pub unsafe extern "C" fn list_truncate(list: *mut c_void, new_size: c_int) -> *mut c_void {
    crate::nodes::pg_list::list_truncate(list as _, new_size) as _
}
// list_make{1..4}_impl take ListCell unions BY VALUE (8 bytes each).
#[no_mangle] pub unsafe extern "C" fn list_make1_impl(t: c_int, d1: usize) -> *mut c_void {
    crate::nodes::pg_list::list_make1_impl(core::mem::transmute(t), core::mem::transmute(d1)) as _
}
#[no_mangle] pub unsafe extern "C" fn list_make2_impl(t: c_int, d1: usize, d2: usize) -> *mut c_void {
    crate::nodes::pg_list::list_make2_impl(core::mem::transmute(t), core::mem::transmute(d1), core::mem::transmute(d2)) as _
}
#[no_mangle] pub unsafe extern "C" fn list_make3_impl(t: c_int, d1: usize, d2: usize, d3: usize) -> *mut c_void {
    crate::nodes::pg_list::list_make3_impl(core::mem::transmute(t), core::mem::transmute(d1), core::mem::transmute(d2), core::mem::transmute(d3)) as _
}
#[no_mangle] pub unsafe extern "C" fn list_make4_impl(t: c_int, d1: usize, d2: usize, d3: usize, d4: usize) -> *mut c_void {
    crate::nodes::pg_list::list_make4_impl(core::mem::transmute(t), core::mem::transmute(d1), core::mem::transmute(d2), core::mem::transmute(d3), core::mem::transmute(d4)) as _
}

// ---- nodes/nodeFuncs.c, nodes/equalfuncs.c ----
#[no_mangle] pub unsafe extern "C" fn exprLocation(expr: *const c_void) -> c_int {
    crate::nodes::nodeFuncs::exprLocation(expr as _)
}
#[no_mangle] pub unsafe extern "C" fn equal(a: *const c_void, b: *const c_void) -> bool {
    crate::nodes::equalfuncs::equal(a, b)
}

// ---- catalog/namespace.c, commands/define.c ----
#[no_mangle] pub unsafe extern "C" fn NameListToString(names: *const c_void) -> *mut c_char {
    crate::catalog::namespace::NameListToString(names as _)
}
#[no_mangle] pub unsafe extern "C" fn defGetInt32(def: *mut c_void) -> i32 {
    crate::commands::define::defGetInt32(def as _)
}

// ---- parser/scansup.c ----
#[no_mangle] pub unsafe extern "C" fn downcase_truncate_identifier(ident: *const c_char, len: c_int, warn: bool) -> *mut c_char {
    crate::parser::scansup::downcase_truncate_identifier(ident, len, warn)
}
#[no_mangle] pub extern "C" fn scanner_isspace(ch: c_char) -> bool {
    crate::parser::scansup::scanner_isspace(ch)
}

// ---- utils/mmgr/mcxt.c (palloc0/repalloc; palloc/pfree are in backend_link_shims) ----
#[no_mangle] pub unsafe extern "C" fn palloc0(size: Size) -> *mut c_void {
    crate::utils::mmgr::mcxt::palloc0(size)
}
#[no_mangle] pub unsafe extern "C" fn repalloc(pointer: *mut c_void, size: Size) -> *mut c_void {
    crate::utils::mmgr::mcxt::repalloc(pointer, size)
}

// ---- utils/mb/mbutils.c ----
#[no_mangle] pub unsafe extern "C" fn pg_get_client_encoding() -> c_int {
    crate::utils::mb::mbutils::pg_get_client_encoding()
}
#[no_mangle] pub unsafe extern "C" fn pg_mbstrlen_with_len(mbstr: *const c_char, limit: c_int) -> c_int {
    crate::utils::mb::mbutils::pg_mbstrlen_with_len(mbstr, limit)
}
#[no_mangle] pub unsafe extern "C" fn pg_unicode_to_server(c: PgWchar, s: *mut c_uchar) {
    crate::utils::mb::mbutils::pg_unicode_to_server(c, s)
}
#[no_mangle] pub unsafe extern "C" fn pg_verifymbstr(mbstr: *const c_char, len: c_int, no_error: bool) -> bool {
    crate::utils::mb::mbutils::pg_verifymbstr(mbstr, len, no_error)
}

// ---- utils/adt/numutils.c ----
#[no_mangle] pub unsafe extern "C" fn pg_strtoint32_safe(s: *const c_char, escontext: *mut c_void) -> i32 {
    crate::utils::adt::numutils::pg_strtoint32_safe(s, escontext as _)
}

// ---- utils/error/elog.c (non-variadic; variadic ones go through C glue) ----
#[no_mangle] pub unsafe extern "C" fn errstart(elevel: c_int, domain: *const c_char) -> bool {
    crate::utils::error::elog_impl::errstart(elevel, domain)
}
#[no_mangle] pub unsafe extern "C" fn errstart_cold(elevel: c_int, domain: *const c_char) -> bool {
    crate::utils::error::elog_impl::errstart_cold(elevel, domain)
}
#[no_mangle] pub unsafe extern "C" fn errfinish(filename: *const c_char, lineno: c_int, funcname: *const c_char) {
    crate::utils::error::elog_impl::errfinish(filename, lineno, funcname)
}
#[no_mangle] pub unsafe extern "C" fn errcode(sqlerrcode: c_int) -> c_int {
    crate::utils::error::elog_impl::errcode_impl(sqlerrcode)
}
#[no_mangle] pub unsafe extern "C" fn errposition(cursorpos: c_int) -> c_int {
    crate::utils::error::elog_impl::errposition(cursorpos)
}
#[no_mangle] pub unsafe extern "C" fn geterrcode() -> c_int {
    crate::utils::error::elog_impl::geterrcode()
}
// Targets for the variadic C glue wrappers (already-formatted message string).
#[no_mangle] pub unsafe extern "C" fn pdb_rs_errmsg(msg: *const c_char) -> c_int {
    crate::utils::error::elog_impl::errmsg_c(msg)
}
#[no_mangle] pub unsafe extern "C" fn pdb_rs_errmsg_internal(msg: *const c_char) -> c_int {
    crate::utils::error::elog_impl::errmsg_internal_c(msg)
}
#[no_mangle] pub unsafe extern "C" fn pdb_rs_errdetail(msg: *const c_char) -> c_int {
    crate::utils::error::elog_impl::errdetail_c(msg)
}
#[no_mangle] pub unsafe extern "C" fn pdb_rs_errhint(msg: *const c_char) -> c_int {
    crate::utils::error::elog_impl::errhint_c(msg)
}
#[no_mangle] pub unsafe extern "C" fn pdb_rs_errdetail_internal(msg: *const c_char) -> c_int {
    crate::utils::error::elog_impl::errdetail_internal_c(msg)
}
#[no_mangle] pub unsafe extern "C" fn pdb_rs_errcontext_msg(msg: *const c_char) -> c_int {
    crate::utils::error::elog_impl::errcontext_msg_c(msg)
}
#[no_mangle] pub unsafe extern "C" fn pdb_rs_errmsg_plural(msg: *const c_char, n: core::ffi::c_ulong) -> c_int {
    crate::utils::error::elog_impl::errmsg_plural_c(msg, n)
}
#[no_mangle] pub unsafe extern "C" fn pdb_rs_format_elog_string(msg: *const c_char) -> *mut c_char {
    crate::utils::error::elog_impl::format_elog_string_c(msg)
}

// ============================================================================
// Cascade batch: symbols pulled in by the now-reachable parser-support Rust code
// (the "extern-C-stub iceberg").  Forward to canonical Rust homes.
// ============================================================================

// ---- nodes/pg_list.c accessors (macros in C; real fns in pg_list.rs) ----
#[no_mangle] pub unsafe extern "C" fn lfirst(lc: *const c_void) -> *mut c_void {
    crate::nodes::pg_list::lfirst(lc as _)
}
#[no_mangle] pub unsafe extern "C" fn lfirst_oid(lc: *const c_void) -> Oid {
    crate::nodes::pg_list::lfirst_oid(lc as _)
}
#[no_mangle] pub unsafe extern "C" fn linitial(l: *const c_void) -> *mut c_void {
    crate::nodes::pg_list::linitial(l as _)
}
#[no_mangle] pub unsafe extern "C" fn linitial_oid(l: *const c_void) -> Oid {
    crate::nodes::pg_list::linitial_oid(l as _)
}
#[no_mangle] pub unsafe extern "C" fn lsecond(l: *const c_void) -> *mut c_void {
    crate::nodes::pg_list::lsecond(l as _)
}
#[no_mangle] pub unsafe extern "C" fn lthird(l: *const c_void) -> *mut c_void {
    crate::nodes::pg_list::lthird(l as _)
}
#[no_mangle] pub unsafe extern "C" fn lnext(l: *const c_void, c: *const c_void) -> *mut c_void {
    crate::nodes::pg_list::lnext(l as _, c as _) as _
}
#[no_mangle] pub unsafe extern "C" fn list_head(l: *const c_void) -> *mut c_void {
    crate::nodes::pg_list::list_head(l as _) as _
}
#[no_mangle] pub unsafe extern "C" fn list_length(l: *const c_void) -> c_int {
    crate::nodes::pg_list::list_length(l as _)
}
#[no_mangle] pub unsafe extern "C" fn list_copy(l: *const c_void) -> *mut c_void {
    crate::nodes::pg_list::list_copy(l as _) as _
}
#[no_mangle] pub unsafe extern "C" fn list_free(l: *mut c_void) {
    crate::nodes::pg_list::list_free(l as _)
}
#[no_mangle] pub unsafe extern "C" fn list_member_oid(l: *const c_void, datum: Oid) -> bool {
    crate::nodes::pg_list::list_member_oid(l as _, datum)
}
#[no_mangle] pub unsafe extern "C" fn lappend_oid(l: *mut c_void, datum: Oid) -> *mut c_void {
    crate::nodes::pg_list::lappend_oid(l as _, datum) as _
}
#[no_mangle] pub unsafe extern "C" fn lcons_oid(datum: Oid, l: *mut c_void) -> *mut c_void {
    crate::nodes::pg_list::lcons_oid(datum, l as _) as _
}

// ---- nodes/nodes, nodes/value (nodeTag macro -> generic fn; strVal macro inline) ----
#[no_mangle] pub unsafe extern "C" fn nodeTag(nodeptr: *const c_void) -> c_int {
    core::mem::transmute(crate::nodes::nodes::nodeTag::<c_void>(nodeptr))
}
#[no_mangle] pub unsafe extern "C" fn strVal(v: *mut c_void) -> *mut c_char {
    (*(v as *mut crate::nodes::value::String)).sval
}

// ---- lib/stringinfo.c ----
#[no_mangle] pub unsafe extern "C" fn initStringInfo(s: *mut c_void) {
    crate::lib::stringinfo::initStringInfo(s as _)
}
#[no_mangle] pub unsafe extern "C" fn appendStringInfoChar(s: *mut c_void, ch: c_char) {
    crate::lib::stringinfo::appendStringInfoChar(s as _, ch)
}
#[no_mangle] pub unsafe extern "C" fn appendStringInfoString(s: *mut c_void, str: *const c_char) {
    crate::lib::stringinfo::appendStringInfoString(s as _, str)
}

// ---- utils/mmgr/mcxt.c ----
#[no_mangle] pub unsafe extern "C" fn MemoryContextStrdup(context: *mut c_void, string: *const c_char) -> *mut c_char {
    crate::utils::mmgr::mcxt::MemoryContextStrdup(context as _, string)
}

// ---- utils/adt/varlena.c ----
#[no_mangle] pub unsafe extern "C" fn SplitIdentifierString(rawstring: *mut c_char, separator: c_char, namelist: *mut c_void) -> bool {
    crate::utils::adt::varlena::SplitIdentifierString(rawstring, separator, namelist as _)
}

// ---- storage/lmgr/lmgr.c ----
#[no_mangle] pub unsafe extern "C" fn LockRelationOid(relid: Oid, lockmode: c_int) {
    crate::storage::lmgr::lmgr::LockRelationOid(relid, lockmode)
}
#[no_mangle] pub unsafe extern "C" fn UnlockRelationOid(relid: Oid, lockmode: c_int) {
    crate::storage::lmgr::lmgr::UnlockRelationOid(relid, lockmode)
}
#[no_mangle] pub unsafe extern "C" fn ConditionalLockRelationOid(relid: Oid, lockmode: c_int) -> bool {
    crate::storage::lmgr::lmgr::ConditionalLockRelationOid(relid, lockmode)
}

// ---- utils/cache/inval.c, lsyscache.c ----
#[no_mangle] pub unsafe extern "C" fn AcceptInvalidationMessages() {
    crate::utils::cache::inval::AcceptInvalidationMessages()
}
#[no_mangle] pub unsafe extern "C" fn get_relname_relid(relname: *const c_char, relnamespace: Oid) -> Oid {
    crate::utils::cache::lsyscache::get_relname_relid(relname, relnamespace)
}

// ---- tcop/postgres.c ----
#[no_mangle] pub unsafe extern "C" fn ProcessInterrupts() {
    crate::tcop::postgres::ProcessInterrupts()
}

// ---- catalog/objectaccess.h InvokeNamespaceSearchHook macro ----
#[no_mangle] pub unsafe extern "C" fn InvokeNamespaceSearchHook(namespace_id: Oid, iserror: bool) -> bool {
    if crate::catalog::objectaccess::object_access_hook.is_none() {
        true
    } else {
        crate::catalog::objectaccess::RunNamespaceSearchHook(namespace_id, iserror)
    }
}

// ---- nodes/bitmapset.c (pulled in by optimizer reachability) ----
#[no_mangle] pub unsafe extern "C" fn bms_add_member(a: *mut c_void, x: c_int) -> *mut c_void {
    crate::nodes::bitmapset::bms_add_member(a as _, x) as _
}
#[no_mangle] pub unsafe extern "C" fn bms_join(a: *mut c_void, b: *mut c_void) -> *mut c_void {
    crate::nodes::bitmapset::bms_join(a as _, b as _) as _
}
#[no_mangle] pub unsafe extern "C" fn bms_make_singleton(x: c_int) -> *mut c_void {
    crate::nodes::bitmapset::bms_make_singleton(x) as _
}

// ---- nodes/nodeFuncs.c + lsyscache.c (optimizer reachability) ----
#[no_mangle] pub unsafe extern "C" fn exprType(node: *const c_void) -> Oid {
    crate::nodes::nodeFuncs::exprType(node as _)
}
#[no_mangle] pub unsafe extern "C" fn exprTypmod(node: *const c_void) -> i32 {
    crate::nodes::nodeFuncs::exprTypmod(node as _)
}
#[no_mangle] pub unsafe extern "C" fn exprCollation(node: *const c_void) -> Oid {
    crate::nodes::nodeFuncs::exprCollation(node as _)
}
#[no_mangle] pub unsafe extern "C" fn set_opfuncid(opexpr: *mut c_void) {
    crate::nodes::nodeFuncs::set_opfuncid(opexpr as _)
}
#[no_mangle] pub unsafe extern "C" fn applyRelabelType(arg: *mut c_void, rtype: Oid, rtypmod: i32, rcollid: Oid, rformat: c_int, rlocation: c_int, overwrite_ok: bool) -> *mut c_void {
    crate::nodes::nodeFuncs::applyRelabelType(arg as _, rtype, rtypmod, rcollid, core::mem::transmute(rformat), rlocation, overwrite_ok) as _
}
#[no_mangle] pub unsafe extern "C" fn func_strict(funcid: Oid) -> bool {
    crate::utils::cache::lsyscache::func_strict(funcid)
}
#[no_mangle] pub unsafe extern "C" fn op_input_types(opno: Oid, lefttype: *mut Oid, righttype: *mut Oid) {
    crate::utils::cache::lsyscache::op_input_types(opno, lefttype, righttype)
}
// not on the name-free-query path: satisfy the link, panic only if exercised.
#[no_mangle] pub unsafe extern "C" fn get_leftop(clause: *const c_void) -> *mut c_void {
    let expr = clause as *const crate::nodes::primnodes::OpExpr;
    if !(*expr).args.is_null() { crate::nodes::pg_list::linitial((*expr).args) as *mut c_void } else { core::ptr::null_mut() }
}
#[no_mangle] pub unsafe extern "C" fn get_rightop(clause: *const c_void) -> *mut c_void {
    let expr = clause as *const crate::nodes::primnodes::OpExpr;
    if crate::nodes::pg_list::list_length((*expr).args) >= 2 { crate::nodes::pg_list::lsecond((*expr).args) as *mut c_void } else { core::ptr::null_mut() }
}
#[no_mangle] pub unsafe extern "C" fn make_restrictinfo() { unimplemented!("make_restrictinfo") }
#[no_mangle] pub unsafe extern "C" fn derives_insert(_tb: *mut c_void, _key: *mut c_void, _found: *mut bool) -> *mut c_void { unimplemented!("derives_insert") }
#[no_mangle] pub unsafe extern "C" fn derives_destroy(_tb: *mut c_void) { unimplemented!("derives_destroy") }

// ==== batch21 (optimizer equivclass/path reachability) ====
#[no_mangle] pub unsafe extern "C" fn add_outer_joins_to_relids(a0: *mut c_void, a1: *mut c_void, a2: *mut c_void, a3: *mut c_void) -> *mut c_void { crate::optimizer::path::joinrels::add_outer_joins_to_relids(a0 as _, a1 as _, a2 as _, a3 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn add_vars_to_attr_needed(a0: *mut c_void, a1: *mut c_void, a2: *mut c_void) { crate::optimizer::plan::initsplan::add_vars_to_attr_needed(a0 as _, a1 as _, a2 as _) }
#[no_mangle] pub unsafe extern "C" fn add_vars_to_targetlist(a0: *mut c_void, a1: *mut c_void, a2: *mut c_void) { crate::optimizer::plan::initsplan::add_vars_to_targetlist(a0 as _, a1 as _, a2 as _) }
#[no_mangle] pub unsafe extern "C" fn adjust_appendrel_attrs_multilevel(a0: *mut c_void, a1: *mut c_void, a2: *mut c_void, a3: *mut c_void) -> *mut c_void { crate::optimizer::util::appendinfo::adjust_appendrel_attrs_multilevel(a0 as _, a1 as _, a2 as _, a3 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn adjust_appendrel_attrs(a0: *mut c_void, a1: *mut c_void, a2: c_int, a3: *mut c_void) -> *mut c_void { crate::optimizer::util::appendinfo::adjust_appendrel_attrs(a0 as _, a1 as _, a2, a3 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn palloc0_array_impl(size: Size, n: Size) -> *mut c_void { crate::utils::mmgr::mcxt::palloc0(size * n) }
#[no_mangle] pub unsafe extern "C" fn repalloc0_array_impl(ptr: *mut c_void, size: Size, old_n: Size, new_n: Size) -> *mut c_void {
    let oldsize = size * old_n;
    let newsize = size * new_n;
    let p = crate::utils::mmgr::mcxt::repalloc(ptr, newsize);
    if newsize > oldsize { core::ptr::write_bytes((p as *mut u8).add(oldsize), 0, newsize - oldsize); }
    p
}
#[no_mangle] pub unsafe extern "C" fn build_implied_join_equality(a0: *mut c_void, a1: Oid, a2: Oid, a3: *mut c_void, a4: *mut c_void, a5: *mut c_void, a6: Index) -> *mut c_void { crate::optimizer::plan::initsplan::build_implied_join_equality(a0 as _, a1, a2, a3 as _, a4 as _, a5 as _, a6) as _ }
#[no_mangle] pub unsafe extern "C" fn contain_agg_clause(a0: *mut c_void) -> bool { crate::optimizer::util::clauses::contain_agg_clause(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn contain_volatile_functions(a0: *mut c_void) -> bool { crate::optimizer::util::clauses::contain_volatile_functions(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn contain_window_function(a0: *mut c_void) -> bool { crate::optimizer::util::clauses::contain_window_function(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn copyObject(a0: *const c_void) -> *mut c_void { crate::nodes::copyfuncs::copyObjectImpl(a0) }
#[no_mangle] pub unsafe extern "C" fn CombineRangeTables(a0: *mut c_void, a1: *mut c_void, a2: *mut c_void, a3: *mut c_void) { crate::rewrite::rewriteManip::CombineRangeTables(a0 as _, a1 as _, a2 as _, a3 as _) }
#[no_mangle] pub unsafe extern "C" fn OffsetVarNodes(a0: *mut c_void, a1: c_int, a2: c_int) { crate::rewrite::rewriteManip::OffsetVarNodes(a0 as _, a1, a2) }
#[no_mangle] pub unsafe extern "C" fn IncrementVarSublevelsUp_rtable(a0: *mut c_void, a1: c_int, a2: c_int) { crate::rewrite::rewriteManip::IncrementVarSublevelsUp_rtable(a0 as _, a1, a2) }
#[no_mangle] pub unsafe extern "C" fn flatten_join_alias_vars(a0: *mut c_void, a1: *mut c_void, a2: *mut c_void) -> *mut c_void { crate::optimizer::util::var::flatten_join_alias_vars(a0 as _, a1 as _, a2 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn bms_copy(a0: *const c_void) -> *mut c_void { crate::nodes::bitmapset::bms_copy(a0 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn derives_create() { unimplemented!("derives_create") }
#[no_mangle] pub unsafe extern "C" fn derives_lookup() { unimplemented!("derives_lookup") }
#[no_mangle] pub unsafe extern "C" fn distribute_restrictinfo_to_rels(a0: *mut c_void, a1: *mut c_void)  { crate::optimizer::plan::initsplan::distribute_restrictinfo_to_rels(a0 as _, a1 as _) }
#[no_mangle] pub unsafe extern "C" fn expression_returns_set(a0: *mut c_void) -> bool { crate::nodes::nodeFuncs::expression_returns_set(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn find_childrel_parents(a0: *mut c_void, a1: *mut c_void) -> *mut c_void { crate::optimizer::util::relnode::find_childrel_parents(a0 as _, a1 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn get_func_leakproof(a0: Oid) -> bool { crate::utils::cache::lsyscache::get_func_leakproof(a0) }
#[no_mangle] pub unsafe extern "C" fn get_mergejoin_opfamilies(a0: Oid) -> *mut c_void { crate::utils::cache::lsyscache::get_mergejoin_opfamilies(a0) as _ }
#[no_mangle] pub unsafe extern "C" fn get_opfamily_member_for_cmptype(a0: Oid, a1: Oid, a2: Oid, a3: c_int) -> Oid { crate::utils::cache::lsyscache::get_opfamily_member_for_cmptype(a0, a1, a2, core::mem::transmute(a3)) }
#[no_mangle] pub unsafe extern "C" fn op_hashjoinable(a0: Oid, a1: Oid) -> bool { crate::utils::cache::lsyscache::op_hashjoinable(a0, a1) }
#[no_mangle] pub unsafe extern "C" fn process_implied_equality(a0: *mut c_void, a1: Oid, a2: Oid, a3: *mut c_void, a4: *mut c_void, a5: *mut c_void, a6: Index, a7: bool) -> *mut c_void { crate::optimizer::plan::initsplan::process_implied_equality(a0 as _, a1, a2, a3 as _, a4 as _, a5 as _, a6, a7) as _ }
#[no_mangle] pub unsafe extern "C" fn pull_var_clause(a0: *mut c_void, a1: c_int) -> *mut c_void { crate::optimizer::util::var::pull_var_clause(a0 as _, a1) as _ }
#[no_mangle] pub unsafe extern "C" fn pull_varnos(a0: *mut c_void, a1: *mut c_void) -> *mut c_void { crate::optimizer::util::var::pull_varnos(a0 as _, a1 as _) as _ }

// ==== batch47 (prepjointree reachability) ====
#[no_mangle] pub unsafe extern "C" fn CHECK_FOR_INTERRUPTS() {}
#[no_mangle] pub unsafe extern "C" fn ChangeVarNodes(a0: *mut c_void, a1: c_int, a2: c_int, a3: c_int)  { crate::rewrite::rewriteManip::ChangeVarNodes(a0 as _, a1, a2, a3) }
#[no_mangle] pub unsafe extern "C" fn IncrementVarSublevelsUp(a0: *mut c_void, a1: c_int, a2: c_int)  { crate::rewrite::rewriteManip::IncrementVarSublevelsUp(a0 as _, a1, a2) }
#[no_mangle] pub unsafe extern "C" fn ReplaceVarFromTargetList(var: *mut c_void, target_rte: *mut c_void, targetlist: *mut c_void, result_relation: c_int, nomatch_option: c_int, nomatch_varno: c_int)->*mut c_void{ crate::rewrite::rewriteManip::ReplaceVarFromTargetList(var as _, target_rte as _, targetlist as _, result_relation, core::mem::transmute(nomatch_option), nomatch_varno) as _ }
#[no_mangle] pub unsafe extern "C" fn add_nulling_relids(a0: *mut c_void, a1: *mut c_void, a2: *mut c_void) -> *mut c_void { crate::rewrite::rewriteManip::add_nulling_relids(a0 as _, a1 as _, a2 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn bms_add_members(a0: *mut c_void, a1: *mut c_void) -> *mut c_void { crate::nodes::bitmapset::bms_add_members(a0 as _, a1 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn bms_del_member(a0: *mut c_void, a1: c_int) -> *mut c_void { crate::nodes::bitmapset::bms_del_member(a0 as _, a1) as _ }
#[no_mangle] pub unsafe extern "C" fn bms_del_members(a0: *mut c_void, a1: *mut c_void) -> *mut c_void { crate::nodes::bitmapset::bms_del_members(a0 as _, a1 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn bms_equal(a0: *mut c_void, a1: *mut c_void) -> bool { crate::nodes::bitmapset::bms_equal(a0 as _, a1 as _) }
#[no_mangle] pub unsafe extern "C" fn bms_intersect(a0: *mut c_void, a1: *mut c_void) -> *mut c_void { crate::nodes::bitmapset::bms_intersect(a0 as _, a1 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn bms_is_empty(a0: *mut c_void) -> bool { crate::nodes::bitmapset::bms_is_empty(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn bms_is_member(a0: c_int, a1: *mut c_void) -> bool { crate::nodes::bitmapset::bms_is_member(a0, a1 as _) }
#[no_mangle] pub unsafe extern "C" fn bms_is_subset(a0: *mut c_void, a1: *mut c_void) -> bool { crate::nodes::bitmapset::bms_is_subset(a0 as _, a1 as _) }
#[no_mangle] pub unsafe extern "C" fn bms_next_member(a0: *mut c_void, a1: c_int) -> c_int { crate::nodes::bitmapset::bms_next_member(a0 as _, a1) }
#[no_mangle] pub unsafe extern "C" fn bms_overlap(a0: *mut c_void, a1: *mut c_void) -> bool { crate::nodes::bitmapset::bms_overlap(a0 as _, a1 as _) }
#[no_mangle] pub unsafe extern "C" fn bms_singleton_member(a0: *mut c_void) -> c_int { crate::nodes::bitmapset::bms_singleton_member(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn bms_union(a0: *mut c_void, a1: *mut c_void) -> *mut c_void { crate::nodes::bitmapset::bms_union(a0 as _, a1 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn build_generation_expression(a0: *mut c_void, a1: c_int) -> *mut c_void { crate::rewrite::rewriteHandler::build_generation_expression(a0 as _, a1) as _ }
#[no_mangle] pub unsafe extern "C" fn check_stack_depth()  { crate::utils::misc::stack_depth::check_stack_depth() }
#[no_mangle] pub unsafe extern "C" fn contain_nonstrict_functions(a0: *mut c_void) -> bool { crate::optimizer::util::clauses::contain_nonstrict_functions(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn contain_vars_of_level(a0: *mut c_void, a1: c_int) -> bool { crate::optimizer::util::var::contain_vars_of_level(a0 as _, a1) }
#[no_mangle] pub unsafe extern "C" fn convert_ANY_sublink_to_join(a0: *mut c_void, a1: *mut c_void, a2: *mut c_void) -> *mut c_void { crate::optimizer::plan::subselect::convert_ANY_sublink_to_join(a0 as _, a1 as _, a2 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn convert_EXISTS_sublink_to_join(a0: *mut c_void, a1: *mut c_void, a2: bool, a3: *mut c_void) -> *mut c_void { crate::optimizer::plan::subselect::convert_EXISTS_sublink_to_join(a0 as _, a1 as _, a2, a3 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn convert_VALUES_to_ANY(a0: *mut c_void, a1: *mut c_void, a2: *mut c_void) -> *mut c_void { crate::optimizer::plan::subselect::convert_VALUES_to_ANY(a0 as _, a1 as _, a2 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn eval_const_expressions(a0: *mut c_void, a1: *mut c_void) -> *mut c_void { crate::optimizer::util::clauses::eval_const_expressions(a0 as _, a1 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn expression_tree_walker(a0: *mut c_void, a1: *mut c_void, a2: *mut c_void) -> bool { crate::nodes::nodeFuncs::expression_tree_walker(a0 as _, core::mem::transmute(a1), a2 as _) }
#[no_mangle] pub unsafe extern "C" fn get_expr_result_type(a0:*mut c_void,a1:*mut c_void,a2:*mut c_void)->c_int{ core::mem::transmute::<crate::utils::fmgr::funcapi::TypeFuncClass,c_int>(crate::utils::fmgr::funcapi::get_expr_result_type(a0 as _, a1 as _, a2 as _)) }
#[no_mangle] pub unsafe extern "C" fn linitial_node_RangeTblFunction(list: *mut c_void) -> *mut c_void { crate::nodes::pg_list::linitial(list as _) as *mut c_void }
#[no_mangle] pub unsafe extern "C" fn makeFromExpr(a0: *mut c_void, a1: *mut c_void) -> *mut c_void { crate::nodes::makefuncs::makeFromExpr(a0 as _, a1 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn makeTargetEntry(a0: *mut c_void, a1: AttrNumber, a2: *mut c_void, a3: bool) -> *mut c_void { crate::nodes::makefuncs::makeTargetEntry(a0 as _, a1, a2 as _, a3) as _ }
#[no_mangle] pub unsafe extern "C" fn makeVar(a0: c_int, a1: AttrNumber, a2: Oid, a3: c_int, a4: Oid, a5: Index) -> *mut c_void { crate::nodes::makefuncs::makeVar(a0, a1, a2, core::mem::transmute(a3), a4, a5) as _ }
#[no_mangle] pub unsafe extern "C" fn makeWholeRowVar_simple() { unimplemented!("makeWholeRowVar_simple") }
#[no_mangle] pub unsafe extern "C" fn make_and_qual(a0: *mut c_void, a1: *mut c_void) -> *mut c_void { crate::nodes::makefuncs::make_and_qual(a0 as _, a1 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn make_andclause(a0: *mut c_void) -> *mut c_void { crate::nodes::makefuncs::make_andclause(a0 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn make_placeholder_expr(a0: *mut c_void, a1: *mut c_void, a2: *mut c_void) -> *mut c_void { crate::optimizer::util::placeholder::make_placeholder_expr(a0 as _, a1 as _, a2 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn pull_varnos_of_level(a0: *mut c_void, a1: *mut c_void, a2: c_int) -> *mut c_void { crate::optimizer::util::var::pull_varnos_of_level(a0 as _, a1 as _, a2) as _ }
#[no_mangle] pub unsafe extern "C" fn query_or_expression_tree_walker(a0: *mut c_void, a1: *mut c_void, a2: *mut c_void, a3: c_int) -> bool { crate::nodes::nodeFuncs::query_or_expression_tree_walker(a0 as _, core::mem::transmute(a1), a2 as _, a3) }
#[no_mangle] pub unsafe extern "C" fn query_tree_walker(a0: *mut c_void, a1: *mut c_void, a2: *mut c_void, a3: c_int) -> bool { crate::nodes::nodeFuncs::query_tree_walker(a0 as _, core::mem::transmute(a1), a2 as _, a3) }
#[no_mangle] pub unsafe extern "C" fn range_table_entry_walker(a0: *mut c_void, a1: *mut c_void, a2: *mut c_void, a3: c_int) -> bool { crate::nodes::nodeFuncs::range_table_entry_walker(a0 as _, core::mem::transmute(a1), a2 as _, a3) }
#[no_mangle] pub unsafe extern "C" fn remove_nulling_relids(a0: *mut c_void, a1: *mut c_void, a2: *mut c_void) -> *mut c_void { crate::rewrite::rewriteManip::remove_nulling_relids(a0 as _, a1 as _, a2 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn replace_rte_variables(node: *mut c_void, target_varno: c_int, sublevels_up: c_int, callback: *mut c_void, callback_arg: *mut c_void, outer_has_sublinks: *mut bool) -> *mut c_void {
    crate::rewrite::rewriteManip::replace_rte_variables(node as _, target_varno, sublevels_up, core::mem::transmute(callback), callback_arg as _, outer_has_sublinks) as _
}
#[no_mangle] pub unsafe extern "C" fn table_close(a0: *mut c_void, a1: LOCKMODE)  { crate::access::table::table::table_close(a0 as _, a1) }
#[no_mangle] pub unsafe extern "C" fn table_open(a0: Oid, a1: LOCKMODE) -> *mut c_void { crate::access::table::table::table_open(a0, a1) as _ }
#[no_mangle] pub unsafe extern "C" fn tlist_same_datatypes(a0: *mut c_void, a1: *mut c_void, a2: bool) -> bool { crate::optimizer::util::tlist::tlist_same_datatypes(a0 as _, a1 as _, a2) }
#[no_mangle] pub unsafe extern "C" fn tuple_desc_attr_stub() { unimplemented!("tuple_desc_attr_stub") }

#[no_mangle] pub unsafe extern "C" fn rt_fetch(rtindex: Index, rtable: *mut c_void) -> *mut c_void {
    crate::optimizer::path::allpaths::rt_fetch(rtindex, rtable as _) as _
}
#[no_mangle] pub unsafe extern "C" fn inline_set_returning_function(root: *mut c_void, rte: *mut c_void) -> *mut c_void {
    crate::optimizer::util::clauses::inline_set_returning_function(root as _, rte as _) as _
}

// ==== batch createplan-cascade ====
#[no_mangle] pub unsafe extern "C" fn RelationGetIndexExpressions(a0: *mut c_void) -> *mut c_void { crate::utils::cache::relcache::RelationGetIndexExpressions(a0 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn RelationGetIndexList(a0: *mut c_void) -> *mut c_void { crate::utils::cache::relcache::RelationGetIndexList(a0 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn RelationGetIndexPredicate(a0: *mut c_void) -> *mut c_void { crate::utils::cache::relcache::RelationGetIndexPredicate(a0 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn RelationGetNumberOfAttributes(a0: *mut c_void) -> c_int { crate::utils::cache::relcache::RelationGetNumberOfAttributes(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn TupleDescAttr(a0: *mut c_void, a1: c_int) -> *mut c_void { crate::access::common::tupdesc::TupleDescAttr(a0 as _, a1) as _ }
#[no_mangle] pub unsafe extern "C" fn expandRTE(a0: *mut c_void, a1: c_int, a2: c_int, a3: c_int, a4: c_int, a5: bool, a6: *mut c_void, a7: *mut c_void)  { crate::parser::parse_relation::expandRTE(a0 as _, a1, a2, core::mem::transmute(a3), a4, a5, a6 as _, a7 as _) }
#[no_mangle] pub unsafe extern "C" fn get_constraint_index(a0: Oid) -> Oid { crate::utils::cache::lsyscache::get_constraint_index(a0) }
#[no_mangle] pub unsafe extern "C" fn get_opclass_family(a0: Oid) -> Oid { crate::utils::cache::lsyscache::get_opclass_family(a0) }
#[no_mangle] pub unsafe extern "C" fn get_opclass_input_type(a0: Oid) -> Oid { crate::utils::cache::lsyscache::get_opclass_input_type(a0) }
#[no_mangle] pub unsafe extern "C" fn index_close(a0: *mut c_void, a1: LOCKMODE)  { crate::access::index::indexam::index_close(a0 as _, a1) }
#[no_mangle] pub unsafe extern "C" fn index_open(a0: Oid, a1: LOCKMODE) -> *mut c_void { crate::access::index::indexam::index_open(a0, a1) as _ }
#[no_mangle] pub unsafe extern "C" fn is_parallel_safe(a0: *mut c_void, a1: *mut c_void) -> bool { crate::optimizer::util::clauses::is_parallel_safe(a0 as _, a1 as _) }
#[no_mangle] pub unsafe extern "C" fn makeVarFromTargetEntry(a0: c_int, a1: *mut c_void) -> *mut c_void { crate::nodes::makefuncs::makeVarFromTargetEntry(a0, a1 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn planner_rt_fetch(a0: Index, a1: *mut c_void) -> *mut c_void { crate::optimizer::util::pathnode::planner_rt_fetch(a0, a1 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn predicate_implied_by(a0: *mut c_void, a1: *mut c_void, a2: bool) -> bool { crate::optimizer::util::predtest::predicate_implied_by(a0 as _, a1 as _, a2) }

#[no_mangle] pub unsafe extern "C" fn shm_mq_receive(_mqh: *mut c_void, _nbytesp: *mut usize, _datap: *mut *mut c_void, _nowait: bool) -> c_int { unimplemented!("shm_mq_receive") }
// ---- syscache lookups pulled in by operator/function resolution ----
#[no_mangle] pub unsafe extern "C" fn SearchSysCache4(cache_id: c_int, k1: usize, k2: usize, k3: usize, k4: usize) -> *mut c_void {
    crate::utils::cache::syscache::SearchSysCache4(cache_id, k1 as _, k2 as _, k3 as _, k4 as _) as _
}
#[no_mangle] pub unsafe extern "C" fn SearchSysCacheList3(cache_id: c_int, k1: usize, k2: usize, k3: usize) -> *mut c_void {
    crate::utils::cache::syscache::SearchSysCacheList(cache_id, 3, k1 as _, k2 as _, k3 as _) as _
}

#[no_mangle] pub unsafe extern "C" fn pdb_rs_append_string(str: *mut c_void, s: *const c_char) { crate::lib::stringinfo::appendStringInfoString(str as _, s) }
// ---- ORPHANED / STUB targets (not reached by simple queries; satisfy the link) ----
// copyObjectImpl now provided by the wired nodes/copyfuncs.rs.
// commands/dbcommands.rs is unwired.
#[no_mangle] pub unsafe extern "C" fn get_database_name(_dbid: Oid) -> *mut c_char {
    unimplemented!("get_database_name: dbcommands not yet wired")
}
// namespace search-path cache (PG simplehash); not exercised by name-free queries.
#[no_mangle] pub unsafe extern "C" fn nsphash_create(_ctx: *mut c_void, _nelem: u32, _pd: *mut c_void) -> *mut c_void {
    unimplemented!("nsphash_create")
}
#[no_mangle] pub unsafe extern "C" fn nsphash_get_num_entries(_tb: *mut c_void) -> u32 {
    unimplemented!("nsphash_get_num_entries")
}
#[no_mangle] pub unsafe extern "C" fn nsphash_insert(_tb: *mut c_void, _key: *mut c_void, _found: *mut bool) -> *mut c_void {
    unimplemented!("nsphash_insert")
}
#[no_mangle] pub unsafe extern "C" fn nsphash_lookup(_tb: *mut c_void, _key: *mut c_void) -> *mut c_void {
    unimplemented!("nsphash_lookup")
}
// syslogger.rs is unwired; we log to stderr (redirection_done = false).
#[no_mangle] pub static mut redirection_done: bool = false;
#[no_mangle] pub unsafe extern "C" fn write_syslogger_file(_buffer: *const c_char, _count: c_int, _destination: c_int) {
    unimplemented!("write_syslogger_file: syslogger not wired")
}

// ==== batch48 (plancat get_relation_info reachability) ====
#[no_mangle] pub unsafe extern "C" fn CreatePartitionDirectory(a0: *mut c_void, a1: bool) -> *mut c_void { crate::partitioning::partdesc::CreatePartitionDirectory(a0 as _, a1) as _ }
#[no_mangle] pub unsafe extern "C" fn PartitionDirectoryLookup(a0: *mut c_void, a1: *mut c_void) -> *mut c_void { crate::partitioning::partdesc::PartitionDirectoryLookup(a0 as _, a1 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn GetFdwRoutineForRelation(a0: *mut c_void, a1: bool) -> *mut c_void { crate::foreign::foreign::GetFdwRoutineForRelation(a0 as _, a1) as _ }
#[no_mangle] pub unsafe extern "C" fn GetForeignServerIdByRelId(a0: Oid) -> Oid { crate::foreign::foreign::GetForeignServerIdByRelId(a0) }
#[no_mangle] pub unsafe extern "C" fn HeapTupleHeaderGetXmin(a0: *mut c_void) -> u32 { crate::access::htup_details::HeapTupleHeaderGetXmin(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn IsSystemRelation(a0: *mut c_void) -> bool { crate::catalog::catalog::IsSystemRelation(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn ObjectIdGetDatum(a0: Oid) -> u64 { crate::postgres::ObjectIdGetDatum(a0) as u64 }
#[no_mangle] pub unsafe extern "C" fn RelationGetFKeyList(a0: *mut c_void) -> *mut c_void { crate::utils::cache::relcache::RelationGetFKeyList(a0 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn RelationGetIndexAttOptions(a0: *mut c_void, a1: bool) -> *mut c_void { crate::utils::cache::relcache::RelationGetIndexAttOptions(a0 as _, a1) as _ }
#[no_mangle] pub unsafe extern "C" fn RelationGetParallelWorkers(_a0: *mut c_void, default_val: c_int) -> c_int { default_val }
#[no_mangle] pub unsafe extern "C" fn RelationGetPartitionKey(a0: *mut c_void) -> *mut c_void { crate::utils::cache::partcache::RelationGetPartitionKey(a0 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn RelationGetPartitionQual(a0: *mut c_void) -> *mut c_void { crate::utils::cache::partcache::RelationGetPartitionQual(a0 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn RelationGetStatExtList(a0: *mut c_void) -> *mut c_void { crate::utils::cache::relcache::RelationGetStatExtList(a0 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn RelationIsPermanent(a0: *mut c_void) -> bool { crate::utils::cache::relcache::RelationIsPermanent(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn SystemAttributeDefinition(a0: i16) -> *const c_void { crate::catalog::heap::SystemAttributeDefinition(a0) as _ }
#[no_mangle] pub unsafe extern "C" fn TransactionIdPrecedes(a0: u32, a1: u32) -> bool { crate::access::transam::transam::TransactionIdPrecedes(a0, a1) }
#[no_mangle] pub unsafe extern "C" fn estimate_rel_size(a0: *mut c_void, a1: *mut i32, a2: *mut u32, a3: *mut f64, a4: *mut f64) { crate::optimizer::util::plancat::estimate_rel_size_local(a0 as _, a1, a2 as _, a3, a4) }
#[no_mangle] pub unsafe extern "C" fn expression_planner(a0: *mut c_void) -> *mut c_void { crate::optimizer::optimizer::expression_planner(a0 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn fmgr_info_copy(a0: *mut c_void, a1: *mut c_void, a2: *mut c_void) { crate::utils::fmgr::fmgr_info_copy(a0 as _, a1 as _, a2 as _) }
#[no_mangle] pub unsafe extern "C" fn index_can_return(a0: *mut c_void, a1: c_int) -> bool { crate::access::index::indexam::index_can_return(a0 as _, a1) }
#[no_mangle] pub unsafe extern "C" fn CurrentMemoryContext() -> *mut c_void { crate::utils::mmgr::mcxt::CurrentMemoryContext as _ }
#[no_mangle] pub unsafe extern "C" fn TransactionXmin() -> u32 { crate::utils::time::snapmgr::TransactionXmin }
#[no_mangle] pub unsafe extern "C" fn InvalidAttrNumber() -> AttrNumber { 0 }
#[no_mangle] pub unsafe extern "C" fn RelationGetForm(a0: *mut c_void) -> *mut c_void { crate::utils::rel::RelationGetForm(a0 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn RelationGetRelid(a0: *mut c_void) -> Oid { crate::utils::rel::RelationGetRelid(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn RELKIND_HAS_TABLE_AM(a0: i8) -> bool { crate::utils::cache::relcache::RELKIND_HAS_TABLE_AM(a0) }
#[no_mangle] pub unsafe extern "C" fn table_relation_estimate_size(rel: *mut c_void, attr_widths: *mut i32, pages: *mut u32, tuples: *mut f64, allvisfrac: *mut f64) {
    let r = rel as crate::utils::rel::Relation;
    let am = (*r).rd_tableam as *const crate::access::table::tableam::TableAmRoutine;
    if std::env::var("PDB_BT").is_ok() {
        eprintln!("PDB_BT tres rd_tableam={:p} am={:p} est_some={}", (*r).rd_tableam, am,
            if am.is_null() { false } else { (*am).relation_estimate_size.is_some() });
    }
    ((*am).relation_estimate_size.unwrap())(r, attr_widths, pages, tuples, allvisfrac);
}

// ==== batch49 (plancat stats/proc reachability) ====
#[no_mangle] pub unsafe extern "C" fn BoolGetDatum(a0: bool) -> u64 { crate::postgres::BoolGetDatum(a0) as u64 }
#[no_mangle] pub unsafe extern "C" fn GETSTRUCT(a0: *mut c_void) -> *mut c_void { crate::access::htup_details::GETSTRUCT(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn HeapTupleIsValid(a0: *mut c_void) -> bool { crate::access::htup_details::HeapTupleIsValid(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn SearchSysCache2(a0: c_int, a1: u64, a2: u64) -> *mut c_void { crate::utils::cache::syscache::SearchSysCache2(a0, a1 as _, a2 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn TextDatumGetCString(a0: u64) -> *mut c_char { crate::utils::builtins::TextDatumGetCString(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn fix_opfuncids(a0: *mut c_void) { crate::nodes::nodeFuncs::fix_opfuncids(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn statext_is_kind_built(a0: *mut c_void, a1: u8) -> bool { crate::statistics::extended_stats::statext_is_kind_built(a0 as _, a1 as _) }
#[no_mangle] pub unsafe extern "C" fn stringToNode(a0: *const c_char) -> *mut c_void { crate::nodes::read::stringToNode(a0) }

// ==== batch50 (get_rel_data_width reachability) ====
#[no_mangle] pub unsafe extern "C" fn clamp_width_est(a0: i64) -> i32 { crate::optimizer::optimizer::clamp_width_est(a0) }
#[no_mangle] pub unsafe extern "C" fn get_attavgwidth(a0: Oid, a1: c_int) -> i32 { crate::utils::cache::lsyscache::get_attavgwidth(a0, a1 as i16) }
#[no_mangle] pub unsafe extern "C" fn get_typavgwidth(a0: Oid, a1: i32) -> i32 { crate::utils::cache::lsyscache::get_typavgwidth(a0, a1) }

// ==== batch51 (dbcommands/createdb reachability) ====
#[no_mangle] pub static DatabaseRelationId: Oid = 1262;
#[no_mangle] pub static DatabaseNameIndexId: Oid = 2671;
#[no_mangle] pub unsafe extern "C" fn CStringGetDatum(a0: *const c_char) -> usize { crate::postgres::CStringGetDatum(a0) as _ }
#[no_mangle] pub unsafe extern "C" fn check_can_set_role(a0: Oid, a1: Oid) { crate::utils::adt::acl::check_can_set_role(a0, a1) }
#[no_mangle] pub unsafe extern "C" fn ScanKeyInit(a0: *mut c_void, a1: c_int, a2: c_int, a3: Oid, a4: usize) { crate::access::common::scankey::ScanKeyInit(a0 as _, a1 as _, a2 as _, a3 as _, a4 as _) }
#[no_mangle] pub unsafe extern "C" fn systable_beginscan(a0: *mut c_void, a1: Oid, a2: bool, a3: *mut c_void, a4: c_int, a5: *mut c_void) -> *mut c_void { crate::access::index::genam::systable_beginscan(a0 as _, a1, a2, a3 as _, a4, a5 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn systable_getnext(a0: *mut c_void) -> *mut c_void { crate::access::index::genam::systable_getnext(a0 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn systable_endscan(a0: *mut c_void) { crate::access::index::genam::systable_endscan(a0 as _) }

// ==== batch52 (createdb defGet/role) ====
#[no_mangle] pub static FirstNormalObjectId: Oid = 16384;
#[no_mangle] pub unsafe extern "C" fn defGetBoolean(a0: *mut c_void) -> bool { crate::commands::define::defGetBoolean(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn defGetObjectId(a0: *mut c_void) -> Oid { crate::commands::define::defGetObjectId(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn defGetString(a0: *mut c_void) -> *mut c_char { crate::commands::define::defGetString(a0 as _) }
#[no_mangle] pub unsafe extern "C" fn errorConflictingDefElem(a0: *mut c_void, a1: *mut c_void) { crate::commands::define::errorConflictingDefElem(a0 as _, a1 as _) }
#[no_mangle] pub unsafe extern "C" fn get_role_oid(a0: *const c_char, a1: bool) -> Oid { crate::utils::adt::acl::get_role_oid(a0, a1) }

// ==== batch53 (createdb shared-object locks + NameStr) ====
#[no_mangle] pub unsafe extern "C" fn LockSharedObject(a0: Oid, a1: Oid, a2: u16, a3: c_int) { crate::catalog::objectaddress_impl::LockSharedObject(a0, a1, a2, a3) }
#[no_mangle] pub unsafe extern "C" fn UnlockSharedObject(a0: Oid, a1: Oid, a2: u16, a3: c_int) { crate::catalog::objectaddress_impl::UnlockSharedObject(a0, a1, a2, a3) }
#[no_mangle] pub unsafe extern "C" fn NameStr(a0: *const c_void) -> *const c_char { a0 as *const c_char }

// ==== batch54 (CREATE DATABASE / dbcommands.rs) ====
// Statics
#[no_mangle] pub static InvalidOid: Oid = 0;
#[no_mangle] pub static TableSpaceRelationId: Oid = 1213;
#[no_mangle] pub static GLOBALTABLESPACE_OID: Oid = 1664;
#[no_mangle] pub static DatabaseOidIndexId: Oid = 2672;
// Datum constructors
#[no_mangle] pub unsafe extern "C" fn CharGetDatum(a0: c_char) -> usize { crate::postgres::CharGetDatum(a0) as _ }
#[no_mangle] pub unsafe extern "C" fn Int32GetDatum(a0: c_int) -> usize { crate::postgres::Int32GetDatum(a0) as _ }
#[no_mangle] pub unsafe extern "C" fn TransactionIdGetDatum(a0: u32) -> usize { crate::postgres::TransactionIdGetDatum(a0) as _ }
#[no_mangle] pub unsafe extern "C" fn PointerGetDatum(a0: *const c_void) -> usize { crate::postgres::PointerGetDatum(a0) as _ }
#[no_mangle] pub unsafe extern "C" fn CStringGetTextDatum(a0: *const c_char) -> usize { crate::utils::builtins::CStringGetTextDatum(a0) as _ }
// fmgr / name
#[no_mangle] pub unsafe extern "C" fn DirectFunctionCall1(a0: unsafe extern "C" fn(usize) -> usize, a1: usize) -> usize { crate::utils::fmgr::DirectFunctionCall1Coll(core::mem::transmute(a0), 0, a1 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn namein(a0: usize) -> usize { crate::utils::adt::name::namein(a0 as _) as _ }
// catalog / heap
#[no_mangle] pub unsafe extern "C" fn CatalogTupleInsert(a0: *mut c_void, a1: *mut c_void) -> Oid { crate::catalog::indexing::CatalogTupleInsert(a0 as _, a1 as _); 0 }
#[no_mangle] pub unsafe extern "C" fn heap_form_tuple(a0: *mut c_void, a1: *mut usize, a2: *mut bool) -> *mut c_void { crate::access::common::heaptuple::heap_form_tuple(a0 as _, a1 as _, a2 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn GetNewOidWithIndex(a0: *mut c_void, a1: Oid, a2: c_int) -> Oid { crate::catalog::catalog::GetNewOidWithIndex(a0 as _, a1, a2 as _) }
// table scan
#[no_mangle] pub unsafe extern "C" fn table_beginscan_catalog(a0: *mut c_void, a1: c_int, a2: *const c_void) -> *mut c_void { crate::access::table::tableam::table_beginscan_catalog(a0 as _, a1, a2 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn heap_getnext(a0: *mut c_void, a1: c_int) -> *mut c_void { crate::access::heap::heapam::heap_getnext(a0 as _, a1 as _) as _ }
#[no_mangle] pub unsafe extern "C" fn table_endscan(a0: *mut c_void) { crate::access::table::tableam::table_endscan(a0 as _) }
// dependency / hooks
#[no_mangle] pub unsafe extern "C" fn recordDependencyOnOwner(a0: Oid, a1: Oid, a2: Oid) { crate::catalog::pg_shdepend::recordDependencyOnOwner(a0, a1, a2) }
#[no_mangle] pub unsafe extern "C" fn copyTemplateDependencies(a0: Oid, a1: Oid) { crate::catalog::pg_shdepend::copyTemplateDependencies(a0, a1) }
#[no_mangle] pub unsafe extern "C" fn InvokeObjectPostCreateHook(_a0: Oid, _a1: Oid, _a2: c_int) {}
// process / checkpoint / xact
#[no_mangle] pub unsafe extern "C" fn CountOtherDBBackends(a0: Oid, a1: *mut c_int, a2: *mut c_int) -> bool { crate::storage::ipc::procarray::CountOtherDBBackends(a0, a1, a2) }
#[no_mangle] pub unsafe extern "C" fn RequestCheckpoint(a0: c_int) { crate::postmaster::checkpointer::RequestCheckpoint(a0) }
#[no_mangle] pub unsafe extern "C" fn ForceSyncCommit() { crate::access::transam::xact::ForceSyncCommit() }
// fs / path
#[no_mangle] pub unsafe extern "C" fn MakePGDirectory(a0: *const c_char) -> c_int { crate::storage::file::fd::MakePGDirectory(a0) }
#[no_mangle] pub unsafe extern "C" fn GetDatabasePath(a0: Oid, a1: Oid) -> *mut c_char { crate::common::relpath::GetDatabasePath(a0, a1) }
// tablespace
#[no_mangle] pub unsafe extern "C" fn get_tablespace_oid(_a0: *const c_char, _a1: bool) -> Oid { unimplemented!("get_tablespace_oid not ported (commands::tablespace unwired)") }
// encoding
#[no_mangle] pub unsafe extern "C" fn pg_encoding_to_char(a0: c_int) -> *const c_char { crate::common::encnames::pg_encoding_to_char(a0) }
#[no_mangle] pub unsafe extern "C" fn pg_get_encoding_from_locale(a0: *const c_char, a1: bool) -> c_int { crate::port::port_api::pg_get_encoding_from_locale(a0, a1) }
#[no_mangle] pub unsafe extern "C" fn PG_VALID_BE_ENCODING(a0: c_int) -> bool { crate::mb::pg_wchar::PG_VALID_BE_ENCODING(a0) }
// locale / ICU
#[no_mangle] pub unsafe extern "C" fn check_locale(a0: c_int, a1: *const c_char, a2: *mut *mut c_char) -> bool { crate::utils::adt::pg_locale::check_locale(a0, a1, a2) }
#[no_mangle] pub unsafe extern "C" fn builtin_validate_locale(a0: c_int, a1: *const c_char) -> *const c_char { crate::utils::adt::pg_locale::builtin_validate_locale(a0, a1) }
#[no_mangle] pub unsafe extern "C" fn icu_validate_locale(a0: *const c_char) { crate::utils::adt::pg_locale::icu_validate_locale(a0) }
#[no_mangle] pub unsafe extern "C" fn icu_language_tag(a0: *const c_char, a1: c_int) -> *mut c_char { crate::utils::adt::pg_locale::icu_language_tag(a0, a1) }
#[no_mangle] pub unsafe extern "C" fn get_collation_actual_version(a0: c_char, a1: *const c_char) -> *mut c_char { crate::utils::adt::pg_locale::get_collation_actual_version(a0, a1) }
#[no_mangle] pub unsafe extern "C" fn collprovider_name(_a0: c_char) -> *const c_char { unimplemented!("collprovider_name not ported") }

// =====================================================================
// CREATE DATABASE WAL-log copy path (commands::dbcommands)
// CreateDatabaseUsingWalLog / CreateAndCopyRelationData symbols.
// Pointer args normalized to c_void; RelFileLocator passed by value uses
// the canonical (ABI-identical {spcOid,dbOid,relNumber}) struct.
// =====================================================================
use crate::storage::relfilelocator::RelFileLocator as PdbRelFileLocator;

// ---- relmapper ----
#[no_mangle] pub unsafe extern "C" fn RelationMapCopy(dbid: Oid, tsid: Oid, srcpath: *const c_char, dstpath: *const c_char) { crate::utils::cache::relmapper::RelationMapCopy(dbid, tsid, srcpath as *mut c_char, dstpath as *mut c_char) }
#[no_mangle] pub unsafe extern "C" fn RelationMapOidToFilenumberForDatabase(srcpath: *const c_char, relid: Oid) -> Oid { crate::utils::cache::relmapper::RelationMapOidToFilenumberForDatabase(srcpath as *mut c_char, relid) as _ }

// ---- lmgr (LockRelId) ----
#[no_mangle] pub unsafe extern "C" fn LockRelationId(relid: *mut c_void, lockmode: LOCKMODE) { crate::storage::lmgr::lmgr::LockRelationId(relid as _, lockmode) }
#[no_mangle] pub unsafe extern "C" fn UnlockRelationId(relid: *mut c_void, lockmode: LOCKMODE) { crate::storage::lmgr::lmgr::UnlockRelationId(relid as _, lockmode) }

// ---- smgr ----
#[no_mangle] pub unsafe extern "C" fn smgropen(rlocator: PdbRelFileLocator, backend: c_int) -> *mut c_void { crate::storage::smgr::smgr::smgropen(rlocator, backend as _) as _ }
#[no_mangle] pub unsafe extern "C" fn smgrnblocks(smgr: *mut c_void, forknum: c_int) -> u32 { crate::storage::smgr::smgr::smgrnblocks(smgr as _, forknum as _) }
#[no_mangle] pub unsafe extern "C" fn smgrclose(smgr: *mut c_void) { crate::storage::smgr::smgr::smgrclose(smgr as _) }

// ---- snapmgr ----
#[no_mangle] pub unsafe extern "C" fn RegisterSnapshot(snapshot: *mut c_void) -> *mut c_void { crate::utils::time::snapmgr::RegisterSnapshot(snapshot as _) as _ }
#[no_mangle] pub unsafe extern "C" fn UnregisterSnapshot(snapshot: *mut c_void) { crate::utils::time::snapmgr::UnregisterSnapshot(snapshot as _) }
#[no_mangle] pub unsafe extern "C" fn GetLatestSnapshot() -> *mut c_void { crate::utils::time::snapmgr::GetLatestSnapshot() as _ }

// ---- bufmgr ----
#[no_mangle] pub unsafe extern "C" fn ReadBufferWithoutRelcache(rlocator: PdbRelFileLocator, forknum: c_int, blocknum: u32, mode: c_int, strategy: *mut c_void, permanent: bool) -> i32 { crate::storage::buffer::bufmgr::ReadBufferWithoutRelcache(rlocator, forknum as _, blocknum, mode as _, strategy as _, permanent) }
#[no_mangle] pub unsafe extern "C" fn LockBuffer(buf: i32, mode: c_int) { crate::storage::buffer::bufmgr::LockBuffer(buf, mode) }
#[no_mangle] pub unsafe extern "C" fn UnlockReleaseBuffer(buf: i32) { crate::storage::buffer::bufmgr::UnlockReleaseBuffer(buf) }
#[no_mangle] pub unsafe extern "C" fn CreateAndCopyRelationData(src: PdbRelFileLocator, dst: PdbRelFileLocator, permanent: bool) { crate::storage::buffer::bufmgr::CreateAndCopyRelationData(src, dst, permanent) }

// ---- bufpage / page accessors ----
#[no_mangle] pub unsafe extern "C" fn PageIsEmpty(page: *mut u8) -> bool { crate::storage::bufpage::PageIsEmpty(page as *const c_char) }
#[no_mangle] pub unsafe extern "C" fn PageGetMaxOffsetNumber(page: *mut u8) -> u16 { crate::storage::bufpage::PageGetMaxOffsetNumber(page as *const c_char) }
#[no_mangle] pub unsafe extern "C" fn PageGetItemId(page: *mut u8, offnum: u16) -> *mut u8 { crate::storage::bufpage::PageGetItemId(page as _, offnum) as _ }
#[no_mangle] pub unsafe extern "C" fn PageGetItem(page: *mut u8, itemid: *const u8) -> *mut u8 { crate::storage::bufpage::PageGetItem(page as *const c_char, itemid as _) as _ }

// ---- itemid accessors ----
#[no_mangle] pub unsafe extern "C" fn ItemIdGetLength(itemid: *const u8) -> u32 { crate::storage::itemid::ItemIdGetLength(itemid as _) }
#[no_mangle] pub unsafe extern "C" fn ItemIdIsUsed(itemid: *const u8) -> bool { crate::storage::itemid::ItemIdIsUsed(itemid as _) }
#[no_mangle] pub unsafe extern "C" fn ItemIdIsDead(itemid: *const u8) -> bool { crate::storage::itemid::ItemIdIsDead(itemid as _) }
#[no_mangle] pub unsafe extern "C" fn ItemIdIsRedirected(itemid: *const u8) -> bool { crate::storage::itemid::ItemIdIsRedirected(itemid as _) }

// ---- itemptr ----
#[no_mangle] pub unsafe extern "C" fn ItemPointerSet(pointer: *mut c_void, blockno: u32, off: u16) { crate::storage::itemptr::ItemPointerSet(pointer as _, blockno, off) }

// ---- heap visibility ----
#[no_mangle] pub unsafe extern "C" fn HeapTupleSatisfiesVisibility(tuple: *mut c_void, snapshot: *mut c_void, buf: i32) -> bool { crate::access::heap::heapam_visibility::HeapTupleSatisfiesVisibility(tuple as _, snapshot as _, buf) }

// ---- pg_list ----
#[no_mangle] pub unsafe extern "C" fn list_free_deep(list: *mut c_void) { crate::nodes::pg_list::list_free_deep(list as _) }

// ---- fd (transient files / fsync) ----
#[no_mangle] pub unsafe extern "C" fn OpenTransientFile(path: *const c_char, flags: c_int) -> c_int { crate::storage::file::fd::OpenTransientFile(path, flags) }
#[no_mangle] pub unsafe extern "C" fn CloseTransientFile(fd: c_int) -> c_int { crate::storage::file::fd::CloseTransientFile(fd) }
#[no_mangle] pub unsafe extern "C" fn pg_fsync(fd: c_int) -> c_int { crate::storage::file::fd::pg_fsync(fd) }
#[no_mangle] pub unsafe extern "C" fn fsync_fname(fname: *const c_char, isdir: bool) { crate::storage::file::fd::fsync_fname(fname, isdir) }
#[no_mangle] pub unsafe extern "C" fn data_sync_elevel(elevel: c_int) -> c_int { crate::storage::file::fd::data_sync_elevel(elevel) }

// ---- pgstat wait events (set *my_wait_event_info) ----
#[no_mangle] pub unsafe extern "C" fn pgstat_report_wait_start(event: u32) { *crate::utils::activity::wait_event::my_wait_event_info = event }
#[no_mangle] pub unsafe extern "C" fn pgstat_report_wait_end() { *crate::utils::activity::wait_event::my_wait_event_info = 0 }

// ---- crit sections (CritSectionCount via miscadmin) ----
#[no_mangle] pub unsafe extern "C" fn START_CRIT_SECTION() { crate::miscadmin::START_CRIT_SECTION() }
#[no_mangle] pub unsafe extern "C" fn END_CRIT_SECTION() { crate::miscadmin::END_CRIT_SECTION() }

// ---- xloginsert ----
#[no_mangle] pub unsafe extern "C" fn XLogBeginInsert() { crate::access::transam::xloginsert::XLogBeginInsert() }
#[no_mangle] pub unsafe extern "C" fn XLogRegisterData(data: *const c_void, len: usize) { crate::access::transam::xloginsert::XLogRegisterData(data, len as _) }
#[no_mangle] pub unsafe extern "C" fn XLogInsert(rmid: u8, info: u8) -> u64 { crate::access::transam::xloginsert::XLogInsert(rmid as _, info) }

// ---- statics ----
#[no_mangle] pub static RelationRelationId: Oid = 1259;
#[no_mangle] pub static io_combine_limit: c_int = 16;

// ==== batch55 (AlterDatabaseSet) ====
#[no_mangle] pub unsafe extern "C" fn AlterSetting(a0: Oid, a1: Oid, a2: *mut c_void) { crate::catalog::pg_db_role_setting::AlterSetting(a0, a1, a2 as _) }
#[no_mangle] pub unsafe extern "C" fn shdepLockAndCheckObject(a0: Oid, a1: Oid) { crate::catalog::pg_shdepend::shdepLockAndCheckObject(a0, a1) }

#[no_mangle] pub unsafe extern "C" fn makeBoolConst(value: bool, isnull: bool) -> *mut c_void {
    crate::nodes::makefuncs::makeBoolConst(value, isnull) as *mut c_void
}

#[no_mangle] pub unsafe extern "C" fn IsTransactionState() -> bool {
    crate::access::transam::xact::IsTransactionState()
}
#[no_mangle] pub unsafe extern "C" fn SetTempTablespaces(table_spaces: *mut Oid, num_spaces: c_int) {
    crate::storage::file::fd::SetTempTablespaces(table_spaces as _, num_spaces as _)
}
#[no_mangle] pub unsafe extern "C" fn TempTablespacesAreSet() -> bool {
    crate::storage::file::fd::TempTablespacesAreSet()
}

/// libc-equivalent helpers used by utils/adt/varlena.rs (declared there as
/// extern "C" memcpy_v/memcmp_v/memset_v/strncmp_v to avoid clashing with the
/// libc symbols the backend stubs). Defined here once for the whole crate.
#[no_mangle]
pub unsafe extern "C" fn memcpy_v(dst: *mut c_void, src: *const c_void, n: usize) -> *mut c_void {
    core::ptr::copy(src as *const u8, dst as *mut u8, n);
    dst
}
#[no_mangle]
pub unsafe extern "C" fn memset_v(s: *mut c_void, c: c_int, n: usize) -> *mut c_void {
    core::ptr::write_bytes(s as *mut u8, c as u8, n);
    s
}
#[no_mangle]
pub unsafe extern "C" fn memcmp_v(a: *const c_void, b: *const c_void, n: usize) -> c_int {
    let (a, b) = (a as *const u8, b as *const u8);
    let mut i = 0usize;
    while i < n {
        let (x, y) = (*a.add(i), *b.add(i));
        if x != y {
            return x as c_int - y as c_int;
        }
        i += 1;
    }
    0
}
#[no_mangle]
pub unsafe extern "C" fn strncmp_v(a: *const c_char, b: *const c_char, n: usize) -> c_int {
    let mut i = 0usize;
    while i < n {
        let (x, y) = (*a.add(i) as u8, *b.add(i) as u8);
        if x != y {
            return x as c_int - y as c_int;
        }
        if x == 0 {
            return 0;
        }
        i += 1;
    }
    0
}
