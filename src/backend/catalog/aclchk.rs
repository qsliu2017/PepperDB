//! Privilege (GRANT/REVOKE) machinery. Translated from
//! `src/backend/catalog/aclchk.c` (disposition: grow).
//!
//! `execute_grant_stmt` resolves the named objects + grantees, computes the new ACL
//! per object, and updates the object's ACL column (pg_class.relacl for tables). The
//! ACL is the standard PG representation: a varlena 1-D array of `AclItem`
//! (`{grantee, grantor, privs}`); `merge_acl_with_grant`/`aclupdate` merge the grant
//! (or revoke) into the existing ACL.
//!
//! M10 reach (rules.md s4): GRANT/REVOKE of table privileges (SELECT/INSERT/UPDATE/
//! DELETE/TRUNCATE/REFERENCES/TRIGGER/MAINTAIN, ALL), TO/FROM PUBLIC and the
//! bootstrap superuser, WITH GRANT OPTION. The catalog write (relacl reflects the
//! grant + persists) is the deliverable; named non-bootstrap roles STAGE (pg_authid
//! is not seeded with arbitrary roles yet), as do schema/sequence/function objects,
//! per-column privileges, grantor selection, and query-time enforcement.

use std::sync::Arc;

use crate::access::htup::HeapTupleData;
use crate::access::skey::ScanKeyData;
use crate::backend::access::common::heaptuple::{
    heap_deform_tuple, heap_form_tuple, heap_freetuple,
};
use crate::backend::access::index::genam::{
    systable_beginscan, systable_endscan, systable_getnext, systable_scan_snapshot,
};
use crate::backend::catalog::indexing::catalog_tuple_update;
use crate::backend::utils::cache::relcache::{
    relation_close, relation_forget_relation, relation_id_get_relation,
};
use crate::catalog::pg_authid::BOOTSTRAP_SUPERUSERID;
use crate::catalog::pg_class::{self as c, RelationRelationId};
use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{
    AclMode, GrantStmt, GrantTargetType, ObjectType, RoleSpec, RoleSpecType,
};
use crate::nodes::primnodes::RangeVar;
use crate::postgres::{Datum, DatumGetPointer, ObjectIdGetDatum, PointerGetDatum};
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::storage::itemptr::ItemPointerData;
use crate::utils::acl::{
    aclitem_get_goptions, aclitem_get_privs, AclItem, ACL_ALL_RIGHTS_RELATION, ACL_ID_PUBLIC,
};

/// The `aclitem` element type OID (pg_type.dat). The relacl/nspacl arrays carry it.
const ACLITEMOID: Oid = Oid::new(1033);

fn zero_fmgr_info() -> crate::fmgr::FmgrInfo {
    crate::fmgr::FmgrInfo {
        fn_addr: None,
        oid: InvalidOid,
        nargs: 0,
        strict: false,
        retset: false,
        stats: 0,
        extra: 0,
        mcxt: (),
        expr: None,
    }
}

/// PG `ExecuteGrantStmt`: GRANT/REVOKE on objects. M10 reaches the TABLE object
/// kind; other targets STAGE.
pub async fn execute_grant_stmt(shared: &Arc<SharedState>, stmt: &GrantStmt) {
    if stmt.targtype != GrantTargetType::OBJECT {
        unimplemented!("ExecuteGrantStmt: target type {:?} not yet reachable", stmt.targtype);
    }
    if stmt.objtype != ObjectType::TABLE {
        unimplemented!("ExecuteGrantStmt: object type {:?} not yet reachable", stmt.objtype);
    }

    let privileges = parse_privileges(stmt, ACL_ALL_RIGHTS_RELATION);
    let grantees = parse_grantees(&stmt.grantees);
    let grantor = crate::backend::utils::init::miscinit::get_user_id();

    for obj in &stmt.objects {
        let relid = resolve_table_object(shared, obj).await;
        exec_grant_relation(shared, relid, stmt.is_grant, stmt.grant_option, grantor, &grantees, privileges)
            .await;
    }
}

/// Resolve one GRANT object (a `RangeVar` naming a table) to its relation OID.
async fn resolve_table_object(shared: &Arc<SharedState>, obj: &Node) -> Oid {
    let rv: &RangeVar = match obj {
        Node::RangeVar(rv) => rv,
        _ => unreachable!("GRANT ON TABLE object is a RangeVar"),
    };
    let addr = crate::backend::catalog::objectaddress::get_object_address(
        shared,
        ObjectType::TABLE,
        rv,
        false,
    )
    .await;
    addr.objectId
}

/// PG `ExecGrant_Relation` (M10 subset): merge the grant/revoke into the relation's
/// pg_class.relacl and write it back. The default (NULL relacl -> owner's full
/// rights) is materialized before the first GRANT, matching PG's `acldefault`.
async fn exec_grant_relation(
    shared: &Arc<SharedState>,
    relid: Oid,
    is_grant: bool,
    grant_option: bool,
    grantor: Oid,
    grantees: &[Oid],
    privileges: AclMode,
) {
    let rows = scan_pg_class_by_oid(shared, relid).await;
    let Some(pg_class) = relation_id_get_relation(RelationRelationId) else {
        unreachable!("pg_class is seeded on-disk");
    };
    let desc = pg_class.rd_att.clone().unwrap_or_else(|| unreachable!("pg_class desc"));

    for row in rows {
        // SAFETY: owned tuple + matching descriptor.
        let (mut vals, mut nulls) = unsafe { heap_deform_tuple(&row.tuple, &desc) };
        let acl_idx = (c::Anum_pg_class_relacl - 1) as usize;
        let owner = owner_of(&vals, &nulls);

        // Old ACL: the stored array, or the owner's default if NULL.
        let mut acl = if nulls[acl_idx] {
            acl_default_relation(owner)
        } else {
            // SAFETY: relacl is a live, non-toasted aclitem[] varlena.
            let p = DatumGetPointer(vals[acl_idx]);
            unsafe { read_acl(p) }
        };

        // Merge each (grantee) grant/revoke into the working ACL.
        for &grantee in grantees {
            acl = aclupdate(&acl, grantee, grantor, privileges, grant_option, is_grant, owner);
        }

        let acl_datum = PointerGetDatum(build_acl(&acl).cast::<u8>());
        vals[acl_idx] = acl_datum;
        nulls[acl_idx] = false;

        let mut newtup = heap_form_tuple(&desc, &vals, &nulls);
        catalog_tuple_update(shared, &pg_class, &row.tid, &mut newtup).await;
        heap_freetuple(newtup);
        heap_freetuple(row.tuple);
    }

    relation_close(pg_class);
    relation_forget_relation(relid);
}

/// The relation owner from a deformed pg_class tuple (relowner), or the bootstrap
/// superuser if somehow NULL (it never is).
fn owner_of(vals: &[Datum], nulls: &[bool]) -> Oid {
    let i = (c::Anum_pg_class_relowner - 1) as usize;
    if nulls[i] {
        BOOTSTRAP_SUPERUSERID
    } else {
        crate::postgres::DatumGetObjectId(vals[i])
    }
}

/// PG `acldefault(OBJECT_TABLE, owner)` (the M10 form): the owner holds all relation
/// rights with grant option. PUBLIC gets nothing by default for relations.
fn acl_default_relation(owner: Oid) -> Vec<AclItem> {
    let mut item = AclItem { grantee: owner, grantor: owner, privs: AclMode::empty() };
    crate::utils::acl::aclitem_set_privs_goptions(
        &mut item,
        ACL_ALL_RIGHTS_RELATION,
        ACL_ALL_RIGHTS_RELATION,
    );
    vec![item]
}

/// PG `aclupdate` (the in-memory merge over a `Vec<AclItem>`): apply a grant or
/// revoke of `privileges` (optionally WITH GRANT OPTION) from `grantor` to
/// `grantee`. On GRANT, OR the privilege bits (and the grant-option bits when
/// `grant_option`) into the matching `(grantee, grantor)` item, creating it if
/// absent. On REVOKE, clear those bits; when `grant_option` only the grant-option
/// bits are removed (the privilege itself stays). Items that drop to no rights are
/// removed (except the owner's self-grant is left as-is by the caller's default).
fn aclupdate(
    old: &[AclItem],
    grantee: Oid,
    grantor: Oid,
    privileges: AclMode,
    grant_option: bool,
    is_grant: bool,
    _owner: Oid,
) -> Vec<AclItem> {
    let mut acl: Vec<AclItem> = old.to_vec();

    let pos = acl.iter().position(|it| it.grantee == grantee && it.grantor == grantor);
    let idx = if let Some(i) = pos {
        i
    } else {
        // No matching item: a REVOKE has nothing to do; a GRANT creates one.
        if !is_grant {
            return acl;
        }
        acl.push(AclItem { grantee, grantor, privs: AclMode::empty() });
        acl.len() - 1
    };

    let cur_privs = aclitem_get_privs(acl[idx]);
    let cur_gopts = aclitem_get_goptions(acl[idx]);

    let (new_privs, new_gopts) = if is_grant {
        let privs = cur_privs | privileges;
        let gopts = if grant_option { cur_gopts | privileges } else { cur_gopts };
        (privs, gopts)
    } else if grant_option {
        // REVOKE GRANT OPTION FOR: drop only the grant-option bits.
        (cur_privs, cur_gopts & !privileges)
    } else {
        // REVOKE: drop both the privilege and its grant-option bits.
        (cur_privs & !privileges, cur_gopts & !privileges)
    };

    crate::utils::acl::aclitem_set_privs_goptions(&mut acl[idx], new_privs, new_gopts);

    // Drop an item that has been fully revoked (no privs and no grant options).
    if new_privs.is_empty() && new_gopts.is_empty() {
        acl.remove(idx);
    }
    acl
}

/// Parse the statement's privilege list into a single `AclMode` bitmask. An empty
/// privilege name (PG's "ALL PRIVILEGES") expands to `all_rights` for the object.
fn parse_privileges(stmt: &GrantStmt, all_rights: AclMode) -> AclMode {
    if stmt.privileges.is_empty() {
        return all_rights;
    }
    let mut mask = AclMode::empty();
    for p in &stmt.privileges {
        // make_access_priv carries the privilege name as a String_ node; "" = ALL.
        let name = match p {
            Node::String_(s) => s.sval.as_str(),
            _ => unreachable!("GRANT privilege is a String_ node"),
        };
        if name.is_empty() {
            return all_rights;
        }
        mask |= privilege_bit(name);
    }
    mask
}

/// Map a privilege keyword to its `AclMode` bit (the relation privileges).
fn privilege_bit(name: &str) -> AclMode {
    match name.to_ascii_lowercase().as_str() {
        "select" => AclMode::SELECT,
        "insert" => AclMode::INSERT,
        "update" => AclMode::UPDATE,
        "delete" => AclMode::DELETE,
        "truncate" => AclMode::TRUNCATE,
        "references" => AclMode::REFERENCES,
        "trigger" => AclMode::TRIGGER,
        "maintain" => AclMode::MAINTAIN,
        other => {
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_SYNTAX_ERROR)
                    .errmsg(format!("unrecognized privilege type \"{other}\""));
            });
            unreachable!("ereport(ERROR) diverges");
        }
    }
}

/// Resolve the grantee role specs to OIDs. PUBLIC -> `ACL_ID_PUBLIC` (0); a named
/// role -> its pg_authid OID (the bootstrap superuser is recognized directly;
/// arbitrary named roles STAGE until pg_authid carries them).
fn parse_grantees(grantees: &[Node]) -> Vec<Oid> {
    grantees
        .iter()
        .map(|g| {
            let rs: &RoleSpec = match g {
                Node::RoleSpec(rs) => rs,
                _ => unreachable!("GRANT grantee is a RoleSpec node"),
            };
            grantee_oid(rs)
        })
        .collect()
}

/// One grantee `RoleSpec` -> OID. PUBLIC and the special CURRENT/SESSION user map to
/// known OIDs; a named role resolves the bootstrap superuser by name, else STAGES.
fn grantee_oid(rs: &RoleSpec) -> Oid {
    match rs.roletype {
        RoleSpecType::PUBLIC => ACL_ID_PUBLIC,
        RoleSpecType::CURRENT_ROLE | RoleSpecType::CURRENT_USER => {
            crate::backend::utils::init::miscinit::get_user_id()
        }
        RoleSpecType::SESSION_USER => crate::backend::utils::init::miscinit::get_session_user_id(),
        RoleSpecType::CSTRING => {
            let name = rs.rolename.as_deref().unwrap_or_else(|| unreachable!("named role"));
            if name.eq_ignore_ascii_case("public") {
                ACL_ID_PUBLIC
            } else if name == "postgres" {
                BOOTSTRAP_SUPERUSERID
            } else {
                // STAGED (rules.md s4): pg_authid does not carry arbitrary named
                // roles yet, so a named grantee beyond the bootstrap role has no OID.
                unimplemented!("ExecuteGrantStmt: named role \"{name}\" not yet seeded in pg_authid");
            }
        }
    }
}

/// One pg_class row copied out of a scan: owned tuple + its on-disk TID.
struct ClassRow {
    tuple: HeapTupleData,
    tid: ItemPointerData,
}

/// Scan pg_class for the row whose oid == `relid`, returning it as an owned
/// `(tuple, tid)`. Fully drained before returning so the write crosses no live scan.
async fn scan_pg_class_by_oid(shared: &Arc<SharedState>, relid: Oid) -> Vec<ClassRow> {
    let Some(pg_class) = relation_id_get_relation(RelationRelationId) else {
        return Vec::new();
    };
    let key = [ScanKeyData {
        flags: 0,
        attno: c::Anum_pg_class_oid as i16,
        strategy: crate::access::stratnum::BT_EQUAL_STRATEGY_NUMBER,
        subtype: InvalidOid,
        collation: InvalidOid,
        func: zero_fmgr_info(),
        argument: ObjectIdGetDatum(relid),
    }];
    let snap = systable_scan_snapshot(shared, &pg_class, None);
    let mut scan = systable_beginscan(shared, &pg_class, c::ClassOidIndexId, false, &snap, &key);
    let mut rows = Vec::new();
    while let Some(tref) = Box::pin(systable_getnext(shared, &mut scan)).await {
        // SAFETY: live scan tuple; copy (with TID) before endscan.
        let tuple = unsafe { crate::backend::access::common::heaptuple::heap_copytuple(tref) };
        rows.push(ClassRow { tid: tuple.t_self, tuple });
    }
    systable_endscan(shared, &mut scan);
    relation_close(pg_class);
    rows
}

// ---------------------------------------------------------------------------
//  Acl varlena (de)serialization. An Acl is a 1-D varlena array of fixed-length
//  16-byte AclItem elements (`i` alignment, not pass-by-value). construct/deconstruct
//  array are stubs in utils/array.rs (rules.md s4), so the relacl array is built /
//  read directly via the ARR_* layout (no nulls: a 1-D no-null array).
// ---------------------------------------------------------------------------

/// Build an `Acl` varlena (1-D, no nulls) from the AclItem slice. Layout matches PG
/// `construct_array(ACLITEMOID, ...)`: header + dims[1] + lbound[1] + MAXALIGNed,
/// then the packed elements. Returns a leaked palloc-equivalent buffer pointer.
#[allow(
    clippy::cast_ptr_alignment,
    clippy::manual_slice_size_calculation,
    reason = "faithful PG on-disk varlena array layout; buffer is over-aligned (Vec<u8>) and writes land at the MAXALIGNed offsets"
)]
fn build_acl(items: &[AclItem]) -> *mut u8 {
    use crate::c::MAXALIGN;
    let nitems = items.len();
    let header = crate::utils::array::ArrayType::arr_overhead_nonulls(1);
    let elem_sz = core::mem::size_of::<AclItem>();
    debug_assert_eq!(MAXALIGN(elem_sz), elem_sz, "AclItem is 16-byte aligned");
    let total = header + elem_sz * nitems;

    let mut buf = vec![0u8; total].into_boxed_slice();
    let base = buf.as_mut_ptr();
    // SAFETY: `base` heads a freshly-allocated `total`-byte buffer; the writes stay
    // within it (header, then `nitems` packed AclItem at the MAXALIGNed data offset).
    unsafe {
        crate::varatt::SET_VARSIZE(base, total as u32);
        let arr = base.cast::<crate::utils::array::ArrayType>();
        (*arr).ndim = 1;
        (*arr).dataoffset = 0; // no null bitmap
        (*arr).elemtype = ACLITEMOID;
        // dims[0] = nitems, lbound[0] = 1 (immediately after the fixed header).
        let dims = base.add(core::mem::size_of::<crate::utils::array::ArrayType>()).cast::<i32>();
        *dims = nitems as i32;
        *dims.add(1) = 1;
        // Packed element data at the MAXALIGNed offset.
        let data = base.add(header).cast::<AclItem>();
        for (i, it) in items.iter().enumerate() {
            core::ptr::write(data.add(i), *it);
        }
    }
    Box::leak(buf).as_mut_ptr()
}

/// Read an `Acl` varlena (1-D, no-null aclitem[]) back into a `Vec<AclItem>`.
///
/// SAFETY: `p` must point at a live, non-toasted `aclitem[]` varlena (the relacl
/// column value, kept alive by the caller's tuple). The varlena may carry a 1-byte
/// (short) or 4-byte header -- `heap_form_tuple` shortens small varlenas -- so the
/// array body is reached via `VARDATA_ANY`, not a fixed-offset `ArrayType` overlay.
unsafe fn read_acl(p: *const u8) -> Vec<AclItem> {
    // The array body layout (after the varlena header): ndim(i32), dataoffset(i32),
    // elemtype(Oid), then dims[ndim], lbound[ndim], then MAXALIGNed element data.
    // ARR_OVERHEAD_NONULLS measures from the start of the (4-byte-header) ArrayType,
    // i.e. it already counts the header slot; relative to the body it is that minus
    // the 4-byte vl_len_ slot.
    // Body field offsets: ndim@0, dataoffset@4, elemtype@8, dims[]@12, lbound[]@...
    let body = crate::varatt::VARDATA_ANY(p.cast_mut());
    let read_i32 = |off: usize| -> i32 {
        let mut b = [0u8; 4];
        core::ptr::copy_nonoverlapping(body.add(off), b.as_mut_ptr(), 4);
        i32::from_ne_bytes(b)
    };
    let ndim = read_i32(0);
    if ndim <= 0 {
        return Vec::new();
    }
    let nitems = read_i32(12).max(0) as usize; // dims[0]
    // Element data starts after the array header. PG's ARR_DATA_OFFSET measures from
    // the 4-byte-header ArrayType start; relative to the body it is that minus the
    // 4-byte vl_len_ slot.
    let data_off =
        crate::utils::array::ArrayType::arr_overhead_nonulls(ndim as usize) - crate::c::VARHDRSZ as usize;
    let elem_sz = core::mem::size_of::<AclItem>();
    let mut out = Vec::with_capacity(nitems);
    for i in 0..nitems {
        let mut item = core::mem::MaybeUninit::<AclItem>::uninit();
        core::ptr::copy_nonoverlapping(
            body.add(data_off + i * elem_sz),
            item.as_mut_ptr().cast::<u8>(),
            elem_sz,
        );
        out.push(item.assume_init());
    }
    out
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use super::*;
    use crate::nodes::nodes::{CmdType, Node};
    use crate::parser::parser::RawParseMode;
    use crate::shared_state::{SharedState, SharedStateConfig};

    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
    const DB_OID: Oid = Oid::new(90000);

    fn new_shared() -> Arc<SharedState> {
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-aclchk-{}-{}", std::process::id(), n));
        let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
        let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
        SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            nbuffers: 256,
            ..Default::default()
        })
    }

    async fn in_scopes<F, Fut, T>(shared: Arc<SharedState>, f: F) -> T
    where
        F: FnOnce(Arc<SharedState>) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        use crate::backend::access::transam::xloginsert::with_insertion;
        use crate::backend::catalog::indexing::scope_async as catalog_index_scope;
        use crate::backend::utils::cache::catcache::scope_async as catcache_scope;
        use crate::backend::utils::cache::relcache::scope_async as relcache_scope;
        use crate::backend::utils::time::{combocid::combocid_scope, snapmgr::snapmgr_scope};

        let sess = Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND));
        sess.set_database_id(DB_OID);
        sess.set_database_tablespace(crate::common::relpath::DEFAULTTABLESPACE_OID);
        sess.set_authenticated_user_id(BOOTSTRAP_SUPERUSERID);
        sess.set_current_user_id(BOOTSTRAP_SUPERUSERID);
        let owner = crate::backend::utils::resowner::resowner::ResourceOwner::create(None, "Test");

        let body = Box::pin(catalog_index_scope(Box::pin(relcache_scope(Box::pin(f(shared))))));
        let body = Box::pin(catcache_scope(body));
        let body = Box::pin(with_insertion(body));
        let body = Box::pin(combocid_scope(body));
        let body = Box::pin(snapmgr_scope(body));
        let body = Box::pin(crate::backend::access::transam::xact::xact_scope(body));
        crate::session::scope(
            sess,
            crate::backend::utils::resowner::resowner::scope(owner, body),
        )
        .await
    }

    async fn init_db(shared: &Arc<SharedState>) {
        use crate::backend::access::transam::xact::{GetCurrentCommandId, StartTransactionCommand};
        use crate::backend::utils::time::snapmgr::{GetTransactionSnapshot, PushActiveSnapshot};

        StartTransactionCommand(shared).await;
        let mut snap = GetTransactionSnapshot(shared);
        if let Some(s) = snap.as_mut() {
            Arc::make_mut(s).curcid = GetCurrentCommandId(false);
        }
        PushActiveSnapshot(snap);
        crate::backend::bootstrap::bootstrap::bootstrap_catalogs(shared).await;
        bump_command(shared);
    }

    fn bump_command(shared: &Arc<SharedState>) {
        use crate::backend::access::transam::xact::{CommandCounterIncrement, GetCurrentCommandId};
        use crate::backend::utils::time::snapmgr::{
            GetTransactionSnapshot, InvalidateCatalogSnapshot, PopActiveSnapshot, PushActiveSnapshot,
        };
        CommandCounterIncrement();
        InvalidateCatalogSnapshot();
        PopActiveSnapshot();
        let mut snap = GetTransactionSnapshot(shared);
        if let Some(s) = snap.as_mut() {
            Arc::make_mut(s).curcid = GetCurrentCommandId(false);
        }
        PushActiveSnapshot(snap);
    }

    async fn run_utility(shared: &Arc<SharedState>, sql: &str) {
        let mut list = crate::backend::parser::parser::raw_parser(sql, RawParseMode::Default);
        let Node::RawStmt(rs) = list.remove(0) else { panic!("not a RawStmt") };
        let rs = *rs;
        let q = crate::backend::parser::analyze::parse_analyze_fixedparams(&rs, sql, &[], 0, None);
        let pstmt = crate::nodes::plannodes::PlannedStmt {
            command_type: CmdType::UTILITY,
            query_id: q.queryId,
            plan_id: 0,
            has_returning: false,
            has_modifying_cte: false,
            can_set_tag: q.canSetTag,
            transient_plan: false,
            depends_on_role: false,
            parallel_mode_needed: false,
            jit_flags: 0,
            plan_tree: Node::A_Star(Box::new(crate::nodes::parsenodes::A_Star {})),
            part_prune_infos: Vec::new(),
            rtable: Vec::new(),
            unprunable_relids: None,
            perm_infos: Vec::new(),
            result_relations: Vec::new(),
            append_relations: Vec::new(),
            subplans: Vec::new(),
            subplan_nodes: Vec::new(),
            rewind_plan_ids: None,
            row_marks: Vec::new(),
            relation_oids: Vec::new(),
            inval_items: Vec::new(),
            param_exec_types: Vec::new(),
            utility_stmt: q.utilityStmt.clone(),
            stmt_location: q.stmt_location,
            stmt_len: q.stmt_len,
        };
        let mut dest = crate::backend::tcop::dest::NoneReceiver;
        crate::backend::tcop::utility::process_utility(
            shared,
            &pstmt,
            sql,
            crate::tcop::utility::ProcessUtilityContext::Toplevel,
            &mut dest,
            None,
        )
        .await;
        bump_command(shared);
    }

    async fn relid(shared: &Arc<SharedState>, name: &str) -> Oid {
        crate::backend::catalog::namespace::range_var_get_relid(shared, None, name)
            .await
            .expect("relation resolves")
    }

    /// Read the relacl AclItems for a relation (empty if NULL).
    async fn relacl_of(shared: &Arc<SharedState>, oid: Oid) -> Vec<AclItem> {
        let rows = scan_pg_class_by_oid(shared, oid).await;
        let row = rows.into_iter().next().expect("pg_class row");
        let pg_class = relation_id_get_relation(RelationRelationId).unwrap();
        let desc = pg_class.rd_att.clone().unwrap();
        let (vals, nulls) = unsafe { heap_deform_tuple(&row.tuple, &desc) };
        relation_close(pg_class);
        let i = (c::Anum_pg_class_relacl - 1) as usize;
        let out = if nulls[i] {
            Vec::new()
        } else {
            let p = DatumGetPointer(vals[i]);
            unsafe { read_acl(p) }
        };
        heap_freetuple(row.tuple);
        out
    }

    /// The privileges PUBLIC holds on `oid`, or empty.
    async fn public_privs(shared: &Arc<SharedState>, oid: Oid) -> AclMode {
        relacl_of(shared, oid)
            .await
            .into_iter()
            .find(|it| it.grantee == ACL_ID_PUBLIC)
            .map_or(AclMode::empty(), aclitem_get_privs)
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn grant_revoke_select_public() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            run_utility(&shared, "CREATE TABLE t (a int)").await;
            let oid = relid(&shared, "t").await;

            run_utility(&shared, "GRANT SELECT ON TABLE t TO PUBLIC").await;
            assert_eq!(
                public_privs(&shared, oid).await,
                AclMode::SELECT,
                "GRANT SELECT TO PUBLIC reflected in relacl"
            );

            run_utility(&shared, "REVOKE SELECT ON TABLE t FROM PUBLIC").await;
            assert_eq!(
                public_privs(&shared, oid).await,
                AclMode::empty(),
                "REVOKE removes the PUBLIC grant"
            );
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn grant_multiple_privileges() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            run_utility(&shared, "CREATE TABLE t2 (a int)").await;
            let oid = relid(&shared, "t2").await;

            run_utility(&shared, "GRANT SELECT, INSERT, UPDATE ON TABLE t2 TO PUBLIC").await;
            assert_eq!(
                public_privs(&shared, oid).await,
                AclMode::SELECT | AclMode::INSERT | AclMode::UPDATE,
                "all three privileges present"
            );
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn grant_all_and_grant_option() {
        let shared = new_shared();
        Box::pin(in_scopes(shared.clone(), |shared| async move {
            init_db(&shared).await;
            run_utility(&shared, "CREATE TABLE t3 (a int)").await;
            let oid = relid(&shared, "t3").await;

            run_utility(&shared, "GRANT ALL ON TABLE t3 TO PUBLIC WITH GRANT OPTION").await;
            let item = relacl_of(&shared, oid)
                .await
                .into_iter()
                .find(|it| it.grantee == ACL_ID_PUBLIC)
                .expect("PUBLIC item present");
            assert_eq!(
                aclitem_get_privs(item),
                ACL_ALL_RIGHTS_RELATION,
                "ALL expands to all relation rights"
            );
            assert_eq!(
                aclitem_get_goptions(item),
                ACL_ALL_RIGHTS_RELATION,
                "WITH GRANT OPTION sets the grant-option bits"
            );
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn acl_roundtrip_build_read() {
        // build_acl -> read_acl is an identity over the items.
        let items = vec![
            AclItem { grantee: ACL_ID_PUBLIC, grantor: BOOTSTRAP_SUPERUSERID, privs: AclMode::SELECT },
            AclItem {
                grantee: BOOTSTRAP_SUPERUSERID,
                grantor: BOOTSTRAP_SUPERUSERID,
                privs: ACL_ALL_RIGHTS_RELATION,
            },
        ];
        let p = build_acl(&items);
        let back = unsafe { read_acl(p) };
        assert_eq!(back, items, "Acl varlena round-trips");
    }
}
