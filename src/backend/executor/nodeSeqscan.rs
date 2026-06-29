//! Sequential-scan node executor. Translated from
//! backend/executor/nodeSeqscan.c (disposition: full for the M2 forward,
//! qual-free seqscan).
//!
//! `ExecInitSeqScan` opens the scan relation, builds the scan tuple slot + the
//! scan projection, and (lazily) the heap scan descriptor; `ExecSeqScan` drives
//! the generic `ExecScan` loop whose access method is `SeqNext`
//! (`table_scan_getnext` -> heap, then deform into the scan slot); `ExecEndSeqScan`
//! releases the scan. WHERE quals, backward/rescan, parallel scan, and EPQ grow
//! at later milestones (rules.md s4).
//!
//! Slot ownership: the `SeqScanState` OWNS its scan tuple slot
//! (`ss_scan_tuple_slot`). Each `ExecScan` deforms the current heap tuple into
//! that slot, points `econtext->ecxt_scantuple` at it (an O(1) `Box` move, no data
//! copy), runs the projection (whose Vars read the scan slot), and returns a
//! borrow of the projection's result slot -- no per-tuple deep clone.
//!
//! Async coloring: the scan reaches the table AM's buffer reads, so `ExecSeqScan`
//! is `async` (rules.md s5). No content lock is held across the `.await` (the AM
//! manages page locks internally, step 12).


use std::sync::Arc;

use crate::access::sdir::ScanDirection;
use crate::backend::access::common::heaptuple::heap_deform_tuple;
use crate::backend::access::heap::heapam::HeapScanDescData;
use crate::backend::access::table::tableam::{table_beginscan, table_endscan, table_scan_getnext};
use crate::backend::executor::execScan::exec_scan;
use crate::backend::executor::execTuples::{exec_type_from_tl, make_tuple_table_slot, TTS_OPS_VIRTUAL};
use crate::backend::executor::execUtils::{create_expr_context, exec_assign_projection_info};
use crate::nodes::execnodes::{EState, PlanState, ScanState, SeqScanState, TupleTableSlot};
use crate::nodes::nodes::Node;
use crate::nodes::plannodes::SeqScan;
use crate::shared_state::SharedState;
use crate::utils::rel::RelationData;

/// Run-state pairing the PG `SeqScanState` with the open heap scan descriptor.
/// The C node holds the descriptor through `ss_currentScanDesc` (a base-class
/// pointer down-cast to the AM's `HeapScanDescData`); the Rust `ScanState` field
/// is typed to the AM-independent base, so the concrete heap descriptor is kept
/// here instead (a fresh wrapper in this island, no edit to nodes/execnodes.rs).
///
/// Borrow-based ownership (relation-ownership-plan step 5): the node BORROWS its
/// scan relation and snapshot (`&'rel RelationData` / `&'rel SnapshotData`) from the
/// `EState` range-table, whose owners are the command frame's `Arc`s (the `'rel`
/// root that strictly encloses the executor run). No node-owned `Arc`, no resume
/// cursor -- the borrows come from a suspended ancestor frame, so the descriptor
/// holding them rides every scan `.await` soundly and is not self-referential.
pub struct SeqScanRun<'rel> {
    pub state: Box<SeqScanState>,
    /// the scan relation borrowed from `EState.es_range_table_rels` (PG keeps it in
    /// `ss_currentRelation`); the AM scan descriptor borrows it for the scan's life.
    pub relation: &'rel RelationData,
    /// the query snapshot borrowed from `EState.es_snapshot_ref` (PG `es_snapshot`).
    pub snapshot: &'rel crate::utils::snapshot::SnapshotData,
    /// the open heap scan descriptor, created on the first `ExecSeqScan` (it needs
    /// the query snapshot, available by then). Borrows `relation`/`snapshot`; boxed
    /// so the large `vistuples` array stays off the scan future's frame.
    pub scan: Option<Box<HeapScanDescData<'rel, 'rel>>>,
}

/// PG `ExecInitSeqScan`: build the SeqScanState. Opens the scan relation (from the
/// EState's per-RTI relation slots), builds the virtual scan tuple slot from the
/// relation's rowtype, sets up `ss_ScanTupleSlot` + the result slot + the scan
/// projection, and the per-node exprcontext. The heap scan descriptor is opened
/// lazily on the first `ExecSeqScan` (it needs the query snapshot, set by then).
pub fn exec_init_seq_scan<'rel>(node: &SeqScan, estate: &mut EState<'rel>, eflags: i32) -> Box<SeqScanRun<'rel>> {
    let _ = eflags;
    crate::assert!(
        node.scan.plan.lefttree.is_none() && node.scan.plan.righttree.is_none(),
        "ExecInitSeqScan: a scan node is childless"
    );

    let scanrelid = node.scan.scanrelid;
    let relation = exec_get_range_table_relation(estate, scanrelid);
    // es_snapshot: the scan reads under the query snapshot, borrowed from the
    // command frame's `Arc` (set on the EState before InitPlan).
    let snapshot = estate
        .es_snapshot_ref
        .unwrap_or_else(|| unimplemented!("ExecInitSeqScan: no active snapshot for the scan"));

    // The scan slot's descriptor IS the relation's rowtype (the AM yields tuples
    // of that shape). It is a virtual slot here: the deform fills its value/null
    // arrays from each heap tuple.
    let scan_desc = relation_tupdesc(relation);
    let scan_slot = make_tuple_table_slot(Some(Arc::clone(&scan_desc)), &TTS_OPS_VIRTUAL);

    let mut ps = PlanState {
        plan: Some(Node::SeqScan(Box::new(node.clone()))),
        scandesc: Some(Arc::clone(&scan_desc)),
        scanops: Some(&TTS_OPS_VIRTUAL),
        scanopsset: true,
        scanopsfixed: true,
        ..PlanState::default()
    };
    // ExecAssignExprContext: per-node exprcontext (holds ecxt_scantuple during a
    // projection).
    ps.ps_expr_context = Some(create_expr_context(estate));

    // ExecInitResultTypeTL + result slot from the plan targetlist.
    let result_desc = exec_type_from_tl(&node.scan.plan.targetlist);
    let result_slot = make_tuple_table_slot(Some(Arc::clone(&result_desc)), &TTS_OPS_VIRTUAL);
    ps.ps_result_tuple_desc = Some(result_desc);
    ps.ps_result_tuple_slot = Some(result_slot);
    ps.resultops = Some(&TTS_OPS_VIRTUAL);
    ps.resultopsset = true;
    ps.resultopsfixed = true;

    // ExecAssignScanProjectionInfo: build the projection from the plan tlist,
    // with input desc = the scan slot's descriptor (so scan Vars resolve).
    exec_assign_projection_info(&mut ps, Some(Arc::clone(&scan_desc)));

    // ExecInitQual: compile the WHERE qual (the scan slot is virtual, so the scan
    // Vars in the qual read ecxt_scantuple directly). None when there is no WHERE.
    ps.qual = crate::backend::executor::execExpr::exec_init_qual(&node.scan.plan.qual, None);

    let ss = ScanState {
        ps,
        ss_current_relation: None,
        ss_current_scan_desc: None,
        ss_scan_tuple_slot: Some(scan_slot),
    };

    Box::new(SeqScanRun {
        state: Box::new(SeqScanState { ss, pscan_len: 0 }),
        relation,
        snapshot,
        scan: None,
    })
}

/// PG `ExecSeqScan`: drive the generic scan loop with `SeqNext` as the access
/// method. Returns a borrow of the node-owned projection result slot (or None at
/// end of scan).
pub async fn exec_seq_scan<'r>(
    shared: &Arc<SharedState>,
    run: &'r mut SeqScanRun<'_>,
) -> Option<&'r mut TupleTableSlot> {
    // PG's ExecScan loops fetch -> ExecQual -> project; the async fetch lives here,
    // so loop: fetch+deform the next tuple (SeqNext), evaluate the WHERE qual +
    // project (ExecScan). A qual-failed row yields None from exec_scan -> fetch the
    // next one; a passing row returns the projected slot; end-of-scan returns None
    // from seq_next. The borrow-checker needs the success/failure split below
    // because `exec_scan` borrows `run` mutably for the returned slot's lifetime.
    loop {
        let got = seq_next(shared, run).await;
        if !got {
            return None;
        }
        // ExecScan: qual-check the scan slot; if it passes, project and stop.
        if exec_scan(&mut run.state.ss).is_some() {
            return run
                .state
                .ss
                .ps
                .ps_proj_info
                .as_mut()
                .and_then(|p| p.state.resultslot.as_deref_mut());
        }
    }
}

/// PG `SeqNext`: fetch the next tuple from the table AM and store it in the scan
/// slot. M2: forward, MVCC, page-at-a-time; `heap_getnext` + `heap_deform_tuple`
/// into the (virtual) scan slot. Returns false at end of scan.
async fn seq_next(shared: &Arc<SharedState>, run: &mut SeqScanRun<'_>) -> bool {
    // table_beginscan on the first call (needs the query snapshot, available now).
    // The descriptor borrows the relation + snapshot the node holds (themselves
    // borrowed from the command frame via EState), so it can be stored directly.
    if run.scan.is_none() {
        run.scan = Some(table_beginscan(run.relation, run.snapshot));
    }
    let scan = run
        .scan
        .as_mut()
        .unwrap_or_else(|| unreachable!("scan just opened"));

    let tuple = table_scan_getnext(shared, scan, ScanDirection::Forward).await;
    let Some(tuple) = tuple else {
        // ExecClearTuple(slot): end of scan -> empty the scan slot.
        if let Some(slot) = run.state.ss.ss_scan_tuple_slot.as_mut() {
            crate::executor::tuptable::ExecClearTuple(slot);
        }
        return false;
    };

    // Deform the heap tuple into the scan slot's value/null arrays, then store.
    // SAFETY: `tuple` references scan.ctup (valid until the next getnext), whose
    // body is an owned copy of the page item; deform reads the header + data bytes.
    let desc = scan_slot_desc(&run.state.ss);
    let htd = unsafe { &*tuple };
    let (values, isnull) = unsafe { heap_deform_tuple(htd, &desc) };

    let slot = run
        .state
        .ss
        .ss_scan_tuple_slot
        .as_mut()
        .unwrap_or_else(|| unimplemented!("SeqNext: scan node has no scan tuple slot"));
    crate::executor::tuptable::ExecClearTuple(slot);
    let n = values.len();
    slot.values[..n].copy_from_slice(&values);
    slot.isnull[..n].copy_from_slice(&isnull);
    crate::backend::executor::execTuples::exec_store_virtual_tuple(slot);
    true
}

/// PG `ExecEndSeqScan`: release the scan resources. Closes the heap scan (unpins
/// the current buffer, reclaims the leaked snapshot Arc); the relation is owned by
/// the caller (M2), not closed here. Slots drop with the node state.
pub fn exec_end_seq_scan(shared: &Arc<SharedState>, run: &mut SeqScanRun<'_>) {
    if let Some(scan) = run.scan.as_mut() {
        table_endscan(shared, scan);
    }
    run.scan = None;
}

/// PG `ExecReScanSeqScan`: restart the scan from the beginning. M2: drop the open
/// descriptor so the next `ExecSeqScan` re-opens it.
pub fn exec_rescan_seq_scan(shared: &Arc<SharedState>, run: &mut SeqScanRun<'_>) {
    if let Some(scan) = run.scan.as_mut() {
        table_endscan(shared, scan);
    }
    run.scan = None;
}

/// PG `ExecGetRangeTableRelation` (M2 subset): the open relation for range-table
/// index `rti`, BORROWED from `EState.es_range_table_rels` (PG's `es_relations`,
/// indexed by RTI). The command frame opened the relations into its owning `Arc`s
/// and published the borrows on the EState before InitPlan. The heavyweight-lock
/// relation_open path grows with the relcache lock-manager wiring.
fn exec_get_range_table_relation<'rel>(
    estate: &EState<'rel>,
    rti: crate::nodes::primnodes::Index,
) -> &'rel RelationData {
    crate::assert!(rti > 0);
    estate
        .es_range_table_rels
        .get(rti - 1)
        .copied()
        .flatten()
        .unwrap_or_else(|| unimplemented!("ExecGetRangeTableRelation: scan relation not registered for RTI"))
}

/// The rowtype descriptor of a relation (`RelationGetDescr`).
fn relation_tupdesc(relation: &RelationData) -> crate::access::tupdesc::TupleDesc {
    relation.rd_att
        .clone()
        .unwrap_or_else(|| unimplemented!("relation_tupdesc: relation has no rowtype descriptor"))
}

/// The scan slot's descriptor (clone of the scan tuple desc).
fn scan_slot_desc(ss: &ScanState) -> crate::access::tupdesc::TupleDesc {
    ss.ps
        .scandesc
        .clone()
        .unwrap_or_else(|| unimplemented!("scan_slot_desc: no scan descriptor"))
}

#[cfg(test)]
mod tests {
    //! M2 executor integration: heap insert + forward seqscan + scan-Var
    //! projection + ModifyTable(Insert), exercised end-to-end over a real
    //! foundation `SharedState` (tempdir) with the full per-task scope stack. The
    //! relation is a hand-built `RelationData` (no syscache / initdb dependency,
    //! mirroring the heapam M2 tests); the SeqScan / ModifyTable plan nodes are
    //! built directly (the planner's createplan is covered by its own step-17
    //! tests). Verifies the slot-ownership + EEOP_SCAN_VAR path produces the exact
    //! inserted Datums in scan order.
    use super::*;
    use std::sync::{Arc, Mutex};

    use crate::access::tupdesc::{TupleDesc, TupleDescData};
    use crate::backend::access::common::heaptuple::heap_form_tuple;
    use crate::backend::access::heap::heapam::heap_insert;
    use crate::backend::access::transam::xact::{GetCurrentCommandId, StartTransactionCommand};
    use crate::backend::executor::execMain::{standard_executor_end, standard_executor_start, standard_executor_run};
    use crate::backend::executor::execProcnode::PlanStateNode;
    use crate::backend::utils::time::snapmgr::GetTransactionSnapshot;
    use crate::catalog::pg_class::{FormData_pg_class, RELKIND_RELATION, RELPERSISTENCE_PERMANENT};
    use crate::common::relpath::ForkNumber;
    use crate::executor::execdesc::QueryDesc;
    use crate::executor::instrument::InstrumentOption;
    use crate::executor::tuptable::{slot_getattr, TupleTableSlot};
    use crate::backend::nodes::makefuncs::{make_const, make_target_entry, make_var};
    use crate::nodes::nodes::CmdType;
    use crate::nodes::plannodes::{Plan, PlannedStmt, Scan, SeqScan as SeqScanPlan};
    use crate::postgres::{Datum, DatumGetInt32, Int32GetDatum};
    use crate::storage::relfilelocator::RelFileLocator;
    use crate::tcop::dest::{CommandDest, DestReceiver};
    use crate::utils::rel::{LockInfoData, LockRelId, RelationData};
    use crate::access::sdir::ScanDirection;

    const INT4OID: crate::postgres_ext::Oid = crate::postgres_ext::Oid(23);
    static COUNTER: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);

    fn new_shared() -> Arc<SharedState> {
        use crate::shared_state::{SharedState, SharedStateConfig};
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("pepperdb-exec18a-{}-{}", std::process::id(), n));
        let _ = std::fs::create_dir_all(dir.join(crate::access::xlog_internal::XLOGDIR));
        let _ = std::fs::create_dir_all(dir.join("base").join("90000"));
        SharedState::new(SharedStateConfig {
            data_dir: Some(dir.to_string_lossy().into_owned()),
            nbuffers: 64,
            ..Default::default()
        })
    }

    async fn in_all_scopes<F, Fut, T>(shared: Arc<SharedState>, f: F) -> T
    where
        F: FnOnce(Arc<SharedState>) -> Fut,
        Fut: std::future::Future<Output = T>,
    {
        use crate::backend::access::transam::xact::xact_scope;
        use crate::backend::access::transam::xloginsert::with_insertion;
        use crate::backend::utils::time::{combocid::combocid_scope, snapmgr::snapmgr_scope};
        let sess = Arc::new(crate::session::Session::new(crate::miscadmin::BackendType::BACKEND));
        let owner = crate::backend::utils::resowner::resowner::ResourceOwner::create(None, "Test");
        crate::session::scope(
            sess,
            crate::backend::utils::resowner::resowner::scope(
                owner,
                xact_scope(snapmgr_scope(combocid_scope(with_insertion(f(shared))))),
            ),
        )
        .await
    }

    fn rloc(rel: u32) -> RelFileLocator {
        RelFileLocator {
            spcOid: crate::postgres_ext::Oid(1663),
            dbOid: crate::postgres_ext::Oid(90000),
            relNumber: crate::postgres_ext::Oid(81000 + rel),
        }
    }

    /// One- or two-column int4 descriptor (cols named a[, b]).
    fn int4_desc(ncols: usize) -> TupleDesc {
        let mut d = TupleDescData::create_template(ncols as i32);
        for (i, name) in ["a", "b", "c"].iter().take(ncols).enumerate() {
            d.init_builtin_entry((i + 1) as i16, name, INT4OID, -1, 0);
        }
        Arc::new(d)
    }

    /// Build a minimal heap `RelationData` (boxed, leaked) backed by `locator`.
    fn make_relation(locator: RelFileLocator, tupdesc: TupleDesc) -> Arc<RelationData> {
        use std::sync::atomic::Ordering;
        // SAFETY: FormData_pg_class is repr(C) POD; all-zero is valid. Patch the
        // fields heap reads.
        let mut form: Box<FormData_pg_class> = Box::new(unsafe { core::mem::zeroed() });
        form.relkind = RELKIND_RELATION;
        form.relpersistence = RELPERSISTENCE_PERMANENT;
        form.relnatts = tupdesc.natts as i16;
        form.relam = crate::postgres_ext::Oid(2);
        let form_ptr = Some(form);

        let mut rel = RelationData::blank();
        rel.rd_locator = locator;
        rel.rd_refcnt.store(1, Ordering::Relaxed);
        rel.rd_isvalid.store(true, Ordering::Relaxed);
        rel.rd_rel = form_ptr;
        rel.rd_att = Some(tupdesc);
        rel.rd_id = locator.relNumber;
        rel.rd_lockInfo = LockInfoData {
            lockRelId: LockRelId { relId: locator.relNumber, dbId: locator.dbOid },
        };
        rel.rd_amhandler = crate::postgres_ext::Oid(2);
        Arc::new(rel)
    }

    async fn create_main_fork(shared: &Arc<SharedState>, locator: RelFileLocator) {
        let mut smgr = crate::storage::smgr::SmgrRelation::open(
            locator,
            crate::storage::procnumber::INVALID_PROC_NUMBER,
        );
        smgr.create(shared, ForkNumber::MAIN_FORKNUM, false).await;
    }

    /// Insert a row of int4 values directly via heap_insert.
    async fn insert_row(shared: &Arc<SharedState>, relation: &Arc<RelationData>, vals: &[i32]) {
        let desc = relation.rd_att.clone().unwrap();
        let values: Vec<Datum> = vals.iter().map(|&v| Int32GetDatum(v)).collect();
        let isnull = vec![false; vals.len()];
        let mut tuple = heap_form_tuple(&desc, &values, &isnull);
        let cid = GetCurrentCommandId(true);
        heap_insert(shared, relation, &mut tuple, cid, 0).await;
        crate::backend::access::common::heaptuple::heap_freetuple(tuple);
    }

    /// A collecting DestReceiver (Send: Arc<Mutex>) recording each row's int4s.
    #[derive(Default)]
    struct Collected {
        rows: Vec<Vec<(Datum, bool)>>,
        startups: u32,
        shutdowns: u32,
    }
    struct CollectingDest {
        sink: Arc<Mutex<Collected>>,
    }
    impl DestReceiver for CollectingDest {
        fn receive_slot(&mut self, slot: &mut TupleTableSlot) -> bool {
            let natts = i32::from(slot.nvalid);
            let row = (1..=natts)
                .map(|attno| {
                    let v = slot_getattr(slot, attno);
                    (v.unwrap_or(Datum(0)), v.is_none())
                })
                .collect();
            self.sink.lock().unwrap().rows.push(row);
            true
        }
        fn r_startup(&mut self, _op: CmdType, _ti: TupleDesc) {
            self.sink.lock().unwrap().startups += 1;
        }
        fn r_shutdown(&mut self) {
            self.sink.lock().unwrap().shutdowns += 1;
        }
        fn mydest(&self) -> CommandDest {
            CommandDest::DestNone
        }
    }

    /// Build a SeqScan plan over relation RTI 1 projecting `attnos` (1-based) of a
    /// rowtype `desc`.
    fn seqscan_plan(desc: &TupleDesc, attnos: &[i16]) -> PlannedStmt {
        let targetlist: Vec<crate::nodes::nodes::Node> = attnos
            .iter()
            .enumerate()
            .map(|(i, &attno)| {
                let att = desc.attr((attno - 1) as usize);
                let var = make_var(1, attno, att.atttypid, att.atttypmod, att.attcollation, 0);
                let tle = make_target_entry(
                    Some(crate::nodes::nodes::Node::Var(Box::new(var))),
                    (i + 1) as i16,
                    Some(format!("col{attno}")),
                    false,
                );
                crate::nodes::nodes::Node::TargetEntry(Box::new(tle))
            })
            .collect();
        let plan = Plan {
            disabled_nodes: 0,
            startup_cost: 0.0,
            total_cost: 0.0,
            plan_rows: 0.0,
            plan_width: 0,
            parallel_aware: false,
            parallel_safe: false,
            async_capable: false,
            plan_node_id: 0,
            targetlist,
            qual: Vec::new(),
            lefttree: None,
            righttree: None,
            init_plan: Vec::new(),
            ext_param: None,
            all_param: None,
        };
        let seqscan = SeqScanPlan { scan: Scan { plan, scanrelid: 1 } };
        planned_stmt(CmdType::SELECT, crate::nodes::nodes::Node::SeqScan(Box::new(seqscan)), Vec::new())
    }

    fn planned_stmt(
        op: CmdType,
        plan_tree: crate::nodes::nodes::Node,
        result_relations: Vec<i32>,
    ) -> PlannedStmt {
        PlannedStmt {
            command_type: op,
            query_id: 0,
            plan_id: 0,
            has_returning: false,
            has_modifying_cte: false,
            can_set_tag: true,
            transient_plan: false,
            depends_on_role: false,
            parallel_mode_needed: false,
            jit_flags: 0,
            plan_tree,
            part_prune_infos: Vec::new(),
            // one RTE slot so range_table_size == 1 (RTI 1); content unused (the
            // relation is resolved from the executor registry).
            rtable: vec![crate::nodes::nodes::Node::Const(Box::new(make_const(
                INT4OID, -1, crate::postgres_ext::InvalidOid, 4, Int32GetDatum(0), false, true,
            )))],
            unprunable_relids: None,
            perm_infos: Vec::new(),
            result_relations,
            append_relations: Vec::new(),
            subplans: Vec::new(),
            rewind_plan_ids: None,
            row_marks: Vec::new(),
            relation_oids: Vec::new(),
            inval_items: Vec::new(),
            param_exec_types: Vec::new(),
            utility_stmt: None,
            stmt_location: -1,
            stmt_len: 0,
        }
    }

    #[allow(deprecated)]
    fn query_desc(stmt: PlannedStmt, snap: crate::utils::snapshot::Snapshot, dest: Box<dyn DestReceiver>) -> QueryDesc<'static> {
        QueryDesc {
            operation: stmt.command_type,
            plannedstmt: Some(Box::new(stmt)),
            sourceText: String::new(),
            snapshot: Some(Box::new(snap)),
            crosscheck_snapshot: None,
            dest: Some(dest),
            params: None,
            queryEnv: None,
            instrument_options: InstrumentOption::empty(),
            tupDesc: None,
            estate: None,
            planstate: None,
            already_executed: false,
            totaltime: None,
        }
    }

    #[allow(
        clippy::unnecessary_wraps,
        reason = "returns the `Snapshot` type alias (Option<Arc<SnapshotData>>) the QueryDesc field expects"
    )]
    fn current_snapshot(shared: &Arc<SharedState>) -> crate::utils::snapshot::Snapshot {
        let mut snap = GetTransactionSnapshot(shared).expect("a transaction snapshot");
        Arc::make_mut(&mut snap).curcid = GetCurrentCommandId(false);
        Some(snap)
    }

    /// Run a SeqScan QueryDesc to completion, returning each row's int4 columns.
    async fn run_seqscan(
        shared: &Arc<SharedState>,
        relation: &Arc<RelationData>,
        stmt: PlannedStmt,
    ) -> Vec<Vec<i32>> {
        let sink = Arc::new(Mutex::new(Collected::default()));
        let dest = Box::new(CollectingDest { sink: Arc::clone(&sink) });
        let snap = current_snapshot(shared);
        // The command frame owns the relation + snapshot `Arc`s; the executor
        // borrows them via the EState range-table (relation-ownership-plan step 5).
        let range_table_rels: Vec<Option<&RelationData>> = vec![Some(&**relation)];
        let snapshot_ref = snap.as_deref();
        let mut qd = query_desc(stmt, snap.clone(), dest);

        standard_executor_start(&mut qd, &range_table_rels, snapshot_ref, 0);
        standard_executor_run(Some(shared), &mut qd, ScanDirection::Forward, 0).await;
        standard_executor_end(Some(shared), &mut qd);
        drop(qd);

        let s = sink.lock().unwrap();
        assert_eq!(s.startups, 1);
        assert_eq!(s.shutdowns, 1);
        s.rows
            .iter()
            .map(|r| r.iter().map(|&(d, n)| { assert!(!n); DatumGetInt32(d) }).collect())
            .collect()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn insert_then_seqscan_sees_all_rows_in_order() {
        let shared = new_shared();
        Box::pin(in_all_scopes(shared.clone(), |shared| async move {
            StartTransactionCommand(&shared).await;
            let loc = rloc(1);
            create_main_fork(&shared, loc).await;
            let desc = int4_desc(1);
            let rel = make_relation(loc, desc.clone());

            for v in [10, 20, 30] {
                insert_row(&shared, &rel, &[v]).await;
            }
            crate::backend::access::transam::xact::CommandCounterIncrement();

            // SELECT * (the one column a).
            let rows = run_seqscan(&shared, &rel, seqscan_plan(&desc, &[1])).await;
            assert_eq!(rows, vec![vec![10], vec![20], vec![30]]);
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn seqscan_select_star_two_columns() {
        let shared = new_shared();
        Box::pin(in_all_scopes(shared.clone(), |shared| async move {
            StartTransactionCommand(&shared).await;
            let loc = rloc(2);
            create_main_fork(&shared, loc).await;
            let desc = int4_desc(2);
            let rel = make_relation(loc, desc.clone());

            insert_row(&shared, &rel, &[1, 2]).await;
            insert_row(&shared, &rel, &[3, 4]).await;
            crate::backend::access::transam::xact::CommandCounterIncrement();

            // SELECT * -> both columns (a, b).
            let rows = run_seqscan(&shared, &rel, seqscan_plan(&desc, &[1, 2])).await;
            assert_eq!(rows, vec![vec![1, 2], vec![3, 4]]);
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn seqscan_select_single_column_projects_via_scan_var() {
        let shared = new_shared();
        Box::pin(in_all_scopes(shared.clone(), |shared| async move {
            StartTransactionCommand(&shared).await;
            let loc = rloc(3);
            create_main_fork(&shared, loc).await;
            let desc = int4_desc(2);
            let rel = make_relation(loc, desc.clone());

            insert_row(&shared, &rel, &[100, 200]).await;
            insert_row(&shared, &rel, &[300, 400]).await;
            crate::backend::access::transam::xact::CommandCounterIncrement();

            // SELECT b (the second column only) -> EEOP_SCAN_VAR attno 1 (0-based).
            let rows = run_seqscan(&shared, &rel, seqscan_plan(&desc, &[2])).await;
            assert_eq!(rows, vec![vec![200], vec![400]]);

            // SELECT a -> the first column.
            let rows = run_seqscan(&shared, &rel, seqscan_plan(&desc, &[1])).await;
            assert_eq!(rows, vec![vec![100], vec![300]]);
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn empty_relation_seqscan_returns_no_rows() {
        let shared = new_shared();
        Box::pin(in_all_scopes(shared.clone(), |shared| async move {
            StartTransactionCommand(&shared).await;
            let loc = rloc(4);
            create_main_fork(&shared, loc).await;
            let desc = int4_desc(1);
            let rel = make_relation(loc, desc.clone());
            let rows = run_seqscan(&shared, &rel, seqscan_plan(&desc, &[1])).await;
            assert!(rows.is_empty());
        }))
        .await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn modifytable_insert_then_seqscan_sees_it() {
        use crate::nodes::plannodes::ModifyTable;
        let shared = new_shared();
        Box::pin(in_all_scopes(shared.clone(), |shared| async move {
            StartTransactionCommand(&shared).await;
            let loc = rloc(5);
            create_main_fork(&shared, loc).await;
            let desc = int4_desc(1);
            let rel = make_relation(loc, desc.clone());

            // Source: a Result projecting a single const row (42) -- the row to
            // insert (mirrors the planner's ModifyTable(lefttree=Result) for
            // INSERT INTO t VALUES(42)).
            let con = crate::backend::nodes::makefuncs::make_const(
                INT4OID, -1, crate::postgres_ext::InvalidOid, 4, Int32GetDatum(42), false, true,
            );
            let tle = make_target_entry(
                Some(crate::nodes::nodes::Node::Const(Box::new(con))),
                1,
                Some("a".to_string()),
                false,
            );
            let result = crate::nodes::plannodes::Result {
                plan: Plan {
                    disabled_nodes: 0, startup_cost: 0.0, total_cost: 0.0, plan_rows: 1.0,
                    plan_width: 4, parallel_aware: false, parallel_safe: false,
                    async_capable: false, plan_node_id: 0,
                    targetlist: vec![crate::nodes::nodes::Node::TargetEntry(Box::new(tle))],
                    qual: Vec::new(), lefttree: None, righttree: None,
                    init_plan: Vec::new(), ext_param: None, all_param: None,
                },
                resconstantqual: None,
            };
            let modify = ModifyTable {
                plan: Plan {
                    disabled_nodes: 0, startup_cost: 0.0, total_cost: 0.0, plan_rows: 0.0,
                    plan_width: 0, parallel_aware: false, parallel_safe: false,
                    async_capable: false, plan_node_id: 0, targetlist: Vec::new(),
                    qual: Vec::new(),
                    lefttree: Some(crate::nodes::nodes::Node::Result(Box::new(result))),
                    righttree: None, init_plan: Vec::new(), ext_param: None, all_param: None,
                },
                operation: CmdType::INSERT,
                can_set_tag: true,
                nominal_relation: 1,
                root_relation: 0,
                part_cols_updated: false,
                result_relations: vec![1],
                update_colnos_lists: Vec::new(),
                with_check_option_lists: Vec::new(),
                returning_old_alias: None,
                returning_new_alias: None,
                returning_lists: Vec::new(),
                fdw_priv_lists: Vec::new(),
                fdw_direct_modify_plans: None,
                row_marks: Vec::new(),
                epq_param: -1,
                on_conflict_action: crate::nodes::nodes::OnConflictAction::NONE,
                arbiter_indexes: Vec::new(),
                on_conflict_set: Vec::new(),
                on_conflict_cols: Vec::new(),
                on_conflict_where: None,
                excl_rel_rti: 0,
                excl_rel_tlist: Vec::new(),
                merge_action_lists: Vec::new(),
                merge_join_conditions: Vec::new(),
            };
            let insert_stmt = planned_stmt(
                CmdType::INSERT,
                crate::nodes::nodes::Node::ModifyTable(Box::new(modify)),
                vec![1],
            );

            // Run the INSERT. The command frame owns the target relation +
            // snapshot; the executor borrows them via the EState range-table.
            let snap = current_snapshot(&shared);
            let range_table_rels: Vec<Option<&RelationData>> = vec![Some(&*rel)];
            let snapshot_ref = snap.as_deref();
            let mut qd = query_desc(insert_stmt, snap.clone(), Box::new(CollectingDest { sink: Arc::new(Mutex::new(Collected::default())) }));
            standard_executor_start(&mut qd, &range_table_rels, snapshot_ref, 0);
            standard_executor_run(Some(&shared), &mut qd, ScanDirection::Forward, 0).await;
            standard_executor_end(Some(&shared), &mut qd);
            assert_eq!(qd.estate.as_ref().map_or(0, |e| e.processed), 0, "estate moved out by End");
            drop(qd);
            drop(range_table_rels);
            crate::backend::access::transam::xact::CommandCounterIncrement();

            // A subsequent SeqScan sees the inserted row.
            let rows = run_seqscan(&shared, &rel, seqscan_plan(&desc, &[1])).await;
            assert_eq!(rows, vec![vec![42]]);
        }))
        .await;
    }
}
