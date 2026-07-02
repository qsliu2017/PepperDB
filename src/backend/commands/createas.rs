//! Execution of CREATE TABLE ... AS, a/k/a SELECT INTO. Translated from
//! `commands/createas.c` (disposition: grow).
//!
//! CTAS diverts the query's output to a specialized `DestReceiver` (`DR_intorel`)
//! that creates the target relation from the query's tuple descriptor and inserts
//! the result rows. `CREATE MATERIALIZED VIEW` shares this path in PG; the matview
//! bits (viewQuery / REFRESH) grow with matviews.
//!
//! ## Port shape: sync receiver, async storage
//!
//! C's `intorel_startup` creates the relation and each `intorel_receive` inserts one
//! tuple, all inside the executor run. Here the storage layer (`DefineRelation`,
//! `heap_insert`) is `async` and needs `&Arc<SharedState>`, while `DestReceiver`
//! callbacks are sync (rules.md s5). So `DR_intorel` MATERIALIZES the result rows
//! into a tuplestore during the run (capturing the result `TupleDesc` at startup),
//! and `ExecCreateTableAs` -- which owns the async context -- creates the relation
//! from that descriptor afterward and drains the store row-by-row through
//! `heap_insert`. The observable behavior (relation built from the query's rowtype,
//! all result rows inserted) matches C; only the create/insert ordering shifts to
//! after the run, exactly as the cursor-materialization path does (pquery.rs).

use std::sync::Arc;
use std::sync::Mutex;

use crate::backend::access::common::heaptuple::{heap_form_tuple, heap_freetuple};
use crate::backend::access::heap::heapam::heap_insert;
use crate::backend::nodes::makefuncs::make_column_def;
use crate::backend::nodes::nodeFuncs::{exprCollation, exprType, exprTypmod};
use crate::backend::utils::cache::lsyscache::type_is_collatable;
use crate::catalog::objectaddress::{ObjectAddress, INVALID_OBJECT_ADDRESS};
use crate::catalog::pg_class::RELKIND_RELATION;
use crate::nodes::nodes::{CmdType, Node};
use crate::nodes::parsenodes::{CreateStmt, CreateTableAsStmt};
use crate::nodes::primnodes::IntoClause;
use crate::nodes::value::strVal;
use crate::postgres::Datum;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::shared_state::SharedState;
use crate::tcop::dest::{CommandDest, DestReceiver};
use crate::utils::tuplestore::Tuplestorestate;

use crate::access::tupdesc::TupleDesc;
use crate::executor::executor::ExecFlag;
use crate::executor::tuptable::TupleTableSlot;

/// Cursor options passed to `pg_plan_query` for the CTAS SELECT. PG passes
/// `CURSOR_OPT_PARALLEL_OK`, but this port's planner does not accept the parallel bit
/// yet (it only tweaks the tuple fraction there); the reachable wire path plans with
/// `0`, so CTAS does the same.
const CTAS_CURSOR_OPTIONS: i32 = 0;

/// The stash the caller and the receiver share to hand the filled store back after
/// the run (the executor owns + drops its receiver, so the store is passed out here).
type IntoRelStash = Arc<Mutex<Option<(Box<Tuplestorestate>, Option<TupleDesc>)>>>;

/// PG `DR_intorel`: the DestReceiver that collects a CTAS/SELECT-INTO query's rows.
///
/// Unlike C (which opens the target relation at startup and inserts each row as it
/// arrives), this receiver materializes the rows into `store` and captures the
/// result `TupleDesc` at startup; `ExecCreateTableAs` creates the relation and does
/// the inserts once the run finishes (see module docs). `skipData` (WITH NO DATA)
/// suppresses row collection.
pub struct DrIntoRel {
    into: Box<IntoClause>,
    store: Option<Box<Tuplestorestate>>,
    tupdesc: Option<TupleDesc>,
    /// Shutdown drops the captured (store, tupdesc) here so `ExecCreateTableAs` can
    /// reclaim them after the run.
    stash: IntoRelStash,
}

impl DrIntoRel {
    fn new(into: Box<IntoClause>, stash: IntoRelStash) -> Self {
        // PG's intorel_receive inserts rows into the (already-created) relation
        // directly, so CTAS is never bounded by work_mem. This port stashes the
        // rows in a tuplestore and drains it after creating the relation, so the
        // store must not spill (BufFile is untranslated): uncap it.
        let store =
            crate::backend::utils::sort::tuplestore::tuplestore_begin_heap(true, false, i32::MAX);
        Self { into, store: Some(store), tupdesc: None, stash }
    }
}

impl DestReceiver for DrIntoRel {
    /// PG `intorel_receive`: collect one result tuple (unless WITH NO DATA).
    fn receive_slot(&mut self, slot: &mut TupleTableSlot) -> bool {
        if !self.into.skipData
            && let Some(store) = self.store.as_mut()
        {
            crate::backend::utils::sort::tuplestore::tuplestore_puttupleslot(store, slot);
        }
        true
    }

    /// PG `intorel_startup`: capture the result descriptor. (The relation is created
    /// by `ExecCreateTableAs` afterward -- see module docs.)
    fn r_startup(&mut self, _operation: CmdType, typeinfo: TupleDesc) {
        if let Some(store) = self.store.as_mut() {
            crate::backend::utils::sort::tuplestore::tuplestore_set_tupdesc(store, typeinfo.clone());
        }
        self.tupdesc = Some(typeinfo);
    }

    /// PG `intorel_shutdown`: hand the filled store + descriptor back to the caller.
    fn r_shutdown(&mut self) {
        if let Some(mut store) = self.store.take() {
            crate::backend::utils::sort::tuplestore::tuplestore_rescan(&mut store);
            *self.stash.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some((store, self.tupdesc.take()));
        }
    }

    fn mydest(&self) -> CommandDest {
        CommandDest::DestIntoRel
    }
}

impl Drop for DrIntoRel {
    fn drop(&mut self) {
        // A run that never sent tuples (WITH NO DATA, or an empty result whose
        // send-loop still ran r_shutdown) leaves nothing to stash; but if r_shutdown
        // was never reached, reclaim the store so the caller does not block.
        if let Some(mut store) = self.store.take() {
            crate::backend::utils::sort::tuplestore::tuplestore_rescan(&mut store);
            *self.stash.lock().unwrap_or_else(std::sync::PoisonError::into_inner) =
                Some((store, self.tupdesc.take()));
        }
    }
}

/// PG `GetIntoRelEFlags`: executor flags for CREATE TABLE AS. WITH NO DATA sets
/// `EXEC_FLAG_WITH_NO_DATA` so the executor skips producing rows.
#[must_use]
pub fn GetIntoRelEFlags(into_clause: &IntoClause) -> i32 {
    let mut flags = 0;
    if into_clause.skipData {
        flags |= ExecFlag::WITH_NO_DATA.bits();
    }
    flags
}

/// PG `CreateIntoRelDestReceiver`: build a `DR_intorel` receiver bound to `into`.
/// Returns the boxed receiver alongside the shared stash `ExecCreateTableAs` uses to
/// reclaim the materialized rows after the run.
fn create_into_rel_dest_receiver(into: Box<IntoClause>) -> (Box<dyn DestReceiver>, IntoRelStash) {
    let stash: IntoRelStash = Arc::new(Mutex::new(None));
    let recv = Box::new(DrIntoRel::new(into, Arc::clone(&stash)));
    (recv, stash)
}

/// PG `create_ctas_internal`: create the target relation by faking up a `CreateStmt`
/// from the attribute list + `IntoClause` and running `DefineRelation`. The toast
/// table / matview `viewQuery` steps grow with those features.
async fn create_ctas_internal(
    shared: &Arc<SharedState>,
    attr_list: Vec<Node>,
    into: &IntoClause,
) -> ObjectAddress {
    let rel = into
        .rel
        .as_ref()
        .unwrap_or_else(|| unreachable!("an IntoClause names its target relation"))
        .clone();

    let create = CreateStmt {
        relation: Some(rel),
        tableElts: attr_list,
        inhRelations: Vec::new(),
        partbound: None,
        partspec: None,
        ofTypename: None,
        constraints: Vec::new(),
        nnconstraints: Vec::new(),
        options: into.options.clone(),
        oncommit: into.onCommit,
        tablespacename: into.tableSpaceName.clone(),
        accessMethod: into.accessMethod.clone(),
        if_not_exists: false,
    };

    // DefineRelation errors out if the relation already exists (CreateTableAsRelExists
    // handles the IF NOT EXISTS pre-check), so no extra "replace" logic is needed.
    let addr = crate::backend::commands::tablecmds::DefineRelation(
        shared,
        &create,
        RELKIND_RELATION,
        InvalidOid,
        "",
    )
    .await;

    // A CommandCounterIncrement so the new relation is visible for insertion (PG also
    // creates the toast table here; that grows with TOAST).
    crate::backend::access::transam::xact::CommandCounterIncrement();

    addr
}

/// Build a `ColumnDef` for one CTAS column, overriding its name from `colnames` if
/// one was supplied. Mirrors the shared attrList body of `create_ctas_nodata` /
/// `intorel_startup`, including the unresolved-collation double check.
fn ctas_column_def(
    colname: &str,
    typid: Oid,
    typmod: i32,
    collation: Oid,
) -> Node {
    let col = make_column_def(colname, typid, typmod, collation);
    if !col.collOid.is_valid() && type_is_collatable(typid) {
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_INDETERMINATE_COLLATION)
                .errmsg(format!(
                    "no collation was derived for column \"{colname}\" with collatable type {}",
                    crate::utils::builtins::format_type_be(typid)
                ))
                .errhint("Use the COLLATE clause to set the collation explicitly.");
        });
    }
    Node::ColumnDef(Box::new(col))
}

/// Read the optional column-name override list of an IntoClause (String value nodes).
fn into_colnames(into: &IntoClause) -> Vec<String> {
    into.colNames
        .iter()
        .filter_map(|n| match n {
            Node::String_(s) => Some(strVal(s).to_owned()),
            _ => None,
        })
        .collect()
}

/// PG `create_ctas_nodata`: build the attribute list from the query's target list
/// (non-junk entries) for WITH NO DATA, then create the relation. Overrides column
/// names from the IntoClause; too many names is an error.
async fn create_ctas_nodata(
    shared: &Arc<SharedState>,
    tlist: &[Node],
    into: &IntoClause,
) -> ObjectAddress {
    let colnames = into_colnames(into);
    let mut names = colnames.iter();
    let mut attr_list = Vec::new();

    for t in tlist {
        let Node::TargetEntry(tle) = t else { continue };
        if tle.resjunk {
            continue;
        }
        let expr = tle
            .expr
            .as_ref()
            .unwrap_or_else(|| unreachable!("a non-junk TargetEntry carries an expression"));
        let colname = names
            .next()
            .map_or_else(|| tle.resname.clone().unwrap_or_default(), Clone::clone);
        attr_list.push(ctas_column_def(
            &colname,
            exprType(expr),
            exprTypmod(expr),
            exprCollation(expr),
        ));
    }

    if names.next().is_some() {
        too_many_column_names();
    }

    create_ctas_internal(shared, attr_list, into).await
}

/// PG `intorel_startup`'s attrList build, for the WITH DATA path: build the attribute
/// list from the result tuple descriptor, overriding names from the IntoClause.
fn attr_list_from_tupdesc(tupdesc: &TupleDesc, into: &IntoClause) -> Vec<Node> {
    let colnames = into_colnames(into);
    let mut names = colnames.iter();
    let mut attr_list = Vec::new();

    for attnum in 0..tupdesc.natts as usize {
        let attr = tupdesc.attr(attnum);
        let colname = names.next().map_or_else(
            || crate::backend::commands::tablecmds::name_to_string(&attr.attname),
            Clone::clone,
        );
        attr_list.push(ctas_column_def(
            &colname,
            attr.atttypid,
            attr.atttypmod,
            attr.attcollation,
        ));
    }

    if names.next().is_some() {
        too_many_column_names();
    }

    attr_list
}

/// PG's "too many column names were specified" (`ERRCODE_SYNTAX_ERROR`).
#[cold]
fn too_many_column_names() -> ! {
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_SYNTAX_ERROR)
            .errmsg("too many column names were specified".to_string());
    });
    unreachable!("ereport(ERROR) diverges");
}

/// PG `CreateTableAsRelExists`: check whether the CTAS target already exists. Returns
/// true if it does (and skips per IF NOT EXISTS); raises "already exists" otherwise.
/// (PG's sync `get_relname_relid` is an async catalog lookup here.)
async fn create_table_as_rel_exists(shared: &Arc<SharedState>, ctas: &CreateTableAsStmt) -> bool {
    let into = ctas
        .into
        .as_ref()
        .unwrap_or_else(|| unreachable!("a CreateTableAsStmt carries its IntoClause"));
    let rel = into
        .rel
        .as_ref()
        .unwrap_or_else(|| unreachable!("an IntoClause names its target relation"));
    let relname = rel.relname.as_deref().unwrap_or("");

    let oldrelid = crate::backend::catalog::namespace::range_var_get_relid(
        shared,
        rel.schemaname.as_deref(),
        relname,
    )
    .await;

    if oldrelid.is_some() {
        if !ctas.if_not_exists {
            crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
                e.errcode(crate::utils::errcodes::ERRCODE_DUPLICATE_TABLE)
                    .errmsg(format!("relation \"{relname}\" already exists"));
            });
        }
        // Relation exists and IF NOT EXISTS was given: skip with a NOTICE.
        crate::ereport!(crate::utils::elog::NOTICE, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_DUPLICATE_TABLE)
                .errmsg(format!("relation \"{relname}\" already exists, skipping"));
        });
        return true;
    }
    false
}

/// PG `ExecCreateTableAs`: execute a CREATE TABLE AS / SELECT INTO command. Returns
/// the address of the created relation (or the invalid address when IF NOT EXISTS
/// skips an existing one).
///
/// WITH NO DATA builds the relation from the query's target list without running the
/// plan (`create_ctas_nodata`). WITH DATA plans + runs the contained SELECT into a
/// `DR_intorel` receiver, creates the relation from the result descriptor, and
/// inserts the materialized rows. The EXECUTE source and MATERIALIZED VIEW REFRESH
/// paths grow with PREPARE-into-CTAS and matviews.
pub async fn ExecCreateTableAs(
    shared: &Arc<SharedState>,
    stmt: &CreateTableAsStmt,
    query_string: &str,
    qc: Option<&mut crate::tcop::cmdtag::QueryCompletion>,
) -> ObjectAddress {
    // Check whether the relation already exists (IF NOT EXISTS short-circuits).
    if create_table_as_rel_exists(shared, stmt).await {
        return INVALID_OBJECT_ADDRESS;
    }

    let into = stmt
        .into
        .as_ref()
        .unwrap_or_else(|| unreachable!("a CreateTableAsStmt carries its IntoClause"));

    // The contained query was analyzed into a Node::Query by transformCreateTableAsStmt.
    let query = match stmt.query.as_ref() {
        Some(Node::Query(q)) => q.as_ref(),
        _ => unreachable!("ExecCreateTableAs: CTAS query was analyzed into a Query"),
    };
    if query.commandType != CmdType::SELECT {
        // The EXECUTE source (CMD_UTILITY ExecuteStmt) is not reachable yet.
        crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("CREATE TABLE AS ... EXECUTE is not yet supported".to_string());
        });
    }

    if into.skipData {
        // WITH NO DATA: build the relation from the target list, no execution. PG no
        // longer sets a tuples-processed count for CTAS ... WITH NO DATA -- the qc
        // keeps the CreateCommandTag default (CREATE TABLE AS / SELECT INTO).
        return create_ctas_nodata(shared, &query.targetList, into).await;
    }

    // WITH DATA: plan + run the SELECT into a DR_intorel receiver, then create the
    // relation from the result descriptor and insert the collected rows.
    let mut plan_query = (*query).clone();
    let plan = crate::backend::optimizer::plan::planner::standard_planner(
        &mut plan_query,
        query_string,
        CTAS_CURSOR_OPTIONS,
        None,
    );

    let (receiver, stash) = create_into_rel_dest_receiver(into.clone());
    let processed =
        crate::backend::tcop::postgres::execute_plan_into(shared, &plan, query_string, None, receiver, 0)
            .await;

    let (mut store, tupdesc) = stash
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .take()
        .unwrap_or_else(|| unreachable!("DR_intorel stashed its store at shutdown"));
    let tupdesc = tupdesc
        .unwrap_or_else(|| unreachable!("DR_intorel captured the result descriptor at startup"));

    // Create the relation from the query's result rowtype.
    let attr_list = attr_list_from_tupdesc(&tupdesc, into);
    let address = create_ctas_internal(shared, attr_list, into).await;

    // Open the freshly-created relation and insert every collected row.
    insert_store_into_rel(shared, address.objectId, &tupdesc, &mut store).await;

    if let Some(qc) = qc {
        qc.set(select_into_or_ctas_tag(stmt), processed);
    }

    address
}

/// Drain the materialized store into the target relation via `heap_insert`. The
/// relation is opened by OID (its relcache entry was built by DefineRelation) and
/// closed afterward.
async fn insert_store_into_rel(
    shared: &Arc<SharedState>,
    relid: Oid,
    tupdesc: &TupleDesc,
    store: &mut Tuplestorestate,
) {
    use crate::backend::utils::cache::relcache::{
        relation_build_desc, relation_close, relation_id_get_relation,
    };

    let rel = match relation_id_get_relation(relid) {
        Some(rel) => rel,
        None => relation_build_desc(shared, relid)
            .await
            .unwrap_or_else(|| unreachable!("CTAS relation {relid:?} just created")),
    };

    let cid = crate::backend::access::transam::xact::GetCurrentCommandId(true);
    let natts = tupdesc.natts as usize;
    let mut slot = crate::backend::executor::execTuples::make_single_tuple_table_slot(
        Some(tupdesc.clone()),
        &crate::backend::executor::execTuples::TTS_OPS_VIRTUAL,
    );

    while crate::backend::utils::sort::tuplestore::tuplestore_gettupleslot(store, true, false, &mut slot) {
        crate::executor::tuptable::slot_getallattrs(&mut slot);
        let values: Vec<Datum> = slot.values[..natts].to_vec();
        let isnull: Vec<bool> = slot.isnull[..natts].to_vec();
        let mut tuple = heap_form_tuple(tupdesc, &values, &isnull);
        Box::pin(heap_insert(shared, &rel, &mut tuple, cid, 0)).await;
        heap_freetuple(tuple);
    }

    relation_close(rel);
}

/// PG `ExecCreateTableAs`'s `SetQueryCompletion` tag: it reports `CMDTAG_SELECT` with
/// the query's rowcount for BOTH plain CTAS and SELECT INTO (the legacy CTAS behavior
/// -- the completion message the client sees is "SELECT n").
fn select_into_or_ctas_tag(_stmt: &CreateTableAsStmt) -> crate::tcop::cmdtaglist::CommandTag {
    crate::tcop::cmdtaglist::CommandTag::Select
}
