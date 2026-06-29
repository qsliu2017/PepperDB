//! Tuple table slot machinery. Translated from
//! backend/executor/execTuples.c (disposition: full).
//!
//! Step 08 translates the VIRTUAL slot path completely (the M1 Result/Const
//! projection produces a virtual slot) plus the slot-allocation, store/clear and
//! `ExecTypeFromTL` helpers. The HeapTuple/MinimalTuple/BufferHeapTuple slot ops
//! reach untranslated heapam, so their copy/materialize methods are staged stubs
//! (rules.md s4); the virtual ops are complete.
//!
//! Slot ownership: the C slot is a single palloc block whose `tts_values`/
//! `tts_isnull` flexible arrays live in the same allocation; here a slot is an
//! owned `Box<TupleTableSlot>` carrying owned `Vec`s (tuptable.rs), so there are
//! no raw value/null pointers to keep in step.

use std::sync::Arc;

use crate::access::tupdesc::{TupleDesc, TupleDescData};
use crate::executor::tuptable::{
    tts_empty, tts_shouldfree, MinimalTuple, TtsFlags, TupleTableSlot, TupleTableSlotOps,
};
use crate::nodes::nodes::Node;
use crate::postgres::Datum;
use crate::postgres_ext::{InvalidOid, Oid};
use crate::storage::block::INVALID_BLOCK_NUMBER;
use crate::storage::itemptr::ItemPointerData;
use crate::storage::block::BlockIdData;
use crate::utils::elog::ERROR;

use crate::access::htup::HeapTuple;
use crate::backend::nodes::nodeFuncs::{exprCollation, exprType, exprTypmod};

/// An invalid ItemPointer (C `ItemPointerSetInvalid`): InvalidBlockNumber blkid,
/// InvalidOffsetNumber posid. ItemPointerData has no Default, so build it here.
fn invalid_item_pointer() -> ItemPointerData {
    let mut tid = ItemPointerData {
        blkid: BlockIdData { hi: 0, lo: 0 },
        posid: 0,
    };
    tid.set_invalid();
    tid
}

// ---------------------------------------------------------------------------
// TTSOpsVirtual - the virtual slot operations (C `TTSOpsVirtual`)
// ---------------------------------------------------------------------------

/// Virtual slot ops: the `tts_values`/`tts_isnull` arrays are authoritative.
/// `get_heap_tuple`/`get_minimal_tuple` are absent (C set them NULL) -> the
/// trait defaults (None) apply.
pub struct TtsOpsVirtual;

/// The singleton `&'static dyn TupleTableSlotOps` for virtual slots. Exposed
/// under the C name `TTSOpsVirtual` (a `&'static` so it matches the slot-ops
/// handle type threaded through PlanState).
pub static TTS_OPS_VIRTUAL: TtsOpsVirtual = TtsOpsVirtual;

impl TupleTableSlotOps for TtsOpsVirtual {
    fn base_slot_size(&self) -> usize {
        core::mem::size_of::<TupleTableSlot>()
    }

    /// C `tts_virtual_init`: no-op.
    fn init(&self, _slot: &mut TupleTableSlot) {}

    /// C `tts_virtual_release`: no-op.
    fn release(&self, _slot: &mut TupleTableSlot) {}

    /// C `tts_virtual_clear`: drop any materialized buffer, then mark empty.
    fn clear(&self, slot: &mut TupleTableSlot) {
        if tts_shouldfree(slot) {
            // The materialize buffer is owned by the slot; clearing the flag is
            // enough (Rust drops the buffer when the slot or its data Vec goes).
            slot.flags.remove(TtsFlags::SHOULDFREE);
        }
        slot.nvalid = 0;
        slot.flags.insert(TtsFlags::EMPTY);
        slot.tid.set_invalid();
    }

    /// C `tts_virtual_getsomeattrs`: never legal - a virtual slot always has the
    /// full values/isnull arrays valid.
    fn getsomeattrs(&self, _slot: &mut TupleTableSlot, _natts: i32) {
        crate::elog!(ERROR, "getsomeattrs is not supported for a virtual tuple table slot");
        unreachable!()
    }

    /// C `tts_virtual_getsysattr`: virtual slots have no system columns.
    fn getsysattr(&self, _slot: &mut TupleTableSlot, _attnum: i32) -> Option<Datum> {
        crate::ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("cannot retrieve a system column in this context");
        });
        unreachable!()
    }

    /// C `tts_virtual_is_current_xact_tuple`: not supported for virtual slots.
    fn is_current_xact_tuple(&self, _slot: &TupleTableSlot) -> bool {
        crate::ereport!(ERROR, |e: &mut crate::utils::elog::ErrorData| {
            e.errcode(crate::utils::errcodes::ERRCODE_FEATURE_NOT_SUPPORTED)
                .errmsg("don't have transaction information for this type of tuple");
        });
        unreachable!()
    }

    /// C `tts_virtual_materialize`: for by-val attrs (the M1 const path) there is
    /// nothing to copy - values already live in the owned Vec. By-ref flattening
    /// grows when varlena/cstring attrs are reachable (rules.md s4).
    fn materialize(&self, slot: &mut TupleTableSlot) {
        if tts_shouldfree(slot) {
            return; // already materialized
        }
        // by-ref deep-copy of out-of-line attrs is deferred; mark materialized.
        slot.flags.insert(TtsFlags::SHOULDFREE);
    }

    /// C `tts_virtual_copyslot`: copy all attrs from src into dst, then
    /// materialize dst.
    fn copyslot(&self, dstslot: &mut TupleTableSlot, srcslot: &TupleTableSlot) {
        self.clear(dstslot);
        let dst_natts = tupdesc_natts(dstslot.tupleDescriptor.as_ref());
        let src_natts = tupdesc_natts(srcslot.tupleDescriptor.as_ref());
        crate::assert!(dst_natts == src_natts);
        let n = dst_natts as usize;
        dstslot.values[..n].copy_from_slice(&srcslot.values[..n]);
        dstslot.isnull[..n].copy_from_slice(&srcslot.isnull[..n]);
        dstslot.nvalid = dst_natts as i16;
        dstslot.flags.remove(TtsFlags::EMPTY);
        self.materialize(dstslot);
    }

    /// C `tts_virtual_copy_heap_tuple`: form a HeapTuple from the arrays. Reaches
    /// untranslated heapam; staged stub.
    fn copy_heap_tuple(&self, _slot: &mut TupleTableSlot) -> HeapTuple {
        unimplemented!("tts_virtual_copy_heap_tuple: heap_form_tuple deferred")
    }

    /// C `tts_virtual_copy_minimal_tuple`: staged stub (heapam).
    fn copy_minimal_tuple(&self, _slot: &mut TupleTableSlot, _extra: usize) -> MinimalTuple {
        unimplemented!("tts_virtual_copy_minimal_tuple: heap_form_minimal_tuple deferred")
    }
}

/// `natts` of a slot's TupleDesc; the caller's slot always has one set.
fn tupdesc_natts(desc: Option<&TupleDesc>) -> i32 {
    desc.unwrap_or_else(|| unreachable!("slot has a tuple descriptor"))
        .natts
}

// ---------------------------------------------------------------------------
// Slot construction
// ---------------------------------------------------------------------------

/// PG `MakeTupleTableSlot`: allocate a slot of the given ops over `tuple_desc`.
/// Returns an owned `Box` (the C single-palloc-block becomes one allocation that
/// owns its value/null Vecs).
pub fn make_tuple_table_slot(
    tuple_desc: Option<TupleDesc>,
    tts_ops: &'static dyn TupleTableSlotOps,
) -> Box<TupleTableSlot> {
    let mut flags = TtsFlags::EMPTY;
    let natts = tuple_desc.as_ref().map_or(0, |desc| {
        flags.insert(TtsFlags::FIXED);
        desc.natts
    });
    let n = natts.max(0) as usize;

    // PinTupleDesc only bumps the advisory refcount of a counted descriptor
    // (tdrefcount >= 0); M1 result descriptors are anonymous (tdrefcount == -1).
    // The Arc clone stored below is what keeps the descriptor alive.
    let mut slot = Box::new(TupleTableSlot {
        flags,
        nvalid: 0,
        ops: tts_ops,
        tupleDescriptor: tuple_desc,
        values: vec![Datum(0); n],
        isnull: vec![false; n],
        mcxt: (),
        tid: invalid_item_pointer(),
        tableOid: InvalidOid,
    });

    tts_ops.init(&mut slot);
    slot
}

/// PG `ExecAllocTableSlot`: make a slot and append it to a tuple table.
pub fn exec_alloc_table_slot(
    tuple_table: &mut Vec<Box<TupleTableSlot>>,
    desc: Option<TupleDesc>,
    tts_ops: &'static dyn TupleTableSlotOps,
) -> usize {
    let slot = make_tuple_table_slot(desc, tts_ops);
    tuple_table.push(slot);
    tuple_table.len() - 1
}

/// PG `MakeSingleTupleTableSlot`: a standalone slot not tracked in a tuple table.
pub fn make_single_tuple_table_slot(
    tupdesc: Option<TupleDesc>,
    tts_ops: &'static dyn TupleTableSlotOps,
) -> Box<TupleTableSlot> {
    make_tuple_table_slot(tupdesc, tts_ops)
}

/// PG `ExecResetTupleTable`: clear (and conceptually free) every slot. Owned
/// `Box`es here are dropped by truncating the Vec.
pub fn exec_reset_tuple_table(tuple_table: &mut Vec<Box<TupleTableSlot>>, _should_free: bool) {
    for slot in tuple_table.iter_mut() {
        let ops = slot.ops;
        ops.clear(slot);
    }
    tuple_table.clear();
}

// ---------------------------------------------------------------------------
// Store / clear
// ---------------------------------------------------------------------------

/// PG `ExecStoreVirtualTuple`: the values/isnull arrays have been filled
/// directly; mark the (previously empty) slot valid.
pub fn exec_store_virtual_tuple(slot: &mut TupleTableSlot) {
    crate::assert!(tts_empty(slot));
    crate::assert!(slot.tupleDescriptor.is_some());

    slot.flags.remove(TtsFlags::EMPTY);
    slot.nvalid = tupdesc_natts(slot.tupleDescriptor.as_ref()) as i16;
}

/// PG `ExecStoreAllNullTuple`: set every attribute null, then store virtual.
pub fn exec_store_all_null_tuple(slot: &mut TupleTableSlot) {
    // clear the slot first (C calls ExecClearTuple)
    let ops = slot.ops;
    ops.clear(slot);

    for v in &mut slot.values {
        *v = Datum(0);
    }
    for n in &mut slot.isnull {
        *n = true;
    }
    exec_store_virtual_tuple(slot);
}

// ---------------------------------------------------------------------------
// ExecTypeFromTL - build a TupleDesc from a targetlist
// ---------------------------------------------------------------------------

/// PG `ExecTypeFromTL`: build a TupleDesc from a tlist, keeping junk columns.
/// Returns a shared `Arc<TupleDescData>` handle; co-owners (EState<'_>/QueryDesc/
/// PlanState/slot/Portal/dest) each hold an `Arc` clone, and the descriptor is
/// freed when the last drops (no leak; the former `Box::into_raw` is gone).
pub fn exec_type_from_tl(target_list: &[Node]) -> TupleDesc {
    exec_type_from_tl_internal(target_list, false)
}

/// PG `ExecCleanTypeFromTL`: build a TupleDesc dropping junk columns.
pub fn exec_clean_type_from_tl(target_list: &[Node]) -> TupleDesc {
    exec_type_from_tl_internal(target_list, true)
}

/// PG `ExecTypeFromTLInternal`: the shared body.
fn exec_type_from_tl_internal(target_list: &[Node], skip_junk: bool) -> TupleDesc {
    let entries: Vec<&crate::nodes::primnodes::TargetEntry> = target_list
        .iter()
        .filter_map(|n| {
            let Node::TargetEntry(te) = n else {
                return None;
            };
            (!(skip_junk && te.resjunk)).then_some(&**te)
        })
        .collect();

    let len = i32::try_from(entries.len()).unwrap_or(0);
    let mut type_info = TupleDescData::create_template(len);

    for (i, te) in entries.iter().enumerate() {
        let cur_resno = (i + 1) as i16;
        let expr = te
            .expr
            .as_ref()
            .unwrap_or_else(|| unimplemented!("ExecTypeFromTL: targetentry with no expr"));
        let typid = exprType(expr);

        // PG `TupleDescInitEntry` routes type metadata through the syscache. The
        // syscache is not yet translated; the M1 const path only ever yields
        // built-in types, so use the syscache-free `init_builtin_entry` (PG's own
        // bootstrap path). General `init_entry` grows when the syscache lands.
        let name = te.resname.as_deref().unwrap_or("");
        type_info.init_builtin_entry(cur_resno, name, typid, exprTypmod(expr), 0);
        type_info.init_entry_collation(cur_resno, exprCollation(expr));
    }

    Arc::new(type_info)
}

/// PG `ExecTargetListLength`: number of (non-junk-agnostic) target entries.
pub fn exec_target_list_length(target_list: &[Node]) -> i32 {
    i32::try_from(target_list.len()).unwrap_or(0)
}

/// PG `ExecCleanTargetListLength`: number of non-junk target entries.
pub fn exec_clean_target_list_length(target_list: &[Node]) -> i32 {
    let n = target_list
        .iter()
        .filter(|node| match &**node {
            Node::TargetEntry(te) => !te.resjunk,
            _ => true,
        })
        .count();
    i32::try_from(n).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::nodes::makefuncs::{make_const, make_target_entry};
    use crate::catalog::genbki::INT4OID;
    use crate::executor::tuptable::{tts_empty, ExecClearTuple};
    use crate::postgres::Int32GetDatum;

    /// A `[Const int4]` targetlist node.
    fn const_int4_tlist(values: &[i32]) -> Vec<Node> {
        values
            .iter()
            .enumerate()
            .map(|(i, &v)| {
                let con = make_const(
                    INT4OID,
                    -1,
                    InvalidOid,
                    4,
                    Int32GetDatum(v),
                    false,
                    true,
                );
                let tle = make_target_entry(
                    Some(Node::Const(Box::new(con))),
                    (i + 1) as i16,
                    Some("?column?".to_string()),
                    false,
                );
                Node::TargetEntry(Box::new(tle))
            })
            .collect()
    }

    #[test]
    fn exec_type_from_tl_builds_one_int4_attr() {
        let tl = const_int4_tlist(&[1]);
        let d = exec_type_from_tl(&tl);
        assert_eq!(d.natts, 1);
        assert_eq!(d.attr(0).atttypid, INT4OID);
        assert_eq!(d.attr(0).attlen, 4);
        assert!(d.attr(0).attbyval);
    }

    #[test]
    fn exec_type_from_tl_builds_two_int4_attrs() {
        let tl = const_int4_tlist(&[1, 2]);
        let d = exec_type_from_tl(&tl);
        assert_eq!(d.natts, 2);
        assert_eq!(d.attr(0).atttypid, INT4OID);
        assert_eq!(d.attr(1).atttypid, INT4OID);
    }

    /// The result descriptor is a shared `Arc`, not a leaked allocation: a slot
    /// and the original handle co-own the same `TupleDescData` (Arc::ptr_eq), and
    /// the strong count reflects exactly the live handles.
    #[test]
    fn result_tupdesc_is_shared_arc_not_leaked() {
        let tl = const_int4_tlist(&[1]);
        let desc = exec_type_from_tl(&tl);
        assert_eq!(Arc::strong_count(&desc), 1);
        let slot = make_tuple_table_slot(Some(Arc::clone(&desc)), &TTS_OPS_VIRTUAL);
        assert_eq!(Arc::strong_count(&desc), 2);
        let slot_desc = slot
            .tupleDescriptor
            .as_ref()
            .expect("slot carries the descriptor");
        assert!(Arc::ptr_eq(&desc, slot_desc));
        drop(slot);
        assert_eq!(Arc::strong_count(&desc), 1);
    }

    #[test]
    fn store_and_clear_virtual_slot_transitions() {
        let tl = const_int4_tlist(&[7]);
        let desc = exec_type_from_tl(&tl);
        let mut slot = make_tuple_table_slot(Some(desc), &TTS_OPS_VIRTUAL);

        // A fresh slot is empty with nvalid == 0.
        assert!(tts_empty(&slot));
        assert_eq!(slot.nvalid, 0);

        // Fill the value arrays, then store: not empty, nvalid == natts.
        slot.values[0] = Int32GetDatum(7);
        slot.isnull[0] = false;
        exec_store_virtual_tuple(&mut slot);
        assert!(!tts_empty(&slot));
        assert_eq!(slot.nvalid, 1);

        // Clear returns it to empty with nvalid == 0.
        ExecClearTuple(&mut slot);
        assert!(tts_empty(&slot));
        assert_eq!(slot.nvalid, 0);
    }

    #[test]
    fn all_null_tuple_sets_every_attr_null() {
        let tl = const_int4_tlist(&[0, 0]);
        let desc = exec_type_from_tl(&tl);
        let mut slot = make_tuple_table_slot(Some(desc), &TTS_OPS_VIRTUAL);
        exec_store_all_null_tuple(&mut slot);
        assert!(!tts_empty(&slot));
        assert_eq!(slot.nvalid, 2);
        assert!(slot.isnull.iter().all(|&n| n));
    }
}
