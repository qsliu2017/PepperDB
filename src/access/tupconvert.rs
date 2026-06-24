//! Translated from PostgreSQL src/include/access/tupconvert.h
//! Tuple conversion support.

use crate::access::attmap::AttrMap;
use crate::access::htup::HeapTuple;
use crate::access::tupdesc::TupleDesc;
use crate::executor::tuptable::TupleTableSlot;
use crate::nodes::bitmapset::Bitmapset;
use crate::postgres::Datum;

/// In-memory: a mapping plus deconstruct/construct workspaces.
pub struct TupleConversionMap {
    pub indesc: TupleDesc,       // tupdesc for source rowtype
    pub outdesc: TupleDesc,      // tupdesc for result rowtype
    pub attrMap: Box<AttrMap>,   // indexes of input fields, or 0 for null
    pub invalues: Vec<Datum>,    // workspace for deconstructing source
    pub inisnull: Vec<bool>,
    pub outvalues: Vec<Datum>,   // workspace for constructing result
    pub outisnull: Vec<bool>,
}

pub fn convert_tuples_by_position(
    _indesc: TupleDesc,
    _outdesc: TupleDesc,
    _msg: &str,
) -> Box<TupleConversionMap> {
    unimplemented!()
}

pub fn convert_tuples_by_name(_indesc: TupleDesc, _outdesc: TupleDesc) -> Box<TupleConversionMap> {
    unimplemented!()
}

pub fn convert_tuples_by_name_attrmap(
    _indesc: TupleDesc,
    _outdesc: TupleDesc,
    _attr_map: &AttrMap,
) -> Box<TupleConversionMap> {
    unimplemented!()
}

pub fn execute_attr_map_tuple(_tuple: HeapTuple, _map: &mut TupleConversionMap) -> HeapTuple {
    unimplemented!()
}

pub fn execute_attr_map_slot<'a>(
    _attr_map: &AttrMap,
    _in_slot: &TupleTableSlot,
    _out_slot: &'a mut TupleTableSlot,
) -> &'a mut TupleTableSlot {
    unimplemented!()
}

pub fn execute_attr_map_cols(_attr_map: &AttrMap, _in_cols: &Bitmapset) -> Box<Bitmapset> {
    unimplemented!()
}

pub fn free_conversion_map(_map: Box<TupleConversionMap>) {
    unimplemented!()
}
