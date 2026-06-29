//! Translated from PostgreSQL src/include/executor/nodeMaterial.h

use crate::nodes::execnodes::{EState, MaterialState};
use crate::nodes::plannodes::Material;

// TODO(ptr)
pub fn ExecInitMaterial(
    _node: &Material,
    _estate: &mut EState<'_>,
    _eflags: i32,
) -> *mut MaterialState {
    unimplemented!()
}

pub fn ExecEndMaterial(_node: &mut MaterialState) {
    unimplemented!()
}

pub fn ExecMaterialMarkPos(_node: &mut MaterialState) {
    unimplemented!()
}

pub fn ExecMaterialRestrPos(_node: &mut MaterialState) {
    unimplemented!()
}

pub fn ExecReScanMaterial(_node: &mut MaterialState) {
    unimplemented!()
}
