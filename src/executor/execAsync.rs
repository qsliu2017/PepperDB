//! Translated from PostgreSQL src/include/executor/execAsync.h

use crate::nodes::execnodes::{AsyncRequest, TupleTableSlot};

pub fn ExecAsyncRequest(_areq: &mut AsyncRequest) {
    unimplemented!()
}

pub fn ExecAsyncConfigureWait(_areq: &mut AsyncRequest) {
    unimplemented!()
}

pub fn ExecAsyncNotify(_areq: &mut AsyncRequest) {
    unimplemented!()
}

pub fn ExecAsyncResponse(_areq: &mut AsyncRequest) {
    unimplemented!()
}

pub fn ExecAsyncRequestDone(_areq: &mut AsyncRequest, _result: Option<Box<TupleTableSlot>>) {
    unimplemented!()
}

pub fn ExecAsyncRequestPending(_areq: &mut AsyncRequest) {
    unimplemented!()
}
