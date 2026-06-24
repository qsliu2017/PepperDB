//! Translated from PostgreSQL src/include/executor/tstoreReceiver.h

use crate::access::tupdesc::TupleDesc;
use crate::tcop::dest::DestReceiver;
use crate::utils::palloc::MemoryContext;
use crate::utils::tuplestore::Tuplestorestate;

/// CreateTuplestoreDestReceiver: a DestReceiver that stows tuples in a
/// tuplestore. C returns `DestReceiver *`; here it is a boxed trait object.
pub fn create_tuplestore_dest_receiver() -> Box<dyn DestReceiver> {
    unimplemented!()
}

/// SetTuplestoreDestReceiverParams: bind the receiver to its target store. The
/// C `const char *map_failure_msg` becomes `Option<&str>` (NULL -> None).
pub fn set_tuplestore_dest_receiver_params(
    _self_: &mut dyn DestReceiver,
    _t_store: &mut Tuplestorestate,
    _t_context: MemoryContext,
    _detoast: bool,
    _target_tupdesc: TupleDesc,
    _map_failure_msg: Option<&str>,
) {
    unimplemented!()
}
