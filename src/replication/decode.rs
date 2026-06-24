//! Translated from PostgreSQL src/include/replication/decode.h
//! PostgreSQL WAL to logical transformation.

use crate::access::xlogdefs::XLogRecPtr;
use crate::access::xlogreader::XLogReaderState;
use crate::replication::logical::LogicalDecodingContext;

pub struct XLogRecordBuffer {
    pub origptr: XLogRecPtr,
    pub endptr: XLogRecPtr,
    pub record: XLogReaderState,
}

pub fn xlog_decode(_ctx: &mut LogicalDecodingContext, _buf: &mut XLogRecordBuffer) {
    unimplemented!()
}

pub fn heap_decode(_ctx: &mut LogicalDecodingContext, _buf: &mut XLogRecordBuffer) {
    unimplemented!()
}

pub fn heap2_decode(_ctx: &mut LogicalDecodingContext, _buf: &mut XLogRecordBuffer) {
    unimplemented!()
}

pub fn xact_decode(_ctx: &mut LogicalDecodingContext, _buf: &mut XLogRecordBuffer) {
    unimplemented!()
}

pub fn standby_decode(_ctx: &mut LogicalDecodingContext, _buf: &mut XLogRecordBuffer) {
    unimplemented!()
}

pub fn logicalmsg_decode(_ctx: &mut LogicalDecodingContext, _buf: &mut XLogRecordBuffer) {
    unimplemented!()
}

pub fn LogicalDecodingProcessRecord(
    _ctx: &mut LogicalDecodingContext,
    _record: &mut XLogReaderState,
) {
    unimplemented!()
}
