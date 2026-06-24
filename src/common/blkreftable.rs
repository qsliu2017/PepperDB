//! Translated from PostgreSQL src/include/common/blkreftable.h
//! Block reference tables: track which blocks were modified within an LSN range.

use crate::common::relpath::ForkNumber;
use crate::storage::block::BlockNumber;
use crate::storage::relfilelocator::RelFileLocator;

/// Magic number for serialization file format.
pub const BLOCKREFTABLE_MAGIC: u32 = 0x652b_137b;

/// Opaque in-memory block reference table.
pub struct BlockRefTable;
/// Opaque per-(relation,fork) entry.
pub struct BlockRefTableEntry;
/// Opaque incremental on-disk reader.
pub struct BlockRefTableReader;
/// Opaque incremental on-disk writer.
pub struct BlockRefTableWriter;

// io_callback_fn / report_error_fn (C function pointers + void *callback_arg) ->
// closures (function-mapping.md 6.3). The io closure returns the byte count;
// the error closure does not return (panics / aborts in C). `// TODO(panic)`.

/* Manipulating an entire in-memory block reference table. */
pub fn create_empty_block_ref_table() -> BlockRefTable {
    unimplemented!()
}

pub fn block_ref_table_set_limit_block(
    _brtab: &mut BlockRefTable,
    _rlocator: &RelFileLocator,
    _forknum: ForkNumber,
    _limit_block: BlockNumber,
) {
    unimplemented!()
}

pub fn block_ref_table_mark_block_modified(
    _brtab: &mut BlockRefTable,
    _rlocator: &RelFileLocator,
    _forknum: ForkNumber,
    _blknum: BlockNumber,
) {
    unimplemented!()
}

pub fn write_block_ref_table(
    _brtab: &BlockRefTable,
    _write_callback: impl FnMut(&[u8]) -> i32,
) {
    unimplemented!()
}

/// NULL return -> None; the `*limit_block` out-param folds into the tuple.
pub fn block_ref_table_get_entry<'a>(
    _brtab: &'a BlockRefTable,
    _rlocator: &RelFileLocator,
    _forknum: ForkNumber,
) -> Option<(&'a BlockRefTableEntry, BlockNumber)> {
    unimplemented!()
}

/// Fills `blocks` and returns the count written.
pub fn block_ref_table_entry_get_blocks(
    _entry: &BlockRefTableEntry,
    _start_blkno: BlockNumber,
    _stop_blkno: BlockNumber,
    _blocks: &mut [BlockNumber],
) -> i32 {
    unimplemented!()
}

/* Reading a block reference table incrementally from disk. */
pub fn create_block_ref_table_reader(
    _read_callback: impl FnMut(&mut [u8]) -> i32,
    _error_filename: &str,
    _error_callback: impl FnMut(&str),
) -> BlockRefTableReader {
    unimplemented!()
}

/// bool + out-params -> Option of the per-relation tuple.
pub fn block_ref_table_reader_next_relation(
    _reader: &mut BlockRefTableReader,
) -> Option<(RelFileLocator, ForkNumber, BlockNumber)> {
    unimplemented!()
}

pub fn block_ref_table_reader_get_blocks(
    _reader: &mut BlockRefTableReader,
    _blocks: &mut [BlockNumber],
) -> u32 {
    unimplemented!()
}

pub fn destroy_block_ref_table_reader(_reader: BlockRefTableReader) {
    unimplemented!()
}

/* Writing a block reference table incrementally to disk. */
pub fn create_block_ref_table_writer(
    _write_callback: impl FnMut(&[u8]) -> i32,
) -> BlockRefTableWriter {
    unimplemented!()
}

pub fn block_ref_table_write_entry(_writer: &mut BlockRefTableWriter, _entry: &BlockRefTableEntry) {
    unimplemented!()
}

pub fn destroy_block_ref_table_writer(_writer: BlockRefTableWriter) {
    unimplemented!()
}

pub fn create_block_ref_table_entry(
    _rlocator: RelFileLocator,
    _forknum: ForkNumber,
) -> BlockRefTableEntry {
    unimplemented!()
}

pub fn block_ref_table_entry_set_limit_block(
    _entry: &mut BlockRefTableEntry,
    _limit_block: BlockNumber,
) {
    unimplemented!()
}

pub fn block_ref_table_entry_mark_block_modified(
    _entry: &mut BlockRefTableEntry,
    _forknum: ForkNumber,
    _blknum: BlockNumber,
) {
    unimplemented!()
}

pub fn block_ref_table_free_entry(_entry: BlockRefTableEntry) {
    unimplemented!()
}
