//! Translated from PostgreSQL src/include/executor/tablefunc.h
//
// TableFunc executor routine struct -> trait (per routine-struct.md). All
// callbacks are required except SetNamespace (may be NULL) -> default no-op.
// The C `struct TableFuncScanState *state` private context is modeled as
// `&mut self` owning the builder state.

use crate::postgres::Datum;
use crate::postgres_ext::Oid;

// TableFuncScanState is an executor node defined in nodes/execnodes.h (not in
// this batch); the builder threads its private state through `self`.
// TODO(struct-forward): repoint state to crate::nodes::execnodes::TableFuncScanState in Phase 2.

/// Function pointers used to generate content of table-producer functions such
/// as XMLTABLE. Implemented as a Rust trait; the table builder owns its state.
pub trait TableFuncRoutine {
    /// InitOpaque: initialize table-builder private objects for `natts` columns.
    fn init_opaque(&mut self, natts: i32);

    /// SetDocument: define the input document.
    fn set_document(&mut self, value: Datum);

    /// SetNamespace (optional, may be NULL): pass a namespace declaration. A
    /// `None` name is the default namespace.
    fn set_namespace(&mut self, _name: Option<&str>, _uri: &str) {}

    /// SetRowFilter: define the row-generating filter.
    fn set_row_filter(&mut self, path: &str);

    /// SetColumnFilter: define the column-generating filter for column `colnum`.
    fn set_column_filter(&mut self, path: &str, colnum: i32);

    /// FetchRow: advance to the next row; returns false when none remain.
    fn fetch_row(&mut self) -> bool;

    /// GetValue: value for `colnum` in the current row. The C `bool *isnull`
    /// out-param folds into `Option<Datum>` (None = SQL NULL).
    fn get_value(&mut self, colnum: i32, typid: Oid, typmod: i32) -> Option<Datum>;

    /// DestroyOpaque: release all builder resources.
    fn destroy_opaque(&mut self);
}
