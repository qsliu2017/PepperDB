//! The `nodes` subsystem (postgres/src/backend/nodes + postgres/src/include/nodes).
//!
//! Tagged-node infrastructure (`Node`/`NodeTag`), the ubiquitous `List` type, and
//! `Bitmapset`, plus the (generated) copy/equal/out/read support.

pub mod bitmapset;
pub mod equalfuncs;
pub mod execnodes;
pub mod extensible;
pub mod lockoptions;
pub mod makefuncs;
pub mod miscnodes;
// TODO: copyfuncs.rs written but deferred - ManuallyDrop union-field access needs
// explicit deref + copyObjectImpl dedups with the pg_list.rs stub.
// pub mod copyfuncs;
pub mod multibitmapset;
pub mod nodeFuncs;
pub mod nodes;
pub mod params;
pub mod print;
pub mod queryjumble;
pub mod subscripting;
pub mod supportnodes;
pub mod parsenodes;
pub mod pathnodes;
pub mod pg_list;
pub mod plannodes;
pub mod primnodes;
pub mod read;
pub mod replnodes;
pub mod tidbitmap;
pub mod value;
pub mod readfuncs;
pub mod queryjumblefuncs;
