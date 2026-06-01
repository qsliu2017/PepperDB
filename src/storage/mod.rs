//! Backend `storage` subsystem (postgres/src/backend/storage + postgres/src/include/storage).
//!
//! Only the block/offset/item-pointer addressing types are present so far; the
//! buffer manager, smgr, locks, and WAL are not yet translated.

pub mod block;
pub mod buf;
pub mod buf_internals;
pub mod buffer;
pub mod bufpage;
pub mod checksum;
pub mod file;
pub mod freespace;
// TODO: aio_internal.h written (src/storage/aio_internal.rs) but deferred from the
// build until storage/aio.h + condition_variable.h + port/pg_iovec.h are ported
// (it imports PgAioOp/PgAioOpData/PgAioHandleState/ConditionVariable/iovec from them).
pub mod aio;
pub mod aio_internal;
pub mod aio_subsys;
pub mod aio_types;
pub mod io_worker;
pub mod ipc;
pub mod item;
pub mod itemid;
pub mod lmgr;
pub mod lockdefs;
pub mod itemptr;
pub mod large_object;
pub mod lwlocklist;
pub mod off;
pub mod smgr;
pub mod sync;
pub mod pg_sema;
pub mod pg_shmem;
pub mod predicate_internals;
pub mod relfilelocator;
pub mod proclist;
pub mod procnumber;
pub mod proclist_types;
pub mod spin;
pub mod standbydefs;
