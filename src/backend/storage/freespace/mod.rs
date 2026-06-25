//! Directory module: src/backend/storage/freespace
//!
//! The Free Space Map: `fsmpage` (single-page binary max-tree ops), `freespace`
//! (the 3-level FSM-fork addressing + heap-insert record/get path through the
//! buffer manager), and `indexfsm` (the free/used index-page wrappers).

pub mod freespace;
pub mod fsmpage;
pub mod indexfsm;
