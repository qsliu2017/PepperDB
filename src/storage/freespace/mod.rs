//! Free Space Map (postgres/src/backend/storage/freespace).
//!
//! So far: the per-page FSM binary-tree logic (`fsmpage`) and the index-AM
//! free-space wrappers (`indexfsm`).

pub mod fsmpage;
pub mod indexfsm;
pub mod freespace;
