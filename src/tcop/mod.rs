//! Traffic-cop / command-dispatch layer (postgres/src/backend/tcop +
//! postgres/src/include/tcop).
//!
//! The destination-receiver abstraction (dest) comes first; the postgres.c main
//! loop and command dispatch are future work.

pub mod cmdtag;
pub mod cmdtaglist;
pub mod deparse_utility;
pub mod dest;
pub mod fastpath;
pub mod tcopprot;
pub mod backend_startup;
pub mod postgres;
pub mod utility;
