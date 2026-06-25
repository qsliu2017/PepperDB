//! Translated from PostgreSQL src/include/replication/pgoutput.h
//! Logical replication output plugin data.

use crate::catalog::pg_publication::Publication;
use crate::nodes::memnodes::MemoryContext;

pub struct PGOutputData {
    pub context: MemoryContext,  // private context for transient allocations
    pub cachectx: MemoryContext, // private context for cache data
    pub pubctx: MemoryContext,   // private context for publication data

    pub in_streaming: bool, // true while streaming a chunk of a transaction

    /* client-supplied info: */
    pub protocol_version: u32,
    pub publication_names: Vec<String>,
    pub publications: Vec<Publication>,
    pub binary: bool,
    pub streaming: u8, // a char-coded streaming mode, not a flag
    pub messages: bool,
    pub two_phase: bool,
    pub publish_no_origin: bool,
}
