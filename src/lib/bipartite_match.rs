//! Translated from PostgreSQL src/include/lib/bipartite_match.h
//!
//! Maximum-cardinality bipartite matching (Hopcroft-Karp). In-memory; the C
//! `short **adjacency` / `short *` arrays become Rust `Vec`s.

/// State for the bipartite matching algorithm.
pub struct BipartiteMatchState {
    // inputs:
    pub u_size: i32,            // size of U
    pub v_size: i32,            // size of V
    pub adjacency: Vec<Vec<i16>>, // adjacency[u] = [k, v1, v2, ..., vk]
    // outputs:
    pub matching: i32,    // number of edges in matching
    pub pair_uv: Vec<i16>, // pair_uv[u] -> v
    pub pair_vu: Vec<i16>, // pair_vu[v] -> u
    // private state for matching algorithm:
    pub distance: Vec<i16>, // distance[u]
    pub queue: Vec<i16>,    // queue storage for breadth search
}

/// BipartiteMatch: compute a maximum-cardinality matching.
pub fn BipartiteMatch(_u_size: i32, _v_size: i32, _adjacency: Vec<Vec<i16>>) -> BipartiteMatchState {
    unimplemented!()
}

/// BipartiteMatchFree: free the state (RAII; provided for parity).
pub fn BipartiteMatchFree(state: BipartiteMatchState) {
    drop(state);
}
