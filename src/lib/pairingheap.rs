//! Translated from PostgreSQL src/include/lib/pairingheap.h
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]
//
// A pairing heap. C uses an intrusive node (pairingheap_node embedded in the
// caller's struct) plus a comparator that gets a void* arg. Rust resists
// intrusive pointer soup; we model the same API over an arena of boxed nodes
// linked by indices, with the comparator captured as a closure. The public
// operations (add, first, remove_first, remove) keep their C meaning.
//
// For a max-heap the comparator returns Ordering::Less iff a < b. Element type
// `T` is whatever the caller stored (the "containing struct" in C).

use std::cmp::Ordering;

type NodeIdx = usize;

struct PhNode<T> {
    value: Option<T>, // None for a freed slot
    first_child: Option<NodeIdx>,
    next_sibling: Option<NodeIdx>,
    prev_or_parent: Option<NodeIdx>,
}

/// C: `pairingheap`. `ph_compare`/`ph_arg` collapse into one captured closure,
/// stored as a generic type param (static dispatch, no trait object).
pub struct PairingHeap<T, F: Fn(&T, &T) -> Ordering> {
    nodes: Vec<PhNode<T>>,
    free: Vec<NodeIdx>,
    root: Option<NodeIdx>,
    compare: F,
}

impl<T, F: Fn(&T, &T) -> Ordering> PairingHeap<T, F> {
    /// C: `pairingheap_allocate(compare, arg)`.
    pub fn allocate(compare: F) -> Self {
        Self {
            nodes: Vec::new(),
            free: Vec::new(),
            root: None,
            compare,
        }
    }

    /// C: `pairingheap_reset(h)`.
    pub fn reset(&mut self) {
        self.nodes.clear();
        self.free.clear();
        self.root = None;
    }

    /// C: `pairingheap_is_empty(h)`.
    pub fn is_empty(&self) -> bool {
        self.root.is_none()
    }

    /// C: `pairingheap_is_singular(h)`.
    pub fn is_singular(&self) -> bool {
        matches!(self.root, Some(r) if self.nodes[r].first_child.is_none())
    }

    /// C: `pairingheap_add(heap, node)`. Returns a handle usable with `remove`.
    pub fn add(&mut self, value: T) -> NodeIdx {
        let idx = self.alloc(value);
        self.root = Some(self.merge_nodes(self.root, Some(idx)));
        idx
    }

    /// C: `pairingheap_first(heap)` - peek the top element (NULL if empty).
    pub fn first(&self) -> Option<&T> {
        self.root.map(|r| self.nodes[r].value.as_ref().unwrap())
    }

    /// C: `pairingheap_remove_first(heap)` - pop the top element.
    pub fn remove_first(&mut self) -> Option<T> {
        let r = self.root?;
        let children = self.nodes[r].first_child;
        self.root = self.merge_children(children);
        Some(self.dealloc(r))
    }

    /// C: `pairingheap_remove(heap, node)` - remove an arbitrary node by handle.
    pub fn remove(&mut self, idx: NodeIdx) -> T {
        if self.root == Some(idx) {
            return self.remove_first().unwrap();
        }
        self.unlink(idx);
        let children = self.nodes[idx].first_child;
        let merged = self.merge_children(children);
        if let Some(m) = merged {
            self.root = Some(self.merge_nodes(self.root, Some(m)));
        }
        self.dealloc(idx)
    }

    fn alloc(&mut self, value: T) -> NodeIdx {
        let node = PhNode {
            value: Some(value),
            first_child: None,
            next_sibling: None,
            prev_or_parent: None,
        };
        if let Some(i) = self.free.pop() {
            self.nodes[i] = node;
            i
        } else {
            self.nodes.push(node);
            self.nodes.len() - 1
        }
    }

    fn dealloc(&mut self, idx: NodeIdx) -> T {
        self.free.push(idx);
        self.nodes[idx].first_child = None;
        self.nodes[idx].next_sibling = None;
        self.nodes[idx].prev_or_parent = None;
        self.nodes[idx].value.take().expect("dealloc of freed node")
    }

    fn unlink(&mut self, idx: NodeIdx) {
        let prev = self.nodes[idx].prev_or_parent;
        let next = self.nodes[idx].next_sibling;
        match prev {
            Some(p) if self.nodes[p].first_child == Some(idx) => {
                self.nodes[p].first_child = next;
            }
            Some(p) => {
                self.nodes[p].next_sibling = next;
            }
            None => {}
        }
        if let Some(n) = next {
            self.nodes[n].prev_or_parent = prev;
        }
        self.nodes[idx].next_sibling = None;
        self.nodes[idx].prev_or_parent = None;
    }

    fn merge_nodes(&mut self, a: Option<NodeIdx>, b: Option<NodeIdx>) -> NodeIdx {
        match (a, b) {
            (None, Some(b)) => b,
            (Some(a), None) => a,
            (Some(a), Some(b)) => {
                let va = self.nodes[a].value.as_ref().unwrap();
                let vb = self.nodes[b].value.as_ref().unwrap();
                let (parent, child) = if (self.compare)(va, vb) == Ordering::Less {
                    (b, a)
                } else {
                    (a, b)
                };
                let old_first = self.nodes[parent].first_child;
                self.nodes[child].next_sibling = old_first;
                if let Some(f) = old_first {
                    self.nodes[f].prev_or_parent = Some(child);
                }
                self.nodes[child].prev_or_parent = Some(parent);
                self.nodes[parent].first_child = Some(child);
                parent
            }
            (None, None) => unreachable!("merge_nodes called with two None"),
        }
    }

    fn merge_children(&mut self, mut first: Option<NodeIdx>) -> Option<NodeIdx> {
        let mut pairs: Vec<NodeIdx> = Vec::new();
        while let Some(c) = first {
            let next = self.nodes[c].next_sibling;
            self.nodes[c].next_sibling = None;
            self.nodes[c].prev_or_parent = None;
            pairs.push(c);
            first = next;
        }
        let mut merged: Option<NodeIdx> = None;
        let mut i = 0;
        while i < pairs.len() {
            let m = if i + 1 < pairs.len() {
                self.merge_nodes(Some(pairs[i]), Some(pairs[i + 1]))
            } else {
                pairs[i]
            };
            merged = Some(merged.map_or(m, |prev| self.merge_nodes(Some(prev), Some(m))));
            i += 2;
        }
        merged
    }
}
