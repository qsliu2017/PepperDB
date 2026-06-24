//! Translated from PostgreSQL src/include/fe_utils/simple_list.h
//
// Simple frontend lists. Per the container table these collapse to Vec-backed
// types; the C head/tail singly-linked cells disappear. The append API is
// implemented inline; membership/destroy follow.

use crate::postgres_ext::Oid;

#[derive(Debug, Default, Clone)]
pub struct SimpleOidList {
    pub items: Vec<Oid>,
}

impl SimpleOidList {
    pub fn append(&mut self, val: Oid) {
        self.items.push(val);
    }
    pub fn member(&self, val: Oid) -> bool {
        self.items.contains(&val)
    }
    pub fn destroy(&mut self) {
        self.items.clear();
    }
}

// String list cells carry a `touched` flag (set when searched).
#[derive(Debug, Clone)]
pub struct SimpleStringListCell {
    pub touched: bool,
    pub val: String,
}

#[derive(Debug, Default, Clone)]
pub struct SimpleStringList {
    pub items: Vec<SimpleStringListCell>,
}

impl SimpleStringList {
    pub fn append(&mut self, val: &str) {
        self.items.push(SimpleStringListCell { touched: false, val: val.to_string() });
    }
    /// Sets `touched` on a match and returns whether it was present.
    pub fn member(&mut self, val: &str) -> bool {
        for cell in &mut self.items {
            if cell.val == val {
                cell.touched = true;
                return true;
            }
        }
        false
    }
    pub fn destroy(&mut self) {
        self.items.clear();
    }
    /// First entry never `touched` by a search, or `None`.
    pub fn not_touched(&self) -> Option<&str> {
        self.items.iter().find(|c| !c.touched).map(|c| c.val.as_str())
    }
}

// SimplePtrList stored arbitrary `void *`; a homogeneous element type per use
// site maps to a generic Vec<T>.
#[derive(Debug, Default, Clone)]
pub struct SimplePtrList<T> {
    pub items: Vec<T>,
}

impl<T> SimplePtrList<T> {
    pub fn append(&mut self, ptr: T) {
        self.items.push(ptr);
    }
    pub fn destroy(&mut self) {
        self.items.clear();
    }
}
