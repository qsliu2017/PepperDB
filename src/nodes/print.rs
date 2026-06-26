//! Translated from PostgreSQL src/include/nodes/print.h

use crate::executor::tuptable::TupleTableSlot;
use crate::nodes::nodes::Node;

/// C: `#define nodeDisplay(x) pprint(x)`
pub fn node_display(obj: &Node) {
    pprint(obj);
}

pub fn print(_obj: &Node) {
    unimplemented!()
}
pub fn pprint(_obj: &Node) {
    unimplemented!()
}
pub fn elog_node_display(_lev: i32, _title: &str, _obj: &Node, _pretty: bool) {
    unimplemented!()
}
pub fn format_node_dump(_dump: &str) -> String {
    unimplemented!()
}
pub fn pretty_format_node_dump(_dump: &str) -> String {
    unimplemented!()
}
pub fn print_rt(_rtable: &[Box<Node>]) {
    unimplemented!()
}
pub fn print_expr(_expr: &Node, _rtable: &[Box<Node>]) {
    unimplemented!()
}
pub fn print_pathkeys(_pathkeys: &[Box<Node>], _rtable: &[Box<Node>]) {
    unimplemented!()
}
pub fn print_tl(_tlist: &[Box<Node>], _rtable: &[Box<Node>]) {
    unimplemented!()
}
pub fn print_slot(_slot: &mut TupleTableSlot) {
    unimplemented!()
}
