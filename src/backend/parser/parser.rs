//! Translated from PostgreSQL src/backend/parser/parser.c
//!
//! Driver for the "raw" parser (the flex+bison phases). `raw_parser` feeds the
//! logos lexer (scan.rs) into the lalrpop grammar (gram.lalrpop) and returns the
//! list of RawStmt nodes. This module also holds the small `makeXxxConst` /
//! `makeRawStmt` node constructors that gram.y keeps as static helpers at the foot
//! of the grammar file; lalrpop semantic actions cannot hold Rust fn bodies as
//! cleanly as bison, so they live here and the grammar calls them.

use crate::nodes::nodes::Node;
use crate::nodes::parsenodes::{
    A_Const, A_Star, ColumnRef, ColumnRefField, RawStmt, ResTarget, ValUnion,
};
use crate::nodes::value::{makeFloat, makeInteger, makeString};
use crate::parser::parser::RawParseMode;

// The parse tree's node-tree currency is the `Node` enum value itself: each
// variant already boxes its payload (`Node::A_Const(Box<A_Const>)`), so a `Node`
// is one pointer wide and needs no second box at the field/list level. gram.y's
// `Node *` thus maps to `Node`, and its `List *` of nodes to `Vec<Node>`.

/// gram.y `ResTarget *`.
pub type ResTargetBox = Box<ResTarget>;
/// gram.y `SelectStmt *`.
pub type SelectStmtBox = Box<crate::nodes::parsenodes::SelectStmt>;
/// gram.y `List *` of RawStmt (the parser result).
pub type RawStmtVec = Vec<Node>;

/// PG `makeIntConst`: an A_Const holding a T_Integer value.
pub fn make_int_const(val: i32) -> Node {
    a_const(ValUnion::Integer(makeInteger(val)))
}

/// PG `makeFloatConst`: an A_Const holding a T_Float value (kept as its text).
pub fn make_float_const(text: String) -> Node {
    a_const(ValUnion::Float(makeFloat(text)))
}

/// PG `makeStringConst`: an A_Const holding a T_String value.
pub fn make_string_const(text: String) -> Node {
    a_const(ValUnion::String(makeString(text)))
}

/// Build an A_Const node from an already-constructed value (location currently -1
/// pending location threading through the lexer; PG passes the `@N` token loc).
fn a_const(val: ValUnion) -> Node {
    Node::A_Const(Box::new(A_Const { val, isnull: false, location: -1 }))
}

/// PG `doNegate` for a numeric A_Const operand: an integer flips its sign in
/// place; a float gets a leading '-' prepended to its text (matching gram.y's
/// `doNegateFloat`). The general case (`- arg` -> a '-' operator OpExpr over a
/// non-constant) needs the A_Expr/operator path, deferred to M3.
pub fn do_negate(arg: Node) -> Node {
    let Node::A_Const(mut c) = arg else {
        unimplemented!("unary minus over non-constant: A_Expr operator path deferred to M3");
    };
    match &mut c.val {
        ValUnion::Integer(i) => i.ival = -i.ival,
        ValUnion::Float(f) => {
            if f.fval.starts_with('-') {
                f.fval.remove(0);
            } else {
                f.fval.insert(0, '-');
            }
        }
        _ => unimplemented!("unary minus over a non-numeric constant: deferred to M3"),
    }
    Node::A_Const(c)
}

/// gram.y `target_el: '*'` - a ResTarget whose value is a ColumnRef of one A_Star.
pub fn make_star_target() -> Node {
    let cr = ColumnRef {
        fields: vec![ColumnRefField::Star(A_Star {})],
        location: -1,
    };
    let rt = ResTarget {
        name: None,
        indirection: Vec::new(),
        val: Some(Node::ColumnRef(Box::new(cr))),
        location: -1,
    };
    Node::ResTarget(Box::new(rt))
}

/// PG `makeRawStmt`: wrap a top-level statement node in a RawStmt. The
/// stmt_location / stmt_len text bookkeeping that gram.y threads from `@N` is
/// added when location tracking is wired through the lexer.
pub fn make_raw_stmt(stmt: Node) -> Node {
    Node::RawStmt(Box::new(RawStmt {
        stmt: Some(stmt),
        stmt_location: 0,
        stmt_len: 0,
    }))
}

/// PG `raw_parser`: parse `s` into a list of RawStmt nodes. Only RAW_PARSE_DEFAULT
/// (a `;`-separated command list) is supported in M1; the MODE_* alternate entry
/// points (type-name / plpgsql) are added with that grammar.
///
/// A lexing or grammar error raises `ereport(ERROR, syntax error)` through the
/// normal unwind path (error.md s3) - never a bare panic.
pub fn raw_parser(s: &str, mode: RawParseMode) -> RawStmtVec {
    if mode != RawParseMode::Default {
        unimplemented!("raw_parser modes other than DEFAULT are deferred");
    }

    let tokens = crate::backend::parser::scan::lex(s);
    match crate::backend::parser::gram::StmtmultiParser::new().parse(tokens) {
        Ok(list) => list,
        Err(err) => syntax_error(s, &err),
    }
}

/// Raise a PG-style "syntax error" for a lalrpop parse error, locating it like
/// PG's `scanner_yyerror`. Diverges (>= ERROR).
fn syntax_error(
    src: &str,
    err: &lalrpop_util::ParseError<
        i32,
        crate::backend::parser::scan::Token,
        crate::backend::parser::scan::LexError,
    >,
) -> ! {
    use lalrpop_util::ParseError;
    let loc: usize = match err {
        ParseError::InvalidToken { location }
        | ParseError::UnrecognizedToken { token: (location, _, _), .. }
        | ParseError::ExtraToken { token: (location, _, _) } => *location as usize,
        ParseError::UnrecognizedEof { .. } => src.len(),
        ParseError::User { error } => error.location as usize,
    };
    let at = if loc < src.len() {
        format!(" at or near \"{}\"", &src[loc..])
    } else {
        " at end of input".to_string()
    };
    crate::ereport!(crate::utils::elog::ERROR, |e: &mut crate::utils::elog::ErrorData| {
        e.errcode(crate::utils::errcodes::ERRCODE_SYNTAX_ERROR)
            .errmsg(format!("syntax error{at}"));
    });
    unreachable!("ereport(ERROR) diverges");
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(s: &str) -> RawStmtVec {
        raw_parser(s, RawParseMode::Default)
    }

    /// Unwrap the single SelectStmt out of a one-statement parse.
    fn one_select(list: &RawStmtVec) -> &crate::nodes::parsenodes::SelectStmt {
        assert_eq!(list.len(), 1, "expected exactly one RawStmt");
        let Node::RawStmt(rs) = &list[0] else { panic!("not a RawStmt") };
        let Some(stmt) = &rs.stmt else { panic!("empty RawStmt") };
        let Node::SelectStmt(sel) = stmt else { panic!("not a SelectStmt") };
        sel
    }

    fn int_of_restarget(node: &Node) -> i32 {
        let Node::ResTarget(rt) = node else { panic!("not a ResTarget") };
        let Some(val) = &rt.val else { panic!("ResTarget has no val") };
        let Node::A_Const(c) = val else { panic!("ResTarget val not A_Const") };
        let ValUnion::Integer(i) = &c.val else { panic!("A_Const not Integer") };
        i.ival
    }

    #[test]
    fn select_one() {
        let list = parse("SELECT 1");
        let sel = one_select(&list);
        assert_eq!(sel.targetList.len(), 1);
        assert_eq!(int_of_restarget(&sel.targetList[0]), 1);
    }

    #[test]
    fn select_one_trailing_semicolon() {
        let list = parse("SELECT 1;");
        let sel = one_select(&list);
        assert_eq!(int_of_restarget(&sel.targetList[0]), 1);
    }

    #[test]
    fn two_statements() {
        let list = parse("SELECT 1; SELECT 2;");
        assert_eq!(list.len(), 2);
        // each is a SelectStmt
        for (i, stmt) in list.iter().enumerate() {
            let Node::RawStmt(rs) = stmt else { panic!("not RawStmt") };
            let Node::SelectStmt(sel) = rs.stmt.as_ref().unwrap() else { panic!() };
            assert_eq!(int_of_restarget(&sel.targetList[0]), (i + 1) as i32);
        }
    }

    #[test]
    fn select_large_literal() {
        let list = parse("SELECT 42");
        assert_eq!(int_of_restarget(&one_select(&list).targetList[0]), 42);
    }

    #[test]
    fn select_negative_literal() {
        let list = parse("SELECT -7");
        assert_eq!(int_of_restarget(&one_select(&list).targetList[0]), -7);
    }

    #[test]
    fn select_overflow_literal_is_float() {
        let list = parse("SELECT 9999999999");
        let sel = one_select(&list);
        let Node::ResTarget(rt) = &sel.targetList[0] else { panic!() };
        let Node::A_Const(c) = rt.val.as_ref().unwrap() else { panic!() };
        assert!(matches!(&c.val, ValUnion::Float(f) if f.fval == "9999999999"));
    }

    #[test]
    fn select_star() {
        let list = parse("SELECT *");
        let sel = one_select(&list);
        let Node::ResTarget(rt) = &sel.targetList[0] else { panic!("not ResTarget") };
        let Node::ColumnRef(cr) = rt.val.as_ref().unwrap() else { panic!("not ColumnRef") };
        assert_eq!(cr.fields.len(), 1);
        assert!(matches!(&cr.fields[0], ColumnRefField::Star(_)));
    }

    #[test]
    fn empty_input_is_empty_list() {
        assert!(parse("").is_empty());
        assert!(parse(";").is_empty());
    }

    #[test]
    fn syntax_error_select_from() {
        // SELECT FROM has no FROM production yet -> grammar error -> ereport(ERROR),
        // which raises via panic_any(ErrorData) (not a bare string panic). Catch it
        // and confirm it is a structured syntax error, not an unwrap/index panic.
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let res = std::panic::catch_unwind(|| parse("SELECT FROM"));
        std::panic::set_hook(prev);

        let payload = res.expect_err("SELECT FROM must raise a parse error");
        let edata = payload
            .downcast_ref::<crate::utils::elog::ErrorData>()
            .expect("panic payload should be a structured ErrorData");
        assert_eq!(edata.elevel, crate::utils::elog::ERROR);
        assert_eq!(edata.sqlerrcode, crate::utils::errcodes::ERRCODE_SYNTAX_ERROR);
        assert!(edata.message.as_deref().unwrap_or("").contains("syntax error"));
    }
}
