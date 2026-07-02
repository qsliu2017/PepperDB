//! Translated from PostgreSQL src/backend/parser/scan.l
//!
//! The core SQL lexer. PG generates this with flex; we use `logos` (a declarative
//! lexer generator) as the token source feeding the lalrpop grammar (gram.lalrpop)
//! in external-token mode. This file GROWS one token family at a time as each
//! milestone's grammar needs it - it is correct for every currently-reachable
//! token at all times (README "grow" discipline).
//!
//! M1 token set: the keyword family (resolved against `kwlist` like PG's
//! `base_yylex`), identifiers (IDENT), integer (ICONST), float (FCONST), string
//! (SCONST), and the punctuation `; , * ( ) . + -`. Operators, comments, dollar
//! quotes, bit/hex strings, typecast `::`, etc. are added at their milestones.

use logos::Logos;

use crate::common::keywords::scan_keywords;
use crate::parser::kwlist::Keyword;

/// A scanned token plus its byte location (the bison `@N` source location). The
/// grammar's `extern` block names `Token` as the terminal type; `Loc` is the byte
/// offset of the token start (PG `YYLTYPE`).
pub type Loc = i32;

/// One terminal produced by the lexer, mirroring the bison token set. Variants
/// carry the same payloads PG's `core_YYSTYPE` union carries for that token.
///
/// Keywords are NOT separate variants: a word that matches `kwlist` becomes
/// `Token::Keyword(canonical_spelling)` (PG's `base_yylex` keyword filtering),
/// and the grammar matches a specific keyword by its canonical string. This keeps
/// the enum small and growable - adding a keyword to the grammar needs no enum
/// change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    /// A reserved/unreserved keyword, by canonical (lowercase) spelling.
    Keyword(&'static str),
    /// IDENT - an identifier (already downcased/truncated, PG-style).
    Ident(String),
    /// ICONST - a non-negative integer literal that fits in i32.
    IConst(i32),
    /// FCONST - a numeric literal that is not an i32 ICONST (kept as its text).
    FConst(String),
    /// SCONST - a single-quoted string literal (contents, quotes stripped).
    SConst(String),
    /// PARAM - a `$n` positional parameter reference (PG scan.l `{param}`).
    Param(i32),

    // Punctuation / single-char operators (PG returns the char code; we name them).
    Semi,    // ;
    Comma,   // ,
    Star,    // *
    LParen,  // (
    RParen,  // )
    Dot,     // .
    Plus,    // +
    Minus,   // -
    Slash,   // /
    Percent, // %
    Lt,      // <
    Gt,      // >
    Eq,      // =
    Typecast, // ::  (PG TYPECAST)
    // Multi-character comparison operators (PG's LESS_EQUALS / GREATER_EQUALS /
    // NOT_EQUALS; `!=` is lexed as `<>` per PG scan.l).
    LessEquals,    // <=
    GreaterEquals, // >=
    NotEquals,     // <>  (also the spelling for !=)
    // PG lexes `||` as a generic {operator} (an `Op` terminal); this port names
    // the currently reachable multi-char operators individually.
    Concat, // ||
    Shl,    // <<
    Shr,    // >>
}

/// Raw token classes emitted by logos before keyword resolution and literal
/// post-processing. A separate enum so logos owns only the regex matching; the
/// keyword lookup and integer-overflow -> FCONST decision (PG `process_integer_literal`)
/// happen in `Lexerror`-free Rust afterward.
#[derive(Logos, Debug, Clone, PartialEq)]
#[logos(skip r"[ \t\n\r\x0c]+")]            // scanner_isspace whitespace
#[logos(skip r"--[^\n]*")]                   // SQL line comment
enum Raw {
    // PG {xcstart}..{xcstop}: C-style block comment, NESTED per SQL spec (PG
    // scan.l's <xc> state tracks depth). logos regexes cannot express nesting,
    // so the callback consumes up to the matching close and skips. An
    // unterminated comment consumes to EOF (the parser then reports a syntax
    // error at end of input; PG raises "unterminated /* comment" in the scanner).
    #[token("/*", skip_block_comment)]
    BlockComment,

    // identifier: PG `{ident_start}{ident_cont}*` for the ASCII case, plus UTF-8
    // letters (PG accepts high-bit bytes in identifiers under multibyte encodings).
    #[regex(r"[A-Za-z_\x80-\u{10FFFF}][A-Za-z_0-9$\x80-\u{10FFFF}]*", |lex| lex.slice().to_string())]
    Word(String),

    // integer: {digit}+ (PG decints). Sign is handled by the grammar (u_expr '-'),
    // never the lexer - matching PG, where '-' is its own token.
    #[regex(r"[0-9]+", |lex| lex.slice().to_string())]
    Integer(String),

    // float forms PG's {real}: digits with a '.' and/or exponent.
    #[regex(r"[0-9]+\.[0-9]*([eE][-+]?[0-9]+)?", |lex| lex.slice().to_string())]
    #[regex(r"\.[0-9]+([eE][-+]?[0-9]+)?", |lex| lex.slice().to_string())]
    #[regex(r"[0-9]+[eE][-+]?[0-9]+", |lex| lex.slice().to_string())]
    Float(String),

    // string: '...' with '' as an embedded quote (PG `{xqstart}`/`{xqdouble}`).
    // Escape strings (E'..'), dollar quotes, etc. are added at a later milestone.
    #[regex(r"'([^']|'')*'", |lex| unquote_sconst(lex.slice()))]
    String(String),

    // delimited identifier: "..." with "" as an embedded quote (PG `{xdstart}`/
    // `{xddouble}`). Case-preserving and NEVER keyword-folded (so `AS "TRUE"` and
    // `'a'::"char"` work). Empty `""` is a zero-length identifier (a lex error in
    // PG; not reachable in the covered tests).
    #[regex(r#""([^"]|"")*""#, |lex| unquote_dconst(lex.slice()))]
    QuotedIdent(String),

    // PARAM: PG `\${decdigit}+`. The digits after `$` are the parameter number;
    // overflowing i32 is a lex failure (PG's process_integer_literal errors on a
    // `$n` that exceeds int range).
    #[regex(r"\$[0-9]+", |lex| lex.slice()[1..].parse::<i32>().ok())]
    Param(i32),

    #[token(";")] Semi,
    #[token(",")] Comma,
    #[token("*")] Star,
    #[token("(")] LParen,
    #[token(")")] RParen,
    #[token(".")] Dot,
    #[token("+")] Plus,
    #[token("-")] Minus,
    #[token("/")] Slash,
    #[token("%")] Percent,
    // PG's TYPECAST (`::`). Listed before the single-char operators so logos's
    // longest-match picks it (there is no bare `:` token in the grammar yet).
    #[token("::")] Typecast,
    // Multi-char comparison operators first (logos picks the longest match, but
    // the spellings are listed explicitly to mirror PG scan.l's self/op rules).
    #[token("<=")] LessEquals,
    #[token(">=")] GreaterEquals,
    #[token("<>")] NotEquals,
    #[token("!=")] BangEquals,
    #[token("||")] Concat,
    // Bit shifts (generic {operator} in PG scan.l; named here). Listed before
    // the single-char `<`/`>` so logos's longest-match picks them.
    #[token("<<")] Shl,
    #[token(">>")] Shr,
    #[token("<")] Lt,
    #[token(">")] Gt,
    #[token("=")] Eq,
}

/// Consume a (nested) block comment body after the opening `/*` (PG scan.l xc).
fn skip_block_comment(lex: &mut logos::Lexer<Raw>) -> logos::Skip {
    let bytes = lex.remainder().as_bytes();
    let mut depth = 1usize;
    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'/' && bytes.get(i + 1) == Some(&b'*') {
            depth += 1;
            i += 2;
        } else if bytes[i] == b'*' && bytes.get(i + 1) == Some(&b'/') {
            depth -= 1;
            i += 2;
            if depth == 0 {
                break;
            }
        } else {
            i += 1;
        }
    }
    lex.bump(i);
    logos::Skip
}

/// Strip the surrounding double quotes and collapse doubled `""` to a single `"`.
fn unquote_dconst(slice: &str) -> String {
    slice[1..slice.len() - 1].replace("\"\"", "\"")
}

/// Strip the surrounding single quotes and collapse doubled `''` to a single `'`.
fn unquote_sconst(slice: &str) -> String {
    slice[1..slice.len() - 1].replace("''", "'")
}

/// A lexing failure carries the byte offset of the offending character (the bison
/// `@N` location), so the driver can raise a PG-style "syntax error at ...".
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LexError {
    pub location: i32,
}

/// Tokenize `src` into `(start_loc, Token, end_loc)` triples for lalrpop's
/// external lexer interface. Keyword resolution (kwlist lookup) and the
/// ICONST-overflow -> FCONST rule run here (PG `base_yylex` / `process_integer_literal`).
pub fn lex(src: &str) -> impl Iterator<Item = Result<(Loc, Token, Loc), LexError>> + '_ {
    Raw::lexer(src).spanned().map(|(res, span)| {
        let start = span.start as i32;
        let end = span.end as i32;
        let Ok(raw) = res else { return Err(LexError { location: start }) };
        let tok = match raw {
            // Skip-callback variant; logos never yields it.
            Raw::BlockComment => unreachable!("BlockComment is skipped by its callback"),
            Raw::Word(w) => word_token(&w),
            // A delimited identifier is a plain Ident (case-preserved, unfolded).
            Raw::QuotedIdent(s) => Token::Ident(s),
            Raw::Integer(s) => integer_token(&s),
            Raw::Float(s) => Token::FConst(s),
            Raw::String(s) => Token::SConst(s),
            Raw::Param(n) => Token::Param(n),
            Raw::Semi => Token::Semi,
            Raw::Comma => Token::Comma,
            Raw::Star => Token::Star,
            Raw::LParen => Token::LParen,
            Raw::RParen => Token::RParen,
            Raw::Dot => Token::Dot,
            Raw::Plus => Token::Plus,
            Raw::Minus => Token::Minus,
            Raw::Slash => Token::Slash,
            Raw::Percent => Token::Percent,
            Raw::Lt => Token::Lt,
            Raw::Gt => Token::Gt,
            Raw::Eq => Token::Eq,
            Raw::Typecast => Token::Typecast,
            Raw::LessEquals => Token::LessEquals,
            Raw::GreaterEquals => Token::GreaterEquals,
            // PG scan.l rewrites `!=` to `<>` at scan time; both yield NotEquals.
            Raw::NotEquals | Raw::BangEquals => Token::NotEquals,
            Raw::Concat => Token::Concat,
            Raw::Shl => Token::Shl,
            Raw::Shr => Token::Shr,
        };
        Ok((start, tok, end))
    })
}

/// Resolve a word to a keyword token (canonical spelling) or an IDENT. Mirrors
/// PG's keyword filtering: downcase, look up in the SQL keyword list; a hit yields
/// the keyword's canonical spelling, a miss yields the (downcased, truncated)
/// identifier.
fn word_token(word: &str) -> Token {
    let downcased =
        crate::backend::parser::scansup::downcase_truncate_identifier(word, word.len() as i32, true);
    crate::common::keywords::scan_keyword_lookup(&downcased)
        .map_or_else(|| Token::Ident(downcased), |idx| Token::Keyword(keyword_name(idx)))
}

/// Canonical (`'static`) spelling of the keyword at `idx` in the keyword list.
fn keyword_name(idx: usize) -> &'static str {
    let kw: &'static Keyword = &scan_keywords()[idx];
    kw.0
}

/// PG `process_integer_literal`: a {decinteger} that overflows i32 is re-emitted
/// as an FCONST (so very large integers become numeric), otherwise ICONST.
fn integer_token(text: &str) -> Token {
    text.parse::<i32>().map_or_else(|_| Token::FConst(text.to_string()), Token::IConst)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn toks(src: &str) -> Vec<Token> {
        lex(src).map(|r| r.unwrap().1).collect()
    }

    #[test]
    fn select_keyword_and_int() {
        assert_eq!(toks("SELECT 1"), vec![Token::Keyword("select"), Token::IConst(1)]);
    }

    #[test]
    fn keyword_case_insensitive() {
        assert_eq!(toks("sElEcT"), vec![Token::Keyword("select")]);
    }

    #[test]
    fn identifier_downcased() {
        assert_eq!(toks("Foo"), vec![Token::Ident("foo".to_string())]);
    }

    #[test]
    fn punctuation() {
        assert_eq!(
            toks("; , * ( ) . + -"),
            vec![
                Token::Semi, Token::Comma, Token::Star, Token::LParen,
                Token::RParen, Token::Dot, Token::Plus, Token::Minus,
            ]
        );
    }

    #[test]
    fn arithmetic_and_comparison_operators() {
        assert_eq!(
            toks("+ - * / % < > = <= >= <> !="),
            vec![
                Token::Plus, Token::Minus, Token::Star, Token::Slash, Token::Percent,
                Token::Lt, Token::Gt, Token::Eq, Token::LessEquals, Token::GreaterEquals,
                Token::NotEquals, Token::NotEquals,
            ]
        );
    }

    #[test]
    fn big_integer_becomes_fconst() {
        assert_eq!(toks("9999999999"), vec![Token::FConst("9999999999".to_string())]);
    }

    #[test]
    fn string_literal_unquotes() {
        assert_eq!(toks("'it''s'"), vec![Token::SConst("it's".to_string())]);
    }

    #[test]
    fn param_lexes_to_number() {
        assert_eq!(toks("$1 $42"), vec![Token::Param(1), Token::Param(42)]);
    }

    #[test]
    fn param_overflow_is_lex_error() {
        assert_eq!(lex("$99999999999").filter_map(Result::err).count(), 1);
    }

    #[test]
    fn line_comment_skipped() {
        assert_eq!(toks("1 -- a comment\n 2"), vec![Token::IConst(1), Token::IConst(2)]);
    }

    #[test]
    fn lex_error_has_location() {
        // '@' is not a token in the M1 set.
        let errs: Vec<_> = lex("1 @ 2").filter_map(Result::err).collect();
        assert_eq!(errs, vec![LexError { location: 2 }]);
    }
}

