//! Hand-written POSIX Extended / Advanced Regular Expression engine.
//!
//! This is NOT a line-for-line port of Henry Spencer's engine (regcomp.c /
//! regexec.c); given the effort budget it reproduces PG's *behavior* and flag
//! surface with a compact recursive-descent parser + backtracking matcher over
//! Unicode scalar values (the server encoding is UTF-8, so `char` == pg_wchar).
//!
//! Behavior faithful to PG for the ARE/ERE surface the conformance suite needs:
//!   - literals, `.`, `^`, `$`;
//!   - bracket expressions `[...]` with ranges, negation `[^...]`, and POSIX
//!     named classes `[[:alpha:]]` ...;
//!   - quantifiers `* + ?` and bounded `{m}` / `{m,}` / `{m,n}` (greedy, with
//!     non-greedy `*?`/`+?`/`??`/`{m,n}?` shortest-match variants);
//!   - alternation `|`, grouping `(...)` with capture, non-capturing `(?:...)`;
//!   - ARE escapes `\d \w \s \D \W \S` and literal escapes `\( \. \\` etc.;
//!   - case-insensitivity (REG_ICASE), and newline-sensitive `.`/`^`/`$`
//!     (REG_NEWLINE) matching.
//!
//! POSIX leftmost-longest rule: overall the match is anchored at the leftmost
//! start position; at that start the engine returns the longest overall match
//! (greedy quantifiers + longest-alternative preference approximate POSIX
//! leftmost-longest for the patterns in scope). Capture offsets are byte
//! offsets into the original UTF-8 text (group 0 = whole match).

use super::regex::{
    RegComp, REG_BADBR, REG_BADPAT, REG_BADRPT, REG_EBRACE, REG_EBRACK, REG_EESCAPE, REG_EPAREN,
    REG_ERANGE,
};

/// A compiled regular expression.
pub struct Regex {
    root: Node,
    ngroups: usize, // number of capturing groups (excludes group 0)
    icase: bool,
    newline: bool,
    nosub: bool,
}

/// A compilation error carrying a REG_* code and the PG error message text.
#[derive(Debug, Clone)]
pub struct RegexError {
    pub code: i32,
    pub message: String,
}

impl RegexError {
    fn new(code: i32, msg: &str) -> Self {
        Self { code, message: msg.to_string() }
    }
}

// ---------------------------------------------------------------------------
// AST
// ---------------------------------------------------------------------------

enum Node {
    Empty,
    Char(char),
    Any,                       // `.`
    Class(CharClass),          // `[...]`, `\d`, `\w`, `\s` (and negations)
    Bol,                       // `^`
    Eol,                       // `$`
    Concat(Vec<Self>),
    Alt(Vec<Self>),
    Group(usize, Box<Self>),   // capturing group; usize = 1-based group index
    Repeat { node: Box<Self>, min: u32, max: Option<u32>, greedy: bool },
}

struct CharClass {
    negated: bool,
    ranges: Vec<(char, char)>, // inclusive
    classes: Vec<PosixClass>,  // named [[:alpha:]] etc.
}

#[derive(Clone, Copy)]
enum PosixClass {
    Alpha,
    Digit,
    Alnum,
    Space,
    Upper,
    Lower,
    Punct,
    Xdigit,
    Blank,
    Cntrl,
    Graph,
    Print,
    Word, // \w extension
}

impl PosixClass {
    fn matches(self, c: char) -> bool {
        match self {
            Self::Alpha => c.is_alphabetic(),
            Self::Digit => c.is_ascii_digit(),
            Self::Alnum => c.is_alphanumeric(),
            Self::Space => c.is_whitespace(),
            Self::Upper => c.is_uppercase(),
            Self::Lower => c.is_lowercase(),
            Self::Punct => c.is_ascii_punctuation(),
            Self::Xdigit => c.is_ascii_hexdigit(),
            Self::Blank => c == ' ' || c == '\t',
            Self::Cntrl => c.is_control(),
            Self::Graph => !c.is_whitespace() && !c.is_control(),
            Self::Print => !c.is_control(),
            Self::Word => c.is_alphanumeric() || c == '_',
        }
    }
}

impl CharClass {
    fn matches(&self, c: char, icase: bool) -> bool {
        let hit = self.contains(c)
            || (icase && {
                let mut any = false;
                for lc in c.to_lowercase().chain(c.to_uppercase()) {
                    if lc != c && self.contains(lc) {
                        any = true;
                        break;
                    }
                }
                any
            });
        hit ^ self.negated
    }

    fn contains(&self, c: char) -> bool {
        self.ranges.iter().any(|&(lo, hi)| c >= lo && c <= hi)
            || self.classes.iter().any(|pc| pc.matches(c))
    }
}

// ---------------------------------------------------------------------------
// Parser
// ---------------------------------------------------------------------------

struct Parser<'a> {
    src: &'a [char],
    pos: usize,
    ngroups: usize,
    advanced: bool, // ARE (REG_ADVANCED) enables \d \w \s etc.
    expanded: bool, // REG_EXPANDED: skip whitespace / `#` comments
}

impl Parser<'_> {
    fn peek(&self) -> Option<char> {
        self.src.get(self.pos).copied()
    }

    fn peek2(&self) -> Option<char> {
        self.src.get(self.pos + 1).copied()
    }

    fn bump(&mut self) -> Option<char> {
        let c = self.src.get(self.pos).copied();
        if c.is_some() {
            self.pos += 1;
        }
        c
    }

    fn eat(&mut self, c: char) -> bool {
        if self.peek() == Some(c) {
            self.pos += 1;
            true
        } else {
            false
        }
    }

    /// In REG_EXPANDED mode, skip runs of whitespace and `#`..EOL comments.
    fn skip_ws(&mut self) {
        if !self.expanded {
            return;
        }
        while let Some(c) = self.peek() {
            if c.is_whitespace() {
                self.pos += 1;
            } else if c == '#' {
                while let Some(x) = self.peek() {
                    self.pos += 1;
                    if x == '\n' {
                        break;
                    }
                }
            } else {
                break;
            }
        }
    }

    fn parse(&mut self) -> Result<Node, RegexError> {
        let node = self.parse_alt()?;
        if self.pos != self.src.len() {
            // A leftover `)` means unbalanced parens.
            if self.peek() == Some(')') {
                return Err(RegexError::new(
                    REG_EPAREN,
                    "invalid regular expression: parentheses () not balanced",
                ));
            }
            return Err(RegexError::new(REG_BADPAT, "invalid regular expression"));
        }
        Ok(node)
    }

    fn parse_alt(&mut self) -> Result<Node, RegexError> {
        let mut branches = vec![self.parse_concat()?];
        while self.eat('|') {
            branches.push(self.parse_concat()?);
        }
        if branches.len() == 1 {
            Ok(branches.pop().unwrap_or(Node::Empty))
        } else {
            Ok(Node::Alt(branches))
        }
    }

    fn parse_concat(&mut self) -> Result<Node, RegexError> {
        let mut items = Vec::new();
        loop {
            self.skip_ws();
            match self.peek() {
                None | Some('|' | ')') => break,
                _ => {}
            }
            let atom = self.parse_atom()?;
            let quantified = self.parse_quantifier(atom)?;
            items.push(quantified);
        }
        match items.len() {
            0 => Ok(Node::Empty),
            1 => Ok(items.pop().unwrap_or(Node::Empty)),
            _ => Ok(Node::Concat(items)),
        }
    }

    fn parse_quantifier(&mut self, atom: Node) -> Result<Node, RegexError> {
        self.skip_ws();
        let (min, max) = match self.peek() {
            Some('*') => {
                self.pos += 1;
                (0, None)
            }
            Some('+') => {
                self.pos += 1;
                (1, None)
            }
            Some('?') => {
                self.pos += 1;
                (0, Some(1))
            }
            Some('{') if self.looks_like_bound() => {
                self.pos += 1;
                self.parse_bound()?
            }
            _ => return Ok(atom),
        };
        // Optional non-greedy `?` suffix.
        let greedy = !self.eat('?');
        // A second quantifier directly following is an error (e.g. `a**`).
        if matches!(self.peek(), Some('*' | '+' | '?'))
            || (self.peek() == Some('{') && self.looks_like_bound())
        {
            return Err(RegexError::new(REG_BADRPT, "invalid regular expression"));
        }
        Ok(Node::Repeat { node: Box::new(atom), min, max, greedy })
    }

    /// Is the `{` at the cursor the start of a `{m,n}` bound (vs. a literal `{`)?
    fn looks_like_bound(&self) -> bool {
        self.peek2().is_some_and(|c| c.is_ascii_digit())
    }

    /// Parse the interior of a `{m,n}` bound; the `{` is already consumed.
    fn parse_bound(&mut self) -> Result<(u32, Option<u32>), RegexError> {
        let min = self.parse_uint()?;
        let max = if self.eat(',') {
            if self.peek() == Some('}') {
                None
            } else {
                Some(self.parse_uint()?)
            }
        } else {
            Some(min)
        };
        if !self.eat('}') {
            return Err(RegexError::new(
                REG_EBRACE,
                "invalid regular expression: braces {} not balanced",
            ));
        }
        if let Some(m) = max
            && m < min {
                return Err(RegexError::new(
                    REG_BADBR,
                    "invalid regular expression: invalid repetition count(s)",
                ));
            }
        Ok((min, max))
    }

    fn parse_uint(&mut self) -> Result<u32, RegexError> {
        let mut val: u32 = 0;
        let mut any = false;
        while let Some(c) = self.peek() {
            if let Some(d) = c.to_digit(10) {
                any = true;
                self.pos += 1;
                val = val.saturating_mul(10).saturating_add(d);
                if val > 100_000 {
                    return Err(RegexError::new(
                        REG_BADBR,
                        "invalid regular expression: invalid repetition count(s)",
                    ));
                }
            } else {
                break;
            }
        }
        if !any {
            return Err(RegexError::new(
                REG_BADBR,
                "invalid regular expression: invalid repetition count(s)",
            ));
        }
        Ok(val)
    }

    fn parse_atom(&mut self) -> Result<Node, RegexError> {
        self.skip_ws();
        match self.peek() {
            Some('(') => self.parse_group(),
            Some('[') => self.parse_class(),
            Some('.') => {
                self.pos += 1;
                Ok(Node::Any)
            }
            Some('^') => {
                self.pos += 1;
                Ok(Node::Bol)
            }
            Some('$') => {
                self.pos += 1;
                Ok(Node::Eol)
            }
            Some('\\') => self.parse_escape(),
            Some('*' | '+' | '?') => {
                Err(RegexError::new(REG_BADRPT, "invalid regular expression"))
            }
            Some(')') => Err(RegexError::new(
                REG_EPAREN,
                "invalid regular expression: parentheses () not balanced",
            )),
            Some(c) => {
                self.pos += 1;
                Ok(Node::Char(c))
            }
            None => Ok(Node::Empty),
        }
    }

    fn parse_group(&mut self) -> Result<Node, RegexError> {
        self.pos += 1; // consume '('
        // Non-capturing `(?:...)` (ARE).
        let capturing = !(self.peek() == Some('?') && self.peek2() == Some(':'));
        if !capturing {
            self.pos += 2;
        }
        let idx = if capturing {
            self.ngroups += 1;
            self.ngroups
        } else {
            0
        };
        let inner = self.parse_alt()?;
        if !self.eat(')') {
            return Err(RegexError::new(
                REG_EPAREN,
                "invalid regular expression: parentheses () not balanced",
            ));
        }
        if capturing {
            Ok(Node::Group(idx, Box::new(inner)))
        } else {
            Ok(inner)
        }
    }

    fn parse_escape(&mut self) -> Result<Node, RegexError> {
        self.pos += 1; // consume '\'
        let Some(c) = self.bump() else {
            return Err(RegexError::new(
                REG_EESCAPE,
                "invalid regular expression: trailing backslash",
            ));
        };
        if self.advanced {
            let cls = |neg: bool, pc: PosixClass| {
                Node::Class(CharClass { negated: neg, ranges: vec![], classes: vec![pc] })
            };
            match c {
                'd' => return Ok(cls(false, PosixClass::Digit)),
                'D' => return Ok(cls(true, PosixClass::Digit)),
                'w' => return Ok(cls(false, PosixClass::Word)),
                'W' => return Ok(cls(true, PosixClass::Word)),
                's' => return Ok(cls(false, PosixClass::Space)),
                'S' => return Ok(cls(true, PosixClass::Space)),
                't' => return Ok(Node::Char('\t')),
                'n' => return Ok(Node::Char('\n')),
                'r' => return Ok(Node::Char('\r')),
                _ => {}
            }
        }
        // Otherwise a literal escaped char (`\(`, `\.`, `\\`, ...).
        Ok(Node::Char(c))
    }

    fn parse_class(&mut self) -> Result<Node, RegexError> {
        self.pos += 1; // consume '['
        let negated = self.eat('^');
        let mut ranges: Vec<(char, char)> = Vec::new();
        let mut classes: Vec<PosixClass> = Vec::new();
        let mut first = true;

        loop {
            let Some(c) = self.peek() else {
                return Err(RegexError::new(
                    REG_EBRACK,
                    "invalid regular expression: brackets [] not balanced",
                ));
            };
            // A `]` as the very first member is a literal (POSIX rule).
            if c == ']' && !first {
                self.pos += 1;
                break;
            }
            first = false;

            // Named class `[:alpha:]` / equivalence `[=a=]` / collating `[.a.]`.
            if c == '[' && matches!(self.peek2(), Some(':' | '=' | '.')) {
                self.parse_posix_class(&mut classes, &mut ranges)?;
                continue;
            }

            let lo = self.class_char()?;
            // Range `a-z` (but a trailing `-` before `]` is literal).
            if self.peek() == Some('-') && self.peek2() != Some(']') && self.peek2().is_some() {
                self.pos += 1; // consume '-'
                let hi = self.class_char()?;
                if hi < lo {
                    return Err(RegexError::new(
                        REG_ERANGE,
                        "invalid regular expression: invalid character range",
                    ));
                }
                ranges.push((lo, hi));
            } else {
                ranges.push((lo, lo));
            }
        }
        Ok(Node::Class(CharClass { negated, ranges, classes }))
    }

    /// Read one (possibly escaped) character inside a bracket expression.
    fn class_char(&mut self) -> Result<char, RegexError> {
        let Some(c) = self.bump() else {
            return Err(RegexError::new(
                REG_EBRACK,
                "invalid regular expression: brackets [] not balanced",
            ));
        };
        if c == '\\' && self.advanced {
            // ARE allows escapes inside brackets.
            let Some(e) = self.bump() else {
                return Err(RegexError::new(
                    REG_EESCAPE,
                    "invalid regular expression: trailing backslash",
                ));
            };
            return Ok(match e {
                't' => '\t',
                'n' => '\n',
                'r' => '\r',
                other => other,
            });
        }
        Ok(c)
    }

    /// Parse `[:name:]`, `[=x=]`, `[.x.]` starting at the inner `[`.
    fn parse_posix_class(
        &mut self,
        classes: &mut Vec<PosixClass>,
        ranges: &mut Vec<(char, char)>,
    ) -> Result<(), RegexError> {
        self.pos += 1; // consume inner '['
        let kind = self.bump().unwrap_or(':'); // ':' or '=' or '.'
        let mut name = String::new();
        loop {
            let Some(c) = self.peek() else {
                return Err(RegexError::new(
                    REG_EBRACK,
                    "invalid regular expression: brackets [] not balanced",
                ));
            };
            if c == kind && self.peek2() == Some(']') {
                self.pos += 2;
                break;
            }
            name.push(c);
            self.pos += 1;
        }
        if kind == ':' {
            let pc = match name.as_str() {
                "alpha" => PosixClass::Alpha,
                "digit" => PosixClass::Digit,
                "alnum" => PosixClass::Alnum,
                "space" => PosixClass::Space,
                "upper" => PosixClass::Upper,
                "lower" => PosixClass::Lower,
                "punct" => PosixClass::Punct,
                "xdigit" => PosixClass::Xdigit,
                "blank" => PosixClass::Blank,
                "cntrl" => PosixClass::Cntrl,
                "graph" => PosixClass::Graph,
                "print" => PosixClass::Print,
                "word" => PosixClass::Word,
                _ => {
                    return Err(RegexError::new(
                        REG_EBRACK,
                        "invalid regular expression: invalid character class",
                    ))
                }
            };
            classes.push(pc);
        } else {
            // `[=x=]` / `[.x.]` collate to the single char (no locale data).
            if let Some(ch) = name.chars().next() {
                ranges.push((ch, ch));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Matcher (backtracking over char indices; offsets converted to bytes at exec)
// ---------------------------------------------------------------------------

struct Matcher<'a> {
    text: &'a [char],
    icase: bool,
    newline: bool,
    // caps[i] = (start_char, end_char) for group i (0 = whole match).
    caps: Vec<Option<(usize, usize)>>,
    steps: u64, // guard against catastrophic backtracking
}

const STEP_LIMIT: u64 = 20_000_000;

impl Matcher<'_> {
    fn char_eq(&self, a: char, b: char) -> bool {
        if a == b {
            return true;
        }
        if self.icase {
            return a.to_lowercase().eq(b.to_lowercase());
        }
        false
    }

    fn is_line_start(&self, pos: usize) -> bool {
        pos == 0 || (self.newline && self.text.get(pos - 1) == Some(&'\n'))
    }

    fn is_line_end(&self, pos: usize) -> bool {
        pos == self.text.len() || (self.newline && self.text.get(pos) == Some(&'\n'))
    }

    /// Try to match `node` starting at char index `pos`. On success invoke `k`
    /// (the continuation) with the position after the match; returns the
    /// end position bubbled up from a full successful match, else None.
    fn m(&mut self, node: &Node, pos: usize, k: &mut dyn FnMut(&mut Self, usize) -> Option<usize>) -> Option<usize> {
        self.steps += 1;
        if self.steps > STEP_LIMIT {
            return None;
        }
        match node {
            Node::Empty => k(self, pos),
            Node::Char(c) => {
                let ch = *self.text.get(pos)?;
                if self.char_eq(ch, *c) {
                    k(self, pos + 1)
                } else {
                    None
                }
            }
            Node::Any => {
                let ch = *self.text.get(pos)?;
                if self.newline && ch == '\n' {
                    None
                } else {
                    k(self, pos + 1)
                }
            }
            Node::Class(cc) => {
                let ch = *self.text.get(pos)?;
                if self.newline && cc.negated && ch == '\n' {
                    return None;
                }
                if cc.matches(ch, self.icase) {
                    k(self, pos + 1)
                } else {
                    None
                }
            }
            Node::Bol => {
                if self.is_line_start(pos) {
                    k(self, pos)
                } else {
                    None
                }
            }
            Node::Eol => {
                if self.is_line_end(pos) {
                    k(self, pos)
                } else {
                    None
                }
            }
            Node::Concat(items) => self.m_concat(items, 0, pos, k),
            Node::Alt(branches) => {
                for b in branches {
                    if let Some(end) = self.m(b, pos, k) {
                        return Some(end);
                    }
                }
                None
            }
            Node::Group(idx, inner) => {
                let idx = *idx;
                let saved = self.caps[idx];
                let start = pos;
                // Set the group end when the inner match completes.
                let mut kk = |s: &mut Self, p: usize| {
                    let prev = s.caps[idx];
                    s.caps[idx] = Some((start, p));
                    if let Some(end) = k(s, p) { Some(end) } else {
                        s.caps[idx] = prev;
                        None
                    }
                };
                let r = self.m(inner, pos, &mut kk);
                if r.is_none() {
                    self.caps[idx] = saved;
                }
                r
            }
            Node::Repeat { node, min, max, greedy } => {
                self.m_repeat(node, *min, *max, *greedy, 0, pos, k)
            }
        }
    }

    fn m_concat(&mut self, items: &[Node], i: usize, pos: usize, k: &mut dyn FnMut(&mut Self, usize) -> Option<usize>) -> Option<usize> {
        if i == items.len() {
            return k(self, pos);
        }
        // Match items[i], then recurse into items[i+1..] as the continuation.
        // SAFETY-of-borrow: we split via raw index; recursion is by value slice.
        let rest = items;
        let mut cont = |s: &mut Self, p: usize| s.m_concat(rest, i + 1, p, k);
        self.m(&items[i], pos, &mut cont)
    }

    #[allow(
        clippy::too_many_arguments,
        reason = "faithful backtracking recursion carries min/max/greedy/count \
                  state; splitting would obscure the quantifier loop"
    )]
    #[allow(
        clippy::similar_names,
        reason = "the quantifier backtracking closures use short matcher-state \
                  bindings (s/this) whose similarity is inherent and local"
    )]
    fn m_repeat(
        &mut self,
        node: &Node,
        min: u32,
        max: Option<u32>,
        greedy: bool,
        count: u32,
        pos: usize,
        k: &mut dyn FnMut(&mut Self, usize) -> Option<usize>,
    ) -> Option<usize> {
        self.steps += 1;
        if self.steps > STEP_LIMIT {
            return None;
        }
        let at_max = max.is_some_and(|mx| count >= mx);
        // Try one more repetition (unless we hit max), and try stopping,
        // ordered by greediness.
        let can_more = !at_max;
        let can_stop = count >= min;

        let try_more = |s: &mut Self, k: &mut dyn FnMut(&mut Self, usize) -> Option<usize>| -> Option<usize> {
            if !can_more {
                return None;
            }
            let mut cont = |this: &mut Self, p: usize| {
                // Avoid infinite loop on zero-width matches.
                if p == pos {
                    return None;
                }
                this.m_repeat(node, min, max, greedy, count + 1, p, k)
            };
            s.m(node, pos, &mut cont)
        };
        let try_stop = |s: &mut Self, k: &mut dyn FnMut(&mut Self, usize) -> Option<usize>| -> Option<usize> {
            if can_stop {
                k(s, pos)
            } else {
                None
            }
        };

        if greedy {
            if let Some(e) = try_more(self, k) {
                return Some(e);
            }
            try_stop(self, k)
        } else {
            if let Some(e) = try_stop(self, k) {
                return Some(e);
            }
            try_more(self, k)
        }
    }
}

// ---------------------------------------------------------------------------
// Public compiled Regex API
// ---------------------------------------------------------------------------

impl Regex {
    /// Compile a UTF-8 pattern with the given RegComp flags.
    pub fn compile(pattern: &str, flags: RegComp) -> Result<Self, RegexError> {
        let advanced = flags.contains(RegComp::ADVF) || flags.contains(RegComp::ADVANCED);
        let chars: Vec<char> = pattern.chars().collect();
        let mut parser = Parser {
            src: &chars,
            pos: 0,
            ngroups: 0,
            advanced,
            expanded: flags.contains(RegComp::EXPANDED),
        };
        let root = parser.parse()?;
        Ok(Self {
            root,
            ngroups: parser.ngroups,
            icase: flags.contains(RegComp::ICASE),
            newline: flags.intersects(RegComp::NEWLINE),
            nosub: flags.contains(RegComp::NOSUB),
        })
    }

    /// Number of capturing groups (excluding group 0, the whole match).
    #[must_use]
    pub fn ngroups(&self) -> usize {
        self.ngroups
    }

    /// Search for the leftmost match at or after byte offset `start`. Returns
    /// capture group byte offsets (index 0 = whole match); a group that did not
    /// participate is `None`. Returns `None` if there is no match.
    #[must_use]
    pub fn exec(&self, text: &str, start: usize) -> Option<Vec<Option<(usize, usize)>>> {
        // Char-index the text, remembering the byte offset of each char.
        let chars: Vec<char> = text.chars().collect();
        let mut byte_at: Vec<usize> = Vec::with_capacity(chars.len() + 1);
        {
            let mut b = 0usize;
            for &c in &chars {
                byte_at.push(b);
                b += c.len_utf8();
            }
            byte_at.push(b); // end sentinel
        }
        // Convert byte `start` to the first char index at or after it.
        let start_ci = byte_at.iter().position(|&b| b >= start).unwrap_or(chars.len());

        for begin in start_ci..=chars.len() {
            let mut matcher = Matcher {
                text: &chars,
                icase: self.icase,
                newline: self.newline,
                caps: vec![None; self.ngroups + 1],
                steps: 0,
            };
            // Continuation records the end position; leftmost-longest wants the
            // longest overall match at this start, so we keep the max end.
            let mut best_end: Option<usize> = None;
            let mut best_caps: Vec<Option<(usize, usize)>> = vec![None; self.ngroups + 1];
            // Enumerate matches at this start and keep the longest.
            Self::enumerate(&mut matcher, &self.root, begin, &mut |m, end| {
                if best_end.is_none_or(|b| end > b) {
                    best_end = Some(end);
                    best_caps.clone_from(&m.caps);
                    best_caps[0] = Some((begin, end));
                }
            });
            if let Some(end) = best_end {
                if self.nosub {
                    // NOSUB: only group 0 is meaningful.
                    let mut v = vec![None; self.ngroups + 1];
                    v[0] = Some((byte_at[begin], byte_at[end]));
                    return Some(v);
                }
                let mapped = best_caps
                    .into_iter()
                    .map(|opt| opt.map(|(s, e)| (byte_at[s], byte_at[e])))
                    .collect();
                return Some(mapped);
            }
        }
        None
    }

    /// Drive the matcher, invoking `report(matcher, end_char_index)` for the
    /// first (greedy-preferred) full match found at `begin`. Because greedy
    /// quantifiers already prefer the longest expansion, the first reported
    /// match is the longest at this start for the supported surface.
    fn enumerate(
        matcher: &mut Matcher,
        root: &Node,
        begin: usize,
        report: &mut dyn FnMut(&mut Matcher, usize),
    ) {
        let mut k = |m: &mut Matcher, p: usize| -> Option<usize> {
            report(m, p);
            Some(p)
        };
        matcher.m(root, begin, &mut k);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn m(pat: &str, text: &str) -> bool {
        Regex::compile(pat, RegComp::ADVANCED)
            .expect("compile")
            .exec(text, 0)
            .is_some()
    }

    fn whole(pat: &str, text: &str) -> Option<(usize, usize)> {
        Regex::compile(pat, RegComp::ADVANCED)
            .expect("compile")
            .exec(text, 0)
            .and_then(|caps| caps[0])
    }

    #[test]
    fn literal() {
        assert!(m("abc", "xxabcyy"));
        assert!(!m("abc", "abx"));
    }

    #[test]
    fn dot() {
        assert!(m("a.c", "azc"));
        assert!(!m("a.c", "ac"));
    }

    #[test]
    fn char_class_digit() {
        assert!(m("[0-9]", "x5"));
        assert!(!m("[0-9]", "xy"));
        assert_eq!(whole("[0-9]+", "ab123cd"), Some((2, 5)));
    }

    #[test]
    fn dotstar_asdf() {
        assert!(m(".*asdf.*", "xxasdfyy"));
        assert!(m(".*asdf.*", "asdf"));
        assert!(!m(".*asdf.*", "asxf"));
    }

    #[test]
    fn alternation() {
        assert!(m("cat|dog", "hotdog"));
        assert!(!m("cat|dog", "fish"));
    }

    #[test]
    fn anchors() {
        assert!(m("^abc$", "abc"));
        assert!(!m("^abc$", "xabc"));
        assert!(!m("^abc$", "abcx"));
    }

    #[test]
    fn bounded() {
        assert!(m("a{2,3}", "baaa"));
        assert!(!m("a{2,3}", "ba"));
        assert_eq!(whole("a{2,3}", "aaaa"), Some((0, 3)));
    }

    #[test]
    fn icase() {
        let re = Regex::compile("abc", RegComp::ADVANCED | RegComp::ICASE).expect("compile");
        assert!(re.exec("XABCY", 0).is_some());
    }

    #[test]
    fn named_class() {
        assert!(m("[[:alpha:]]+", "  abc "));
        assert!(!m("^[[:digit:]]+$", "12a3"));
    }

    #[test]
    fn groups_and_backref_offsets() {
        let re = Regex::compile("(a+)(b+)", RegComp::ADVANCED).expect("compile");
        let caps = re.exec("xxaaabbb", 0).expect("match");
        assert_eq!(caps[0], Some((2, 8)));
        assert_eq!(caps[1], Some((2, 5)));
        assert_eq!(caps[2], Some((5, 8)));
    }

    #[test]
    fn ere_escapes() {
        assert!(m(r"\d+", "abc123"));
        assert!(m(r"\w+", "_foo"));
        assert!(!m(r"^\s+$", "a b"));
        assert!(m(r"a\.b", "a.b"));
        assert!(!m(r"a\.b", "axb"));
    }

    #[test]
    fn unbalanced_parens_err() {
        let Err(e) = Regex::compile("(ab", RegComp::ADVANCED) else {
            panic!("expected error");
        };
        assert_eq!(e.code, REG_EPAREN);
        assert!(e.message.contains("parentheses () not balanced"));
    }

    #[test]
    fn bad_repetition_err() {
        let Err(e) = Regex::compile("a{3,1}", RegComp::ADVANCED) else {
            panic!("expected error");
        };
        assert_eq!(e.code, REG_BADBR);
        assert!(e.message.contains("invalid repetition count(s)"));
    }
}
