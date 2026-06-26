//! Translated from PostgreSQL src/include/fe_utils/conditional.h
//
// A stack of \if/\elif/\else states for psql/pgbench. The C singly-linked stack
// collapses to a Vec; the inline API is implemented in full.

/// Possible states of a single level of \if block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IfState {
    None,       // not currently in an \if block
    True,       // in an \if/\elif that is true (and all parents true)
    False,      // in an \if/\elif that is false, no true branch seen yet
    Ignored,    // \elif after a true branch, or child of a false parent
    ElseTrue,   // in an \else that is true (and all parents true)
    ElseFalse,  // in an \else that is false or ignored
}

// One stack frame. query_len/paren_depth let the lexer discard accumulated text
// at the end of an inactive branch.
#[derive(Debug, Clone, Copy)]
pub struct IfStackElem {
    pub if_state: IfState,
    pub query_len: i32,
    pub paren_depth: i32,
}

#[derive(Debug, Default)]
pub struct ConditionalStack {
    pub stack: Vec<IfStackElem>,
}

impl ConditionalStack {
    pub fn create() -> Self {
        Self { stack: Vec::new() }
    }

    pub fn reset(&mut self) {
        self.stack.clear();
    }

    pub fn depth(&self) -> i32 {
        self.stack.len() as i32
    }

    pub fn push(&mut self, new_state: IfState) {
        self.stack.push(IfStackElem { if_state: new_state, query_len: 0, paren_depth: 0 });
    }

    /// Pops the top frame; returns whether one was present.
    pub fn pop(&mut self) -> bool {
        self.stack.pop().is_some()
    }

    /// Top state, or IFSTATE_NONE when empty.
    pub fn peek(&self) -> IfState {
        self.stack.last().map_or(IfState::None, |e| e.if_state)
    }

    /// Replaces the top state; returns false when empty.
    pub fn poke(&mut self, new_state: IfState) -> bool {
        match self.stack.last_mut() {
            Some(top) => {
                top.if_state = new_state;
                true
            }
            None => false,
        }
    }

    pub fn empty(&self) -> bool {
        self.stack.is_empty()
    }

    /// True when the current branch is active (executing).
    pub fn active(&self) -> bool {
        matches!(self.peek(), IfState::None | IfState::True | IfState::ElseTrue)
    }

    pub fn set_query_len(&mut self, len: i32) {
        if let Some(top) = self.stack.last_mut() {
            top.query_len = len;
        }
    }

    pub fn get_query_len(&self) -> i32 {
        self.stack.last().map_or(0, |e| e.query_len)
    }

    pub fn set_paren_depth(&mut self, depth: i32) {
        if let Some(top) = self.stack.last_mut() {
            top.paren_depth = depth;
        }
    }

    pub fn get_paren_depth(&self) -> i32 {
        self.stack.last().map_or(0, |e| e.paren_depth)
    }
}
