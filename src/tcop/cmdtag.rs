//! Translated from PostgreSQL src/include/tcop/cmdtag.h
//
// The CommandTag enum itself is generated from cmdtaglist.h (level 1) and lives
// in crate::tcop::cmdtaglist; we re-use it here.

use crate::c::Size;
use crate::tcop::cmdtaglist::CommandTag;

/// Buffer size required for command completion tags.
pub const COMPLETION_TAG_BUFSIZE: usize = 64;

/// C: `QueryCompletion`. In-memory.
#[derive(Debug, Clone, Copy)]
pub struct QueryCompletion {
    pub command_tag: CommandTag,
    pub nprocessed: u64,
}

impl QueryCompletion {
    /// C: `SetQueryCompletion(qc, tag, n)`.
    pub fn set(&mut self, command_tag: CommandTag, nprocessed: u64) {
        self.command_tag = command_tag;
        self.nprocessed = nprocessed;
    }

    /// C: `CopyQueryCompletion(dst, src)`.
    pub fn copy_from(&mut self, src: &QueryCompletion) {
        *self = *src;
    }
}

pub fn InitializeQueryCompletion(qc: &mut QueryCompletion) {
    unimplemented!()
}

pub fn GetCommandTagName(command_tag: CommandTag) -> &'static str {
    unimplemented!()
}

/// C: `const char *GetCommandTagNameAndLen(tag, Size *len)` - name + its length.
pub fn GetCommandTagNameAndLen(command_tag: CommandTag) -> (&'static str, Size) {
    unimplemented!()
}

pub fn command_tag_display_rowcount(command_tag: CommandTag) -> bool {
    unimplemented!()
}

pub fn command_tag_event_trigger_ok(command_tag: CommandTag) -> bool {
    unimplemented!()
}

pub fn command_tag_table_rewrite_ok(command_tag: CommandTag) -> bool {
    unimplemented!()
}

/// C: `CommandTag GetCommandTagEnum(const char *commandname)` - returns
/// CommandTag::Unknown for an unrecognized name (the C sentinel).
pub fn GetCommandTagEnum(commandname: &str) -> CommandTag {
    unimplemented!()
}

pub fn BuildQueryCompletionString(buff: &mut [u8], qc: &QueryCompletion, nameonly: bool) -> Size {
    unimplemented!()
}
