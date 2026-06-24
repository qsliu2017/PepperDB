//! Translated from PostgreSQL src/include/common/string.h
// String helpers + line/prompt readers. printf-style + char* boundaries.
// (StringInfo line buffers map to std String - stringinfo.h is tombstoned.)

/// Context for interruptible prompt reads. In C `jmpbuf` is a `void *` to a
/// longjmp buffer and `enabled` a `sig_atomic_t *` flag.
pub struct PromptInterruptContext {
    /// Existing longjmp buffer (opaque).
    pub jmpbuf: Option<*mut core::ffi::c_void>,
    /// Flag enabling longjmp-on-interrupt.
    pub enabled: Option<*mut i32>,
    /// Whether cancellation occurred.
    pub canceled: bool,
}

// --- src/common/string.c ---

/// True if `str` ends with `end`.
pub fn pg_str_endswith(s: &str, end: &str) -> bool {
    let _ = (s, end);
    unimplemented!()
}

/// Parse an integer in the given base. Returns (value, rest-of-string).
pub fn strtoint(s: &str, base: i32) -> (i32, usize) {
    let _ = (s, base);
    unimplemented!()
}

/// Replace non-printable ASCII per `alloc_flags`; returns the cleaned string.
pub fn pg_clean_ascii(s: &str, alloc_flags: i32) -> String {
    let _ = (s, alloc_flags);
    unimplemented!()
}

/// Strip trailing CR/LF in place; returns the new length.
pub fn pg_strip_crlf(s: &mut String) -> i32 {
    let _ = s;
    unimplemented!()
}

/// True if `str` contains only 7-bit ASCII.
pub fn pg_is_ascii(s: &str) -> bool {
    let _ = s;
    unimplemented!()
}

// --- src/common/pg_get_line.c ---

/// Read one line from `stream`; None at EOF.
pub fn pg_get_line(
    stream: &mut dyn std::io::Read,
    prompt_ctx: Option<&PromptInterruptContext>,
) -> Option<String> {
    let _ = (stream, prompt_ctx);
    unimplemented!()
}

/// Read one line into `buf`; false at EOF.
pub fn pg_get_line_buf(stream: &mut dyn std::io::Read, buf: &mut String) -> bool {
    let _ = (stream, buf);
    unimplemented!()
}

/// Append one line onto `buf`; false at EOF.
pub fn pg_get_line_append(
    stream: &mut dyn std::io::Read,
    buf: &mut String,
    prompt_ctx: Option<&PromptInterruptContext>,
) -> bool {
    let _ = (stream, buf, prompt_ctx);
    unimplemented!()
}

// --- src/common/sprompt.c ---

/// Prompt and read a line, optionally echoing input.
pub fn simple_prompt(prompt: &str, echo: bool) -> String {
    let _ = (prompt, echo);
    unimplemented!()
}

/// As `simple_prompt` with an interrupt context.
pub fn simple_prompt_extended(
    prompt: &str,
    echo: bool,
    prompt_ctx: Option<&PromptInterruptContext>,
) -> String {
    let _ = (prompt, echo, prompt_ctx);
    unimplemented!()
}
