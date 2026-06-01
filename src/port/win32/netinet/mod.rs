//! port/win32/netinet - Windows shim for <netinet/*> POSIX headers.

// `in` is a Rust keyword; the file is in.rs (mirroring netinet/in.h).
#[path = "in.rs"]
pub mod r#in;
pub mod tcp;
