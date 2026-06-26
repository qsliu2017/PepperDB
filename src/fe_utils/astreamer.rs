//! Translated from PostgreSQL src/include/fe_utils/astreamer.h
#![allow(
    clippy::needless_pass_by_value,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]
//
// The "archive streamer" - a composable chain of stream processors. The C
// `astreamer_ops` vtable (content/finalize/free, all required) maps to a trait
// (routine-struct.md recipe: required callbacks -> base trait; the C `free`
// callback becomes Drop). The `next` successor pointer becomes an owned
// boxed successor; the StringInfo buffer (tombstoned) becomes a `Vec<u8>`.

use crate::common::compression::PgCompressSpecification;

/// Classification of each chunk passed to a streamer. (C enum)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AstreamerArchiveContext {
    Unknown,
    MemberHeader,
    MemberContents,
    MemberTrailer,
    ArchiveTrailer,
}

const MAXPGPATH: usize = 1024;

/// Per-archive-member metadata. In-memory.
pub struct AstreamerMember {
    pub pathname: String,
    pub size: i64,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub is_regular: bool,
    pub is_directory: bool,
    pub is_symlink: bool,
    pub linktarget: String,
}

/// C: `astreamer_ops` (content/finalize/free) + the base `astreamer` state. A
/// streamer owns its successor and a scratch buffer; `free` is Drop.
pub trait Astreamer {
    /// C: `content(streamer, member, data, len, context)`.
    fn content(
        &mut self,
        member: &AstreamerMember,
        data: &[u8],
        context: AstreamerArchiveContext,
    );
    /// C: `finalize(streamer)`.
    fn finalize(&mut self);
    // C `free` callback -> Drop.
}

// Constructors. Each returns a boxed streamer; the C "next" arg is the owned
// successor for the composable types.
pub fn astreamer_plain_writer_new(pathname: &str, file: std::fs::File) -> Box<dyn Astreamer> {
    unimplemented!()
}
pub fn astreamer_gzip_writer_new(
    pathname: &str,
    file: std::fs::File,
    compress: &PgCompressSpecification,
) -> Box<dyn Astreamer> {
    unimplemented!()
}
pub fn astreamer_extractor_new(
    basepath: &str,
    link_map: impl Fn(&str) -> Option<String> + 'static,
    report_output_file: impl Fn(&str) + 'static,
) -> Box<dyn Astreamer> {
    unimplemented!()
}
pub fn astreamer_gzip_decompressor_new(next: Box<dyn Astreamer>) -> Box<dyn Astreamer> {
    unimplemented!()
}
pub fn astreamer_lz4_compressor_new(
    next: Box<dyn Astreamer>,
    compress: &PgCompressSpecification,
) -> Box<dyn Astreamer> {
    unimplemented!()
}
pub fn astreamer_lz4_decompressor_new(next: Box<dyn Astreamer>) -> Box<dyn Astreamer> {
    unimplemented!()
}
pub fn astreamer_zstd_compressor_new(
    next: Box<dyn Astreamer>,
    compress: &PgCompressSpecification,
) -> Box<dyn Astreamer> {
    unimplemented!()
}
pub fn astreamer_zstd_decompressor_new(next: Box<dyn Astreamer>) -> Box<dyn Astreamer> {
    unimplemented!()
}
pub fn astreamer_tar_parser_new(next: Box<dyn Astreamer>) -> Box<dyn Astreamer> {
    unimplemented!()
}
pub fn astreamer_tar_terminator_new(next: Box<dyn Astreamer>) -> Box<dyn Astreamer> {
    unimplemented!()
}
pub fn astreamer_tar_archiver_new(next: Box<dyn Astreamer>) -> Box<dyn Astreamer> {
    unimplemented!()
}
