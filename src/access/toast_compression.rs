//! Translated from PostgreSQL src/include/access/toast_compression.h

/// GUC: default_toast_compression (stores one of the char values below).
pub static mut DEFAULT_TOAST_COMPRESSION: i32 = 0;

/// Built-in compression method ID; only 2 bits are available on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum ToastCompressionId {
    Pglz = 0,
    Lz4 = 1,
    Invalid = 2,
}

/// Built-in compression methods, stored in pg_attribute.attcompression.
pub const TOAST_PGLZ_COMPRESSION: u8 = b'p';
pub const TOAST_LZ4_COMPRESSION: u8 = b'l';
pub const INVALID_COMPRESSION_METHOD: u8 = b'\0';

pub const fn compression_method_is_valid(cm: u8) -> bool {
    cm != INVALID_COMPRESSION_METHOD
}

// TODO(struct-forward): varlena lives in c.h; repoint to crate::c in Phase 2.
#[deprecated(note = "TODO(struct-forward): repoint to crate::c::varlena in Phase 2")]
pub struct varlena {
    _opaque: [u8; 0],
}

/// pglz compression/decompression routines.
#[allow(deprecated)]
pub fn pglz_compress_datum(_value: &varlena) -> &varlena {
    unimplemented!()
}

#[allow(deprecated)]
pub fn pglz_decompress_datum(_value: &varlena) -> &varlena {
    unimplemented!()
}

#[allow(deprecated)]
pub fn pglz_decompress_datum_slice(_value: &varlena, _slicelength: i32) -> &varlena {
    unimplemented!()
}

/// lz4 compression/decompression routines.
#[allow(deprecated)]
pub fn lz4_compress_datum(_value: &varlena) -> &varlena {
    unimplemented!()
}

#[allow(deprecated)]
pub fn lz4_decompress_datum(_value: &varlena) -> &varlena {
    unimplemented!()
}

#[allow(deprecated)]
pub fn lz4_decompress_datum_slice(_value: &varlena, _slicelength: i32) -> &varlena {
    unimplemented!()
}

/// other stuff
#[allow(deprecated)]
pub fn toast_get_compression_id(_attr: &varlena) -> ToastCompressionId {
    unimplemented!()
}

pub fn compression_name_to_method(_compression: &str) -> u8 {
    unimplemented!()
}

pub fn get_compression_method_name(_method: u8) -> &'static str {
    unimplemented!()
}
