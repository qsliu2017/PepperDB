//! Translated from PostgreSQL src/include/utils/bytea.h

pub enum ByteaOutputType {
    Escape,
    Hex,
}

/// ByteaOutputType, but int for GUC enum.
pub static mut bytea_output: i32 = 0;
