//! Translated from PostgreSQL src/include/common/sha2.h

pub const PG_SHA224_BLOCK_LENGTH: usize = 64;
pub const PG_SHA224_DIGEST_LENGTH: usize = 28;
pub const PG_SHA224_DIGEST_STRING_LENGTH: usize = PG_SHA224_DIGEST_LENGTH * 2 + 1;
pub const PG_SHA256_BLOCK_LENGTH: usize = 64;
pub const PG_SHA256_DIGEST_LENGTH: usize = 32;
pub const PG_SHA256_DIGEST_STRING_LENGTH: usize = PG_SHA256_DIGEST_LENGTH * 2 + 1;
pub const PG_SHA384_BLOCK_LENGTH: usize = 128;
pub const PG_SHA384_DIGEST_LENGTH: usize = 48;
pub const PG_SHA384_DIGEST_STRING_LENGTH: usize = PG_SHA384_DIGEST_LENGTH * 2 + 1;
pub const PG_SHA512_BLOCK_LENGTH: usize = 128;
pub const PG_SHA512_DIGEST_LENGTH: usize = 64;
pub const PG_SHA512_DIGEST_STRING_LENGTH: usize = PG_SHA512_DIGEST_LENGTH * 2 + 1;
