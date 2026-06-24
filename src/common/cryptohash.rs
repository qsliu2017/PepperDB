//! Translated from PostgreSQL src/include/common/cryptohash.h
//! Generic interface for cryptographic hash functions.

/// Supported cryptographic hash types.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgCryptohashType {
    Md5 = 0,
    Sha1,
    Sha224,
    Sha256,
    Sha384,
    Sha512,
}

/// Opaque context, private to each cryptohash implementation.
pub struct PgCryptohashCtx {
    _private: (),
}

/// Create a context for the given hash type.
pub fn pg_cryptohash_create(hash_type: PgCryptohashType) -> Option<Box<PgCryptohashCtx>> {
    let _ = hash_type;
    unimplemented!()
}

/// Initialize the context. Err on failure.
pub fn pg_cryptohash_init(ctx: &mut PgCryptohashCtx) -> Result<(), ()> {
    let _ = ctx;
    unimplemented!()
}

/// Feed data into the context. Err on failure.
pub fn pg_cryptohash_update(ctx: &mut PgCryptohashCtx, data: &[u8]) -> Result<(), ()> {
    let _ = (ctx, data);
    unimplemented!()
}

/// Finalize the digest into `dest`. Err on failure.
pub fn pg_cryptohash_final(ctx: &mut PgCryptohashCtx, dest: &mut [u8]) -> Result<(), ()> {
    let _ = (ctx, dest);
    unimplemented!()
}

/// Release the context.
pub fn pg_cryptohash_free(ctx: Box<PgCryptohashCtx>) {
    let _ = ctx;
    unimplemented!()
}

/// Human-readable error string for the last failure.
pub fn pg_cryptohash_error(ctx: &PgCryptohashCtx) -> &str {
    let _ = ctx;
    unimplemented!()
}
