//! Translated from PostgreSQL src/include/common/hmac.h
//! Generic interface for HMAC.
#![allow(
    clippy::boxed_local,
    clippy::needless_pass_by_value,
    reason = "TODO(stub): drop when implemented; hollow stubs mirror PG signatures 1:1; real impl consumes params"
)]

use crate::common::cryptohash::PgCryptohashType;

/// Opaque context, private to each HMAC implementation.
pub struct PgHmacCtx {
    _private: (),
}

/// Create an HMAC context for the given hash type.
pub fn pg_hmac_create(hash_type: PgCryptohashType) -> Option<Box<PgHmacCtx>> {
    let _ = hash_type;
    unimplemented!()
}

/// Initialize the context with `key`. Err on failure.
pub fn pg_hmac_init(ctx: &mut PgHmacCtx, key: &[u8]) -> Result<(), ()> {
    let _ = (ctx, key);
    unimplemented!()
}

/// Feed `data` into the context. Err on failure.
pub fn pg_hmac_update(ctx: &mut PgHmacCtx, data: &[u8]) -> Result<(), ()> {
    let _ = (ctx, data);
    unimplemented!()
}

/// Finalize the HMAC into `dest`. Err on failure.
pub fn pg_hmac_final(ctx: &mut PgHmacCtx, dest: &mut [u8]) -> Result<(), ()> {
    let _ = (ctx, dest);
    unimplemented!()
}

/// Release the context.
pub fn pg_hmac_free(ctx: Box<PgHmacCtx>) {
    let _ = ctx;
    unimplemented!()
}

/// Human-readable error string for the last failure.
pub fn pg_hmac_error(ctx: &PgHmacCtx) -> &str {
    let _ = ctx;
    unimplemented!()
}
