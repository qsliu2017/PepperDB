//! port/atomics/arch-arm.h - Atomic operations considerations specific to ARM
//!
//! Faithful 1:1 translation of PostgreSQL 18.3
//! src/include/port/atomics/arch-arm.h.
//!
//! The C header has intentionally no include guards and is only meant to be
//! included by atomics.h (it #error's if INSIDE_ATOMICS_H is not defined).
//! That include-guard machinery has no Rust analogue and is omitted.
//!
//! The header consists solely of preprocessor feature-detection #defines gated
//! on `__aarch64__`:
//!
//!   #if !defined(__aarch64__)
//!   #define PG_DISABLE_64_BIT_ATOMICS
//!   #else
//!   #define PG_HAVE_8BYTE_SINGLE_COPY_ATOMICITY
//!   #endif
//!
//! These C build macros are translated as `pub const` marker flags. Per the
//! port convention we emit the DEFAULT/target branch unconditionally rather
//! than using cfg(...): PepperDB targets ARM64 (aarch64), where 64-bit atomics
//! are NOT disabled and 8-byte single-copy atomicity IS available.
//!
//! NOTES:
//!   64 bit atomics on ARM32 are implemented using kernel fallbacks and thus
//!   might be slow, so they are disabled entirely there. On ARM64 that problem
//!   doesn't exist. The Architecture Reference Manual for ARMv8 states that an
//!   aligned read/write to/from a general purpose register is atomic, hence
//!   PG_HAVE_8BYTE_SINGLE_COPY_ATOMICITY.

/*
 * 64 bit atomics on ARM32 are implemented using kernel fallbacks and thus
 * might be slow, so disable entirely. On ARM64 that problem doesn't exist.
 *
 * #if !defined(__aarch64__)
 *   #define PG_DISABLE_64_BIT_ATOMICS
 * #else
 *   ... (see below)
 * #endif
 *
 * Target is ARM64 (aarch64): PG_DISABLE_64_BIT_ATOMICS is NOT defined.
 * It is provided here as `false` so dependent cfg-like logic can reference it.
 */
pub const PG_DISABLE_64_BIT_ATOMICS: bool = false;

/*
 * Architecture Reference Manual for ARMv8 states aligned read/write to/from
 * general purpose register is atomic.
 *
 * On the aarch64 branch the C header does:
 *   #define PG_HAVE_8BYTE_SINGLE_COPY_ATOMICITY
 */
pub const PG_HAVE_8BYTE_SINGLE_COPY_ATOMICITY: bool = true;
