//! Snowball stemmer runtime: environment lifecycle.
//!
//! 1:1 translation of `src/backend/snowball/libstemmer/api.c` merged with the
//! shared declarations from `src/include/snowball/libstemmer/api.h` and the
//! buffer-header / `among` macros from `src/include/snowball/libstemmer/header.h`.
//!
//! This module OWNS the shared Snowball definitions that both `api.rs` and
//! `utilities.rs` rely on: `symbol`, `SN_env`, `among`, `HEAD`, the SIZE /
//! CAPACITY accessor helpers, and the `MAXINT` / `MININT` limits. Everything
//! else (`create_s`, `lose_s`, `replace_s`, ...) lives in
//! `crate::snowball::utilities` and is imported here.

use crate::prelude::*;

// create_s / lose_s / replace_s are defined in utilities.rs. Mutual import
// between the two sibling modules of one crate is fine.
use crate::snowball::utilities::{create_s, lose_s, replace_s};

// ---------------------------------------------------------------------------
// Shared types (api.h)
// ---------------------------------------------------------------------------

// api.h: `typedef unsigned char symbol;`
pub type symbol = u8;

// api.h: struct SN_env. Field order/layout must stay #[repr(C)] because the
// generated stemmer code and utilities.rs both index into it.
#[repr(C)]
pub struct SN_env {
    pub p: *mut symbol,
    pub c: c_int,
    pub l: c_int,
    pub lb: c_int,
    pub bra: c_int,
    pub ket: c_int,
    pub S: *mut *mut symbol,
    pub I: *mut c_int,
}

// header.h: struct among. The `function` member is a nullable function pointer,
// represented as Option<extern "C" fn ...> so that a NULL slot is expressible.
#[repr(C)]
pub struct among {
    pub s_size: c_int,                                              // number of chars in string
    pub s: *const symbol,                                          // search string
    pub substring_i: c_int,                                        // index to longest matching substring
    pub result: c_int,                                            // result of the lookup
    pub function: Option<unsafe extern "C" fn(*mut SN_env) -> c_int>,
}

// The generated stemmers declare `static` among-tables. `among` holds a
// `*const symbol` (raw pointer), which is not `Sync`, so the statics would be
// rejected. The tables are immutable and their `s` pointers reference other
// 'static read-only symbol arrays, so sharing them across threads is sound.
unsafe impl Sync for among {}

// ---------------------------------------------------------------------------
// Limits + buffer-header macros (header.h)
// ---------------------------------------------------------------------------

// header.h: #define MAXINT INT_MAX / #define MININT INT_MIN
pub const MAXINT: c_int = i32::MAX;
pub const MININT: c_int = i32::MIN;

// header.h: #define HEAD 2*sizeof(int)  == 8 bytes. The `symbol *p` returned by
// create_s points HEAD bytes past the allocation; the two ints immediately
// before p hold CAPACITY (at p-8) and SIZE (at p-4).
pub const HEAD: usize = 8;

// header.h: #define SIZE(p) ((int *)(p))[-1]
#[inline]
pub unsafe fn SIZE(p: *const symbol) -> c_int {
    *((p as *const c_int).offset(-1))
}

// header.h: #define SET_SIZE(p, n) ((int *)(p))[-1] = n
#[inline]
pub unsafe fn SET_SIZE(p: *mut symbol, n: c_int) {
    *((p as *mut c_int).offset(-1)) = n;
}

// header.h: #define CAPACITY(p) ((int *)(p))[-2]
#[inline]
pub unsafe fn CAPACITY(p: *const symbol) -> c_int {
    *((p as *const c_int).offset(-2))
}

// Companion setter for CAPACITY (create_s/replace_s in utilities.rs set it).
#[inline]
pub unsafe fn SET_CAPACITY(p: *mut symbol, n: c_int) {
    *((p as *mut c_int).offset(-2)) = n;
}

// ---------------------------------------------------------------------------
// api.c
// ---------------------------------------------------------------------------

// api.c: extern struct SN_env * SN_create_env(int S_size, int I_size)
pub unsafe extern "C" fn SN_create_env(S_size: c_int, I_size: c_int) -> *mut SN_env {
    // calloc(1, sizeof(struct SN_env)) -> palloc0.
    let z = palloc0(core::mem::size_of::<SN_env>()) as *mut SN_env;
    if z.is_null() {
        return null_mut();
    }
    (*z).p = create_s();
    if (*z).p.is_null() {
        // goto error
        SN_close_env(z, S_size);
        return null_mut();
    }
    if S_size != 0 {
        // calloc(S_size, sizeof(symbol *)) -> palloc0.
        (*z).S = palloc0((S_size as usize) * core::mem::size_of::<*mut symbol>())
            as *mut *mut symbol;
        if (*z).S.is_null() {
            SN_close_env(z, S_size);
            return null_mut();
        }

        let mut i: c_int = 0;
        while i < S_size {
            let slot = (*z).S.offset(i as isize);
            *slot = create_s();
            if (*slot).is_null() {
                SN_close_env(z, S_size);
                return null_mut();
            }
            i += 1;
        }
    }

    if I_size != 0 {
        // calloc(I_size, sizeof(int)) -> palloc0.
        (*z).I = palloc0((I_size as usize) * core::mem::size_of::<c_int>()) as *mut c_int;
        if (*z).I.is_null() {
            SN_close_env(z, S_size);
            return null_mut();
        }
    }

    z
}

// api.c: extern void SN_close_env(struct SN_env * z, int S_size)
pub unsafe extern "C" fn SN_close_env(z: *mut SN_env, S_size: c_int) {
    if z.is_null() {
        return;
    }
    if !(*z).S.is_null() {
        let mut i: c_int = 0;
        while i < S_size {
            lose_s(*(*z).S.offset(i as isize));
            i += 1;
        }
        pfree((*z).S as *mut c_void);
    }
    if !(*z).I.is_null() {
        // C calls free(z->I) unconditionally; free(NULL) is a no-op, but pfree
        // requires a real chunk, so guard on non-null (z->I is NULL when never
        // allocated, having come from palloc0).
        pfree((*z).I as *mut c_void);
    }
    if !(*z).p.is_null() {
        lose_s((*z).p);
    }
    pfree(z as *mut c_void);
}

// api.c: extern int SN_set_current(struct SN_env * z, int size, const symbol * s)
pub unsafe extern "C" fn SN_set_current(z: *mut SN_env, size: c_int, s: *const symbol) -> c_int {
    let err = replace_s(z, 0, (*z).l, size, s, null_mut());
    (*z).c = 0;
    err
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    // Verify the buffer-header accessors round-trip at the documented offsets
    // (CAPACITY at p-8, SIZE at p-4) without depending on utilities.rs.
    #[test]
    fn header_accessors_roundtrip() {
        // Lay out [CAPACITY:i32][SIZE:i32][payload...], with p pointing past HEAD.
        let mut buf = [0u8; HEAD + 16];
        unsafe {
            let base = buf.as_mut_ptr();
            let p = base.add(HEAD) as *mut symbol;

            SET_CAPACITY(p, 7);
            SET_SIZE(p, 3);
            assert_eq!(CAPACITY(p), 7);
            assert_eq!(SIZE(p), 3);

            // The two ints land exactly in the HEAD region preceding p.
            assert_eq!(*(base as *const c_int).offset(0), 7); // CAPACITY at p-8
            assert_eq!(*(base as *const c_int).offset(1), 3); // SIZE at p-4
        }
    }

    #[test]
    fn limits_match_c() {
        assert_eq!(MAXINT, 2147483647);
        assert_eq!(MININT, -2147483648);
        assert_eq!(HEAD, 8);
        // sizeof(symbol) must divide HEAD without remainder (api.h note).
        assert_eq!(HEAD % core::mem::size_of::<symbol>(), 0);
    }
}
