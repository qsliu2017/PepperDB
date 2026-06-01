//! regex/regc_cvec.c - utility functions for handling cvecs
//!
//! This file is #included by regcomp.c.
//!
//! Copyright (c) 1998, 1999 Henry Spencer.  See the PostgreSQL source for the
//! full license text.
//!
//! Notes:
//! Only (selected) functions in _this_ file should treat the chr arrays
//! of a cvec as non-constant.

use crate::prelude::*;

use std::ffi::c_int;

use crate::regex::regcustom::chr;
use crate::regex::regerror::REG_ESPACE;
use crate::regex::regguts::cvec;

// ---------------------------------------------------------------------------
// Local stubs for items defined in regcomp.c (not yet ported).
//
// struct vars is the regex compilation context; only its `cv` (transient cvec)
// and `err` fields are touched here. The real definition lives in regcomp.c.
// TODO: replace with the real `struct vars` once regcomp.c is translated.
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct vars {
    /// transient cvec used while interpreting bracket expressions
    pub cv: *mut cvec,
    /// error code captured by the ERR() macro
    pub err: c_int,
    // ... other fields omitted in this partial port
}

/// C: #define ERR(e) VERR(NULL, (e))  -- records an error in the vars context.
/// TODO: route through the real ERR() once regcomp.c is ported.
unsafe fn ERR(v: *mut vars, e: c_int) {
    if (*v).err == 0 {
        (*v).err = e;
    }
}

/// C: #define MALLOC(n) palloc_extended((n), MCXT_ALLOC_NO_OOM)
/// Returns NULL on failure rather than throwing.
unsafe fn MALLOC(n: usize) -> *mut std::ffi::c_void {
    palloc_extended(n, MCXT_ALLOC_NO_OOM)
}

/// C: #define FREE(p) pfree(VS(p))
unsafe fn FREE(p: *mut std::ffi::c_void) {
    pfree(p);
}

// palloc_extended / pfree / MCXT_ALLOC_NO_OOM come from the memory-manager
// subsystem. Provide local stubs if not yet wired by the prelude.
// TODO: import from crate::utils::mmgr once available.
const MCXT_ALLOC_NO_OOM: c_int = 0x02;

unsafe fn palloc_extended(_size: usize, _flags: c_int) -> *mut std::ffi::c_void {
    unimplemented!()
}

unsafe fn pfree(_pointer: *mut std::ffi::c_void) {
    unimplemented!()
}

/// newcvec - allocate a new cvec
pub unsafe fn newcvec(nchrs: c_int, nranges: c_int) -> *mut cvec {
    let nc: usize = nchrs as usize + nranges as usize * 2;
    let n: usize = std::mem::size_of::<cvec>() + nc * std::mem::size_of::<chr>();
    let cv = MALLOC(n) as *mut cvec;

    if cv.is_null() {
        return std::ptr::null_mut();
    }
    (*cv).chrspace = nchrs;
    (*cv).chrs = ((cv as *mut std::ffi::c_char).add(std::mem::size_of::<cvec>())) as *mut chr;
    (*cv).ranges = (*cv).chrs.add(nchrs as usize);
    (*cv).rangespace = nranges;
    clearcvec(cv)
}

/// clearcvec - clear a possibly-new cvec
/// Returns pointer as convenience.
pub unsafe fn clearcvec(cv: *mut cvec) -> *mut cvec {
    assert!(!cv.is_null());
    (*cv).nchrs = 0;
    (*cv).nranges = 0;
    (*cv).cclasscode = -1;
    cv
}

/// addchr - add a chr to a cvec
pub unsafe fn addchr(cv: *mut cvec, c: chr) {
    assert!((*cv).nchrs < (*cv).chrspace);
    let idx = (*cv).nchrs as usize;
    *(*cv).chrs.add(idx) = c;
    (*cv).nchrs += 1;
}

/// addrange - add a range to a cvec
pub unsafe fn addrange(cv: *mut cvec, from: chr, to: chr) {
    assert!((*cv).nranges < (*cv).rangespace);
    let base = ((*cv).nranges * 2) as usize;
    *(*cv).ranges.add(base) = from;
    *(*cv).ranges.add(base + 1) = to;
    (*cv).nranges += 1;
}

/// getcvec - get a transient cvec, initialized to empty
///
/// The returned cvec is valid only until the next call of getcvec, which
/// typically will recycle the space.  Callers should *not* free the cvec
/// explicitly; it will be cleaned up when the struct vars is destroyed.
///
/// This is typically used while interpreting bracket expressions.  In that
/// usage the cvec is only needed momentarily until we build arcs from it,
/// so transientness is a convenient behavior.
pub unsafe fn getcvec(v: *mut vars, nchrs: c_int, nranges: c_int) -> *mut cvec {
    /* recycle existing transient cvec if large enough */
    if !(*v).cv.is_null()
        && nchrs <= (*(*v).cv).chrspace
        && nranges <= (*(*v).cv).rangespace
    {
        return clearcvec((*v).cv);
    }

    /* nope, make a new one */
    if !(*v).cv.is_null() {
        freecvec((*v).cv);
    }
    (*v).cv = newcvec(nchrs, nranges);
    if (*v).cv.is_null() {
        ERR(v, REG_ESPACE);
    }

    (*v).cv
}

/// freecvec - free a cvec
pub unsafe fn freecvec(cv: *mut cvec) {
    FREE(cv as *mut std::ffi::c_void);
}
