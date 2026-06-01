//! utilities.rs - 1:1 Rust port of
//! postgres/src/backend/snowball/libstemmer/utilities.c
//!
//! The Snowball stemmer runtime: buffer management (create_s/lose_s with the
//! 8-byte HEAD header trick), UTF-8 skip/get helpers, character grouping tests,
//! string equality, the binary-search find_among engine, and the replace_s
//! slice/realloc machinery plus its slice_* / insert_* / assign_to callers.
//!
//! Shared definitions (symbol, SN_env, among, HEAD, SIZE/SET_SIZE/CAPACITY/
//! SET_CAPACITY) come from crate::snowball::api. malloc/calloc/realloc/free are
//! #defined to palloc/palloc0/repalloc/pfree by header.h, so we use those.

use crate::prelude::*;
use crate::snowball::api::{
    among, symbol, SN_env, CAPACITY, HEAD, SET_CAPACITY, SET_SIZE, SIZE,
};

extern "C" {
    fn memmove(d: *mut c_void, s: *const c_void, n: usize) -> *mut c_void;
    fn memcmp(a: *const c_void, b: *const c_void, n: usize) -> c_int;
}

// #define CREATE_SIZE 1
const CREATE_SIZE: c_int = 1;

// extern symbol * create_s(void)
pub unsafe extern "C" fn create_s() -> *mut symbol {
    // void * mem = malloc(HEAD + (CREATE_SIZE + 1) * sizeof(symbol));
    // malloc -> palloc; sizeof(symbol) == 1
    let mem = palloc((HEAD + ((CREATE_SIZE + 1) as usize) * 1) as Size) as *mut c_char;
    if mem.is_null() {
        return null_mut();
    }
    // p = (symbol *) (HEAD + (char *) mem);
    let p = mem.add(HEAD) as *mut symbol;
    // CAPACITY(p) = CREATE_SIZE;
    SET_CAPACITY(p, CREATE_SIZE);
    // SET_SIZE(p, 0);
    SET_SIZE(p, 0);
    p
}

// extern void lose_s(symbol * p)
pub unsafe extern "C" fn lose_s(p: *mut symbol) {
    if p.is_null() {
        return;
    }
    // free((char *) p - HEAD);
    pfree((p as *mut c_char).sub(HEAD) as *mut c_void);
}

/*
   new_p = skip_utf8(p, c, l, n); skips n characters forwards from p + c.
   new_p is the new position, or -1 on failure.
   -- used to implement hop and next in the utf8 case.
*/
// extern int skip_utf8(const symbol * p, int c, int limit, int n)
pub unsafe extern "C" fn skip_utf8(
    p: *const symbol,
    mut c: c_int,
    limit: c_int,
    mut n: c_int,
) -> c_int {
    let mut b: c_int;
    if n < 0 {
        return -1;
    }
    while n > 0 {
        if c >= limit {
            return -1;
        }
        b = *p.offset(c as isize) as c_int;
        c += 1;
        if b >= 0xC0 {
            // 1100 0000
            while c < limit {
                b = *p.offset(c as isize) as c_int;
                if b >= 0xC0 || b < 0x80 {
                    break;
                }
                // break unless b is 10------
                c += 1;
            }
        }
        n -= 1;
    }
    c
}

/*
   new_p = skip_b_utf8(p, c, lb, n); skips n characters backwards from p + c - 1
   new_p is the new position, or -1 on failure.
   -- used to implement hop and next in the utf8 case.
*/
// extern int skip_b_utf8(const symbol * p, int c, int limit, int n)
pub unsafe extern "C" fn skip_b_utf8(
    p: *const symbol,
    mut c: c_int,
    limit: c_int,
    mut n: c_int,
) -> c_int {
    let mut b: c_int;
    if n < 0 {
        return -1;
    }
    while n > 0 {
        if c <= limit {
            return -1;
        }
        c -= 1;
        b = *p.offset(c as isize) as c_int;
        if b >= 0x80 {
            // 1000 0000
            while c > limit {
                b = *p.offset(c as isize) as c_int;
                if b >= 0xC0 {
                    break; // 1100 0000
                }
                c -= 1;
            }
        }
        n -= 1;
    }
    c
}

/* Code for character groupings: utf8 cases */

// static int get_utf8(const symbol * p, int c, int l, int * slot)
unsafe fn get_utf8(p: *const symbol, mut c: c_int, l: c_int, slot: *mut c_int) -> c_int {
    let b0: c_int;
    let b1: c_int;
    let b2: c_int;
    if c >= l {
        return 0;
    }
    b0 = *p.offset(c as isize) as c_int;
    c += 1;
    if b0 < 0xC0 || c == l {
        // 1100 0000
        *slot = b0;
        return 1;
    }
    b1 = (*p.offset(c as isize) as c_int) & 0x3F;
    c += 1;
    if b0 < 0xE0 || c == l {
        // 1110 0000
        *slot = (b0 & 0x1F) << 6 | b1;
        return 2;
    }
    b2 = (*p.offset(c as isize) as c_int) & 0x3F;
    c += 1;
    if b0 < 0xF0 || c == l {
        // 1111 0000
        *slot = (b0 & 0xF) << 12 | b1 << 6 | b2;
        return 3;
    }
    *slot = (b0 & 0x7) << 18 | b1 << 12 | b2 << 6 | ((*p.offset(c as isize) as c_int) & 0x3F);
    4
}

// static int get_b_utf8(const symbol * p, int c, int lb, int * slot)
unsafe fn get_b_utf8(p: *const symbol, mut c: c_int, lb: c_int, slot: *mut c_int) -> c_int {
    let mut a: c_int;
    let mut b: c_int;
    if c <= lb {
        return 0;
    }
    c -= 1;
    b = *p.offset(c as isize) as c_int;
    if b < 0x80 || c == lb {
        // 1000 0000
        *slot = b;
        return 1;
    }
    a = b & 0x3F;
    c -= 1;
    b = *p.offset(c as isize) as c_int;
    if b >= 0xC0 || c == lb {
        // 1100 0000
        *slot = (b & 0x1F) << 6 | a;
        return 2;
    }
    a |= (b & 0x3F) << 6;
    c -= 1;
    b = *p.offset(c as isize) as c_int;
    if b >= 0xE0 || c == lb {
        // 1110 0000
        *slot = (b & 0xF) << 12 | a;
        return 3;
    }
    c -= 1;
    *slot = ((*p.offset(c as isize) as c_int) & 0x7) << 18 | (b & 0x3F) << 12 | a;
    4
}

// extern int in_grouping_U(struct SN_env * z, const unsigned char * s, int min, int max, int repeat)
pub unsafe extern "C" fn in_grouping_U(
    z: *mut SN_env,
    s: *const c_uchar,
    min: c_int,
    max: c_int,
    repeat: c_int,
) -> c_int {
    loop {
        let mut ch: c_int = 0;
        let w = get_utf8((*z).p, (*z).c, (*z).l, &mut ch);
        if w == 0 {
            return -1;
        }
        if ch > max || {
            ch -= min;
            ch < 0
        } || (*s.offset((ch >> 3) as isize) as c_int & (0x1 << (ch & 0x7))) == 0
        {
            return w;
        }
        (*z).c += w;
        if repeat == 0 {
            break;
        }
    }
    0
}

// extern int in_grouping_b_U(...)
pub unsafe extern "C" fn in_grouping_b_U(
    z: *mut SN_env,
    s: *const c_uchar,
    min: c_int,
    max: c_int,
    repeat: c_int,
) -> c_int {
    loop {
        let mut ch: c_int = 0;
        let w = get_b_utf8((*z).p, (*z).c, (*z).lb, &mut ch);
        if w == 0 {
            return -1;
        }
        if ch > max || {
            ch -= min;
            ch < 0
        } || (*s.offset((ch >> 3) as isize) as c_int & (0x1 << (ch & 0x7))) == 0
        {
            return w;
        }
        (*z).c -= w;
        if repeat == 0 {
            break;
        }
    }
    0
}

// extern int out_grouping_U(...)
pub unsafe extern "C" fn out_grouping_U(
    z: *mut SN_env,
    s: *const c_uchar,
    min: c_int,
    max: c_int,
    repeat: c_int,
) -> c_int {
    loop {
        let mut ch: c_int = 0;
        let w = get_utf8((*z).p, (*z).c, (*z).l, &mut ch);
        if w == 0 {
            return -1;
        }
        if !(ch > max || {
            ch -= min;
            ch < 0
        } || (*s.offset((ch >> 3) as isize) as c_int & (0x1 << (ch & 0x7))) == 0)
        {
            return w;
        }
        (*z).c += w;
        if repeat == 0 {
            break;
        }
    }
    0
}

// extern int out_grouping_b_U(...)
pub unsafe extern "C" fn out_grouping_b_U(
    z: *mut SN_env,
    s: *const c_uchar,
    min: c_int,
    max: c_int,
    repeat: c_int,
) -> c_int {
    loop {
        let mut ch: c_int = 0;
        let w = get_b_utf8((*z).p, (*z).c, (*z).lb, &mut ch);
        if w == 0 {
            return -1;
        }
        if !(ch > max || {
            ch -= min;
            ch < 0
        } || (*s.offset((ch >> 3) as isize) as c_int & (0x1 << (ch & 0x7))) == 0)
        {
            return w;
        }
        (*z).c -= w;
        if repeat == 0 {
            break;
        }
    }
    0
}

/* Code for character groupings: non-utf8 cases */

// extern int in_grouping(...)
pub unsafe extern "C" fn in_grouping(
    z: *mut SN_env,
    s: *const c_uchar,
    min: c_int,
    max: c_int,
    repeat: c_int,
) -> c_int {
    loop {
        let mut ch: c_int;
        if (*z).c >= (*z).l {
            return -1;
        }
        ch = *(*z).p.offset((*z).c as isize) as c_int;
        if ch > max || {
            ch -= min;
            ch < 0
        } || (*s.offset((ch >> 3) as isize) as c_int & (0x1 << (ch & 0x7))) == 0
        {
            return 1;
        }
        (*z).c += 1;
        if repeat == 0 {
            break;
        }
    }
    0
}

// extern int in_grouping_b(...)
pub unsafe extern "C" fn in_grouping_b(
    z: *mut SN_env,
    s: *const c_uchar,
    min: c_int,
    max: c_int,
    repeat: c_int,
) -> c_int {
    loop {
        let mut ch: c_int;
        if (*z).c <= (*z).lb {
            return -1;
        }
        ch = *(*z).p.offset(((*z).c - 1) as isize) as c_int;
        if ch > max || {
            ch -= min;
            ch < 0
        } || (*s.offset((ch >> 3) as isize) as c_int & (0x1 << (ch & 0x7))) == 0
        {
            return 1;
        }
        (*z).c -= 1;
        if repeat == 0 {
            break;
        }
    }
    0
}

// extern int out_grouping(...)
pub unsafe extern "C" fn out_grouping(
    z: *mut SN_env,
    s: *const c_uchar,
    min: c_int,
    max: c_int,
    repeat: c_int,
) -> c_int {
    loop {
        let mut ch: c_int;
        if (*z).c >= (*z).l {
            return -1;
        }
        ch = *(*z).p.offset((*z).c as isize) as c_int;
        if !(ch > max || {
            ch -= min;
            ch < 0
        } || (*s.offset((ch >> 3) as isize) as c_int & (0x1 << (ch & 0x7))) == 0)
        {
            return 1;
        }
        (*z).c += 1;
        if repeat == 0 {
            break;
        }
    }
    0
}

// extern int out_grouping_b(...)
pub unsafe extern "C" fn out_grouping_b(
    z: *mut SN_env,
    s: *const c_uchar,
    min: c_int,
    max: c_int,
    repeat: c_int,
) -> c_int {
    loop {
        let mut ch: c_int;
        if (*z).c <= (*z).lb {
            return -1;
        }
        ch = *(*z).p.offset(((*z).c - 1) as isize) as c_int;
        if !(ch > max || {
            ch -= min;
            ch < 0
        } || (*s.offset((ch >> 3) as isize) as c_int & (0x1 << (ch & 0x7))) == 0)
        {
            return 1;
        }
        (*z).c -= 1;
        if repeat == 0 {
            break;
        }
    }
    0
}

// extern int eq_s(struct SN_env * z, int s_size, const symbol * s)
pub unsafe extern "C" fn eq_s(z: *mut SN_env, s_size: c_int, s: *const symbol) -> c_int {
    if (*z).l - (*z).c < s_size
        || memcmp(
            (*z).p.offset((*z).c as isize) as *const c_void,
            s as *const c_void,
            (s_size as usize) * 1,
        ) != 0
    {
        return 0;
    }
    (*z).c += s_size;
    1
}

// extern int eq_s_b(struct SN_env * z, int s_size, const symbol * s)
pub unsafe extern "C" fn eq_s_b(z: *mut SN_env, s_size: c_int, s: *const symbol) -> c_int {
    if (*z).c - (*z).lb < s_size
        || memcmp(
            (*z).p.offset(((*z).c - s_size) as isize) as *const c_void,
            s as *const c_void,
            (s_size as usize) * 1,
        ) != 0
    {
        return 0;
    }
    (*z).c -= s_size;
    1
}

// extern int eq_v(struct SN_env * z, const symbol * p)
pub unsafe extern "C" fn eq_v(z: *mut SN_env, p: *const symbol) -> c_int {
    eq_s(z, SIZE(p), p)
}

// extern int eq_v_b(struct SN_env * z, const symbol * p)
pub unsafe extern "C" fn eq_v_b(z: *mut SN_env, p: *const symbol) -> c_int {
    eq_s_b(z, SIZE(p), p)
}

// extern int find_among(struct SN_env * z, const struct among * v, int v_size)
pub unsafe extern "C" fn find_among(z: *mut SN_env, v: *const among, v_size: c_int) -> c_int {
    let mut i: c_int = 0;
    let mut j: c_int = v_size;

    let c = (*z).c;
    let l = (*z).l;
    let q = (*z).p.offset(c as isize);

    let mut w: *const among;

    let mut common_i: c_int = 0;
    let mut common_j: c_int = 0;

    let mut first_key_inspected: c_int = 0;

    loop {
        let k = i + ((j - i) >> 1);
        let mut diff: c_int = 0;
        let mut common = if common_i < common_j { common_i } else { common_j }; /* smaller */
        w = v.offset(k as isize);
        {
            let mut i2 = common;
            while i2 < (*w).s_size {
                if c + common == l {
                    diff = -1;
                    break;
                }
                diff = *q.offset(common as isize) as c_int - *(*w).s.offset(i2 as isize) as c_int;
                if diff != 0 {
                    break;
                }
                common += 1;
                i2 += 1;
            }
        }
        if diff < 0 {
            j = k;
            common_j = common;
        } else {
            i = k;
            common_i = common;
        }
        if j - i <= 1 {
            if i > 0 {
                break; /* v->s has been inspected */
            }
            if j == i {
                break; /* only one item in v */
            }
            /* - but now we need to go round once more to get
               v->s inspected. This looks messy, but is actually
               the optimal approach.  */
            if first_key_inspected != 0 {
                break;
            }
            first_key_inspected = 1;
        }
    }
    loop {
        w = v.offset(i as isize);
        if common_i >= (*w).s_size {
            (*z).c = c + (*w).s_size;
            if (*w).function.is_none() {
                return (*w).result;
            }
            {
                let res = ((*w).function.unwrap())(z);
                (*z).c = c + (*w).s_size;
                if res != 0 {
                    return (*w).result;
                }
            }
        }
        i = (*w).substring_i;
        if i < 0 {
            return 0;
        }
    }
}

/* find_among_b is for backwards processing. Same comments apply */
// extern int find_among_b(struct SN_env * z, const struct among * v, int v_size)
pub unsafe extern "C" fn find_among_b(z: *mut SN_env, v: *const among, v_size: c_int) -> c_int {
    let mut i: c_int = 0;
    let mut j: c_int = v_size;

    let c = (*z).c;
    let lb = (*z).lb;
    let q = (*z).p.offset((c - 1) as isize);

    let mut w: *const among;

    let mut common_i: c_int = 0;
    let mut common_j: c_int = 0;

    let mut first_key_inspected: c_int = 0;

    loop {
        let k = i + ((j - i) >> 1);
        let mut diff: c_int = 0;
        let mut common = if common_i < common_j { common_i } else { common_j };
        w = v.offset(k as isize);
        {
            let mut i2 = (*w).s_size - 1 - common;
            while i2 >= 0 {
                if c - common == lb {
                    diff = -1;
                    break;
                }
                diff = *q.offset((-common) as isize) as c_int - *(*w).s.offset(i2 as isize) as c_int;
                if diff != 0 {
                    break;
                }
                common += 1;
                i2 -= 1;
            }
        }
        if diff < 0 {
            j = k;
            common_j = common;
        } else {
            i = k;
            common_i = common;
        }
        if j - i <= 1 {
            if i > 0 {
                break;
            }
            if j == i {
                break;
            }
            if first_key_inspected != 0 {
                break;
            }
            first_key_inspected = 1;
        }
    }
    loop {
        w = v.offset(i as isize);
        if common_i >= (*w).s_size {
            (*z).c = c - (*w).s_size;
            if (*w).function.is_none() {
                return (*w).result;
            }
            {
                let res = ((*w).function.unwrap())(z);
                (*z).c = c - (*w).s_size;
                if res != 0 {
                    return (*w).result;
                }
            }
        }
        i = (*w).substring_i;
        if i < 0 {
            return 0;
        }
    }
}

/* Increase the size of the buffer pointed to by p to at least n symbols.
 * If insufficient memory, returns NULL and frees the old buffer.
 */
// static symbol * increase_size(symbol * p, int n)
unsafe fn increase_size(p: *mut symbol, n: c_int) -> *mut symbol {
    let q: *mut symbol;
    let new_size = n + 20;
    // void * mem = realloc((char *) p - HEAD, HEAD + (new_size + 1) * sizeof(symbol));
    // realloc -> repalloc; sizeof(symbol) == 1
    let mem = repalloc(
        (p as *mut c_char).sub(HEAD) as *mut c_void,
        (HEAD + ((new_size + 1) as usize) * 1) as Size,
    ) as *mut c_char;
    if mem.is_null() {
        lose_s(p);
        return null_mut();
    }
    // q = (symbol *) (HEAD + (char *)mem);
    q = mem.add(HEAD) as *mut symbol;
    // CAPACITY(q) = new_size;
    SET_CAPACITY(q, new_size);
    q
}

/* to replace symbols between c_bra and c_ket in z->p by the
   s_size symbols at s.
   Returns 0 on success, -1 on error.
   Also, frees z->p (and sets it to NULL) on error.
*/
// extern int replace_s(struct SN_env * z, int c_bra, int c_ket, int s_size, const symbol * s, int * adjptr)
pub unsafe extern "C" fn replace_s(
    z: *mut SN_env,
    c_bra: c_int,
    c_ket: c_int,
    s_size: c_int,
    s: *const symbol,
    adjptr: *mut c_int,
) -> c_int {
    let adjustment: c_int;
    let len: c_int;
    if (*z).p.is_null() {
        (*z).p = create_s();
        if (*z).p.is_null() {
            return -1;
        }
    }
    adjustment = s_size - (c_ket - c_bra);
    len = SIZE((*z).p);
    if adjustment != 0 {
        if adjustment + len > CAPACITY((*z).p) {
            (*z).p = increase_size((*z).p, adjustment + len);
            if (*z).p.is_null() {
                return -1;
            }
        }
        memmove(
            (*z).p.offset((c_ket + adjustment) as isize) as *mut c_void,
            (*z).p.offset(c_ket as isize) as *const c_void,
            ((len - c_ket) as usize) * 1,
        );
        SET_SIZE((*z).p, adjustment + len);
        (*z).l += adjustment;
        if (*z).c >= c_ket {
            (*z).c += adjustment;
        } else if (*z).c > c_bra {
            (*z).c = c_bra;
        }
    }
    if s_size != 0 {
        memmove(
            (*z).p.offset(c_bra as isize) as *mut c_void,
            s as *const c_void,
            (s_size as usize) * 1,
        );
    }
    if !adjptr.is_null() {
        *adjptr = adjustment;
    }
    0
}

// static int slice_check(struct SN_env * z)
unsafe fn slice_check(z: *mut SN_env) -> c_int {
    if (*z).bra < 0
        || (*z).bra > (*z).ket
        || (*z).ket > (*z).l
        || (*z).p.is_null()
        || (*z).l > SIZE((*z).p)
    /* this line could be removed */
    {
        return -1;
    }
    0
}

// extern int slice_from_s(struct SN_env * z, int s_size, const symbol * s)
pub unsafe extern "C" fn slice_from_s(z: *mut SN_env, s_size: c_int, s: *const symbol) -> c_int {
    if slice_check(z) != 0 {
        return -1;
    }
    replace_s(z, (*z).bra, (*z).ket, s_size, s, null_mut())
}

// extern int slice_from_v(struct SN_env * z, const symbol * p)
pub unsafe extern "C" fn slice_from_v(z: *mut SN_env, p: *const symbol) -> c_int {
    slice_from_s(z, SIZE(p), p)
}

// extern int slice_del(struct SN_env * z)
pub unsafe extern "C" fn slice_del(z: *mut SN_env) -> c_int {
    slice_from_s(z, 0, null())
}

// extern int insert_s(struct SN_env * z, int bra, int ket, int s_size, const symbol * s)
pub unsafe extern "C" fn insert_s(
    z: *mut SN_env,
    bra: c_int,
    ket: c_int,
    s_size: c_int,
    s: *const symbol,
) -> c_int {
    let mut adjustment: c_int = 0;
    if replace_s(z, bra, ket, s_size, s, &mut adjustment) != 0 {
        return -1;
    }
    if bra <= (*z).bra {
        (*z).bra += adjustment;
    }
    if bra <= (*z).ket {
        (*z).ket += adjustment;
    }
    0
}

// extern int insert_v(struct SN_env * z, int bra, int ket, const symbol * p)
pub unsafe extern "C" fn insert_v(z: *mut SN_env, bra: c_int, ket: c_int, p: *const symbol) -> c_int {
    insert_s(z, bra, ket, SIZE(p), p)
}

// extern symbol * slice_to(struct SN_env * z, symbol * p)
pub unsafe extern "C" fn slice_to(z: *mut SN_env, mut p: *mut symbol) -> *mut symbol {
    if slice_check(z) != 0 {
        lose_s(p);
        return null_mut();
    }
    {
        let len = (*z).ket - (*z).bra;
        if CAPACITY(p) < len {
            p = increase_size(p, len);
            if p.is_null() {
                return null_mut();
            }
        }
        memmove(
            p as *mut c_void,
            (*z).p.offset((*z).bra as isize) as *const c_void,
            (len as usize) * 1,
        );
        SET_SIZE(p, len);
    }
    p
}

// extern symbol * assign_to(struct SN_env * z, symbol * p)
pub unsafe extern "C" fn assign_to(z: *mut SN_env, mut p: *mut symbol) -> *mut symbol {
    let len = (*z).l;
    if CAPACITY(p) < len {
        p = increase_size(p, len);
        if p.is_null() {
            return null_mut();
        }
    }
    memmove(
        p as *mut c_void,
        (*z).p as *const c_void,
        (len as usize) * 1,
    );
    SET_SIZE(p, len);
    p
}

// extern int len_utf8(const symbol * p)
pub unsafe extern "C" fn len_utf8(mut p: *const symbol) -> c_int {
    let mut size = SIZE(p);
    let mut len: c_int = 0;
    while size != 0 {
        size -= 1;
        let b: symbol = *p;
        p = p.offset(1);
        if b >= 0xC0 || b < 0x80 {
            len += 1;
        }
    }
    len
}

// extern void debug(struct SN_env * z, int number, int line_count)
// The C source wraps debug() in `#if 0`, so it is a no-op stub here.
pub unsafe extern "C" fn debug(_z: *mut SN_env, _number: c_int, _line_count: c_int) {}

#[cfg(test)]
mod tests {
    use super::*;
    use core::mem::zeroed;

    // Build an SN_env by hand: zero it, allocate a HEAD-prefixed buffer via
    // create_s, copy bytes in, and set p/c/l (SN_set_current-style).
    unsafe fn make_env(bytes: &[u8]) -> SN_env {
        let mut z: SN_env = zeroed();
        let p = create_s();
        // grow capacity if needed, then memmove bytes and set SIZE.
        let n = bytes.len() as c_int;
        let mut buf = p;
        if CAPACITY(buf) < n {
            buf = increase_size(buf, n);
        }
        for (i, &b) in bytes.iter().enumerate() {
            *buf.add(i) = b;
        }
        SET_SIZE(buf, n);
        z.p = buf;
        z.c = 0;
        z.l = n;
        z.lb = 0;
        z
    }

    #[test]
    fn test_create_s_header() {
        unsafe {
            let p = create_s();
            assert_eq!(CAPACITY(p), CREATE_SIZE);
            assert_eq!(SIZE(p), 0);
            lose_s(p);
        }
    }

    #[test]
    fn test_eq_s_match_and_reject() {
        unsafe {
            let mut z = make_env(b"hello");
            // matches known prefix "he"
            let pat = b"he";
            let r = eq_s(&mut z, 2, pat.as_ptr());
            assert_eq!(r, 1);
            assert_eq!(z.c, 2); // advanced by s_size

            // reset cursor; reject a non-match "xy"
            z.c = 0;
            let bad = b"xy";
            let r2 = eq_s(&mut z, 2, bad.as_ptr());
            assert_eq!(r2, 0);
            assert_eq!(z.c, 0); // unchanged on failure

            lose_s(z.p);
        }
    }

    #[test]
    fn test_skip_utf8_ascii() {
        unsafe {
            let z = make_env(b"abcd");
            // ASCII: skipping n chars advances by exactly n.
            let r = skip_utf8(z.p, 0, z.l, 3);
            assert_eq!(r, 3);
            let r2 = skip_utf8(z.p, 1, z.l, 2);
            assert_eq!(r2, 3);
            // overrun returns -1
            let r3 = skip_utf8(z.p, 0, z.l, 5);
            assert_eq!(r3, -1);
            lose_s(z.p);
        }
    }

    #[test]
    fn test_find_among_two_entries() {
        unsafe {
            // Tiny 2-entry among table over buffer "ba".
            // Entry layout mirrors how Snowball generators order tables:
            // index 0 = "a", index 1 = "ba" (substring_i chains to 0).
            // Searching "ba" at c=0 should match the longer "ba" -> result 2.
            let s_a = b"a";
            let s_ba = b"ba";
            let v: [among; 2] = [
                among {
                    s_size: 1,
                    s: s_a.as_ptr(),
                    substring_i: -1,
                    result: 1,
                    function: None,
                },
                among {
                    s_size: 2,
                    s: s_ba.as_ptr(),
                    substring_i: 0,
                    result: 2,
                    function: None,
                },
            ];
            let mut z = make_env(b"ba");
            let r = find_among(&mut z, v.as_ptr(), 2);
            assert_eq!(r, 2);
            assert_eq!(z.c, 2); // cursor advanced past "ba"
            lose_s(z.p);

            // Searching "ax" should match only "a" -> result 1.
            let mut z2 = make_env(b"ax");
            let r2 = find_among(&mut z2, v.as_ptr(), 2);
            assert_eq!(r2, 1);
            assert_eq!(z2.c, 1);
            lose_s(z2.p);
        }
    }

    #[test]
    fn test_len_utf8_ascii() {
        unsafe {
            let z = make_env(b"hello");
            assert_eq!(len_utf8(z.p), 5);
            lose_s(z.p);
        }
    }
}
