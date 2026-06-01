//! tsearch/spell.c - Normalizing word with ISpell
//!
//! Source: postgres/src/backend/tsearch/spell.c
//! Merged header: postgres/src/include/tsearch/dicts/spell.h
//!
//! Ispell dictionary
//! -----------------
//!
//! Rules of dictionaries are defined in two files with .affix and .dict
//! extensions. They are used by spell checker programs Ispell and Hunspell.
//!
//! An .affix file declares morphological rules to get a basic form of words.
//! The format of an .affix file has different structure for Ispell and Hunspell
//! dictionaries. The Hunspell format is more complicated. But when an .affix
//! file is imported and compiled, it is stored in the same structure AffixNode.
//!
//! A .dict file stores a list of basic forms of words with references to
//! affix rules. The format of a .dict file has the same structure for Ispell
//! and Hunspell dictionaries.
//!
//! #include mapping:
//!   - "postgres.h"                  -> crate::prelude::*
//!   - "catalog/pg_collation.h"      -> DEFAULT_COLLATION_OID const below
//!   - "miscadmin.h"                 -> crate::miscadmin::check_stack_depth
//!   - "tsearch/dicts/spell.h"       -> merged below (IspellDict, SPELL, AFFIX, ...)
//!   - "tsearch/ts_locale.h"         -> crate::tsearch::ts_locale (t_iseq, t_isalpha_cstr,
//!                                      ts_copychar_cstr, ts_copychar_with_len,
//!                                      tsearch_readline*)
//!   - "utils/formatting.h"          -> crate::utils::adt::formatting::str_tolower
//!   - "utils/memutils.h"            -> crate::prelude (MemoryContext*, ALLOCSET_*)
//!   - "regex/regex.h"               -> crate::regex::regex (regex_t, pg_regcomp, ...)
//!   - "tsearch/dicts/regis.h"       -> crate::tsearch::regis (Regis, RS_*)
//!   - "mb/pg_wchar.h"               -> crate::mb::mbutils (pg_mblen_cstr, pg_mb2wchar_with_len)

use crate::prelude::*;

use crate::mb::mbutils::{pg_mb2wchar_with_len, pg_mblen_cstr};
use crate::mb::wchar::pg_wchar;
use crate::miscadmin::check_stack_depth;
use crate::regex::regex::{
    pg_regcomp, pg_regerror, pg_regexec, regex_t, REG_ADVANCED, REG_NOSUB, REG_OKAY,
};
use crate::tsearch::regis::{Regis, RS_compile, RS_execute, RS_isRegis};
use crate::tsearch::ts_locale::{
    t_isalpha_cstr, t_iseq, ts_copychar_cstr, ts_copychar_with_len, tsearch_readline,
    tsearch_readline_begin, tsearch_readline_end, tsearch_readline_state,
};
use crate::tsearch::ts_public::TSLexeme;
use crate::utils::adt::formatting::str_tolower;

use core::ffi::CStr;

// catalog/pg_collation_d.h
const DEFAULT_COLLATION_OID: Oid = 100;

extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn strcpy(dest: *mut c_char, src: *const c_char) -> *mut c_char;
    fn strcat(dest: *mut c_char, src: *const c_char) -> *mut c_char;
    fn strchr(s: *const c_char, c: c_int) -> *mut c_char;
    fn strstr(haystack: *const c_char, needle: *const c_char) -> *mut c_char;
    fn strtol(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> c_long;
    fn atoi(s: *const c_char) -> c_int;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
    fn memset(s: *mut c_void, c: c_int, n: usize) -> *mut c_void;
    fn sprintf(buf: *mut c_char, fmt: *const c_char, ...) -> c_int;
    fn isdigit(c: c_int) -> c_int;
    fn isspace(c: c_int) -> c_int;
    fn isprint(c: c_int) -> c_int;

    /* errno access (Darwin) */
    #[link_name = "__error"]
    fn __error() -> *mut c_int;

    /* libc qsort / bsearch */
    fn qsort(
        base: *mut c_void,
        nmemb: usize,
        size: usize,
        compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
    );
    fn bsearch(
        key: *const c_void,
        base: *const c_void,
        nmemb: usize,
        size: usize,
        compar: unsafe extern "C" fn(*const c_void, *const c_void) -> c_int,
    ) -> *mut c_void;
}

#[inline]
unsafe fn get_errno() -> c_int {
    *__error()
}
#[inline]
unsafe fn set_errno(v: c_int) {
    *__error() = v;
}

// ERANGE on Darwin
const ERANGE: c_int = 34;
// BUFSIZ (stdio.h, Darwin)
const BUFSIZ: usize = 1024;
// C size_t
#[allow(non_camel_case_types)]
type size_t = usize;

// errcode placeholders folded from ereport (kept for "C also:" comments below).
// ERRCODE_CONFIG_FILE_ERROR / ERRCODE_INVALID_REGULAR_EXPRESSION are dropped into
// the single-message ereport! per conventions.

// ----------------------------------------------------------------------------
// Merged tsearch/dicts/spell.h definitions.
// ----------------------------------------------------------------------------

/*
 * Names of FF_ are correlated with Hunspell options in affix file
 * https://hunspell.github.io/
 */
pub const FF_COMPOUNDONLY: u32 = 0x01;
pub const FF_COMPOUNDBEGIN: u32 = 0x02;
pub const FF_COMPOUNDMIDDLE: u32 = 0x04;
pub const FF_COMPOUNDLAST: u32 = 0x08;
pub const FF_COMPOUNDFLAG: u32 = FF_COMPOUNDBEGIN | FF_COMPOUNDMIDDLE | FF_COMPOUNDLAST;
pub const FF_COMPOUNDFLAGMASK: u32 = 0x0f;

/*
 * affixes use dictionary flags too
 */
pub const FF_COMPOUNDPERMITFLAG: u32 = 0x10;
pub const FF_COMPOUNDFORBIDFLAG: u32 = 0x20;
pub const FF_CROSSPRODUCT: u32 = 0x40;

/*
 * Don't change the order of these. Initialization sorts by these,
 * and expects prefixes to come first after sorting.
 */
pub const FF_SUFFIX: c_int = 1;
pub const FF_PREFIX: c_int = 0;

pub const FLAGNUM_MAXSIZE: c_int = 1 << 16;

/*
 * SPNode and SPNodeData are used to represent prefix tree (Trie) to store
 * a words list.
 *
 * Upstream packs val:8/isword:1/compoundflag:4/affix:19 into one uint32 bitfield.
 * The :19 affix index easily exceeds the other widths, so we keep all four as
 * plain fields; behaviour is identical and the only cost is a wider struct.
 */
#[repr(C)]
pub struct SPNodeData {
    /// upstream `val:8`
    pub val: u32,
    /// upstream `isword:1`
    pub isword: u32,
    /// Stores compound flags (upstream `compoundflag:4`).
    pub compoundflag: u32,
    /// Reference to an entry of the AffixData field (upstream `affix:19`).
    pub affix: u32,
    pub node: *mut SPNode,
}

/*
 * typedef struct SPNode {
 *     uint32 length;
 *     SPNodeData data[FLEXIBLE_ARRAY_MEMBER];
 * } SPNode;
 */
#[repr(C)]
pub struct SPNode {
    pub length: uint32,
    // data[] (FLEXIBLE_ARRAY_MEMBER) lives in the cpalloc'd bytes after the
    // header; addressed via spnode_data().
}

/// `#define SPNHDRSZ (offsetof(SPNode,data))`
#[inline]
fn SPNHDRSZ() -> Size {
    core::mem::size_of::<SPNode>()
}

/// Raw pointer to the inline `data[]` array following an SPNode header.
#[inline]
unsafe fn spnode_data(node: *mut SPNode) -> *mut SPNodeData {
    (node as *mut u8).add(SPNHDRSZ()) as *mut SPNodeData
}

/*
 * Represents an entry in a words list.
 *
 * union p { const char *flag;  struct { int affix; int len; } d; }
 * Rust unions can't hold a (Copy-free) raw-pointer/struct overlap cleanly, but
 * both alternatives are POD here, so a #[repr(C)] union works.
 */
#[repr(C)]
#[derive(Clone, Copy)]
pub struct SpellDPart {
    /// Reference to an entry of the AffixData field
    pub affix: c_int,
    /// Length of the word
    pub len: c_int,
}

#[repr(C)]
pub union SpellP {
    /// flag is filled in by NIImportDictionary(). After NISortDictionary(),
    /// d is used instead of flag.
    pub flag: *const c_char,
    /// d is used in mkSPNode()
    pub d: SpellDPart,
}

/*
 * typedef struct spell_struct {
 *     union { ... } p;
 *     char word[FLEXIBLE_ARRAY_MEMBER];
 * } SPELL;
 */
#[repr(C)]
pub struct SPELL {
    pub p: SpellP,
    // word[] (FLEXIBLE_ARRAY_MEMBER) follows the header; addressed via spell_word().
}

/// `#define SPELLHDRSZ (offsetof(SPELL, word))`
#[inline]
fn SPELLHDRSZ() -> Size {
    core::mem::size_of::<SPELL>()
}

/// Raw pointer to the inline `word[]` bytes following a SPELL header.
#[inline]
unsafe fn spell_word(s: *mut SPELL) -> *mut c_char {
    (s as *mut u8).add(SPELLHDRSZ()) as *mut c_char
}

/*
 * Represents an entry in an affix list.
 *
 * Upstream packs type:1/flagflags:7/issimple:1/isregis:1/replen:14 into one
 * uint32 bitfield.  We keep them as plain fields.
 */
#[repr(C)]
pub struct AFFIX {
    pub flag: *const c_char,
    /// FF_SUFFIX or FF_PREFIX (upstream `type:1`)
    pub r#type: u32,
    /// upstream `flagflags:7`
    pub flagflags: u32,
    /// upstream `issimple:1`
    pub issimple: u32,
    /// upstream `isregis:1`
    pub isregis: u32,
    /// upstream `replen:14`
    pub replen: u32,
    pub find: *const c_char,
    pub repl: *const c_char,
    pub reg: AffixReg,
}

/*
 * union reg { regex_t *pregex; Regis regis; }
 *
 * Arrays of AFFIX are moved and sorted.  We use a pointer to regex_t to keep
 * this struct small, and avoid assuming that regex_t is movable.
 */
#[repr(C)]
pub union AffixReg {
    pub pregex: *mut regex_t,
    pub regis: core::mem::ManuallyDrop<Regis>,
}

/*
 * AffixNode and AffixNodeData are used to represent prefix tree (Trie) to store
 * an affix list.
 *
 * Upstream packs val:8/naff:24 into one uint32 bitfield; kept as plain fields.
 */
#[repr(C)]
pub struct AffixNodeData {
    /// upstream `val:8`
    pub val: u32,
    /// upstream `naff:24`
    pub naff: u32,
    pub aff: *mut *mut AFFIX,
    pub node: *mut AffixNode,
}

/*
 * typedef struct AffixNode {
 *     uint32 isvoid:1, length:31;
 *     AffixNodeData data[FLEXIBLE_ARRAY_MEMBER];
 * } AffixNode;
 */
#[repr(C)]
pub struct AffixNode {
    /// upstream `isvoid:1`
    pub isvoid: u32,
    /// upstream `length:31`
    pub length: u32,
    // data[] (FLEXIBLE_ARRAY_MEMBER) follows the header; addressed via anode_data().
}

/// `#define ANHRDSZ (offsetof(AffixNode, data))`
#[inline]
fn ANHRDSZ() -> Size {
    core::mem::size_of::<AffixNode>()
}

/// Raw pointer to the inline `data[]` array following an AffixNode header.
#[inline]
unsafe fn anode_data(node: *mut AffixNode) -> *mut AffixNodeData {
    (node as *mut u8).add(ANHRDSZ()) as *mut AffixNodeData
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct CMPDAffix {
    pub affix: *const c_char,
    pub len: c_int,
    pub issuffix: bool,
}

/*
 * Type of encoding affix flags in Hunspell dictionaries
 */
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum FlagMode {
    /// one character (like ispell)
    FM_CHAR,
    /// two characters
    FM_LONG,
    /// number, >= 0 and < 65536
    FM_NUM,
}
use FlagMode::*;

/*
 * Structure to store Hunspell options. Flag representation depends on flag
 * type. These flags are about support of compound words.
 */
#[repr(C)]
pub union CompoundAffixFlagVal {
    /// Flag name if flagMode is FM_CHAR or FM_LONG
    pub s: *const c_char,
    /// Flag name if flagMode is FM_NUM
    pub i: u32,
}

#[repr(C)]
pub struct CompoundAffixFlag {
    pub flag: CompoundAffixFlagVal,
    /// we don't have a bsearch_arg version, so, copy FlagMode
    pub flagMode: FlagMode,
    pub value: u32,
}

#[repr(C)]
pub struct IspellDict {
    pub maffixes: c_int,
    pub naffixes: c_int,
    pub Affix: *mut AFFIX,

    pub Suffix: *mut AffixNode,
    pub Prefix: *mut AffixNode,

    pub Dictionary: *mut SPNode,
    /// Array of sets of affixes
    pub AffixData: *mut *const c_char,
    pub lenAffixData: c_int,
    pub nAffixData: c_int,
    pub useFlagAliases: bool,

    pub CompoundAffix: *mut CMPDAffix,

    pub usecompound: bool,
    pub flagMode: FlagMode,

    /*
     * All follow fields are actually needed only for initialization
     */

    /// Array of Hunspell options in affix file
    pub CompoundAffixFlags: *mut CompoundAffixFlag,
    /// number of entries in CompoundAffixFlags array
    pub nCompoundAffixFlag: c_int,
    /// allocated length of CompoundAffixFlags array
    pub mCompoundAffixFlag: c_int,

    /*
     * Remaining fields are only used during dictionary construction; they are
     * set up by NIStartBuild and cleared by NIFinishBuild.
     */
    pub buildCxt: MemoryContext, /* temp context for construction */

    /// Temporary array of all words in the dict file
    pub Spell: *mut *mut SPELL,
    pub nspell: c_int, /* number of valid entries in Spell array */
    pub mspell: c_int, /* allocated length of Spell array */

    /* These are used to allocate "compact" data without palloc overhead */
    pub firstfree: *mut c_char, /* first free address (always maxaligned) */
    pub avail: Size,            /* free space remaining at firstfree */
}

// ----------------------------------------------------------------------------
// Local alloc helpers.
//
// Upstream:
//   #define tmpalloc(sz)  MemoryContextAlloc(Conf->buildCxt, (sz))
//   #define tmpalloc0(sz) MemoryContextAllocZero(Conf->buildCxt, (sz))
// ----------------------------------------------------------------------------

#[inline]
unsafe fn tmpalloc(Conf: *mut IspellDict, sz: Size) -> *mut c_void {
    MemoryContextAlloc((*Conf).buildCxt, sz)
}
#[inline]
unsafe fn tmpalloc0(Conf: *mut IspellDict, sz: Size) -> *mut c_void {
    MemoryContextAllocZero((*Conf).buildCxt, sz)
}

// MAXALIGN (c.h)
#[inline]
const fn MAXALIGN(len: usize) -> usize {
    (len + 7) & !7
}

/*
 * Prepare for constructing an ISpell dictionary.
 *
 * The IspellDict struct is assumed to be zeroed when allocated.
 */
#[unsafe(no_mangle)]
pub unsafe fn NIStartBuild(Conf: *mut IspellDict) {
    /*
     * The temp context is a child of CurTransactionContext, so that it will
     * go away automatically on error.
     */
    (*Conf).buildCxt = AllocSetContextCreate!(
        CurTransactionContext,
        c"Ispell dictionary init context".as_ptr(),
        ALLOCSET_DEFAULT_SIZES
    );
}

/*
 * Clean up when dictionary construction is complete.
 */
#[unsafe(no_mangle)]
pub unsafe fn NIFinishBuild(Conf: *mut IspellDict) {
    /* Release no-longer-needed temp memory */
    MemoryContextDelete((*Conf).buildCxt);
    /* Just for cleanliness, zero the now-dangling pointers */
    (*Conf).buildCxt = null_mut();
    (*Conf).Spell = null_mut();
    (*Conf).firstfree = null_mut();
    (*Conf).CompoundAffixFlags = null_mut();
}

/*
 * "Compact" palloc: allocate without extra palloc overhead.
 *
 * Since we have no need to free the ispell data items individually, there's
 * not much value in the per-chunk overhead normally consumed by palloc.
 * Getting rid of it is helpful since ispell can allocate a lot of small nodes.
 *
 * We currently pre-zero all data allocated this way, even though some of it
 * doesn't need that.  The cpalloc and cpalloc0 macros are just documentation
 * to indicate which allocations actually require zeroing.
 */
const COMPACT_ALLOC_CHUNK: Size = 8192; /* amount to get from palloc at once */
const COMPACT_MAX_REQ: Size = 1024; /* must be < COMPACT_ALLOC_CHUNK */

unsafe fn compact_palloc0(Conf: *mut IspellDict, mut size: Size) -> *mut c_void {
    let result: *mut c_void;

    /* Should only be called during init */
    Assert!(!(*Conf).buildCxt.is_null());

    /* No point in this for large chunks */
    if size > COMPACT_MAX_REQ {
        return palloc0(size);
    }

    /* Keep everything maxaligned */
    size = MAXALIGN(size);

    /* Need more space? */
    if size > (*Conf).avail {
        (*Conf).firstfree = palloc0(COMPACT_ALLOC_CHUNK) as *mut c_char;
        (*Conf).avail = COMPACT_ALLOC_CHUNK;
    }

    result = (*Conf).firstfree as *mut c_void;
    (*Conf).firstfree = (*Conf).firstfree.add(size);
    (*Conf).avail -= size;

    result
}

#[inline]
unsafe fn cpalloc(Conf: *mut IspellDict, size: Size) -> *mut c_void {
    compact_palloc0(Conf, size)
}
#[inline]
unsafe fn cpalloc0(Conf: *mut IspellDict, size: Size) -> *mut c_void {
    compact_palloc0(Conf, size)
}

unsafe fn cpstrdup(Conf: *mut IspellDict, str: *const c_char) -> *mut c_char {
    let res = cpalloc(Conf, strlen(str) + 1) as *mut c_char;

    strcpy(res, str);
    res
}

/*
 * Apply str_tolower(), producing a temporary result (in the buildCxt).
 */
unsafe fn lowerstr_ctx(Conf: *mut IspellDict, src: *const c_char) -> *mut c_char {
    let saveCtx: MemoryContext;
    let dst: *mut c_char;

    saveCtx = MemoryContextSwitchTo((*Conf).buildCxt);
    dst = str_tolower(src, strlen(src), DEFAULT_COLLATION_OID);
    MemoryContextSwitchTo(saveCtx);

    dst
}

const MAX_NORM: usize = 1024;
const MAXNORMLEN: usize = 256;

/// `#define STRNCMP(s,p) strncmp((s),(p),strlen(p))`
#[inline]
unsafe fn STRNCMP(s: *const c_char, p: *const c_char) -> c_int {
    strncmp(s, p, strlen(p))
}

/// `#define GETWCHAR(W,L,N,T) ( ((const uint8*)(W))[ ((T)==FF_PREFIX) ? (N) : ((L)-1-(N)) ] )`
#[inline]
unsafe fn GETWCHAR(w: *const c_char, l: c_int, n: c_int, t: c_int) -> u8 {
    let idx = if t == FF_PREFIX { n } else { l - 1 - n };
    *(w as *const u8).add(idx as usize)
}

/// `#define GETCHAR(A,N,T) GETWCHAR((A)->repl, (A)->replen, N, T)`
#[inline]
unsafe fn GETCHAR(a: *const AFFIX, n: c_int, t: c_int) -> u8 {
    GETWCHAR((*a).repl, (*a).replen as c_int, n, t)
}

static mut VoidString: [c_char; 1] = [0];

#[inline]
unsafe fn voidstring() -> *const c_char {
    core::ptr::addr_of!(VoidString) as *const c_char
}

unsafe extern "C" fn cmpspell(s1: *const c_void, s2: *const c_void) -> c_int {
    let a = *(s1 as *const *const SPELL);
    let b = *(s2 as *const *const SPELL);
    strcmp(spell_word(a as *mut SPELL), spell_word(b as *mut SPELL))
}

unsafe extern "C" fn cmpspellaffix(s1: *const c_void, s2: *const c_void) -> c_int {
    let a = *(s1 as *const *const SPELL);
    let b = *(s2 as *const *const SPELL);
    strcmp((*a).p.flag, (*b).p.flag)
}

unsafe extern "C" fn cmpcmdflag(f1: *const c_void, f2: *const c_void) -> c_int {
    let fv1 = f1 as *const CompoundAffixFlag;
    let fv2 = f2 as *const CompoundAffixFlag;

    Assert!((*fv1).flagMode == (*fv2).flagMode);

    if (*fv1).flagMode == FM_NUM {
        if (*fv1).flag.i == (*fv2).flag.i {
            return 0;
        }

        return if (*fv1).flag.i > (*fv2).flag.i { 1 } else { -1 };
    }

    strcmp((*fv1).flag.s, (*fv2).flag.s)
}

unsafe fn findchar(mut str: *mut c_char, c: c_int) -> *mut c_char {
    while *str != 0 {
        if t_iseq(str, c as c_char) {
            return str;
        }
        str = str.add(pg_mblen_cstr(str) as usize);
    }

    null_mut()
}

unsafe fn findchar2(mut str: *mut c_char, c1: c_int, c2: c_int) -> *mut c_char {
    while *str != 0 {
        if t_iseq(str, c1 as c_char) || t_iseq(str, c2 as c_char) {
            return str;
        }
        str = str.add(pg_mblen_cstr(str) as usize);
    }

    null_mut()
}

/* backward string compare for suffix tree operations */
unsafe fn strbcmp(s1: *const c_uchar, s2: *const c_uchar) -> c_int {
    let mut l1: c_int = strlen(s1 as *const c_char) as c_int - 1;
    let mut l2: c_int = strlen(s2 as *const c_char) as c_int - 1;

    while l1 >= 0 && l2 >= 0 {
        if *s1.add(l1 as usize) < *s2.add(l2 as usize) {
            return -1;
        }
        if *s1.add(l1 as usize) > *s2.add(l2 as usize) {
            return 1;
        }
        l1 -= 1;
        l2 -= 1;
    }
    if l1 < l2 {
        return -1;
    }
    if l1 > l2 {
        return 1;
    }

    0
}

unsafe fn strbncmp(s1: *const c_uchar, s2: *const c_uchar, count: Size) -> c_int {
    let mut l1: c_int = strlen(s1 as *const c_char) as c_int - 1;
    let mut l2: c_int = strlen(s2 as *const c_char) as c_int - 1;
    let mut l: c_int = count as c_int;

    while l1 >= 0 && l2 >= 0 && l > 0 {
        if *s1.add(l1 as usize) < *s2.add(l2 as usize) {
            return -1;
        }
        if *s1.add(l1 as usize) > *s2.add(l2 as usize) {
            return 1;
        }
        l1 -= 1;
        l2 -= 1;
        l -= 1;
    }
    if l == 0 {
        return 0;
    }
    if l1 < l2 {
        return -1;
    }
    if l1 > l2 {
        return 1;
    }
    0
}

/*
 * Compares affixes.
 * First compares the type of an affix. Prefixes should go before affixes.
 * If types are equal then compares replaceable string.
 */
unsafe extern "C" fn cmpaffix(s1: *const c_void, s2: *const c_void) -> c_int {
    let a1 = s1 as *const AFFIX;
    let a2 = s2 as *const AFFIX;

    if (*a1).r#type < (*a2).r#type {
        return -1;
    }
    if (*a1).r#type > (*a2).r#type {
        return 1;
    }
    if (*a1).r#type == FF_PREFIX as u32 {
        strcmp((*a1).repl, (*a2).repl)
    } else {
        strbcmp((*a1).repl as *const c_uchar, (*a2).repl as *const c_uchar)
    }
}

/*
 * Gets an affix flag from the set of affix flags (sflagset).
 *
 * Several flags can be stored in a single string. Flags can be represented by:
 * - 1 character (FM_CHAR). A character may be Unicode.
 * - 2 characters (FM_LONG). A character may be Unicode.
 * - numbers from 1 to 65000 (FM_NUM).
 *
 * Depending on the flagMode an affix string can have the following format:
 * - FM_CHAR: ABCD
 *	 Here we have 4 flags: A, B, C and D
 * - FM_LONG: ABCDE*
 *	 Here we have 3 flags: AB, CD and E*
 * - FM_NUM: 200,205,50
 *	 Here we have 3 flags: 200, 205 and 50
 *
 * Conf: current dictionary.
 * sflagset: the set of affix flags. Returns a reference to the start of a next
 *			 affix flag.
 * sflag: returns an affix flag from sflagset.
 */
unsafe fn getNextFlagFromString(
    Conf: *mut IspellDict,
    sflagset: *mut *const c_char,
    mut sflag: *mut c_char,
) {
    let mut s: int32;
    let mut next: *mut c_char = null_mut();
    let sbuf: *const c_char = *sflagset;
    let maxstep_init: c_int;
    let mut maxstep: c_int;
    let mut clen: c_int;
    let mut stop: bool = false;
    let mut met_comma: bool = false;

    maxstep_init = if (*Conf).flagMode == FM_LONG { 2 } else { 1 };
    maxstep = maxstep_init;

    while **sflagset != 0 {
        match (*Conf).flagMode {
            FM_LONG | FM_CHAR => {
                clen = ts_copychar_cstr(sflag, *sflagset);
                sflag = sflag.add(clen as usize);

                /* Go to start of the next flag */
                *sflagset = (*sflagset).add(clen as usize);

                /* Check if we get all characters of flag */
                maxstep -= 1;
                stop = maxstep == 0;
            }
            FM_NUM => {
                set_errno(0);
                s = strtol(*sflagset, &mut next, 10) as int32;
                if *sflagset == next as *const c_char || get_errno() == ERANGE {
                    /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
                    ereport!(
                        ERROR,
                        errmsg!(
                            "invalid affix flag \"{}\"",
                            CStr::from_ptr(*sflagset).to_string_lossy()
                        )
                    );
                }
                if s < 0 || s > FLAGNUM_MAXSIZE {
                    /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
                    ereport!(
                        ERROR,
                        errmsg!(
                            "affix flag \"{}\" is out of range",
                            CStr::from_ptr(*sflagset).to_string_lossy()
                        )
                    );
                }
                sflag = sflag.add(sprintf(sflag, c"%0d".as_ptr(), s) as usize);

                /* Go to start of the next flag */
                *sflagset = next;
                while **sflagset != 0 {
                    if isdigit(**sflagset as c_uchar as c_int) != 0 {
                        if !met_comma {
                            /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
                            ereport!(
                                ERROR,
                                errmsg!(
                                    "invalid affix flag \"{}\"",
                                    CStr::from_ptr(*sflagset).to_string_lossy()
                                )
                            );
                        }
                        break;
                    } else if t_iseq(*sflagset, b',' as c_char) {
                        if met_comma {
                            /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
                            ereport!(
                                ERROR,
                                errmsg!(
                                    "invalid affix flag \"{}\"",
                                    CStr::from_ptr(*sflagset).to_string_lossy()
                                )
                            );
                        }
                        met_comma = true;
                    } else if isspace(**sflagset as c_uchar as c_int) == 0 {
                        /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
                        ereport!(
                            ERROR,
                            errmsg!(
                                "invalid character in affix flag \"{}\"",
                                CStr::from_ptr(*sflagset).to_string_lossy()
                            )
                        );
                    }

                    *sflagset = (*sflagset).add(pg_mblen_cstr(*sflagset) as usize);
                }
                stop = true;
            }
        }

        if stop {
            break;
        }
    }

    if (*Conf).flagMode == FM_LONG && maxstep > 0 {
        /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
        ereport!(
            ERROR,
            errmsg!(
                "invalid affix flag \"{}\" with \"long\" flag value",
                CStr::from_ptr(sbuf).to_string_lossy()
            )
        );
    }

    *sflag = b'\0' as c_char;
}

/*
 * Checks if the affix set Conf->AffixData[affix] contains affixflag.
 * Conf->AffixData[affix] does not contain affixflag if this flag is not used
 * actually by the .dict file.
 *
 * Conf: current dictionary.
 * affix: index of the Conf->AffixData array.
 * affixflag: the affix flag.
 *
 * Returns true if the string Conf->AffixData[affix] contains affixflag,
 * otherwise returns false.
 */
unsafe fn IsAffixFlagInUse(
    Conf: *mut IspellDict,
    affix: c_int,
    affixflag: *const c_char,
) -> bool {
    let mut flagcur: *const c_char;
    let mut flag: [c_char; BUFSIZ] = [0; BUFSIZ];

    if *affixflag == 0 {
        return true;
    }

    Assert!(affix < (*Conf).nAffixData);

    flagcur = *(*Conf).AffixData.add(affix as usize);

    while *flagcur != 0 {
        getNextFlagFromString(Conf, &mut flagcur, flag.as_mut_ptr());
        /* Compare first affix flag in flagcur with affixflag */
        if strcmp(flag.as_ptr(), affixflag) == 0 {
            return true;
        }
    }

    /* Could not find affixflag */
    false
}

/*
 * Adds the new word into the temporary array Spell.
 *
 * Conf: current dictionary.
 * word: new word.
 * flag: set of affix flags. Single flag can be get by getNextFlagFromString().
 */
unsafe fn NIAddSpell(Conf: *mut IspellDict, word: *const c_char, flag: *const c_char) {
    if (*Conf).nspell >= (*Conf).mspell {
        if (*Conf).mspell != 0 {
            (*Conf).mspell *= 2;
            (*Conf).Spell = repalloc(
                (*Conf).Spell as *mut c_void,
                (*Conf).mspell as usize * core::mem::size_of::<*mut SPELL>(),
            ) as *mut *mut SPELL;
        } else {
            (*Conf).mspell = 1024 * 20;
            (*Conf).Spell = tmpalloc(
                Conf,
                (*Conf).mspell as usize * core::mem::size_of::<*mut SPELL>(),
            ) as *mut *mut SPELL;
        }
    }
    *(*Conf).Spell.add((*Conf).nspell as usize) =
        tmpalloc(Conf, SPELLHDRSZ() + strlen(word) + 1) as *mut SPELL;
    let cur = *(*Conf).Spell.add((*Conf).nspell as usize);
    strcpy(spell_word(cur), word);
    (*cur).p.flag = if *flag != b'\0' as c_char {
        cpstrdup(Conf, flag)
    } else {
        voidstring()
    };
    (*Conf).nspell += 1;
}

/*
 * Imports dictionary into the temporary array Spell.
 *
 * Note caller must already have applied get_tsearch_config_filename.
 *
 * Conf: current dictionary.
 * filename: path to the .dict file.
 */
#[unsafe(no_mangle)]
pub unsafe fn NIImportDictionary(Conf: *mut IspellDict, filename: *const c_char) {
    let mut trst: tsearch_readline_state = core::mem::zeroed();
    let mut line: *mut c_char;

    if !tsearch_readline_begin(&mut trst, filename) {
        /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR); C appends ": %m" errno
         * text, not ported here. */
        ereport!(
            ERROR,
            errmsg!(
                "could not open dictionary file \"{}\"",
                CStr::from_ptr(filename).to_string_lossy()
            )
        );
    }

    line = tsearch_readline(&mut trst);
    while !line.is_null() {
        let mut s: *mut c_char;
        let mut pstr: *mut c_char;

        /* Set of affix flags */
        let mut flag: *const c_char;

        /* Extract flag from the line */
        s = findchar(line, b'/' as c_int);
        if !s.is_null() {
            *s = b'\0' as c_char;
            s = s.add(1);
            flag = s;
            while *s != 0 {
                /* we allow only single encoded flags for faster works */
                if pg_mblen_cstr(s) == 1
                    && isprint(*s as c_uchar as c_int) != 0
                    && isspace(*s as c_uchar as c_int) == 0
                {
                    s = s.add(1);
                } else {
                    *s = b'\0' as c_char;
                    break;
                }
            }
        } else {
            flag = c"".as_ptr();
        }

        /* Remove trailing spaces */
        s = line;
        while *s != 0 {
            if isspace(*s as c_uchar as c_int) != 0 {
                *s = b'\0' as c_char;
                break;
            }
            s = s.add(pg_mblen_cstr(s) as usize);
        }
        pstr = lowerstr_ctx(Conf, line);

        NIAddSpell(Conf, pstr, flag);
        pfree(pstr as *mut c_void);

        pfree(line as *mut c_void);

        line = tsearch_readline(&mut trst);
    }
    tsearch_readline_end(&mut trst);
}

/*
 * Searches a basic form of word in the prefix tree. This word was generated
 * using an affix rule. This rule may not be presented in an affix set of
 * a basic form of word.
 *
 * For example, we have the entry in the .dict file:
 * meter/GMD
 *
 * The affix rule with the flag S:
 * SFX S   y	 ies		[^aeiou]y
 * is not presented here.
 *
 * The affix rule with the flag M:
 * SFX M   0	 's         .
 * is presented here.
 *
 * Conf: current dictionary.
 * word: basic form of word.
 * affixflag: affix flag, by which a basic form of word was generated.
 * flag: compound flag used to compare with StopMiddle->compoundflag.
 *
 * Returns 1 if the word was found in the prefix tree, else returns 0.
 */
unsafe fn FindWord(
    Conf: *mut IspellDict,
    word: *const c_char,
    affixflag: *const c_char,
    mut flag: c_int,
) -> c_int {
    let mut node: *mut SPNode = (*Conf).Dictionary;
    let mut StopLow: *mut SPNodeData;
    let mut StopHigh: *mut SPNodeData;
    let mut StopMiddle: *mut SPNodeData;
    let mut ptr: *const u8 = word as *const u8;

    flag &= FF_COMPOUNDFLAGMASK as c_int;

    while !node.is_null() && *ptr != 0 {
        let data = spnode_data(node);
        StopLow = data;
        StopHigh = data.add((*node).length as usize);
        while StopLow < StopHigh {
            StopMiddle = StopLow.add((StopHigh.offset_from(StopLow) as usize) >> 1);
            if (*StopMiddle).val == *ptr as u32 {
                if *ptr.add(1) == b'\0' && (*StopMiddle).isword != 0 {
                    if flag == 0 {
                        /*
                         * The word can be formed only with another word. And
                         * in the flag parameter there is not a sign that we
                         * search compound words.
                         */
                        if (*StopMiddle).compoundflag & FF_COMPOUNDONLY != 0 {
                            return 0;
                        }
                    } else if (flag as u32 & (*StopMiddle).compoundflag) == 0 {
                        return 0;
                    }

                    /*
                     * Check if this affix rule is presented in the affix set
                     * with index StopMiddle->affix.
                     */
                    if IsAffixFlagInUse(Conf, (*StopMiddle).affix as c_int, affixflag) {
                        return 1;
                    }
                }
                node = (*StopMiddle).node;
                ptr = ptr.add(1);
                break;
            } else if (*StopMiddle).val < *ptr as u32 {
                StopLow = StopMiddle.add(1);
            } else {
                StopHigh = StopMiddle;
            }
        }
        if StopLow >= StopHigh {
            break;
        }
    }
    0
}

/*
 * Adds a new affix rule to the Affix field.
 *
 * Conf: current dictionary.
 * flag: affix flag ('\' in the below example).
 * flagflags: set of flags from the flagval field for this affix rule. This set
 *			  is listed after '/' character in the added string (repl).
 *
 *			  For example L flag in the hunspell_sample.affix:
 *			  SFX \   0 Y/L [^Y]
 *
 * mask: condition for search ('[^Y]' in the above example).
 * find: stripping characters from beginning (at prefix) or end (at suffix)
 *		 of the word ('0' in the above example, 0 means that there is not
 *		 stripping character).
 * repl: adding string after stripping ('Y' in the above example).
 * type: FF_SUFFIX or FF_PREFIX.
 */
unsafe fn NIAddAffix(
    Conf: *mut IspellDict,
    flag: *const c_char,
    flagflags: c_char,
    mask: *const c_char,
    find: *const c_char,
    repl: *const c_char,
    r#type: c_int,
) {
    let Affix: *mut AFFIX;

    if (*Conf).naffixes >= (*Conf).maffixes {
        if (*Conf).maffixes != 0 {
            (*Conf).maffixes *= 2;
            (*Conf).Affix = repalloc(
                (*Conf).Affix as *mut c_void,
                (*Conf).maffixes as usize * core::mem::size_of::<AFFIX>(),
            ) as *mut AFFIX;
        } else {
            (*Conf).maffixes = 16;
            (*Conf).Affix =
                palloc((*Conf).maffixes as usize * core::mem::size_of::<AFFIX>()) as *mut AFFIX;
        }
    }

    Affix = (*Conf).Affix.add((*Conf).naffixes as usize);

    /* This affix rule can be applied for words with any ending */
    if strcmp(mask, c".".as_ptr()) == 0 || *mask == b'\0' as c_char {
        (*Affix).issimple = 1;
        (*Affix).isregis = 0;
    }
    /* This affix rule will use regis to search word ending */
    else if RS_isRegis(mask) {
        (*Affix).issimple = 0;
        (*Affix).isregis = 1;
        RS_compile(
            core::ptr::addr_of_mut!((*Affix).reg.regis) as *mut Regis,
            r#type == FF_SUFFIX,
            if *mask != 0 { mask } else { voidstring() },
        );
    }
    /* This affix rule will use regex_t to search word ending */
    else {
        let masklen: c_int;
        let wmasklen: c_int;
        let err: c_int;
        let wmask: *mut pg_wchar;
        let tmask: *mut c_char;

        (*Affix).issimple = 0;
        (*Affix).isregis = 0;
        tmask = tmpalloc(Conf, strlen(mask) + 3) as *mut c_char;
        if r#type == FF_SUFFIX {
            sprintf(tmask, c"%s$".as_ptr(), mask);
        } else {
            sprintf(tmask, c"^%s".as_ptr(), mask);
        }

        masklen = strlen(tmask) as c_int;
        wmask = tmpalloc(
            Conf,
            (masklen as usize + 1) * core::mem::size_of::<pg_wchar>(),
        ) as *mut pg_wchar;
        wmasklen = pg_mb2wchar_with_len(tmask, wmask, masklen);

        /*
         * The regex and all internal state created by pg_regcomp are
         * allocated in the dictionary's memory context, and will be freed
         * automatically when it is destroyed.
         */
        (*Affix).reg.pregex = palloc(core::mem::size_of::<regex_t>()) as *mut regex_t;
        err = pg_regcomp(
            (*Affix).reg.pregex,
            wmask,
            wmasklen,
            REG_ADVANCED | REG_NOSUB,
            DEFAULT_COLLATION_OID,
        );
        if err != 0 {
            let mut errstr: [c_char; 100] = [0; 100];

            pg_regerror(err, (*Affix).reg.pregex, errstr.as_mut_ptr(), errstr.len());
            /* C also: errcode(ERRCODE_INVALID_REGULAR_EXPRESSION) */
            ereport!(
                ERROR,
                errmsg!(
                    "invalid regular expression: {}",
                    CStr::from_ptr(errstr.as_ptr()).to_string_lossy()
                )
            );
        }
    }

    (*Affix).flagflags = flagflags as u32;
    if ((*Affix).flagflags & FF_COMPOUNDONLY) != 0
        || ((*Affix).flagflags & FF_COMPOUNDPERMITFLAG) != 0
    {
        if ((*Affix).flagflags & FF_COMPOUNDFLAG) == 0 {
            (*Affix).flagflags |= FF_COMPOUNDFLAG;
        }
    }
    (*Affix).flag = cpstrdup(Conf, flag);
    (*Affix).r#type = r#type as u32;

    (*Affix).find = if !find.is_null() && *find != 0 {
        cpstrdup(Conf, find)
    } else {
        voidstring()
    };
    (*Affix).replen = strlen(repl) as u32;
    if (*Affix).replen > 0 {
        (*Affix).repl = cpstrdup(Conf, repl);
    } else {
        (*Affix).repl = voidstring();
    }
    (*Conf).naffixes += 1;
}

/* Parsing states for parse_affentry() and friends */
const PAE_WAIT_MASK: c_int = 0;
const PAE_INMASK: c_int = 1;
const PAE_WAIT_FIND: c_int = 2;
const PAE_INFIND: c_int = 3;
const PAE_WAIT_REPL: c_int = 4;
const PAE_INREPL: c_int = 5;
const PAE_WAIT_TYPE: c_int = 6;
const PAE_WAIT_FLAG: c_int = 7;

/*
 * Parse next space-separated field of an .affix file line.
 *
 * *str is the input pointer (will be advanced past field)
 * next is where to copy the field value to, with null termination
 *
 * The buffer at "next" must be of size BUFSIZ; we truncate the input to fit.
 *
 * Returns true if we found a field, false if not.
 */
unsafe fn get_nextfield(str: *mut *mut c_char, mut next: *mut c_char) -> bool {
    let mut state: c_int = PAE_WAIT_MASK;
    let mut avail: c_int = BUFSIZ as c_int;

    while **str != 0 {
        let clen: c_int = pg_mblen_cstr(*str);

        if state == PAE_WAIT_MASK {
            if t_iseq(*str, b'#' as c_char) {
                return false;
            } else if isspace(**str as c_uchar as c_int) == 0 {
                if clen < avail {
                    ts_copychar_with_len(next, *str, clen as usize);
                    next = next.add(clen as usize);
                    avail -= clen;
                }
                state = PAE_INMASK;
            }
        } else {
            /* state == PAE_INMASK */
            if isspace(**str as c_uchar as c_int) != 0 {
                *next = b'\0' as c_char;
                return true;
            } else if clen < avail {
                ts_copychar_with_len(next, *str, clen as usize);
                next = next.add(clen as usize);
                avail -= clen;
            }
        }
        *str = (*str).add(clen as usize);
    }

    *next = b'\0' as c_char;

    state == PAE_INMASK /* OK if we got a nonempty field */
}

/*
 * Parses entry of an .affix file of MySpell or Hunspell format.
 *
 * An .affix file entry has the following format:
 * - header
 *	 <type>  <flag>  <cross_flag>  <flag_count>
 * - fields after header:
 *	 <type>  <flag>  <find>  <replace>	<mask>
 *
 * str is the input line
 * field values are returned to type etc, which must be buffers of size BUFSIZ.
 *
 * Returns number of fields found; any omitted fields are set to empty strings.
 */
unsafe fn parse_ooaffentry(
    mut str: *mut c_char,
    r#type: *mut c_char,
    flag: *mut c_char,
    find: *mut c_char,
    repl: *mut c_char,
    mask: *mut c_char,
) -> c_int {
    let mut state: c_int = PAE_WAIT_TYPE;
    let mut fields_read: c_int = 0;
    let mut valid: bool = false;

    *r#type = b'\0' as c_char;
    *flag = b'\0' as c_char;
    *find = b'\0' as c_char;
    *repl = b'\0' as c_char;
    *mask = b'\0' as c_char;

    while *str != 0 {
        match state {
            PAE_WAIT_TYPE => {
                valid = get_nextfield(&mut str, r#type);
                state = PAE_WAIT_FLAG;
            }
            PAE_WAIT_FLAG => {
                valid = get_nextfield(&mut str, flag);
                state = PAE_WAIT_FIND;
            }
            PAE_WAIT_FIND => {
                valid = get_nextfield(&mut str, find);
                state = PAE_WAIT_REPL;
            }
            PAE_WAIT_REPL => {
                valid = get_nextfield(&mut str, repl);
                state = PAE_WAIT_MASK;
            }
            PAE_WAIT_MASK => {
                valid = get_nextfield(&mut str, mask);
                state = -1; /* force loop exit */
            }
            _ => {
                elog!(ERROR, "unrecognized state in parse_ooaffentry: {}", state);
            }
        }
        if valid {
            fields_read += 1;
        } else {
            break; /* early EOL */
        }
        if state < 0 {
            break; /* got all fields */
        }
    }

    fields_read
}

/*
 * Parses entry of an .affix file of Ispell format
 *
 * An .affix file entry has the following format:
 * <mask>  >  [-<find>,]<replace>
 */
unsafe fn parse_affentry(
    mut str: *mut c_char,
    mask: *mut c_char,
    find: *mut c_char,
    repl: *mut c_char,
) -> bool {
    let mut state: c_int = PAE_WAIT_MASK;
    let mut pmask: *mut c_char = mask;
    let mut pfind: *mut c_char = find;
    let mut prepl: *mut c_char = repl;

    *mask = b'\0' as c_char;
    *find = b'\0' as c_char;
    *repl = b'\0' as c_char;

    while *str != 0 {
        let clen: c_int = pg_mblen_cstr(str);

        if state == PAE_WAIT_MASK {
            if t_iseq(str, b'#' as c_char) {
                return false;
            } else if isspace(*str as c_uchar as c_int) == 0 {
                pmask = pmask.add(ts_copychar_with_len(pmask, str, clen as usize) as usize);
                state = PAE_INMASK;
            }
        } else if state == PAE_INMASK {
            if t_iseq(str, b'>' as c_char) {
                *pmask = b'\0' as c_char;
                state = PAE_WAIT_FIND;
            } else if isspace(*str as c_uchar as c_int) == 0 {
                pmask = pmask.add(ts_copychar_with_len(pmask, str, clen as usize) as usize);
            }
        } else if state == PAE_WAIT_FIND {
            if t_iseq(str, b'-' as c_char) {
                state = PAE_INFIND;
            } else if t_isalpha_cstr(str) || t_iseq(str, b'\'' as c_char)
            /* english 's */
            {
                prepl = prepl.add(ts_copychar_with_len(prepl, str, clen as usize) as usize);
                state = PAE_INREPL;
            } else if isspace(*str as c_uchar as c_int) == 0 {
                /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
                ereport!(ERROR, errmsg!("syntax error"));
            }
        } else if state == PAE_INFIND {
            if t_iseq(str, b',' as c_char) {
                *pfind = b'\0' as c_char;
                state = PAE_WAIT_REPL;
            } else if t_isalpha_cstr(str) {
                pfind = pfind.add(ts_copychar_with_len(pfind, str, clen as usize) as usize);
            } else if isspace(*str as c_uchar as c_int) == 0 {
                /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
                ereport!(ERROR, errmsg!("syntax error"));
            }
        } else if state == PAE_WAIT_REPL {
            if t_iseq(str, b'-' as c_char) {
                break; /* void repl */
            } else if t_isalpha_cstr(str) {
                prepl = prepl.add(ts_copychar_with_len(prepl, str, clen as usize) as usize);
                state = PAE_INREPL;
            } else if isspace(*str as c_uchar as c_int) == 0 {
                /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
                ereport!(ERROR, errmsg!("syntax error"));
            }
        } else if state == PAE_INREPL {
            if t_iseq(str, b'#' as c_char) {
                *prepl = b'\0' as c_char;
                break;
            } else if t_isalpha_cstr(str) {
                prepl = prepl.add(ts_copychar_with_len(prepl, str, clen as usize) as usize);
            } else if isspace(*str as c_uchar as c_int) == 0 {
                /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
                ereport!(ERROR, errmsg!("syntax error"));
            }
        } else {
            elog!(ERROR, "unrecognized state in parse_affentry: {}", state);
        }

        str = str.add(clen as usize);
    }

    *pmask = b'\0' as c_char;
    *pfind = b'\0' as c_char;
    *prepl = b'\0' as c_char;

    *mask != 0 && (*find != 0 || *repl != 0)
}

/*
 * Sets a Hunspell options depending on flag type.
 */
unsafe fn setCompoundAffixFlagValue(
    Conf: *mut IspellDict,
    entry: *mut CompoundAffixFlag,
    s: *mut c_char,
    val: u32,
) {
    if (*Conf).flagMode == FM_NUM {
        let mut next: *mut c_char = null_mut();
        let i: c_int;

        set_errno(0);
        i = strtol(s, &mut next, 10) as c_int;
        if s == next || get_errno() == ERANGE {
            /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
            ereport!(
                ERROR,
                errmsg!("invalid affix flag \"{}\"", CStr::from_ptr(s).to_string_lossy())
            );
        }
        if i < 0 || i > FLAGNUM_MAXSIZE {
            /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
            ereport!(
                ERROR,
                errmsg!(
                    "affix flag \"{}\" is out of range",
                    CStr::from_ptr(s).to_string_lossy()
                )
            );
        }

        (*entry).flag.i = i as u32;
    } else {
        (*entry).flag.s = cpstrdup(Conf, s);
    }

    (*entry).flagMode = (*Conf).flagMode;
    (*entry).value = val;
}

/*
 * Sets up a correspondence for the affix parameter with the affix flag.
 *
 * Conf: current dictionary.
 * s: affix flag in string.
 * val: affix parameter.
 */
unsafe fn addCompoundAffixFlagValue(Conf: *mut IspellDict, mut s: *mut c_char, val: u32) {
    let newValue: *mut CompoundAffixFlag;
    let mut sbuf: [c_char; BUFSIZ] = [0; BUFSIZ];
    let mut sflag: *mut c_char;

    while *s != 0 && isspace(*s as c_uchar as c_int) != 0 {
        s = s.add(pg_mblen_cstr(s) as usize);
    }

    if *s == 0 {
        /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
        ereport!(ERROR, errmsg!("syntax error"));
    }

    /* Get flag without \n */
    sflag = sbuf.as_mut_ptr();
    while *s != 0 && isspace(*s as c_uchar as c_int) == 0 && *s != b'\n' as c_char {
        let clen: c_int = ts_copychar_cstr(sflag, s);

        sflag = sflag.add(clen as usize);
        s = s.add(clen as usize);
    }
    *sflag = b'\0' as c_char;

    /* Resize array or allocate memory for array CompoundAffixFlag */
    if (*Conf).nCompoundAffixFlag >= (*Conf).mCompoundAffixFlag {
        if (*Conf).mCompoundAffixFlag != 0 {
            (*Conf).mCompoundAffixFlag *= 2;
            (*Conf).CompoundAffixFlags = repalloc(
                (*Conf).CompoundAffixFlags as *mut c_void,
                (*Conf).mCompoundAffixFlag as usize * core::mem::size_of::<CompoundAffixFlag>(),
            ) as *mut CompoundAffixFlag;
        } else {
            (*Conf).mCompoundAffixFlag = 10;
            (*Conf).CompoundAffixFlags = tmpalloc(
                Conf,
                (*Conf).mCompoundAffixFlag as usize * core::mem::size_of::<CompoundAffixFlag>(),
            ) as *mut CompoundAffixFlag;
        }
    }

    newValue = (*Conf)
        .CompoundAffixFlags
        .add((*Conf).nCompoundAffixFlag as usize);

    setCompoundAffixFlagValue(Conf, newValue, sbuf.as_mut_ptr(), val);

    (*Conf).usecompound = true;
    (*Conf).nCompoundAffixFlag += 1;
}

/*
 * Returns a set of affix parameters which correspondence to the set of affix
 * flags s.
 */
unsafe fn getCompoundAffixFlagValue(Conf: *mut IspellDict, s: *const c_char) -> c_int {
    let mut flag: u32 = 0;
    let found: *mut CompoundAffixFlag;
    let mut key: CompoundAffixFlag = core::mem::zeroed();
    let mut sflag: [c_char; BUFSIZ] = [0; BUFSIZ];
    let mut flagcur: *const c_char;

    if (*Conf).nCompoundAffixFlag == 0 {
        return 0;
    }

    flagcur = s;
    while *flagcur != 0 {
        getNextFlagFromString(Conf, &mut flagcur, sflag.as_mut_ptr());
        setCompoundAffixFlagValue(Conf, &mut key, sflag.as_mut_ptr(), 0);

        found = bsearch(
            &key as *const CompoundAffixFlag as *const c_void,
            (*Conf).CompoundAffixFlags as *const c_void,
            (*Conf).nCompoundAffixFlag as usize,
            core::mem::size_of::<CompoundAffixFlag>(),
            cmpcmdflag,
        ) as *mut CompoundAffixFlag;
        if !found.is_null() {
            flag |= (*found).value;
        }
    }

    flag as c_int
}

/*
 * Returns a flag set using the s parameter.
 *
 * If Conf->useFlagAliases is true then the s parameter is index of the
 * Conf->AffixData array and function returns its entry.
 * Else function returns the s parameter.
 */
unsafe fn getAffixFlagSet(Conf: *mut IspellDict, s: *mut c_char) -> *const c_char {
    if (*Conf).useFlagAliases && *s != b'\0' as c_char {
        let curaffix: c_int;
        let mut end: *mut c_char = null_mut();

        set_errno(0);
        curaffix = strtol(s, &mut end, 10) as c_int;
        if s == end || get_errno() == ERANGE {
            /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
            ereport!(
                ERROR,
                errmsg!("invalid affix alias \"{}\"", CStr::from_ptr(s).to_string_lossy())
            );
        }

        if curaffix > 0 && curaffix < (*Conf).nAffixData {
            /*
             * Do not subtract 1 from curaffix because empty string was added
             * in NIImportOOAffixes
             */
            *(*Conf).AffixData.add(curaffix as usize)
        } else if curaffix > (*Conf).nAffixData {
            /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
            ereport!(
                ERROR,
                errmsg!("invalid affix alias \"{}\"", CStr::from_ptr(s).to_string_lossy())
            );
            voidstring()
        } else {
            voidstring()
        }
    } else {
        s
    }
}

/*
 * Import an affix file that follows MySpell or Hunspell format.
 *
 * Conf: current dictionary.
 * filename: path to the .affix file.
 */
unsafe fn NIImportOOAffixes(Conf: *mut IspellDict, filename: *const c_char) {
    let mut r#type: [c_char; BUFSIZ] = [0; BUFSIZ];
    let mut ptype: *mut c_char = null_mut();
    let mut sflag: [c_char; BUFSIZ] = [0; BUFSIZ];
    let mut mask: [c_char; BUFSIZ] = [0; BUFSIZ];
    let mut pmask: *mut c_char;
    let mut find: [c_char; BUFSIZ] = [0; BUFSIZ];
    let mut pfind: *mut c_char;
    let mut repl: [c_char; BUFSIZ] = [0; BUFSIZ];
    let mut prepl: *mut c_char;
    let mut isSuffix: bool = false;
    let mut naffix: c_int = 0;
    let mut curaffix: c_int = 0;
    let mut sflaglen: c_int;
    let mut flagflags: c_char = 0;
    let mut trst: tsearch_readline_state = core::mem::zeroed();
    let mut recoded: *mut c_char;

    /* read file to find any flag */
    (*Conf).usecompound = false;
    (*Conf).useFlagAliases = false;
    (*Conf).flagMode = FM_CHAR;

    if !tsearch_readline_begin(&mut trst, filename) {
        /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR); C appends ": %m" errno
         * text, not ported here. */
        ereport!(
            ERROR,
            errmsg!(
                "could not open affix file \"{}\"",
                CStr::from_ptr(filename).to_string_lossy()
            )
        );
    }

    recoded = tsearch_readline(&mut trst);
    while !recoded.is_null() {
        if *recoded == b'\0' as c_char
            || isspace(*recoded as c_uchar as c_int) != 0
            || t_iseq(recoded, b'#' as c_char)
        {
            pfree(recoded as *mut c_void);
            recoded = tsearch_readline(&mut trst);
            continue;
        }

        if STRNCMP(recoded, c"COMPOUNDFLAG".as_ptr()) == 0 {
            addCompoundAffixFlagValue(
                Conf,
                recoded.add(strlen(c"COMPOUNDFLAG".as_ptr())),
                FF_COMPOUNDFLAG,
            );
        } else if STRNCMP(recoded, c"COMPOUNDBEGIN".as_ptr()) == 0 {
            addCompoundAffixFlagValue(
                Conf,
                recoded.add(strlen(c"COMPOUNDBEGIN".as_ptr())),
                FF_COMPOUNDBEGIN,
            );
        } else if STRNCMP(recoded, c"COMPOUNDLAST".as_ptr()) == 0 {
            addCompoundAffixFlagValue(
                Conf,
                recoded.add(strlen(c"COMPOUNDLAST".as_ptr())),
                FF_COMPOUNDLAST,
            );
        }
        /* COMPOUNDLAST and COMPOUNDEND are synonyms */
        else if STRNCMP(recoded, c"COMPOUNDEND".as_ptr()) == 0 {
            addCompoundAffixFlagValue(
                Conf,
                recoded.add(strlen(c"COMPOUNDEND".as_ptr())),
                FF_COMPOUNDLAST,
            );
        } else if STRNCMP(recoded, c"COMPOUNDMIDDLE".as_ptr()) == 0 {
            addCompoundAffixFlagValue(
                Conf,
                recoded.add(strlen(c"COMPOUNDMIDDLE".as_ptr())),
                FF_COMPOUNDMIDDLE,
            );
        } else if STRNCMP(recoded, c"ONLYINCOMPOUND".as_ptr()) == 0 {
            addCompoundAffixFlagValue(
                Conf,
                recoded.add(strlen(c"ONLYINCOMPOUND".as_ptr())),
                FF_COMPOUNDONLY,
            );
        } else if STRNCMP(recoded, c"COMPOUNDPERMITFLAG".as_ptr()) == 0 {
            addCompoundAffixFlagValue(
                Conf,
                recoded.add(strlen(c"COMPOUNDPERMITFLAG".as_ptr())),
                FF_COMPOUNDPERMITFLAG,
            );
        } else if STRNCMP(recoded, c"COMPOUNDFORBIDFLAG".as_ptr()) == 0 {
            addCompoundAffixFlagValue(
                Conf,
                recoded.add(strlen(c"COMPOUNDFORBIDFLAG".as_ptr())),
                FF_COMPOUNDFORBIDFLAG,
            );
        } else if STRNCMP(recoded, c"FLAG".as_ptr()) == 0 {
            let mut s: *mut c_char = recoded.add(strlen(c"FLAG".as_ptr()));

            while *s != 0 && isspace(*s as c_uchar as c_int) != 0 {
                s = s.add(pg_mblen_cstr(s) as usize);
            }

            if *s != 0 {
                if STRNCMP(s, c"long".as_ptr()) == 0 {
                    (*Conf).flagMode = FM_LONG;
                } else if STRNCMP(s, c"num".as_ptr()) == 0 {
                    (*Conf).flagMode = FM_NUM;
                } else if STRNCMP(s, c"default".as_ptr()) != 0 {
                    /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
                    ereport!(
                        ERROR,
                        errmsg!(
                            "Ispell dictionary supports only \"default\", \"long\", and \"num\" flag values"
                        )
                    );
                }
            }
        }

        pfree(recoded as *mut c_void);
        recoded = tsearch_readline(&mut trst);
    }
    tsearch_readline_end(&mut trst);

    if (*Conf).nCompoundAffixFlag > 1 {
        qsort(
            (*Conf).CompoundAffixFlags as *mut c_void,
            (*Conf).nCompoundAffixFlag as usize,
            core::mem::size_of::<CompoundAffixFlag>(),
            cmpcmdflag,
        );
    }

    if !tsearch_readline_begin(&mut trst, filename) {
        /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR); C appends ": %m" errno
         * text, not ported here. */
        ereport!(
            ERROR,
            errmsg!(
                "could not open affix file \"{}\"",
                CStr::from_ptr(filename).to_string_lossy()
            )
        );
    }

    recoded = tsearch_readline(&mut trst);
    while !recoded.is_null() {
        'nextline: {
            let fields_read: c_int;

            if *recoded == b'\0' as c_char
                || isspace(*recoded as c_uchar as c_int) != 0
                || t_iseq(recoded, b'#' as c_char)
            {
                break 'nextline;
            }

            fields_read = parse_ooaffentry(
                recoded,
                r#type.as_mut_ptr(),
                sflag.as_mut_ptr(),
                find.as_mut_ptr(),
                repl.as_mut_ptr(),
                mask.as_mut_ptr(),
            );

            if !ptype.is_null() {
                pfree(ptype as *mut c_void);
            }
            ptype = lowerstr_ctx(Conf, r#type.as_ptr());

            /* First try to parse AF parameter (alias compression) */
            if STRNCMP(ptype, c"af".as_ptr()) == 0 {
                /* First line is the number of aliases */
                if !(*Conf).useFlagAliases {
                    (*Conf).useFlagAliases = true;
                    naffix = atoi(sflag.as_ptr());
                    if naffix <= 0 {
                        /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
                        ereport!(
                            ERROR,
                            errmsg!("invalid number of flag vector aliases")
                        );
                    }

                    /* Also reserve place for empty flag set */
                    naffix += 1;

                    (*Conf).AffixData = palloc0(
                        naffix as usize * core::mem::size_of::<*const c_char>(),
                    ) as *mut *const c_char;
                    (*Conf).nAffixData = naffix;
                    (*Conf).lenAffixData = naffix;

                    /* Add empty flag set into AffixData */
                    *(*Conf).AffixData.add(curaffix as usize) = voidstring();
                    curaffix += 1;
                }
                /* Other lines are aliases */
                else if curaffix < naffix {
                    *(*Conf).AffixData.add(curaffix as usize) = cpstrdup(Conf, sflag.as_ptr());
                    curaffix += 1;
                } else {
                    /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
                    ereport!(
                        ERROR,
                        errmsg!("number of aliases exceeds specified number {}", naffix - 1)
                    );
                }
                break 'nextline;
            }
            /* Else try to parse prefixes and suffixes */
            if fields_read < 4
                || (STRNCMP(ptype, c"sfx".as_ptr()) != 0 && STRNCMP(ptype, c"pfx".as_ptr()) != 0)
            {
                break 'nextline;
            }

            sflaglen = strlen(sflag.as_ptr()) as c_int;
            if sflaglen == 0
                || (sflaglen > 1 && (*Conf).flagMode == FM_CHAR)
                || (sflaglen > 2 && (*Conf).flagMode == FM_LONG)
            {
                break 'nextline;
            }

            /*--------
             * Affix header. For example:
             * SFX \ N 1
             *--------
             */
            if fields_read == 4 {
                isSuffix = STRNCMP(ptype, c"sfx".as_ptr()) == 0;
                if t_iseq(find.as_ptr(), b'y' as c_char) || t_iseq(find.as_ptr(), b'Y' as c_char) {
                    flagflags = FF_CROSSPRODUCT as c_char;
                } else {
                    flagflags = 0;
                }
            }
            /*--------
             * Affix fields. For example:
             * SFX \   0	Y/L [^Y]
             *--------
             */
            else {
                let mut ptr: *mut c_char;
                let mut aflg: c_int = 0;

                /* Get flags after '/' (flags are case sensitive) */
                ptr = strchr(repl.as_ptr(), b'/' as c_int);
                if !ptr.is_null() {
                    aflg |= getCompoundAffixFlagValue(Conf, getAffixFlagSet(Conf, ptr.add(1)));
                }
                /* Get lowercased version of string before '/' */
                prepl = lowerstr_ctx(Conf, repl.as_ptr());
                ptr = strchr(prepl, b'/' as c_int);
                if !ptr.is_null() {
                    *ptr = b'\0' as c_char;
                }
                pfind = lowerstr_ctx(Conf, find.as_ptr());
                pmask = lowerstr_ctx(Conf, mask.as_ptr());
                if t_iseq(find.as_ptr(), b'0' as c_char) {
                    *pfind = b'\0' as c_char;
                }
                if t_iseq(repl.as_ptr(), b'0' as c_char) {
                    *prepl = b'\0' as c_char;
                }

                NIAddAffix(
                    Conf,
                    sflag.as_ptr(),
                    flagflags | aflg as c_char,
                    pmask,
                    pfind,
                    prepl,
                    if isSuffix { FF_SUFFIX } else { FF_PREFIX },
                );
                pfree(prepl as *mut c_void);
                pfree(pfind as *mut c_void);
                pfree(pmask as *mut c_void);
            }
        }

        /* nextline: */
        pfree(recoded as *mut c_void);
        recoded = tsearch_readline(&mut trst);
    }

    tsearch_readline_end(&mut trst);
    if !ptype.is_null() {
        pfree(ptype as *mut c_void);
    }
}

/*
 * import affixes
 *
 * Note caller must already have applied get_tsearch_config_filename
 *
 * This function is responsible for parsing ispell ("old format") affix files.
 * If we realize that the file contains new-format commands, we pass off the
 * work to NIImportOOAffixes(), which will re-read the whole file.
 */
#[unsafe(no_mangle)]
pub unsafe fn NIImportAffixes(Conf: *mut IspellDict, filename: *const c_char) {
    let mut pstr: *mut c_char = null_mut();
    let mut flag: [c_char; BUFSIZ] = [0; BUFSIZ];
    let mut mask: [c_char; BUFSIZ] = [0; BUFSIZ];
    let mut find: [c_char; BUFSIZ] = [0; BUFSIZ];
    let mut repl: [c_char; BUFSIZ] = [0; BUFSIZ];
    let mut s: *mut c_char;
    let mut suffixes: bool = false;
    let mut prefixes: bool = false;
    let mut flagflags: c_char = 0;
    let mut trst: tsearch_readline_state = core::mem::zeroed();
    let mut oldformat: bool = false;
    let mut recoded: *mut c_char;

    if !tsearch_readline_begin(&mut trst, filename) {
        /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR); C appends ": %m" errno
         * text, not ported here. */
        ereport!(
            ERROR,
            errmsg!(
                "could not open affix file \"{}\"",
                CStr::from_ptr(filename).to_string_lossy()
            )
        );
    }

    (*Conf).usecompound = false;
    (*Conf).useFlagAliases = false;
    (*Conf).flagMode = FM_CHAR;

    /*
     * The C function uses `goto isnewformat` to bail out of the parse loop and
     * hand off to NIImportOOAffixes after the loop.  We emulate that with a
     * flag that breaks the loop; the normal completion path returns early.
     */
    let mut isnewformat: bool = false;

    recoded = tsearch_readline(&mut trst);
    'mainloop: while !recoded.is_null() {
        pstr = str_tolower(recoded, strlen(recoded), DEFAULT_COLLATION_OID);

        'nextline: {
            /* Skip comments and empty lines */
            if *pstr == b'#' as c_char || *pstr == b'\n' as c_char {
                break 'nextline;
            }

            if STRNCMP(pstr, c"compoundwords".as_ptr()) == 0 {
                /* Find case-insensitive L flag in non-lowercased string */
                s = findchar2(recoded, b'l' as c_int, b'L' as c_int);
                if !s.is_null() {
                    while *s != 0 && isspace(*s as c_uchar as c_int) == 0 {
                        s = s.add(pg_mblen_cstr(s) as usize);
                    }
                    while *s != 0 && isspace(*s as c_uchar as c_int) != 0 {
                        s = s.add(pg_mblen_cstr(s) as usize);
                    }

                    if *s != 0 && pg_mblen_cstr(s) == 1 {
                        addCompoundAffixFlagValue(Conf, s, FF_COMPOUNDFLAG);
                        (*Conf).usecompound = true;
                    }
                    oldformat = true;
                    break 'nextline;
                }
            }
            if STRNCMP(pstr, c"suffixes".as_ptr()) == 0 {
                suffixes = true;
                prefixes = false;
                oldformat = true;
                break 'nextline;
            }
            if STRNCMP(pstr, c"prefixes".as_ptr()) == 0 {
                suffixes = false;
                prefixes = true;
                oldformat = true;
                break 'nextline;
            }
            if STRNCMP(pstr, c"flag".as_ptr()) == 0 {
                s = recoded.add(4); /* we need non-lowercased string */
                flagflags = 0;

                while *s != 0 && isspace(*s as c_uchar as c_int) != 0 {
                    s = s.add(pg_mblen_cstr(s) as usize);
                }

                if *s == b'*' as c_char {
                    flagflags |= FF_CROSSPRODUCT as c_char;
                    s = s.add(1);
                } else if *s == b'~' as c_char {
                    flagflags |= FF_COMPOUNDONLY as c_char;
                    s = s.add(1);
                }

                if *s == b'\\' as c_char {
                    s = s.add(1);
                }

                /*
                 * An old-format flag is a single ASCII character; we expect it to
                 * be followed by EOL, whitespace, or ':'.  Otherwise this is a
                 * new-format flag command.
                 */
                if *s != 0 && pg_mblen_cstr(s) == 1 {
                    flag[0] = *s;
                    s = s.add(1);
                    flag[1] = b'\0' as c_char;

                    if *s == b'\0' as c_char
                        || *s == b'#' as c_char
                        || *s == b'\n' as c_char
                        || *s == b':' as c_char
                        || isspace(*s as c_uchar as c_int) != 0
                    {
                        oldformat = true;
                        break 'nextline;
                    }
                }
                isnewformat = true;
                break 'mainloop; /* goto isnewformat */
            }
            if STRNCMP(recoded, c"COMPOUNDFLAG".as_ptr()) == 0
                || STRNCMP(recoded, c"COMPOUNDMIN".as_ptr()) == 0
                || STRNCMP(recoded, c"PFX".as_ptr()) == 0
                || STRNCMP(recoded, c"SFX".as_ptr()) == 0
            {
                isnewformat = true;
                break 'mainloop; /* goto isnewformat */
            }

            if !suffixes && !prefixes {
                break 'nextline;
            }

            if !parse_affentry(pstr, mask.as_mut_ptr(), find.as_mut_ptr(), repl.as_mut_ptr()) {
                break 'nextline;
            }

            NIAddAffix(
                Conf,
                flag.as_ptr(),
                flagflags,
                mask.as_ptr(),
                find.as_ptr(),
                repl.as_ptr(),
                if suffixes { FF_SUFFIX } else { FF_PREFIX },
            );
        }

        /* nextline: */
        pfree(recoded as *mut c_void);
        pfree(pstr as *mut c_void);

        recoded = tsearch_readline(&mut trst);
    }

    if !isnewformat {
        tsearch_readline_end(&mut trst);
        return;
    }

    /* isnewformat: */
    if oldformat {
        /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
        ereport!(
            ERROR,
            errmsg!("affix file contains both old-style and new-style commands")
        );
    }
    tsearch_readline_end(&mut trst);

    NIImportOOAffixes(Conf, filename);
}

/*
 * Merges two affix flag sets and stores a new affix flag set into
 * Conf->AffixData.
 *
 * Returns index of a new affix flag set.
 */
unsafe fn MergeAffix(Conf: *mut IspellDict, a1: c_int, a2: c_int) -> c_int {
    let mut ptr: *mut *const c_char;

    Assert!(a1 < (*Conf).nAffixData && a2 < (*Conf).nAffixData);

    /* Do not merge affix flags if one of affix flags is empty */
    if **(*Conf).AffixData.add(a1 as usize) == b'\0' as c_char {
        return a2;
    } else if **(*Conf).AffixData.add(a2 as usize) == b'\0' as c_char {
        return a1;
    }

    /* Double the size of AffixData if there's not enough space */
    if (*Conf).nAffixData + 1 >= (*Conf).lenAffixData {
        (*Conf).lenAffixData *= 2;
        (*Conf).AffixData = repalloc(
            (*Conf).AffixData as *mut c_void,
            core::mem::size_of::<*const c_char>() * (*Conf).lenAffixData as usize,
        ) as *mut *const c_char;
    }

    ptr = (*Conf).AffixData.add((*Conf).nAffixData as usize);
    if (*Conf).flagMode == FM_NUM {
        let p = cpalloc(
            Conf,
            strlen(*(*Conf).AffixData.add(a1 as usize))
                + strlen(*(*Conf).AffixData.add(a2 as usize))
                + 1 /* comma */ + 1, /* \0 */
        ) as *mut c_char;

        sprintf(
            p,
            c"%s,%s".as_ptr(),
            *(*Conf).AffixData.add(a1 as usize),
            *(*Conf).AffixData.add(a2 as usize),
        );
        *ptr = p;
    } else {
        let p = cpalloc(
            Conf,
            strlen(*(*Conf).AffixData.add(a1 as usize))
                + strlen(*(*Conf).AffixData.add(a2 as usize))
                + 1, /* \0 */
        ) as *mut c_char;

        sprintf(
            p,
            c"%s%s".as_ptr(),
            *(*Conf).AffixData.add(a1 as usize),
            *(*Conf).AffixData.add(a2 as usize),
        );
        *ptr = p;
    }
    ptr = ptr.add(1);
    *ptr = core::ptr::null();
    (*Conf).nAffixData += 1;

    (*Conf).nAffixData - 1
}

/*
 * Returns a set of affix parameters which correspondence to the set of affix
 * flags with the given index.
 */
unsafe fn makeCompoundFlags(Conf: *mut IspellDict, affix: c_int) -> u32 {
    Assert!(affix < (*Conf).nAffixData);

    getCompoundAffixFlagValue(Conf, *(*Conf).AffixData.add(affix as usize)) as u32
        & FF_COMPOUNDFLAGMASK
}

/*
 * Makes a prefix tree for the given level.
 *
 * Conf: current dictionary.
 * low: lower index of the Conf->Spell array.
 * high: upper index of the Conf->Spell array.
 * level: current prefix tree level.
 */
unsafe fn mkSPNode(Conf: *mut IspellDict, low: c_int, high: c_int, level: c_int) -> *mut SPNode {
    let mut i: c_int;
    let mut nchar: c_int = 0;
    let mut lastchar: c_char = b'\0' as c_char;
    let rs: *mut SPNode;
    let mut data: *mut SPNodeData;
    let mut lownew: c_int = low;

    i = low;
    while i < high {
        let sp = *(*Conf).Spell.add(i as usize);
        if (*sp).p.d.len > level && lastchar != *spell_word(sp).add(level as usize) {
            nchar += 1;
            lastchar = *spell_word(sp).add(level as usize);
        }
        i += 1;
    }

    if nchar == 0 {
        return null_mut();
    }

    rs = cpalloc0(
        Conf,
        SPNHDRSZ() + nchar as usize * core::mem::size_of::<SPNodeData>(),
    ) as *mut SPNode;
    (*rs).length = nchar as u32;
    data = spnode_data(rs);

    lastchar = b'\0' as c_char;
    i = low;
    while i < high {
        let sp = *(*Conf).Spell.add(i as usize);
        if (*sp).p.d.len > level {
            if lastchar != *spell_word(sp).add(level as usize) {
                if lastchar != 0 {
                    /* Next level of the prefix tree */
                    (*data).node = mkSPNode(Conf, lownew, i, level + 1);
                    lownew = i;
                    data = data.add(1);
                }
                lastchar = *spell_word(sp).add(level as usize);
            }
            (*data).val = *(spell_word(sp) as *mut u8).add(level as usize) as u32;
            if (*sp).p.d.len == level + 1 {
                let mut clearCompoundOnly: bool = false;

                if (*data).isword != 0 && (*data).affix != (*sp).p.d.affix as u32 {
                    /*
                     * MergeAffix called a few times. If one of word is
                     * allowed to be in compound word and another isn't, then
                     * clear FF_COMPOUNDONLY flag.
                     */

                    clearCompoundOnly = !((FF_COMPOUNDONLY
                        & (*data).compoundflag
                        & makeCompoundFlags(Conf, (*sp).p.d.affix))
                        != 0);
                    (*data).affix =
                        MergeAffix(Conf, (*data).affix as c_int, (*sp).p.d.affix) as u32;
                } else {
                    (*data).affix = (*sp).p.d.affix as u32;
                }
                (*data).isword = 1;

                (*data).compoundflag = makeCompoundFlags(Conf, (*data).affix as c_int);

                if ((*data).compoundflag & FF_COMPOUNDONLY) != 0
                    && ((*data).compoundflag & FF_COMPOUNDFLAG) == 0
                {
                    (*data).compoundflag |= FF_COMPOUNDFLAG;
                }

                if clearCompoundOnly {
                    (*data).compoundflag &= !FF_COMPOUNDONLY;
                }
            }
        }
        i += 1;
    }

    /* Next level of the prefix tree */
    (*data).node = mkSPNode(Conf, lownew, high, level + 1);

    rs
}

/*
 * Builds the Conf->Dictionary tree and AffixData from the imported dictionary
 * and affixes.
 */
#[unsafe(no_mangle)]
pub unsafe fn NISortDictionary(Conf: *mut IspellDict) {
    let mut i: c_int;
    let naffix: c_int;
    let mut curaffix: c_int;

    /* compress affixes */

    /*
     * If we use flag aliases then we need to use Conf->AffixData filled in
     * the NIImportOOAffixes().
     */
    if (*Conf).useFlagAliases {
        i = 0;
        while i < (*Conf).nspell {
            let sp = *(*Conf).Spell.add(i as usize);
            let mut end: *mut c_char = null_mut();

            if *(*sp).p.flag != b'\0' as c_char {
                set_errno(0);
                curaffix = strtol((*sp).p.flag, &mut end, 10) as c_int;
                if (*sp).p.flag == end as *const c_char || get_errno() == ERANGE {
                    /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
                    ereport!(
                        ERROR,
                        errmsg!(
                            "invalid affix alias \"{}\"",
                            CStr::from_ptr((*sp).p.flag).to_string_lossy()
                        )
                    );
                }
                if curaffix < 0 || curaffix >= (*Conf).nAffixData {
                    /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
                    ereport!(
                        ERROR,
                        errmsg!(
                            "invalid affix alias \"{}\"",
                            CStr::from_ptr((*sp).p.flag).to_string_lossy()
                        )
                    );
                }
                if *end != b'\0' as c_char
                    && isdigit(*end as c_uchar as c_int) == 0
                    && isspace(*end as c_uchar as c_int) == 0
                {
                    /* C also: errcode(ERRCODE_CONFIG_FILE_ERROR) */
                    ereport!(
                        ERROR,
                        errmsg!(
                            "invalid affix alias \"{}\"",
                            CStr::from_ptr((*sp).p.flag).to_string_lossy()
                        )
                    );
                }
            } else {
                /*
                 * If Conf->Spell[i]->p.flag is empty, then get empty value of
                 * Conf->AffixData (0 index).
                 */
                curaffix = 0;
            }

            (*sp).p.d.affix = curaffix;
            (*sp).p.d.len = strlen(spell_word(sp)) as c_int;
            i += 1;
        }
    }
    /* Otherwise fill Conf->AffixData here */
    else {
        /* Count the number of different flags used in the dictionary */
        qsort(
            (*Conf).Spell as *mut c_void,
            (*Conf).nspell as usize,
            core::mem::size_of::<*mut SPELL>(),
            cmpspellaffix,
        );

        let mut naffix_cnt: c_int = 0;
        i = 0;
        while i < (*Conf).nspell {
            if i == 0
                || strcmp(
                    (**(*Conf).Spell.add(i as usize)).p.flag,
                    (**(*Conf).Spell.add((i - 1) as usize)).p.flag,
                ) != 0
            {
                naffix_cnt += 1;
            }
            i += 1;
        }
        naffix = naffix_cnt;

        /*
         * Fill in Conf->AffixData with the affixes that were used in the
         * dictionary. Replace textual flag-field of Conf->Spell entries with
         * indexes into Conf->AffixData array.
         */
        (*Conf).AffixData =
            palloc0(naffix as usize * core::mem::size_of::<*const c_char>()) as *mut *const c_char;

        curaffix = -1;
        i = 0;
        while i < (*Conf).nspell {
            let sp = *(*Conf).Spell.add(i as usize);
            if i == 0
                || strcmp((*sp).p.flag, *(*Conf).AffixData.add(curaffix as usize)) != 0
            {
                curaffix += 1;
                Assert!(curaffix < naffix);
                *(*Conf).AffixData.add(curaffix as usize) = cpstrdup(Conf, (*sp).p.flag);
            }

            (*sp).p.d.affix = curaffix;
            (*sp).p.d.len = strlen(spell_word(sp)) as c_int;
            i += 1;
        }

        (*Conf).nAffixData = naffix;
        (*Conf).lenAffixData = naffix;
    }

    /* Start build a prefix tree */
    qsort(
        (*Conf).Spell as *mut c_void,
        (*Conf).nspell as usize,
        core::mem::size_of::<*mut SPELL>(),
        cmpspell,
    );
    (*Conf).Dictionary = mkSPNode(Conf, 0, (*Conf).nspell, 0);
}

/*
 * Makes a prefix tree for the given level using the repl string of an affix
 * rule. Affixes with empty replace string do not include in the prefix tree.
 * This affixes are included by mkVoidAffix().
 *
 * Conf: current dictionary.
 * low: lower index of the Conf->Affix array.
 * high: upper index of the Conf->Affix array.
 * level: current prefix tree level.
 * type: FF_SUFFIX or FF_PREFIX.
 */
unsafe fn mkANode(
    Conf: *mut IspellDict,
    low: c_int,
    high: c_int,
    level: c_int,
    r#type: c_int,
) -> *mut AffixNode {
    let mut i: c_int;
    let mut nchar: c_int = 0;
    let mut lastchar: u8 = b'\0';
    let rs: *mut AffixNode;
    let mut data: *mut AffixNodeData;
    let mut lownew: c_int = low;
    let mut naff: c_int;
    let aff: *mut *mut AFFIX;

    i = low;
    while i < high {
        if (*(*Conf).Affix.add(i as usize)).replen > level as u32
            && lastchar != GETCHAR((*Conf).Affix.add(i as usize), level, r#type)
        {
            nchar += 1;
            lastchar = GETCHAR((*Conf).Affix.add(i as usize), level, r#type);
        }
        i += 1;
    }

    if nchar == 0 {
        return null_mut();
    }

    aff = tmpalloc(
        Conf,
        core::mem::size_of::<*mut AFFIX>() * (high - low + 1) as usize,
    ) as *mut *mut AFFIX;
    naff = 0;

    rs = cpalloc0(
        Conf,
        ANHRDSZ() + nchar as usize * core::mem::size_of::<AffixNodeData>(),
    ) as *mut AffixNode;
    (*rs).length = nchar as u32;
    data = anode_data(rs);

    lastchar = b'\0';
    i = low;
    while i < high {
        if (*(*Conf).Affix.add(i as usize)).replen > level as u32 {
            if lastchar != GETCHAR((*Conf).Affix.add(i as usize), level, r#type) {
                if lastchar != 0 {
                    /* Next level of the prefix tree */
                    (*data).node = mkANode(Conf, lownew, i, level + 1, r#type);
                    if naff != 0 {
                        (*data).naff = naff as u32;
                        (*data).aff = cpalloc(
                            Conf,
                            core::mem::size_of::<*mut AFFIX>() * naff as usize,
                        ) as *mut *mut AFFIX;
                        memcpy(
                            (*data).aff as *mut c_void,
                            aff as *const c_void,
                            core::mem::size_of::<*mut AFFIX>() * naff as usize,
                        );
                        naff = 0;
                    }
                    data = data.add(1);
                    lownew = i;
                }
                lastchar = GETCHAR((*Conf).Affix.add(i as usize), level, r#type);
            }
            (*data).val = GETCHAR((*Conf).Affix.add(i as usize), level, r#type) as u32;
            if (*(*Conf).Affix.add(i as usize)).replen == (level + 1) as u32 {
                /* affix stopped */
                *aff.add(naff as usize) = (*Conf).Affix.add(i as usize);
                naff += 1;
            }
        }
        i += 1;
    }

    /* Next level of the prefix tree */
    (*data).node = mkANode(Conf, lownew, high, level + 1, r#type);
    if naff != 0 {
        (*data).naff = naff as u32;
        (*data).aff =
            cpalloc(Conf, core::mem::size_of::<*mut AFFIX>() * naff as usize) as *mut *mut AFFIX;
        memcpy(
            (*data).aff as *mut c_void,
            aff as *const c_void,
            core::mem::size_of::<*mut AFFIX>() * naff as usize,
        );
        naff = 0;
    }
    let _ = naff;

    pfree(aff as *mut c_void);

    rs
}

/*
 * Makes the root void node in the prefix tree. The root void node is created
 * for affixes which have empty replace string ("repl" field).
 */
unsafe fn mkVoidAffix(Conf: *mut IspellDict, issuffix: bool, startsuffix: c_int) {
    let mut i: c_int;
    let mut cnt: c_int = 0;
    let start: c_int = if issuffix { startsuffix } else { 0 };
    let end: c_int = if issuffix { (*Conf).naffixes } else { startsuffix };
    let Affix: *mut AffixNode =
        palloc0(ANHRDSZ() + core::mem::size_of::<AffixNodeData>()) as *mut AffixNode;

    (*Affix).length = 1;
    (*Affix).isvoid = 1;

    if issuffix {
        (*anode_data(Affix)).node = (*Conf).Suffix;
        (*Conf).Suffix = Affix;
    } else {
        (*anode_data(Affix)).node = (*Conf).Prefix;
        (*Conf).Prefix = Affix;
    }

    /* Count affixes with empty replace string */
    i = start;
    while i < end {
        if (*(*Conf).Affix.add(i as usize)).replen == 0 {
            cnt += 1;
        }
        i += 1;
    }

    /* There is not affixes with empty replace string */
    if cnt == 0 {
        return;
    }

    (*anode_data(Affix)).aff =
        cpalloc(Conf, core::mem::size_of::<*mut AFFIX>() * cnt as usize) as *mut *mut AFFIX;
    (*anode_data(Affix)).naff = cnt as u32;

    cnt = 0;
    i = start;
    while i < end {
        if (*(*Conf).Affix.add(i as usize)).replen == 0 {
            *(*anode_data(Affix)).aff.add(cnt as usize) = (*Conf).Affix.add(i as usize);
            cnt += 1;
        }
        i += 1;
    }
}

/*
 * Checks if the affixflag is used by dictionary. Conf->AffixData does not
 * contain affixflag if this flag is not used actually by the .dict file.
 *
 * Conf: current dictionary.
 * affixflag: affix flag.
 *
 * Returns true if the Conf->AffixData array contains affixflag, otherwise
 * returns false.
 */
unsafe fn isAffixInUse(Conf: *mut IspellDict, affixflag: *const c_char) -> bool {
    let mut i: c_int;

    i = 0;
    while i < (*Conf).nAffixData {
        if IsAffixFlagInUse(Conf, i, affixflag) {
            return true;
        }
        i += 1;
    }

    false
}

/*
 * Builds Conf->Prefix and Conf->Suffix trees from the imported affixes.
 */
#[unsafe(no_mangle)]
pub unsafe fn NISortAffixes(Conf: *mut IspellDict) {
    let mut Affix: *mut AFFIX;
    let mut i: size_t;
    let mut ptr: *mut CMPDAffix;
    let mut firstsuffix: c_int = (*Conf).naffixes;

    if (*Conf).naffixes == 0 {
        return;
    }

    /* Store compound affixes in the Conf->CompoundAffix array */
    if (*Conf).naffixes > 1 {
        qsort(
            (*Conf).Affix as *mut c_void,
            (*Conf).naffixes as usize,
            core::mem::size_of::<AFFIX>(),
            cmpaffix,
        );
    }
    (*Conf).CompoundAffix =
        palloc(core::mem::size_of::<CMPDAffix>() * (*Conf).naffixes as usize) as *mut CMPDAffix;
    ptr = (*Conf).CompoundAffix;
    (*ptr).affix = core::ptr::null();

    i = 0;
    while i < (*Conf).naffixes as size_t {
        Affix = &mut *((*Conf).Affix as *mut AFFIX).add(i as usize);
        if (*Affix).r#type == FF_SUFFIX as u32 && (i as c_int) < firstsuffix {
            firstsuffix = i as c_int;
        }

        if ((*Affix).flagflags & FF_COMPOUNDFLAG) != 0
            && (*Affix).replen > 0
            && isAffixInUse(Conf, (*Affix).flag)
        {
            let issuffix: bool = (*Affix).r#type == FF_SUFFIX as u32;

            if ptr == (*Conf).CompoundAffix
                || issuffix != (*ptr.sub(1)).issuffix
                || strbncmp(
                    (*ptr.sub(1)).affix as *const c_uchar,
                    (*Affix).repl as *const c_uchar,
                    (*ptr.sub(1)).len as Size,
                ) != 0
            {
                /* leave only unique and minimal suffixes */
                (*ptr).affix = (*Affix).repl;
                (*ptr).len = (*Affix).replen as c_int;
                (*ptr).issuffix = issuffix;
                ptr = ptr.add(1);
            }
        }
        i += 1;
    }
    (*ptr).affix = core::ptr::null();
    (*Conf).CompoundAffix = repalloc(
        (*Conf).CompoundAffix as *mut c_void,
        core::mem::size_of::<CMPDAffix>()
            * (ptr.offset_from((*Conf).CompoundAffix) as usize + 1),
    ) as *mut CMPDAffix;

    /* Start build a prefix tree */
    (*Conf).Prefix = mkANode(Conf, 0, firstsuffix, 0, FF_PREFIX);
    (*Conf).Suffix = mkANode(Conf, firstsuffix, (*Conf).naffixes, 0, FF_SUFFIX);
    mkVoidAffix(Conf, true, firstsuffix);
    mkVoidAffix(Conf, false, firstsuffix);
}

unsafe fn FindAffixes(
    mut node: *mut AffixNode,
    word: *const c_char,
    wrdlen: c_int,
    level: *mut c_int,
    r#type: c_int,
) -> *mut AffixNodeData {
    let mut StopLow: *mut AffixNodeData;
    let mut StopHigh: *mut AffixNodeData;
    let mut StopMiddle: *mut AffixNodeData;
    let mut symbol: u8;

    if (*node).isvoid != 0 {
        /* search void affixes */
        if (*anode_data(node)).naff != 0 {
            return anode_data(node);
        }
        node = (*anode_data(node)).node;
    }

    while !node.is_null() && *level < wrdlen {
        let data = anode_data(node);
        StopLow = data;
        StopHigh = data.add((*node).length as usize);
        while StopLow < StopHigh {
            StopMiddle = StopLow.add((StopHigh.offset_from(StopLow) as usize) >> 1);
            symbol = GETWCHAR(word, wrdlen, *level, r#type);

            if (*StopMiddle).val == symbol as u32 {
                *level += 1;
                if (*StopMiddle).naff != 0 {
                    return StopMiddle;
                }
                node = (*StopMiddle).node;
                break;
            } else if (*StopMiddle).val < symbol as u32 {
                StopLow = StopMiddle.add(1);
            } else {
                StopHigh = StopMiddle;
            }
        }
        if StopLow >= StopHigh {
            break;
        }
    }
    null_mut()
}

unsafe fn CheckAffix(
    word: *const c_char,
    len: size_t,
    Affix: *mut AFFIX,
    flagflags: c_int,
    newword: *mut c_char,
    baselen: *mut c_int,
) -> *mut c_char {
    /*
     * Check compound allow flags
     */

    if flagflags == 0 {
        if (*Affix).flagflags & FF_COMPOUNDONLY != 0 {
            return null_mut();
        }
    } else if flagflags as u32 & FF_COMPOUNDBEGIN != 0 {
        if (*Affix).flagflags & FF_COMPOUNDFORBIDFLAG != 0 {
            return null_mut();
        }
        if ((*Affix).flagflags & FF_COMPOUNDBEGIN) == 0 && (*Affix).r#type == FF_SUFFIX as u32 {
            return null_mut();
        }
    } else if flagflags as u32 & FF_COMPOUNDMIDDLE != 0 {
        if ((*Affix).flagflags & FF_COMPOUNDMIDDLE) == 0
            || ((*Affix).flagflags & FF_COMPOUNDFORBIDFLAG) != 0
        {
            return null_mut();
        }
    } else if flagflags as u32 & FF_COMPOUNDLAST != 0 {
        if (*Affix).flagflags & FF_COMPOUNDFORBIDFLAG != 0 {
            return null_mut();
        }
        if ((*Affix).flagflags & FF_COMPOUNDLAST) == 0 && (*Affix).r#type == FF_PREFIX as u32 {
            return null_mut();
        }
    }

    /*
     * make replace pattern of affix
     */
    if (*Affix).r#type == FF_SUFFIX as u32 {
        strcpy(newword, word);
        strcpy(
            newword.add(len - (*Affix).replen as size_t),
            (*Affix).find,
        );
        if !baselen.is_null() {
            /* store length of non-changed part of word */
            *baselen = (len - (*Affix).replen as size_t) as c_int;
        }
    } else {
        /*
         * if prefix is an all non-changed part's length then all word
         * contains only prefix and suffix, so out
         */
        if !baselen.is_null()
            && *baselen as size_t + strlen((*Affix).find) <= (*Affix).replen as size_t
        {
            return null_mut();
        }
        strcpy(newword, (*Affix).find);
        strcat(newword, word.add((*Affix).replen as size_t));
    }

    /*
     * check resulting word
     */
    if (*Affix).issimple != 0 {
        return newword;
    } else if (*Affix).isregis != 0 {
        if RS_execute(
            core::ptr::addr_of_mut!((*Affix).reg.regis) as *mut Regis,
            newword,
        ) {
            return newword;
        }
    } else {
        let data: *mut pg_wchar;
        let data_len: size_t;
        let newword_len: c_int;

        /* Convert data string to wide characters */
        newword_len = strlen(newword) as c_int;
        data = palloc((newword_len as usize + 1) * core::mem::size_of::<pg_wchar>()) as *mut pg_wchar;
        data_len = pg_mb2wchar_with_len(newword, data, newword_len) as size_t;

        if pg_regexec(
            (*Affix).reg.pregex,
            data,
            data_len,
            0,
            null_mut(),
            0,
            null_mut(),
            0,
        ) == REG_OKAY
        {
            pfree(data as *mut c_void);
            return newword;
        }
        pfree(data as *mut c_void);
    }

    null_mut()
}

unsafe fn addToResult(forms: *mut *mut c_char, cur: *mut *mut c_char, word: *mut c_char) -> c_int {
    if cur.offset_from(forms) >= (MAX_NORM - 1) as isize {
        return 0;
    }
    if forms == cur || strcmp(word, *cur.sub(1)) != 0 {
        *cur = pstrdup(word);
        *cur.add(1) = null_mut();
        return 1;
    }

    0
}

unsafe fn NormalizeSubWord(
    Conf: *mut IspellDict,
    word: *const c_char,
    flag: c_int,
) -> *mut *mut c_char {
    let mut suffix: *mut AffixNodeData;
    let mut prefix: *mut AffixNodeData;
    let mut slevel: c_int = 0;
    let mut plevel: c_int;
    let wrdlen: c_int = strlen(word) as c_int;
    let mut swrdlen: c_int;
    let forms: *mut *mut c_char;
    let mut cur: *mut *mut c_char;
    let mut newword: [c_char; 2 * MAXNORMLEN] = [0; 2 * MAXNORMLEN];
    let mut pnewword: [c_char; 2 * MAXNORMLEN] = [0; 2 * MAXNORMLEN];
    let mut snode: *mut AffixNode = (*Conf).Suffix;
    let mut pnode: *mut AffixNode;
    let mut i: c_int;
    let mut j: c_int;

    if wrdlen > MAXNORMLEN as c_int {
        return null_mut();
    }
    forms = palloc(MAX_NORM * core::mem::size_of::<*mut c_char>()) as *mut *mut c_char;
    cur = forms;
    *cur = null_mut();

    /* Check that the word itself is normal form */
    if FindWord(Conf, word, voidstring(), flag) != 0 {
        *cur = pstrdup(word);
        cur = cur.add(1);
        *cur = null_mut();
    }

    /* Find all other NORMAL forms of the 'word' (check only prefix) */
    pnode = (*Conf).Prefix;
    plevel = 0;
    while !pnode.is_null() {
        prefix = FindAffixes(pnode, word, wrdlen, &mut plevel, FF_PREFIX);
        if prefix.is_null() {
            break;
        }
        j = 0;
        while j < (*prefix).naff as c_int {
            if !CheckAffix(
                word,
                wrdlen as size_t,
                *(*prefix).aff.add(j as usize),
                flag,
                newword.as_mut_ptr(),
                null_mut(),
            )
            .is_null()
            {
                /* prefix success */
                if FindWord(
                    Conf,
                    newword.as_ptr(),
                    (**(*prefix).aff.add(j as usize)).flag,
                    flag,
                ) != 0
                {
                    cur = cur.offset(addToResult(forms, cur, newword.as_mut_ptr()) as isize);
                }
            }
            j += 1;
        }
        pnode = (*prefix).node;
    }

    /*
     * Find all other NORMAL forms of the 'word' (check suffix and then
     * prefix)
     */
    while !snode.is_null() {
        let mut baselen: c_int = 0;

        /* find possible suffix */
        suffix = FindAffixes(snode, word, wrdlen, &mut slevel, FF_SUFFIX);
        if suffix.is_null() {
            break;
        }
        /* foreach suffix check affix */
        i = 0;
        while i < (*suffix).naff as c_int {
            if !CheckAffix(
                word,
                wrdlen as size_t,
                *(*suffix).aff.add(i as usize),
                flag,
                newword.as_mut_ptr(),
                &mut baselen,
            )
            .is_null()
            {
                /* suffix success */
                if FindWord(
                    Conf,
                    newword.as_ptr(),
                    (**(*suffix).aff.add(i as usize)).flag,
                    flag,
                ) != 0
                {
                    cur = cur.offset(addToResult(forms, cur, newword.as_mut_ptr()) as isize);
                }

                /* now we will look changed word with prefixes */
                pnode = (*Conf).Prefix;
                plevel = 0;
                swrdlen = strlen(newword.as_ptr()) as c_int;
                while !pnode.is_null() {
                    prefix = FindAffixes(pnode, newword.as_ptr(), swrdlen, &mut plevel, FF_PREFIX);
                    if prefix.is_null() {
                        break;
                    }
                    j = 0;
                    while j < (*prefix).naff as c_int {
                        if !CheckAffix(
                            newword.as_ptr(),
                            swrdlen as size_t,
                            *(*prefix).aff.add(j as usize),
                            flag,
                            pnewword.as_mut_ptr(),
                            &mut baselen,
                        )
                        .is_null()
                        {
                            /* prefix success */
                            let ff: *const c_char = if ((**(*prefix).aff.add(j as usize)).flagflags
                                & (**(*suffix).aff.add(i as usize)).flagflags
                                & FF_CROSSPRODUCT)
                                != 0
                            {
                                voidstring()
                            } else {
                                (**(*prefix).aff.add(j as usize)).flag
                            };

                            if FindWord(Conf, pnewword.as_ptr(), ff, flag) != 0 {
                                cur = cur
                                    .offset(addToResult(forms, cur, pnewword.as_mut_ptr()) as isize);
                            }
                        }
                        j += 1;
                    }
                    pnode = (*prefix).node;
                }
            }
            i += 1;
        }

        snode = (*suffix).node;
    }

    if cur == forms {
        pfree(forms as *mut c_void);
        return null_mut();
    }
    forms
}

#[repr(C)]
struct SplitVar {
    nstem: c_int,
    lenstem: c_int,
    stem: *mut *mut c_char,
    next: *mut SplitVar,
}

unsafe fn CheckCompoundAffixes(
    ptr: *mut *mut CMPDAffix,
    word: *const c_char,
    mut len: c_int,
    CheckInPlace: bool,
) -> c_int {
    let issuffix: bool;

    /* in case CompoundAffix is null: */
    if (*ptr).is_null() {
        return -1;
    }

    if CheckInPlace {
        while !(**ptr).affix.is_null() {
            if len > (**ptr).len
                && strncmp((**ptr).affix, word, (**ptr).len as usize) == 0
            {
                len = (**ptr).len;
                issuffix = (**ptr).issuffix;
                *ptr = (*ptr).add(1);
                return if issuffix { len } else { 0 };
            }
            *ptr = (*ptr).add(1);
        }
    } else {
        let mut affbegin: *const c_char;

        while !(**ptr).affix.is_null() {
            affbegin = strstr(word, (**ptr).affix);
            if len > (**ptr).len && !affbegin.is_null() {
                len = (**ptr).len + affbegin.offset_from(word) as c_int;
                issuffix = (**ptr).issuffix;
                *ptr = (*ptr).add(1);
                return if issuffix { len } else { 0 };
            }
            *ptr = (*ptr).add(1);
        }
    }
    -1
}

unsafe fn CopyVar(s: *mut SplitVar, makedup: c_int) -> *mut SplitVar {
    let v: *mut SplitVar = palloc(core::mem::size_of::<SplitVar>()) as *mut SplitVar;

    (*v).next = null_mut();
    if !s.is_null() {
        let mut i: c_int;

        (*v).lenstem = (*s).lenstem;
        (*v).stem = palloc(core::mem::size_of::<*mut c_char>() * (*v).lenstem as usize)
            as *mut *mut c_char;
        (*v).nstem = (*s).nstem;
        i = 0;
        while i < (*s).nstem {
            *(*v).stem.add(i as usize) = if makedup != 0 {
                pstrdup(*(*s).stem.add(i as usize))
            } else {
                *(*s).stem.add(i as usize)
            };
            i += 1;
        }
    } else {
        (*v).lenstem = 16;
        (*v).stem = palloc(core::mem::size_of::<*mut c_char>() * (*v).lenstem as usize)
            as *mut *mut c_char;
        (*v).nstem = 0;
    }
    v
}

unsafe fn AddStem(v: *mut SplitVar, word: *mut c_char) {
    if (*v).nstem >= (*v).lenstem {
        (*v).lenstem *= 2;
        (*v).stem = repalloc(
            (*v).stem as *mut c_void,
            core::mem::size_of::<*mut c_char>() * (*v).lenstem as usize,
        ) as *mut *mut c_char;
    }

    *(*v).stem.add((*v).nstem as usize) = word;
    (*v).nstem += 1;
}

unsafe fn SplitToVariants(
    Conf: *mut IspellDict,
    snode: *mut SPNode,
    orig: *mut SplitVar,
    word: *const c_char,
    wordlen: c_int,
    mut startpos: c_int,
    minpos: c_int,
) -> *mut SplitVar {
    let var: *mut SplitVar;
    let mut StopLow: *mut SPNodeData;
    let mut StopHigh: *mut SPNodeData;
    let mut StopMiddle: *mut SPNodeData = null_mut();
    let mut node: *mut SPNode = if !snode.is_null() {
        snode
    } else {
        (*Conf).Dictionary
    };
    let mut level: c_int = if !snode.is_null() { minpos } else { startpos }; /* recursive
                                                                              * minpos==level */
    let mut lenaff: c_int;
    let mut caff: *mut CMPDAffix;
    let notprobed: *mut c_char;
    let mut compoundflag: c_int = 0;

    /* since this function recurses, it could be driven to stack overflow */
    check_stack_depth();

    notprobed = palloc(wordlen as usize) as *mut c_char;
    memset(notprobed as *mut c_void, 1, wordlen as usize);
    var = CopyVar(orig, 1);

    while level < wordlen {
        /* find word with epenthetic or/and compound affix */
        caff = (*Conf).CompoundAffix;
        loop {
            if !(level > startpos) {
                break;
            }
            lenaff = CheckCompoundAffixes(
                &mut caff,
                word.add(level as usize),
                wordlen - level,
                !node.is_null(),
            );
            if lenaff < 0 {
                break;
            }
            /*
             * there is one of compound affixes, so check word for existings
             */
            let mut buf: [c_char; MAXNORMLEN] = [0; MAXNORMLEN];
            let subres: *mut *mut c_char;

            lenaff = level - startpos + lenaff;

            if *notprobed.add((startpos + lenaff - 1) as usize) == 0 {
                continue;
            }

            if level + lenaff - 1 <= minpos {
                continue;
            }

            if lenaff >= MAXNORMLEN as c_int {
                continue; /* skip too big value */
            }
            if lenaff > 0 {
                memcpy(
                    buf.as_mut_ptr() as *mut c_void,
                    word.add(startpos as usize) as *const c_void,
                    lenaff as usize,
                );
            }
            buf[lenaff as usize] = b'\0' as c_char;

            if level == 0 {
                compoundflag = FF_COMPOUNDBEGIN as c_int;
            } else if level == wordlen - 1 {
                compoundflag = FF_COMPOUNDLAST as c_int;
            } else {
                compoundflag = FF_COMPOUNDMIDDLE as c_int;
            }
            subres = NormalizeSubWord(Conf, buf.as_ptr(), compoundflag);
            if !subres.is_null() {
                /* Yes, it was a word from dictionary */
                let new: *mut SplitVar = CopyVar(var, 0);
                let mut ptr: *mut SplitVar = var;
                let mut sptr: *mut *mut c_char = subres;

                *notprobed.add((startpos + lenaff - 1) as usize) = 0;

                while !(*sptr).is_null() {
                    AddStem(new, *sptr);
                    sptr = sptr.add(1);
                }
                pfree(subres as *mut c_void);

                while !(*ptr).next.is_null() {
                    ptr = (*ptr).next;
                }
                (*ptr).next = SplitToVariants(
                    Conf,
                    null_mut(),
                    new,
                    word,
                    wordlen,
                    startpos + lenaff,
                    startpos + lenaff,
                );

                pfree((*new).stem as *mut c_void);
                pfree(new as *mut c_void);
            }
        }

        if node.is_null() {
            break;
        }

        let data = spnode_data(node);
        StopLow = data;
        StopHigh = data.add((*node).length as usize);
        while StopLow < StopHigh {
            StopMiddle = StopLow.add((StopHigh.offset_from(StopLow) as usize) >> 1);
            if (*StopMiddle).val == *(word as *const u8).add(level as usize) as u32 {
                break;
            } else if (*StopMiddle).val < *(word as *const u8).add(level as usize) as u32 {
                StopLow = StopMiddle.add(1);
            } else {
                StopHigh = StopMiddle;
            }
        }

        if StopLow < StopHigh {
            if startpos == 0 {
                compoundflag = FF_COMPOUNDBEGIN as c_int;
            } else if level == wordlen - 1 {
                compoundflag = FF_COMPOUNDLAST as c_int;
            } else {
                compoundflag = FF_COMPOUNDMIDDLE as c_int;
            }

            /* find infinitive */
            if (*StopMiddle).isword != 0
                && ((*StopMiddle).compoundflag & compoundflag as u32) != 0
                && *notprobed.add(level as usize) != 0
            {
                /* ok, we found full compoundallowed word */
                if level > minpos {
                    /* and its length more than minimal */
                    if wordlen == level + 1 {
                        /* well, it was last word */
                        AddStem(
                            var,
                            pnstrdup(
                                word.add(startpos as usize),
                                (wordlen - startpos) as Size,
                            ),
                        );
                        pfree(notprobed as *mut c_void);
                        return var;
                    } else {
                        /* then we will search more big word at the same point */
                        let mut ptr: *mut SplitVar = var;

                        while !(*ptr).next.is_null() {
                            ptr = (*ptr).next;
                        }
                        (*ptr).next =
                            SplitToVariants(Conf, node, var, word, wordlen, startpos, level);
                        /* we can find next word */
                        level += 1;
                        AddStem(
                            var,
                            pnstrdup(
                                word.add(startpos as usize),
                                (level - startpos) as Size,
                            ),
                        );
                        node = (*Conf).Dictionary;
                        startpos = level;
                        continue;
                    }
                }
            }
            node = (*StopMiddle).node;
        } else {
            node = null_mut();
        }
        level += 1;
    }

    AddStem(
        var,
        pnstrdup(word.add(startpos as usize), (wordlen - startpos) as Size),
    );
    pfree(notprobed as *mut c_void);
    var
}

unsafe fn addNorm(
    lres: *mut *mut TSLexeme,
    lcur: *mut *mut TSLexeme,
    word: *mut c_char,
    flags: c_int,
    NVariant: uint16,
) {
    if (*lres).is_null() {
        *lres = palloc(MAX_NORM * core::mem::size_of::<TSLexeme>()) as *mut TSLexeme;
        *lcur = *lres;
    }

    if (*lcur).offset_from(*lres) < (MAX_NORM - 1) as isize {
        (**lcur).lexeme = word;
        (**lcur).flags = flags as uint16;
        (**lcur).nvariant = NVariant;
        *lcur = (*lcur).add(1);
        (**lcur).lexeme = null_mut();
    }
}

#[unsafe(no_mangle)]
pub unsafe fn NINormalizeWord(Conf: *mut IspellDict, word: *const c_char) -> *mut TSLexeme {
    let res: *mut *mut c_char;
    let mut lcur: *mut TSLexeme = null_mut();
    let mut lres: *mut TSLexeme = null_mut();
    let mut NVariant: uint16 = 1;

    res = NormalizeSubWord(Conf, word, 0);

    if !res.is_null() {
        let mut ptr: *mut *mut c_char = res;

        while !(*ptr).is_null() && lcur.offset_from(lres) < MAX_NORM as isize {
            addNorm(&mut lres, &mut lcur, *ptr, 0, {
                let v = NVariant;
                NVariant += 1;
                v
            });
            ptr = ptr.add(1);
        }
        pfree(res as *mut c_void);
    }

    if (*Conf).usecompound {
        let wordlen: c_int = strlen(word) as c_int;
        let mut ptr: *mut SplitVar;
        let mut var: *mut SplitVar =
            SplitToVariants(Conf, null_mut(), null_mut(), word, wordlen, 0, -1);
        let mut i: c_int;

        while !var.is_null() {
            if (*var).nstem > 1 {
                let subres: *mut *mut c_char = NormalizeSubWord(
                    Conf,
                    *(*var).stem.add(((*var).nstem - 1) as usize),
                    FF_COMPOUNDLAST as c_int,
                );

                if !subres.is_null() {
                    let mut subptr: *mut *mut c_char = subres;

                    while !(*subptr).is_null() {
                        i = 0;
                        while i < (*var).nstem - 1 {
                            addNorm(
                                &mut lres,
                                &mut lcur,
                                if subptr == subres {
                                    *(*var).stem.add(i as usize)
                                } else {
                                    pstrdup(*(*var).stem.add(i as usize))
                                },
                                0,
                                NVariant,
                            );
                            i += 1;
                        }

                        addNorm(&mut lres, &mut lcur, *subptr, 0, NVariant);
                        subptr = subptr.add(1);
                        NVariant += 1;
                    }

                    pfree(subres as *mut c_void);
                    *(*var).stem.add(0) = null_mut();
                    pfree(*(*var).stem.add(((*var).nstem - 1) as usize) as *mut c_void);
                }
            }

            i = 0;
            while i < (*var).nstem && !(*(*var).stem.add(i as usize)).is_null() {
                pfree(*(*var).stem.add(i as usize) as *mut c_void);
                i += 1;
            }
            ptr = (*var).next;
            pfree((*var).stem as *mut c_void);
            pfree(var as *mut c_void);
            var = ptr;
        }
    }

    lres
}
