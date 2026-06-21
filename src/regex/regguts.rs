//! regex/regguts.h - internal interface definitions for the reg package.
//!
//! Copyright (c) 1998, 1999 Henry Spencer. See PostgreSQL source for the full
//! license text. Internal NFA/colormap/subre representations shared between
//! regcomp.c and regexec.c.

#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

use std::ffi::{c_char, c_int, c_long, c_void};

use crate::c::{Size, FLEXIBLE_ARRAY_MEMBER};
use crate::postgres_ext::Oid;
use crate::regex::regcustom::{chr, CHR_MIN, MAX_SIMPLE_CHR};

// ---------------------------------------------------------------------------
// Locally stubbed types not yet ported.
// ---------------------------------------------------------------------------

// regex.h's regex_t is not yet translated; stub locally. // TODO: dedup
pub type regex_t = c_void;

// struct vars is defined in regcomp.c (not a header); opaque here.
pub type vars = c_void;

// ---------------------------------------------------------------------------
// Things that regcustom.h might override (defaults here).
// ---------------------------------------------------------------------------

/// #define DISCARD void -- for throwing values away.
pub type DISCARD = c_void;

/// #define _POSIX2_RE_DUP_MAX 255 (normally from <limits.h>)
pub const _POSIX2_RE_DUP_MAX: c_int = 255;

// ---------------------------------------------------------------------------
// misc
// ---------------------------------------------------------------------------

pub const NOTREACHED: c_int = 0;

pub const DUPMAX: c_int = _POSIX2_RE_DUP_MAX;
pub const DUPINF: c_int = DUPMAX + 1;

/// magic number for main struct
pub const REMAGIC: c_int = 0xfed7;

/* Type codes for lookaround constraints */
/// positive lookahead
pub const LATYPE_AHEAD_POS: c_int = 0o3;
/// negative lookahead
pub const LATYPE_AHEAD_NEG: c_int = 0o2;
/// positive lookbehind
pub const LATYPE_BEHIND_POS: c_int = 0o1;
/// negative lookbehind
pub const LATYPE_BEHIND_NEG: c_int = 0o0;

/// #define LATYPE_IS_POS(la) ((la) & 01)
#[inline]
pub fn LATYPE_IS_POS(la: c_int) -> c_int {
    la & 0o1
}

/// #define LATYPE_IS_AHEAD(la) ((la) & 02)
#[inline]
pub fn LATYPE_IS_AHEAD(la: c_int) -> c_int {
    la & 0o2
}

// ---------------------------------------------------------------------------
// known character classes (enum char_classes)
// ---------------------------------------------------------------------------

pub type char_classes = c_int;
pub const CC_ALNUM: char_classes = 0;
pub const CC_ALPHA: char_classes = 1;
pub const CC_ASCII: char_classes = 2;
pub const CC_BLANK: char_classes = 3;
pub const CC_CNTRL: char_classes = 4;
pub const CC_DIGIT: char_classes = 5;
pub const CC_GRAPH: char_classes = 6;
pub const CC_LOWER: char_classes = 7;
pub const CC_PRINT: char_classes = 8;
pub const CC_PUNCT: char_classes = 9;
pub const CC_SPACE: char_classes = 10;
pub const CC_UPPER: char_classes = 11;
pub const CC_XDIGIT: char_classes = 12;
pub const CC_WORD: char_classes = 13;

pub const NUM_CCLASSES: usize = 14;

// ---------------------------------------------------------------------------
// colors
// ---------------------------------------------------------------------------

/// colors of characters: typedef short color;
pub type color = i16;

/// max color (must fit in 'color' datatype)
pub const MAX_COLOR: c_int = 32767;
/// impossible color
pub const COLORLESS: color = -1;
/// represents all colors except pseudocolors
pub const RAINBOW: color = -2;
/// default color, parent of all others
pub const WHITE: color = 0;

/// value of "sub" when no open subcolor: #define NOSUB COLORLESS
pub const NOSUB: color = COLORLESS;

/* colordesc.flags bits */
/// currently free
pub const FREECOL: c_int = 0o1;
/// pseudocolor, no real chars
pub const PSEUDO: c_int = 0o2;
/// temporary marker used in some functions
pub const COLMARK: c_int = 0o4;

/// Per-color data structure for the compile-time color machinery.
#[repr(C)]
pub struct colordesc {
    /// number of simple chars of this color
    pub nschrs: c_int,
    /// number of upper map entries of this color
    pub nuchrs: c_int,
    /// open subcolor, if any; or free-chain ptr
    pub sub: color,
    /// chain of all arcs of this color
    pub arcs: *mut arc,
    /// simple char first assigned to this color
    pub firstchr: chr,
    /// bitmask of FREECOL/PSEUDO/COLMARK
    pub flags: c_int,
}

/// #define UNUSEDCOLOR(cd) ((cd)->flags & FREECOL)
#[inline]
pub unsafe fn UNUSEDCOLOR(cd: *const colordesc) -> c_int {
    (*cd).flags & FREECOL
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct colormaprange {
    /// range represents cmin..cmax inclusive
    pub cmin: chr,
    pub cmax: chr,
    /// row index in hicolormap array (>= 1)
    pub rownum: c_int,
}

/// #define CMMAGIC 0x876
pub const CMMAGIC: c_int = 0x876;

/// If we need up to NINLINECDS, we store them here to save a malloc.
pub const NINLINECDS: Size = 10;

/// The color map itself.
#[repr(C)]
pub struct colormap {
    pub magic: c_int,
    /// for compile error reporting
    pub v: *mut vars,
    /// allocated length of colordescs array
    pub ncds: Size,
    /// highest color number currently in use
    pub max: Size,
    /// beginning of free chain (if non-0)
    pub free: color,
    /// pointer to array of colordescs
    pub cd: *mut colordesc,

    /* mapping data for chrs <= MAX_SIMPLE_CHR: */
    /// simple array indexed by chr code
    pub locolormap: *mut color,

    /* mapping data for chrs > MAX_SIMPLE_CHR: */
    /// see comment in header
    pub classbits: [c_int; NUM_CCLASSES],
    /// number of colormapranges
    pub numcmranges: c_int,
    /// ranges of high chrs
    pub cmranges: *mut colormaprange,
    /// 2-D array of color entries
    pub hicolormap: *mut color,
    /// number of array rows allocated
    pub maxarrayrows: c_int,
    /// number of array rows in use
    pub hiarrayrows: c_int,
    /// number of array columns (2^N)
    pub hiarraycols: c_int,

    /// inline colordescs to save a malloc
    pub cdspace: [colordesc; NINLINECDS as usize],
}

/// #define CDEND(cm) (&(cm)->cd[(cm)->max + 1])
#[inline]
pub unsafe fn CDEND(cm: *mut colormap) -> *mut colordesc {
    (*cm).cd.add(((*cm).max as usize) + 1)
}

/// fetch color for chr; beware of multiple evaluation of c argument
/// #define GETCOLOR(cm, c) ...
#[inline]
pub unsafe fn GETCOLOR(cm: *mut colormap, c: chr) -> color {
    if c <= MAX_SIMPLE_CHR {
        *(*cm).locolormap.add((c - CHR_MIN) as usize)
    } else {
        pg_reg_getcolor(cm, c)
    }
}

// ---------------------------------------------------------------------------
// Representation of a set of characters (cvec).
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct cvec {
    /// number of chrs
    pub nchrs: c_int,
    /// number of chrs allocated in chrs[]
    pub chrspace: c_int,
    /// pointer to vector of chrs
    pub chrs: *mut chr,
    /// number of ranges (chr pairs)
    pub nranges: c_int,
    /// number of ranges allocated in ranges[]
    pub rangespace: c_int,
    /// pointer to vector of chr pairs
    pub ranges: *mut chr,
    /// value of "enum classes", or -1
    pub cclasscode: c_int,
}

// ---------------------------------------------------------------------------
// definitions for NFA internal representation
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct arc {
    /// 0 if free, else an NFA arc type code
    pub r#type: c_int,
    /// color the arc matches (possibly RAINBOW)
    pub co: color,
    /// where it's from
    pub from: *mut state,
    /// where it's to
    pub to: *mut state,
    /// link in *from's outs chain or free chain
    pub outchain: *mut arc,
    /// back-link in *from's outs chain
    pub outchainRev: *mut arc,
    /// link in *to's ins chain
    pub inchain: *mut arc,
    /// back-link in *to's ins chain
    pub inchainRev: *mut arc,
    /// link in color's arc chain (unused when co == RAINBOW)
    pub colorchain: *mut arc,
    /// back-link in color's arc chain (unused when co == RAINBOW)
    pub colorchainRev: *mut arc,
}

#[repr(C)]
pub struct arcbatch {
    /// chain link
    pub next: *mut arcbatch,
    /// number of arcs allocated in this arcbatch
    pub narcs: Size,
    /// flexible array of arcs
    pub a: [arc; FLEXIBLE_ARRAY_MEMBER],
}

/// #define ARCBATCHSIZE(n) ((n) * sizeof(struct arc) + offsetof(struct arcbatch, a))
#[inline]
pub fn ARCBATCHSIZE(n: Size) -> Size {
    n * (std::mem::size_of::<arc>() as Size)
        + (std::mem::offset_of!(arcbatch, a) as Size)
}

/// first batch will have FIRSTABSIZE arcs; then double it until MAXABSIZE
pub const FIRSTABSIZE: c_int = 64;
pub const MAXABSIZE: c_int = 1024;

/// state number value marking a free state
pub const FREESTATE: c_int = -1;

#[repr(C)]
pub struct state {
    /// state number, zero and up; or FREESTATE
    pub no: c_int,
    /// marks special states
    pub flag: c_char,
    /// number of inarcs
    pub nins: c_int,
    /// number of outarcs
    pub nouts: c_int,
    /// chain of inarcs
    pub ins: *mut arc,
    /// chain of outarcs
    pub outs: *mut arc,
    /// temporary for traversal algorithms
    pub tmp: *mut state,
    /// chain for traversing all live states (also free-state chain)
    pub next: *mut state,
    /// back-link in chain of all live states
    pub prev: *mut state,
}

#[repr(C)]
pub struct statebatch {
    /// chain link
    pub next: *mut statebatch,
    /// number of states allocated in this batch
    pub nstates: Size,
    /// flexible array of states
    pub s: [state; FLEXIBLE_ARRAY_MEMBER],
}

/// #define STATEBATCHSIZE(n) ((n) * sizeof(struct state) + offsetof(struct statebatch, s))
#[inline]
pub fn STATEBATCHSIZE(n: Size) -> Size {
    n * (std::mem::size_of::<state>() as Size)
        + (std::mem::offset_of!(statebatch, s) as Size)
}

/// first batch will have FIRSTSBSIZE states; then double it until MAXSBSIZE
pub const FIRSTSBSIZE: c_int = 32;
pub const MAXSBSIZE: c_int = 1024;

#[repr(C)]
pub struct nfa {
    /// pre-initial state
    pub pre: *mut state,
    /// initial state
    pub init: *mut state,
    /// final state
    pub r#final: *mut state,
    /// post-final state
    pub post: *mut state,
    /// for numbering states
    pub nstates: c_int,
    /// chain of live states
    pub states: *mut state,
    /// tail of the chain
    pub slast: *mut state,
    /// chain of free states
    pub freestates: *mut state,
    /// chain of free arcs
    pub freearcs: *mut arc,
    /// chain of statebatches
    pub lastsb: *mut statebatch,
    /// chain of arcbatches
    pub lastab: *mut arcbatch,
    /// number of states consumed from *lastsb
    pub lastsbused: Size,
    /// number of arcs consumed from *lastab
    pub lastabused: Size,
    /// the color map
    pub cm: *mut colormap,
    /// colors, if any, assigned to BOS and BOL
    pub bos: [color; 2],
    /// colors, if any, assigned to EOS and EOL
    pub eos: [color; 2],
    /// flags to pass forward to cNFA
    pub flags: c_int,
    /// min number of chrs to match, if matchall
    pub minmatchall: c_int,
    /// max number of chrs to match, or DUPINF
    pub maxmatchall: c_int,
    /// simplifies compile error reporting
    pub v: *mut vars,
    /// parent NFA, if any
    pub parent: *mut nfa,
}

// ---------------------------------------------------------------------------
// definitions for compacted NFA
// ---------------------------------------------------------------------------

#[repr(C)]
#[derive(Clone, Copy)]
pub struct carc {
    /// COLORLESS is list terminator
    pub co: color,
    /// next-state number
    pub to: c_int,
}

/* cnfa.flags bits */
/// uses lookaround constraints
pub const HASLACONS: c_int = 0o1;
/// matches all strings of a range of lengths
pub const MATCHALL: c_int = 0o2;
/// contains CANTMATCH arcs
pub const HASCANTMATCH: c_int = 0o4;

/// flag bit for a no-progress state (in stflags)
pub const CNFA_NOPROGRESS: c_int = 0o1;

#[repr(C)]
pub struct cnfa {
    /// number of states
    pub nstates: c_int,
    /// number of colors (max color in use + 1)
    pub ncolors: c_int,
    /// bitmask of HASLACONS/MATCHALL/HASCANTMATCH
    pub flags: c_int,
    /// setup state number
    pub pre: c_int,
    /// teardown state number
    pub post: c_int,
    /// colors, if any, assigned to BOS and BOL
    pub bos: [color; 2],
    /// colors, if any, assigned to EOS and EOL
    pub eos: [color; 2],
    /// vector of per-state flags bytes
    pub stflags: *mut c_char,
    /// vector of pointers to outarc lists
    pub states: *mut *mut carc,
    /// the area for the lists
    pub arcs: *mut carc,
    /// min number of chrs to match (MATCHALL only, else -1)
    pub minmatchall: c_int,
    /// max number of chrs to match, or DUPINF (MATCHALL only, else -1)
    pub maxmatchall: c_int,
}

/// #define ZAPCNFA(cnfa) ((cnfa).nstates = 0)  (non-REG_DEBUG variant)
#[inline]
pub fn ZAPCNFA(cnfa: &mut cnfa) {
    cnfa.nstates = 0;
}

/// #define NULLCNFA(cnfa) ((cnfa).nstates == 0)
#[inline]
pub fn NULLCNFA(cnfa: &cnfa) -> bool {
    cnfa.nstates == 0
}

/// This symbol limits the transient heap space used by the regex compiler.
/// #define REG_MAX_COMPILE_SPACE (500000 * (sizeof(struct state) + 4 * sizeof(struct arc)))
#[inline]
pub fn REG_MAX_COMPILE_SPACE() -> Size {
    500000 * (std::mem::size_of::<state>() as Size + 4 * std::mem::size_of::<arc>() as Size)
}

// ---------------------------------------------------------------------------
// subexpression tree (subre)
// ---------------------------------------------------------------------------

/* subre.flags bits */
/// prefers longer match
pub const LONGER: c_int = 0o1;
/// prefers shorter match
pub const SHORTER: c_int = 0o2;
/// mixed preference below
pub const MIXED: c_int = 0o4;
/// capturing parens here or below
pub const CAP: c_int = 0o10;
/// back reference here or below
pub const BACKR: c_int = 0o20;
/// is referenced by a back reference
pub const BRUSE: c_int = 0o40;
/// in use in final tree
pub const INUSE: c_int = 0o100;
/// flags which should propagate up
pub const UPPROP: c_int = MIXED | CAP | BACKR;

/// #define LMIX(f) ((f)<<2)  -- LONGER -> MIXED
#[inline]
pub fn LMIX(f: c_int) -> c_int {
    f << 2
}

/// #define SMIX(f) ((f)<<1)  -- SHORTER -> MIXED
#[inline]
pub fn SMIX(f: c_int) -> c_int {
    f << 1
}

/// #define UP(f) (((f)&UPPROP) | (LMIX(f) & SMIX(f) & MIXED))
#[inline]
pub fn UP(f: c_int) -> c_int {
    (f & UPPROP) | (LMIX(f) & SMIX(f) & MIXED)
}

/// #define MESSY(f) ((f)&(MIXED|CAP|BACKR))
#[inline]
pub fn MESSY(f: c_int) -> c_int {
    f & (MIXED | CAP | BACKR)
}

/// #define PREF(f) ((f)&(LONGER|SHORTER))
#[inline]
pub fn PREF(f: c_int) -> c_int {
    f & (LONGER | SHORTER)
}

/// #define PREF2(f1, f2) ((PREF(f1) != 0) ? PREF(f1) : PREF(f2))
#[inline]
pub fn PREF2(f1: c_int, f2: c_int) -> c_int {
    if PREF(f1) != 0 {
        PREF(f1)
    } else {
        PREF(f2)
    }
}

/// #define COMBINE(f1, f2) (UP((f1)|(f2)) | PREF2(f1, f2))
#[inline]
pub fn COMBINE(f1: c_int, f2: c_int) -> c_int {
    UP(f1 | f2) | PREF2(f1, f2)
}

#[repr(C)]
pub struct subre {
    /// see type codes above
    pub op: c_char,
    /// bitmask of LONGER/SHORTER/MIXED/CAP/BACKR/BRUSE/INUSE
    pub flags: c_char,
    /// LATYPE code, if lookaround constraint
    pub latype: c_char,
    /// ID of subre (1..ntree-1)
    pub id: c_int,
    /// if capture node, subno to capture into
    pub capno: c_int,
    /// if backref node, subno it refers to
    pub backno: c_int,
    /// min repetitions for iteration or backref
    pub min: i16,
    /// max repetitions for iteration or backref
    pub max: i16,
    /// first child, if any (also freelist chain)
    pub child: *mut subre,
    /// next child of same parent, if any
    pub sibling: *mut subre,
    /// outarcs from here...
    pub begin: *mut state,
    /// ...ending in inarcs here
    pub end: *mut state,
    /// compacted NFA, if any
    pub cnfa: cnfa,
    /// for bookkeeping and error cleanup
    pub chain: *mut subre,
}

// ---------------------------------------------------------------------------
// table of function pointers for generic manipulation functions
// ---------------------------------------------------------------------------

#[repr(C)]
pub struct fns {
    /// void (*free)(regex_t *);
    pub free: Option<unsafe extern "C" fn(*mut regex_t)>,
    /// int (*stack_too_deep)(void);
    pub stack_too_deep: Option<unsafe extern "C" fn() -> c_int>,
}

/// #define STACK_TOO_DEEP(re) ((*((struct fns *)(re)->re_fns)->stack_too_deep)())
/// re_fns lives inside regex_t (not yet ported); caller passes the fns pointer.
#[inline]
pub unsafe fn STACK_TOO_DEEP(re_fns: *mut fns) -> c_int {
    ((*re_fns).stack_too_deep.unwrap())()
}

// ---------------------------------------------------------------------------
// the insides of a regex_t, hidden behind a void *
// ---------------------------------------------------------------------------

/// #define GUTSMAGIC 0xfed9
pub const GUTSMAGIC: c_int = 0xfed9;

#[repr(C)]
pub struct guts {
    pub magic: c_int,
    /// copy of compile flags
    pub cflags: c_int,
    /// copy of re_info
    pub info: c_long,
    /// copy of re_nsub
    pub nsub: Size,
    pub tree: *mut subre,
    /// for fast preliminary search
    pub search: cnfa,
    /// number of subre's, plus one
    pub ntree: c_int,
    pub cmap: colormap,
    /// int (*compare)(const chr *, const chr *, size_t);
    pub compare: Option<unsafe extern "C" fn(*const chr, *const chr, Size) -> c_int>,
    /// lookaround-constraint vector
    pub lacons: *mut subre,
    /// size of lacons[]; only slots 1 .. nlacons-1 are used
    pub nlacons: c_int,
}

// ---------------------------------------------------------------------------
// prototypes for functions exported from regcomp.c to regexec.c
// ---------------------------------------------------------------------------

pub unsafe fn pg_set_regex_collation(collation: Oid) {
    crate::regex::regc_pg_locale::pg_set_regex_collation(collation)
}

pub unsafe fn pg_reg_getcolor(cm: *mut colormap, c: chr) -> color {
    crate::regex::regc_color::pg_reg_getcolor(cm, c)
}
