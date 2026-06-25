//! Translated from PostgreSQL src/include/regex/regguts.h
//! Regex engine internals. All in-memory (NOT #[repr(C)]); the NFA structs are
//! heavily self-referential pointer chains, so links stay raw pointers with
//! TODO(ptr) until ownership is resolved in Phase 2.

use crate::postgres_ext::Oid;
use crate::regex::regcustom::chr;
use crate::regex::regex::pg_regex_t as regex_t;

// misc
pub const NOTREACHED: i32 = 0;
pub const DUPMAX: i32 = 255; // _POSIX2_RE_DUP_MAX
pub const DUPINF: i32 = DUPMAX + 1;
pub const REMAGIC: u32 = 0xfed7; // magic number for main struct

// Type codes for lookaround constraints.
pub const LATYPE_AHEAD_POS: i32 = 0o3; // positive lookahead
pub const LATYPE_AHEAD_NEG: i32 = 0o2; // negative lookahead
pub const LATYPE_BEHIND_POS: i32 = 0o1; // positive lookbehind
pub const LATYPE_BEHIND_NEG: i32 = 0o0; // negative lookbehind

pub const fn latype_is_pos(la: i32) -> bool {
    la & 0o1 != 0
}
pub const fn latype_is_ahead(la: i32) -> bool {
    la & 0o2 != 0
}

// bitmap manipulation: UBITS = CHAR_BIT * sizeof(unsigned) on a 32-bit `unsigned`.
pub const UBITS: usize = 8 * core::mem::size_of::<u32>();

pub fn bset(uv: &mut [u32], sn: usize) {
    uv[sn / UBITS] |= 1u32 << (sn % UBITS);
}
pub const fn isbset(uv: &[u32], sn: usize) -> bool {
    uv[sn / UBITS] & (1u32 << (sn % UBITS)) != 0
}

/// `char_classes` - known character classes.
#[repr(i32)]
pub enum char_classes {
    CC_ALNUM,
    CC_ALPHA,
    CC_ASCII,
    CC_BLANK,
    CC_CNTRL,
    CC_DIGIT,
    CC_GRAPH,
    CC_LOWER,
    CC_PRINT,
    CC_PUNCT,
    CC_SPACE,
    CC_UPPER,
    CC_XDIGIT,
    CC_WORD,
}

pub const NUM_CCLASSES: usize = 14;

/// `color` - colors of characters.
pub type color = i16;

pub const MAX_COLOR: color = 32767;
pub const COLORLESS: color = -1; // impossible color
pub const RAINBOW: color = -2; // all colors except pseudocolors
pub const WHITE: color = 0; // default color, parent of all others

// colordesc "sub" sentinel and flag bits.
pub const NOSUB: color = COLORLESS; // value of "sub" when no open subcolor
pub const FREECOL: i32 = 0o1; // currently free
pub const PSEUDO: i32 = 0o2; // pseudocolor, no real chars
pub const COLMARK: i32 = 0o4; // temporary marker

/// `colordesc` - per-color compile-time data.
pub struct colordesc {
    pub nschrs: i32,
    pub nuchrs: i32,
    pub sub: color,        // open subcolor, or free-chain ptr
    pub arcs: *mut arc,    // chain of all arcs of this color. TODO(ptr)
    pub firstchr: chr,
    pub flags: i32,
}

pub const fn unusedcolor(cd: &colordesc) -> bool {
    cd.flags & FREECOL != 0
}

pub const CMMAGIC: i32 = 0x876;
pub const NINLINECDS: usize = 10;

/// `colormaprange` - a range of high chrs mapping to a hicolormap row.
pub struct colormaprange {
    pub cmin: chr, // range represents cmin..cmax inclusive
    pub cmax: chr,
    pub rownum: i32, // row index in hicolormap array (>= 1)
}

/// `colormap` - the chr->color mapping plus compile-time machinery.
pub struct colormap {
    pub magic: i32,
    pub v: *mut vars, // for compile error reporting. TODO(ptr)
    pub ncds: usize,
    pub max: usize,
    pub free: color, // beginning of free chain
    pub cd: *mut colordesc, // array of colordescs. TODO(ptr)

    pub locolormap: *mut color, // simple array indexed by chr code. TODO(ptr)

    pub classbits: [i32; NUM_CCLASSES],
    pub numcmranges: i32,
    pub cmranges: *mut colormaprange, // TODO(ptr)
    pub hicolormap: *mut color, // 2-D array of color entries. TODO(ptr)
    pub maxarrayrows: i32,
    pub hiarrayrows: i32,
    pub hiarraycols: i32,

    pub cdspace: [colordesc; NINLINECDS],
}

/// `cvec` - representation of a set of characters.
pub struct cvec {
    pub nchrs: i32,
    pub chrspace: i32,
    pub chrs: *mut chr, // vector of chrs. TODO(ptr)
    pub nranges: i32,
    pub rangespace: i32,
    pub ranges: *mut chr, // vector of chr pairs. TODO(ptr)
    pub cclasscode: i32, // value of "enum classes", or -1
}

// NFA internal representation.

/// `arc` - an NFA arc.
pub struct arc {
    pub type_: i32, // 0 if free, else an NFA arc type code
    pub co: color,  // color the arc matches (possibly RAINBOW)
    pub from: *mut state, // TODO(ptr)
    pub to: *mut state,   // TODO(ptr)
    pub outchain: *mut arc,    // link in *from's outs chain or free chain. TODO(ptr)
    pub outchainRev: *mut arc, // back-link. TODO(ptr)
    pub inchain: *mut arc,     // link in *to's ins chain. TODO(ptr)
    pub inchainRev: *mut arc,  // back-link. TODO(ptr)
    pub colorchain: *mut arc,    // link in color's arc chain. TODO(ptr)
    pub colorchainRev: *mut arc, // back-link. TODO(ptr)
}

pub const FIRSTABSIZE: usize = 64;
pub const MAXABSIZE: usize = 1024;

/// `arcbatch` - bulk allocation of arcs (on-heap FAM: `a[]` lives past header).
pub struct arcbatch {
    pub next: *mut arcbatch, // chain link. TODO(ptr)
    pub narcs: usize,
    // a: [arc; FLEXIBLE_ARRAY_MEMBER] - trailing in buffer
}

pub const FREESTATE: i32 = -1;

/// `state` - an NFA state.
pub struct state {
    pub no: i32, // state number; or FREESTATE
    pub flag: i8, // marks special states
    pub nins: i32,
    pub nouts: i32,
    pub ins: *mut arc,  // chain of inarcs. TODO(ptr)
    pub outs: *mut arc, // chain of outarcs. TODO(ptr)
    pub tmp: *mut state,  // temporary for traversal. TODO(ptr)
    pub next: *mut state, // chain of live states / free chain. TODO(ptr)
    pub prev: *mut state, // back-link. TODO(ptr)
}

pub const FIRSTSBSIZE: usize = 32;
pub const MAXSBSIZE: usize = 1024;

/// `statebatch` - bulk allocation of states (on-heap FAM: `s[]` past header).
pub struct statebatch {
    pub next: *mut statebatch, // chain link. TODO(ptr)
    pub nstates: usize,
    // s: [state; FLEXIBLE_ARRAY_MEMBER] - trailing in buffer
}

/// `nfa` - an NFA under construction.
pub struct nfa {
    pub pre: *mut state,   // TODO(ptr)
    pub init: *mut state,  // TODO(ptr)
    pub final_: *mut state, // TODO(ptr)
    pub post: *mut state,  // TODO(ptr)
    pub nstates: i32,
    pub states: *mut state, // chain of live states. TODO(ptr)
    pub slast: *mut state,  // tail of the chain. TODO(ptr)
    pub freestates: *mut state, // TODO(ptr)
    pub freearcs: *mut arc,     // TODO(ptr)
    pub lastsb: *mut statebatch, // TODO(ptr)
    pub lastab: *mut arcbatch,   // TODO(ptr)
    pub lastsbused: usize,
    pub lastabused: usize,
    pub cm: *mut colormap, // the color map. TODO(ptr)
    pub bos: [color; 2],
    pub eos: [color; 2],
    pub flags: i32,
    pub minmatchall: i32,
    pub maxmatchall: i32,
    pub v: *mut vars,      // TODO(ptr)
    pub parent: *mut nfa, // parent NFA, if any. TODO(ptr)
}

// compacted NFA flags (in nfa/cnfa `flags`).
pub const HASLACONS: i32 = 0o1; // uses lookaround constraints
pub const MATCHALL: i32 = 0o2; // matches all strings of a range of lengths
pub const HASCANTMATCH: i32 = 0o4; // contains CANTMATCH arcs (nfa only)
pub const CNFA_NOPROGRESS: i32 = 0o1; // per-state flag: a no-progress state

/// `carc` - a compacted NFA arc.
pub struct carc {
    pub co: color, // COLORLESS is list terminator
    pub to: i32,   // next-state number
}

/// `cnfa` - a compacted NFA.
pub struct cnfa {
    pub nstates: i32,
    pub ncolors: i32, // number of colors (max color in use + 1)
    pub flags: i32,
    pub pre: i32,  // setup state number
    pub post: i32, // teardown state number
    pub bos: [color; 2],
    pub eos: [color; 2],
    pub stflags: *mut u8, // vector of per-state flags bytes. TODO(ptr)
    pub states: *mut *mut carc, // vector of pointers to outarc lists. TODO(ptr)
    pub arcs: *mut carc, // the area for the lists. TODO(ptr)
    pub minmatchall: i32, // MATCHALL only (else -1)
    pub maxmatchall: i32, // MATCHALL only (else -1)
}

pub const fn nullcnfa(cnfa: &cnfa) -> bool {
    cnfa.nstates == 0
}

/// Transient-heap-space limit for the compiler (bounds NFA complexity).
pub const REG_MAX_COMPILE_SPACE: usize =
    500000 * (core::mem::size_of::<state>() + 4 * core::mem::size_of::<arc>());

// subre flag bits (in subre.flags).
pub const LONGER: i32 = 0o1; // prefers longer match
pub const SHORTER: i32 = 0o2; // prefers shorter match
pub const MIXED: i32 = 0o4; // mixed preference below
pub const CAP: i32 = 0o10; // capturing parens here or below
pub const BACKR: i32 = 0o20; // back reference here or below
pub const BRUSE: i32 = 0o40; // is referenced by a back reference
pub const INUSE: i32 = 0o100; // in use in final tree
pub const UPPROP: i32 = MIXED | CAP | BACKR; // flags which propagate up

pub const fn lmix(f: i32) -> i32 {
    f << 2 // LONGER -> MIXED
}
pub const fn smix(f: i32) -> i32 {
    f << 1 // SHORTER -> MIXED
}
pub const fn up(f: i32) -> i32 {
    (f & UPPROP) | (lmix(f) & smix(f) & MIXED)
}
pub const fn messy(f: i32) -> i32 {
    f & (MIXED | CAP | BACKR)
}
pub const fn pref(f: i32) -> i32 {
    f & (LONGER | SHORTER)
}
pub const fn pref2(f1: i32, f2: i32) -> i32 {
    if pref(f1) != 0 {
        pref(f1)
    } else {
        pref(f2)
    }
}
pub const fn combine(f1: i32, f2: i32) -> i32 {
    up(f1 | f2) | pref2(f1, f2)
}

/// `subre` - a node of the subexpression tree.
pub struct subre {
    pub op: i8, // see type codes in the header comment
    pub flags: i8,
    pub latype: i8, // LATYPE code, if lookaround constraint
    pub id: i32, // ID of subre (1..ntree-1)
    pub capno: i32, // if capture node, subno to capture into
    pub backno: i32, // if backref node, subno it refers to
    pub min: i16, // min repetitions for iteration or backref
    pub max: i16, // max repetitions for iteration or backref
    pub child: *mut subre,   // first child / freelist chain. TODO(ptr)
    pub sibling: *mut subre, // next child of same parent. TODO(ptr)
    pub begin: *mut state, // outarcs from here. TODO(ptr)
    pub end: *mut state,   // ...ending in inarcs here. TODO(ptr)
    pub cnfa: cnfa, // compacted NFA, if any
    pub chain: *mut subre, // for bookkeeping and error cleanup. TODO(ptr)
}

/// `fns` - table of fn pointers for generic regex manipulation. A regex_t's
/// re_fns points to one of these (a routine struct; left as fn pointers per
/// routine-struct.md, since this vtable is reached opaquely via re_fns).
pub struct fns {
    pub free: Option<unsafe extern "C" fn(*mut regex_t)>,
    pub stack_too_deep: Option<unsafe extern "C" fn() -> i32>,
}

pub const GUTSMAGIC: i32 = 0xfed9;

/// `guts` - the insides of a regex_t, hidden behind a void *.
pub struct guts {
    pub magic: i32,
    pub cflags: i32, // copy of compile flags
    pub info: i64,   // copy of re_info (C long)
    pub nsub: usize, // copy of re_nsub
    pub tree: *mut subre, // TODO(ptr)
    pub search: cnfa, // for fast preliminary search
    pub ntree: i32, // number of subre's, plus one
    pub cmap: colormap,
    pub compare: Option<unsafe extern "C" fn(*const chr, *const chr, usize) -> i32>,
    pub lacons: *mut subre, // lookaround-constraint vector. TODO(ptr)
    pub nlacons: i32, // size of lacons[]; slots 1..nlacons-1 used
}

/// Opaque; `struct vars` is regcomp.c's private per-compile state, not ported.
pub struct vars {
    _opaque: [u8; 0],
}

// prototypes exported from regcomp.c to regexec.c.

/// `pg_set_regex_collation` - select the collation for subsequent regex work.
pub fn pg_set_regex_collation(_collation: Oid) {
    unimplemented!()
}

/// `pg_reg_getcolor` - look up the color of a high-valued chr.
pub fn pg_reg_getcolor(_cm: &mut colormap, _c: chr) -> color {
    unimplemented!()
}
