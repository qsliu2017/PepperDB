//! ts_public.h - Public interface to various tsearch modules (parsers/dictionaries).

use std::ffi::{c_char, c_int};

use crate::c::{int16, int32, uint16, uint32, Size};
use crate::postgres_ext::Oid;
use crate::utils::adt::tsquery_util::QueryOperand;
use crate::utils::adt::tsvector::WordEntryPos;

/*
 * Parser's framework
 */

/*
 * returning type for prslextype method of parser
 */
#[repr(C)]
pub struct LexDescr {
    pub lexid: c_int,
    pub alias: *mut c_char,
    pub descr: *mut c_char,
}

/*
 * Interface to headline generator (tsparser's prsheadline function)
 *
 * The original C struct packs several bitfields into a single uint32.
 * We pack them into one backing uint32 `bits` with accessor methods.
 *
 * Bit layout (matching the C declaration order, low bits first):
 *   selected:1, in:1, replace:1, repeated:1, skip:1, unused:3, type:8, len:16
 */
#[repr(C)]
pub struct HeadlineWordEntry {
    /// Packed bitfield: selected:1, in:1, replace:1, repeated:1, skip:1, unused:3, type:8, len:16
    pub bits: uint32,
    pub pos: WordEntryPos, // position of token
    pub word: *mut c_char, // text of token (not null-terminated)
    pub item: *mut QueryOperand, // a matching query operand, or NULL if none
}

impl HeadlineWordEntry {
    #[inline]
    pub fn selected(&self) -> uint32 {
        self.bits & 0x1
    }
    #[inline]
    pub fn set_selected(&mut self, v: uint32) {
        self.bits = (self.bits & !0x1) | (v & 0x1);
    }

    #[inline]
    pub fn r#in(&self) -> uint32 {
        (self.bits >> 1) & 0x1
    }
    #[inline]
    pub fn set_in(&mut self, v: uint32) {
        self.bits = (self.bits & !(0x1 << 1)) | ((v & 0x1) << 1);
    }

    #[inline]
    pub fn replace(&self) -> uint32 {
        (self.bits >> 2) & 0x1
    }
    #[inline]
    pub fn set_replace(&mut self, v: uint32) {
        self.bits = (self.bits & !(0x1 << 2)) | ((v & 0x1) << 2);
    }

    #[inline]
    pub fn repeated(&self) -> uint32 {
        (self.bits >> 3) & 0x1
    }
    #[inline]
    pub fn set_repeated(&mut self, v: uint32) {
        self.bits = (self.bits & !(0x1 << 3)) | ((v & 0x1) << 3);
    }

    #[inline]
    pub fn skip(&self) -> uint32 {
        (self.bits >> 4) & 0x1
    }
    #[inline]
    pub fn set_skip(&mut self, v: uint32) {
        self.bits = (self.bits & !(0x1 << 4)) | ((v & 0x1) << 4);
    }

    #[inline]
    pub fn unused(&self) -> uint32 {
        (self.bits >> 5) & 0x7
    }
    #[inline]
    pub fn set_unused(&mut self, v: uint32) {
        self.bits = (self.bits & !(0x7 << 5)) | ((v & 0x7) << 5);
    }

    #[inline]
    pub fn r#type(&self) -> uint32 {
        (self.bits >> 8) & 0xFF
    }
    #[inline]
    pub fn set_type(&mut self, v: uint32) {
        self.bits = (self.bits & !(0xFF << 8)) | ((v & 0xFF) << 8);
    }

    #[inline]
    pub fn len(&self) -> uint32 {
        (self.bits >> 16) & 0xFFFF
    }
    #[inline]
    pub fn set_len(&mut self, v: uint32) {
        self.bits = (self.bits & !(0xFFFF << 16)) | ((v & 0xFFFF) << 16);
    }
}

#[repr(C)]
pub struct HeadlineParsedText {
    /* Fields filled by core code before calling prsheadline function: */
    pub words: *mut HeadlineWordEntry,
    pub lenwords: int32, // allocated length of words[]
    pub curwords: int32, // current number of valid entries
    pub vectorpos: int32, // used by ts_parse.c in filling pos fields

    /* The prsheadline function must fill these fields: */
    /* Strings for marking selected tokens and separating fragments: */
    pub startsel: *mut c_char, // palloc'd strings
    pub stopsel: *mut c_char,
    pub fragdelim: *mut c_char,
    pub startsellen: int16, // lengths of strings
    pub stopsellen: int16,
    pub fragdelimlen: int16,
}

/*
 * Common useful things for tsearch subsystem
 */
pub unsafe fn get_tsearch_config_filename(
    basename: *const c_char,
    extension: *const c_char,
) -> *mut c_char {
    unimplemented!()
}

/*
 * Often useful stopword list management
 */
#[repr(C)]
pub struct StopList {
    pub len: c_int,
    pub stop: *mut *mut c_char,
}

pub unsafe fn readstoplist(
    fname: *const c_char,
    s: *mut StopList,
    wordop: Option<unsafe extern "C" fn(*const c_char, Size, Oid) -> *mut c_char>,
) {
    unimplemented!()
}

pub unsafe fn searchstoplist(s: *mut StopList, key: *mut c_char) -> bool {
    unimplemented!()
}

/*
 * Interface with dictionaries
 */

/* return struct for any lexize function */
#[repr(C)]
pub struct TSLexeme {
    /*
     * Number of current variant of split word.  See ts_public.h for the
     * detailed explanation of how nvariant groups lexemes into split
     * variants.
     */
    pub nvariant: uint16,

    pub flags: uint16, // See flag bits below

    pub lexeme: *mut c_char, // C string
}

/* Flag bits that can appear in TSLexeme.flags */
pub const TSL_ADDPOS: c_int = 0x01;
pub const TSL_PREFIX: c_int = 0x02;
pub const TSL_FILTER: c_int = 0x04;

/*
 * Struct for supporting complex dictionaries like thesaurus.
 * 4th argument for dictlexize method is a pointer to this
 */
#[repr(C)]
pub struct DictSubState {
    pub isend: bool, // in: marks for lexize_info about text end is reached
    pub getnext: bool, // out: dict wants next lexeme
    pub private_state: *mut std::ffi::c_void, // internal dict state between calls with getnext == true
}
