//! Translated from PostgreSQL src/include/tsearch/ts_public.h

use bitflags::bitflags;

use crate::postgres_ext::Oid;
use crate::tsearch::ts_type::{QueryOperand, WordEntryPos};

/// `LexDescr` - return type for a parser's prslextype method.
pub struct LexDescr {
    pub lexid: i32,
    pub alias: Option<String>,
    pub descr: Option<String>,
}

/// `HeadlineWordEntry` - one token of headline text. The leading bitfield word
/// (`selected:1|in:1|replace:1|repeated:1|skip:1|unused:3|type:8|len:16`) packs
/// flags beside numeric type/len fields, so per bitflags appendix C it is a raw
/// u32 with accessors rather than a flag set.
pub struct HeadlineWordEntry {
    pub bits: HeadlineWordBits,
    pub pos: WordEntryPos,
    pub word: *mut u8, // not null-terminated. TODO(ptr)
    pub item: *mut QueryOperand, // a matching query operand, or null. TODO(ptr)
}

/// Packed flags + type/len word of HeadlineWordEntry.
#[repr(transparent)]
pub struct HeadlineWordBits(pub u32);

impl HeadlineWordBits {
    pub const fn selected(self) -> bool {
        self.0 & 0x1 != 0
    }
    pub const fn is_in(self) -> bool {
        (self.0 >> 1) & 0x1 != 0
    }
    pub const fn replace(self) -> bool {
        (self.0 >> 2) & 0x1 != 0
    }
    pub const fn repeated(self) -> bool {
        (self.0 >> 3) & 0x1 != 0
    }
    pub const fn skip(self) -> bool {
        (self.0 >> 4) & 0x1 != 0
    }
    pub const fn type_(self) -> u8 {
        ((self.0 >> 8) & 0xff) as u8
    }
    #[allow(clippy::len_without_is_empty, reason = "mirrors PG length accessor; is_empty not part of PG API")]
    pub const fn len(self) -> u16 {
        ((self.0 >> 16) & 0xffff) as u16
    }
}

/// `HeadlineParsedText` - text to be highlighted by a parser's prsheadline.
pub struct HeadlineParsedText {
    // Filled by core code before calling prsheadline:
    pub words: *mut HeadlineWordEntry, // array of length lenwords. TODO(ptr)
    pub lenwords: i32,
    pub curwords: i32,
    pub vectorpos: i32,

    // Filled by the prsheadline function:
    pub startsel: Option<String>,
    pub stopsel: Option<String>,
    pub fragdelim: Option<String>,
    pub startsellen: i16,
    pub stopsellen: i16,
    pub fragdelimlen: i16,
}

/// `get_tsearch_config_filename` - build a tsearch config file path.
pub fn get_tsearch_config_filename(_basename: &str, _extension: &str) -> String {
    unimplemented!()
}

/// `StopList` - a stopword list.
pub struct StopList {
    pub len: i32,
    pub stop: Vec<String>,
}

/// `readstoplist` - load a stopword file into `s`; `wordop` normalizes each word.
pub fn readstoplist(_fname: &str, _s: &mut StopList, _wordop: impl Fn(&str, usize, Oid) -> String) {
    unimplemented!()
}

/// `searchstoplist` - test whether `key` is in the stoplist.
pub fn searchstoplist(_s: &StopList, _key: &str) -> bool {
    unimplemented!()
}

/// `TSLexeme` - return struct for any lexize function.
pub struct TSLexeme {
    pub nvariant: u16,
    pub flags: TslFlags,
    pub lexeme: Option<String>, // C string
}

bitflags! {
    /// `TSL_*` - flag bits that can appear in TSLexeme.flags.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct TslFlags: u16 {
        const ADDPOS = 0x01;
        const PREFIX = 0x02;
        const FILTER = 0x04;
    }
}

/// `DictSubState` - state passed to dictlexize for complex dictionaries.
pub struct DictSubState {
    pub isend: bool,  // in: text end reached
    pub getnext: bool, // out: dict wants next lexeme
    pub private_state: *mut core::ffi::c_void, // internal dict state. TODO(ptr)
}
