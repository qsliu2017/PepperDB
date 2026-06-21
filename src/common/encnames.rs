//! Encoding names and routines for working with them.
//!
//! Translated from:
//!   - src/common/encnames.c
//!   - declarations live in src/include/mb/pg_wchar.h (translated in
//!     src/mb/wchar.rs)
//!
//! Portions Copyright (c) 2001-2025, PostgreSQL Global Development Group

use crate::prelude::*;
// NAMEDATALEN lives in pg_config.rs; the prelude pulls in c.rs but not the raw
// configure-time constants, so name it explicitly.
use crate::pg_config::NAMEDATALEN;
// pg_enc, pg_enc2name and the validity tests/macros all come from the encoding
// layer. pg_enc's variants are re-exported via `pub use pg_enc::*` in wchar.rs,
// so a glob import brings the bare `PG_UTF8`, `PG_LATIN1`, ... names into scope
// exactly as the C source uses them.
use crate::mb::wchar::*;

/* ----------
 * All encoding names, sorted:		 *** A L P H A B E T I C ***
 *
 * All names must be without irrelevant chars, search routines use
 * isalnum() chars only. It means ISO-8859-1, iso_8859-1 and Iso8859_1
 * are always converted to 'iso88591'. All must be lower case.
 *
 * The table doesn't contain 'cs' aliases (like csISOLatin1). It's needed?
 *
 * Karel Zak, Aug 2001
 * ----------
 */
#[repr(C)]
pub struct pg_encname {
    pub name: *const c_char,
    pub encoding: pg_enc,
}

// The `name` fields are pointers into 'static byte-string literals, so the table
// is sound to share across threads; assert that to satisfy `static`'s Sync bound
// without weakening the type.
unsafe impl Sync for pg_encname {}

/// Build a `pg_encname` entry from a NUL-terminated byte string and an encoding.
const fn ENCNAME(name: &'static [u8], encoding: pg_enc) -> pg_encname {
    pg_encname {
        name: name.as_ptr() as *const c_char,
        encoding,
    }
}

pub static pg_encname_tbl: [pg_encname; 81] = [
    ENCNAME(b"abc\0", PG_WIN1258),                  /* alias for WIN1258 */
    ENCNAME(b"alt\0", PG_WIN866),                   /* IBM866 */
    ENCNAME(b"big5\0", PG_BIG5),                    /* Big5; Chinese for Taiwan multibyte set */
    ENCNAME(b"euccn\0", PG_EUC_CN),                 /* EUC-CN; Extended Unix Code for simplified
                                                     * Chinese */
    ENCNAME(b"eucjis2004\0", PG_EUC_JIS_2004),      /* EUC-JIS-2004; Extended UNIX Code fixed
                                                     * Width for Japanese, standard JIS X 0213 */
    ENCNAME(b"eucjp\0", PG_EUC_JP),                 /* EUC-JP; Extended UNIX Code fixed Width for
                                                     * Japanese, standard OSF */
    ENCNAME(b"euckr\0", PG_EUC_KR),                 /* EUC-KR; Extended Unix Code for Korean , KS
                                                     * X 1001 standard */
    ENCNAME(b"euctw\0", PG_EUC_TW),                 /* EUC-TW; Extended Unix Code for
                                                     *
                                                     * traditional Chinese */
    ENCNAME(b"gb18030\0", PG_GB18030),              /* GB18030;GB18030 */
    ENCNAME(b"gbk\0", PG_GBK),                      /* GBK; Chinese Windows CodePage 936
                                                     * simplified Chinese */
    ENCNAME(b"iso88591\0", PG_LATIN1),              /* ISO-8859-1; RFC1345,KXS2 */
    ENCNAME(b"iso885910\0", PG_LATIN6),             /* ISO-8859-10; RFC1345,KXS2 */
    ENCNAME(b"iso885913\0", PG_LATIN7),             /* ISO-8859-13; RFC1345,KXS2 */
    ENCNAME(b"iso885914\0", PG_LATIN8),             /* ISO-8859-14; RFC1345,KXS2 */
    ENCNAME(b"iso885915\0", PG_LATIN9),             /* ISO-8859-15; RFC1345,KXS2 */
    ENCNAME(b"iso885916\0", PG_LATIN10),            /* ISO-8859-16; RFC1345,KXS2 */
    ENCNAME(b"iso88592\0", PG_LATIN2),              /* ISO-8859-2; RFC1345,KXS2 */
    ENCNAME(b"iso88593\0", PG_LATIN3),              /* ISO-8859-3; RFC1345,KXS2 */
    ENCNAME(b"iso88594\0", PG_LATIN4),              /* ISO-8859-4; RFC1345,KXS2 */
    ENCNAME(b"iso88595\0", PG_ISO_8859_5),          /* ISO-8859-5; RFC1345,KXS2 */
    ENCNAME(b"iso88596\0", PG_ISO_8859_6),          /* ISO-8859-6; RFC1345,KXS2 */
    ENCNAME(b"iso88597\0", PG_ISO_8859_7),          /* ISO-8859-7; RFC1345,KXS2 */
    ENCNAME(b"iso88598\0", PG_ISO_8859_8),          /* ISO-8859-8; RFC1345,KXS2 */
    ENCNAME(b"iso88599\0", PG_LATIN5),              /* ISO-8859-9; RFC1345,KXS2 */
    ENCNAME(b"johab\0", PG_JOHAB),                  /* JOHAB; Extended Unix Code for simplified
                                                     * Chinese */
    ENCNAME(b"koi8\0", PG_KOI8R),                   /* _dirty_ alias for KOI8-R (backward
                                                     * compatibility) */
    ENCNAME(b"koi8r\0", PG_KOI8R),                  /* KOI8-R; RFC1489 */
    ENCNAME(b"koi8u\0", PG_KOI8U),                  /* KOI8-U; RFC2319 */
    ENCNAME(b"latin1\0", PG_LATIN1),                /* alias for ISO-8859-1 */
    ENCNAME(b"latin10\0", PG_LATIN10),              /* alias for ISO-8859-16 */
    ENCNAME(b"latin2\0", PG_LATIN2),                /* alias for ISO-8859-2 */
    ENCNAME(b"latin3\0", PG_LATIN3),                /* alias for ISO-8859-3 */
    ENCNAME(b"latin4\0", PG_LATIN4),                /* alias for ISO-8859-4 */
    ENCNAME(b"latin5\0", PG_LATIN5),                /* alias for ISO-8859-9 */
    ENCNAME(b"latin6\0", PG_LATIN6),                /* alias for ISO-8859-10 */
    ENCNAME(b"latin7\0", PG_LATIN7),                /* alias for ISO-8859-13 */
    ENCNAME(b"latin8\0", PG_LATIN8),                /* alias for ISO-8859-14 */
    ENCNAME(b"latin9\0", PG_LATIN9),                /* alias for ISO-8859-15 */
    ENCNAME(b"mskanji\0", PG_SJIS),                 /* alias for Shift_JIS */
    ENCNAME(b"muleinternal\0", PG_MULE_INTERNAL),
    ENCNAME(b"shiftjis\0", PG_SJIS),                /* Shift_JIS; JIS X 0202-1991 */
    ENCNAME(b"shiftjis2004\0", PG_SHIFT_JIS_2004),  /* SHIFT-JIS-2004; Shift JIS for Japanese,
                                                     * standard JIS X 0213 */
    ENCNAME(b"sjis\0", PG_SJIS),                    /* alias for Shift_JIS */
    ENCNAME(b"sqlascii\0", PG_SQL_ASCII),
    ENCNAME(b"tcvn\0", PG_WIN1258),                 /* alias for WIN1258 */
    ENCNAME(b"tcvn5712\0", PG_WIN1258),             /* alias for WIN1258 */
    ENCNAME(b"uhc\0", PG_UHC),                      /* UHC; Korean Windows CodePage 949 */
    ENCNAME(b"unicode\0", PG_UTF8),                 /* alias for UTF8 */
    ENCNAME(b"utf8\0", PG_UTF8),                    /* alias for UTF8 */
    ENCNAME(b"vscii\0", PG_WIN1258),                /* alias for WIN1258 */
    ENCNAME(b"win\0", PG_WIN1251),                  /* _dirty_ alias for windows-1251 (backward
                                                     * compatibility) */
    ENCNAME(b"win1250\0", PG_WIN1250),              /* alias for Windows-1250 */
    ENCNAME(b"win1251\0", PG_WIN1251),              /* alias for Windows-1251 */
    ENCNAME(b"win1252\0", PG_WIN1252),              /* alias for Windows-1252 */
    ENCNAME(b"win1253\0", PG_WIN1253),              /* alias for Windows-1253 */
    ENCNAME(b"win1254\0", PG_WIN1254),              /* alias for Windows-1254 */
    ENCNAME(b"win1255\0", PG_WIN1255),              /* alias for Windows-1255 */
    ENCNAME(b"win1256\0", PG_WIN1256),              /* alias for Windows-1256 */
    ENCNAME(b"win1257\0", PG_WIN1257),              /* alias for Windows-1257 */
    ENCNAME(b"win1258\0", PG_WIN1258),              /* alias for Windows-1258 */
    ENCNAME(b"win866\0", PG_WIN866),                /* IBM866 */
    ENCNAME(b"win874\0", PG_WIN874),                /* alias for Windows-874 */
    ENCNAME(b"win932\0", PG_SJIS),                  /* alias for Shift_JIS */
    ENCNAME(b"win936\0", PG_GBK),                   /* alias for GBK */
    ENCNAME(b"win949\0", PG_UHC),                   /* alias for UHC */
    ENCNAME(b"win950\0", PG_BIG5),                  /* alias for BIG5 */
    ENCNAME(b"windows1250\0", PG_WIN1250),          /* Windows-1251; Microsoft */
    ENCNAME(b"windows1251\0", PG_WIN1251),          /* Windows-1251; Microsoft */
    ENCNAME(b"windows1252\0", PG_WIN1252),          /* Windows-1252; Microsoft */
    ENCNAME(b"windows1253\0", PG_WIN1253),          /* Windows-1253; Microsoft */
    ENCNAME(b"windows1254\0", PG_WIN1254),          /* Windows-1254; Microsoft */
    ENCNAME(b"windows1255\0", PG_WIN1255),          /* Windows-1255; Microsoft */
    ENCNAME(b"windows1256\0", PG_WIN1256),          /* Windows-1256; Microsoft */
    ENCNAME(b"windows1257\0", PG_WIN1257),          /* Windows-1257; Microsoft */
    ENCNAME(b"windows1258\0", PG_WIN1258),          /* Windows-1258; Microsoft */
    ENCNAME(b"windows866\0", PG_WIN866),            /* IBM866 */
    ENCNAME(b"windows874\0", PG_WIN874),            /* Windows-874; Microsoft */
    ENCNAME(b"windows932\0", PG_SJIS),              /* alias for Shift_JIS */
    ENCNAME(b"windows936\0", PG_GBK),               /* alias for GBK */
    ENCNAME(b"windows949\0", PG_UHC),               /* alias for UHC */
    ENCNAME(b"windows950\0", PG_BIG5),              /* alias for BIG5 */
];

/* ----------
 * These are "official" encoding names.
 *
 * In C the table is built with the DEF_ENC2NAME(name, codepage) macro
 * (`{ #name, PG_##name }` on non-WIN32 builds; the WIN32 build appends the
 * codepage). The codepage member is omitted in the wchar.rs `pg_enc2name`
 * struct (non-WIN32 build), so the codepage arguments preserved below are
 * documentary only.
 * ----------
 */

/// Build a `pg_enc2name` entry from a NUL-terminated byte string and an encoding.
const fn ENC2NAME(name: &'static [u8], encoding: pg_enc) -> pg_enc2name {
    pg_enc2name {
        name: name.as_ptr() as *const c_char,
        encoding,
    }
}

// As with pg_encname, every `name` points at a 'static literal.
unsafe impl Sync for pg_enc2name {}

// The C source uses C99 designated initializers (`[PG_SQL_ASCII] = ...`); the
// entries below are written in pg_enc discriminant order (PG_SQL_ASCII = 0 ..
// PG_SHIFT_JIS_2004), which is dense, so this positional array is equivalent.
#[no_mangle]
pub static pg_enc2name_tbl: [pg_enc2name; pg_enc::_PG_LAST_ENCODING_ as usize] = [
    /* [PG_SQL_ASCII] */ ENC2NAME(b"SQL_ASCII\0", PG_SQL_ASCII), /* codepage 0 */
    /* [PG_EUC_JP] */ ENC2NAME(b"EUC_JP\0", PG_EUC_JP),          /* codepage 20932 */
    /* [PG_EUC_CN] */ ENC2NAME(b"EUC_CN\0", PG_EUC_CN),          /* codepage 20936 */
    /* [PG_EUC_KR] */ ENC2NAME(b"EUC_KR\0", PG_EUC_KR),          /* codepage 51949 */
    /* [PG_EUC_TW] */ ENC2NAME(b"EUC_TW\0", PG_EUC_TW),          /* codepage 0 */
    /* [PG_EUC_JIS_2004] */ ENC2NAME(b"EUC_JIS_2004\0", PG_EUC_JIS_2004), /* codepage 20932 */
    /* [PG_UTF8] */ ENC2NAME(b"UTF8\0", PG_UTF8),                /* codepage 65001 */
    /* [PG_MULE_INTERNAL] */ ENC2NAME(b"MULE_INTERNAL\0", PG_MULE_INTERNAL), /* codepage 0 */
    /* [PG_LATIN1] */ ENC2NAME(b"LATIN1\0", PG_LATIN1),          /* codepage 28591 */
    /* [PG_LATIN2] */ ENC2NAME(b"LATIN2\0", PG_LATIN2),          /* codepage 28592 */
    /* [PG_LATIN3] */ ENC2NAME(b"LATIN3\0", PG_LATIN3),          /* codepage 28593 */
    /* [PG_LATIN4] */ ENC2NAME(b"LATIN4\0", PG_LATIN4),          /* codepage 28594 */
    /* [PG_LATIN5] */ ENC2NAME(b"LATIN5\0", PG_LATIN5),          /* codepage 28599 */
    /* [PG_LATIN6] */ ENC2NAME(b"LATIN6\0", PG_LATIN6),          /* codepage 0 */
    /* [PG_LATIN7] */ ENC2NAME(b"LATIN7\0", PG_LATIN7),          /* codepage 0 */
    /* [PG_LATIN8] */ ENC2NAME(b"LATIN8\0", PG_LATIN8),          /* codepage 0 */
    /* [PG_LATIN9] */ ENC2NAME(b"LATIN9\0", PG_LATIN9),          /* codepage 28605 */
    /* [PG_LATIN10] */ ENC2NAME(b"LATIN10\0", PG_LATIN10),       /* codepage 0 */
    /* [PG_WIN1256] */ ENC2NAME(b"WIN1256\0", PG_WIN1256),       /* codepage 1256 */
    /* [PG_WIN1258] */ ENC2NAME(b"WIN1258\0", PG_WIN1258),       /* codepage 1258 */
    /* [PG_WIN866] */ ENC2NAME(b"WIN866\0", PG_WIN866),          /* codepage 866 */
    /* [PG_WIN874] */ ENC2NAME(b"WIN874\0", PG_WIN874),          /* codepage 874 */
    /* [PG_KOI8R] */ ENC2NAME(b"KOI8R\0", PG_KOI8R),             /* codepage 20866 */
    /* [PG_WIN1251] */ ENC2NAME(b"WIN1251\0", PG_WIN1251),       /* codepage 1251 */
    /* [PG_WIN1252] */ ENC2NAME(b"WIN1252\0", PG_WIN1252),       /* codepage 1252 */
    /* [PG_ISO_8859_5] */ ENC2NAME(b"ISO_8859_5\0", PG_ISO_8859_5), /* codepage 28595 */
    /* [PG_ISO_8859_6] */ ENC2NAME(b"ISO_8859_6\0", PG_ISO_8859_6), /* codepage 28596 */
    /* [PG_ISO_8859_7] */ ENC2NAME(b"ISO_8859_7\0", PG_ISO_8859_7), /* codepage 28597 */
    /* [PG_ISO_8859_8] */ ENC2NAME(b"ISO_8859_8\0", PG_ISO_8859_8), /* codepage 28598 */
    /* [PG_WIN1250] */ ENC2NAME(b"WIN1250\0", PG_WIN1250),       /* codepage 1250 */
    /* [PG_WIN1253] */ ENC2NAME(b"WIN1253\0", PG_WIN1253),       /* codepage 1253 */
    /* [PG_WIN1254] */ ENC2NAME(b"WIN1254\0", PG_WIN1254),       /* codepage 1254 */
    /* [PG_WIN1255] */ ENC2NAME(b"WIN1255\0", PG_WIN1255),       /* codepage 1255 */
    /* [PG_WIN1257] */ ENC2NAME(b"WIN1257\0", PG_WIN1257),       /* codepage 1257 */
    /* [PG_KOI8U] */ ENC2NAME(b"KOI8U\0", PG_KOI8U),             /* codepage 21866 */
    /* [PG_SJIS] */ ENC2NAME(b"SJIS\0", PG_SJIS),               /* codepage 932 */
    /* [PG_BIG5] */ ENC2NAME(b"BIG5\0", PG_BIG5),               /* codepage 950 */
    /* [PG_GBK] */ ENC2NAME(b"GBK\0", PG_GBK),                  /* codepage 936 */
    /* [PG_UHC] */ ENC2NAME(b"UHC\0", PG_UHC),                  /* codepage 949 */
    /* [PG_GB18030] */ ENC2NAME(b"GB18030\0", PG_GB18030),       /* codepage 54936 */
    /* [PG_JOHAB] */ ENC2NAME(b"JOHAB\0", PG_JOHAB),            /* codepage 0 */
    /* [PG_SHIFT_JIS_2004] */ ENC2NAME(b"SHIFT_JIS_2004\0", PG_SHIFT_JIS_2004), /* codepage 932 */
];

/* ----------
 * These are encoding names for gettext.
 *
 * This covers all encodings except MULE_INTERNAL, which is alien to gettext.
 * ----------
 */
// A `static` array of raw C-string pointers isn't `Sync` (raw pointers aren't),
// so wrap such tables in this newtype; every pointer is a 'static string literal.
#[repr(transparent)]
pub struct SyncCStrArr<const N: usize>(pub [*const c_char; N]);
unsafe impl<const N: usize> Sync for SyncCStrArr<N> {}

// As above, written positionally in pg_enc discriminant order. NULL entries
// (MULE_INTERNAL) become a null pointer.
pub static pg_enc2gettext_tbl: SyncCStrArr<{ pg_enc::_PG_LAST_ENCODING_ as usize }> = SyncCStrArr([
    /* [PG_SQL_ASCII] */ b"US-ASCII\0".as_ptr() as *const c_char,
    /* [PG_EUC_JP] */ b"EUC-JP\0".as_ptr() as *const c_char,
    /* [PG_EUC_CN] */ b"EUC-CN\0".as_ptr() as *const c_char,
    /* [PG_EUC_KR] */ b"EUC-KR\0".as_ptr() as *const c_char,
    /* [PG_EUC_TW] */ b"EUC-TW\0".as_ptr() as *const c_char,
    /* [PG_EUC_JIS_2004] */ b"EUC-JP\0".as_ptr() as *const c_char,
    /* [PG_UTF8] */ b"UTF-8\0".as_ptr() as *const c_char,
    /* [PG_MULE_INTERNAL] */ null(),
    /* [PG_LATIN1] */ b"LATIN1\0".as_ptr() as *const c_char,
    /* [PG_LATIN2] */ b"LATIN2\0".as_ptr() as *const c_char,
    /* [PG_LATIN3] */ b"LATIN3\0".as_ptr() as *const c_char,
    /* [PG_LATIN4] */ b"LATIN4\0".as_ptr() as *const c_char,
    /* [PG_LATIN5] */ b"LATIN5\0".as_ptr() as *const c_char,
    /* [PG_LATIN6] */ b"LATIN6\0".as_ptr() as *const c_char,
    /* [PG_LATIN7] */ b"LATIN7\0".as_ptr() as *const c_char,
    /* [PG_LATIN8] */ b"LATIN8\0".as_ptr() as *const c_char,
    /* [PG_LATIN9] */ b"LATIN-9\0".as_ptr() as *const c_char,
    /* [PG_LATIN10] */ b"LATIN10\0".as_ptr() as *const c_char,
    /* [PG_WIN1256] */ b"CP1256\0".as_ptr() as *const c_char,
    /* [PG_WIN1258] */ b"CP1258\0".as_ptr() as *const c_char,
    /* [PG_WIN866] */ b"CP866\0".as_ptr() as *const c_char,
    /* [PG_WIN874] */ b"CP874\0".as_ptr() as *const c_char,
    /* [PG_KOI8R] */ b"KOI8-R\0".as_ptr() as *const c_char,
    /* [PG_WIN1251] */ b"CP1251\0".as_ptr() as *const c_char,
    /* [PG_WIN1252] */ b"CP1252\0".as_ptr() as *const c_char,
    /* [PG_ISO_8859_5] */ b"ISO-8859-5\0".as_ptr() as *const c_char,
    /* [PG_ISO_8859_6] */ b"ISO_8859-6\0".as_ptr() as *const c_char,
    /* [PG_ISO_8859_7] */ b"ISO-8859-7\0".as_ptr() as *const c_char,
    /* [PG_ISO_8859_8] */ b"ISO-8859-8\0".as_ptr() as *const c_char,
    /* [PG_WIN1250] */ b"CP1250\0".as_ptr() as *const c_char,
    /* [PG_WIN1253] */ b"CP1253\0".as_ptr() as *const c_char,
    /* [PG_WIN1254] */ b"CP1254\0".as_ptr() as *const c_char,
    /* [PG_WIN1255] */ b"CP1255\0".as_ptr() as *const c_char,
    /* [PG_WIN1257] */ b"CP1257\0".as_ptr() as *const c_char,
    /* [PG_KOI8U] */ b"KOI8-U\0".as_ptr() as *const c_char,
    /* [PG_SJIS] */ b"SHIFT-JIS\0".as_ptr() as *const c_char,
    /* [PG_BIG5] */ b"BIG5\0".as_ptr() as *const c_char,
    /* [PG_GBK] */ b"GBK\0".as_ptr() as *const c_char,
    /* [PG_UHC] */ b"UHC\0".as_ptr() as *const c_char,
    /* [PG_GB18030] */ b"GB18030\0".as_ptr() as *const c_char,
    /* [PG_JOHAB] */ b"JOHAB\0".as_ptr() as *const c_char,
    /* [PG_SHIFT_JIS_2004] */ b"SHIFT_JISX0213\0".as_ptr() as *const c_char,
]);

/*
 * Table of encoding names for ICU (currently covers backend encodings only)
 *
 * Reference: <https://ssl.icu-project.org/icu-bin/convexp>
 *
 * NULL entries are not supported by ICU, or their mapping is unclear.
 */
// Backend-only: length is PG_ENCODING_BE_LAST + 1, written positionally in
// pg_enc discriminant order (PG_SQL_ASCII .. PG_KOI8U).
static pg_enc2icu_tbl: SyncCStrArr<{ (PG_ENCODING_BE_LAST + 1) as usize }> = SyncCStrArr([
    /* [PG_SQL_ASCII] */ null(),
    /* [PG_EUC_JP] */ b"EUC-JP\0".as_ptr() as *const c_char,
    /* [PG_EUC_CN] */ b"EUC-CN\0".as_ptr() as *const c_char,
    /* [PG_EUC_KR] */ b"EUC-KR\0".as_ptr() as *const c_char,
    /* [PG_EUC_TW] */ b"EUC-TW\0".as_ptr() as *const c_char,
    /* [PG_EUC_JIS_2004] */ null(),
    /* [PG_UTF8] */ b"UTF-8\0".as_ptr() as *const c_char,
    /* [PG_MULE_INTERNAL] */ null(),
    /* [PG_LATIN1] */ b"ISO-8859-1\0".as_ptr() as *const c_char,
    /* [PG_LATIN2] */ b"ISO-8859-2\0".as_ptr() as *const c_char,
    /* [PG_LATIN3] */ b"ISO-8859-3\0".as_ptr() as *const c_char,
    /* [PG_LATIN4] */ b"ISO-8859-4\0".as_ptr() as *const c_char,
    /* [PG_LATIN5] */ b"ISO-8859-9\0".as_ptr() as *const c_char,
    /* [PG_LATIN6] */ b"ISO-8859-10\0".as_ptr() as *const c_char,
    /* [PG_LATIN7] */ b"ISO-8859-13\0".as_ptr() as *const c_char,
    /* [PG_LATIN8] */ b"ISO-8859-14\0".as_ptr() as *const c_char,
    /* [PG_LATIN9] */ b"ISO-8859-15\0".as_ptr() as *const c_char,
    /* [PG_LATIN10] */ null(),
    /* [PG_WIN1256] */ b"CP1256\0".as_ptr() as *const c_char,
    /* [PG_WIN1258] */ b"CP1258\0".as_ptr() as *const c_char,
    /* [PG_WIN866] */ b"CP866\0".as_ptr() as *const c_char,
    /* [PG_WIN874] */ null(),
    /* [PG_KOI8R] */ b"KOI8-R\0".as_ptr() as *const c_char,
    /* [PG_WIN1251] */ b"CP1251\0".as_ptr() as *const c_char,
    /* [PG_WIN1252] */ b"CP1252\0".as_ptr() as *const c_char,
    /* [PG_ISO_8859_5] */ b"ISO-8859-5\0".as_ptr() as *const c_char,
    /* [PG_ISO_8859_6] */ b"ISO-8859-6\0".as_ptr() as *const c_char,
    /* [PG_ISO_8859_7] */ b"ISO-8859-7\0".as_ptr() as *const c_char,
    /* [PG_ISO_8859_8] */ b"ISO-8859-8\0".as_ptr() as *const c_char,
    /* [PG_WIN1250] */ b"CP1250\0".as_ptr() as *const c_char,
    /* [PG_WIN1253] */ b"CP1253\0".as_ptr() as *const c_char,
    /* [PG_WIN1254] */ b"CP1254\0".as_ptr() as *const c_char,
    /* [PG_WIN1255] */ b"CP1255\0".as_ptr() as *const c_char,
    /* [PG_WIN1257] */ b"CP1257\0".as_ptr() as *const c_char,
    /* [PG_KOI8U] */ b"KOI8-U\0".as_ptr() as *const c_char,
]);

// StaticAssertDecl(lengthof(pg_enc2icu_tbl) == PG_ENCODING_BE_LAST + 1,
//                  "pg_enc2icu_tbl incomplete");
const _: () = assert!(
    pg_enc2icu_tbl.0.len() == (PG_ENCODING_BE_LAST + 1) as usize,
    "pg_enc2icu_tbl incomplete"
);

/*
 * Is this encoding supported by ICU?
 */
pub fn is_encoding_supported_by_icu(encoding: c_int) -> bool {
    if !PG_VALID_BE_ENCODING(encoding) {
        return false;
    }
    !pg_enc2icu_tbl.0[encoding as usize].is_null()
}

/*
 * Returns ICU's name for encoding, or NULL if not supported
 */
pub fn get_encoding_name_for_icu(encoding: c_int) -> *const c_char {
    if !PG_VALID_BE_ENCODING(encoding) {
        return null();
    }
    pg_enc2icu_tbl.0[encoding as usize]
}

/* ----------
 * Encoding checks, for error returns -1 else encoding id
 * ----------
 */
pub unsafe fn pg_valid_client_encoding(name: *const c_char) -> c_int {
    let enc: c_int;

    enc = pg_char_to_encoding(name);
    if enc < 0 {
        return -1;
    }

    if !PG_VALID_FE_ENCODING(enc) {
        return -1;
    }

    enc
}

#[no_mangle]
pub unsafe fn pg_valid_server_encoding(name: *const c_char) -> c_int {
    let enc: c_int;

    enc = pg_char_to_encoding(name);
    if enc < 0 {
        return -1;
    }

    if !PG_VALID_BE_ENCODING(enc) {
        return -1;
    }

    enc
}

pub fn pg_valid_server_encoding_id(encoding: c_int) -> c_int {
    PG_VALID_BE_ENCODING(encoding) as c_int
}

/*
 * Remove irrelevant chars from encoding name, store at *newkey
 *
 * (Caller's responsibility to provide a large enough buffer)
 */
unsafe fn clean_encoding_name(key: *const c_char, newkey: *mut c_char) -> *mut c_char {
    let mut p: *const c_char;
    let mut np: *mut c_char;

    p = key;
    np = newkey;
    while *p != b'\0' as c_char {
        /* isalnum((unsigned char) *p) -- ASCII alnum, locale-independent */
        let c = *p as c_uchar;
        if c.is_ascii_alphanumeric() {
            if *p >= b'A' as c_char && *p <= b'Z' as c_char {
                *np = *p + (b'a' as c_char) - (b'A' as c_char);
                np = np.add(1);
            } else {
                *np = *p;
                np = np.add(1);
            }
        }
        p = p.add(1);
    }
    *np = b'\0' as c_char;
    newkey
}

/*
 * Search encoding by encoding name
 *
 * Returns encoding ID, or -1 if not recognized
 */
pub unsafe fn pg_char_to_encoding(name: *const c_char) -> c_int {
    let nel: c_uint = lengthof!(pg_encname_tbl) as c_uint;
    // C uses three `const pg_encname *` cursors (base/last/position) and walks
    // them with pointer arithmetic. We track the same window with signed element
    // indices to avoid forming a pointer one-before-the-start (which is UB in
    // Rust): when `result < 0` at index 0, C sets `last = position - 1`, i.e. an
    // out-of-range cursor that ends the loop. `isize` lets `last` go to -1 and
    // terminate the `last >= base` test exactly as C does.
    let mut base: isize = 0;
    let mut last: isize = nel as isize - 1;
    let mut position: isize;
    let mut result: c_int;
    let mut buff: [c_char; NAMEDATALEN] = [0; NAMEDATALEN];
    let key: *mut c_char;

    if name.is_null() || *name == b'\0' as c_char {
        return -1;
    }

    if strlen(name) >= NAMEDATALEN {
        return -1; /* it's certainly not in the table */
    }

    key = clean_encoding_name(name, buff.as_mut_ptr());

    while last >= base {
        position = base + ((last - base) >> 1);
        let pos = &pg_encname_tbl[position as usize];
        result = (*key.add(0) as c_int) - (*pos.name.add(0) as c_int);

        if result == 0 {
            result = strcmp(key, pos.name);
            if result == 0 {
                return pos.encoding as c_int;
            }
        }
        if result < 0 {
            last = position - 1;
        } else {
            base = position + 1;
        }
    }
    -1
}

pub fn pg_encoding_to_char(encoding: c_int) -> *const c_char {
    if PG_VALID_ENCODING(encoding) {
        let p: &pg_enc2name = &pg_enc2name_tbl[encoding as usize];

        Assert!(encoding == p.encoding as c_int);
        return p.name;
    }
    b"\0".as_ptr() as *const c_char
}

// The prelude does not export strlen/strcmp; provide private helpers matching
// the C semantics used above (NUL-terminated C strings, byte comparison).
unsafe fn strlen(s: *const c_char) -> usize {
    let mut len: usize = 0;
    while *s.add(len) != 0 {
        len += 1;
    }
    len
}

unsafe fn strcmp(a: *const c_char, b: *const c_char) -> c_int {
    let mut i: usize = 0;
    loop {
        let ca = *a.add(i) as c_uchar;
        let cb = *b.add(i) as c_uchar;
        if ca != cb {
            return ca as c_int - cb as c_int;
        }
        if ca == 0 {
            return 0;
        }
        i += 1;
    }
}
