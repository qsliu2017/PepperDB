//! utils/misc/tzparser.c - parse timezone offset files for timezone_abbreviations GUC

use crate::prelude::*;

use crate::port::pgstrcasecmp::{pg_strcasecmp, pg_strncasecmp, pg_tolower};
use crate::port::path::get_share_path;
use crate::pg_config_manual::MAXPGPATH;
use crate::utils::init::globals::my_exec_path;

/* WHITESPACE " \t\n\r" */
const WHITESPACE: &[u8] = b" \t\n\r\0";

/* ----------------------------------------------------------------
 * Constants borrowed from utils/datetime.h / datatype/timestamp.h
 * (datetime.c not yet ported).
 * ---------------------------------------------------------------- */

/* datetime.h: only this many chars are stored in datetktbl */
const TOKMAXLEN: c_int = 10;

/* datatype/timestamp.h */
const SECS_PER_HOUR: c_int = 3600;

/* ----------------------------------------------------------------
 * tzparser.h: tzEntry, the parsed result element.
 * Exported there because datetime.c needs it.  Defined locally until
 * utils/datetime.c is ported.
 * ---------------------------------------------------------------- */

/// The result of parsing a timezone configuration file is an array of
/// these structs, in order by abbrev.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct tzEntry {
    /* the actual data */
    pub abbrev: *mut c_char,   /* TZ abbreviation (downcased) */
    pub zone: *mut c_char,     /* zone name if dynamic abbrev, else NULL */
    /* for a dynamic abbreviation, offset/is_dst are not used */
    pub offset: c_int,         /* offset in seconds from UTC */
    pub is_dst: bool,          /* true if a DST abbreviation */
    /* source information (for error messages) */
    pub lineno: c_int,
    pub filename: *const c_char,
}

/* ----------------------------------------------------------------
 * datetime.h: TimeZoneAbbrevTable + ConvertTimeZoneAbbrevs.
 * Stubbed until utils/datetime.c is ported.
 * ---------------------------------------------------------------- */

#[repr(C)]
pub struct TimeZoneAbbrevTable {
    pub tblsize: Size,
    pub numabbrevs: c_int,
    /* datetkn abbrevs[FLEXIBLE_ARRAY_MEMBER]; */
}

// datetime.c: ConvertTimeZoneAbbrevs(struct tzEntry *abbrevs, int n)
// TODO(pg-port): port utils/adt/datetime.c.
unsafe fn ConvertTimeZoneAbbrevs(
    _abbrevs: *mut tzEntry,
    _n: c_int,
) -> *mut TimeZoneAbbrevTable {
    unimplemented!()
}

/* ----------------------------------------------------------------
 * storage/fd.h: AllocateFile/AllocateDir/FreeFile/FreeDir.
 * fd.c not yet ported; stub locally (mirrors other ported files).
 * ---------------------------------------------------------------- */

// Opaque C FILE.
#[allow(non_camel_case_types)]
pub enum FILE {}
// Opaque DIR.
pub enum DIR {}

unsafe fn AllocateFile(_name: *const c_char, _mode: *const c_char) -> *mut FILE {
    unimplemented!()
}
unsafe fn AllocateDir(_dirname: *const c_char) -> *mut DIR {
    unimplemented!()
}
unsafe fn FreeDir(_dir: *mut DIR) -> c_int {
    unimplemented!()
}
unsafe fn FreeFile(_file: *mut FILE) -> c_int {
    unimplemented!()
}

/* ----------------------------------------------------------------
 * libc / stdio shims used by ParseTzFile.
 * ---------------------------------------------------------------- */

extern "C" {
    fn strtok_r(s: *mut c_char, delim: *const c_char, saveptr: *mut *mut c_char) -> *mut c_char;
    fn strtol(s: *const c_char, endptr: *mut *mut c_char, base: c_int) -> std::ffi::c_long;
    fn strlen(s: *const c_char) -> Size;
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn snprintf(s: *mut c_char, n: Size, fmt: *const c_char, ...) -> c_int;
    fn fgets(s: *mut c_char, n: c_int, stream: *mut FILE) -> *mut c_char;
    fn feof(stream: *mut FILE) -> c_int;
    fn ferror(stream: *mut FILE) -> c_int;
    fn isdigit(c: c_int) -> c_int;
    fn isalpha(c: c_int) -> c_int;
    fn isspace(c: c_int) -> c_int;
    fn memmove(dest: *mut c_void, src: *const c_void, n: Size) -> *mut c_void;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: Size) -> *mut c_void;
}

/* ----------------------------------------------------------------
 * guc.h: GUC_check_errmsg/errdetail/errhint.
 * guc.c not yet ported; model as no-op shims that format their args
 * (mirroring utils/misc/stack_depth.rs).
 * ---------------------------------------------------------------- */
macro_rules! GUC_check_errmsg {
    ($($arg:tt)*) => {{
        let _msg: String = format!($($arg)*);
        let _ = _msg;
    }};
}
macro_rules! GUC_check_errdetail {
    ($($arg:tt)*) => {{
        let _detail: String = format!($($arg)*);
        let _ = _detail;
    }};
}
macro_rules! GUC_check_errhint {
    ($($arg:tt)*) => {{
        let _hint: String = format!($($arg)*);
        let _ = _hint;
    }};
}

/* Helper: build a Rust String from a NUL-terminated C string for logging. */
unsafe fn cstr(p: *const c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    std::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
}

/*
 * Apply additional validation checks to a tzEntry
 *
 * Returns true if OK, else false
 */
unsafe fn validateTzEntry(tzentry: *mut tzEntry) -> bool {
    let mut p: *mut std::ffi::c_uchar;

    /*
     * Check restrictions imposed by datetktbl storage format (see datetime.c)
     */
    if strlen((*tzentry).abbrev) > TOKMAXLEN as Size {
        GUC_check_errmsg!(
            "time zone abbreviation \"{}\" is too long (maximum {} characters) in time zone file \"{}\", line {}",
            cstr((*tzentry).abbrev),
            TOKMAXLEN,
            cstr((*tzentry).filename),
            (*tzentry).lineno
        );
        return false;
    }

    /*
     * Sanity-check the offset: shouldn't exceed 14 hours
     */
    if (*tzentry).offset > 14 * SECS_PER_HOUR || (*tzentry).offset < -14 * SECS_PER_HOUR {
        GUC_check_errmsg!(
            "time zone offset {} is out of range in time zone file \"{}\", line {}",
            (*tzentry).offset,
            cstr((*tzentry).filename),
            (*tzentry).lineno
        );
        return false;
    }

    /*
     * Convert abbrev to lowercase (must match datetime.c's conversion)
     */
    p = (*tzentry).abbrev as *mut std::ffi::c_uchar;
    while *p != 0 {
        *p = pg_tolower(*p);
        p = p.add(1);
    }

    true
}

/*
 * Attempt to parse the line as a timezone abbrev spec
 *
 * Valid formats are:
 *	name  zone
 *	name  offset  dst
 *
 * Returns true if OK, else false; data is stored in *tzentry
 */
unsafe fn splitTzLine(
    filename: *const c_char,
    lineno: c_int,
    line: *mut c_char,
    tzentry: *mut tzEntry,
) -> bool {
    let mut brkl: *mut c_char = null_mut();
    let abbrev: *mut c_char;
    let offset: *mut c_char;
    let mut offset_endptr: *mut c_char = null_mut();
    let remain: *mut c_char;
    let is_dst: *mut c_char;

    (*tzentry).lineno = lineno;
    (*tzentry).filename = filename;

    abbrev = strtok_r(line, WHITESPACE.as_ptr() as *const c_char, &mut brkl);
    if abbrev.is_null() {
        GUC_check_errmsg!(
            "missing time zone abbreviation in time zone file \"{}\", line {}",
            cstr(filename),
            lineno
        );
        return false;
    }
    (*tzentry).abbrev = pstrdup(abbrev);

    offset = strtok_r(null_mut(), WHITESPACE.as_ptr() as *const c_char, &mut brkl);
    if offset.is_null() {
        GUC_check_errmsg!(
            "missing time zone offset in time zone file \"{}\", line {}",
            cstr(filename),
            lineno
        );
        return false;
    }

    /* We assume zone names don't begin with a digit or sign */
    if isdigit(*offset as std::ffi::c_uchar as c_int) != 0 || *offset == b'+' as c_char || *offset == b'-' as c_char {
        (*tzentry).zone = null_mut();
        (*tzentry).offset = strtol(offset, &mut offset_endptr, 10) as c_int;
        if offset_endptr == offset || *offset_endptr != 0 {
            GUC_check_errmsg!(
                "invalid number for time zone offset in time zone file \"{}\", line {}",
                cstr(filename),
                lineno
            );
            return false;
        }

        is_dst = strtok_r(null_mut(), WHITESPACE.as_ptr() as *const c_char, &mut brkl);
        if !is_dst.is_null() && pg_strcasecmp(is_dst, b"D\0".as_ptr() as *const c_char) == 0 {
            (*tzentry).is_dst = true;
            remain = strtok_r(null_mut(), WHITESPACE.as_ptr() as *const c_char, &mut brkl);
        } else {
            /* there was no 'D' dst specifier */
            (*tzentry).is_dst = false;
            remain = is_dst;
        }
    } else {
        /*
         * Assume entry is a zone name.  We do not try to validate it by
         * looking up the zone, because that would force loading of a lot of
         * zones that probably will never be used in the current session.
         */
        (*tzentry).zone = pstrdup(offset);
        (*tzentry).offset = 0 * SECS_PER_HOUR;
        (*tzentry).is_dst = false;
        remain = strtok_r(null_mut(), WHITESPACE.as_ptr() as *const c_char, &mut brkl);
    }

    if remain.is_null() {
        /* no more non-whitespace chars */
        return true;
    }

    if *remain.add(0) != b'#' as c_char {
        /* must be a comment */
        GUC_check_errmsg!(
            "invalid syntax in time zone file \"{}\", line {}",
            cstr(filename),
            lineno
        );
        return false;
    }
    true
}

/*
 * Insert entry into sorted array
 *
 * *base: base address of array (changeable if must enlarge array)
 * *arraysize: allocated length of array (changeable if must enlarge array)
 * n: current number of valid elements in array
 * entry: new data to insert
 * override: true if OK to override
 *
 * Returns the new array length (new value for n), or -1 if error
 */
unsafe fn addToArray(
    base: *mut *mut tzEntry,
    arraysize: *mut c_int,
    n: c_int,
    entry: *mut tzEntry,
    override_: bool,
) -> c_int {
    let mut arrayptr: *mut tzEntry;
    let mut low: c_int;
    let mut high: c_int;

    /*
     * Search the array for a duplicate; as a useful side effect, the array is
     * maintained in sorted order.  We use strcmp() to ensure we match the
     * sort order datetime.c expects.
     */
    arrayptr = *base;
    low = 0;
    high = n - 1;
    while low <= high {
        let mid: c_int = (low + high) >> 1;
        let midptr: *mut tzEntry = arrayptr.add(mid as usize);
        let cmp: c_int;

        cmp = strcmp((*entry).abbrev, (*midptr).abbrev);
        if cmp < 0 {
            high = mid - 1;
        } else if cmp > 0 {
            low = mid + 1;
        } else {
            /*
             * Found a duplicate entry; complain unless it's the same.
             */
            if ((*midptr).zone.is_null()
                && (*entry).zone.is_null()
                && (*midptr).offset == (*entry).offset
                && (*midptr).is_dst == (*entry).is_dst)
                || (!(*midptr).zone.is_null()
                    && !(*entry).zone.is_null()
                    && strcmp((*midptr).zone, (*entry).zone) == 0)
            {
                /* return unchanged array */
                return n;
            }
            if override_ {
                /* same abbrev but something is different, override */
                (*midptr).zone = (*entry).zone;
                (*midptr).offset = (*entry).offset;
                (*midptr).is_dst = (*entry).is_dst;
                return n;
            }
            /* same abbrev but something is different, complain */
            GUC_check_errmsg!(
                "time zone abbreviation \"{}\" is multiply defined",
                cstr((*entry).abbrev)
            );
            GUC_check_errdetail!(
                "Entry in time zone file \"{}\", line {}, conflicts with entry in file \"{}\", line {}.",
                cstr((*midptr).filename),
                (*midptr).lineno,
                cstr((*entry).filename),
                (*entry).lineno
            );
            return -1;
        }
    }

    /*
     * No match, insert at position "low".
     */
    if n >= *arraysize {
        *arraysize *= 2;
        *base = repalloc(
            *base as *mut c_void,
            (*arraysize as Size) * std::mem::size_of::<tzEntry>() as Size,
        ) as *mut tzEntry;
    }

    arrayptr = (*base).add(low as usize);

    memmove(
        arrayptr.add(1) as *mut c_void,
        arrayptr as *const c_void,
        ((n - low) as Size) * std::mem::size_of::<tzEntry>() as Size,
    );

    memcpy(
        arrayptr as *mut c_void,
        entry as *const c_void,
        std::mem::size_of::<tzEntry>() as Size,
    );

    n + 1
}

/*
 * Parse a single timezone abbrev file --- can recurse to handle @INCLUDE
 *
 * filename: user-specified file name (does not include path)
 * depth: current recursion depth
 * *base: array for results (changeable if must enlarge array)
 * *arraysize: allocated length of array (changeable if must enlarge array)
 * n: current number of valid elements in array
 *
 * Returns the new array length (new value for n), or -1 if error
 */
unsafe fn ParseTzFile(
    filename: *const c_char,
    depth: c_int,
    base: *mut *mut tzEntry,
    arraysize: *mut c_int,
    mut n: c_int,
) -> c_int {
    let mut share_path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut file_path: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let tzFile: *mut FILE;
    let mut tzbuf: [c_char; 1024] = [0; 1024];
    let mut line: *mut c_char;
    let mut tzentry: tzEntry = std::mem::zeroed();
    let mut lineno: c_int = 0;
    let mut override_: bool = false;
    let mut p: *const c_char;

    /*
     * We enforce that the filename is all alpha characters.  This may be
     * overly restrictive, but we don't want to allow access to anything
     * outside the timezonesets directory, so for instance '/' *must* be
     * rejected.
     */
    p = filename;
    while *p != 0 {
        if isalpha(*p as std::ffi::c_uchar as c_int) == 0 {
            /* at level 0, just use guc.c's regular "invalid value" message */
            if depth > 0 {
                GUC_check_errmsg!("invalid time zone file name \"{}\"", cstr(filename));
            }
            return -1;
        }
        p = p.add(1);
    }

    /*
     * The maximal recursion depth is a pretty arbitrary setting. It is hard
     * to imagine that someone needs more than 3 levels so stick with this
     * conservative setting until someone complains.
     */
    if depth > 3 {
        GUC_check_errmsg!(
            "time zone file recursion limit exceeded in file \"{}\"",
            cstr(filename)
        );
        return -1;
    }

    get_share_path(my_exec_path.as_ptr(), share_path.as_mut_ptr());
    snprintf(
        file_path.as_mut_ptr(),
        std::mem::size_of_val(&file_path) as Size,
        b"%s/timezonesets/%s\0".as_ptr() as *const c_char,
        share_path.as_ptr(),
        filename,
    );
    tzFile = AllocateFile(file_path.as_ptr(), b"r\0".as_ptr() as *const c_char);
    if tzFile.is_null() {
        /*
         * Check to see if the problem is not the filename but the directory.
         * This is worth troubling over because if the installation share/
         * directory is missing or unreadable, this is likely to be the first
         * place we notice a problem during postmaster startup.
         */
        let save_errno: c_int = errno();
        let tzdir: *mut DIR;

        snprintf(
            file_path.as_mut_ptr(),
            std::mem::size_of_val(&file_path) as Size,
            b"%s/timezonesets\0".as_ptr() as *const c_char,
            share_path.as_ptr(),
        );
        tzdir = AllocateDir(file_path.as_ptr());
        if tzdir.is_null() {
            GUC_check_errmsg!(
                "could not open directory \"{}\": {}",
                cstr(file_path.as_ptr()),
                strerror_errno()
            );
            GUC_check_errhint!(
                "This may indicate an incomplete PostgreSQL installation, or that the file \"{}\" has been moved away from its proper location.",
                cstr(my_exec_path.as_ptr())
            );
            return -1;
        }
        FreeDir(tzdir);
        set_errno(save_errno);

        /*
         * otherwise, if file doesn't exist and it's level 0, guc.c's
         * complaint is enough
         */
        if errno() != ENOENT || depth > 0 {
            GUC_check_errmsg!(
                "could not read time zone file \"{}\": {}",
                cstr(filename),
                strerror_errno()
            );
        }

        return -1;
    }

    while feof(tzFile) == 0 {
        lineno += 1;
        if fgets(
            tzbuf.as_mut_ptr(),
            std::mem::size_of_val(&tzbuf) as c_int,
            tzFile,
        )
        .is_null()
        {
            if ferror(tzFile) != 0 {
                GUC_check_errmsg!(
                    "could not read time zone file \"{}\": {}",
                    cstr(filename),
                    strerror_errno()
                );
                n = -1;
                break;
            }
            /* else we're at EOF after all */
            break;
        }
        if strlen(tzbuf.as_ptr()) == (std::mem::size_of_val(&tzbuf) - 1) as Size {
            /* the line is too long for tzbuf */
            GUC_check_errmsg!(
                "line is too long in time zone file \"{}\", line {}",
                cstr(filename),
                lineno
            );
            n = -1;
            break;
        }

        /* skip over whitespace */
        line = tzbuf.as_mut_ptr();
        while *line != 0 && isspace(*line as std::ffi::c_uchar as c_int) != 0 {
            line = line.add(1);
        }

        if *line == b'\0' as c_char {
            /* empty line */
            continue;
        }
        if *line == b'#' as c_char {
            /* comment line */
            continue;
        }

        if pg_strncasecmp(
            line,
            b"@INCLUDE\0".as_ptr() as *const c_char,
            strlen(b"@INCLUDE\0".as_ptr() as *const c_char),
        ) == 0
        {
            /* pstrdup so we can use filename in result data structure */
            let mut includeFile: *mut c_char =
                pstrdup(line.add(strlen(b"@INCLUDE\0".as_ptr() as *const c_char) as usize));
            let mut brki: *mut c_char = null_mut();

            includeFile = strtok_r(includeFile, WHITESPACE.as_ptr() as *const c_char, &mut brki);
            if includeFile.is_null() || *includeFile == 0 {
                GUC_check_errmsg!(
                    "@INCLUDE without file name in time zone file \"{}\", line {}",
                    cstr(filename),
                    lineno
                );
                n = -1;
                break;
            }
            n = ParseTzFile(includeFile, depth + 1, base, arraysize, n);
            if n < 0 {
                break;
            }
            continue;
        }

        if pg_strncasecmp(
            line,
            b"@OVERRIDE\0".as_ptr() as *const c_char,
            strlen(b"@OVERRIDE\0".as_ptr() as *const c_char),
        ) == 0
        {
            override_ = true;
            continue;
        }

        if !splitTzLine(filename, lineno, line, &mut tzentry) {
            n = -1;
            break;
        }
        if !validateTzEntry(&mut tzentry) {
            n = -1;
            break;
        }
        n = addToArray(base, arraysize, n, &mut tzentry, override_);
        if n < 0 {
            break;
        }
    }

    FreeFile(tzFile);

    n
}

/*
 * load_tzoffsets --- read and parse the specified timezone offset file
 *
 * On success, return a filled-in TimeZoneAbbrevTable, which must have been
 * guc_malloc'd not palloc'd.  On failure, return NULL, using GUC_check_errmsg
 * and friends to give details of the problem.
 */
pub unsafe fn load_tzoffsets(filename: *const c_char) -> *mut TimeZoneAbbrevTable {
    let mut result: *mut TimeZoneAbbrevTable = null_mut();
    let tmpContext: MemoryContext;
    let oldContext: MemoryContext;
    let mut array: *mut tzEntry;
    let mut arraysize: c_int;
    let n: c_int;

    /*
     * Create a temp memory context to work in.  This makes it easy to clean
     * up afterwards.
     */
    tmpContext = AllocSetContextCreate!(
        CurrentMemoryContext,
        "TZParserMemory",
        ALLOCSET_SMALL_SIZES
    ) as *mut _;
    oldContext = MemoryContextSwitchTo(tmpContext);

    /* Initialize array at a reasonable size */
    arraysize = 128;
    array = palloc((arraysize as Size) * std::mem::size_of::<tzEntry>() as Size) as *mut tzEntry;

    /* Parse the file(s) */
    n = ParseTzFile(filename, 0, &mut array, &mut arraysize, 0);

    /* If no errors so far, let datetime.c allocate memory & convert format */
    if n >= 0 {
        result = ConvertTimeZoneAbbrevs(array, n);
        if result.is_null() {
            GUC_check_errmsg!("out of memory");
        }
    }

    /* Clean up */
    MemoryContextSwitchTo(oldContext);
    MemoryContextDelete(tmpContext);

    result
}

/* ----------------------------------------------------------------
 * errno helpers (C `errno` / `%m`).
 * ---------------------------------------------------------------- */
fn errno() -> c_int {
    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
}
fn set_errno(e: c_int) {
    // Best-effort: there is no portable setter; mirror by storing via libc.
    unsafe {
        *__errno_location() = e;
    }
}
fn strerror_errno() -> String {
    std::io::Error::last_os_error().to_string()
}

extern "C" {
    #[cfg_attr(target_os = "macos", link_name = "__error")]
    #[cfg_attr(target_os = "linux", link_name = "__errno_location")]
    fn __errno_location() -> *mut c_int;
}

/* errno.h: ENOENT */
const ENOENT: c_int = 2;
