//! dfmgr.rs
//!   Dynamic loader interface
//!
//! Translated 1:1 from postgres/src/backend/utils/fmgr/dfmgr.c
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/utils/fmgr/dfmgr.c
#![allow(unused_variables)]
#![allow(dead_code)]

use crate::prelude::*; // Datum, Oid, bool, palloc, pfree, pstrdup, elog!, ereport!, errmsg!, Assert!, MemSet

use crate::utils::fmgr::{
    Pg_abi_values, Pg_magic_struct, PGModuleMagicFunction, PG_MAGIC_FUNCTION_NAME_STRING,
};
use crate::lib::stringinfo::{
    appendStringInfoChar, appendStringInfoString, initStringInfo, StringInfoData,
};
use crate::utils::hash::dynahash::{
    hash_create, hash_search, HASHACTION, HASHCTL, HASH_ELEM, HASH_STRINGS, HTAB,
};
use crate::pg_config_manual::NAMEDATALEN;
use crate::port::path::{canonicalize_path, first_dir_separator, first_path_var_separator};
use crate::port::port_api::{is_absolute_path, strlcpy};
use crate::storage::ipc::shmem::add_size;
use crate::utils::elog::{errcode, DEBUG3, ERROR};

use std::ffi::{c_char, c_int, c_void, CStr};

// ---------------------------------------------------------------------------
// Local stubs for symbols with no real home in the port yet.
// ---------------------------------------------------------------------------

// TODO(pg-port): real DLSUFFIX lives in (generated) pg_config.h; mirrored in
// utils/misc/injection_point.rs.
const DLSUFFIX: &CStr = c".so";

// TODO(pg-port): real pkglib_path lives in utils/init/globals.rs.
use crate::utils::init::globals::pkglib_path;

// TODO(pg-port): real Dynamic_library_path GUC lives in utils/misc/guc_tables.c.
#[no_mangle]
pub static mut Dynamic_library_path: *mut c_char = std::ptr::null_mut();

// TODO(pg-port): real pg_file_exists lives in storage/file/fd.c.
unsafe fn pg_file_exists(name: *const c_char) -> bool {
    if name.is_null() {
        return false;
    }
    let s = CStr::from_ptr(name).to_string_lossy();
    std::path::Path::new(s.as_ref()).exists()
}

// TODO(pg-port): real psprintf lives in utils/mmgr/mcxt.c (palloc-owned result).
unsafe fn psprintf_concat(a: &str, b: &str) -> *mut c_char {
    let s = format!("{}{}", a, b);
    pstrdup(std::ffi::CString::new(s).unwrap().as_ptr())
}

// TODO(pg-port): real errcode_for_file_access lives in utils/error/elog.c.
fn errcode_for_file_access() -> &'static str {
    ""
}

// ---------------------------------------------------------------------------
// dl* extern interface (dlfcn.h).
// ---------------------------------------------------------------------------

const RTLD_NOW: c_int = 0x2;
const RTLD_GLOBAL: c_int = 0x100;

extern "C" {
    fn dlopen(filename: *const c_char, flag: c_int) -> *mut c_void;
    fn dlsym(handle: *mut c_void, symbol: *const c_char) -> *mut c_void;
    fn dlclose(handle: *mut c_void) -> c_int;
    fn dlerror() -> *mut c_char;
}

// ---------------------------------------------------------------------------

/* signature for PostgreSQL-specific library init function */
type PG_init_t = unsafe extern "C" fn();

/* hashtable entry for rendezvous variables */
#[repr(C)]
struct rendezvousHashEntry {
    varName: [c_char; NAMEDATALEN], /* hash key (must be first) */
    varValue: *mut c_void,
}

/*
 * List of dynamically loaded files (kept in malloc'd memory).
 *
 * Note: "typedef struct DynamicFileList DynamicFileList" appears in fmgr.h.
 */
#[repr(C)]
pub struct DynamicFileList {
    next: *mut DynamicFileList, /* List link */
    device: libc_dev_t,         /* Device file is on */
    inode: libc_ino_t,          /* Inode number of file */
    handle: *mut c_void,        /* a handle for pg_dl* functions */
    magic: *const Pg_magic_struct, /* Location of module's magic block */
    /* Full pathname of file (FLEXIBLE_ARRAY_MEMBER in C; here a CString tail
     * is heap-managed separately via a Rust owned buffer pointer) */
    filename: *mut c_char,
}

// TODO(pg-port): dev_t / ino_t come from <sys/stat.h>; modeled as 64-bit ids.
type libc_dev_t = u64;
type libc_ino_t = u64;

static mut file_list: *mut DynamicFileList = std::ptr::null_mut();
static mut file_tail: *mut DynamicFileList = std::ptr::null_mut();

/*
 * Load the specified dynamic-link library file, and look for a function
 * named funcname in it.
 *
 * If the function is not found, we raise an error if signalNotFound is true,
 * else return NULL.  Note that errors in loading the library
 * will provoke ereport() regardless of signalNotFound.
 *
 * If filehandle is not NULL, then *filehandle will be set to a handle
 * identifying the library file.  The filehandle can be used with
 * lookup_external_function to lookup additional functions in the same file
 * at less cost than repeating load_external_function.
 */
pub unsafe fn load_external_function(
    mut filename: *const c_char,
    funcname: *const c_char,
    signalNotFound: bool,
    filehandle: *mut *mut c_void,
) -> *mut c_void {
    let fullname: *mut c_char;
    let lib_handle: *mut c_void;
    let retval: *mut c_void;

    /*
     * For extensions with hardcoded '$libdir/' library names, we strip the
     * prefix to allow the library search path to be used. This is done only
     * for simple names (e.g., "$libdir/foo"), not for nested paths (e.g.,
     * "$libdir/foo/bar").
     *
     * For nested paths, 'expand_dynamic_library_name' directly expands the
     * '$libdir' macro, so we leave them untouched.
     */
    if strncmp_c(filename, c"$libdir/".as_ptr(), 8) == 0 {
        if first_dir_separator(filename.add(8)).is_null() {
            filename = filename.add(8);
        }
    }

    /* Expand the possibly-abbreviated filename to an exact path name */
    fullname = expand_dynamic_library_name(filename);

    /* Load the shared library, unless we already did */
    lib_handle = internal_load_library(fullname);

    /* Return handle if caller wants it */
    if !filehandle.is_null() {
        *filehandle = lib_handle;
    }

    /* Look up the function within the library. */
    retval = dlsym(lib_handle, funcname);

    if retval.is_null() && signalNotFound {
        ereport!(
            ERROR,
            errmsg!(
                "could not find function \"{}\" in file \"{}\"",
                CStr::from_ptr(funcname).to_string_lossy(),
                CStr::from_ptr(fullname).to_string_lossy()
            )
        );
    }

    pfree(fullname as *mut c_void);
    retval
}

/*
 * This function loads a shlib file without looking up any particular
 * function in it.  If the same shlib has previously been loaded,
 * we do not load it again.
 *
 * When 'restricted' is true, only libraries in the presumed-secure
 * directory $libdir/plugins may be referenced.
 */
pub unsafe fn load_file(filename: *const c_char, restricted: bool) {
    let fullname: *mut c_char;

    /* Apply security restriction if requested */
    if restricted {
        check_restricted_library_name(filename);
    }

    /* Expand the possibly-abbreviated filename to an exact path name */
    fullname = expand_dynamic_library_name(filename);

    /* Load the shared library, unless we already did */
    let _ = internal_load_library(fullname);

    pfree(fullname as *mut c_void);
}

/*
 * Lookup a function whose library file is already loaded.
 * Return NULL if not found.
 */
pub unsafe fn lookup_external_function(
    filehandle: *mut c_void,
    funcname: *const c_char,
) -> *mut c_void {
    dlsym(filehandle, funcname)
}

/*
 * Load the specified dynamic-link library file, unless it already is
 * loaded.  Return the pg_dl* handle for the file.
 *
 * Note: libname is expected to be an exact name for the library file.
 *
 * NB: There is presently no way to unload a dynamically loaded file.  We might
 * add one someday if we can convince ourselves we have safe protocols for un-
 * hooking from hook function pointers, releasing custom GUC variables, and
 * perhaps other things that are definitely unsafe currently.
 */
unsafe fn internal_load_library(libname: *const c_char) -> *mut c_void {
    let mut file_scanner: *mut DynamicFileList;
    let magic_func: PGModuleMagicFunction;
    let load_error: *mut c_char;
    let mut stat_buf: libc_stat = std::mem::zeroed();
    let PG_init: Option<PG_init_t>;

    /*
     * Scan the list of loaded FILES to see if the file has been loaded.
     */
    file_scanner = file_list;
    while !file_scanner.is_null() && strcmp_c(libname, (*file_scanner).filename) != 0 {
        file_scanner = (*file_scanner).next;
    }

    if file_scanner.is_null() {
        /*
         * Check for same files - different paths (ie, symlink or link)
         */
        if pg_stat(libname, &mut stat_buf) == -1 {
            ereport!(
                ERROR,
                errmsg!(
                    "could not access file \"{}\"",
                    CStr::from_ptr(libname).to_string_lossy()
                )
            );
        }

        file_scanner = file_list;
        while !file_scanner.is_null() && !SAME_INODE(&stat_buf, &*file_scanner) {
            file_scanner = (*file_scanner).next;
        }
    }

    if file_scanner.is_null() {
        /*
         * File not loaded yet.
         */
        let libname_len = strlen_c(libname);
        file_scanner = libc_malloc(std::mem::size_of::<DynamicFileList>()) as *mut DynamicFileList;
        if file_scanner.is_null() {
            ereport!(ERROR, errmsg!("out of memory"));
        }

        MemSet(file_scanner as *mut c_void, 0, std::mem::size_of::<DynamicFileList>());
        /* allocate + copy the filename tail (FLEXIBLE_ARRAY_MEMBER in C) */
        (*file_scanner).filename = libc_malloc(libname_len + 1) as *mut c_char;
        strcpy_c((*file_scanner).filename, libname);
        (*file_scanner).device = stat_buf.st_dev;
        (*file_scanner).inode = stat_buf.st_ino;
        (*file_scanner).next = std::ptr::null_mut();

        (*file_scanner).handle = dlopen((*file_scanner).filename, RTLD_NOW | RTLD_GLOBAL);
        if (*file_scanner).handle.is_null() {
            load_error = dlerror();
            libc_free(file_scanner as *mut c_void);
            /* errcode_for_file_access might not be appropriate here? */
            ereport!(
                ERROR,
                errmsg!(
                    "could not load library \"{}\": {}",
                    CStr::from_ptr(libname).to_string_lossy(),
                    CStr::from_ptr(load_error).to_string_lossy()
                )
            );
        }

        /* Check the magic function to determine compatibility */
        let magic_sym = dlsym((*file_scanner).handle, PG_MAGIC_FUNCTION_NAME_STRING_C.as_ptr());
        if !magic_sym.is_null() {
            magic_func = std::mem::transmute::<*mut c_void, PGModuleMagicFunction>(magic_sym);
            let magic_data_ptr: *const Pg_magic_struct = magic_func();

            /* Check ABI compatibility fields */
            if (*magic_data_ptr).len != std::mem::size_of::<Pg_magic_struct>() as c_int
                || memcmp_c(
                    &(*magic_data_ptr).abi_fields as *const Pg_abi_values as *const c_void,
                    &magic_data as *const Pg_abi_values as *const c_void,
                    std::mem::size_of::<Pg_abi_values>(),
                ) != 0
            {
                /* copy data block before unlinking library */
                let module_magic_data: Pg_magic_struct = std::ptr::read(magic_data_ptr);

                /* try to close library */
                dlclose((*file_scanner).handle);
                libc_free(file_scanner as *mut c_void);

                /* issue suitable complaint */
                incompatible_module_error(libname, &module_magic_data.abi_fields);
            }

            /* Remember the magic block's location for future use */
            (*file_scanner).magic = magic_data_ptr;
        } else {
            /* try to close library */
            dlclose((*file_scanner).handle);
            libc_free(file_scanner as *mut c_void);
            /* complain */
            ereport!(
                ERROR,
                errmsg!(
                    "incompatible library \"{}\": missing magic block",
                    CStr::from_ptr(libname).to_string_lossy()
                )
            );
        }

        /*
         * If the library has a _PG_init() function, call it.
         */
        let pg_init_sym = dlsym((*file_scanner).handle, c"_PG_init".as_ptr());
        if !pg_init_sym.is_null() {
            PG_init = Some(std::mem::transmute::<*mut c_void, PG_init_t>(pg_init_sym));
            (PG_init.unwrap())();
        }

        /* OK to link it into list */
        if file_list.is_null() {
            file_list = file_scanner;
        } else {
            (*file_tail).next = file_scanner;
        }
        file_tail = file_scanner;
    }

    (*file_scanner).handle
}

/*
 * Report a suitable error for an incompatible magic block.
 */
unsafe fn incompatible_module_error(
    libname: *const c_char,
    module_magic_data: *const Pg_abi_values,
) -> ! {
    let mut details: StringInfoData = std::mem::zeroed();

    /*
     * If the version doesn't match, just report that, because the rest of the
     * block might not even have the fields we expect.
     */
    if magic_data.version != (*module_magic_data).version {
        let library_version: String;

        if (*module_magic_data).version >= 1000 {
            library_version = format!("{}", (*module_magic_data).version / 100);
        } else {
            library_version = format!(
                "{}.{}",
                (*module_magic_data).version / 100,
                (*module_magic_data).version % 100
            );
        }
        ereport!(
            ERROR,
            errmsg!(
                "incompatible library \"{}\": version mismatch (Server is version {}, library is version {}.)",
                CStr::from_ptr(libname).to_string_lossy(),
                magic_data.version / 100,
                library_version
            )
        );
    }

    /*
     * Similarly, if the ABI extra field doesn't match, error out.  Other
     * fields below might also mismatch, but that isn't useful information if
     * you're using the wrong product altogether.
     */
    if strcmp_c(
        (*module_magic_data).abi_extra.as_ptr(),
        magic_data.abi_extra.as_ptr(),
    ) != 0
    {
        ereport!(
            ERROR,
            errmsg!(
                "incompatible library \"{}\": ABI mismatch (Server has ABI \"{}\", library has \"{}\".)",
                CStr::from_ptr(libname).to_string_lossy(),
                CStr::from_ptr(magic_data.abi_extra.as_ptr()).to_string_lossy(),
                CStr::from_ptr((*module_magic_data).abi_extra.as_ptr()).to_string_lossy()
            )
        );
    }

    /*
     * Otherwise, spell out which fields don't agree.
     *
     * XXX this code has to be adjusted any time the set of fields in a magic
     * block change!
     */
    initStringInfo(&mut details);

    if (*module_magic_data).funcmaxargs != magic_data.funcmaxargs {
        if details.len != 0 {
            appendStringInfoChar(&mut details, b'\n' as c_char);
        }
        /* translator: %s is a variable name and %d its values */
        crate::appendStringInfo!(
            &mut details,
            "Server has {} = {}, library has {}.",
            "FUNC_MAX_ARGS",
            magic_data.funcmaxargs,
            (*module_magic_data).funcmaxargs
        );
    }
    if (*module_magic_data).indexmaxkeys != magic_data.indexmaxkeys {
        if details.len != 0 {
            appendStringInfoChar(&mut details, b'\n' as c_char);
        }
        /* translator: %s is a variable name and %d its values */
        crate::appendStringInfo!(
            &mut details,
            "Server has {} = {}, library has {}.",
            "INDEX_MAX_KEYS",
            magic_data.indexmaxkeys,
            (*module_magic_data).indexmaxkeys
        );
    }
    if (*module_magic_data).namedatalen != magic_data.namedatalen {
        if details.len != 0 {
            appendStringInfoChar(&mut details, b'\n' as c_char);
        }
        /* translator: %s is a variable name and %d its values */
        crate::appendStringInfo!(
            &mut details,
            "Server has {} = {}, library has {}.",
            "NAMEDATALEN",
            magic_data.namedatalen,
            (*module_magic_data).namedatalen
        );
    }
    if (*module_magic_data).float8byval != magic_data.float8byval {
        if details.len != 0 {
            appendStringInfoChar(&mut details, b'\n' as c_char);
        }
        /* translator: %s is a variable name and %d its values */
        crate::appendStringInfo!(
            &mut details,
            "Server has {} = {}, library has {}.",
            "FLOAT8PASSBYVAL",
            if magic_data.float8byval != 0 { "true" } else { "false" },
            if (*module_magic_data).float8byval != 0 {
                "true"
            } else {
                "false"
            }
        );
    }

    if details.len == 0 {
        appendStringInfoString(
            &mut details,
            c"Magic block has unexpected length or padding difference.".as_ptr(),
        );
    }

    ereport!(
        ERROR,
        errmsg!(
            "incompatible library \"{}\": magic block mismatch ({})",
            CStr::from_ptr(libname).to_string_lossy(),
            CStr::from_ptr(details.data).to_string_lossy()
        )
    );
    unreachable!()
}

/*
 * Iterator functions to allow callers to scan the list of loaded modules.
 *
 * Note: currently, there is no special provision for dealing with changes
 * in the list while a scan is happening.  Current callers don't need it.
 */
pub unsafe fn get_first_loaded_module() -> *mut DynamicFileList {
    file_list
}

pub unsafe fn get_next_loaded_module(dfptr: *mut DynamicFileList) -> *mut DynamicFileList {
    (*dfptr).next
}

/*
 * Return some details about the specified module.
 *
 * Note that module_name and module_version could be returned as NULL.
 *
 * We could dispense with this function by exposing struct DynamicFileList
 * globally, but this way seems preferable.
 */
pub unsafe fn get_loaded_module_details(
    dfptr: *mut DynamicFileList,
    library_path: *mut *const c_char,
    module_name: *mut *const c_char,
    module_version: *mut *const c_char,
) {
    *library_path = (*dfptr).filename;
    *module_name = (*(*dfptr).magic).name;
    *module_version = (*(*dfptr).magic).version;
}

/*
 * If name contains a slash, check if the file exists, if so return
 * the name.  Else (no slash) try to expand using search path (see
 * find_in_path below); if that works, return the fully
 * expanded file name.  If the previous failed, append DLSUFFIX and
 * try again.  If all fails, just return the original name.
 *
 * The result will always be freshly palloc'd.
 */
unsafe fn expand_dynamic_library_name(name: *const c_char) -> *mut c_char {
    let have_slash: bool;
    let new: *mut c_char;
    let mut full: *mut c_char;

    Assert!(!name.is_null());

    have_slash = !first_dir_separator(name).is_null();

    if !have_slash {
        full = find_in_path(
            name,
            Dynamic_library_path,
            c"dynamic_library_path".as_ptr(),
            c"$libdir".as_ptr(),
            pkglib_path.as_ptr(),
        );
        if !full.is_null() {
            return full;
        }
    } else {
        full = substitute_path_macro(name, c"$libdir".as_ptr(), pkglib_path.as_ptr());
        if pg_file_exists(full) {
            return full;
        }
        pfree(full as *mut c_void);
    }

    new = psprintf_concat(
        &CStr::from_ptr(name).to_string_lossy(),
        &DLSUFFIX.to_string_lossy(),
    );

    if !have_slash {
        full = find_in_path(
            new,
            Dynamic_library_path,
            c"dynamic_library_path".as_ptr(),
            c"$libdir".as_ptr(),
            pkglib_path.as_ptr(),
        );
        pfree(new as *mut c_void);
        if !full.is_null() {
            return full;
        }
    } else {
        full = substitute_path_macro(new, c"$libdir".as_ptr(), pkglib_path.as_ptr());
        pfree(new as *mut c_void);
        if pg_file_exists(full) {
            return full;
        }
        pfree(full as *mut c_void);
    }

    /*
     * If we can't find the file, just return the string as-is. The ensuing
     * load attempt will fail and report a suitable message.
     */
    pstrdup(name)
}

/*
 * Check a restricted library name.  It must begin with "$libdir/plugins/"
 * and there must not be any directory separators after that (this is
 * sufficient to prevent ".." style attacks).
 */
unsafe fn check_restricted_library_name(name: *const c_char) {
    if strncmp_c(name, c"$libdir/plugins/".as_ptr(), 16) != 0
        || !first_dir_separator(name.add(16)).is_null()
    {
        ereport!(
            ERROR,
            errmsg!(
                "access to library \"{}\" is not allowed",
                CStr::from_ptr(name).to_string_lossy()
            )
        );
    }
}

/*
 * Substitute for any macros appearing in the given string.
 * Result is always freshly palloc'd.
 */
pub unsafe fn substitute_path_macro(
    str: *const c_char,
    macro_: *const c_char,
    value: *const c_char,
) -> *mut c_char {
    let mut sep_ptr: *const c_char;

    Assert!(!str.is_null());
    Assert!(*macro_ == b'$' as c_char);

    /* Currently, we only recognize $macro at the start of the string */
    if *str != b'$' as c_char {
        return pstrdup(str);
    }

    sep_ptr = first_dir_separator(str);
    if sep_ptr.is_null() {
        sep_ptr = str.add(strlen_c(str));
    }

    if strlen_c(macro_) != sep_ptr.offset_from(str) as usize
        || strncmp_c(str, macro_, strlen_c(macro_)) != 0
    {
        ereport!(
            ERROR,
            errmsg!(
                "invalid macro name in path: {}",
                CStr::from_ptr(str).to_string_lossy()
            )
        );
    }

    psprintf_concat(
        &CStr::from_ptr(value).to_string_lossy(),
        &CStr::from_ptr(sep_ptr).to_string_lossy(),
    )
}

/*
 * Search for a file called 'basename' in the colon-separated search
 * path given.  If the file is found, the full file name
 * is returned in freshly palloc'd memory.  If the file is not found,
 * return NULL.
 *
 * path_param is the name of the parameter that path came from, for error
 * messages.
 *
 * macro and macro_val allow substituting a macro; see
 * substitute_path_macro().
 */
pub unsafe fn find_in_path(
    basename: *const c_char,
    path: *const c_char,
    path_param: *const c_char,
    macro_: *const c_char,
    macro_val: *const c_char,
) -> *mut c_char {
    let mut p: *const c_char;
    let baselen: usize;

    Assert!(!basename.is_null());
    Assert!(first_dir_separator(basename).is_null());
    Assert!(!path.is_null());
    Assert!(!path_param.is_null());

    p = path;

    /*
     * If the path variable is empty, don't do a path search.
     */
    if strlen_c(p) == 0 {
        return std::ptr::null_mut();
    }

    baselen = strlen_c(basename);

    loop {
        let len: usize;
        let piece: *mut c_char;
        let mangled: *mut c_char;
        let full: *mut c_char;

        let piece_sep = first_path_var_separator(p);
        if piece_sep == p as *mut c_char {
            ereport!(
                ERROR,
                errmsg!(
                    "zero-length component in parameter \"{}\"",
                    CStr::from_ptr(path_param).to_string_lossy()
                )
            );
        }

        if piece_sep.is_null() {
            len = strlen_c(p);
        } else {
            len = piece_sep.offset_from(p) as usize;
        }

        piece = palloc(len + 1) as *mut c_char;
        strlcpy(piece, p, len + 1);

        mangled = substitute_path_macro(piece, macro_, macro_val);
        pfree(piece as *mut c_void);

        canonicalize_path(mangled);

        /* only absolute paths */
        if !is_absolute_path(mangled) {
            ereport!(
                ERROR,
                errmsg!(
                    "component in parameter \"{}\" is not an absolute path",
                    CStr::from_ptr(path_param).to_string_lossy()
                )
            );
        }

        full = palloc(strlen_c(mangled) + 1 + baselen + 1) as *mut c_char;
        sprintf_slash(full, mangled, basename);
        pfree(mangled as *mut c_void);

        elog!(
            DEBUG3,
            "find_in_path: trying \"{}\"",
            CStr::from_ptr(full).to_string_lossy()
        );

        if pg_file_exists(full) {
            return full;
        }

        pfree(full as *mut c_void);

        if *p.add(len) == 0 {
            break;
        } else {
            p = p.add(len + 1);
        }
    }

    std::ptr::null_mut()
}

/*
 * Find (or create) a rendezvous variable that one dynamically
 * loaded library can use to meet up with another.
 *
 * On the first call of this function for a particular varName,
 * a "rendezvous variable" is created with the given name.
 * The value of the variable is a void pointer (initially set to NULL).
 * Subsequent calls with the same varName just return the address of
 * the existing variable.  Once created, a rendezvous variable lasts
 * for the life of the process.
 *
 * Dynamically loaded libraries can use rendezvous variables
 * to find each other and share information: they just need to agree
 * on the variable name and the data it will point to.
 */
pub unsafe fn find_rendezvous_variable(varName: *const c_char) -> *mut *mut c_void {
    static mut rendezvousHash: *mut HTAB = std::ptr::null_mut();

    let hentry: *mut rendezvousHashEntry;
    let mut found: bool = false;

    /* Create a hashtable if we haven't already done so in this process */
    if rendezvousHash.is_null() {
        let mut ctl: HASHCTL = std::mem::zeroed();

        ctl.keysize = NAMEDATALEN;
        ctl.entrysize = std::mem::size_of::<rendezvousHashEntry>();
        rendezvousHash = hash_create(
            c"Rendezvous variable hash".as_ptr(),
            16,
            &ctl,
            HASH_ELEM | HASH_STRINGS,
        );
    }

    /* Find or create the hashtable entry for this varName */
    hentry = hash_search(
        rendezvousHash,
        varName as *const c_void,
        HASHACTION::HASH_ENTER,
        &mut found,
    ) as *mut rendezvousHashEntry;

    /* Initialize to NULL if first time */
    if !found {
        (*hentry).varValue = std::ptr::null_mut();
    }

    &mut (*hentry).varValue
}

/*
 * Estimate the amount of space needed to serialize the list of libraries
 * we have loaded.
 */
pub unsafe fn EstimateLibraryStateSpace() -> Size {
    let mut file_scanner: *mut DynamicFileList;
    let mut size: Size = 1;

    file_scanner = file_list;
    while !file_scanner.is_null() {
        size = add_size(size, strlen_c((*file_scanner).filename) + 1);
        file_scanner = (*file_scanner).next;
    }

    size
}

/*
 * Serialize the list of libraries we have loaded to a chunk of memory.
 */
pub unsafe fn SerializeLibraryState(mut maxsize: Size, mut start_address: *mut c_char) {
    let mut file_scanner: *mut DynamicFileList;

    file_scanner = file_list;
    while !file_scanner.is_null() {
        let len: Size;

        len = strlcpy(start_address, (*file_scanner).filename, maxsize) + 1;
        Assert!(len < maxsize);
        maxsize -= len;
        start_address = start_address.add(len);
        file_scanner = (*file_scanner).next;
    }
    *start_address.add(0) = 0;
}

/*
 * Load every library the serializing backend had loaded.
 */
pub unsafe fn RestoreLibraryState(mut start_address: *mut c_char) {
    while *start_address != 0 {
        internal_load_library(start_address);
        start_address = start_address.add(strlen_c(start_address) + 1);
    }
}

// ---------------------------------------------------------------------------
// File-local helpers mirroring C standard-library calls.
// ---------------------------------------------------------------------------

/* ABI values that module needs to match to be accepted */
// TODO(pg-port): real PG_MODULE_ABI_DATA macro lives in fmgr.h; populate from
// FUNC_MAX_ARGS / INDEX_MAX_KEYS / NAMEDATALEN / FLOAT8PASSBYVAL / FUNC_MAX_ARGS.
static magic_data: Pg_abi_values = Pg_abi_values {
    version: 1800, /* PG_VERSION_NUM / 100 baseline (PostgreSQL 18) */
    funcmaxargs: 100,
    indexmaxkeys: 32,
    namedatalen: NAMEDATALEN as c_int,
    float8byval: 1,
    abi_extra: [0; 32],
};

/* PG_MAGIC_FUNCTION_NAME_STRING as a NUL-terminated C symbol name */
const PG_MAGIC_FUNCTION_NAME_STRING_C: &CStr = c"Pg_magic_func";

// TODO(pg-port): struct stat fields from <sys/stat.h>; minimal subset used here.
#[repr(C)]
struct libc_stat {
    st_dev: libc_dev_t,
    st_ino: libc_ino_t,
}

#[inline]
unsafe fn SAME_INODE(a: *const libc_stat, b: *const DynamicFileList) -> bool {
    (*a).st_ino == (*b).inode && (*a).st_dev == (*b).device
}

extern "C" {
    fn malloc(size: usize) -> *mut c_void;
    fn free(ptr: *mut c_void);
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
    fn strcpy(dst: *mut c_char, src: *const c_char) -> *mut c_char;
    fn strlen(s: *const c_char) -> usize;
    fn memcmp(s1: *const c_void, s2: *const c_void, n: usize) -> c_int;
    fn sprintf(buf: *mut c_char, fmt: *const c_char, ...) -> c_int;
    fn stat(path: *const c_char, buf: *mut libc_stat) -> c_int;
}

#[inline]
unsafe fn libc_malloc(n: usize) -> *mut c_void {
    malloc(n)
}
#[inline]
unsafe fn libc_free(p: *mut c_void) {
    free(p)
}
#[inline]
unsafe fn strcmp_c(a: *const c_char, b: *const c_char) -> c_int {
    strcmp(a, b)
}
#[inline]
unsafe fn strncmp_c(a: *const c_char, b: *const c_char, n: usize) -> c_int {
    strncmp(a, b, n)
}
#[inline]
unsafe fn strcpy_c(dst: *mut c_char, src: *const c_char) {
    strcpy(dst, src);
}
#[inline]
unsafe fn strlen_c(s: *const c_char) -> usize {
    strlen(s)
}
#[inline]
unsafe fn memcmp_c(a: *const c_void, b: *const c_void, n: usize) -> c_int {
    memcmp(a, b, n)
}
#[inline]
unsafe fn pg_stat(path: *const c_char, buf: *mut libc_stat) -> c_int {
    stat(path, buf)
}
#[inline]
unsafe fn sprintf_slash(buf: *mut c_char, dir: *const c_char, base: *const c_char) {
    sprintf(buf, c"%s/%s".as_ptr(), dir, base);
}
