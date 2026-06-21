//! utils/misc/help_config.c - display GUC options under the grand unified configuration scheme.
//!
//! Options whose flag bits are set to GUC_NO_SHOW_ALL, GUC_NOT_IN_SAMPLE,
//! or GUC_DISALLOW_IN_FILE are not displayed, unless the user specifically
//! requests that variable by name.

use crate::prelude::*;
use std::ffi::CStr;
use std::process::exit;

// ---------------------------------------------------------------------------
// GUC flag bits (utils/guc.h). Not yet ported; defined locally.
// ---------------------------------------------------------------------------
const GUC_NO_SHOW_ALL: c_int = 0x000004; // exclude from SHOW ALL
const GUC_NOT_IN_SAMPLE: c_int = 0x000080; // not in postgresql.conf.sample
const GUC_DISALLOW_IN_FILE: c_int = 0x000100; // can't set in postgresql.conf

// ---------------------------------------------------------------------------
// GUC table types (utils/guc_tables.h). Not yet ported; minimal local mirrors
// of just the fields this file touches. The typed config structs all embed
// config_generic ("gen") as their first member, which is what makes the C
// mixedStruct union punning valid.
// ---------------------------------------------------------------------------

// enum config_type
const PGC_BOOL: c_int = 0;
const PGC_INT: c_int = 1;
const PGC_REAL: c_int = 2;
const PGC_STRING: c_int = 3;
const PGC_ENUM: c_int = 4;

type GucContext = c_int;

#[repr(C)]
pub struct config_generic {
    pub name: *const c_char,
    pub context: GucContext,
    pub group: c_int, // enum config_group
    pub short_desc: *const c_char,
    pub long_desc: *const c_char,
    pub flags: c_int,
    pub vartype: c_int, // enum config_type
    // remaining runtime fields elided; not accessed here.
}

#[repr(C)]
pub struct config_bool {
    pub gen: config_generic,
    pub variable: *mut bool,
    pub boot_val: bool,
    // hooks elided
    pub reset_val: bool,
}

#[repr(C)]
pub struct config_int {
    pub gen: config_generic,
    pub variable: *mut c_int,
    pub boot_val: c_int,
    pub min: c_int,
    pub max: c_int,
    // hooks elided
    pub reset_val: c_int,
}

#[repr(C)]
pub struct config_real {
    pub gen: config_generic,
    pub variable: *mut f64,
    pub boot_val: f64,
    pub min: f64,
    pub max: f64,
    // hooks elided
    pub reset_val: f64,
}

#[repr(C)]
pub struct config_string {
    pub gen: config_generic,
    pub variable: *mut *mut c_char,
    pub boot_val: *const c_char,
    // hooks elided
    pub reset_val: *mut c_char,
}

#[repr(C)]
pub struct config_enum {
    pub gen: config_generic,
    pub variable: *mut c_int,
    pub boot_val: c_int,
    // options + hooks elided
    pub reset_val: c_int,
}

// This union allows us to mix the numerous different types of structs
// that we are organizing.  As in C, every member shares the leading
// config_generic, so we just keep a raw config_generic pointer and cast.
type mixedStruct = config_generic;

// ---------------------------------------------------------------------------
// Stubs for not-yet-ported GUC machinery (utils/guc_tables.h, utils/guc.c).
// ---------------------------------------------------------------------------

// TODO: not ported - utils/guc.c build_guc_variables()
unsafe fn build_guc_variables() { crate::utils::misc::guc::build_guc_variables() }

// TODO: not ported - utils/guc.c get_guc_variables()
unsafe fn get_guc_variables(num_vars: *mut c_int) -> *mut *mut config_generic {
    let _ = num_vars;
    unimplemented!()
}

// TODO: not ported - utils/guc.c config_enum_lookup_by_value()
unsafe fn config_enum_lookup_by_value(record: *mut config_enum, val: c_int) -> *const c_char {
    let _ = (record, val);
    unimplemented!()
}

// TODO: not ported - utils/guc_tables.c GucContext_Names[]
unsafe fn GucContext_Names(context: GucContext) -> *const c_char {
    let _ = context;
    unimplemented!()
}

// TODO: not ported - utils/guc_tables.c config_group_names[]
unsafe fn config_group_names(group: c_int) -> *const c_char {
    let _ = group;
    unimplemented!()
}

// _() is the NLS gettext wrapper; we pass the string through unchanged.
unsafe fn gettext_(s: *const c_char) -> *const c_char {
    s
}

// TODO: not ported - port write_stderr(); route through low-level stderr write.
unsafe fn write_stderr(msg: &str) {
    eprint!("{}", msg);
}

// Render a possibly-NULL C string for printf; returns "" for NULL.
unsafe fn cstr(p: *const c_char) -> String {
    if p.is_null() {
        String::new()
    } else {
        CStr::from_ptr(p).to_string_lossy().into_owned()
    }
}

// ---------------------------------------------------------------------------
// help_config.c proper
// ---------------------------------------------------------------------------

pub unsafe fn GucInfoMain() {
    let guc_vars: *mut *mut config_generic;
    let mut numOpts: c_int = 0;

    // Initialize the GUC hash table
    build_guc_variables();

    guc_vars = get_guc_variables(&mut numOpts);

    let mut i: c_int = 0;
    while i < numOpts {
        let var = *guc_vars.offset(i as isize) as *mut mixedStruct;

        if displayStruct(var) {
            printMixedStruct(var);
        }
        i += 1;
    }

    exit(0);
}

// This function will return true if the struct passed to it
// should be displayed to the user.
unsafe fn displayStruct(structToDisplay: *mut mixedStruct) -> bool {
    ((*structToDisplay).flags & (GUC_NO_SHOW_ALL | GUC_NOT_IN_SAMPLE | GUC_DISALLOW_IN_FILE)) == 0
}

// This function prints out the generic struct passed to it. It will print out
// a different format, depending on what the user wants to see.
unsafe fn printMixedStruct(structToPrint: *mut mixedStruct) {
    let generic = structToPrint;

    print!(
        "{}\t{}\t{}\t",
        cstr((*generic).name),
        cstr(GucContext_Names((*generic).context)),
        cstr(gettext_(config_group_names((*generic).group))),
    );

    match (*generic).vartype {
        PGC_BOOL => {
            let _bool = structToPrint as *mut config_bool;
            print!(
                "BOOLEAN\t{}\t\t\t",
                if !(*_bool).reset_val { "FALSE" } else { "TRUE" }
            );
        }

        PGC_INT => {
            let integer = structToPrint as *mut config_int;
            print!(
                "INTEGER\t{}\t{}\t{}\t",
                (*integer).reset_val,
                (*integer).min,
                (*integer).max
            );
        }

        PGC_REAL => {
            let real = structToPrint as *mut config_real;
            print!(
                "REAL\t{}\t{}\t{}\t",
                fmt_g((*real).reset_val),
                fmt_g((*real).min),
                fmt_g((*real).max)
            );
        }

        PGC_STRING => {
            let string = structToPrint as *mut config_string;
            let bv = if !(*string).boot_val.is_null() {
                cstr((*string).boot_val)
            } else {
                String::new()
            };
            print!("STRING\t{}\t\t\t", bv);
        }

        PGC_ENUM => {
            let _enum = structToPrint as *mut config_enum;
            print!(
                "ENUM\t{}\t\t\t",
                cstr(config_enum_lookup_by_value(_enum, (*_enum).boot_val))
            );
        }

        _ => {
            write_stderr("internal error: unrecognized run-time parameter type\n");
        }
    }

    let short_desc = if (*generic).short_desc.is_null() {
        String::new()
    } else {
        cstr(gettext_((*generic).short_desc))
    };
    let long_desc = if (*generic).long_desc.is_null() {
        String::new()
    } else {
        cstr(gettext_((*generic).long_desc))
    };
    println!("{}\t{}", short_desc, long_desc);
}

// Mimic printf's "%g" formatting for a double.
fn fmt_g(v: f64) -> String {
    // Rust's default float Display is close to %g for our purposes (shortest
    // round-trippable representation without trailing zeros).
    let s = format!("{}", v);
    s
}
