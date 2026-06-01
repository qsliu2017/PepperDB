//! explain_state.rs - Code for initializing and accessing ExplainState objects.
//!
//! 1:1 translation of `src/backend/commands/explain_state.c` together with the
//! `ExplainState` / `ExplainFormat` / `ExplainSerializeOption` /
//! `ExplainWorkersState` declarations from `commands/explain_state.h` that the
//! `.c` reads and writes.
//!
//! In-core options have hard-coded fields inside ExplainState; e.g. if the user
//! writes EXPLAIN (BUFFERS) then ExplainState's `buffers` member is set to true.
//! Extensions can also register options using RegisterExtensionExplainOption, so
//! that e.g. EXPLAIN (BICYCLE 'red') invokes a designated handler. Because an
//! ExplainState has no `bicycle` field, an ExplainState carries an array of
//! opaque pointers, one per extension: GetExplainExtensionId reserves an offset,
//! and Get/SetExplainExtensionState read/write that private slot.

use crate::prelude::*;

use crate::nodes::bitmapset::Bitmapset;
use crate::nodes::parsenodes::DefElem;
use crate::nodes::pg_list::{lfirst, List};
use crate::nodes::plannodes::PlannedStmt;
use crate::lib::stringinfo::{makeStringInfo, StringInfo, StringInfoData};
use crate::port::pg_bitutils::pg_nextpower2_32;
use crate::commands::define::{defGetBoolean, defGetString};
use crate::{current_cell, foreach};

// C library strcmp, bound directly (mirrors the pattern used elsewhere; the
// in-core option dispatch in ParseExplainOptionList uses strcmp, not the
// case-insensitive pg_strcasecmp, exactly as the C source does).
extern "C" {
    fn strcmp(s1: *const c_char, s2: *const c_char) -> c_int;
}

// errcode classifications referenced by the C source.  The ported ereport!
// shim takes only (level, msg), so these are dropped at the call sites; kept
// here as named stubs to document the original classification.
#[allow(dead_code)]
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;
#[allow(dead_code)]
const ERRCODE_SYNTAX_ERROR: c_int = 0;

// ----------------------------------------------------------------
// STUBbed dependencies (finest granularity).
// ----------------------------------------------------------------

/// STUB: parser/parse_node.h `ParseState` -- opaque parse-time context.  Only
/// passed through to handlers / parser_errposition (which is dropped), so an
/// opaque void is sufficient here.
pub type ParseState = c_void;

// ----------------------------------------------------------------
// Declarations merged from commands/explain_state.h
// ----------------------------------------------------------------

/// ExplainSerializeOption: whether (and how) to serialize the query's output.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ExplainSerializeOption {
    EXPLAIN_SERIALIZE_NONE,
    EXPLAIN_SERIALIZE_TEXT,
    EXPLAIN_SERIALIZE_BINARY,
}
pub use ExplainSerializeOption::*;

/// ExplainFormat: EXPLAIN output format.
#[repr(C)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ExplainFormat {
    EXPLAIN_FORMAT_TEXT,
    EXPLAIN_FORMAT_XML,
    EXPLAIN_FORMAT_JSON,
    EXPLAIN_FORMAT_YAML,
}
pub use ExplainFormat::*;

/// Per-worker output state for a parallel plan.
#[repr(C)]
pub struct ExplainWorkersState {
    pub num_workers: c_int,               /* # of worker processes the plan used */
    pub worker_inited: *mut bool,         /* per-worker state-initialized flags */
    pub worker_str: *mut StringInfoData,  /* per-worker transient output buffers */
    pub worker_state_save: *mut c_int,    /* per-worker grouping state save areas */
    pub prev_str: StringInfo,             /* saved output buffer while redirecting */
}

/// ExplainState - the running state of an EXPLAIN command.
#[repr(C)]
pub struct ExplainState {
    pub str: StringInfo, /* output buffer */
    /* options */
    pub verbose: bool,  /* be verbose */
    pub analyze: bool,  /* print actual times */
    pub costs: bool,    /* print estimated costs */
    pub buffers: bool,  /* print buffer usage */
    pub wal: bool,      /* print WAL usage */
    pub timing: bool,   /* print detailed node timing */
    pub summary: bool,  /* print total planning and execution timing */
    pub memory: bool,   /* print planner's memory usage information */
    pub settings: bool, /* print modified settings */
    pub generic: bool,  /* generate a generic plan */
    pub serialize: ExplainSerializeOption, /* serialize the query's output? */
    pub format: ExplainFormat,             /* output format */
    /* state for output formatting --- not reset for each new plan tree */
    pub indent: c_int,             /* current indentation level */
    pub grouping_stack: *mut List, /* format-specific grouping state */
    /* state related to the current plan tree (filled by ExplainPrintPlan) */
    pub pstmt: *mut PlannedStmt, /* top of plan */
    pub rtable: *mut List,       /* range table */
    pub rtable_names: *mut List, /* alias names for RTEs */
    pub deparse_cxt: *mut List,  /* context list for deparsing expressions */
    pub printed_subplans: *mut Bitmapset, /* ids of SubPlans we've printed */
    pub hide_workers: bool,      /* set if we find an invisible Gather */
    pub rtable_size: c_int,      /* length of rtable excluding the RTE_GROUP entry */
    /* state related to the current plan node */
    pub workers_state: *mut ExplainWorkersState, /* needed if parallel plan */
    /* extensions */
    pub extension_state: *mut *mut c_void,
    pub extension_state_allocated: c_int,
}

/// Handler invoked for an extension-registered EXPLAIN option.
pub type ExplainOptionHandler =
    unsafe extern "C" fn(*mut ExplainState, *mut DefElem, *mut ParseState);

/// Hook to perform additional EXPLAIN options validation.
pub type explain_validate_options_hook_type =
    unsafe extern "C" fn(es: *mut ExplainState, options: *mut List, pstate: *mut ParseState);

// ----------------------------------------------------------------
// File-scope statics (mirroring the C file-static globals).
// ----------------------------------------------------------------

/// Hook to perform additional EXPLAIN options validation.
pub static mut explain_validate_options_hook: Option<explain_validate_options_hook_type> = None;

#[repr(C)]
#[derive(Clone, Copy)]
struct ExplainExtensionOption {
    option_name: *const c_char,
    option_handler: ExplainOptionHandler,
}

static mut ExplainExtensionNameArray: *mut *const c_char = null_mut();
static mut ExplainExtensionNamesAssigned: c_int = 0;
static mut ExplainExtensionNamesAllocated: c_int = 0;

static mut ExplainExtensionOptionArray: *mut ExplainExtensionOption = null_mut();
static mut ExplainExtensionOptionsAssigned: c_int = 0;
static mut ExplainExtensionOptionsAllocated: c_int = 0;

// ----------------------------------------------------------------
// Functions
// ----------------------------------------------------------------

/// Create a new ExplainState struct initialized with default options.
pub unsafe fn NewExplainState() -> *mut ExplainState {
    let es = palloc0(core::mem::size_of::<ExplainState>()) as *mut ExplainState;

    /* Set default options (most fields can be left as zeroes). */
    (*es).costs = true;
    /* Prepare output buffer. */
    (*es).str = makeStringInfo();

    es
}

/// Parse a list of EXPLAIN options and update an ExplainState accordingly.
pub unsafe fn ParseExplainOptionList(
    es: *mut ExplainState,
    options: *mut List,
    pstate: *mut ParseState,
) {
    let mut timing_set = false;
    let mut buffers_set = false;
    let mut summary_set = false;

    /* Parse options list. */
    foreach!(lc, options, {
        let opt = lfirst(current_cell!(lc)) as *mut DefElem;

        if strcmp((*opt).defname, c"analyze".as_ptr()) == 0 {
            (*es).analyze = defGetBoolean(opt);
        } else if strcmp((*opt).defname, c"verbose".as_ptr()) == 0 {
            (*es).verbose = defGetBoolean(opt);
        } else if strcmp((*opt).defname, c"costs".as_ptr()) == 0 {
            (*es).costs = defGetBoolean(opt);
        } else if strcmp((*opt).defname, c"buffers".as_ptr()) == 0 {
            buffers_set = true;
            (*es).buffers = defGetBoolean(opt);
        } else if strcmp((*opt).defname, c"wal".as_ptr()) == 0 {
            (*es).wal = defGetBoolean(opt);
        } else if strcmp((*opt).defname, c"settings".as_ptr()) == 0 {
            (*es).settings = defGetBoolean(opt);
        } else if strcmp((*opt).defname, c"generic_plan".as_ptr()) == 0 {
            (*es).generic = defGetBoolean(opt);
        } else if strcmp((*opt).defname, c"timing".as_ptr()) == 0 {
            timing_set = true;
            (*es).timing = defGetBoolean(opt);
        } else if strcmp((*opt).defname, c"summary".as_ptr()) == 0 {
            summary_set = true;
            (*es).summary = defGetBoolean(opt);
        } else if strcmp((*opt).defname, c"memory".as_ptr()) == 0 {
            (*es).memory = defGetBoolean(opt);
        } else if strcmp((*opt).defname, c"serialize".as_ptr()) == 0 {
            if !(*opt).arg.is_null() {
                let p = defGetString(opt);

                if strcmp(p, c"off".as_ptr()) == 0 || strcmp(p, c"none".as_ptr()) == 0 {
                    (*es).serialize = EXPLAIN_SERIALIZE_NONE;
                } else if strcmp(p, c"text".as_ptr()) == 0 {
                    (*es).serialize = EXPLAIN_SERIALIZE_TEXT;
                } else if strcmp(p, c"binary".as_ptr()) == 0 {
                    (*es).serialize = EXPLAIN_SERIALIZE_BINARY;
                } else {
                    // C: ereport(ERROR, (errcode(ERRCODE_INVALID_PARAMETER_VALUE),
                    //     errmsg("unrecognized value for %s option \"%s\": \"%s\"",
                    //            "EXPLAIN", opt->defname, p), parser_errposition(...)))
                    // errcode/parser_errposition dropped; ereport! panics here.
                    ereport!(
                        ERROR,
                        errmsg!(
                            "unrecognized value for {} option \"{}\": \"{}\"",
                            "EXPLAIN",
                            cstr(  (*opt).defname ),
                            cstr(p)
                        )
                    );
                }
            } else {
                /* SERIALIZE without an argument is taken as 'text' */
                (*es).serialize = EXPLAIN_SERIALIZE_TEXT;
            }
        } else if strcmp((*opt).defname, c"format".as_ptr()) == 0 {
            let p = defGetString(opt);

            if strcmp(p, c"text".as_ptr()) == 0 {
                (*es).format = EXPLAIN_FORMAT_TEXT;
            } else if strcmp(p, c"xml".as_ptr()) == 0 {
                (*es).format = EXPLAIN_FORMAT_XML;
            } else if strcmp(p, c"json".as_ptr()) == 0 {
                (*es).format = EXPLAIN_FORMAT_JSON;
            } else if strcmp(p, c"yaml".as_ptr()) == 0 {
                (*es).format = EXPLAIN_FORMAT_YAML;
            } else {
                ereport!(
                    ERROR,
                    errmsg!(
                        "unrecognized value for {} option \"{}\": \"{}\"",
                        "EXPLAIN",
                        cstr( (*opt).defname ),
                        cstr(p)
                    )
                );
            }
        } else if !ApplyExtensionExplainOption(es, opt, pstate) {
            ereport!(
                ERROR,
                errmsg!(
                    "unrecognized {} option \"{}\"",
                    "EXPLAIN",
                    cstr( (*opt).defname )
                )
            );
        }
    });

    /* check that WAL is used with EXPLAIN ANALYZE */
    if (*es).wal && !(*es).analyze {
        ereport!(ERROR, errmsg!("EXPLAIN option {} requires ANALYZE", "WAL"));
    }

    /* if the timing was not set explicitly, set default value */
    (*es).timing = if timing_set { (*es).timing } else { (*es).analyze };

    /* if the buffers was not set explicitly, set default value */
    (*es).buffers = if buffers_set { (*es).buffers } else { (*es).analyze };

    /* check that timing is used with EXPLAIN ANALYZE */
    if (*es).timing && !(*es).analyze {
        ereport!(ERROR, errmsg!("EXPLAIN option {} requires ANALYZE", "TIMING"));
    }

    /* check that serialize is used with EXPLAIN ANALYZE */
    if (*es).serialize != EXPLAIN_SERIALIZE_NONE && !(*es).analyze {
        ereport!(ERROR, errmsg!("EXPLAIN option {} requires ANALYZE", "SERIALIZE"));
    }

    /* check that GENERIC_PLAN is not used with EXPLAIN ANALYZE */
    if (*es).generic && (*es).analyze {
        ereport!(
            ERROR,
            errmsg!(
                "{} options {} and {} cannot be used together",
                "EXPLAIN",
                "ANALYZE",
                "GENERIC_PLAN"
            )
        );
    }

    /* if the summary was not set explicitly, set default value */
    (*es).summary = if summary_set { (*es).summary } else { (*es).analyze };

    /* plugin specific option validation */
    if let Some(hook) = explain_validate_options_hook {
        hook(es, options, pstate);
    }
}

/// Map the name of an EXPLAIN extension to an integer ID.
///
/// Within the lifetime of a particular backend, the same name maps to the same
/// ID every time. IDs are not stable across backends. Use the ID returned here
/// to call Get/SetExplainExtensionState.
///
/// extension_name is assumed to be a constant string or allocated in storage
/// that will never be freed.
pub unsafe fn GetExplainExtensionId(extension_name: *const c_char) -> c_int {
    /* Search for an existing extension by this name; if found, return ID. */
    for i in 0..ExplainExtensionNamesAssigned {
        if strcmp(*ExplainExtensionNameArray.add(i as usize), extension_name) == 0 {
            return i;
        }
    }

    /* If there is no array yet, create one. */
    if ExplainExtensionNameArray.is_null() {
        ExplainExtensionNamesAllocated = 16;
        ExplainExtensionNameArray = MemoryContextAlloc(
            TopMemoryContext,
            ExplainExtensionNamesAllocated as Size * core::mem::size_of::<*const c_char>(),
        ) as *mut *const c_char;
    }

    /* If there's an array but it's currently full, expand it. */
    if ExplainExtensionNamesAssigned >= ExplainExtensionNamesAllocated {
        let i = pg_nextpower2_32((ExplainExtensionNamesAssigned + 1) as uint32) as c_int;

        ExplainExtensionNameArray = repalloc(
            ExplainExtensionNameArray as *mut c_void,
            i as Size * core::mem::size_of::<*const c_char>(),
        ) as *mut *const c_char;
        ExplainExtensionNamesAllocated = i;
    }

    /* Assign and return new ID. */
    *ExplainExtensionNameArray.add(ExplainExtensionNamesAssigned as usize) = extension_name;
    let id = ExplainExtensionNamesAssigned;
    ExplainExtensionNamesAssigned += 1;
    id
}

/// Get extension-specific state from an ExplainState.
pub unsafe fn GetExplainExtensionState(es: *mut ExplainState, extension_id: c_int) -> *mut c_void {
    Assert!(extension_id >= 0);

    if extension_id >= (*es).extension_state_allocated {
        return null_mut();
    }

    *(*es).extension_state.add(extension_id as usize)
}

/// Store extension-specific state into an ExplainState.
///
/// To use this function, first obtain an integer extension_id using
/// GetExplainExtensionId. Then use this function to store an opaque pointer;
/// later, retrieve it using GetExplainExtensionState.
pub unsafe fn SetExplainExtensionState(
    es: *mut ExplainState,
    extension_id: c_int,
    opaque: *mut c_void,
) {
    Assert!(extension_id >= 0);

    /* If there is no array yet, create one. */
    if (*es).extension_state.is_null() {
        (*es).extension_state_allocated =
            Max(16, pg_nextpower2_32((extension_id + 1) as uint32) as c_int);
        (*es).extension_state = palloc0(
            (*es).extension_state_allocated as Size * core::mem::size_of::<*mut c_void>(),
        ) as *mut *mut c_void;
    }

    /* If there's an array but it's currently full, expand it. */
    if extension_id >= (*es).extension_state_allocated {
        let i = pg_nextpower2_32((extension_id + 1) as uint32) as c_int;
        (*es).extension_state = repalloc0(
            (*es).extension_state as *mut c_void,
            (*es).extension_state_allocated as Size * core::mem::size_of::<*mut c_void>(),
            i as Size * core::mem::size_of::<*mut c_void>(),
        ) as *mut *mut c_void;
        (*es).extension_state_allocated = i;
    }

    *(*es).extension_state.add(extension_id as usize) = opaque;
}

/// Register a new EXPLAIN option.
///
/// When option_name is used as an EXPLAIN option, handler is called and should
/// update the ExplainState passed to it.
///
/// option_name is assumed to be a constant string or allocated in storage that
/// will never be freed.
pub unsafe fn RegisterExtensionExplainOption(
    option_name: *const c_char,
    handler: ExplainOptionHandler,
) {
    /* Search for an existing option by this name; if found, update handler. */
    for i in 0..ExplainExtensionOptionsAssigned {
        if strcmp(
            (*ExplainExtensionOptionArray.add(i as usize)).option_name,
            option_name,
        ) == 0
        {
            (*ExplainExtensionOptionArray.add(i as usize)).option_handler = handler;
            return;
        }
    }

    /* If there is no array yet, create one. */
    if ExplainExtensionOptionArray.is_null() {
        ExplainExtensionOptionsAllocated = 16;
        ExplainExtensionOptionArray = MemoryContextAlloc(
            TopMemoryContext,
            ExplainExtensionOptionsAllocated as Size
                * core::mem::size_of::<ExplainExtensionOption>(),
        ) as *mut ExplainExtensionOption;
    }

    /* If there's an array but it's currently full, expand it. */
    if ExplainExtensionOptionsAssigned >= ExplainExtensionOptionsAllocated {
        let i = pg_nextpower2_32((ExplainExtensionOptionsAssigned + 1) as uint32) as c_int;

        ExplainExtensionOptionArray = repalloc(
            ExplainExtensionOptionArray as *mut c_void,
            i as Size * core::mem::size_of::<ExplainExtensionOption>(),
        ) as *mut ExplainExtensionOption;
        ExplainExtensionOptionsAllocated = i;
    }

    /* Assign and return new ID. */
    let exopt = ExplainExtensionOptionArray.add(ExplainExtensionOptionsAssigned as usize);
    ExplainExtensionOptionsAssigned += 1;
    (*exopt).option_name = option_name;
    (*exopt).option_handler = handler;
}

/// Apply an EXPLAIN option registered by an extension.
///
/// If no extension has registered the named option, returns false. Otherwise,
/// calls the appropriate handler function and then returns true.
pub unsafe fn ApplyExtensionExplainOption(
    es: *mut ExplainState,
    opt: *mut DefElem,
    pstate: *mut ParseState,
) -> bool {
    for i in 0..ExplainExtensionOptionsAssigned {
        if strcmp(
            (*ExplainExtensionOptionArray.add(i as usize)).option_name,
            (*opt).defname,
        ) == 0
        {
            ((*ExplainExtensionOptionArray.add(i as usize)).option_handler)(es, opt, pstate);
            return true;
        }
    }

    false
}

// Helper: render a NUL-terminated C string for the Rust-formatted errmsg!.
// A *const c_char cannot be {}-formatted directly, so convert to &str (lossy).
unsafe fn cstr<'a>(s: *const c_char) -> &'a str {
    if s.is_null() {
        return "(null)";
    }
    core::ffi::CStr::from_ptr(s).to_str().unwrap_or("(invalid utf8)")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::nodes::parsenodes::DefElem;
    use crate::nodes::nodes::NodeTag;
    use crate::nodes::pg_list::{lappend, List};
    use crate::nodes::value::makeString;

    // Tests touch the process-global extension registry indirectly (via
    // ParseExplainOptionList -> ApplyExtensionExplainOption read path), and
    // rely on palloc; serialize to be safe.
    static LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    // Build a boolean-style DefElem (arg == NULL means "true" to defGetBoolean).
    unsafe fn make_defelem(name: &core::ffi::CStr, arg: *mut crate::nodes::nodes::Node) -> *mut DefElem {
        let d = palloc0(core::mem::size_of::<DefElem>()) as *mut DefElem;
        (*d).r#type = NodeTag::T_DefElem;
        (*d).defname = pstrdup(name.as_ptr());
        (*d).arg = arg;
        (*d).location = -1;
        d
    }

    #[test]
    fn new_explain_state_defaults() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            let es = NewExplainState();
            assert_eq!((*es).costs, true);
            assert_eq!((*es).format, EXPLAIN_FORMAT_TEXT);
            assert_eq!((*es).analyze, false);
            assert_eq!((*es).verbose, false);
            assert_eq!((*es).serialize, EXPLAIN_SERIALIZE_NONE);
            assert!(!(*es).str.is_null());
        }
    }

    #[test]
    fn parse_analyze_and_verbose() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            let es = NewExplainState();

            // [analyze = true, verbose]  (verbose with NULL arg => true)
            let analyze = make_defelem(c"analyze", makeString(pstrdup(c"true".as_ptr())) as *mut _);
            let verbose = make_defelem(c"verbose", null_mut());

            let mut options: *mut List = null_mut();
            options = lappend(options, analyze as *mut c_void);
            options = lappend(options, verbose as *mut c_void);

            ParseExplainOptionList(es, options, null_mut());

            assert_eq!((*es).analyze, true);
            assert_eq!((*es).verbose, true);
            // timing/buffers/summary default to analyze when unset
            assert_eq!((*es).timing, true);
            assert_eq!((*es).buffers, true);
            assert_eq!((*es).summary, true);
        }
    }

    #[test]
    fn parse_format_json() {
        let _g = LOCK.lock().unwrap();
        unsafe {
            let es = NewExplainState();
            let fmt = make_defelem(c"format", makeString(pstrdup(c"json".as_ptr())) as *mut _);
            let mut options: *mut List = null_mut();
            options = lappend(options, fmt as *mut c_void);
            ParseExplainOptionList(es, options, null_mut());
            assert_eq!((*es).format, EXPLAIN_FORMAT_JSON);
        }
    }
}
