//! Translation of postgres/src/include/nodes/extensible.h
//!                + postgres/src/backend/nodes/extensible.c
//!
//! Support for extensible node types and custom scans. Loadable modules define
//! new node types flagged T_ExtensibleNode (distinguished by `extnodename`), and
//! custom scan/path/exec methods, registered in name-keyed hash tables.

use crate::prelude::*;
use crate::lib::stringinfo::StringInfoData;
use crate::nodes::execnodes::{CustomScanState, EState, TupleTableSlot};
use crate::nodes::nodes::{Node, NodeTag};
use crate::nodes::pg_list::List;
use crate::nodes::plannodes::{CustomScan, Plan};
use crate::nodes::pathnodes::{CustomPath, PlannerInfo, RelOptInfo};
use crate::utils::hash::dynahash::{
    hash_create, hash_search, HASHCTL, HTAB, HASH_ELEM, HASH_FIND, HASH_STRINGS,
};
use crate::utils::hash::dynahash::HASHACTION::HASH_ENTER;
use core::ffi::{c_char, c_int, c_void};

/// maximum length of an extensible node identifier
pub const EXTNODENAME_MAX_LEN: usize = 64;

// Opaque forward types from headers not yet translated.
/// TODO(pg-port): access/parallel.h
#[repr(C)]
pub struct ParallelContext {
    _opaque: [u8; 0],
}
/// TODO(pg-port): storage/shm_toc.h
#[repr(C)]
pub struct shm_toc {
    _opaque: [u8; 0],
}
/// TODO(pg-port): commands/explain_state.h
#[repr(C)]
pub struct ExplainState {
    _opaque: [u8; 0],
}

/// An extensible node: type is always T_ExtensibleNode; `extnodename` identifies
/// the specific type (looked up to find its ExtensibleNodeMethods).
#[repr(C)]
pub struct ExtensibleNode {
    pub r#type: NodeTag,
    /// identifier of ExtensibleNodeMethods
    pub extnodename: *const c_char,
}

/// Callbacks implementing copy/equal/out/read for an extensible node type.
/// All callbacks are mandatory.
#[repr(C)]
pub struct ExtensibleNodeMethods {
    pub extnodename: *const c_char,
    pub node_size: Size,
    pub nodeCopy:
        Option<unsafe fn(newnode: *mut ExtensibleNode, oldnode: *const ExtensibleNode)>,
    pub nodeEqual:
        Option<unsafe fn(a: *const ExtensibleNode, b: *const ExtensibleNode) -> bool>,
    pub nodeOut: Option<unsafe fn(str: *mut StringInfoData, node: *const ExtensibleNode)>,
    pub nodeRead: Option<unsafe fn(node: *mut ExtensibleNode)>,
}

// Flags for custom paths (bitmask in CustomPath/CustomScan .flags).
pub const CUSTOMPATH_SUPPORT_BACKWARD_SCAN: c_int = 0x0001;
pub const CUSTOMPATH_SUPPORT_MARK_RESTORE: c_int = 0x0002;
pub const CUSTOMPATH_SUPPORT_PROJECTION: c_int = 0x0004;

/// Custom path methods: mainly how to convert a CustomPath to a Plan.
#[repr(C)]
pub struct CustomPathMethods {
    pub CustomName: *const c_char,
    pub PlanCustomPath: Option<
        unsafe fn(
            root: *mut PlannerInfo,
            rel: *mut RelOptInfo,
            best_path: *mut CustomPath,
            tlist: *mut List,
            clauses: *mut List,
            custom_plans: *mut List,
        ) -> *mut Plan,
    >,
    pub ReparameterizeCustomPathByChild: Option<
        unsafe fn(
            root: *mut PlannerInfo,
            custom_private: *mut List,
            child_rel: *mut RelOptInfo,
        ) -> *mut List,
    >,
}

/// Custom scan methods: how to make a ScanState from a CustomScan plan node.
#[repr(C)]
pub struct CustomScanMethods {
    pub CustomName: *const c_char,
    pub CreateCustomScanState: Option<unsafe fn(cscan: *mut CustomScan) -> *mut Node>,
}

/// Execution-time methods for a CustomScanState.
#[repr(C)]
pub struct CustomExecMethods {
    pub CustomName: *const c_char,
    /* Required executor methods */
    pub BeginCustomScan:
        Option<unsafe fn(node: *mut CustomScanState, estate: *mut EState, eflags: c_int)>,
    pub ExecCustomScan: Option<unsafe fn(node: *mut CustomScanState) -> *mut TupleTableSlot>,
    pub EndCustomScan: Option<unsafe fn(node: *mut CustomScanState)>,
    pub ReScanCustomScan: Option<unsafe fn(node: *mut CustomScanState)>,
    /* Optional: mark/restore */
    pub MarkPosCustomScan: Option<unsafe fn(node: *mut CustomScanState)>,
    pub RestrPosCustomScan: Option<unsafe fn(node: *mut CustomScanState)>,
    /* Optional: parallel execution */
    pub EstimateDSMCustomScan:
        Option<unsafe fn(node: *mut CustomScanState, pcxt: *mut ParallelContext) -> Size>,
    pub InitializeDSMCustomScan: Option<
        unsafe fn(node: *mut CustomScanState, pcxt: *mut ParallelContext, coordinate: *mut c_void),
    >,
    pub ReInitializeDSMCustomScan: Option<
        unsafe fn(node: *mut CustomScanState, pcxt: *mut ParallelContext, coordinate: *mut c_void),
    >,
    pub InitializeWorkerCustomScan:
        Option<unsafe fn(node: *mut CustomScanState, toc: *mut shm_toc, coordinate: *mut c_void)>,
    pub ShutdownCustomScan: Option<unsafe fn(node: *mut CustomScanState)>,
    /* Optional: EXPLAIN */
    pub ExplainCustomScan:
        Option<unsafe fn(node: *mut CustomScanState, ancestors: *mut List, es: *mut ExplainState)>,
}

// ---- implementation (extensible.c) ----

static mut extensible_node_methods: *mut HTAB = core::ptr::null_mut();
static mut custom_scan_methods: *mut HTAB = core::ptr::null_mut();

#[repr(C)]
struct ExtensibleNodeEntry {
    extnodename: [c_char; EXTNODENAME_MAX_LEN],
    extnodemethods: *const c_void,
}

/// An internal function to register a new callback structure.
///
/// # Safety
/// `extnodename` must be a valid C string; `extnodemethods` must outlive the registry.
unsafe fn RegisterExtensibleNodeEntry(
    p_htable: *mut *mut HTAB,
    htable_label: *const c_char,
    extnodename: *const c_char,
    extnodemethods: *const c_void,
) {
    let entry: *mut ExtensibleNodeEntry;
    let mut found: bool = false;

    if (*p_htable).is_null() {
        let mut ctl: HASHCTL = core::mem::zeroed();

        ctl.keysize = EXTNODENAME_MAX_LEN;
        ctl.entrysize = core::mem::size_of::<ExtensibleNodeEntry>();

        *p_htable = hash_create(htable_label, 100, &ctl, HASH_ELEM | HASH_STRINGS);
    }

    if strlen(extnodename) >= EXTNODENAME_MAX_LEN {
        elog!(ERROR, "extensible node name is too long");
    }

    entry = hash_search(
        *p_htable,
        extnodename as *const c_void,
        HASH_ENTER,
        &mut found,
    ) as *mut ExtensibleNodeEntry;
    if found {
        ereport!(
            ERROR,
            errmsg!(
                "extensible node type \"{}\" already exists",
                cstr_to_str(extnodename)
            )
        );
    }

    (*entry).extnodemethods = extnodemethods;
}

/// Register a new type of extensible node.
///
/// # Safety
/// `methods` must point to a static ExtensibleNodeMethods with a valid extnodename.
pub unsafe fn RegisterExtensibleNodeMethods(methods: *const ExtensibleNodeMethods) {
    RegisterExtensibleNodeEntry(
        &raw mut extensible_node_methods,
        c"Extensible Node Methods".as_ptr(),
        (*methods).extnodename,
        methods as *const c_void,
    );
}

/// Register a new type of custom scan node.
///
/// # Safety
/// See [`RegisterExtensibleNodeMethods`].
pub unsafe fn RegisterCustomScanMethods(methods: *const CustomScanMethods) {
    RegisterExtensibleNodeEntry(
        &raw mut custom_scan_methods,
        c"Custom Scan Methods".as_ptr(),
        (*methods).CustomName,
        methods as *const c_void,
    );
}

/// An internal routine to get an ExtensibleNodeEntry by the given identifier.
///
/// # Safety
/// `extnodename` must be a valid C string.
unsafe fn GetExtensibleNodeEntry(
    htable: *mut HTAB,
    extnodename: *const c_char,
    missing_ok: bool,
) -> *const c_void {
    let mut entry: *mut ExtensibleNodeEntry = core::ptr::null_mut();

    if !htable.is_null() {
        entry = hash_search(
            htable,
            extnodename as *const c_void,
            HASH_FIND,
            core::ptr::null_mut(),
        ) as *mut ExtensibleNodeEntry;
    }
    if entry.is_null() {
        if missing_ok {
            return core::ptr::null();
        }
        ereport!(
            ERROR,
            errmsg!(
                "ExtensibleNodeMethods \"{}\" was not registered",
                cstr_to_str(extnodename)
            )
        );
    }

    (*entry).extnodemethods
}

/// Get the methods for a given type of extensible node.
///
/// # Safety
/// `extnodename` must be a valid C string.
pub unsafe fn GetExtensibleNodeMethods(
    extnodename: *const c_char,
    missing_ok: bool,
) -> *const ExtensibleNodeMethods {
    GetExtensibleNodeEntry(extensible_node_methods, extnodename, missing_ok)
        as *const ExtensibleNodeMethods
}

/// Get the methods for a given name of CustomScanMethods.
///
/// # Safety
/// `CustomName` must be a valid C string.
pub unsafe fn GetCustomScanMethods(
    CustomName: *const c_char,
    missing_ok: bool,
) -> *const CustomScanMethods {
    GetExtensibleNodeEntry(custom_scan_methods, CustomName, missing_ok) as *const CustomScanMethods
}

/// Minimal libc-style strlen over a C string.
///
/// # Safety
/// `s` must be a valid NUL-terminated C string.
unsafe fn strlen(s: *const c_char) -> usize {
    let mut n = 0usize;
    while *s.add(n) != 0 {
        n += 1;
    }
    n
}

/// Render a C string for an error message (best-effort, lossy).
///
/// # Safety
/// `s` must be a valid NUL-terminated C string.
unsafe fn cstr_to_str(s: *const c_char) -> std::string::String {
    let bytes = core::slice::from_raw_parts(s as *const u8, strlen(s));
    std::string::String::from_utf8_lossy(bytes).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    unsafe fn name_of(methods: *const CustomScanMethods) -> *const c_char {
        (*methods).CustomName
    }

    #[test]
    fn register_and_lookup_custom_scan_methods() {
        unsafe {
            // Reset the process-global registry for a deterministic test run.
            custom_scan_methods = core::ptr::null_mut();

            // A local (not a `static`, which would require Sync for the raw ptr);
            // it outlives the registry lookups below.
            let m = CustomScanMethods {
                CustomName: c"my_scan".as_ptr(),
                CreateCustomScanState: None,
            };
            RegisterCustomScanMethods(&m);

            let found = GetCustomScanMethods(c"my_scan".as_ptr(), false);
            assert!(!found.is_null());
            assert_eq!(name_of(found), name_of(&m));

            // missing_ok lookup of an unregistered name returns null.
            let miss = GetCustomScanMethods(c"nope".as_ptr(), true);
            assert!(miss.is_null());
        }
    }
}
