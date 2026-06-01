//! utils/adt/mcxtfuncs.c - Functions to show backend memory context.

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_void};

use crate::catalog::pg_type_d::INT4OID;
use crate::mb::mbutils::pg_mbcliplen;
use crate::nodes::execnodes::ReturnSetInfo;
use crate::nodes::nodes::NodeTag;
use crate::nodes::pg_list::{
    lappend, lcons_int, list_free, list_length, List, ListCell, NIL,
};
use crate::utils::array::ArrayType;
use crate::utils::builtins::CStringGetTextDatum;
use crate::utils::fmgr::FunctionCallInfo;
use crate::utils::hash::dynahash::{
    hash_create, hash_destroy, hash_search, HASHACTION, HASHCTL, HTAB, HASH_BLOBS, HASH_CONTEXT,
    HASH_ELEM,
};
// The prelude glob-imports palloc's `MemoryContext`/`MemoryContextData`
// (bootstrap layout) and `MemoryContextIsValid`.  The real context layout
// (parent/firstchild/methods/type/...) lives in memnodes; we use that one for
// all context pointers in this file under a local alias `MemoryContext`, and
// reach the real MemoryContextIsValid fully-qualified.
use crate::utils::mmgr::memnodes::{
    MemoryContextCounters, MemoryContextData,
};

// Shadow the prelude's palloc `MemoryContext` alias with the memnodes one so the
// struct-field accesses below see the real layout.
#[allow(non_camel_case_types)]
type MemoryContext = *mut MemoryContextData;
// Note: `Assert`/`elog`/`ereport` and Int32GetDatum/Int64GetDatum/PointerGetDatum
// already come from the prelude (the latter via `crate::postgres::*`).
use crate::{list_make1, PG_GETARG_INT32, PG_RETURN_BOOL};

/* ----------
 * The max bytes for showing identifiers of MemoryContext.
 * ----------
 */
const MEMORY_CONTEXT_IDENT_DISPLAY_SIZE: usize = 1024;

/*
 * MemoryContextId
 *		Used for storage of transient identifiers for
 *		pg_get_backend_memory_contexts.
 */
#[repr(C)]
struct MemoryContextId {
    context: MemoryContext,
    context_id: c_int,
}

// <string.h> - strcmp/strlen, bound via extern "C".
extern "C" {
    fn strcmp(a: *const c_char, b: *const c_char) -> c_int;
    fn strlen(s: *const c_char) -> usize;
}

// ---- Stubs for not-yet-ported called functions/types ----

// storage/proc.h - PGPROC is opaque here; we only carry the pointer through.
#[allow(non_camel_case_types)]
type PGPROC = c_void;

// storage/procnumber.h - ProcNumber and the invalid sentinel.
type ProcNumber = c_int;
const INVALID_PROC_NUMBER: ProcNumber = -1;

// storage/procsignal.h - ProcSignalReason; PROCSIG_LOG_MEMORY_CONTEXT value.
// TODO: replace once storage/procsignal.h is ported.
type ProcSignalReason = c_int;
const PROCSIG_LOG_MEMORY_CONTEXT: ProcSignalReason = 0;

// utils/array.h - construct_array_builtin.
// TODO: port construct_array_builtin (src/backend/utils/adt/arrayfuncs.c)
unsafe fn construct_array_builtin(
    _elems: *mut Datum,
    _nelems: c_int,
    _elmtype: Oid,
) -> *mut ArrayType {
    unimplemented!()
}

// utils/fmgr/funcapi.c - InitMaterializedSRF.
// TODO: port InitMaterializedSRF (src/backend/utils/fmgr/funcapi.c)
unsafe fn InitMaterializedSRF(_fcinfo: FunctionCallInfo, _flags: c_int) {
    unimplemented!()
}

// utils/sort/tuplestore.c - tuplestore_putvalues.
// TODO: port tuplestore_putvalues (src/backend/utils/sort/tuplestore.c)
unsafe fn tuplestore_putvalues(
    _state: *mut Tuplestorestate,
    _tdesc: TupleDesc,
    _values: *mut Datum,
    _isnull: *mut bool,
) {
    unimplemented!()
}

// storage/proc.h - BackendPidGetProc.
// TODO: port BackendPidGetProc (src/backend/storage/lmgr/proc.c)
unsafe fn BackendPidGetProc(_pid: c_int) -> *mut PGPROC {
    unimplemented!()
}

// storage/proc.h - AuxiliaryPidGetProc.
// TODO: port AuxiliaryPidGetProc (src/backend/storage/lmgr/proc.c)
unsafe fn AuxiliaryPidGetProc(_pid: c_int) -> *mut PGPROC {
    unimplemented!()
}

// storage/proc.h - GetNumberFromPGProc.
// TODO: port GetNumberFromPGProc (storage/proc.h macro/inline)
unsafe fn GetNumberFromPGProc(_proc: *mut PGPROC) -> ProcNumber {
    unimplemented!()
}

// storage/procsignal.h - SendProcSignal.
// TODO: port SendProcSignal (src/backend/storage/ipc/procsignal.c)
unsafe fn SendProcSignal(
    _pid: c_int,
    _reason: ProcSignalReason,
    _procNumber: ProcNumber,
) -> c_int {
    unimplemented!()
}

// Opaque/aliased types used through pointers only (until centrally ported).
#[allow(non_camel_case_types)]
pub enum Tuplestorestate {}
#[allow(non_camel_case_types)]
pub type TupleDesc = *mut c_void;

/*
 * int_list_to_array
 *		Convert an IntList to an array of INT4OIDs.
 */
unsafe fn int_list_to_array(list: *const List) -> Datum {
    let length: c_int = list_length(list);
    let datum_array: *mut Datum = palloc(length as usize * std::mem::size_of::<Datum>()) as *mut Datum;

    // foreach_int(i, list) datum_array[foreach_current_index(i)] = Int32GetDatum(i);
    let mut idx: c_int = 0;
    while !list.is_null() && idx < (*list).length {
        let cell: *mut ListCell = (*list).elements.add(idx as usize);
        let i: c_int = (*cell).int_value;
        *datum_array.add(idx as usize) = Int32GetDatum(i);
        idx += 1;
    }

    let result_array: *mut ArrayType = construct_array_builtin(datum_array, length, INT4OID);

    PointerGetDatum(result_array as *const c_void)
}

/*
 * PutMemoryContextsStatsTupleStore
 *		Add details for the given MemoryContext to 'tupstore'.
 */
unsafe fn PutMemoryContextsStatsTupleStore(
    tupstore: *mut Tuplestorestate,
    tupdesc: TupleDesc,
    context: MemoryContext,
    context_id_lookup: *mut HTAB,
) {
    const PG_GET_BACKEND_MEMORY_CONTEXTS_COLS: usize = 10;

    let mut values: [Datum; PG_GET_BACKEND_MEMORY_CONTEXTS_COLS] =
        [0; PG_GET_BACKEND_MEMORY_CONTEXTS_COLS];
    let mut nulls: [bool; PG_GET_BACKEND_MEMORY_CONTEXTS_COLS] =
        [false; PG_GET_BACKEND_MEMORY_CONTEXTS_COLS];
    let mut stat: MemoryContextCounters = std::mem::zeroed();
    let mut path: *mut List = NIL;
    let mut name: *const c_char;
    let mut ident: *const c_char;
    let r#type: *const c_char;

    Assert!(crate::utils::mmgr::memnodes::MemoryContextIsValid(context));

    /*
     * Figure out the transient context_id of this context and each of its
     * ancestors.
     */
    let mut cur: MemoryContext = context;
    while !cur.is_null() {
        let mut found: bool = false;

        let entry: *mut MemoryContextId = hash_search(
            context_id_lookup,
            &cur as *const MemoryContext as *const c_void,
            HASHACTION::HASH_FIND,
            &mut found,
        ) as *mut MemoryContextId;

        if !found {
            elog!(ERROR, "hash table corrupted");
        }
        path = lcons_int((*entry).context_id, path);

        cur = (*cur).parent;
    }

    /* Examine the context itself */
    // memset(&stat, 0, sizeof(stat)) done above via zeroed().
    let stats_fn = (*(*context).methods).stats.unwrap();
    stats_fn(context, None, std::ptr::null_mut(), &mut stat, true);

    // memset(values, 0, ...) / memset(nulls, 0, ...) done above.

    name = (*context).name;
    ident = (*context).ident;

    /*
     * To be consistent with logging output, we label dynahash contexts with
     * just the hash table name as with MemoryContextStatsPrint().
     */
    if !ident.is_null() && strcmp(name, b"dynahash\0".as_ptr() as *const c_char) == 0 {
        name = ident;
        ident = std::ptr::null();
    }

    if !name.is_null() {
        values[0] = CStringGetTextDatum(name);
    } else {
        nulls[0] = true;
    }

    if !ident.is_null() {
        let mut idlen: c_int = strlen(ident) as c_int;
        let mut clipped_ident: [c_char; MEMORY_CONTEXT_IDENT_DISPLAY_SIZE] =
            [0; MEMORY_CONTEXT_IDENT_DISPLAY_SIZE];

        /*
         * Some identifiers such as SQL query string can be very long,
         * truncate oversize identifiers.
         */
        if idlen >= MEMORY_CONTEXT_IDENT_DISPLAY_SIZE as c_int {
            idlen = pg_mbcliplen(ident, idlen, MEMORY_CONTEXT_IDENT_DISPLAY_SIZE as c_int - 1);
        }

        std::ptr::copy_nonoverlapping(ident, clipped_ident.as_mut_ptr(), idlen as usize);
        clipped_ident[idlen as usize] = 0;
        values[1] = CStringGetTextDatum(clipped_ident.as_ptr());
    } else {
        nulls[1] = true;
    }

    match (*context).r#type {
        NodeTag::T_AllocSetContext => {
            r#type = b"AllocSet\0".as_ptr() as *const c_char;
        }
        NodeTag::T_GenerationContext => {
            r#type = b"Generation\0".as_ptr() as *const c_char;
        }
        NodeTag::T_SlabContext => {
            r#type = b"Slab\0".as_ptr() as *const c_char;
        }
        NodeTag::T_BumpContext => {
            r#type = b"Bump\0".as_ptr() as *const c_char;
        }
        _ => {
            r#type = b"???\0".as_ptr() as *const c_char;
        }
    }

    values[2] = CStringGetTextDatum(r#type);
    values[3] = Int32GetDatum(list_length(path)); /* level */
    values[4] = int_list_to_array(path);
    values[5] = Int64GetDatum(stat.totalspace as int64);
    values[6] = Int64GetDatum(stat.nblocks as int64);
    values[7] = Int64GetDatum(stat.freespace as int64);
    values[8] = Int64GetDatum(stat.freechunks as int64);
    values[9] = Int64GetDatum((stat.totalspace - stat.freespace) as int64);

    tuplestore_putvalues(tupstore, tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
    list_free(path);
}

/*
 * pg_get_backend_memory_contexts
 *		SQL SRF showing backend memory context.
 */
pub unsafe fn pg_get_backend_memory_contexts(fcinfo: FunctionCallInfo) -> Datum {
    let rsinfo: *mut ReturnSetInfo = (*fcinfo).resultinfo as *mut ReturnSetInfo;
    let mut context_id: c_int;
    let mut contexts: *mut List;
    let mut ctl: HASHCTL = std::mem::zeroed();
    let context_id_lookup: *mut HTAB;

    ctl.keysize = std::mem::size_of::<MemoryContext>();
    ctl.entrysize = std::mem::size_of::<MemoryContextId>();
    ctl.hcxt = CurrentMemoryContext as *mut _;

    context_id_lookup = hash_create(
        b"pg_get_backend_memory_contexts\0".as_ptr() as *const c_char,
        256,
        &ctl,
        HASH_ELEM | HASH_BLOBS | HASH_CONTEXT,
    );

    InitMaterializedSRF(fcinfo, 0);

    /*
     * Here we use a non-recursive algorithm to visit all MemoryContexts
     * starting with TopMemoryContext.  The reason we avoid using a recursive
     * algorithm is because we want to assign the context_id breadth-first.
     * I.e. all contexts at level 1 are assigned IDs before contexts at level
     * 2.  Because contexts closer to TopMemoryContext are less likely to
     * change, this makes the assigned context_id more stable.  Otherwise, if
     * the first child of TopMemoryContext obtained an additional grandchild,
     * the context_id for the second child of TopMemoryContext would change.
     */
    contexts = list_make1!(TopMemoryContext);

    /* TopMemoryContext will always have a context_id of 1 */
    context_id = 1;

    // foreach_ptr(MemoryContextData, cur, contexts) -- with contexts appended
    // to inside the loop, so we re-read the (possibly reallocated) list each
    // iteration by index.
    let mut idx: c_int = 0;
    while !contexts.is_null() && idx < (*contexts).length {
        let cell: *mut ListCell = (*contexts).elements.add(idx as usize);
        let cur: *mut MemoryContextData = (*cell).ptr_value as *mut MemoryContextData;

        let mut found: bool = false;

        /*
         * Record the context_id that we've assigned to each MemoryContext.
         * PutMemoryContextsStatsTupleStore needs this to populate the "path"
         * column with the parent context_ids.
         */
        let entry: *mut MemoryContextId = hash_search(
            context_id_lookup,
            &cur as *const *mut MemoryContextData as *const c_void,
            HASHACTION::HASH_ENTER,
            &mut found,
        ) as *mut MemoryContextId;
        (*entry).context_id = context_id;
        context_id += 1;
        Assert!(!found);

        PutMemoryContextsStatsTupleStore(
            (*rsinfo).setResult as *mut Tuplestorestate,
            (*rsinfo).setDesc as TupleDesc,
            cur,
            context_id_lookup,
        );

        /*
         * Append all children onto the contexts list so they're processed by
         * subsequent iterations.
         */
        let mut c: MemoryContext = (*cur).firstchild;
        while !c.is_null() {
            contexts = lappend(contexts, c as *mut c_void);
            c = (*c).nextchild;
        }

        idx += 1;
    }

    hash_destroy(context_id_lookup);

    0 as Datum
}

/*
 * pg_log_backend_memory_contexts
 *		Signal a backend or an auxiliary process to log its memory contexts.
 *
 * By default, only superusers are allowed to signal to log the memory
 * contexts because allowing any users to issue this request at an unbounded
 * rate would cause lots of log messages and which can lead to denial of
 * service. Additional roles can be permitted with GRANT.
 *
 * On receipt of this signal, a backend or an auxiliary process sets the flag
 * in the signal handler, which causes the next CHECK_FOR_INTERRUPTS()
 * or process-specific interrupt handler to log the memory contexts.
 */
pub unsafe fn pg_log_backend_memory_contexts(fcinfo: FunctionCallInfo) -> Datum {
    let pid: c_int = PG_GETARG_INT32!(fcinfo, 0);
    let mut proc: *mut PGPROC;
    let procNumber: ProcNumber;
    let _ = INVALID_PROC_NUMBER;

    /*
     * See if the process with given pid is a backend or an auxiliary process.
     */
    proc = BackendPidGetProc(pid);
    if proc.is_null() {
        proc = AuxiliaryPidGetProc(pid);
    }

    /*
     * BackendPidGetProc() and AuxiliaryPidGetProc() return NULL if the pid
     * isn't valid; but by the time we reach kill(), a process for which we
     * get a valid proc here might have terminated on its own.  There's no way
     * to acquire a lock on an arbitrary process to prevent that. But since
     * this mechanism is usually used to debug a backend or an auxiliary
     * process running and consuming lots of memory, that it might end on its
     * own first and its memory contexts are not logged is not a problem.
     */
    if proc.is_null() {
        /*
         * This is just a warning so a loop-through-resultset will not abort
         * if one backend terminated on its own during the run.
         */
        elog!(WARNING, "PID {} is not a PostgreSQL server process", pid);
        PG_RETURN_BOOL!(false);
    }

    procNumber = GetNumberFromPGProc(proc);
    if SendProcSignal(pid, PROCSIG_LOG_MEMORY_CONTEXT, procNumber) < 0 {
        /* Again, just a warning to allow loops */
        elog!(WARNING, "could not send signal to process {}", pid);
        PG_RETURN_BOOL!(false);
    }

    PG_RETURN_BOOL!(true);
}
