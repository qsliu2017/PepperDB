//! fe_utils/parallel_slot.h - Parallel support for bin/scripts/

use std::ffi::{c_char, c_int, c_void};

// Frontend/libpq types - not yet ported; stub locally.
// TODO: dedup
pub type PGconn = c_void;
pub type PGresult = c_void;

// From fe_utils/connect_utils.h - not yet ported; stub locally.
// TODO: dedup
pub type ConnParams = c_void;

// typedef bool (*ParallelSlotResultHandler)(PGresult *res, PGconn *conn,
//                                            void *context);
pub type ParallelSlotResultHandler =
    Option<unsafe extern "C" fn(res: *mut PGresult, conn: *mut PGconn, context: *mut c_void) -> bool>;

#[repr(C)]
pub struct ParallelSlot {
    pub connection: *mut PGconn, /* One connection */
    pub inUse: bool,             /* Is the slot being used? */

    /*
     * Prior to issuing a command or query on 'connection', a handler callback
     * function may optionally be registered to be invoked to process the
     * results, and context information may optionally be registered for use
     * by the handler.  If unset, these fields should be NULL.
     */
    pub handler: ParallelSlotResultHandler,
    pub handler_context: *mut c_void,
}

#[repr(C)]
pub struct ParallelSlotArray {
    pub numslots: c_int,
    pub cparams: *mut ConnParams,
    pub progname: *const c_char,
    pub echo: bool,
    pub initcmd: *const c_char,
    pub slots: [ParallelSlot; 0], /* FLEXIBLE_ARRAY_MEMBER */
}

#[inline]
pub unsafe fn ParallelSlotSetHandler(
    slot: *mut ParallelSlot,
    handler: ParallelSlotResultHandler,
    context: *mut c_void,
) {
    (*slot).handler = handler;
    (*slot).handler_context = context;
}

#[inline]
pub unsafe fn ParallelSlotClearHandler(slot: *mut ParallelSlot) {
    (*slot).handler = None;
    (*slot).handler_context = std::ptr::null_mut();
}

// extern ParallelSlot *ParallelSlotsGetIdle(ParallelSlotArray *sa,
//                                           const char *dbname);
pub unsafe fn ParallelSlotsGetIdle(
    sa: *mut ParallelSlotArray,
    dbname: *const c_char,
) -> *mut ParallelSlot {
    unimplemented!()
}

// extern ParallelSlotArray *ParallelSlotsSetup(int numslots, ConnParams *cparams,
//                                              const char *progname, bool echo,
//                                              const char *initcmd);
pub unsafe fn ParallelSlotsSetup(
    numslots: c_int,
    cparams: *mut ConnParams,
    progname: *const c_char,
    echo: bool,
    initcmd: *const c_char,
) -> *mut ParallelSlotArray {
    unimplemented!()
}

// extern void ParallelSlotsAdoptConn(ParallelSlotArray *sa, PGconn *conn);
pub unsafe fn ParallelSlotsAdoptConn(sa: *mut ParallelSlotArray, conn: *mut PGconn) {
    unimplemented!()
}

// extern void ParallelSlotsTerminate(ParallelSlotArray *sa);
pub unsafe fn ParallelSlotsTerminate(sa: *mut ParallelSlotArray) {
    unimplemented!()
}

// extern bool ParallelSlotsWaitCompletion(ParallelSlotArray *sa);
pub unsafe fn ParallelSlotsWaitCompletion(sa: *mut ParallelSlotArray) -> bool {
    unimplemented!()
}

// extern bool TableCommandResultHandler(PGresult *res, PGconn *conn,
//                                       void *context);
pub unsafe fn TableCommandResultHandler(
    res: *mut PGresult,
    conn: *mut PGconn,
    context: *mut c_void,
) -> bool {
    unimplemented!()
}
