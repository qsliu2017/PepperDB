//! Translated from PostgreSQL src/include/fe_utils/parallel_slot.h
//
// Parallel connection slots for bin/scripts. The result-handler callback's
// `void *context` collapses into a captured closure.

use crate::fe_utils::connect_utils::ConnParams;

// libpq-fe.h is not in the tree; forward-declare the client handles locally.
#[deprecated(note = "TODO(struct-forward): repoint to crate::interfaces::libpq::PGconn in Phase 2")]
// TODO(struct-forward)
pub struct PGconn;
#[deprecated(note = "TODO(struct-forward): repoint to crate::interfaces::libpq::PGresult in Phase 2")]
// TODO(struct-forward)
pub struct PGresult;

/// C: `typedef bool (*ParallelSlotResultHandler)(PGresult*, PGconn*, void*)`.
/// The `void *context` is captured by the closure.
#[allow(deprecated)]
pub type ParallelSlotResultHandler = Box<dyn FnMut(&PGresult, &mut PGconn) -> bool>;

/// C: `ParallelSlot`. In-memory.
#[allow(deprecated)]
pub struct ParallelSlot {
    pub connection: Option<Box<PGconn>>, // one connection (NULL if none) TODO(ptr)
    pub in_use: bool,
    pub handler: Option<ParallelSlotResultHandler>,
}

#[allow(deprecated)]
impl ParallelSlot {
    /// C: `ParallelSlotSetHandler(slot, handler, context)`.
    pub fn set_handler(&mut self, handler: ParallelSlotResultHandler) {
        self.handler = Some(handler);
    }
    /// C: `ParallelSlotClearHandler(slot)`.
    pub fn clear_handler(&mut self) {
        self.handler = None;
    }
}

/// C: `ParallelSlotArray` with a FLEXIBLE_ARRAY_MEMBER `slots[]` -> Vec.
pub struct ParallelSlotArray<'a> {
    pub cparams: &'a ConnParams,
    pub progname: String,
    pub echo: bool,
    pub initcmd: Option<String>,
    pub slots: Vec<ParallelSlot>, // numslots = slots.len()
}

/// Returns an idle slot, or None if none is available. (C may also return NULL.)
pub fn ParallelSlotsGetIdle<'a>(
    sa: &'a mut ParallelSlotArray<'_>,
    dbname: Option<&str>,
) -> Option<&'a mut ParallelSlot> {
    unimplemented!()
}

pub fn ParallelSlotsSetup<'a>(
    numslots: i32,
    cparams: &'a ConnParams,
    progname: &str,
    echo: bool,
    initcmd: Option<&str>,
) -> ParallelSlotArray<'a> {
    unimplemented!()
}

#[allow(deprecated)]
pub fn ParallelSlotsAdoptConn(sa: &mut ParallelSlotArray<'_>, conn: Box<PGconn>) {
    unimplemented!()
}

pub fn ParallelSlotsTerminate(sa: &mut ParallelSlotArray<'_>) {
    unimplemented!()
}

pub fn ParallelSlotsWaitCompletion(sa: &mut ParallelSlotArray<'_>) -> bool {
    unimplemented!()
}

#[allow(deprecated)]
pub fn TableCommandResultHandler(res: &PGresult, conn: &mut PGconn) -> bool {
    unimplemented!()
}
