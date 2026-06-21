//! storage/ipc/sinval.c - POSTGRES shared cache invalidation communication code.

use crate::prelude::*;

use crate::miscadmin::{sig_atomic_t, Latch, MyLatch};
use crate::access::rmgrdesc::standbydesc::SharedInvalidationMessage;

// uint64 lives in crate::c (re-exported via prelude::*).

#[no_mangle]
pub static mut SharedInvalidMessageCounter: uint64 = 0;

/*
 * Because backends sitting idle will not be reading sinval events, we
 * need a way to give an idle backend a swift kick in the rear and make
 * it catch up before the sinval queue overflows and forces it to go
 * through a cache reset exercise.  This is done by sending
 * PROCSIG_CATCHUP_INTERRUPT to any backend that gets too far behind.
 *
 * The signal handler will set an interrupt pending flag and will set the
 * processes latch. Whenever starting to read from the client, or when
 * interrupted while doing so, ProcessClientReadInterrupt() will call
 * ProcessCatchupEvent().
 */
#[no_mangle]
pub static mut catchupInterruptPending: sig_atomic_t = false as sig_atomic_t;

/*
 * SendSharedInvalidMessages
 *	Add shared-cache-invalidation message(s) to the global SI message queue.
 */
pub unsafe fn SendSharedInvalidMessages(msgs: *const SharedInvalidationMessage, n: c_int) {
    SIInsertDataEntries(msgs, n);
}

const MAXINVALMSGS: usize = 32;

// The C function uses function-local `static` buffers and counters so that a
// recursive call can process messages already sucked out of sinvaladt.c. We
// hoist them to module-level `static mut`s to preserve that exact semantics.
static mut RECEIVE_MESSAGES: [SharedInvalidationMessage; MAXINVALMSGS] =
    [unsafe { std::mem::zeroed() }; MAXINVALMSGS];
static mut RECEIVE_NEXTMSG: c_int = 0;
static mut RECEIVE_NUMMSGS: c_int = 0;

/*
 * ReceiveSharedInvalidMessages
 *		Process shared-cache-invalidation messages waiting for this backend
 *
 * We guarantee to process all messages that had been queued before the
 * routine was entered.  It is of course possible for more messages to get
 * queued right after our last SIGetDataEntries call.
 *
 * NOTE: it is entirely possible for this routine to be invoked recursively
 * as a consequence of processing inside the invalFunction or resetFunction.
 * Furthermore, such a recursive call must guarantee that all outstanding
 * inval messages have been processed before it exits.  This is the reason
 * for the strange-looking choice to use a statically allocated buffer array
 * and counters; it's so that a recursive call can process messages already
 * sucked out of sinvaladt.c.
 */
pub unsafe fn ReceiveSharedInvalidMessages(
    invalFunction: unsafe extern "C" fn(msg: *mut SharedInvalidationMessage),
    resetFunction: unsafe extern "C" fn(),
) {
    // Aliases matching the C names; these refer to the module-level statics.
    // messages = RECEIVE_MESSAGES, nextmsg = RECEIVE_NEXTMSG, nummsgs = RECEIVE_NUMMSGS

    /* Deal with any messages still pending from an outer recursion */
    while RECEIVE_NEXTMSG < RECEIVE_NUMMSGS {
        let msg: SharedInvalidationMessage = RECEIVE_MESSAGES[RECEIVE_NEXTMSG as usize];
        RECEIVE_NEXTMSG += 1;

        SharedInvalidMessageCounter += 1;
        invalFunction(&msg as *const SharedInvalidationMessage as *mut SharedInvalidationMessage);
    }

    loop {
        let getResult: c_int;

        RECEIVE_NEXTMSG = 0;
        RECEIVE_NUMMSGS = 0;

        /* Try to get some more messages */
        getResult = SIGetDataEntries(RECEIVE_MESSAGES.as_mut_ptr(), MAXINVALMSGS as c_int);

        if getResult < 0 {
            /* got a reset message */
            elog!(DEBUG4, "cache state reset");
            SharedInvalidMessageCounter += 1;
            resetFunction();
            break; /* nothing more to do */
        }

        /* Process them, being wary that a recursive call might eat some */
        RECEIVE_NEXTMSG = 0;
        RECEIVE_NUMMSGS = getResult;

        while RECEIVE_NEXTMSG < RECEIVE_NUMMSGS {
            let msg: SharedInvalidationMessage = RECEIVE_MESSAGES[RECEIVE_NEXTMSG as usize];
            RECEIVE_NEXTMSG += 1;

            SharedInvalidMessageCounter += 1;
            invalFunction(
                &msg as *const SharedInvalidationMessage as *mut SharedInvalidationMessage,
            );
        }

        /*
         * We only need to loop if the last SIGetDataEntries call (which might
         * have been within a recursive call) returned a full buffer.
         */
        if RECEIVE_NUMMSGS != MAXINVALMSGS as c_int {
            break;
        }
    }

    /*
     * We are now caught up.  If we received a catchup signal, reset that
     * flag, and call SICleanupQueue().  This is not so much because we need
     * to flush dead messages right now, as that we want to pass on the
     * catchup signal to the next slowest backend.  "Daisy chaining" the
     * catchup signal this way avoids creating spikes in system load for what
     * should be just a background maintenance activity.
     */
    if catchupInterruptPending != 0 {
        catchupInterruptPending = false as sig_atomic_t;
        elog!(DEBUG4, "sinval catchup complete, cleaning queue");
        SICleanupQueue(false, 0);
    }
}

/*
 * HandleCatchupInterrupt
 *
 * This is called when PROCSIG_CATCHUP_INTERRUPT is received.
 *
 * We used to directly call ProcessCatchupEvent directly when idle. These days
 * we just set a flag to do it later and notify the process of that fact by
 * setting the process's latch.
 */
pub unsafe fn HandleCatchupInterrupt() {
    /*
     * Note: this is called by a SIGNAL HANDLER. You must be very wary what
     * you do here.
     */

    catchupInterruptPending = true as sig_atomic_t;

    /* make sure the event is processed in due course */
    SetLatch(MyLatch);
}

/*
 * ProcessCatchupInterrupt
 *
 * The portion of catchup interrupt handling that runs outside of the signal
 * handler, which allows it to actually process pending invalidations.
 */
pub unsafe fn ProcessCatchupInterrupt() {
    while catchupInterruptPending != 0 {
        /*
         * What we need to do here is cause ReceiveSharedInvalidMessages() to
         * run, which will do the necessary work and also reset the
         * catchupInterruptPending flag.  If we are inside a transaction we
         * can just call AcceptInvalidationMessages() to do this.  If we
         * aren't, we start and immediately end a transaction; the call to
         * AcceptInvalidationMessages() happens down inside transaction start.
         *
         * It is awfully tempting to just call AcceptInvalidationMessages()
         * without the rest of the xact start/stop overhead, and I think that
         * would actually work in the normal case; but I am not sure that
         * things would clean up nicely if we got an error partway through.
         */
        if IsTransactionOrTransactionBlock() {
            elog!(DEBUG4, "ProcessCatchupEvent inside transaction");
            AcceptInvalidationMessages();
        } else {
            elog!(DEBUG4, "ProcessCatchupEvent outside transaction");
            StartTransactionCommand();
            CommitTransactionCommand();
        }
    }
}

// ---------------------------------------------------------------------------
// Stubs for not-yet-ported callees.
// ---------------------------------------------------------------------------

// storage/sinvaladt.h - SI message queue access (sinvaladt.c not yet ported).
// TODO: replace with crate::storage::sinvaladt when it lands.
unsafe fn SIInsertDataEntries(_msgs: *const SharedInvalidationMessage, _n: c_int) {
    crate::storage::ipc::sinvaladt::SIInsertDataEntries(_msgs as _, _n)
}
// TODO: replace with crate::storage::sinvaladt when it lands.
unsafe fn SIGetDataEntries(_data: *mut SharedInvalidationMessage, _datasize: c_int) -> c_int {
    crate::storage::ipc::sinvaladt::SIGetDataEntries(_data as _, _datasize)
}
// TODO: replace with crate::storage::sinvaladt when it lands.
unsafe fn SICleanupQueue(_callerHasWriteLock: bool, _minFree: c_int) {
    crate::storage::ipc::sinvaladt::SICleanupQueue(_callerHasWriteLock, _minFree)
}

// storage/latch.h - SetLatch (latch.c not yet ported).
// TODO: replace with crate::storage::latch::SetLatch when it lands.
unsafe fn SetLatch(_latch: *mut Latch) {
    crate::storage::ipc::latch::SetLatch(_latch as _)
}

// utils/inval.h - AcceptInvalidationMessages (inval.c not yet ported).
// TODO: replace with crate::utils::cache::inval::AcceptInvalidationMessages when it lands.
unsafe fn AcceptInvalidationMessages() {
    crate::utils::cache::inval::AcceptInvalidationMessages()
}

// access/xact.h - transaction control (xact.c not yet ported).
// TODO: replace with crate::access::transam::xact when it lands.
unsafe fn IsTransactionOrTransactionBlock() -> bool {
    crate::access::transam::xact::IsTransactionOrTransactionBlock()
}
// TODO: replace with crate::access::transam::xact when it lands.
unsafe fn StartTransactionCommand() {
    crate::access::transam::xact::StartTransactionCommand()
}
// TODO: replace with crate::access::transam::xact when it lands.
unsafe fn CommitTransactionCommand() {
    crate::access::transam::xact::CommitTransactionCommand()
}
