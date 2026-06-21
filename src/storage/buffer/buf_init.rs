//! storage/buffer/buf_init.c - buffer manager initialization routines.

use crate::prelude::*;

use crate::miscadmin::NBuffers;
use crate::pg_config::BLCKSZ;
use crate::pg_config_manual::{PG_CACHE_LINE_SIZE, PG_IO_ALIGN_SIZE};
use crate::port::atomics::pg_atomic_init_u32_impl;
use crate::storage::buf_internals::{
    BufferDescPadded, BufferDescriptorGetContentLock, BufferDescriptorGetIOCV, BufferDescriptors,
    BufferIOCVArray, CkptBufferIds, CkptSortItem, ClearBufferTag, ConditionVariableMinimallyPadded,
    GetBufferDescriptor, StrategyInitialize, StrategyShmemSize, WritebackContext,
    WritebackContextInit, BackendWritebackContext, FREENEXT_END_OF_LIST,
};
use crate::storage::buf_internals::{ConditionVariable, LWLock};
use crate::storage::aio_types::PgAioWaitRef;
use crate::port::atomics::pg_atomic_uint32;

// ----------------------------------------------------------------------------
// Globals defined in buf_init.c
// ----------------------------------------------------------------------------

// `char *BufferBlocks;` -- the data page area. Declared here (it lives in this
// translation unit in C). The other globals (BufferDescriptors, BufferIOCVArray,
// BackendWritebackContext, CkptBufferIds) are declared in buf_internals.rs.
#[no_mangle]
pub static mut BufferBlocks: *mut c_char = null_mut();

// ----------------------------------------------------------------------------
// Local stubs for functions not yet ported.
// ----------------------------------------------------------------------------

/// storage/shmem.h ShmemInitStruct(): allocate (or attach to) a named chunk of
/// shared memory of `size` bytes, setting `*foundPtr`.
// TODO: not ported (shmem.c). Local stub.
unsafe fn ShmemInitStruct(name: *const c_char, size: Size, foundPtr: *mut bool) -> *mut c_void {
    crate::storage::ipc::shmem::ShmemInitStruct(name, size, foundPtr)
}

/// storage/shmem.h add_size(): overflow-checked addition of shared sizes.
// TODO: not ported (shmem.c). Local stub mirroring the real overflow check.
#[inline]
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    let result = s1.wrapping_add(s2);
    if result < s1 || result < s2 {
        ereport!(ERROR, errmsg!("requested shared memory size overflows size_t"));
    }
    result
}

/// storage/shmem.h mul_size(): overflow-checked multiplication of shared sizes.
// TODO: not ported (shmem.c). Local stub mirroring the real overflow check.
#[inline]
unsafe fn mul_size(s1: Size, s2: Size) -> Size {
    if s1 == 0 || s2 == 0 {
        return 0;
    }
    let result = s1.wrapping_mul(s2);
    if result / s2 != s1 {
        ereport!(ERROR, errmsg!("requested shared memory size overflows size_t"));
    }
    result
}

/// storage/atomics.h pg_atomic_init_u32(): the public wrapper (delegates to the
/// generic _impl). Provided here because only the _impl form is currently ported.
// TODO: replace with the real atomics.h wrapper once ported.
#[inline]
unsafe fn pg_atomic_init_u32(ptr: *mut pg_atomic_uint32, val: uint32) {
    pg_atomic_init_u32_impl(&*ptr, val);
}

/// storage/aio.h pgaio_wref_clear(): mark an AIO wait reference as not in use.
// TODO: not ported (aio.c). Local stub.
unsafe fn pgaio_wref_clear(iow: *mut PgAioWaitRef) {
    crate::storage::aio::aio::pgaio_wref_clear(iow as _)
}

/// storage/lwlock.h LWLockInitialize(): initialize an LWLock in a tranche.
// TODO: not ported (lwlock.c). Local stub.
unsafe fn LWLockInitialize(lock: *mut LWLock, tranche_id: c_int) {
    crate::storage::lmgr::lwlock::LWLockInitialize(lock as _, tranche_id)
}

/// storage/condition_variable.h ConditionVariableInit().
// TODO: not ported (condition_variable.c). Local stub.
unsafe fn ConditionVariableInit(cv: *mut ConditionVariable) {
    crate::storage::lmgr::condition_variable::ConditionVariableInit(cv as _)
}

/// storage/procnumber.h INVALID_PROC_NUMBER.
// TODO: import from procnumber once that constant is exported there.
const INVALID_PROC_NUMBER: c_int = -1;

/// storage/lwlock.h LWTRANCHE_BUFFER_CONTENT.
// TODO: import from lwlock.h tranche enum once ported.
const LWTRANCHE_BUFFER_CONTENT: c_int = 54;

// `backend_flush_after` GUC (declared in bufmgr.h, defined in bufmgr.c).
// TODO: not ported (bufmgr.c). Local placeholder for the &address taken below.
static mut backend_flush_after: c_int = 0;

// ----------------------------------------------------------------------------
//  Data Structures:
//		buffers live in a freelist and a lookup data structure.
// ----------------------------------------------------------------------------

/// Initialize shared buffer pool
///
/// This is called once during shared-memory initialization (either in the
/// postmaster, or in a standalone backend).
pub unsafe fn BufferManagerShmemInit() {
    let mut foundBufs: bool = false;
    let mut foundDescs: bool = false;
    let mut foundIOCV: bool = false;
    let mut foundBufCkpt: bool = false;

    /* Align descriptors to a cacheline boundary. */
    BufferDescriptors = ShmemInitStruct(
        c"Buffer Descriptors".as_ptr(),
        NBuffers as Size * std::mem::size_of::<BufferDescPadded>(),
        &mut foundDescs,
    ) as *mut BufferDescPadded;

    /* Align buffer pool on IO page size boundary. */
    BufferBlocks = TYPEALIGN(
        PG_IO_ALIGN_SIZE,
        ShmemInitStruct(
            c"Buffer Blocks".as_ptr(),
            NBuffers as Size * BLCKSZ as Size + PG_IO_ALIGN_SIZE,
            &mut foundBufs,
        ) as usize,
    ) as *mut c_char;

    /* Align condition variables to cacheline boundary. */
    BufferIOCVArray = ShmemInitStruct(
        c"Buffer IO Condition Variables".as_ptr(),
        NBuffers as Size * std::mem::size_of::<ConditionVariableMinimallyPadded>(),
        &mut foundIOCV,
    ) as *mut ConditionVariableMinimallyPadded;

    /*
     * The array used to sort to-be-checkpointed buffer ids is located in
     * shared memory, to avoid having to allocate significant amounts of
     * memory at runtime. As that'd be in the middle of a checkpoint, or when
     * the checkpointer is restarted, memory allocation failures would be
     * painful.
     */
    CkptBufferIds = ShmemInitStruct(
        c"Checkpoint BufferIds".as_ptr(),
        NBuffers as Size * std::mem::size_of::<CkptSortItem>(),
        &mut foundBufCkpt,
    ) as *mut CkptSortItem;

    if foundDescs || foundBufs || foundIOCV || foundBufCkpt {
        /* should find all of these, or none of them */
        Assert!(foundDescs && foundBufs && foundIOCV && foundBufCkpt);
        /* note: this path is only taken in EXEC_BACKEND case */
    } else {
        /*
         * Initialize all the buffer headers.
         */
        let mut i: c_int = 0;
        while i < NBuffers {
            let buf = GetBufferDescriptor(i as uint32);

            ClearBufferTag(&mut (*buf).tag);

            pg_atomic_init_u32(&mut (*buf).state, 0);
            (*buf).wait_backend_pgprocno = INVALID_PROC_NUMBER;

            (*buf).buf_id = i;

            pgaio_wref_clear(&mut (*buf).io_wref);

            /*
             * Initially link all the buffers together as unused. Subsequent
             * management of this list is done by freelist.c.
             */
            (*buf).freeNext = i + 1;

            LWLockInitialize(
                BufferDescriptorGetContentLock(buf),
                LWTRANCHE_BUFFER_CONTENT,
            );

            ConditionVariableInit(BufferDescriptorGetIOCV(buf));

            i += 1;
        }

        /* Correct last entry of linked list */
        (*GetBufferDescriptor((NBuffers - 1) as uint32)).freeNext = FREENEXT_END_OF_LIST;
    }

    /* Init other shared buffer-management stuff */
    StrategyInitialize(!foundDescs);

    /* Initialize per-backend file flush context */
    WritebackContextInit(
        &raw mut BackendWritebackContext,
        &raw mut backend_flush_after,
    );
}

/// BufferManagerShmemSize
///
/// compute the size of shared memory for the buffer pool including
/// data pages, buffer descriptors, hash tables, etc.
pub unsafe fn BufferManagerShmemSize() -> Size {
    let mut size: Size = 0;

    /* size of buffer descriptors */
    size = add_size(
        size,
        mul_size(NBuffers as Size, std::mem::size_of::<BufferDescPadded>()),
    );
    /* to allow aligning buffer descriptors */
    size = add_size(size, PG_CACHE_LINE_SIZE);

    /* size of data pages, plus alignment padding */
    size = add_size(size, PG_IO_ALIGN_SIZE);
    size = add_size(size, mul_size(NBuffers as Size, BLCKSZ as Size));

    /* size of stuff controlled by freelist.c */
    size = add_size(size, StrategyShmemSize());

    /* size of I/O condition variables */
    size = add_size(
        size,
        mul_size(
            NBuffers as Size,
            std::mem::size_of::<ConditionVariableMinimallyPadded>(),
        ),
    );
    /* to allow aligning the above */
    size = add_size(size, PG_CACHE_LINE_SIZE);

    /* size of checkpoint sort array in bufmgr.c */
    size = add_size(
        size,
        mul_size(NBuffers as Size, std::mem::size_of::<CkptSortItem>()),
    );

    size
}
