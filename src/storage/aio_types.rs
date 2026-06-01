//! storage/aio_types.h - AIO related types kept separate to reduce include burden.

use std::ffi::c_int;

use crate::c::{int32, uint16, uint32};
use crate::common::relpath::ForkNumber;
use crate::storage::block::BlockNumber;
use crate::storage::relfilelocator::RelFileLocator;

// Opaque forward-declared structs (typedef struct Foo Foo;). Full definitions
// live in aio_internal.h / aio.h, not in this header.
pub enum PgAioHandle {}
pub enum PgAioHandleCallbacks {}
pub enum PgAioTargetInfo {}

/*
 * A reference to an IO that can be used to wait for the IO (using
 * pgaio_wref_wait()) to complete.
 *
 * These can be passed across process boundaries.
 */
#[repr(C)]
pub struct PgAioWaitRef {
    /* internal ID identifying the specific PgAioHandle */
    pub aio_index: uint32,

    /*
     * IO handles are reused. To detect if a handle was reused, and thereby
     * avoid unnecessarily waiting for a newer IO, each time the handle is
     * reused a generation number is increased.
     *
     * To avoid requiring alignment sufficient for an int64, split the
     * generation into two.
     */
    pub generation_upper: uint32,
    pub generation_lower: uint32,
}

/*
 * Information identifying what the IO is being performed on.
 *
 * This needs sufficient information to
 *
 * a) Reopen the file for the IO if the IO is executed in a context that
 *    cannot use the FD provided initially (e.g. because the IO is executed in
 *    a worker process).
 *
 * b) Describe the object the IO is performed on in log / error messages.
 */
#[repr(C)]
pub union PgAioTargetData {
    pub smgr: PgAioTargetDataSmgr,
}

// The anonymous `smgr` struct inside the PgAioTargetData union, lifted to a
// named type. The C original uses bitfields (forkNum:8, is_temp:1,
// skip_fsync:1) which Rust cannot express directly; packed here into a single
// trailing byte-sized field to preserve layout intent. The bit accessors below
// mirror the C field widths.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgAioTargetDataSmgr {
    pub rlocator: RelFileLocator, /* physical relation identifier */
    pub blockNum: BlockNumber,    /* blknum relative to begin of reln */
    pub nblocks: BlockNumber,
    /*
     * C bitfields:
     *   ForkNumber forkNum:8;  - don't waste 4 byte for four values
     *   bool       is_temp:1;  - proc can be inferred by owning AIO
     *   bool       skip_fsync:1;
     * Rust has no bitfield syntax; store the packed bits in a single field and
     * use the accessors below. forkNum occupies bits 0..8, is_temp bit 8,
     * skip_fsync bit 9.
     */
    pub bits: uint16,
}

impl PgAioTargetDataSmgr {
    #[inline]
    pub fn forkNum(&self) -> ForkNumber {
        // 8-bit field; sign-extend per C signed enum convention.
        (self.bits & 0xFF) as i8 as ForkNumber
    }

    #[inline]
    pub fn set_forkNum(&mut self, v: ForkNumber) {
        self.bits = (self.bits & !0x00FF) | ((v as uint16) & 0x00FF);
    }

    #[inline]
    pub fn is_temp(&self) -> bool {
        (self.bits & 0x0100) != 0
    }

    #[inline]
    pub fn set_is_temp(&mut self, v: bool) {
        if v {
            self.bits |= 0x0100;
        } else {
            self.bits &= !0x0100;
        }
    }

    #[inline]
    pub fn skip_fsync(&self) -> bool {
        (self.bits & 0x0200) != 0
    }

    #[inline]
    pub fn set_skip_fsync(&mut self, v: bool) {
        if v {
            self.bits |= 0x0200;
        } else {
            self.bits &= !0x0200;
        }
    }
}

/*
 * The status of an AIO operation.
 *
 * C enum -> project convention: `pub type` alias + `pub const` variants.
 */
pub type PgAioResultStatus = c_int;
pub const PGAIO_RS_UNKNOWN: PgAioResultStatus = 0; /* not yet completed / uninitialized */
pub const PGAIO_RS_OK: PgAioResultStatus = 1;
pub const PGAIO_RS_PARTIAL: PgAioResultStatus = 2; /* did not fully succeed, no warning/error */
pub const PGAIO_RS_WARNING: PgAioResultStatus = 3; /* [partially] succeeded, with a warning */
pub const PGAIO_RS_ERROR: PgAioResultStatus = 4; /* failed entirely */

/*
 * Result of IO operation, visible only to the initiator of IO.
 *
 * We need to be careful about the size of PgAioResult, as it is embedded in
 * every PgAioHandle, as well as every PgAioReturn. Currently we assume we can
 * fit it into one 8 byte value, restricting the space for per-callback error
 * data to PGAIO_RESULT_ERROR_BITS.
 */
pub const PGAIO_RESULT_ID_BITS: u32 = 6;
pub const PGAIO_RESULT_STATUS_BITS: u32 = 3;
pub const PGAIO_RESULT_ERROR_BITS: u32 = 23;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct PgAioResult {
    /*
     * C bitfields packed into the first 32-bit word:
     *   uint32 id:PGAIO_RESULT_ID_BITS;        - bits 0..6
     *   uint32 status:PGAIO_RESULT_STATUS_BITS;- bits 6..9
     *   uint32 error_data:PGAIO_RESULT_ERROR_BITS; - bits 9..32
     *
     * `id` is of type PgAioHandleCallbackID, but C can't use a bitfield of an
     * enum because some compilers treat enums as signed; `status` is of type
     * PgAioResultStatus; `error_data` meaning is defined by callback->report.
     *
     * Rust lacks bitfield syntax, so the three fields are packed into one
     * uint32 with the accessors below.
     */
    pub bits: uint32,

    pub result: int32,
}

impl PgAioResult {
    #[inline]
    pub fn id(&self) -> uint32 {
        self.bits & ((1 << PGAIO_RESULT_ID_BITS) - 1)
    }

    #[inline]
    pub fn set_id(&mut self, v: uint32) {
        let mask = (1u32 << PGAIO_RESULT_ID_BITS) - 1;
        self.bits = (self.bits & !mask) | (v & mask);
    }

    #[inline]
    pub fn status(&self) -> uint32 {
        (self.bits >> PGAIO_RESULT_ID_BITS) & ((1 << PGAIO_RESULT_STATUS_BITS) - 1)
    }

    #[inline]
    pub fn set_status(&mut self, v: uint32) {
        let width = (1u32 << PGAIO_RESULT_STATUS_BITS) - 1;
        let mask = width << PGAIO_RESULT_ID_BITS;
        self.bits = (self.bits & !mask) | ((v & width) << PGAIO_RESULT_ID_BITS);
    }

    #[inline]
    pub fn error_data(&self) -> uint32 {
        let shift = PGAIO_RESULT_ID_BITS + PGAIO_RESULT_STATUS_BITS;
        (self.bits >> shift) & ((1 << PGAIO_RESULT_ERROR_BITS) - 1)
    }

    #[inline]
    pub fn set_error_data(&mut self, v: uint32) {
        let shift = PGAIO_RESULT_ID_BITS + PGAIO_RESULT_STATUS_BITS;
        let width = (1u32 << PGAIO_RESULT_ERROR_BITS) - 1;
        let mask = width << shift;
        self.bits = (self.bits & !mask) | ((v & width) << shift);
    }
}

// StaticAssertDecl(PGAIO_RESULT_ID_BITS + PGAIO_RESULT_STATUS_BITS +
//                  PGAIO_RESULT_ERROR_BITS == 32, "PgAioResult bits divided up incorrectly");
const _: () = assert!(
    PGAIO_RESULT_ID_BITS + PGAIO_RESULT_STATUS_BITS + PGAIO_RESULT_ERROR_BITS == 32,
    "PgAioResult bits divided up incorrectly"
);
// StaticAssertDecl(sizeof(PgAioResult) == 8, "PgAioResult has unexpected size");
const _: () = assert!(
    core::mem::size_of::<PgAioResult>() == 8,
    "PgAioResult has unexpected size"
);

/*
 * Combination of PgAioResult with minimal metadata about the IO.
 *
 * Contains sufficient information to be able, in case the IO [partially]
 * fails, to log/raise an error under control of the IO issuing code.
 */
#[repr(C)]
pub struct PgAioReturn {
    pub result: PgAioResult,
    pub target_data: PgAioTargetData,
}
