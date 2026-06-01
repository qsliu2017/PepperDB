//! libpq/be-secure-gssapi.c - GSSAPI encryption support
//!
//! Portions Copyright (c) 2018-2025, PostgreSQL Global Development Group
//!
//! IDENTIFICATION
//!  src/backend/libpq/be-secure-gssapi.c

use crate::prelude::*;

use crate::libpq::auth::{pg_gss_accept_delegation, pg_krb_server_keyfile};
use crate::libpq::be_gssapi_common::{pg_GSS_error, pg_store_delegated_credential};
use crate::libpq::libpq::{secure_raw_read, secure_raw_write};
use crate::libpq::libpq_be::{pg_gssinfo, Port};
use crate::libpq::pg_gssapi::{
    gss_buffer_desc, gss_cred_id_t, gss_ctx_id_t, OM_uint32,
};
use crate::port::noblock::pgsocket;
use crate::port::pg_bswap::{pg_hton32, pg_ntoh32};
use crate::storage::ipc::latch::{
    WaitLatchOrSocket, WL_EXIT_ON_PM_DEATH, WL_SOCKET_READABLE, WL_SOCKET_WRITEABLE,
};
use crate::utils::elog::COMMERROR;
use crate::utils::mmgr::mcxt::TopMemoryContext;
use core::ffi::CStr;
use std::ffi::c_void;

pub type ssize_t = isize;

// ---------------------------------------------------------------------------
// GSSAPI system-library constants/types/functions (would come from <gssapi.h>).
// These are NOT part of be-secure-gssapi.c; they stand in for the external
// GSSAPI symbols the C file references via <gssapi.h>.
// TODO: replace with real GSSAPI bindings when GSS support is wired up.
// ---------------------------------------------------------------------------

const GSS_S_COMPLETE: OM_uint32 = 0;
const GSS_S_CONTINUE_NEEDED: OM_uint32 = 1;
const GSS_C_QOP_DEFAULT: OM_uint32 = 0;

const GSS_C_NO_CREDENTIAL: gss_cred_id_t = std::ptr::null_mut();
const GSS_C_NO_CHANNEL_BINDINGS: *mut c_void = std::ptr::null_mut();

/// GSS_C_EMPTY_BUFFER initializer for gss_buffer_desc.
#[inline]
fn GSS_C_EMPTY_BUFFER() -> gss_buffer_desc {
    gss_buffer_desc {
        length: 0,
        value: std::ptr::null_mut(),
    }
}

/// GSS_ERROR(x) macro: nonzero when one of the calling-error or routine-error
/// bits is set in the major status code.
#[inline]
fn GSS_ERROR(x: OM_uint32) -> bool {
    (x & (0o37_0000_0000 | 0o0_3777_0000)) != 0
}

unsafe fn gss_wrap(
    _minor_status: *mut OM_uint32,
    _context_handle: gss_ctx_id_t,
    _conf_req_flag: c_int,
    _qop_req: OM_uint32,
    _input_message_buffer: *const gss_buffer_desc,
    _conf_state: *mut c_int,
    _output_message_buffer: *mut gss_buffer_desc,
) -> OM_uint32 {
    unimplemented!()
}

unsafe fn gss_unwrap(
    _minor_status: *mut OM_uint32,
    _context_handle: gss_ctx_id_t,
    _input_message_buffer: *const gss_buffer_desc,
    _output_message_buffer: *mut gss_buffer_desc,
    _conf_state: *mut c_int,
    _qop_state: *mut OM_uint32,
) -> OM_uint32 {
    unimplemented!()
}

unsafe fn gss_release_buffer(
    _minor_status: *mut OM_uint32,
    _buffer: *mut gss_buffer_desc,
) -> OM_uint32 {
    unimplemented!()
}

unsafe fn gss_accept_sec_context(
    _minor_status: *mut OM_uint32,
    _context_handle: *mut gss_ctx_id_t,
    _acceptor_cred_handle: gss_cred_id_t,
    _input_token_buffer: *const gss_buffer_desc,
    _input_chan_bindings: *mut c_void,
    _src_name: *mut crate::libpq::pg_gssapi::gss_name_t,
    _mech_type: *mut crate::libpq::pg_gssapi::gss_OID,
    _output_token: *mut gss_buffer_desc,
    _ret_flags: *mut OM_uint32,
    _time_rec: *mut OM_uint32,
    _delegated_cred_handle: *mut gss_cred_id_t,
) -> OM_uint32 {
    unimplemented!()
}

unsafe fn gss_wrap_size_limit(
    _minor_status: *mut OM_uint32,
    _context_handle: gss_ctx_id_t,
    _conf_req_flag: c_int,
    _qop_req: OM_uint32,
    _req_output_size: OM_uint32,
    _max_input_size: *mut OM_uint32,
) -> OM_uint32 {
    unimplemented!()
}

// ---------------------------------------------------------------------------
// Other PostgreSQL symbols referenced from this file but defined elsewhere.
// ---------------------------------------------------------------------------

// _() gettext translation marker: identity passthrough.
#[inline]
unsafe fn _(s: *const c_char) -> *const c_char {
    s
}

// WAIT_EVENT_GSS_OPEN_SERVER (wait_event.h). Not yet translated; stub value.
// TODO(pg-port): import from utils/activity/wait_event once available.
const WAIT_EVENT_GSS_OPEN_SERVER: u32 = 0;

// errno access (Darwin __error()), mirroring be-secure.c's shim.
#[cfg(target_os = "macos")]
extern "C" {
    #[link_name = "__error"]
    fn errno_location() -> *mut c_int;
}
#[cfg(not(target_os = "macos"))]
extern "C" {
    #[link_name = "__errno_location"]
    fn errno_location() -> *mut c_int;
}

#[inline]
unsafe fn errno() -> c_int {
    *errno_location()
}
#[inline]
unsafe fn set_errno(v: c_int) {
    *errno_location() = v;
}

/* errno constants (darwin values). */
const EAGAIN: c_int = 35;
const EWOULDBLOCK: c_int = EAGAIN;
const EINTR: c_int = 4;
const ECONNRESET: c_int = 54;

// INJECTION_POINT(name, arg): no-op in this build (injection points disabled).
// TODO(pg-port): wire to utils/injection_point once available.
macro_rules! INJECTION_POINT {
    ($name:expr, $arg:expr) => {{
        let _ = ($name, $arg);
    }};
}

/*
 * Handle the encryption/decryption of data using GSSAPI.
 *
 * In the encrypted data stream on the wire, we break up the data
 * into packets where each packet starts with a uint32-size length
 * word (in network byte order), then encrypted data of that length
 * immediately following.  Decryption yields the same data stream
 * that would appear when not using encryption.
 *
 * Encrypted data typically ends up being larger than the same data
 * unencrypted, so we use fixed-size buffers for handling the
 * encryption/decryption which are larger than PQComm's buffer will
 * typically be to minimize the times where we have to make multiple
 * packets (and therefore multiple recv/send calls for a single
 * read/write call to us).
 *
 * NOTE: The client and server have to agree on the max packet size,
 * because we have to pass an entire packet to GSSAPI at a time and we
 * don't want the other side to send arbitrarily huge packets as we
 * would have to allocate memory for them to then pass them to GSSAPI.
 *
 * Therefore, this #define is effectively part of the protocol
 * spec and can't ever be changed.
 */
const PQ_GSS_MAX_PACKET_SIZE: usize = 16384; /* includes uint32 header word */

/*
 * However, during the authentication exchange we must cope with whatever
 * message size the GSSAPI library wants to send (because our protocol
 * doesn't support splitting those messages).  Depending on configuration
 * those messages might be as much as 64kB.
 */
const PQ_GSS_AUTH_BUFFER_SIZE: usize = 65536; /* includes uint32 header word */

/*
 * Since we manage at most one GSS-encrypted connection per backend,
 * we can just keep all this state in static variables.  The char *
 * variables point to buffers that are allocated once and re-used.
 */
static mut PqGSSSendBuffer: *mut c_char = std::ptr::null_mut(); /* Encrypted data waiting to be sent */
static mut PqGSSSendLength: c_int = 0; /* End of data available in PqGSSSendBuffer */
static mut PqGSSSendNext: c_int = 0; /* Next index to send a byte from PqGSSSendBuffer */
static mut PqGSSSendConsumed: c_int = 0; /* Number of source bytes encrypted but not yet reported as sent */

static mut PqGSSRecvBuffer: *mut c_char = std::ptr::null_mut(); /* Received, encrypted data */
static mut PqGSSRecvLength: c_int = 0; /* End of data available in PqGSSRecvBuffer */

static mut PqGSSResultBuffer: *mut c_char = std::ptr::null_mut(); /* Decryption of data in gss_RecvBuffer */
static mut PqGSSResultLength: c_int = 0; /* End of data available in PqGSSResultBuffer */
static mut PqGSSResultNext: c_int = 0; /* Next index to read a byte from PqGSSResultBuffer */

static mut PqGSSMaxPktSize: uint32 = 0; /* Maximum size we can encrypt and fit the results into our output buffer */

/*
 * Attempt to write len bytes of data from ptr to a GSSAPI-encrypted connection.
 *
 * The connection must be already set up for GSSAPI encryption (i.e., GSSAPI
 * transport negotiation is complete).
 *
 * On success, returns the number of data bytes consumed (possibly less than
 * len).  On failure, returns -1 with errno set appropriately.  For retryable
 * errors, caller should call again (passing the same or more data) once the
 * socket is ready.
 *
 * Dealing with fatal errors here is a bit tricky: we can't invoke elog(FATAL)
 * since it would try to write to the client, probably resulting in infinite
 * recursion.  Instead, use elog(COMMERROR) to log extra info about the
 * failure if necessary, and then return an errno indicating connection loss.
 */
pub unsafe fn be_gssapi_write(port: *mut Port, ptr: *const c_void, len: usize) -> ssize_t {
    let major: OM_uint32;
    let mut minor: OM_uint32 = 0;
    let mut input: gss_buffer_desc = std::mem::zeroed();
    let mut output: gss_buffer_desc = std::mem::zeroed();
    let mut bytes_to_encrypt: usize;
    let mut bytes_encrypted: usize;
    let gctx: gss_ctx_id_t = (*(*port).gss).ctx;

    /*
     * When we get a retryable failure, we must not tell the caller we have
     * successfully transmitted everything, else it won't retry.  For
     * simplicity, we claim we haven't transmitted anything until we have
     * successfully transmitted all "len" bytes.  Between calls, the amount of
     * the current input data that's already been encrypted and placed into
     * PqGSSSendBuffer (and perhaps transmitted) is remembered in
     * PqGSSSendConsumed.  On a retry, the caller *must* be sending that data
     * again, so if it offers a len less than that, something is wrong.
     *
     * Note: it may seem attractive to report partial write completion once
     * we've successfully sent any encrypted packets.  However, doing that
     * expands the state space of this processing and has been responsible for
     * bugs in the past (cf. commit d053a879b).  We won't save much,
     * typically, by letting callers discard data early, so don't risk it.
     */
    if len < PqGSSSendConsumed as usize {
        elog!(
            COMMERROR,
            "GSSAPI caller failed to retransmit all data needing to be retried"
        );
        set_errno(ECONNRESET);
        return -1;
    }

    /* Discount whatever source data we already encrypted. */
    bytes_to_encrypt = len - PqGSSSendConsumed as usize;
    bytes_encrypted = PqGSSSendConsumed as usize;

    /*
     * Loop through encrypting data and sending it out until it's all done or
     * secure_raw_write() complains (which would likely mean that the socket
     * is non-blocking and the requested send() would block, or there was some
     * kind of actual error).
     */
    while bytes_to_encrypt != 0 || PqGSSSendLength != 0 {
        let mut conf_state: c_int = 0;
        let netlen: uint32;

        /*
         * Check if we have data in the encrypted output buffer that needs to
         * be sent (possibly left over from a previous call), and if so, try
         * to send it.  If we aren't able to, return that fact back up to the
         * caller.
         */
        if PqGSSSendLength != 0 {
            let ret: ssize_t;
            let amount: ssize_t = (PqGSSSendLength - PqGSSSendNext) as ssize_t;

            ret = secure_raw_write(
                port,
                PqGSSSendBuffer.add(PqGSSSendNext as usize) as *const c_void,
                amount as Size,
            );
            if ret <= 0 {
                return ret;
            }

            /*
             * Check if this was a partial write, and if so, move forward that
             * far in our buffer and try again.
             */
            if ret < amount {
                PqGSSSendNext += ret as c_int;
                continue;
            }

            /* We've successfully sent whatever data was in the buffer. */
            PqGSSSendLength = 0;
            PqGSSSendNext = 0;
        }

        /*
         * Check if there are any bytes left to encrypt.  If not, we're done.
         */
        if bytes_to_encrypt == 0 {
            break;
        }

        /*
         * Check how much we are being asked to send, if it's too much, then
         * we will have to loop and possibly be called multiple times to get
         * through all the data.
         */
        if bytes_to_encrypt > PqGSSMaxPktSize as usize {
            input.length = PqGSSMaxPktSize as Size;
        } else {
            input.length = bytes_to_encrypt as Size;
        }

        input.value = (ptr as *const c_char).add(bytes_encrypted) as *mut c_void;

        output.value = std::ptr::null_mut();
        output.length = 0;

        /*
         * Create the next encrypted packet.  Any failure here is considered a
         * hard failure, so we return -1 even if some data has been sent.
         */
        let major = gss_wrap(
            &mut minor,
            gctx,
            1,
            GSS_C_QOP_DEFAULT,
            &input,
            &mut conf_state,
            &mut output,
        );
        if major != GSS_S_COMPLETE {
            pg_GSS_error(_(c"GSSAPI wrap error".as_ptr()), major, minor);
            set_errno(ECONNRESET);
            return -1;
        }
        if conf_state == 0 {
            ereport!(
                COMMERROR,
                errmsg!("outgoing GSSAPI message would not use confidentiality")
            );
            set_errno(ECONNRESET);
            return -1;
        }
        if output.length as usize > PQ_GSS_MAX_PACKET_SIZE - std::mem::size_of::<uint32>() {
            ereport!(
                COMMERROR,
                errmsg!(
                    "server tried to send oversize GSSAPI packet ({} > {})",
                    output.length as usize,
                    PQ_GSS_MAX_PACKET_SIZE - std::mem::size_of::<uint32>()
                )
            );
            set_errno(ECONNRESET);
            return -1;
        }

        bytes_encrypted += input.length as usize;
        bytes_to_encrypt -= input.length as usize;
        PqGSSSendConsumed += input.length as c_int;

        /* 4 network-order bytes of length, then payload */
        netlen = pg_hton32(output.length as uint32);
        std::ptr::copy_nonoverlapping(
            &netlen as *const uint32 as *const u8,
            PqGSSSendBuffer.add(PqGSSSendLength as usize) as *mut u8,
            std::mem::size_of::<uint32>(),
        );
        PqGSSSendLength += std::mem::size_of::<uint32>() as c_int;

        std::ptr::copy_nonoverlapping(
            output.value as *const u8,
            PqGSSSendBuffer.add(PqGSSSendLength as usize) as *mut u8,
            output.length as usize,
        );
        PqGSSSendLength += output.length as c_int;

        /* Release buffer storage allocated by GSSAPI */
        gss_release_buffer(&mut minor, &mut output);
    }

    /* If we get here, our counters should all match up. */
    Assert!(len == PqGSSSendConsumed as usize);
    Assert!(len == bytes_encrypted);

    let _ = major;

    /* We're reporting all the data as sent, so reset PqGSSSendConsumed. */
    PqGSSSendConsumed = 0;

    bytes_encrypted as ssize_t
}

/*
 * Read up to len bytes of data into ptr from a GSSAPI-encrypted connection.
 *
 * The connection must be already set up for GSSAPI encryption (i.e., GSSAPI
 * transport negotiation is complete).
 *
 * Returns the number of data bytes read, or on failure, returns -1
 * with errno set appropriately.  For retryable errors, caller should call
 * again once the socket is ready.
 *
 * We treat fatal errors the same as in be_gssapi_write(), even though the
 * argument about infinite recursion doesn't apply here.
 */
pub unsafe fn be_gssapi_read(port: *mut Port, ptr: *mut c_void, len: usize) -> ssize_t {
    let major: OM_uint32;
    let mut minor: OM_uint32 = 0;
    let mut input: gss_buffer_desc = std::mem::zeroed();
    let mut output: gss_buffer_desc = std::mem::zeroed();
    let mut ret: ssize_t;
    let mut bytes_returned: usize = 0;
    let gctx: gss_ctx_id_t = (*(*port).gss).ctx;

    /*
     * The plan here is to read one incoming encrypted packet into
     * PqGSSRecvBuffer, decrypt it into PqGSSResultBuffer, and then dole out
     * data from there to the caller.  When we exhaust the current input
     * packet, read another.
     */
    while bytes_returned < len {
        let mut conf_state: c_int = 0;

        /* Check if we have data in our buffer that we can return immediately */
        if PqGSSResultNext < PqGSSResultLength {
            let bytes_in_buffer: usize = (PqGSSResultLength - PqGSSResultNext) as usize;
            let bytes_to_copy: usize = Min(bytes_in_buffer, len - bytes_returned);

            /*
             * Copy the data from our result buffer into the caller's buffer,
             * at the point where we last left off filling their buffer.
             */
            std::ptr::copy_nonoverlapping(
                PqGSSResultBuffer.add(PqGSSResultNext as usize) as *const u8,
                (ptr as *mut c_char).add(bytes_returned) as *mut u8,
                bytes_to_copy,
            );
            PqGSSResultNext += bytes_to_copy as c_int;
            bytes_returned += bytes_to_copy;

            /*
             * At this point, we've either filled the caller's buffer or
             * emptied our result buffer.  Either way, return to caller.  In
             * the second case, we could try to read another encrypted packet,
             * but the odds are good that there isn't one available.  (If this
             * isn't true, we chose too small a max packet size.)  In any
             * case, there's no harm letting the caller process the data we've
             * already returned.
             */
            break;
        }

        /* Result buffer is empty, so reset buffer pointers */
        PqGSSResultLength = 0;
        PqGSSResultNext = 0;

        /*
         * Because we chose above to return immediately as soon as we emit
         * some data, bytes_returned must be zero at this point.  Therefore
         * the failure exits below can just return -1 without worrying about
         * whether we already emitted some data.
         */
        Assert!(bytes_returned == 0);

        /*
         * At this point, our result buffer is empty with more bytes being
         * requested to be read.  We are now ready to load the next packet and
         * decrypt it (entirely) into our result buffer.
         */

        /* Collect the length if we haven't already */
        if (PqGSSRecvLength as usize) < std::mem::size_of::<uint32>() {
            ret = secure_raw_read(
                port,
                PqGSSRecvBuffer.add(PqGSSRecvLength as usize) as *mut c_void,
                (std::mem::size_of::<uint32>() - PqGSSRecvLength as usize) as Size,
            );

            /* If ret <= 0, secure_raw_read already set the correct errno */
            if ret <= 0 {
                return ret;
            }

            PqGSSRecvLength += ret as c_int;

            /* If we still haven't got the length, return to the caller */
            if (PqGSSRecvLength as usize) < std::mem::size_of::<uint32>() {
                set_errno(EWOULDBLOCK);
                return -1;
            }
        }

        /* Decode the packet length and check for overlength packet */
        input.length = pg_ntoh32(*(PqGSSRecvBuffer as *const uint32)) as Size;

        if input.length as usize > PQ_GSS_MAX_PACKET_SIZE - std::mem::size_of::<uint32>() {
            ereport!(
                COMMERROR,
                errmsg!(
                    "oversize GSSAPI packet sent by the client ({} > {})",
                    input.length as usize,
                    PQ_GSS_MAX_PACKET_SIZE - std::mem::size_of::<uint32>()
                )
            );
            set_errno(ECONNRESET);
            return -1;
        }

        /*
         * Read as much of the packet as we are able to on this call into
         * wherever we left off from the last time we were called.
         */
        ret = secure_raw_read(
            port,
            PqGSSRecvBuffer.add(PqGSSRecvLength as usize) as *mut c_void,
            (input.length as usize - (PqGSSRecvLength as usize - std::mem::size_of::<uint32>()))
                as Size,
        );
        /* If ret <= 0, secure_raw_read already set the correct errno */
        if ret <= 0 {
            return ret;
        }

        PqGSSRecvLength += ret as c_int;

        /* If we don't yet have the whole packet, return to the caller */
        if (PqGSSRecvLength as usize - std::mem::size_of::<uint32>()) < input.length as usize {
            set_errno(EWOULDBLOCK);
            return -1;
        }

        /*
         * We now have the full packet and we can perform the decryption and
         * refill our result buffer, then loop back up to pass data back to
         * the caller.
         */
        output.value = std::ptr::null_mut();
        output.length = 0;
        input.value = PqGSSRecvBuffer.add(std::mem::size_of::<uint32>()) as *mut c_void;

        let major = gss_unwrap(
            &mut minor,
            gctx,
            &input,
            &mut output,
            &mut conf_state,
            std::ptr::null_mut(),
        );
        if major != GSS_S_COMPLETE {
            pg_GSS_error(_(c"GSSAPI unwrap error".as_ptr()), major, minor);
            set_errno(ECONNRESET);
            return -1;
        }
        if conf_state == 0 {
            ereport!(
                COMMERROR,
                errmsg!("incoming GSSAPI message did not use confidentiality")
            );
            set_errno(ECONNRESET);
            return -1;
        }

        std::ptr::copy_nonoverlapping(
            output.value as *const u8,
            PqGSSResultBuffer as *mut u8,
            output.length as usize,
        );
        PqGSSResultLength = output.length as c_int;

        /* Our receive buffer is now empty, reset it */
        PqGSSRecvLength = 0;

        /* Release buffer storage allocated by GSSAPI */
        gss_release_buffer(&mut minor, &mut output);
    }

    let _ = major;

    bytes_returned as ssize_t
}

/*
 * Read the specified number of bytes off the wire, waiting using
 * WaitLatchOrSocket if we would block.
 *
 * Results are read into PqGSSRecvBuffer.
 *
 * Will always return either -1, to indicate a permanent error, or len.
 */
unsafe fn read_or_wait(port: *mut Port, len: ssize_t) -> ssize_t {
    let mut ret: ssize_t;

    /*
     * Keep going until we either read in everything we were asked to, or we
     * error out.
     */
    while (PqGSSRecvLength as ssize_t) < len {
        ret = secure_raw_read(
            port,
            PqGSSRecvBuffer.add(PqGSSRecvLength as usize) as *mut c_void,
            (len - PqGSSRecvLength as ssize_t) as Size,
        );

        /*
         * If we got back an error and it wasn't just
         * EWOULDBLOCK/EAGAIN/EINTR, then give up.
         */
        if ret < 0 && !(errno() == EWOULDBLOCK || errno() == EAGAIN || errno() == EINTR) {
            return -1;
        }

        /*
         * Ok, we got back either a positive value, zero, or a negative result
         * indicating we should retry.
         *
         * If it was zero or negative, then we wait on the socket to be
         * readable again.
         */
        if ret <= 0 {
            WaitLatchOrSocket(
                std::ptr::null_mut(),
                WL_SOCKET_READABLE | WL_EXIT_ON_PM_DEATH,
                (*port).sock,
                0,
                WAIT_EVENT_GSS_OPEN_SERVER,
            );

            /*
             * If we got back zero bytes, and then waited on the socket to be
             * readable and got back zero bytes on a second read, then this is
             * EOF and the client hung up on us.
             *
             * If we did get data here, then we can just fall through and
             * handle it just as if we got data the first time.
             *
             * Otherwise loop back to the top and try again.
             */
            if ret == 0 {
                ret = secure_raw_read(
                    port,
                    PqGSSRecvBuffer.add(PqGSSRecvLength as usize) as *mut c_void,
                    (len - PqGSSRecvLength as ssize_t) as Size,
                );
                if ret == 0 {
                    return -1;
                }
            }
            if ret < 0 {
                continue;
            }
        }

        PqGSSRecvLength += ret as c_int;
    }

    len
}

/*
 * Start up a GSSAPI-encrypted connection.  This performs GSSAPI
 * authentication; after this function completes, it is safe to call
 * be_gssapi_read and be_gssapi_write.  Returns -1 and logs on failure;
 * otherwise, returns 0 and marks the connection as ready for GSSAPI
 * encryption.
 *
 * Note that unlike the be_gssapi_read/be_gssapi_write functions, this
 * function WILL block on the socket to be ready for read/write (using
 * WaitLatchOrSocket) as appropriate while establishing the GSSAPI
 * session.
 */
pub unsafe fn secure_open_gssapi(port: *mut Port) -> ssize_t {
    let mut complete_next: bool = false;
    let mut major: OM_uint32;
    let mut minor: OM_uint32 = 0;
    let mut delegated_creds: gss_cred_id_t;

    INJECTION_POINT!("backend-gssapi-startup", std::ptr::null_mut());

    /*
     * Allocate subsidiary Port data for GSSAPI operations.
     */
    (*port).gss = MemoryContextAllocZero(
        TopMemoryContext,
        std::mem::size_of::<pg_gssinfo>() as Size,
    ) as *mut pg_gssinfo;

    delegated_creds = GSS_C_NO_CREDENTIAL;
    (*(*port).gss).delegated_creds = false;

    /*
     * Allocate buffers and initialize state variables.  By malloc'ing the
     * buffers at this point, we avoid wasting static data space in processes
     * that will never use them, and we ensure that the buffers are
     * sufficiently aligned for the length-word accesses that we do in some
     * places in this file.
     *
     * We'll use PQ_GSS_AUTH_BUFFER_SIZE-sized buffers until transport
     * negotiation is complete, then switch to PQ_GSS_MAX_PACKET_SIZE.
     */
    PqGSSSendBuffer = malloc(PQ_GSS_AUTH_BUFFER_SIZE) as *mut c_char;
    PqGSSRecvBuffer = malloc(PQ_GSS_AUTH_BUFFER_SIZE) as *mut c_char;
    PqGSSResultBuffer = malloc(PQ_GSS_AUTH_BUFFER_SIZE) as *mut c_char;
    if PqGSSSendBuffer.is_null() || PqGSSRecvBuffer.is_null() || PqGSSResultBuffer.is_null() {
        /* C also: errcode(ERRCODE_OUT_OF_MEMORY) */
        ereport!(FATAL, errmsg!("out of memory"));
    }
    PqGSSSendLength = 0;
    PqGSSSendNext = 0;
    PqGSSSendConsumed = 0;
    PqGSSRecvLength = 0;
    PqGSSResultLength = 0;
    PqGSSResultNext = 0;

    /*
     * Use the configured keytab, if there is one.  As we now require MIT
     * Kerberos, we might consider using the credential store extensions in
     * the future instead of the environment variable.
     */
    if !pg_krb_server_keyfile.is_null() && *pg_krb_server_keyfile != 0 {
        let keyfile = CStr::from_ptr(pg_krb_server_keyfile)
            .to_string_lossy()
            .into_owned();
        if setenv(
            c"KRB5_KTNAME".as_ptr(),
            pg_krb_server_keyfile,
            1,
        ) != 0
        {
            let _ = keyfile;
            /* The only likely failure cause is OOM, so use that errcode */
            /* C also: errcode(ERRCODE_OUT_OF_MEMORY) */
            let m = CStr::from_ptr(strerror(errno())).to_string_lossy();
            ereport!(FATAL, errmsg!("could not set environment: {}", m));
        }
    }

    loop {
        let mut ret: ssize_t;
        let mut input: gss_buffer_desc = std::mem::zeroed();
        let mut output: gss_buffer_desc = GSS_C_EMPTY_BUFFER();

        /*
         * The client always sends first, so try to go ahead and read the
         * length and wait on the socket to be readable again if that fails.
         */
        ret = read_or_wait(port, std::mem::size_of::<uint32>() as ssize_t);
        if ret < 0 {
            return ret;
        }

        /*
         * Get the length for this packet from the length header.
         */
        input.length = pg_ntoh32(*(PqGSSRecvBuffer as *const uint32)) as Size;

        /* Done with the length, reset our buffer */
        PqGSSRecvLength = 0;

        /*
         * During initialization, packets are always fully consumed and
         * shouldn't ever be over PQ_GSS_AUTH_BUFFER_SIZE in total length.
         *
         * Verify on our side that the client doesn't do something funny.
         */
        if input.length as usize > PQ_GSS_AUTH_BUFFER_SIZE - std::mem::size_of::<uint32>() {
            ereport!(
                COMMERROR,
                errmsg!(
                    "oversize GSSAPI packet sent by the client ({} > {})",
                    input.length as usize,
                    PQ_GSS_AUTH_BUFFER_SIZE - std::mem::size_of::<uint32>()
                )
            );
            return -1;
        }

        /*
         * Get the rest of the packet so we can pass it to GSSAPI to accept
         * the context.
         */
        ret = read_or_wait(port, input.length as ssize_t);
        if ret < 0 {
            return ret;
        }

        input.value = PqGSSRecvBuffer as *mut c_void;

        /* Process incoming data.  (The client sends first.) */
        major = gss_accept_sec_context(
            &mut minor,
            &mut (*(*port).gss).ctx,
            GSS_C_NO_CREDENTIAL,
            &input,
            GSS_C_NO_CHANNEL_BINDINGS,
            &mut (*(*port).gss).name,
            std::ptr::null_mut(),
            &mut output,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            if pg_gss_accept_delegation {
                &mut delegated_creds
            } else {
                std::ptr::null_mut()
            },
        );

        if GSS_ERROR(major) {
            pg_GSS_error(
                _(c"could not accept GSSAPI security context".as_ptr()),
                major,
                minor,
            );
            gss_release_buffer(&mut minor, &mut output);
            return -1;
        } else if (major & GSS_S_CONTINUE_NEEDED) == 0 {
            /*
             * rfc2744 technically permits context negotiation to be complete
             * both with and without a packet to be sent.
             */
            complete_next = true;
        }

        if delegated_creds != GSS_C_NO_CREDENTIAL {
            pg_store_delegated_credential(delegated_creds);
            (*(*port).gss).delegated_creds = true;
        }

        /* Done handling the incoming packet, reset our buffer */
        PqGSSRecvLength = 0;

        /*
         * Check if we have data to send and, if we do, make sure to send it
         * all
         */
        if output.length > 0 {
            let netlen: uint32 = pg_hton32(output.length as uint32);

            if output.length as usize > PQ_GSS_AUTH_BUFFER_SIZE - std::mem::size_of::<uint32>() {
                ereport!(
                    COMMERROR,
                    errmsg!(
                        "server tried to send oversize GSSAPI packet ({} > {})",
                        output.length as usize,
                        PQ_GSS_AUTH_BUFFER_SIZE - std::mem::size_of::<uint32>()
                    )
                );
                gss_release_buffer(&mut minor, &mut output);
                return -1;
            }

            std::ptr::copy_nonoverlapping(
                &netlen as *const uint32 as *const u8,
                PqGSSSendBuffer as *mut u8,
                std::mem::size_of::<uint32>(),
            );
            PqGSSSendLength += std::mem::size_of::<uint32>() as c_int;

            std::ptr::copy_nonoverlapping(
                output.value as *const u8,
                PqGSSSendBuffer.add(PqGSSSendLength as usize) as *mut u8,
                output.length as usize,
            );
            PqGSSSendLength += output.length as c_int;

            /* we don't bother with PqGSSSendConsumed here */

            while PqGSSSendNext < PqGSSSendLength {
                ret = secure_raw_write(
                    port,
                    PqGSSSendBuffer.add(PqGSSSendNext as usize) as *const c_void,
                    (PqGSSSendLength - PqGSSSendNext) as Size,
                );

                /*
                 * If we got back an error and it wasn't just
                 * EWOULDBLOCK/EAGAIN/EINTR, then give up.
                 */
                if ret < 0
                    && !(errno() == EWOULDBLOCK || errno() == EAGAIN || errno() == EINTR)
                {
                    gss_release_buffer(&mut minor, &mut output);
                    return -1;
                }

                /* Wait and retry if we couldn't write yet */
                if ret <= 0 {
                    WaitLatchOrSocket(
                        std::ptr::null_mut(),
                        WL_SOCKET_WRITEABLE | WL_EXIT_ON_PM_DEATH,
                        (*port).sock,
                        0,
                        WAIT_EVENT_GSS_OPEN_SERVER,
                    );
                    continue;
                }

                PqGSSSendNext += ret as c_int;
            }

            /* Done sending the packet, reset our buffer */
            PqGSSSendLength = 0;
            PqGSSSendNext = 0;

            gss_release_buffer(&mut minor, &mut output);
        }

        /*
         * If we got back that the connection is finished being set up, now
         * that we've sent the last packet, exit our loop.
         */
        if complete_next {
            break;
        }
    }

    /*
     * Release the large authentication buffers and allocate the ones we want
     * for normal operation.
     */
    free(PqGSSSendBuffer as *mut c_void);
    free(PqGSSRecvBuffer as *mut c_void);
    free(PqGSSResultBuffer as *mut c_void);
    PqGSSSendBuffer = malloc(PQ_GSS_MAX_PACKET_SIZE) as *mut c_char;
    PqGSSRecvBuffer = malloc(PQ_GSS_MAX_PACKET_SIZE) as *mut c_char;
    PqGSSResultBuffer = malloc(PQ_GSS_MAX_PACKET_SIZE) as *mut c_char;
    if PqGSSSendBuffer.is_null() || PqGSSRecvBuffer.is_null() || PqGSSResultBuffer.is_null() {
        /* C also: errcode(ERRCODE_OUT_OF_MEMORY) */
        ereport!(FATAL, errmsg!("out of memory"));
    }
    PqGSSSendLength = 0;
    PqGSSSendNext = 0;
    PqGSSSendConsumed = 0;
    PqGSSRecvLength = 0;
    PqGSSResultLength = 0;
    PqGSSResultNext = 0;

    /*
     * Determine the max packet size which will fit in our buffer, after
     * accounting for the length.  be_gssapi_write will need this.
     */
    major = gss_wrap_size_limit(
        &mut minor,
        (*(*port).gss).ctx,
        1,
        GSS_C_QOP_DEFAULT,
        (PQ_GSS_MAX_PACKET_SIZE - std::mem::size_of::<uint32>()) as OM_uint32,
        &mut PqGSSMaxPktSize,
    );

    if GSS_ERROR(major) {
        pg_GSS_error(_(c"GSSAPI size check error".as_ptr()), major, minor);
        return -1;
    }

    (*(*port).gss).enc = true;

    0
}

/*
 * Return if GSSAPI authentication was used on this connection.
 */
pub unsafe fn be_gssapi_get_auth(port: *mut Port) -> bool {
    if port.is_null() || (*port).gss.is_null() {
        return false;
    }

    (*(*port).gss).auth
}

/*
 * Return if GSSAPI encryption is enabled and being used on this connection.
 */
pub unsafe fn be_gssapi_get_enc(port: *mut Port) -> bool {
    if port.is_null() || (*port).gss.is_null() {
        return false;
    }

    (*(*port).gss).enc
}

/*
 * Return the GSSAPI principal used for authentication on this connection
 * (NULL if we did not perform GSSAPI authentication).
 */
pub unsafe fn be_gssapi_get_princ(port: *mut Port) -> *const c_char {
    if port.is_null() || (*port).gss.is_null() {
        return std::ptr::null();
    }

    (*(*port).gss).princ
}

/*
 * Return if GSSAPI delegated credentials were included on this
 * connection.
 */
pub unsafe fn be_gssapi_get_delegation(port: *mut Port) -> bool {
    if port.is_null() || (*port).gss.is_null() {
        return false;
    }

    (*(*port).gss).delegated_creds
}

// ---------------------------------------------------------------------------
// libc / runtime functions referenced from this file (C stdlib / pgport).
// ---------------------------------------------------------------------------

extern "C" {
    fn malloc(size: usize) -> *mut c_void;
    fn free(ptr: *mut c_void);
    fn setenv(name: *const c_char, value: *const c_char, overwrite: c_int) -> c_int;
    fn strerror(errnum: c_int) -> *mut c_char;
}

