//! be-fsstubs.c
//!   Builtin functions for open/close/read/write operations on large objects
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/libpq/be-fsstubs.c
//!
//! NOTES
//!   This should be moved to a more appropriate place.  It is here
//!   for lack of a better place.
//!
//!   These functions store LargeObjectDesc structs in a private MemoryContext,
//!   which means that large object descriptors hang around until we destroy
//!   the context at transaction end.  It'd be possible to prolong the lifetime
//!   of the context so that LO FDs are good across transactions (for example,
//!   we could release the context only if we see that no FDs remain open).
//!   But we'd need additional state in order to do the right thing at the
//!   end of an aborted transaction.  FDs opened during an aborted xact would
//!   still need to be closed, since they might not be pointing at valid
//!   relations at all.  Locking semantics are also an interesting problem
//!   if LOs stay open across transactions.  For now, we'll stick with the
//!   existing documented semantics of LO FDs: they're only good within a
//!   transaction.
//!
//!   As of PostgreSQL 8.0, much of the angst expressed above is no longer
//!   relevant, and in fact it'd be pretty easy to allow LO FDs to stay
//!   open across transactions.  (Snapshot relevancy would still be an issue.)
//!   However backwards compatibility suggests that we should stick to the
//!   status quo.

use crate::prelude::*;

use crate::{
    PG_GETARG_BYTEA_PP, PG_GETARG_INT32, PG_GETARG_INT64, PG_GETARG_OID, PG_GETARG_TEXT_PP,
    PG_RETURN_BYTEA_P, PG_RETURN_INT32, PG_RETURN_INT64, PG_RETURN_OID, PG_RETURN_VOID,
};

use crate::c::{int32, int64, varlena, Size, SubTransactionId};
use crate::libpq::libpq_fs::{INV_READ, INV_WRITE};
use crate::postgres::Datum;
use crate::utils::fmgr::FunctionCallInfo;
use crate::varatt::{SET_VARSIZE, VARDATA, VARDATA_ANY, VARSIZE_ANY_EXHDR};

// c.h: text/bytea are varlena.
use crate::c::{bytea, text};
use crate::c::VARHDRSZ;

// storage/large_object.h: LargeObjectDesc and flag bits.
use crate::storage::large_object::{
    lo_compat_privileges, LargeObjectDesc, IFS_RDLOCK, IFS_WRLOCK,
};
// inv_api.c (translated in parallel): inversion-storage routines.
use crate::storage::large_object::inv_api::{
    close_lo_relation, inv_close, inv_create, inv_drop, inv_open, inv_read, inv_seek, inv_tell,
    inv_truncate, inv_write,
};

// miscadmin.h: PreventCommandIfReadOnly, GetUserId.
use crate::miscadmin::{GetUserId, PreventCommandIfReadOnly};
// utils/memutils.h: alloc/limit helpers come in through the prelude.
// catalog/pg_largeobject.h: LargeObjectRelationId.
use crate::catalog::catalog_oids::LargeObjectRelationId;

// MemoryContext used by inv_open() is the large_object.h local typedef
// (*mut c_void).  Bring it in under an alias so casts read naturally.
use crate::utils::palloc::MemoryContext as LoMemoryContext;
// Snapshot type used by LargeObjectDesc.snapshot.
use crate::storage::large_object::Snapshot;

/* define this to enable debug logging */
/* #define FSDB 1 */
/* chunk size for lo_import/lo_export transfers */
const BUFSIZE: usize = 8192;

/*
 * LO "FD"s are indexes into the cookies array.
 *
 * A non-null entry is a pointer to a LargeObjectDesc allocated in the
 * LO private memory context "fscxt".  The cookies array itself is also
 * dynamically allocated in that context.  Its current allocated size is
 * cookies_size entries, of which any unused entries will be NULL.
 */
static mut cookies: *mut *mut LargeObjectDesc = null_mut();
static mut cookies_size: c_int = 0;

static mut lo_cleanup_needed: bool = false;
static mut fscxt: MemoryContext = null_mut();

/*****************************************************************************
 *	Stubs for symbols whose home module is not yet translated.
 *****************************************************************************/

// utils/elog.h: errcode_for_file_access().  TODO(pg-port): real one lives in
// utils/error/elog.c.
unsafe fn errcode_for_file_access() -> c_int {
    0
}

// access/xact.h: GetCurrentSubTransactionId().
// TODO(pg-port): real GetCurrentSubTransactionId lives in access/transam/xact.c.
unsafe fn GetCurrentSubTransactionId() -> SubTransactionId {
    unimplemented!()
}

// utils/snapmgr.h: snapshot resource-owner registration.
// TODO(pg-port): real RegisterSnapshotOnOwner lives in utils/time/snapmgr.c.
unsafe fn RegisterSnapshotOnOwner(_snapshot: Snapshot, _owner: ResourceOwner) -> Snapshot {
    unimplemented!()
}
// TODO(pg-port): real UnregisterSnapshotFromOwner lives in utils/time/snapmgr.c.
unsafe fn UnregisterSnapshotFromOwner(_snapshot: Snapshot, _owner: ResourceOwner) {
    unimplemented!()
}

// utils/resowner.h: ResourceOwner + TopTransactionResourceOwner.
// TODO(pg-port): real ResourceOwner lives in utils/resowner/resowner.c.
type ResourceOwner = *mut c_void;
#[allow(non_upper_case_globals)]
static mut TopTransactionResourceOwner: ResourceOwner = null_mut();

// catalog/pg_largeobject.h: LargeObjectExists().
// TODO(pg-port): real LargeObjectExists lives in catalog/pg_largeobject.c.
unsafe fn LargeObjectExists(_loid: Oid) -> bool {
    unimplemented!()
}

// utils/acl.h: object_ownercheck().
// TODO(pg-port): real object_ownercheck lives in catalog/aclchk.c.
unsafe fn object_ownercheck(_classid: Oid, _objectid: Oid, _roleid: Oid) -> bool {
    unimplemented!()
}

// utils/builtins.h: text_to_cstring_buffer().
// TODO(pg-port): real text_to_cstring_buffer lives in utils/adt/varlena.c (a
// shim already exists in utils/builtins.rs; re-import once wired).
unsafe fn text_to_cstring_buffer(src: *const text, dst: *mut c_char, dst_len: Size) {
    crate::utils::builtins::text_to_cstring_buffer(src, dst, dst_len)
}

// storage/fd.h: transient-file helpers.
// TODO(pg-port): real OpenTransientFile lives in storage/file/fd.c.
unsafe fn OpenTransientFile(_fileName: *const c_char, _fileFlags: c_int) -> c_int {
    unimplemented!()
}
// TODO(pg-port): real OpenTransientFilePerm lives in storage/file/fd.c.
unsafe fn OpenTransientFilePerm(
    _fileName: *const c_char,
    _fileFlags: c_int,
    _fileMode: mode_t,
) -> c_int {
    unimplemented!()
}
// TODO(pg-port): real CloseTransientFile lives in storage/file/fd.c.
unsafe fn CloseTransientFile(_fd: c_int) -> c_int {
    unimplemented!()
}

// <unistd.h>: read()/write().
// TODO(pg-port): use a real libc binding when the port grows one.
unsafe fn read(_fd: c_int, _buf: *mut c_void, _count: usize) -> isize {
    unimplemented!()
}
unsafe fn write(_fd: c_int, _buf: *const c_void, _count: usize) -> isize {
    unimplemented!()
}
// <sys/stat.h>: umask().
unsafe fn umask(_mask: mode_t) -> mode_t {
    unimplemented!()
}

// repalloc0_array(): grow a palloc'd array, zeroing the new tail.
// TODO(pg-port): real repalloc0_array macro lives in utils/palloc.h.
unsafe fn repalloc0_array(
    pointer: *mut *mut LargeObjectDesc,
    oldlen: c_int,
    newlen: c_int,
) -> *mut *mut LargeObjectDesc {
    let elemsz = core::mem::size_of::<*mut LargeObjectDesc>();
    let newptr = repalloc(pointer as *mut c_void, newlen as usize * elemsz) as *mut *mut LargeObjectDesc;
    let added = (newlen - oldlen) as usize;
    core::ptr::write_bytes(newptr.add(oldlen as usize), 0, added);
    newptr
}

/* type aliases for libc scalar types used below */
#[allow(non_camel_case_types)]
type mode_t = u32;

/* <fcntl.h> open flags */
const O_RDONLY: c_int = 0x0000;
const O_WRONLY: c_int = 0x0001;
const O_CREAT: c_int = 0x0200;
const O_TRUNC: c_int = 0x0400;
/* c.h: PG_BINARY */
use crate::c::PG_BINARY;

/* <sys/stat.h> mode bits */
const S_IRUSR: mode_t = 0o400;
const S_IWUSR: mode_t = 0o200;
const S_IRGRP: mode_t = 0o040;
const S_IWGRP: mode_t = 0o020;
const S_IROTH: mode_t = 0o004;
const S_IWOTH: mode_t = 0o002;

/* <stdio.h>/<unistd.h> seek whence values */
const SEEK_SET: c_int = 0;
const SEEK_END: c_int = 2;

/* pg_config_manual.h: MAXPGPATH */
use crate::pg_config_manual::MAXPGPATH;

/* errcodes.h classification (errcode() shim ignores the value). */
const ERRCODE_UNDEFINED_OBJECT: c_int = 0;
const ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE: c_int = 0;
const ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE: c_int = 0;
const ERRCODE_INSUFFICIENT_PRIVILEGE: c_int = 0;
const ERRCODE_PROGRAM_LIMIT_EXCEEDED: c_int = 0;
const ERRCODE_INVALID_PARAMETER_VALUE: c_int = 0;

/*****************************************************************************
 *	File Interfaces for Large Objects
 *****************************************************************************/

pub unsafe fn be_lo_open(fcinfo: FunctionCallInfo) -> Datum {
    let lobjId: Oid = PG_GETARG_OID!(fcinfo, 0);
    let mode: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let lobjDesc: *mut LargeObjectDesc;
    let fd: c_int;

    // #ifdef FSDB
    // elog!(DEBUG4, "lo_open({},{})", lobjId, mode);
    // #endif

    if mode & INV_WRITE != 0 {
        PreventCommandIfReadOnly(c"lo_open(INV_WRITE)".as_ptr());
    }

    /*
     * Allocate a large object descriptor first.  This will also create
     * 'fscxt' if this is the first LO opened in this transaction.
     */
    fd = newLOfd();

    lobjDesc = inv_open(lobjId, mode, fscxt as LoMemoryContext);
    (*lobjDesc).subid = GetCurrentSubTransactionId();

    /*
     * We must register the snapshot in TopTransaction's resowner so that it
     * stays alive until the LO is closed rather than until the current portal
     * shuts down.
     */
    if !(*lobjDesc).snapshot.is_null() {
        (*lobjDesc).snapshot =
            RegisterSnapshotOnOwner((*lobjDesc).snapshot, TopTransactionResourceOwner);
    }

    Assert!((*cookies.add(fd as usize)).is_null());
    *cookies.add(fd as usize) = lobjDesc;

    PG_RETURN_INT32!(fd)
}

pub unsafe fn be_lo_close(fcinfo: FunctionCallInfo) -> Datum {
    let fd: int32 = PG_GETARG_INT32!(fcinfo, 0);

    if fd < 0 || fd >= cookies_size || (*cookies.add(fd as usize)).is_null() {
        let _ = errcode(ERRCODE_UNDEFINED_OBJECT);
        ereport!(
            ERROR,
            errmsg!("invalid large-object descriptor: {}", fd)
        );
    }

    // #ifdef FSDB
    // elog!(DEBUG4, "lo_close({})", fd);
    // #endif

    closeLOfd(fd);

    PG_RETURN_INT32!(0)
}


/*****************************************************************************
 *	Bare Read/Write operations --- these are not fmgr-callable!
 *
 *	We assume the large object supports byte oriented reads and seeks so
 *	that our work is easier.
 *
 *****************************************************************************/

pub unsafe fn lo_read(fd: c_int, buf: *mut c_char, len: c_int) -> c_int {
    let status: c_int;
    let lobj: *mut LargeObjectDesc;

    if fd < 0 || fd >= cookies_size || (*cookies.add(fd as usize)).is_null() {
        let _ = errcode(ERRCODE_UNDEFINED_OBJECT);
        ereport!(
            ERROR,
            errmsg!("invalid large-object descriptor: {}", fd)
        );
    }
    lobj = *cookies.add(fd as usize);

    /*
     * Check state.  inv_read() would throw an error anyway, but we want the
     * error to be about the FD's state not the underlying privilege; it might
     * be that the privilege exists but user forgot to ask for read mode.
     */
    if (*lobj).flags & IFS_RDLOCK == 0 {
        let _ = errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
        ereport!(
            ERROR,
            errmsg!(
                "large object descriptor {} was not opened for reading",
                fd
            )
        );
    }

    status = inv_read(lobj, buf, len);

    status
}

pub unsafe fn lo_write(fd: c_int, buf: *const c_char, len: c_int) -> c_int {
    let status: c_int;
    let lobj: *mut LargeObjectDesc;

    if fd < 0 || fd >= cookies_size || (*cookies.add(fd as usize)).is_null() {
        let _ = errcode(ERRCODE_UNDEFINED_OBJECT);
        ereport!(
            ERROR,
            errmsg!("invalid large-object descriptor: {}", fd)
        );
    }
    lobj = *cookies.add(fd as usize);

    /* see comment in lo_read() */
    if (*lobj).flags & IFS_WRLOCK == 0 {
        let _ = errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
        ereport!(
            ERROR,
            errmsg!(
                "large object descriptor {} was not opened for writing",
                fd
            )
        );
    }

    status = inv_write(lobj, buf, len);

    status
}

pub unsafe fn be_lo_lseek(fcinfo: FunctionCallInfo) -> Datum {
    let fd: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let offset: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let whence: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let status: int64;

    if fd < 0 || fd >= cookies_size || (*cookies.add(fd as usize)).is_null() {
        let _ = errcode(ERRCODE_UNDEFINED_OBJECT);
        ereport!(
            ERROR,
            errmsg!("invalid large-object descriptor: {}", fd)
        );
    }

    status = inv_seek(*cookies.add(fd as usize), offset as int64, whence);

    /* guard against result overflow */
    if status != status as int32 as int64 {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        ereport!(
            ERROR,
            errmsg!(
                "lo_lseek result out of range for large-object descriptor {}",
                fd
            )
        );
    }

    PG_RETURN_INT32!(status as int32)
}

pub unsafe fn be_lo_lseek64(fcinfo: FunctionCallInfo) -> Datum {
    let fd: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let offset: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let whence: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let status: int64;

    if fd < 0 || fd >= cookies_size || (*cookies.add(fd as usize)).is_null() {
        let _ = errcode(ERRCODE_UNDEFINED_OBJECT);
        ereport!(
            ERROR,
            errmsg!("invalid large-object descriptor: {}", fd)
        );
    }

    status = inv_seek(*cookies.add(fd as usize), offset, whence);

    PG_RETURN_INT64!(status)
}

pub unsafe fn be_lo_creat(_fcinfo: FunctionCallInfo) -> Datum {
    let lobjId: Oid;

    PreventCommandIfReadOnly(c"lo_creat()".as_ptr());

    lo_cleanup_needed = true;
    lobjId = inv_create(InvalidOid);

    PG_RETURN_OID!(lobjId)
}

pub unsafe fn be_lo_create(fcinfo: FunctionCallInfo) -> Datum {
    let mut lobjId: Oid = PG_GETARG_OID!(fcinfo, 0);

    PreventCommandIfReadOnly(c"lo_create()".as_ptr());

    lo_cleanup_needed = true;
    lobjId = inv_create(lobjId);

    PG_RETURN_OID!(lobjId)
}

pub unsafe fn be_lo_tell(fcinfo: FunctionCallInfo) -> Datum {
    let fd: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let offset: int64;

    if fd < 0 || fd >= cookies_size || (*cookies.add(fd as usize)).is_null() {
        let _ = errcode(ERRCODE_UNDEFINED_OBJECT);
        ereport!(
            ERROR,
            errmsg!("invalid large-object descriptor: {}", fd)
        );
    }

    offset = inv_tell(*cookies.add(fd as usize));

    /* guard against result overflow */
    if offset != offset as int32 as int64 {
        let _ = errcode(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE);
        ereport!(
            ERROR,
            errmsg!(
                "lo_tell result out of range for large-object descriptor {}",
                fd
            )
        );
    }

    PG_RETURN_INT32!(offset as int32)
}

pub unsafe fn be_lo_tell64(fcinfo: FunctionCallInfo) -> Datum {
    let fd: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let offset: int64;

    if fd < 0 || fd >= cookies_size || (*cookies.add(fd as usize)).is_null() {
        let _ = errcode(ERRCODE_UNDEFINED_OBJECT);
        ereport!(
            ERROR,
            errmsg!("invalid large-object descriptor: {}", fd)
        );
    }

    offset = inv_tell(*cookies.add(fd as usize));

    PG_RETURN_INT64!(offset)
}

pub unsafe fn be_lo_unlink(fcinfo: FunctionCallInfo) -> Datum {
    let lobjId: Oid = PG_GETARG_OID!(fcinfo, 0);

    PreventCommandIfReadOnly(c"lo_unlink()".as_ptr());

    if !LargeObjectExists(lobjId) {
        let _ = errcode(ERRCODE_UNDEFINED_OBJECT);
        ereport!(
            ERROR,
            errmsg!("large object {} does not exist", lobjId)
        );
    }

    /*
     * Must be owner of the large object.  It would be cleaner to check this
     * in inv_drop(), but we want to throw the error before not after closing
     * relevant FDs.
     */
    if !lo_compat_privileges
        && !object_ownercheck(LargeObjectRelationId, lobjId, GetUserId())
    {
        let _ = errcode(ERRCODE_INSUFFICIENT_PRIVILEGE);
        ereport!(
            ERROR,
            errmsg!("must be owner of large object {}", lobjId)
        );
    }

    /*
     * If there are any open LO FDs referencing that ID, close 'em.
     */
    if !fscxt.is_null() {
        let mut i: c_int = 0;

        while i < cookies_size {
            let c = *cookies.add(i as usize);
            if !c.is_null() && (*c).id == lobjId {
                closeLOfd(i);
            }
            i += 1;
        }
    }

    /*
     * inv_drop does not create a need for end-of-transaction cleanup and
     * hence we don't need to set lo_cleanup_needed.
     */
    PG_RETURN_INT32!(inv_drop(lobjId))
}

/*****************************************************************************
 *	Read/Write using bytea
 *****************************************************************************/

pub unsafe fn be_loread(fcinfo: FunctionCallInfo) -> Datum {
    let fd: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let mut len: int32 = PG_GETARG_INT32!(fcinfo, 1);
    let retval: *mut bytea;
    let totalread: c_int;

    if len < 0 {
        len = 0;
    }

    retval = palloc(VARHDRSZ as usize + len as usize) as *mut bytea;
    totalread = lo_read(fd, VARDATA(retval as *const c_char), len);
    SET_VARSIZE(retval as *mut c_char, totalread + VARHDRSZ);

    PG_RETURN_BYTEA_P!(retval)
}

pub unsafe fn be_lowrite(fcinfo: FunctionCallInfo) -> Datum {
    let fd: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let wbuf: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let bytestowrite: c_int;
    let totalwritten: c_int;

    PreventCommandIfReadOnly(c"lowrite()".as_ptr());

    bytestowrite = VARSIZE_ANY_EXHDR(wbuf as *const c_char) as c_int;
    totalwritten = lo_write(fd, VARDATA_ANY(wbuf as *const c_char), bytestowrite);
    PG_RETURN_INT32!(totalwritten)
}

/*****************************************************************************
 *	 Import/Export of Large Object
 *****************************************************************************/

/*
 * lo_import -
 *	  imports a file as an (inversion) large object.
 */
pub unsafe fn be_lo_import(fcinfo: FunctionCallInfo) -> Datum {
    let filename: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);

    PG_RETURN_OID!(lo_import_internal(filename, InvalidOid))
}

/*
 * lo_import_with_oid -
 *	  imports a file as an (inversion) large object specifying oid.
 */
pub unsafe fn be_lo_import_with_oid(fcinfo: FunctionCallInfo) -> Datum {
    let filename: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 0);
    let oid: Oid = PG_GETARG_OID!(fcinfo, 1);

    PG_RETURN_OID!(lo_import_internal(filename, oid))
}

unsafe fn lo_import_internal(filename: *mut text, lobjOid: Oid) -> Oid {
    let fd: c_int;
    let mut nbytes: c_int;
    #[allow(unused_assignments)]
    let mut tmp: c_int; // PG_USED_FOR_ASSERTS_ONLY
    let mut buf: [c_char; BUFSIZE] = [0; BUFSIZE];
    let mut fnamebuf: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let lobj: *mut LargeObjectDesc;
    let oid: Oid;

    PreventCommandIfReadOnly(c"lo_import()".as_ptr());

    /*
     * open the file to be read in
     */
    text_to_cstring_buffer(filename, fnamebuf.as_mut_ptr(), core::mem::size_of_val(&fnamebuf));
    fd = OpenTransientFile(fnamebuf.as_ptr(), O_RDONLY | PG_BINARY);
    if fd < 0 {
        let _ = errcode_for_file_access();
        ereport!(
            ERROR,
            errmsg!(
                "could not open server file \"{}\": %m",
                cstr(fnamebuf.as_ptr())
            )
        );
    }

    /*
     * create an inversion object
     */
    lo_cleanup_needed = true;
    oid = inv_create(lobjOid);

    /*
     * read in from the filesystem and write to the inversion object
     */
    lobj = inv_open(oid, INV_WRITE, CurrentMemoryContext as LoMemoryContext);

    loop {
        nbytes = read(fd, buf.as_mut_ptr() as *mut c_void, BUFSIZE) as c_int;
        if nbytes <= 0 {
            break;
        }
        tmp = inv_write(lobj, buf.as_ptr(), nbytes);
        Assert!(tmp == nbytes);
    }

    if nbytes < 0 {
        let _ = errcode_for_file_access();
        ereport!(
            ERROR,
            errmsg!(
                "could not read server file \"{}\": %m",
                cstr(fnamebuf.as_ptr())
            )
        );
    }

    inv_close(lobj);

    if CloseTransientFile(fd) != 0 {
        let _ = errcode_for_file_access();
        ereport!(
            ERROR,
            errmsg!(
                "could not close file \"{}\": %m",
                cstr(fnamebuf.as_ptr())
            )
        );
    }

    oid
}

/*
 * lo_export -
 *	  exports an (inversion) large object.
 */
pub unsafe fn be_lo_export(fcinfo: FunctionCallInfo) -> Datum {
    let lobjId: Oid = PG_GETARG_OID!(fcinfo, 0);
    let filename: *mut text = PG_GETARG_TEXT_PP!(fcinfo, 1);
    let fd: c_int;
    let mut nbytes: c_int;
    let mut tmp: c_int;
    let mut buf: [c_char; BUFSIZE] = [0; BUFSIZE];
    let mut fnamebuf: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let lobj: *mut LargeObjectDesc;
    let oumask: mode_t;

    /*
     * open the inversion object (no need to test for failure)
     */
    lo_cleanup_needed = true;
    lobj = inv_open(lobjId, INV_READ, CurrentMemoryContext as LoMemoryContext);

    /*
     * open the file to be written to
     *
     * Note: we reduce backend's normal 077 umask to the slightly friendlier
     * 022. This code used to drop it all the way to 0, but creating
     * world-writable export files doesn't seem wise.
     */
    text_to_cstring_buffer(filename, fnamebuf.as_mut_ptr(), core::mem::size_of_val(&fnamebuf));
    oumask = umask(S_IWGRP | S_IWOTH);
    // PG_TRY()/PG_FINALLY(): restore the umask whatever happens.  The shim
    // ereport! does not unwind, so a straight-line restore matches behavior.
    fd = OpenTransientFilePerm(
        fnamebuf.as_ptr(),
        O_CREAT | O_WRONLY | O_TRUNC | PG_BINARY,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH,
    );
    umask(oumask);
    if fd < 0 {
        let _ = errcode_for_file_access();
        ereport!(
            ERROR,
            errmsg!(
                "could not create server file \"{}\": %m",
                cstr(fnamebuf.as_ptr())
            )
        );
    }

    /*
     * read in from the inversion file and write to the filesystem
     */
    loop {
        nbytes = inv_read(lobj, buf.as_mut_ptr(), BUFSIZE as c_int);
        if nbytes <= 0 {
            break;
        }
        tmp = write(fd, buf.as_ptr() as *const c_void, nbytes as usize) as c_int;
        if tmp != nbytes {
            let _ = errcode_for_file_access();
            ereport!(
                ERROR,
                errmsg!(
                    "could not write server file \"{}\": %m",
                    cstr(fnamebuf.as_ptr())
                )
            );
        }
    }

    if CloseTransientFile(fd) != 0 {
        let _ = errcode_for_file_access();
        ereport!(
            ERROR,
            errmsg!(
                "could not close file \"{}\": %m",
                cstr(fnamebuf.as_ptr())
            )
        );
    }

    inv_close(lobj);

    PG_RETURN_INT32!(1)
}

/*
 * lo_truncate -
 *	  truncate a large object to a specified length
 */
unsafe fn lo_truncate_internal(fd: int32, len: int64) {
    let lobj: *mut LargeObjectDesc;

    if fd < 0 || fd >= cookies_size || (*cookies.add(fd as usize)).is_null() {
        let _ = errcode(ERRCODE_UNDEFINED_OBJECT);
        ereport!(
            ERROR,
            errmsg!("invalid large-object descriptor: {}", fd)
        );
    }
    lobj = *cookies.add(fd as usize);

    /* see comment in lo_read() */
    if (*lobj).flags & IFS_WRLOCK == 0 {
        let _ = errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE);
        ereport!(
            ERROR,
            errmsg!(
                "large object descriptor {} was not opened for writing",
                fd
            )
        );
    }

    inv_truncate(lobj, len);
}

pub unsafe fn be_lo_truncate(fcinfo: FunctionCallInfo) -> Datum {
    let fd: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let len: int32 = PG_GETARG_INT32!(fcinfo, 1);

    PreventCommandIfReadOnly(c"lo_truncate()".as_ptr());

    lo_truncate_internal(fd, len as int64);
    PG_RETURN_INT32!(0)
}

pub unsafe fn be_lo_truncate64(fcinfo: FunctionCallInfo) -> Datum {
    let fd: int32 = PG_GETARG_INT32!(fcinfo, 0);
    let len: int64 = PG_GETARG_INT64!(fcinfo, 1);

    PreventCommandIfReadOnly(c"lo_truncate64()".as_ptr());

    lo_truncate_internal(fd, len);
    PG_RETURN_INT32!(0)
}

/*
 * AtEOXact_LargeObject -
 *		 prepares large objects for transaction commit
 */
pub unsafe fn AtEOXact_LargeObject(isCommit: bool) {
    let mut i: c_int;

    if !lo_cleanup_needed {
        return; /* no LO operations in this xact */
    }

    /*
     * Close LO fds and clear cookies array so that LO fds are no longer good.
     * The memory context and resource owner holding them are going away at
     * the end-of-transaction anyway, but on commit, we need to close them to
     * avoid warnings about leaked resources at commit.  On abort we can skip
     * this step.
     */
    if isCommit {
        i = 0;
        while i < cookies_size {
            if !(*cookies.add(i as usize)).is_null() {
                closeLOfd(i);
            }
            i += 1;
        }
    }

    /* Needn't actually pfree since we're about to zap context */
    cookies = null_mut();
    cookies_size = 0;

    /* Release the LO memory context to prevent permanent memory leaks. */
    if !fscxt.is_null() {
        MemoryContextDelete(fscxt);
    }
    fscxt = null_mut();

    /* Give inv_api.c a chance to clean up, too */
    close_lo_relation(isCommit);

    lo_cleanup_needed = false;
}

/*
 * AtEOSubXact_LargeObject
 *		Take care of large objects at subtransaction commit/abort
 *
 * Reassign LOs created/opened during a committing subtransaction
 * to the parent subtransaction.  On abort, just close them.
 */
pub unsafe fn AtEOSubXact_LargeObject(
    isCommit: bool,
    mySubid: SubTransactionId,
    parentSubid: SubTransactionId,
) {
    let mut i: c_int;

    if fscxt.is_null() {
        return; /* no LO operations in this xact */
    }

    i = 0;
    while i < cookies_size {
        let lo: *mut LargeObjectDesc = *cookies.add(i as usize);

        if !lo.is_null() && (*lo).subid == mySubid {
            if isCommit {
                (*lo).subid = parentSubid;
            } else {
                closeLOfd(i);
            }
        }
        i += 1;
    }
}

/*****************************************************************************
 *	Support routines for this file
 *****************************************************************************/

unsafe fn newLOfd() -> c_int {
    let mut i: c_int;
    let newsize: c_int;

    lo_cleanup_needed = true;
    if fscxt.is_null() {
        fscxt = AllocSetContextCreate!(
            TopMemoryContext,
            "Filesystem",
            ALLOCSET_DEFAULT_SIZES
        );
    }

    /* Try to find a free slot */
    i = 0;
    while i < cookies_size {
        if (*cookies.add(i as usize)).is_null() {
            return i;
        }
        i += 1;
    }

    /* No free slot, so make the array bigger */
    if cookies_size <= 0 {
        /* First time through, arbitrarily make 64-element array */
        i = 0;
        newsize = 64;
        cookies = MemoryContextAllocZero(
            fscxt,
            newsize as usize * core::mem::size_of::<*mut LargeObjectDesc>(),
        ) as *mut *mut LargeObjectDesc;
    } else {
        /* Double size of array */
        i = cookies_size;
        newsize = cookies_size * 2;
        cookies = repalloc0_array(cookies, cookies_size, newsize);
    }
    cookies_size = newsize;

    i
}

unsafe fn closeLOfd(fd: c_int) {
    let lobj: *mut LargeObjectDesc;

    /*
     * Make sure we do not try to free twice if this errors out for some
     * reason.  Better a leak than a crash.
     */
    lobj = *cookies.add(fd as usize);
    *cookies.add(fd as usize) = null_mut();

    if !(*lobj).snapshot.is_null() {
        UnregisterSnapshotFromOwner((*lobj).snapshot, TopTransactionResourceOwner);
    }
    inv_close(lobj);
}

/*****************************************************************************
 *	Wrappers oriented toward SQL callers
 *****************************************************************************/

/*
 * Read [offset, offset+nbytes) within LO; when nbytes is -1, read to end.
 */
unsafe fn lo_get_fragment_internal(loOid: Oid, offset: int64, nbytes: int32) -> *mut bytea {
    let loDesc: *mut LargeObjectDesc;
    let loSize: int64;
    let result_length: int64;
    #[allow(unused_variables)]
    let total_read: c_int; // PG_USED_FOR_ASSERTS_ONLY
    let result: *mut bytea;

    lo_cleanup_needed = true;
    loDesc = inv_open(loOid, INV_READ, CurrentMemoryContext as LoMemoryContext);

    /*
     * Compute number of bytes we'll actually read, accommodating nbytes == -1
     * and reads beyond the end of the LO.
     */
    loSize = inv_seek(loDesc, 0, SEEK_END);
    if loSize > offset {
        if nbytes >= 0 && (nbytes as int64) <= loSize - offset {
            result_length = nbytes as int64; /* request is wholly inside LO */
        } else {
            result_length = loSize - offset; /* adjust to end of LO */
        }
    } else {
        result_length = 0; /* request is wholly outside LO */
    }

    /*
     * A result_length calculated from loSize may not fit in a size_t.  Check
     * that the size will satisfy this and subsequently-enforced size limits.
     */
    if result_length > MaxAllocSize as int64 - VARHDRSZ as int64 {
        let _ = errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED);
        ereport!(
            ERROR,
            errmsg!("large object read request is too large")
        );
    }

    result = palloc(VARHDRSZ as usize + result_length as usize) as *mut bytea;

    inv_seek(loDesc, offset, SEEK_SET);
    total_read = inv_read(loDesc, VARDATA(result as *const c_char), result_length as c_int);
    Assert!(total_read as int64 == result_length);
    SET_VARSIZE(result as *mut c_char, result_length as int32 + VARHDRSZ);

    inv_close(loDesc);

    result
}

/*
 * Read entire LO
 */
pub unsafe fn be_lo_get(fcinfo: FunctionCallInfo) -> Datum {
    let loOid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let result: *mut bytea;

    result = lo_get_fragment_internal(loOid, 0, -1);

    PG_RETURN_BYTEA_P!(result)
}

/*
 * Read range within LO
 */
pub unsafe fn be_lo_get_fragment(fcinfo: FunctionCallInfo) -> Datum {
    let loOid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let offset: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let nbytes: int32 = PG_GETARG_INT32!(fcinfo, 2);
    let result: *mut bytea;

    if nbytes < 0 {
        let _ = errcode(ERRCODE_INVALID_PARAMETER_VALUE);
        ereport!(
            ERROR,
            errmsg!("requested length cannot be negative")
        );
    }

    result = lo_get_fragment_internal(loOid, offset, nbytes);

    PG_RETURN_BYTEA_P!(result)
}

/*
 * Create LO with initial contents given by a bytea argument
 */
pub unsafe fn be_lo_from_bytea(fcinfo: FunctionCallInfo) -> Datum {
    let mut loOid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let str: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 1);
    let loDesc: *mut LargeObjectDesc;
    #[allow(unused_variables)]
    let written: c_int; // PG_USED_FOR_ASSERTS_ONLY

    PreventCommandIfReadOnly(c"lo_from_bytea()".as_ptr());

    lo_cleanup_needed = true;
    loOid = inv_create(loOid);
    loDesc = inv_open(loOid, INV_WRITE, CurrentMemoryContext as LoMemoryContext);
    written = inv_write(
        loDesc,
        VARDATA_ANY(str as *const c_char),
        VARSIZE_ANY_EXHDR(str as *const c_char) as c_int,
    );
    Assert!(written == VARSIZE_ANY_EXHDR(str as *const c_char) as c_int);
    inv_close(loDesc);

    PG_RETURN_OID!(loOid)
}

/*
 * Update range within LO
 */
pub unsafe fn be_lo_put(fcinfo: FunctionCallInfo) -> Datum {
    let loOid: Oid = PG_GETARG_OID!(fcinfo, 0);
    let offset: int64 = PG_GETARG_INT64!(fcinfo, 1);
    let str: *mut bytea = PG_GETARG_BYTEA_PP!(fcinfo, 2);
    let loDesc: *mut LargeObjectDesc;
    #[allow(unused_variables)]
    let written: c_int; // PG_USED_FOR_ASSERTS_ONLY

    PreventCommandIfReadOnly(c"lo_put()".as_ptr());

    lo_cleanup_needed = true;
    loDesc = inv_open(loOid, INV_WRITE, CurrentMemoryContext as LoMemoryContext);
    inv_seek(loDesc, offset, SEEK_SET);
    written = inv_write(
        loDesc,
        VARDATA_ANY(str as *const c_char),
        VARSIZE_ANY_EXHDR(str as *const c_char) as c_int,
    );
    Assert!(written == VARSIZE_ANY_EXHDR(str as *const c_char) as c_int);
    inv_close(loDesc);

    PG_RETURN_VOID!()
}

// Local helper: render a NUL-terminated C string for "%s"-style messages.
// TODO(pg-port): replace with the project's canonical cstr helper if one lands.
unsafe fn cstr(p: *const c_char) -> String {
    if p.is_null() {
        return String::new();
    }
    core::ffi::CStr::from_ptr(p).to_string_lossy().into_owned()
}
