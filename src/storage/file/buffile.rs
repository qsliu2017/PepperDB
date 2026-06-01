//! src/backend/storage/file/buffile.c
//!
//! Management of large buffered temporary files.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! NOTES:
//!
//! BufFiles provide a very incomplete emulation of stdio atop virtual Files
//! (as managed by fd.c).  Currently, we only support the buffered-I/O
//! aspect of stdio: a read or write of the low-level File occurs only
//! when the buffer is filled or emptied.  This is an even bigger win
//! for virtual Files than for ordinary kernel files, since reducing the
//! frequency with which a virtual File is touched reduces "thrashing"
//! of opening/closing file descriptors.
//!
//! Note that BufFile structs are allocated with palloc(), and therefore
//! will go away automatically at query/transaction end.  Since the underlying
//! virtual Files are made with OpenTemporaryFile, all resources for
//! the file are certain to be cleaned up even if processing is aborted
//! by ereport(ERROR).  The data structures required are made in the
//! palloc context that was current when the BufFile was created, and
//! any external resources such as temp files are owned by the ResourceOwner
//! that was current at that time.
//!
//! BufFile also supports temporary files that exceed the OS file size limit
//! (by opening multiple fd.c temporary files).  This is an essential feature
//! for sorts and hashjoins on large amounts of data.
//!
//! BufFile supports temporary files that can be shared with other backends, as
//! infrastructure for parallel execution.  Such files need to be created as a
//! member of a SharedFileSet that all participants are attached to.
//!
//! BufFile also supports temporary files that can be used by the single backend
//! when the corresponding files need to be survived across the transaction and
//! need to be opened and closed multiple times.  Such files need to be created
//! as a member of a FileSet.

use crate::prelude::*;

use crate::c::int64;
use crate::miscadmin::CHECK_FOR_INTERRUPTS;
use crate::pg_config::BLCKSZ;
use crate::pg_config_manual::MAXPGPATH;
use crate::storage::file::fileset::{
    File, FileSet, FileSetCreate, FileSetDelete, FileSetOpen,
};

extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn memcpy(dest: *mut c_void, src: *const c_void, n: usize) -> *mut c_void;
}

/*
 * We break BufFiles into gigabyte-sized segments, regardless of RELSEG_SIZE.
 * The reason is that we'd like large BufFiles to be spread across multiple
 * tablespaces when available.
 */
const MAX_PHYSICAL_FILESIZE: off_t = 0x40000000;
const BUFFILE_SEG_SIZE: i64 = (MAX_PHYSICAL_FILESIZE as i64) / (BLCKSZ as i64);

/* off_t emulation (matches C off_t, 64-bit) */
#[allow(non_camel_case_types)]
type off_t = i64;

/* ResourceOwner is an opaque pointer type here. */
#[allow(non_camel_case_types)]
type ResourceOwner = *mut c_void;

/* PGAlignedBlock: a BLCKSZ-sized aligned buffer holding raw data. */
#[repr(C, align(8))]
pub struct PGAlignedBlock {
    pub data: [c_char; BLCKSZ],
}

/*
 * This data structure represents a buffered file that consists of one or
 * more physical files (each accessed through a virtual file descriptor
 * managed by fd.c).
 */
pub struct BufFile {
    pub numFiles: c_int, /* number of physical files in set */
    /* all files except the last have length exactly MAX_PHYSICAL_FILESIZE */
    pub files: *mut File, /* palloc'd array with numFiles entries */

    pub isInterXact: bool, /* keep open over transactions? */
    pub dirty: bool,       /* does buffer need to be written? */
    pub readOnly: bool,    /* has the file been set to read only? */

    pub fileset: *mut FileSet,  /* space for fileset based segment files */
    pub name: *const c_char,    /* name of fileset based BufFile */

    /*
     * resowner is the ResourceOwner to use for underlying temp files.  (We
     * don't need to remember the memory context we're using explicitly,
     * because after creation we only repalloc our arrays larger.)
     */
    pub resowner: ResourceOwner,

    /*
     * "current pos" is position of start of buffer within the logical file.
     * Position as seen by user of BufFile is (curFile, curOffset + pos).
     */
    pub curFile: c_int,    /* file index (0..n) part of current pos */
    pub curOffset: off_t,  /* offset part of current pos */
    pub pos: c_int,        /* next read/write position in buffer */
    pub nbytes: c_int,     /* total # of valid bytes in buffer */

    /*
     * XXX Should ideally use PGIOAlignedBlock, but might need a way to avoid
     * wasting per-file alignment padding when some users create many files.
     */
    pub buffer: PGAlignedBlock,
}

/* ---- local stubs for not-yet-ported dependencies ---- */

static mut CurrentResourceOwner: ResourceOwner = std::ptr::null_mut();

#[allow(non_upper_case_globals)]
static mut track_io_timing: bool = false;

const O_RDONLY: c_int = 0;
const SEEK_SET: c_int = 0;
const SEEK_CUR: c_int = 1;
const SEEK_END: c_int = 2;
const EOF: c_int = -1;

const WAIT_EVENT_BUFFILE_READ: u32 = 0;
const WAIT_EVENT_BUFFILE_WRITE: u32 = 0;
const WAIT_EVENT_BUFFILE_TRUNCATE: u32 = 0;

unsafe fn PrepareTempTablespaces() {
    unimplemented!() // TODO: commands/tablespace.c
}

unsafe fn OpenTemporaryFile(_interXact: bool) -> File {
    unimplemented!() // TODO: storage/file/fd.c
}

unsafe fn FileClose(_file: File) {
    unimplemented!() // TODO: storage/file/fd.c
}

unsafe fn FileRead(
    _file: File,
    _buffer: *mut c_char,
    _amount: usize,
    _offset: off_t,
    _wait_event_info: u32,
) -> c_int {
    unimplemented!() // TODO: storage/file/fd.c
}

unsafe fn FileWrite(
    _file: File,
    _buffer: *const c_char,
    _amount: c_int,
    _offset: off_t,
    _wait_event_info: u32,
) -> c_int {
    unimplemented!() // TODO: storage/file/fd.c
}

unsafe fn FileSize(_file: File) -> off_t {
    unimplemented!() // TODO: storage/file/fd.c
}

unsafe fn FileTruncate(_file: File, _offset: off_t, _wait_event_info: u32) -> c_int {
    unimplemented!() // TODO: storage/file/fd.c
}

unsafe fn FilePathName(_file: File) -> *mut c_char {
    unimplemented!() // TODO: storage/file/fd.c
}

/*
 * Create BufFile and perform the common initialization.
 */
unsafe fn makeBufFileCommon(nfiles: c_int) -> *mut BufFile {
    let file: *mut BufFile = palloc(std::mem::size_of::<BufFile>()) as *mut BufFile;

    (*file).numFiles = nfiles;
    (*file).isInterXact = false;
    (*file).dirty = false;
    (*file).resowner = CurrentResourceOwner;
    (*file).curFile = 0;
    (*file).curOffset = 0;
    (*file).pos = 0;
    (*file).nbytes = 0;

    file
}

/*
 * Create a BufFile given the first underlying physical file.
 * NOTE: caller must set isInterXact if appropriate.
 */
unsafe fn makeBufFile(firstfile: File) -> *mut BufFile {
    let file: *mut BufFile = makeBufFileCommon(1);

    (*file).files = palloc(std::mem::size_of::<File>()) as *mut File;
    *(*file).files.offset(0) = firstfile;
    (*file).readOnly = false;
    (*file).fileset = std::ptr::null_mut();
    (*file).name = std::ptr::null();

    file
}

/*
 * Add another component temp file.
 */
unsafe fn extendBufFile(file: *mut BufFile) {
    let pfile: File;
    let oldowner: ResourceOwner;

    /* Be sure to associate the file with the BufFile's resource owner */
    oldowner = CurrentResourceOwner;
    CurrentResourceOwner = (*file).resowner;

    if (*file).fileset.is_null() {
        pfile = OpenTemporaryFile((*file).isInterXact);
    } else {
        pfile = MakeNewFileSetSegment(file, (*file).numFiles);
    }

    assert!(pfile >= 0);

    CurrentResourceOwner = oldowner;

    (*file).files = repalloc(
        (*file).files as *mut c_void,
        ((*file).numFiles as usize + 1) * std::mem::size_of::<File>(),
    ) as *mut File;
    *(*file).files.offset((*file).numFiles as isize) = pfile;
    (*file).numFiles += 1;
}

/*
 * Create a BufFile for a new temporary file (which will expand to become
 * multiple temporary files if more than MAX_PHYSICAL_FILESIZE bytes are
 * written to it).
 *
 * If interXact is true, the temp file will not be automatically deleted
 * at end of transaction.
 *
 * Note: if interXact is true, the caller had better be calling us in a
 * memory context, and with a resource owner, that will survive across
 * transaction boundaries.
 */
pub unsafe fn BufFileCreateTemp(interXact: bool) -> *mut BufFile {
    let file: *mut BufFile;
    let pfile: File;

    /*
     * Ensure that temp tablespaces are set up for OpenTemporaryFile to use.
     * Possibly the caller will have done this already, but it seems useful to
     * double-check here.  Failure to do this at all would result in the temp
     * files always getting placed in the default tablespace, which is a
     * pretty hard-to-detect bug.  Callers may prefer to do it earlier if they
     * want to be sure that any required catalog access is done in some other
     * resource context.
     */
    PrepareTempTablespaces();

    pfile = OpenTemporaryFile(interXact);
    assert!(pfile >= 0);

    file = makeBufFile(pfile);
    (*file).isInterXact = interXact;

    file
}

/*
 * Build the name for a given segment of a given BufFile.
 */
unsafe fn FileSetSegmentName(name: *mut c_char, buffile_name: *const c_char, segment: c_int) {
    snprintf(name, MAXPGPATH, c"%s.%d".as_ptr(), buffile_name, segment);
}

/*
 * Create a new segment file backing a fileset based BufFile.
 */
unsafe fn MakeNewFileSetSegment(buffile: *mut BufFile, segment: c_int) -> File {
    let mut name: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let file: File;

    /*
     * It is possible that there are files left over from before a crash
     * restart with the same name.  In order for BufFileOpenFileSet() not to
     * get confused about how many segments there are, we'll unlink the next
     * segment number if it already exists.
     */
    FileSetSegmentName(name.as_mut_ptr(), (*buffile).name, segment + 1);
    FileSetDelete((*buffile).fileset, name.as_ptr(), true);

    /* Create the new segment. */
    FileSetSegmentName(name.as_mut_ptr(), (*buffile).name, segment);
    file = FileSetCreate((*buffile).fileset, name.as_ptr());

    /* FileSetCreate would've errored out */
    assert!(file > 0);

    file
}

/*
 * Create a BufFile that can be discovered and opened read-only by other
 * backends that are attached to the same SharedFileSet using the same name.
 *
 * The naming scheme for fileset based BufFiles is left up to the calling code.
 * The name will appear as part of one or more filenames on disk, and might
 * provide clues to administrators about which subsystem is generating
 * temporary file data.  Since each SharedFileSet object is backed by one or
 * more uniquely named temporary directory, names don't conflict with
 * unrelated SharedFileSet objects.
 */
pub unsafe fn BufFileCreateFileSet(fileset: *mut FileSet, name: *const c_char) -> *mut BufFile {
    let file: *mut BufFile;

    file = makeBufFileCommon(1);
    (*file).fileset = fileset;
    (*file).name = pstrdup(name);
    (*file).files = palloc(std::mem::size_of::<File>()) as *mut File;
    *(*file).files.offset(0) = MakeNewFileSetSegment(file, 0);
    (*file).readOnly = false;

    file
}

/*
 * Open a file that was previously created in another backend (or this one)
 * with BufFileCreateFileSet in the same FileSet using the same name.
 * The backend that created the file must have called BufFileClose() or
 * BufFileExportFileSet() to make sure that it is ready to be opened by other
 * backends and render it read-only.  If missing_ok is true, which indicates
 * that missing files can be safely ignored, then return NULL if the BufFile
 * with the given name is not found, otherwise, throw an error.
 */
pub unsafe fn BufFileOpenFileSet(
    fileset: *mut FileSet,
    name: *const c_char,
    mode: c_int,
    missing_ok: bool,
) -> *mut BufFile {
    let file: *mut BufFile;
    let mut segment_name: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut capacity: Size = 16;
    let mut files: *mut File;
    let mut nfiles: c_int = 0;

    files = palloc(std::mem::size_of::<File>() * capacity) as *mut File;

    /*
     * We don't know how many segments there are, so we'll probe the
     * filesystem to find out.
     */
    loop {
        /* See if we need to expand our file segment array. */
        if (nfiles as usize) + 1 > capacity {
            capacity *= 2;
            files = repalloc(
                files as *mut c_void,
                std::mem::size_of::<File>() * capacity,
            ) as *mut File;
        }
        /* Try to load a segment. */
        FileSetSegmentName(segment_name.as_mut_ptr(), name, nfiles);
        *files.offset(nfiles as isize) = FileSetOpen(fileset, segment_name.as_ptr(), mode);
        if *files.offset(nfiles as isize) <= 0 {
            break;
        }
        nfiles += 1;

        CHECK_FOR_INTERRUPTS();
    }

    /*
     * If we didn't find any files at all, then no BufFile exists with this
     * name.
     */
    if nfiles == 0 {
        /* free the memory */
        pfree(files as *mut c_void);

        if missing_ok {
            return std::ptr::null_mut();
        }

        elog!(
            ERROR,
            "could not open temporary file from BufFile: segment_name and name"
        );
        unreachable!();
    }

    file = makeBufFileCommon(nfiles);
    (*file).files = files;
    (*file).readOnly = mode == O_RDONLY;
    (*file).fileset = fileset;
    (*file).name = pstrdup(name);

    file
}

/*
 * Delete a BufFile that was created by BufFileCreateFileSet in the given
 * FileSet using the given name.
 *
 * It is not necessary to delete files explicitly with this function.  It is
 * provided only as a way to delete files proactively, rather than waiting for
 * the FileSet to be cleaned up.
 *
 * Only one backend should attempt to delete a given name, and should know
 * that it exists and has been exported or closed otherwise missing_ok should
 * be passed true.
 */
pub unsafe fn BufFileDeleteFileSet(fileset: *mut FileSet, name: *const c_char, missing_ok: bool) {
    let mut segment_name: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut segment: c_int = 0;
    let mut found: bool = false;

    /*
     * We don't know how many segments the file has.  We'll keep deleting
     * until we run out.  If we don't manage to find even an initial segment,
     * raise an error.
     */
    loop {
        FileSetSegmentName(segment_name.as_mut_ptr(), name, segment);
        if !FileSetDelete(fileset, segment_name.as_ptr(), true) {
            break;
        }
        found = true;
        segment += 1;

        CHECK_FOR_INTERRUPTS();
    }

    if !found && !missing_ok {
        elog!(ERROR, "could not delete unknown BufFile \"{}\"", "name");
    }
}

/*
 * BufFileExportFileSet --- flush and make read-only, in preparation for sharing.
 */
pub unsafe fn BufFileExportFileSet(file: *mut BufFile) {
    /* Must be a file belonging to a FileSet. */
    assert!(!(*file).fileset.is_null());

    /* It's probably a bug if someone calls this twice. */
    assert!(!(*file).readOnly);

    BufFileFlush(file);
    (*file).readOnly = true;
}

/*
 * Close a BufFile
 *
 * Like fclose(), this also implicitly FileCloses the underlying File.
 */
pub unsafe fn BufFileClose(file: *mut BufFile) {
    let mut i: c_int;

    /* flush any unwritten data */
    BufFileFlush(file);
    /* close and delete the underlying file(s) */
    i = 0;
    while i < (*file).numFiles {
        FileClose(*(*file).files.offset(i as isize));
        i += 1;
    }
    /* release the buffer space */
    pfree((*file).files as *mut c_void);
    pfree(file as *mut c_void);
}

/*
 * BufFileLoadBuffer
 *
 * Load some data into buffer, if possible, starting from curOffset.
 * At call, must have dirty = false, pos and nbytes = 0.
 * On exit, nbytes is number of bytes loaded.
 */
unsafe fn BufFileLoadBuffer(file: *mut BufFile) {
    let thisfile: File;

    /*
     * Advance to next component file if necessary and possible.
     */
    if (*file).curOffset >= MAX_PHYSICAL_FILESIZE && (*file).curFile + 1 < (*file).numFiles {
        (*file).curFile += 1;
        (*file).curOffset = 0;
    }

    thisfile = *(*file).files.offset((*file).curFile as isize);

    /* track_io_timing handling (INSTR_TIME) omitted; faithful structure */

    /*
     * Read whatever we can get, up to a full bufferload.
     */
    (*file).nbytes = FileRead(
        thisfile,
        (*file).buffer.data.as_mut_ptr(),
        std::mem::size_of_val(&(*file).buffer.data),
        (*file).curOffset,
        WAIT_EVENT_BUFFILE_READ,
    );
    if (*file).nbytes < 0 {
        (*file).nbytes = 0;
        elog!(ERROR, "could not read file \"{}\"", "FilePathName(thisfile)");
    }

    if track_io_timing {
        /* INSTR_TIME_ACCUM_DIFF(pgBufferUsage.temp_blk_read_time, ...) */
    }

    /* we choose not to advance curOffset here */

    if (*file).nbytes > 0 {
        /* pgBufferUsage.temp_blks_read++; */
    }
}

/*
 * BufFileDumpBuffer
 *
 * Dump buffer contents starting at curOffset.
 * At call, should have dirty = true, nbytes > 0.
 * On exit, dirty is cleared if successful write, and curOffset is advanced.
 */
unsafe fn BufFileDumpBuffer(file: *mut BufFile) {
    let mut wpos: c_int = 0;
    let mut bytestowrite: c_int;
    let mut thisfile: File;

    /*
     * Unlike BufFileLoadBuffer, we must dump the whole buffer even if it
     * crosses a component-file boundary; so we need a loop.
     */
    while wpos < (*file).nbytes {
        let availbytes: off_t;

        /*
         * Advance to next component file if necessary and possible.
         */
        if (*file).curOffset >= MAX_PHYSICAL_FILESIZE {
            while (*file).curFile + 1 >= (*file).numFiles {
                extendBufFile(file);
            }
            (*file).curFile += 1;
            (*file).curOffset = 0;
        }

        /*
         * Determine how much we need to write into this file.
         */
        bytestowrite = (*file).nbytes - wpos;
        availbytes = MAX_PHYSICAL_FILESIZE - (*file).curOffset;

        if (bytestowrite as off_t) > availbytes {
            bytestowrite = availbytes as c_int;
        }

        thisfile = *(*file).files.offset((*file).curFile as isize);

        /* track_io_timing INSTR_TIME omitted */

        bytestowrite = FileWrite(
            thisfile,
            (*file).buffer.data.as_ptr().offset(wpos as isize),
            bytestowrite,
            (*file).curOffset,
            WAIT_EVENT_BUFFILE_WRITE,
        );
        if bytestowrite <= 0 {
            elog!(
                ERROR,
                "could not write to file \"{}\"",
                "FilePathName(thisfile)"
            );
        }

        if track_io_timing {
            /* INSTR_TIME_ACCUM_DIFF(pgBufferUsage.temp_blk_write_time, ...) */
        }

        (*file).curOffset += bytestowrite as off_t;
        wpos += bytestowrite;

        /* pgBufferUsage.temp_blks_written++; */
    }
    (*file).dirty = false;

    /*
     * At this point, curOffset has been advanced to the end of the buffer,
     * ie, its original value + nbytes.  We need to make it point to the
     * logical file position, ie, original value + pos, in case that is less
     * (as could happen due to a small backwards seek in a dirty buffer!)
     */
    (*file).curOffset -= ((*file).nbytes - (*file).pos) as off_t;
    if (*file).curOffset < 0 {
        /* handle possible segment crossing */
        (*file).curFile -= 1;
        assert!((*file).curFile >= 0);
        (*file).curOffset += MAX_PHYSICAL_FILESIZE;
    }

    /*
     * Now we can set the buffer empty without changing the logical position
     */
    (*file).pos = 0;
    (*file).nbytes = 0;
}

/*
 * BufFileRead variants
 *
 * Like fread() except we assume 1-byte element size and report I/O errors via
 * ereport().
 *
 * If 'exact' is true, then an error is also raised if the number of bytes
 * read is not exactly 'size' (no short reads).  If 'exact' and 'eofOK' are
 * true, then reading zero bytes is ok.
 */
unsafe fn BufFileReadCommon(
    file: *mut BufFile,
    mut ptr: *mut c_void,
    mut size: usize,
    exact: bool,
    eofOK: bool,
) -> usize {
    let start_size: usize = size;
    let mut nread: usize = 0;
    let mut nthistime: usize;

    BufFileFlush(file);

    while size > 0 {
        if (*file).pos >= (*file).nbytes {
            /* Try to load more data into buffer. */
            (*file).curOffset += (*file).pos as off_t;
            (*file).pos = 0;
            (*file).nbytes = 0;
            BufFileLoadBuffer(file);
            if (*file).nbytes <= 0 {
                break; /* no more data available */
            }
        }

        nthistime = ((*file).nbytes - (*file).pos) as usize;
        if nthistime > size {
            nthistime = size;
        }
        assert!(nthistime > 0);

        memcpy(
            ptr,
            (*file).buffer.data.as_ptr().offset((*file).pos as isize) as *const c_void,
            nthistime,
        );

        (*file).pos += nthistime as c_int;
        ptr = (ptr as *mut c_char).add(nthistime) as *mut c_void;
        size -= nthistime;
        nread += nthistime;
    }

    if exact && (nread != start_size && !(nread == 0 && eofOK)) {
        if !(*file).name.is_null() {
            elog!(
                ERROR,
                "could not read from file set \"{}\": read only {} of {} bytes",
                "file->name",
                nread,
                start_size
            );
        } else {
            elog!(
                ERROR,
                "could not read from temporary file: read only {} of {} bytes",
                nread,
                start_size
            );
        }
    }

    nread
}

/*
 * Legacy interface where the caller needs to check for end of file or short
 * reads.
 */
pub unsafe fn BufFileRead(file: *mut BufFile, ptr: *mut c_void, size: usize) -> usize {
    BufFileReadCommon(file, ptr, size, false, false)
}

/*
 * Require read of exactly the specified size.
 */
pub unsafe fn BufFileReadExact(file: *mut BufFile, ptr: *mut c_void, size: usize) {
    BufFileReadCommon(file, ptr, size, true, false);
}

/*
 * Require read of exactly the specified size, but optionally allow end of
 * file (in which case 0 is returned).
 */
pub unsafe fn BufFileReadMaybeEOF(
    file: *mut BufFile,
    ptr: *mut c_void,
    size: usize,
    eofOK: bool,
) -> usize {
    BufFileReadCommon(file, ptr, size, true, eofOK)
}

/*
 * BufFileWrite
 *
 * Like fwrite() except we assume 1-byte element size and report errors via
 * ereport().
 */
pub unsafe fn BufFileWrite(file: *mut BufFile, mut ptr: *const c_void, mut size: usize) {
    let mut nthistime: usize;

    assert!(!(*file).readOnly);

    while size > 0 {
        if (*file).pos >= BLCKSZ as c_int {
            /* Buffer full, dump it out */
            if (*file).dirty {
                BufFileDumpBuffer(file);
            } else {
                /* Hmm, went directly from reading to writing? */
                (*file).curOffset += (*file).pos as off_t;
                (*file).pos = 0;
                (*file).nbytes = 0;
            }
        }

        nthistime = (BLCKSZ as c_int - (*file).pos) as usize;
        if nthistime > size {
            nthistime = size;
        }
        assert!(nthistime > 0);

        memcpy(
            (*file).buffer.data.as_mut_ptr().offset((*file).pos as isize) as *mut c_void,
            ptr,
            nthistime,
        );

        (*file).dirty = true;
        (*file).pos += nthistime as c_int;
        if (*file).nbytes < (*file).pos {
            (*file).nbytes = (*file).pos;
        }
        ptr = (ptr as *const c_char).add(nthistime) as *const c_void;
        size -= nthistime;
    }
}

/*
 * BufFileFlush
 *
 * Like fflush(), except that I/O errors are reported with ereport().
 */
unsafe fn BufFileFlush(file: *mut BufFile) {
    if (*file).dirty {
        BufFileDumpBuffer(file);
    }

    assert!(!(*file).dirty);
}

/*
 * BufFileSeek
 *
 * Like fseek(), except that target position needs two values in order to
 * work when logical filesize exceeds maximum value representable by off_t.
 * We do not support relative seeks across more than that, however.
 * I/O errors are reported by ereport().
 *
 * Result is 0 if OK, EOF if not.  Logical position is not moved if an
 * impossible seek is attempted.
 */
pub unsafe fn BufFileSeek(
    file: *mut BufFile,
    fileno: c_int,
    offset: off_t,
    whence: c_int,
) -> c_int {
    let mut newFile: c_int;
    let mut newOffset: off_t;

    match whence {
        SEEK_SET => {
            if fileno < 0 {
                return EOF;
            }
            newFile = fileno;
            newOffset = offset;
        }
        SEEK_CUR => {
            /*
             * Relative seek considers only the signed offset, ignoring
             * fileno. Note that large offsets (> 1 GB) risk overflow in this
             * add, unless we have 64-bit off_t.
             */
            newFile = (*file).curFile;
            newOffset = ((*file).curOffset + (*file).pos as off_t) + offset;
        }
        SEEK_END => {
            /*
             * The file size of the last file gives us the end offset of that
             * file.
             */
            newFile = (*file).numFiles - 1;
            newOffset = FileSize(*(*file).files.offset(((*file).numFiles - 1) as isize));
            if newOffset < 0 {
                elog!(
                    ERROR,
                    "could not determine size of temporary file from BufFile"
                );
            }
        }
        _ => {
            elog!(ERROR, "invalid whence: {}", whence);
            return EOF;
        }
    }
    while newOffset < 0 {
        newFile -= 1;
        if newFile < 0 {
            return EOF;
        }
        newOffset += MAX_PHYSICAL_FILESIZE;
    }
    if newFile == (*file).curFile
        && newOffset >= (*file).curOffset
        && newOffset <= (*file).curOffset + (*file).nbytes as off_t
    {
        /*
         * Seek is to a point within existing buffer; we can just adjust
         * pos-within-buffer, without flushing buffer.  Note this is OK
         * whether reading or writing, but buffer remains dirty if we were
         * writing.
         */
        (*file).pos = (newOffset - (*file).curOffset) as c_int;
        return 0;
    }
    /* Otherwise, must reposition buffer, so flush any dirty data */
    BufFileFlush(file);

    /*
     * At this point and no sooner, check for seek past last segment. The
     * above flush could have created a new segment, so checking sooner would
     * not work (at least not with this code).
     */

    /* convert seek to "start of next seg" to "end of last seg" */
    if newFile == (*file).numFiles && newOffset == 0 {
        newFile -= 1;
        newOffset = MAX_PHYSICAL_FILESIZE;
    }
    while newOffset > MAX_PHYSICAL_FILESIZE {
        newFile += 1;
        if newFile >= (*file).numFiles {
            return EOF;
        }
        newOffset -= MAX_PHYSICAL_FILESIZE;
    }
    if newFile >= (*file).numFiles {
        return EOF;
    }
    /* Seek is OK! */
    (*file).curFile = newFile;
    (*file).curOffset = newOffset;
    (*file).pos = 0;
    (*file).nbytes = 0;
    0
}

pub unsafe fn BufFileTell(file: *mut BufFile, fileno: *mut c_int, offset: *mut off_t) {
    *fileno = (*file).curFile;
    *offset = (*file).curOffset + (*file).pos as off_t;
}

/*
 * BufFileSeekBlock --- block-oriented seek
 *
 * Performs absolute seek to the start of the n'th BLCKSZ-sized block of
 * the file.  Note that users of this interface will fail if their files
 * exceed BLCKSZ * PG_INT64_MAX bytes, but that is quite a lot; we don't
 * work with tables bigger than that, either...
 *
 * Result is 0 if OK, EOF if not.  Logical position is not moved if an
 * impossible seek is attempted.
 */
pub unsafe fn BufFileSeekBlock(file: *mut BufFile, blknum: int64) -> c_int {
    BufFileSeek(
        file,
        (blknum / BUFFILE_SEG_SIZE) as c_int,
        ((blknum % BUFFILE_SEG_SIZE) as off_t) * (BLCKSZ as off_t),
        SEEK_SET,
    )
}

/*
 * Returns the amount of data in the given BufFile, in bytes.
 *
 * Returned value includes the size of any holes left behind by BufFileAppend.
 * ereport()s on failure.
 */
pub unsafe fn BufFileSize(file: *mut BufFile) -> int64 {
    let lastFileSize: int64;

    /* Get the size of the last physical file. */
    lastFileSize = FileSize(*(*file).files.offset(((*file).numFiles - 1) as isize));
    if lastFileSize < 0 {
        elog!(
            ERROR,
            "could not determine size of temporary file from BufFile"
        );
    }

    (((*file).numFiles - 1) as int64) * (MAX_PHYSICAL_FILESIZE as int64) + lastFileSize
}

/*
 * Append the contents of the source file to the end of the target file.
 *
 * Note that operation subsumes ownership of underlying resources from
 * "source".  Caller should never call BufFileClose against source having
 * called here first.  Resource owners for source and target must match,
 * too.
 *
 * This operation works by manipulating lists of segment files, so the
 * file content is always appended at a MAX_PHYSICAL_FILESIZE-aligned
 * boundary, typically creating empty holes before the boundary.  These
 * areas do not contain any interesting data, and cannot be read from by
 * caller.
 *
 * Returns the block number within target where the contents of source
 * begins.  Caller should apply this as an offset when working off block
 * positions that are in terms of the original BufFile space.
 */
pub unsafe fn BufFileAppend(target: *mut BufFile, source: *mut BufFile) -> int64 {
    let startBlock: int64 = (*target).numFiles as int64 * BUFFILE_SEG_SIZE;
    let newNumFiles: c_int = (*target).numFiles + (*source).numFiles;
    let mut i: c_int;

    assert!((*source).readOnly);
    assert!(!(*source).dirty);

    if (*target).resowner != (*source).resowner {
        elog!(
            ERROR,
            "could not append BufFile with non-matching resource owner"
        );
    }

    (*target).files = repalloc(
        (*target).files as *mut c_void,
        std::mem::size_of::<File>() * newNumFiles as usize,
    ) as *mut File;
    i = (*target).numFiles;
    while i < newNumFiles {
        *(*target).files.offset(i as isize) =
            *(*source).files.offset((i - (*target).numFiles) as isize);
        i += 1;
    }
    (*target).numFiles = newNumFiles;

    startBlock
}

/*
 * Truncate a BufFile created by BufFileCreateFileSet up to the given fileno
 * and the offset.
 */
pub unsafe fn BufFileTruncateFileSet(file: *mut BufFile, fileno: c_int, offset: off_t) {
    let mut numFiles: c_int = (*file).numFiles;
    let mut newFile: c_int = fileno;
    let mut newOffset: off_t = (*file).curOffset;
    let mut segment_name: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut i: c_int;

    /*
     * Loop over all the files up to the given fileno and remove the files
     * that are greater than the fileno and truncate the given file up to the
     * offset. Note that we also remove the given fileno if the offset is 0
     * provided it is not the first file in which we truncate it.
     */
    i = (*file).numFiles - 1;
    while i >= fileno {
        if (i != fileno || offset == 0) && i != 0 {
            FileSetSegmentName(segment_name.as_mut_ptr(), (*file).name, i);
            FileClose(*(*file).files.offset(i as isize));
            if !FileSetDelete((*file).fileset, segment_name.as_ptr(), true) {
                elog!(ERROR, "could not delete fileset \"{}\"", "segment_name");
            }
            numFiles -= 1;
            newOffset = MAX_PHYSICAL_FILESIZE;

            /*
             * This is required to indicate that we have deleted the given
             * fileno.
             */
            if i == fileno {
                newFile -= 1;
            }
        } else {
            if FileTruncate(
                *(*file).files.offset(i as isize),
                offset,
                WAIT_EVENT_BUFFILE_TRUNCATE,
            ) < 0
            {
                elog!(
                    ERROR,
                    "could not truncate file \"{}\"",
                    "FilePathName(file->files[i])"
                );
            }
            newOffset = offset;
        }
        i -= 1;
    }

    (*file).numFiles = numFiles;

    /*
     * If the truncate point is within existing buffer then we can just adjust
     * pos within buffer.
     */
    if newFile == (*file).curFile
        && newOffset >= (*file).curOffset
        && newOffset <= (*file).curOffset + (*file).nbytes as off_t
    {
        /* No need to reset the current pos if the new pos is greater. */
        if newOffset <= (*file).curOffset + (*file).pos as off_t {
            (*file).pos = (newOffset - (*file).curOffset) as c_int;
        }

        /* Adjust the nbytes for the current buffer. */
        (*file).nbytes = (newOffset - (*file).curOffset) as c_int;
    } else if newFile == (*file).curFile && newOffset < (*file).curOffset {
        /*
         * The truncate point is within the existing file but prior to the
         * current position, so we can forget the current buffer and reset the
         * current position.
         */
        (*file).curOffset = newOffset;
        (*file).pos = 0;
        (*file).nbytes = 0;
    } else if newFile < (*file).curFile {
        /*
         * The truncate point is prior to the current file, so need to reset
         * the current position accordingly.
         */
        (*file).curFile = newFile;
        (*file).curOffset = newOffset;
        (*file).pos = 0;
        (*file).nbytes = 0;
    }
    /* Nothing to do, if the truncate point is beyond current file. */
}
