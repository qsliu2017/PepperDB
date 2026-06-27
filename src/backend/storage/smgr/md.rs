//! Translated from PostgreSQL src/backend/storage/smgr/md.c
//!
//! The magnetic-disk storage manager: the interface from the smgr API to the
//! filesystem. A relation is broken into 1GB "segment" files (RELSEG_SIZE
//! blocks); md.c maps (fork, block) -> (segment file, byte offset) and does the
//! actual read/write/extend/fsync.
//!
//! Port notes vs md.c:
//!  * The MdfdVec array (md_seg_fds) is per-fork state owned by the
//!    SMgrRelation; `_fdvec_resize`/MemoryContext bookkeeping becomes a `Vec`.
//!  * All file access goes through the VFD pool ([`File`]/[`FdManager`]); the
//!    `File` handle is a cheap Clone, so we clone the target segment's handle
//!    out of the MdfdVec, drop any borrow, then `.await` the I/O -- no borrow or
//!    lock is held across a suspension point.
//!  * The PG18 AIO read path (mdstartreadv / the pgaio handle machinery and the
//!    md_readv_complete/report callbacks) is DELETED: mdreadv collapses to a
//!    direct async `File::read_v`. The EOF responsibility that the step-05 read
//!    leaf pushed up (its read is all-or-`UnexpectedEof`) is handled here: md
//!    size-checks against the segment and zero-fills past EOF in recovery /
//!    zero_damaged_pages mode, else errors like md.c's mdreadv.
//!  * mdprefetch / mdwriteback (posix_fadvise / sync_file_range) -> no-ops.
//!  * Paths from `relpath(...)` are relative to PGDATA; we join them onto
//!    `DataDir` (PG runs chdir'd into the data dir, so its relative paths work
//!    directly; we have no such cwd, so we resolve here).
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "TODO(error-migration): pre-existing backlog; new code uses OrElog/?/crate::assert!"
)]

use std::io::{IoSlice, IoSliceMut};
use std::path::PathBuf;
use std::sync::Arc;

use crate::backend::storage::file::fd::File;
use crate::backend::storage::sync::sync::RegisterSyncRequest;
use crate::common::relpath::{relpath, ForkNumber, MAX_FORKNUM};
use crate::pg_config::{BLCKSZ, RELSEG_SIZE};
use crate::postgres_ext::Oid;
use crate::shared_state::SharedState;
use crate::storage::block::{BlockNumber, INVALID_BLOCK_NUMBER};
use crate::storage::bufpage::Page;
use crate::storage::io_backend::{self, OpenFlags};
use crate::storage::relfilelocator::RelFileLocatorBackend;
use crate::storage::smgr::SmgrRelation;
use crate::storage::sync::{FileTag, SyncRequestHandler, SyncRequestType};

const BLCKSZ_U64: u64 = BLCKSZ as u64;
const RELSEG_SIZE_BN: BlockNumber = RELSEG_SIZE;

/// One open segment file plus its segment number (md.c `_MdfdVec`). The `File`
/// is a cheap Clone handle into the VFD pool.
pub struct MdfdVec {
    pub mdfd_file: File,
    pub mdfd_segno: BlockNumber,
}

/// Behavior for `_mdfd_getseg` / mdopenfork (md.c EXTENSION_* flags).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Extension {
    /// ereport if segment not present.
    Fail,
    /// return None if segment not present.
    ReturnNull,
    /// create new segments as needed (mdextend).
    Create,
    /// don't open a segment if not already open (mdwriteback).
    DontOpen,
}

/// Build a FileTag for an md segment (md.c INIT_MD_FILETAG).
fn init_md_filetag(rlocator: RelFileLocatorBackend, forknum: ForkNumber, segno: BlockNumber) -> FileTag {
    FileTag {
        handler: SyncRequestHandler::Md as i16,
        forknum: forknum as i16,
        rlocator: rlocator.locator,
        segno: u64::from(segno),
    }
}

/// Resolve a relpath (relative to PGDATA) to an absolute filesystem path using
/// the configured DataDir. If DataDir is unset (tests), use the path as-is.
fn resolve_path(shared: &Arc<SharedState>, rel: &str) -> PathBuf {
    shared
        .config()
        .data_dir()
        .map_or_else(|| PathBuf::from(rel), |dir| PathBuf::from(dir).join(rel))
}

/// Filesystem path for the given segment (md.c `_mdfd_segpath`).
fn mdfd_segpath(
    shared: &Arc<SharedState>,
    reln: &SmgrRelation,
    forknum: ForkNumber,
    segno: BlockNumber,
) -> PathBuf {
    let base = relpath(reln.rlocator, forknum);
    let rel = if segno > 0 { format!("{}.{}", base.as_str(), segno) } else { base.str };
    resolve_path(shared, &rel)
}

#[inline]
fn open_flags() -> OpenFlags {
    OpenFlags::read_write()
}

/// Number of blocks in a single segment file (md.c `_mdnblocks`). Ignores a
/// partial block at EOF.
async fn seg_nblocks(file: &File) -> BlockNumber {
    let len = file.size().await.expect("could not stat segment file");
    (len / BLCKSZ_U64) as BlockNumber
}

// ---------------------------------------------------------------------------
// md smgr backend entry points (the f_smgr methods for SMGR_MD).
// ---------------------------------------------------------------------------

/// mdopen() -- mark all forks not open.
pub fn mdopen(reln: &mut SmgrRelation) {
    for fork in 0..reln.md_seg_fds.len() {
        reln.md_seg_fds[fork].clear();
    }
}

/// mdclose() -- close the specified fork's open segments.
pub fn mdclose(reln: &mut SmgrRelation, forknum: ForkNumber) {
    reln.md_seg_fds[forknum as usize].clear();
}

/// mdcreate() -- create a new relation fork on disk.
pub async fn mdcreate(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    is_redo: bool,
) {
    let fork = forknum as usize;
    if is_redo && !reln.md_seg_fds[fork].is_empty() {
        return; // created and opened already
    }
    debug_assert!(reln.md_seg_fds[fork].is_empty());

    // TODO(tablespace): TablespaceCreateDbspace -- ensure base/<db> exists. For
    // now create the parent directory directly.
    let path = mdfd_segpath(shared, reln, forknum, 0);
    if let Some(parent) = path.parent() {
        let _ = io_backend::mkdir_all(parent).await;
    }

    let mut create_flags = open_flags();
    create_flags.create = true;
    create_flags.create_new = true;

    let file = match shared.fd().open(&path, create_flags).await {
        Ok(f) => f,
        Err(_) if is_redo => {
            // It may already exist during redo: open without O_EXCL.
            shared
                .fd()
                .open(&path, open_flags())
                .await
                .expect("could not create relation file")
        }
        Err(e) => panic!("could not create file {}: {e}", path.display()),
    };

    reln.md_seg_fds[fork] = vec![MdfdVec { mdfd_file: file, mdfd_segno: 0 }];

    if !reln.is_temp() {
        let seg_segno = reln.md_seg_fds[fork][0].mdfd_segno;
        register_dirty_segment(shared, reln, forknum, seg_segno).await;
    }
}

/// mdexists() -- does the physical file for this fork exist?
pub async fn mdexists(shared: &Arc<SharedState>, reln: &mut SmgrRelation, forknum: ForkNumber) -> bool {
    // Close first so we notice an unlink since we last opened (md.c does this
    // outside recovery; we always do it -- correct, just less optimized).
    mdclose(reln, forknum);
    mdopenfork(shared, reln, forknum, Extension::ReturnNull).await.is_some()
}

/// mdunlink() -- unlink one or all forks of a relation.
pub async fn mdunlink(
    shared: &Arc<SharedState>,
    rlocator: RelFileLocatorBackend,
    forknum: ForkNumber,
    is_redo: bool,
) {
    if forknum == ForkNumber::InvalidForkNumber {
        for f in 0..=(MAX_FORKNUM as i32) {
            let fk = fork_from_i32(f);
            mdunlinkfork(shared, rlocator, fk, is_redo).await;
        }
    } else {
        mdunlinkfork(shared, rlocator, forknum, is_redo).await;
    }
}

fn fork_from_i32(f: i32) -> ForkNumber {
    match f {
        0 => ForkNumber::MAIN_FORKNUM,
        1 => ForkNumber::FSM_FORKNUM,
        2 => ForkNumber::VISIBILITYMAP_FORKNUM,
        3 => ForkNumber::INIT_FORKNUM,
        _ => ForkNumber::InvalidForkNumber,
    }
}

async fn truncate_to_zero(shared: &Arc<SharedState>, path: &PathBuf) -> bool {
    // Returns true if the file existed (truncate succeeded), false on ENOENT.
    match shared.fd().open(path, open_flags()).await {
        Ok(f) => {
            let _ = f.truncate(0).await;
            true
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => true,
    }
}

async fn mdunlinkfork(
    shared: &Arc<SharedState>,
    rlocator: RelFileLocatorBackend,
    forknum: ForkNumber,
    is_redo: bool,
) {
    let base = relpath(rlocator, forknum);
    let path0 = resolve_path(shared, base.as_str());

    if is_redo || forknum != ForkNumber::MAIN_FORKNUM || rlocator.is_temp() {
        if !rlocator.is_temp() {
            // Prevent other backends' fds from pinning the disk space.
            truncate_to_zero(shared, &path0).await;
            // Forget any pending sync requests for the first segment.
            register_forget_request(shared, rlocator, forknum, 0);
        }
        let _ = io_backend::unlink(&path0).await;
    } else {
        // Regular main fork, not redo: truncate now, defer the unlink to after
        // the next checkpoint (protects relfilenumber reuse, see md.c).
        truncate_to_zero(shared, &path0).await;
        register_unlink_segment(shared, rlocator, forknum, 0);
    }

    // Delete any additional segments (truncate then unlink), stopping at ENOENT.
    for segno in 1.. {
        let segpath = resolve_path(shared, &format!("{}.{}", base.as_str(), segno));
        if !rlocator.is_temp() {
            if !truncate_to_zero(shared, &segpath).await {
                break; // ENOENT: no more segments
            }
            register_forget_request(shared, rlocator, forknum, segno);
        }
        if io_backend::unlink(&segpath).await.is_err() {
            break; // ENOENT expected after the last segment
        }
    }
}

/// mdextend() -- write a block at or beyond EOF, growing the fork.
pub async fn mdextend(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffer: &Page,
    skip_fsync: bool,
) {
    assert!(blocknum != INVALID_BLOCK_NUMBER, "cannot extend file beyond {INVALID_BLOCK_NUMBER} blocks");
    let (file, segno) = getseg(shared, reln, forknum, blocknum, skip_fsync, Extension::Create)
        .await
        .expect("mdextend: getseg with EXTENSION_CREATE must succeed");
    let seekpos = BLCKSZ_U64 * u64::from(blocknum % RELSEG_SIZE_BN);
    let n = file
        .write(buffer.as_bytes(), seekpos)
        .await
        .expect("could not extend relation file");
    assert_eq!(n, BLCKSZ as usize, "short write extending relation");

    if !skip_fsync && !reln.is_temp() {
        register_dirty_segment(shared, reln, forknum, segno).await;
    }
}

/// mdzeroextend() -- grow the fork by `nblocks` zero-filled blocks at `blocknum`.
pub async fn mdzeroextend(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    nblocks: i32,
    skip_fsync: bool,
) {
    assert!(nblocks > 0);
    assert!(u64::from(blocknum) + (nblocks as u64) < u64::from(INVALID_BLOCK_NUMBER), "cannot extend file beyond {INVALID_BLOCK_NUMBER} blocks");

    let mut curblock = blocknum;
    let mut remblocks = nblocks;
    while remblocks > 0 {
        let segstart = curblock % RELSEG_SIZE_BN;
        let seekpos = BLCKSZ_U64 * u64::from(segstart);
        let numblocks = if segstart + remblocks as BlockNumber > RELSEG_SIZE_BN {
            (RELSEG_SIZE_BN - segstart) as i32
        } else {
            remblocks
        };

        let (file, segno) = getseg(shared, reln, forknum, curblock, skip_fsync, Extension::Create)
            .await
            .expect("mdzeroextend: getseg with EXTENSION_CREATE must succeed");

        // FileZero/FileFallocate collapse to extend (zero-fill) via the leaf.
        file.extend(seekpos, BLCKSZ_U64 * numblocks as u64)
            .await
            .expect("could not extend relation file");

        if !skip_fsync && !reln.is_temp() {
            register_dirty_segment(shared, reln, forknum, segno).await;
        }

        remblocks -= numblocks;
        curblock += numblocks as BlockNumber;
    }
}

/// mdprefetch() -- no-op (posix_fadvise deleted by redesign).
pub fn mdprefetch(
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _nblocks: i32,
) -> bool {
    // TODO(prefetch): posix_fadvise(WILLNEED) is a portability shim; no-op.
    true
}

/// mdmaxcombine() -- blocks combinable into one IO starting at blocknum.
pub fn mdmaxcombine(_reln: &mut SmgrRelation, _forknum: ForkNumber, blocknum: BlockNumber) -> u32 {
    let segoff = blocknum % RELSEG_SIZE_BN;
    RELSEG_SIZE_BN - segoff
}

/// mdreadv() -- read `buffers.len()` consecutive blocks starting at `blocknum`.
///
/// Direct async read (AIO collapsed). md.c forbids a read crossing a segment
/// boundary, so all blocks land in one segment. The step-05 leaf read is
/// all-or-`UnexpectedEof`; we therefore size-check against the segment and, past
/// EOF, zero-fill in recovery / zero_damaged_pages mode, else error like md.c.
pub async fn mdreadv(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: &mut [&mut Page],
) {
    let nblocks = buffers.len() as BlockNumber;
    if nblocks == 0 {
        return;
    }
    let segoff = blocknum % RELSEG_SIZE_BN;
    assert!(segoff + nblocks <= RELSEG_SIZE_BN, "read crosses segment boundary");

    let (file, _segno) = getseg(shared, reln, forknum, blocknum, false, Extension::Fail)
        .await
        .expect("mdreadv: getseg(EXTENSION_FAIL) must succeed");
    let seekpos = BLCKSZ_U64 * u64::from(segoff);

    // Determine how many blocks actually exist in this segment from seekpos.
    let segblocks = seg_nblocks(&file).await;
    let avail = segblocks.saturating_sub(segoff);

    let read_n = avail.min(nblocks);
    if read_n > 0 {
        let mut iov: Vec<IoSliceMut> =
            buffers[..read_n as usize].iter_mut().map(|p| IoSliceMut::new(p.as_mut_bytes())).collect();
        file.read_v(&mut iov, seekpos).await.expect("could not read blocks");
    }

    if read_n < nblocks {
        // Past EOF. md.c: error unless zero_damaged_pages or InRecovery, in
        // which case zero-fill. We don't have those GUCs wired yet; zero-fill is
        // the safe behavior for the buffer manager's relation-extension reads.
        // TODO(zero_damaged_pages/InRecovery): error in the strict (non-recovery)
        // case once those flags exist.
        for p in &mut buffers[read_n as usize..] {
            p.as_mut_bytes().fill(0);
        }
    }
}

/// mdwritev() -- write `buffers.len()` blocks (all before EOF) at `blocknum`.
pub async fn mdwritev(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    blocknum: BlockNumber,
    buffers: &[&Page],
    skip_fsync: bool,
) {
    let nblocks = buffers.len() as BlockNumber;
    if nblocks == 0 {
        return;
    }
    let segoff = blocknum % RELSEG_SIZE_BN;
    assert!(segoff + nblocks <= RELSEG_SIZE_BN, "write crosses segment boundary");

    let (file, segno) =
        getseg(shared, reln, forknum, blocknum, skip_fsync, Extension::Fail)
            .await
            .expect("mdwritev: getseg(EXTENSION_FAIL) must succeed");
    let seekpos = BLCKSZ_U64 * u64::from(segoff);

    let iov: Vec<IoSlice> = buffers.iter().map(|p| IoSlice::new(p.as_bytes())).collect();
    let n = file.write_v(&iov, seekpos).await.expect("could not write blocks");
    assert_eq!(n, (BLCKSZ as usize) * nblocks as usize, "short write");

    if !skip_fsync && !reln.is_temp() {
        register_dirty_segment(shared, reln, forknum, segno).await;
    }
}

/// mdwriteback() -- no-op (sync_file_range deleted by redesign).
pub fn mdwriteback(
    _reln: &mut SmgrRelation,
    _forknum: ForkNumber,
    _blocknum: BlockNumber,
    _nblocks: BlockNumber,
) {
    // TODO(writeback): kernel writeback hint; no-op.
}

/// mdnblocks() -- total blocks in the fork (sum of all active segments). Opens
/// every active segment as a side effect, matching md.c.
pub async fn mdnblocks(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
) -> BlockNumber {
    if mdopenfork(shared, reln, forknum, Extension::Fail).await.is_none() {
        return 0;
    }

    let fork = forknum as usize;
    let mut segno = (reln.md_seg_fds[fork].len() - 1) as BlockNumber;
    loop {
        let nblocks = {
            let file = reln.md_seg_fds[fork][segno as usize].mdfd_file.clone();
            seg_nblocks(&file).await
        };
        assert!(nblocks <= RELSEG_SIZE_BN, "segment too big");
        if nblocks < RELSEG_SIZE_BN {
            return segno * RELSEG_SIZE_BN + nblocks;
        }
        // Exactly RELSEG_SIZE: advance to the next segment (open it on demand).
        segno += 1;
        if openseg(shared, reln, forknum, segno, false).await.is_none() {
            return segno * RELSEG_SIZE_BN;
        }
    }
}

/// mdtruncate() -- truncate the fork to `nblocks` (curnblk is the current size).
pub async fn mdtruncate(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    curnblk: BlockNumber,
    nblocks: BlockNumber,
) {
    if nblocks > curnblk {
        // Bogus request; in recovery this is ignored. We have no InRecovery flag
        // yet, so treat as a no-op (the recovery-tolerant behavior).
        // TODO(InRecovery): error when not in recovery.
        return;
    }
    if nblocks == curnblk {
        return; // no work
    }

    let fork = forknum as usize;
    let mut curopensegs = reln.md_seg_fds[fork].len();
    while curopensegs > 0 {
        let priorblocks = (curopensegs as BlockNumber - 1) * RELSEG_SIZE_BN;
        let segno = curopensegs as BlockNumber - 1;

        if priorblocks > nblocks {
            // No longer active: truncate to 0, keep the file (see md.c header).
            let file = reln.md_seg_fds[fork][curopensegs - 1].mdfd_file.clone();
            file.truncate(0).await.expect("could not truncate segment");
            if !reln.is_temp() {
                register_dirty_segment(shared, reln, forknum, segno).await;
            }
            debug_assert!(curopensegs > 1, "never drop the first segment");
            reln.md_seg_fds[fork].truncate(curopensegs - 1);
        } else if priorblocks + RELSEG_SIZE_BN > nblocks {
            // Last segment to keep: truncate to the right length.
            let lastsegblocks = nblocks - priorblocks;
            let file = reln.md_seg_fds[fork][curopensegs - 1].mdfd_file.clone();
            file.truncate(u64::from(lastsegblocks) * BLCKSZ_U64)
                .await
                .expect("could not truncate segment");
            if !reln.is_temp() {
                register_dirty_segment(shared, reln, forknum, segno).await;
            }
        } else {
            break; // still need this and earlier segments
        }
        curopensegs -= 1;
    }
}

/// mdregistersync() -- mark the whole relation as needing fsync.
pub async fn mdregistersync(shared: &Arc<SharedState>, reln: &mut SmgrRelation, forknum: ForkNumber) {
    mdnblocks(shared, reln, forknum).await; // opens all active segments
    let fork = forknum as usize;

    let min_inactive = reln.md_seg_fds[fork].len() as BlockNumber;
    let mut segno = min_inactive;
    while openseg(shared, reln, forknum, segno, false).await.is_some() {
        segno += 1;
    }

    while segno > 0 {
        let seg = reln.md_seg_fds[fork][segno as usize - 1].mdfd_segno;
        register_dirty_segment(shared, reln, forknum, seg).await;
        if segno > min_inactive {
            reln.md_seg_fds[fork].truncate(segno as usize - 1);
        }
        segno -= 1;
    }
}

/// mdimmedsync() -- immediately fsync all (active + inactive) segments.
pub async fn mdimmedsync(shared: &Arc<SharedState>, reln: &mut SmgrRelation, forknum: ForkNumber) {
    mdnblocks(shared, reln, forknum).await; // opens all active segments
    let fork = forknum as usize;

    let min_inactive = reln.md_seg_fds[fork].len() as BlockNumber;
    let mut segno = min_inactive;
    while openseg(shared, reln, forknum, segno, false).await.is_some() {
        segno += 1;
    }

    while segno > 0 {
        let file = reln.md_seg_fds[fork][segno as usize - 1].mdfd_file.clone();
        file.sync().await.expect("could not fsync segment");
        if segno > min_inactive {
            reln.md_seg_fds[fork].truncate(segno as usize - 1);
        }
        segno -= 1;
    }
}

// ---------------------------------------------------------------------------
// Segment management (md.c mdopenfork / _mdfd_openseg / _mdfd_getseg).
// ---------------------------------------------------------------------------

/// mdopenfork() -- open the first segment of a fork. None per `behavior` if the
/// file is absent.
async fn mdopenfork(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    behavior: Extension,
) -> Option<File> {
    let fork = forknum as usize;
    if !reln.md_seg_fds[fork].is_empty() {
        return Some(reln.md_seg_fds[fork][0].mdfd_file.clone());
    }

    let path = mdfd_segpath(shared, reln, forknum, 0);
    match shared.fd().open(&path, open_flags()).await {
        Ok(file) => {
            reln.md_seg_fds[fork] = vec![MdfdVec { mdfd_file: file.clone(), mdfd_segno: 0 }];
            Some(file)
        }
        Err(e) if behavior == Extension::ReturnNull && is_possibly_deleted(&e) => None,
        Err(e) => panic!("could not open file {}: {e}", path.display()),
    }
}

fn is_possibly_deleted(e: &std::io::Error) -> bool {
    e.kind() == std::io::ErrorKind::NotFound
}

/// _mdfd_openseg() -- open segment `segno` (appended at the end of the array).
/// `create` adds O_CREAT. None on failure.
async fn openseg(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    segno: BlockNumber,
    create: bool,
) -> Option<File> {
    let path = mdfd_segpath(shared, reln, forknum, segno);
    let mut flags = open_flags();
    flags.create = create;

    let file = shared.fd().open(&path, flags).await.ok()?;
    let fork = forknum as usize;
    debug_assert_eq!(segno as usize, reln.md_seg_fds[fork].len(), "segments opened in order");
    reln.md_seg_fds[fork].push(MdfdVec { mdfd_file: file.clone(), mdfd_segno: segno });
    Some(file)
}

/// _mdfd_getseg() -- find/open the segment holding `blkno`, returning its File
/// and segment number. Opens intervening segments per `behavior`.
async fn getseg(
    shared: &Arc<SharedState>,
    reln: &mut SmgrRelation,
    forknum: ForkNumber,
    blkno: BlockNumber,
    skip_fsync: bool,
    behavior: Extension,
) -> Option<(File, BlockNumber)> {
    let fork = forknum as usize;
    let targetseg = blkno / RELSEG_SIZE_BN;

    if (targetseg as usize) < reln.md_seg_fds[fork].len() {
        let v = &reln.md_seg_fds[fork][targetseg as usize];
        return Some((v.mdfd_file.clone(), v.mdfd_segno));
    }

    if behavior == Extension::DontOpen {
        return None;
    }

    // Ensure the first segment is open.
    if reln.md_seg_fds[fork].is_empty() && mdopenfork(shared, reln, forknum, behavior).await.is_none() {
        return None;
    }

    let mut nextsegno = reln.md_seg_fds[fork].len() as BlockNumber;
    while nextsegno <= targetseg {
        let cur_nblocks = {
            let file = reln.md_seg_fds[fork][nextsegno as usize - 1].mdfd_file.clone();
            seg_nblocks(&file).await
        };
        assert!(cur_nblocks <= RELSEG_SIZE_BN, "segment too big");

        let create = behavior == Extension::Create;
        if create {
            // Pad the prior segment to RELSEG_SIZE if needed (invariant: all
            // segments before the last active one are exactly RELSEG_SIZE).
            if cur_nblocks < RELSEG_SIZE_BN {
                let zero = Page::boxed_zeroed();
                let pad_block = nextsegno * RELSEG_SIZE_BN - 1;
                // mdextend on the prior segment fills it out with the zero page.
                Box::pin(mdextend(shared, reln, forknum, pad_block, &zero, skip_fsync)).await;
            }
        } else if cur_nblocks < RELSEG_SIZE_BN {
            // Not extending and the current segment is short: the target segment
            // does not exist.
            if behavior == Extension::ReturnNull {
                return None;
            }
            panic!("could not open segment {nextsegno} (target block {blkno}): previous segment is only {cur_nblocks} blocks");
        }

        match openseg(shared, reln, forknum, nextsegno, create).await {
            Some(_) => {}
            None if behavior == Extension::ReturnNull => return None,
            None => panic!("could not open segment {nextsegno} (target block {blkno})"),
        }
        nextsegno += 1;
    }

    let v = &reln.md_seg_fds[fork][targetseg as usize];
    Some((v.mdfd_file.clone(), v.mdfd_segno))
}

// ---------------------------------------------------------------------------
// Sync-request helpers (md.c register_dirty_segment / _unlink / _forget).
// ---------------------------------------------------------------------------

/// register_dirty_segment() -- mark a segment as needing fsync. If the request
/// cannot be enqueued, fsync it now.
async fn register_dirty_segment(
    shared: &Arc<SharedState>,
    reln: &SmgrRelation,
    forknum: ForkNumber,
    segno: BlockNumber,
) {
    debug_assert!(!reln.is_temp(), "temp relations are never fsync'd");
    let tag = init_md_filetag(reln.rlocator, forknum, segno);
    if !RegisterSyncRequest(shared, &tag, SyncRequestType::SyncRequest, false) {
        // Queue full: fsync locally. The segment is open (caller just wrote it).
        let fork = forknum as usize;
        let file = reln.md_seg_fds[fork]
            .iter()
            .find(|v| v.mdfd_segno == segno)
            .map(|v| v.mdfd_file.clone());
        if let Some(file) = file {
            file.sync().await.expect("could not fsync segment");
        }
    }
}

fn register_unlink_segment(
    shared: &Arc<SharedState>,
    rlocator: RelFileLocatorBackend,
    forknum: ForkNumber,
    segno: BlockNumber,
) {
    debug_assert!(!rlocator.is_temp());
    let tag = init_md_filetag(rlocator, forknum, segno);
    RegisterSyncRequest(shared, &tag, SyncRequestType::SyncUnlinkRequest, true);
}

fn register_forget_request(
    shared: &Arc<SharedState>,
    rlocator: RelFileLocatorBackend,
    forknum: ForkNumber,
    segno: BlockNumber,
) {
    let tag = init_md_filetag(rlocator, forknum, segno);
    RegisterSyncRequest(shared, &tag, SyncRequestType::SyncForgetRequest, true);
}

/// ForgetDatabaseSyncRequests() -- forget all fsyncs/unlinks for a database.
pub fn forget_database_sync_requests(shared: &Arc<SharedState>, dbid: Oid) {
    let rlocator = crate::storage::relfilelocator::RelFileLocator {
        spcOid: Oid(0),
        dbOid: dbid,
        relNumber: Oid(0),
    };
    let tag = FileTag {
        handler: SyncRequestHandler::Md as i16,
        forknum: ForkNumber::InvalidForkNumber as i16,
        rlocator,
        segno: u64::from(INVALID_BLOCK_NUMBER),
    };
    RegisterSyncRequest(shared, &tag, SyncRequestType::SyncFilterRequest, true);
}

// ---------------------------------------------------------------------------
// Checkpointer-side sync callbacks (md.c mdsyncfiletag / mdunlinkfiletag /
// mdfiletagmatches), dispatched from sync.c ProcessSyncRequests.
// ---------------------------------------------------------------------------

/// mdsyncfiletag() -- fsync the segment named by `ftag`. Returns Ok(path) on
/// success, Err on failure (e.g. the file is gone).
pub async fn mdsyncfiletag(shared: &Arc<SharedState>, ftag: &FileTag) -> std::io::Result<String> {
    let forknum = ftag.forknum();
    let rlocator = RelFileLocatorBackend {
        locator: ftag.rlocator,
        backend: crate::storage::procnumber::INVALID_PROC_NUMBER,
    };
    let base = relpath(rlocator, forknum);
    let rel = if ftag.segno > 0 { format!("{}.{}", base.as_str(), ftag.segno) } else { base.str };
    let path = resolve_path(shared, &rel);

    let file = shared.fd().open(&path, open_flags()).await?;
    file.sync().await?;
    Ok(path.to_string_lossy().into_owned())
}

/// mdunlinkfiletag() -- unlink the file named by `ftag` (main fork, perm).
pub async fn mdunlinkfiletag(shared: &Arc<SharedState>, ftag: &FileTag) -> std::io::Result<String> {
    let rlocator = RelFileLocatorBackend {
        locator: ftag.rlocator,
        backend: crate::storage::procnumber::INVALID_PROC_NUMBER,
    };
    let base = relpath(rlocator, ForkNumber::MAIN_FORKNUM);
    let path = resolve_path(shared, base.as_str());
    io_backend::unlink(&path).await?;
    Ok(path.to_string_lossy().into_owned())
}

/// mdfiletagmatches() -- SYNC_FILTER_REQUEST predicate (same database).
pub fn mdfiletagmatches(ftag: &FileTag, candidate: &FileTag) -> bool {
    ftag.rlocator.dbOid == candidate.rlocator.dbOid
}
