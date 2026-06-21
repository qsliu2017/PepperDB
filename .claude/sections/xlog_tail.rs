// section: xlog_tail -- C lines 4601-9568 (GetMockAuthenticationNonce .. SetWalWriterSleeping)

/* Returns the random nonce from control file. */
pub unsafe fn GetMockAuthenticationNonce() -> *mut c_char {
    assert!(!ControlFile.is_null());
    (*ControlFile).mock_authentication_nonce.as_mut_ptr()
}

/*
 * Are checksums enabled for data pages?
 */
pub unsafe fn DataChecksumsEnabled() -> bool {
    assert!(!ControlFile.is_null());
    (*ControlFile).data_checksum_version > 0
}

/*
 * Return true if the cluster was initialized on a platform where the
 * default signedness of char is "signed". This function exists for code
 * that deals with pre-v18 data files that store data sorted by the 'char'
 * type on disk (e.g., GIN and GiST indexes). See the comments in
 * WriteControlFile() for details.
 */
pub unsafe fn GetDefaultCharSignedness() -> bool {
    (*ControlFile).default_char_signedness
}

/*
 * Returns a fake LSN for unlogged relations.
 *
 * Each call generates an LSN that is greater than any previous value
 * returned. The current counter value is saved and restored across clean
 * shutdowns, but like unlogged relations, does not survive a crash. This can
 * be used in lieu of real LSN values returned by XLogInsert, if you need an
 * LSN-like increasing sequence of numbers without writing any WAL.
 */
pub unsafe fn GetFakeLSNForUnloggedRel() -> XLogRecPtr {
    pg_atomic_fetch_add_u64(&mut (*XLogCtl).unloggedLSN, 1)
}

/*
 * Auto-tune the number of XLOG buffers.
 *
 * The preferred setting for wal_buffers is about 3% of shared_buffers, with
 * a maximum of one XLOG segment (there is little reason to think that more
 * is helpful, at least so long as we force an fsync when switching log files)
 * and a minimum of 8 blocks (which was the default value prior to PostgreSQL
 * 9.1, when auto-tuning was added).
 *
 * This should not be called until NBuffers has received its final value.
 */
unsafe fn XLOGChooseNumBuffers() -> c_int {
    let mut xbuffers: c_int = NBuffers / 32;
    if xbuffers > (wal_segment_size / XLOG_BLCKSZ as c_int) {
        xbuffers = wal_segment_size / XLOG_BLCKSZ as c_int;
    }
    if xbuffers < 8 {
        xbuffers = 8;
    }
    xbuffers
}

/*
 * GUC check_hook for wal_buffers
 */
pub unsafe fn check_wal_buffers(newval: *mut c_int, _extra: *mut *mut c_void, _source: GucSource) -> bool {
    /*
     * -1 indicates a request for auto-tune.
     */
    if *newval == -1 {
        /*
         * If we haven't yet changed the boot_val default of -1, just let it
         * be.  We'll fix it when XLOGShmemSize is called.
         */
        if XLOGbuffers == -1 {
            return true;
        }
        /* Otherwise, substitute the auto-tune value */
        *newval = XLOGChooseNumBuffers();
    }

    /*
     * We clamp manually-set values to at least 4 blocks.  Prior to PostgreSQL
     * 9.1, a minimum of 4 was enforced by guc.c, but since that is no longer
     * the case, we just silently treat such values as a request for the
     * minimum.  (We could throw an error instead, but that doesn't seem very
     * helpful.)
     */
    if *newval < 4 {
        *newval = 4;
    }
    true
}

/*
 * GUC check_hook for wal_consistency_checking
 */
pub unsafe fn check_wal_consistency_checking(
    newval: *mut *mut c_char,
    extra: *mut *mut c_void,
    _source: GucSource,
) -> bool {
    let mut newwalconsistency: [bool; RM_MAX_ID as usize + 1] =
        [false; RM_MAX_ID as usize + 1];

    /* Need a modifiable copy of string */
    let rawstring = pstrdup(*newval);

    /* Parse string into list of identifiers */
    let mut elemlist: *mut List = ptr::null_mut();
    if !SplitIdentifierString(rawstring, b',' as c_char, &mut elemlist) {
        /* syntax error in list */
        GUC_check_errdetail(b"List syntax is invalid.\0".as_ptr() as *const c_char);
        pfree(rawstring as *mut c_void);
        list_free(elemlist);
        return false;
    }

    let mut lc = (*elemlist).head;
    while !lc.is_null() {
        let tok = (*lc).ptr_value as *mut c_char;
        lc = (*lc).next;

        /* Check for 'all'. */
        if pg_strcasecmp(tok, b"all\0".as_ptr() as *const c_char) == 0 {
            for rmid in 0..=RM_MAX_ID as usize {
                if RmgrIdExists(rmid as BuiltinRmgrId)
                    && !GetRmgr(rmid as BuiltinRmgrId).rm_mask.is_null()
                {
                    newwalconsistency[rmid] = true;
                }
            }
        } else {
            /* Check if the token matches any known resource manager. */
            let mut found = false;
            for rmid in 0..=RM_MAX_ID as usize {
                if RmgrIdExists(rmid as BuiltinRmgrId)
                    && !GetRmgr(rmid as BuiltinRmgrId).rm_mask.is_null()
                    && pg_strcasecmp(tok, GetRmgr(rmid as BuiltinRmgrId).rm_name) == 0
                {
                    newwalconsistency[rmid] = true;
                    found = true;
                    break;
                }
            }
            if !found {
                /*
                 * During startup, it might be a not-yet-loaded custom
                 * resource manager.  Defer checking until
                 * InitializeWalConsistencyChecking().
                 */
                if !process_shared_preload_libraries_done {
                    check_wal_consistency_checking_deferred = true;
                } else {
                    GUC_check_errdetail_fmt(
                        b"Unrecognized key word: \"%s\".\0".as_ptr() as *const c_char,
                        tok,
                    );
                    pfree(rawstring as *mut c_void);
                    list_free(elemlist);
                    return false;
                }
            }
        }
    }

    pfree(rawstring as *mut c_void);
    list_free(elemlist);

    /* assign new value */
    *extra = guc_malloc(LOG, (RM_MAX_ID as usize + 1) * core::mem::size_of::<bool>());
    if (*extra).is_null() {
        return false;
    }
    ptr::copy_nonoverlapping(
        newwalconsistency.as_ptr(),
        *extra as *mut bool,
        RM_MAX_ID as usize + 1,
    );
    true
}

/*
 * GUC assign_hook for wal_consistency_checking
 */
pub unsafe fn assign_wal_consistency_checking(_newval: *const c_char, extra: *mut c_void) {
    /*
     * If some checks were deferred, it's possible that the checks will fail
     * later during InitializeWalConsistencyChecking(). But in that case, the
     * postmaster will exit anyway, so it's safe to proceed with the
     * assignment.
     *
     * Any built-in resource managers specified are assigned immediately,
     * which affects WAL created before shared_preload_libraries are
     * processed. Any custom resource managers specified won't be assigned
     * until after shared_preload_libraries are processed, but that's OK
     * because WAL for a custom resource manager can't be written before the
     * module is loaded anyway.
     */
    wal_consistency_checking = extra as *mut bool;
}

/*
 * InitializeWalConsistencyChecking: run after loading custom resource managers
 *
 * If any unknown resource managers were specified in the
 * wal_consistency_checking GUC, processing was deferred.  Now that
 * shared_preload_libraries have been loaded, process wal_consistency_checking
 * again.
 */
pub unsafe fn InitializeWalConsistencyChecking() {
    assert!(process_shared_preload_libraries_done);

    if check_wal_consistency_checking_deferred {
        let guc = find_option(
            b"wal_consistency_checking\0".as_ptr() as *const c_char,
            false,
            false,
            ERROR,
        );

        check_wal_consistency_checking_deferred = false;

        set_config_option_ext(
            b"wal_consistency_checking\0".as_ptr() as *const c_char,
            wal_consistency_checking_string,
            (*guc).scontext,
            (*guc).source,
            (*guc).srole,
            GUC_ACTION_SET,
            true,
            ERROR,
            false,
        );

        /* checking should not be deferred again */
        assert!(!check_wal_consistency_checking_deferred);
    }
}

/*
 * GUC show_hook for archive_command
 */
pub unsafe fn show_archive_command() -> *const c_char {
    if XLogArchivingActive() {
        XLogArchiveCommand
    } else {
        b"(disabled)\0".as_ptr() as *const c_char
    }
}

/*
 * GUC show_hook for in_hot_standby
 */
pub unsafe fn show_in_hot_standby() -> *const c_char {
    /*
     * We display the actual state based on shared memory, so that this GUC
     * reports up-to-date state if examined intra-query.  The underlying
     * variable (in_hot_standby_guc) changes only when we transmit a new value
     * to the client.
     */
    if RecoveryInProgress() {
        b"on\0".as_ptr() as *const c_char
    } else {
        b"off\0".as_ptr() as *const c_char
    }
}

/*
 * Read the control file, set respective GUCs.
 *
 * This is to be called during startup, including a crash recovery cycle,
 * unless in bootstrap mode, where no control file yet exists.  As there's no
 * usable shared memory yet (its sizing can depend on the contents of the
 * control file!), first store the contents in local memory. XLOGShmemInit()
 * will then copy it to shared memory later.
 *
 * reset just controls whether previous contents are to be expected (in the
 * reset case, there's a dangling pointer into old shared memory), or not.
 */
pub unsafe fn LocalProcessControlFile(reset: bool) {
    assert!(reset || ControlFile.is_null());
    ControlFile = palloc(core::mem::size_of::<ControlFileData>()) as *mut ControlFileData;
    ReadControlFile();
}

/*
 * Get the wal_level from the control file. For a standby, this value should be
 * considered as its active wal_level, because it may be different from what
 * was originally configured on standby.
 */
pub unsafe fn GetActiveWalLevelOnStandby() -> WalLevel {
    (*ControlFile).wal_level
}

/*
 * Initialization of shared memory for XLOG
 */
pub unsafe fn XLOGShmemSize() -> Size {
    let mut size: Size;

    /*
     * If the value of wal_buffers is -1, use the preferred auto-tune value.
     * This isn't an amazingly clean place to do this, but we must wait till
     * NBuffers has received its final value, and must do it before using the
     * value of XLOGbuffers to do anything important.
     *
     * We prefer to report this value's source as PGC_S_DYNAMIC_DEFAULT.
     * However, if the DBA explicitly set wal_buffers = -1 in the config file,
     * then PGC_S_DYNAMIC_DEFAULT will fail to override that and we must force
     * the matter with PGC_S_OVERRIDE.
     */
    if XLOGbuffers == -1 {
        let mut buf = [0u8; 32];
        let s = format!("{}", XLOGChooseNumBuffers());
        let bytes = s.as_bytes();
        let len = bytes.len().min(31);
        buf[..len].copy_from_slice(&bytes[..len]);
        SetConfigOption(
            b"wal_buffers\0".as_ptr() as *const c_char,
            buf.as_ptr() as *const c_char,
            PGC_POSTMASTER,
            PGC_S_DYNAMIC_DEFAULT,
        );
        if XLOGbuffers == -1 {
            /* failed to apply it? */
            SetConfigOption(
                b"wal_buffers\0".as_ptr() as *const c_char,
                buf.as_ptr() as *const c_char,
                PGC_POSTMASTER,
                PGC_S_OVERRIDE,
            );
        }
    }
    assert!(XLOGbuffers > 0);

    /* XLogCtl */
    size = core::mem::size_of::<XLogCtlData>();

    /* WAL insertion locks, plus alignment */
    size = add_size(
        size,
        mul_size(
            core::mem::size_of::<WALInsertLockPadded>(),
            NUM_XLOGINSERT_LOCKS as usize + 1,
        ),
    );
    /* xlblocks array */
    size = add_size(
        size,
        mul_size(
            core::mem::size_of::<pg_atomic_uint64>(),
            XLOGbuffers as usize,
        ),
    );
    /* extra alignment padding for XLOG I/O buffers */
    size = add_size(size, XLOG_BLCKSZ.max(PG_IO_ALIGN_SIZE));
    /* and the buffers themselves */
    size = add_size(size, mul_size(XLOG_BLCKSZ, XLOGbuffers as usize));

    /*
     * Note: we don't count ControlFileData, it comes out of the "slop factor"
     * added by CreateSharedMemoryAndSemaphores.  This lets us use this
     * routine again below to compute the actual allocation size.
     */
    size
}

pub unsafe fn XLOGShmemInit() {
    let mut foundCFile: bool = false;
    let mut foundXLog: bool = false;
    let mut allocptr: *mut c_char;
    let mut i: c_int;
    let localControlFile: *mut ControlFileData;

    #[cfg(feature = "wal_debug")]
    {
        /*
         * Create a memory context for WAL debugging that's exempt from the normal
         * "no pallocs in critical section" rule. Yes, that can lead to a PANIC if
         * an allocation fails, but wal_debug is not for production use anyway.
         */
        if walDebugCxt.is_null() {
            walDebugCxt = AllocSetContextCreate(
                TopMemoryContext,
                b"WAL Debug\0".as_ptr() as *const c_char,
                ALLOCSET_DEFAULT_SIZES,
            );
            MemoryContextAllowInCriticalSection(walDebugCxt, true);
        }
    }

    XLogCtl = ShmemInitStruct(
        b"XLOG Ctl\0".as_ptr() as *const c_char,
        XLOGShmemSize(),
        &mut foundXLog,
    ) as *mut XLogCtlData;

    localControlFile = ControlFile;
    ControlFile = ShmemInitStruct(
        b"Control File\0".as_ptr() as *const c_char,
        core::mem::size_of::<ControlFileData>(),
        &mut foundCFile,
    ) as *mut ControlFileData;

    if foundCFile || foundXLog {
        /* both should be present or neither */
        assert!(foundCFile && foundXLog);

        /* Initialize local copy of WALInsertLocks */
        WALInsertLocks = (*XLogCtl).Insert.WALInsertLocks;

        if !localControlFile.is_null() {
            pfree(localControlFile as *mut c_void);
        }
        return;
    }
    ptr::write_bytes(XLogCtl as *mut u8, 0, core::mem::size_of::<XLogCtlData>());

    /*
     * Already have read control file locally, unless in bootstrap mode. Move
     * contents into shared memory.
     */
    if !localControlFile.is_null() {
        ptr::copy_nonoverlapping(
            localControlFile,
            ControlFile,
            1,
        );
        pfree(localControlFile as *mut c_void);
    }

    /*
     * Since XLogCtlData contains XLogRecPtr fields, its sizeof should be a
     * multiple of the alignment for same, so no extra alignment padding is
     * needed here.
     */
    allocptr = (XLogCtl as *mut c_char).add(core::mem::size_of::<XLogCtlData>());
    (*XLogCtl).xlblocks = allocptr as *mut pg_atomic_uint64;
    allocptr = allocptr.add(core::mem::size_of::<pg_atomic_uint64>() * XLOGbuffers as usize);

    i = 0;
    while i < XLOGbuffers {
        pg_atomic_init_u64(
            &mut *(*XLogCtl).xlblocks.add(i as usize),
            InvalidXLogRecPtr,
        );
        i += 1;
    }

    /* WAL insertion locks. Ensure they're aligned to the full padded size */
    let align = core::mem::size_of::<WALInsertLockPadded>();
    let offset = (allocptr as usize) % align;
    if offset != 0 {
        allocptr = allocptr.add(align - offset);
    }
    WALInsertLocks = allocptr as *mut WALInsertLockPadded;
    (*XLogCtl).Insert.WALInsertLocks = WALInsertLocks;
    allocptr = allocptr.add(core::mem::size_of::<WALInsertLockPadded>() * NUM_XLOGINSERT_LOCKS as usize);

    i = 0;
    while i < NUM_XLOGINSERT_LOCKS {
        LWLockInitialize(
            &mut (*WALInsertLocks.add(i as usize)).l.lock,
            LWTRANCHE_WAL_INSERT,
        );
        pg_atomic_init_u64(
            &mut (*WALInsertLocks.add(i as usize)).l.insertingAt,
            InvalidXLogRecPtr,
        );
        (*WALInsertLocks.add(i as usize)).l.lastImportantAt = InvalidXLogRecPtr;
        i += 1;
    }

    /*
     * Align the start of the page buffers to a full xlog block size boundary.
     * This simplifies some calculations in XLOG insertion. It is also
     * required for O_DIRECT.
     */
    let blksz = XLOG_BLCKSZ;
    let rem = (allocptr as usize) % blksz;
    if rem != 0 {
        allocptr = allocptr.add(blksz - rem);
    }
    (*XLogCtl).pages = allocptr;
    ptr::write_bytes((*XLogCtl).pages as *mut u8, 0, blksz * XLOGbuffers as usize);

    /*
     * Do basic initialization of XLogCtl shared data. (StartupXLOG will fill
     * in additional info.)
     */
    (*XLogCtl).XLogCacheBlck = XLOGbuffers - 1;
    (*XLogCtl).SharedRecoveryState = RECOVERY_STATE_CRASH;
    (*XLogCtl).InstallXLogFileSegmentActive = false;
    (*XLogCtl).WalWriterSleeping = false;

    SpinLockInit(&mut (*XLogCtl).Insert.insertpos_lck);
    SpinLockInit(&mut (*XLogCtl).info_lck);
    pg_atomic_init_u64(&mut (*XLogCtl).logInsertResult, InvalidXLogRecPtr);
    pg_atomic_init_u64(&mut (*XLogCtl).logWriteResult, InvalidXLogRecPtr);
    pg_atomic_init_u64(&mut (*XLogCtl).logFlushResult, InvalidXLogRecPtr);
    pg_atomic_init_u64(&mut (*XLogCtl).unloggedLSN, InvalidXLogRecPtr);
}

/*
 * This func must be called ONCE on system install.  It creates pg_control
 * and the initial XLOG segment.
 */
pub unsafe fn BootStrapXLOG(data_checksum_version: uint32) {
    let mut checkPoint: CheckPoint = core::mem::zeroed();
    let buffer: *mut c_char;
    let page: XLogPageHeader;
    let longpage: *mut XLogLongPageHeaderData;
    let record: *mut XLogRecord;
    let mut recptr: *mut c_char;
    let sysidentifier: uint64;
    let mut tv: libc::timeval = core::mem::zeroed();
    let mut crc: pg_crc32c = 0;

    /* allow ordinary WAL segment creation, like StartupXLOG() would */
    SetInstallXLogFileSegmentActive();

    /*
     * Select a hopefully-unique system identifier code for this installation.
     * We use the result of gettimeofday(), including the fractional seconds
     * field, as being about as unique as we can easily get.  (Think not to
     * use random(), since it hasn't been seeded and there's no portable way
     * to seed it other than the system clock value...)  The upper half of the
     * uint64 value is just the tv_sec part, while the lower half contains the
     * tv_usec part (which must fit in 20 bits), plus 12 bits from our current
     * PID for a little extra uniqueness.  A person knowing this encoding can
     * determine the initialization time of the installation, which could
     * perhaps be useful sometimes.
     */
    libc::gettimeofday(&mut tv, ptr::null_mut());
    sysidentifier = ((tv.tv_sec as uint64) << 32)
        | ((tv.tv_usec as uint64) << 12)
        | (libc::getpid() as uint64 & 0xFFF);

    /* page buffer must be aligned suitably for O_DIRECT */
    buffer = palloc(XLOG_BLCKSZ + XLOG_BLCKSZ) as *mut c_char;
    let aligned = ((buffer as usize + XLOG_BLCKSZ - 1) & !(XLOG_BLCKSZ - 1)) as *mut c_char;
    page = aligned as XLogPageHeader;
    ptr::write_bytes(page as *mut u8, 0, XLOG_BLCKSZ);

    /*
     * Set up information for the initial checkpoint record
     *
     * The initial checkpoint record is written to the beginning of the WAL
     * segment with logid=0 logseg=1. The very first WAL segment, 0/0, is not
     * used, so that we can use 0/0 to mean "before any valid WAL segment".
     */
    checkPoint.redo = wal_segment_size as XLogRecPtr + SizeOfXLogLongPHD as XLogRecPtr;
    checkPoint.ThisTimeLineID = BootstrapTimeLineID;
    checkPoint.PrevTimeLineID = BootstrapTimeLineID;
    checkPoint.fullPageWrites = fullPageWrites;
    checkPoint.wal_level = wal_level;
    checkPoint.nextXid =
        FullTransactionIdFromEpochAndXid(0, FirstNormalTransactionId);
    checkPoint.nextOid = FirstGenbkiObjectId;
    checkPoint.nextMulti = FirstMultiXactId;
    checkPoint.nextMultiOffset = 0;
    checkPoint.oldestXid = FirstNormalTransactionId;
    checkPoint.oldestXidDB = Template1DbOid;
    checkPoint.oldestMulti = FirstMultiXactId;
    checkPoint.oldestMultiDB = Template1DbOid;
    checkPoint.oldestCommitTsXid = InvalidTransactionId;
    checkPoint.newestCommitTsXid = InvalidTransactionId;
    checkPoint.time = libc::time(ptr::null_mut()) as pg_time_t;
    checkPoint.oldestActiveXid = InvalidTransactionId;

    (*TransamVariables).nextXid = checkPoint.nextXid;
    (*TransamVariables).nextOid = checkPoint.nextOid;
    (*TransamVariables).oidCount = 0;
    MultiXactSetNextMXact(checkPoint.nextMulti, checkPoint.nextMultiOffset);
    AdvanceOldestClogXid(checkPoint.oldestXid);
    SetTransactionIdLimit(checkPoint.oldestXid, checkPoint.oldestXidDB);
    SetMultiXactIdLimit(checkPoint.oldestMulti, checkPoint.oldestMultiDB, true);
    SetCommitTsLimit(InvalidTransactionId, InvalidTransactionId);

    /* Set up the XLOG page header */
    (*page).xlp_magic = XLOG_PAGE_MAGIC;
    (*page).xlp_info = XLP_LONG_HEADER;
    (*page).xlp_tli = BootstrapTimeLineID;
    (*page).xlp_pageaddr = wal_segment_size as XLogRecPtr;
    longpage = page as *mut XLogLongPageHeaderData;
    (*longpage).xlp_sysid = sysidentifier;
    (*longpage).xlp_seg_size = wal_segment_size as uint32;
    (*longpage).xlp_xlog_blcksz = XLOG_BLCKSZ as uint32;

    /* Insert the initial checkpoint record */
    recptr = (page as *mut c_char).add(SizeOfXLogLongPHD);
    record = recptr as *mut XLogRecord;
    (*record).xl_prev = 0;
    (*record).xl_xid = InvalidTransactionId;
    (*record).xl_tot_len = (SizeOfXLogRecord
        + SizeOfXLogRecordDataHeaderShort
        + core::mem::size_of::<CheckPoint>()) as uint32;
    (*record).xl_info = XLOG_CHECKPOINT_SHUTDOWN;
    (*record).xl_rmid = RM_XLOG_ID;
    recptr = recptr.add(SizeOfXLogRecord);
    /* fill the XLogRecordDataHeaderShort struct */
    *recptr = XLR_BLOCK_ID_DATA_SHORT as c_char;
    recptr = recptr.add(1);
    *recptr = core::mem::size_of::<CheckPoint>() as c_char;
    recptr = recptr.add(1);
    ptr::copy_nonoverlapping(
        &checkPoint as *const CheckPoint as *const u8,
        recptr as *mut u8,
        core::mem::size_of::<CheckPoint>(),
    );
    recptr = recptr.add(core::mem::size_of::<CheckPoint>());
    debug_assert_eq!(
        recptr as usize - record as usize,
        (*record).xl_tot_len as usize
    );

    INIT_CRC32C!(crc);
    COMP_CRC32C!(
        crc,
        (record as *const c_char).add(SizeOfXLogRecord),
        (*record).xl_tot_len as usize - SizeOfXLogRecord
    );
    COMP_CRC32C!(crc, record as *const c_char, XLogRecord_crc_offset());
    FIN_CRC32C!(crc);
    (*record).xl_crc = crc;

    /* Create first XLOG segment file */
    openLogTLI = BootstrapTimeLineID;
    openLogFile = XLogFileInit(1, BootstrapTimeLineID);

    /*
     * We needn't bother with Reserve/ReleaseExternalFD here, since we'll
     * close the file again in a moment.
     */

    /* Write the first page with the initial record */
    *libc::__error() = 0;
    pgstat_report_wait_start(WAIT_EVENT_WAL_BOOTSTRAP_WRITE);
    if libc::write(openLogFile, page as *const c_void, XLOG_BLCKSZ) != XLOG_BLCKSZ as isize {
        /* if write didn't set errno, assume problem is no disk space */
        if *libc::__error() == 0 {
            *libc::__error() = libc::ENOSPC;
        }
        ereport!(PANIC, errmsg!("could not write bootstrap write-ahead log file: {}", strerror_r()));
        /* errcode_for_file_access */
    }
    pgstat_report_wait_end();

    pgstat_report_wait_start(WAIT_EVENT_WAL_BOOTSTRAP_SYNC);
    if pg_fsync(openLogFile) != 0 {
        ereport!(PANIC, errmsg!("could not fsync bootstrap write-ahead log file: {}", strerror_r()));
        /* errcode_for_file_access */
    }
    pgstat_report_wait_end();

    if libc::close(openLogFile) != 0 {
        ereport!(PANIC, errmsg!("could not close bootstrap write-ahead log file: {}", strerror_r()));
        /* errcode_for_file_access */
    }
    openLogFile = -1;

    /* Now create pg_control */
    InitControlFile(sysidentifier, data_checksum_version);
    (*ControlFile).time = checkPoint.time;
    (*ControlFile).checkPoint = checkPoint.redo;
    (*ControlFile).checkPointCopy = checkPoint;

    /* some additional ControlFile fields are set in WriteControlFile() */
    WriteControlFile();

    /* Bootstrap the commit log, too */
    BootStrapCLOG();
    BootStrapCommitTs();
    BootStrapSUBTRANS();
    BootStrapMultiXact();

    pfree(buffer as *mut c_void);

    /*
     * Force control file to be read - in contrast to normal processing we'd
     * otherwise never run the checks and GUC related initializations therein.
     */
    ReadControlFile();
}

unsafe fn str_time(tnow: pg_time_t) -> *mut c_char {
    let buf = palloc(128) as *mut c_char;
    pg_strftime(
        buf,
        128,
        b"%Y-%m-%d %H:%M:%S %Z\0".as_ptr() as *const c_char,
        pg_localtime(&tnow, log_timezone),
    );
    buf
}

/*
 * Initialize the first WAL segment on new timeline.
 */
unsafe fn XLogInitNewTimeline(endTLI: TimeLineID, endOfLog: XLogRecPtr, newTLI: TimeLineID) {
    let mut xlogfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let mut endLogSegNo: XLogSegNo = 0;
    let mut startLogSegNo: XLogSegNo = 0;

    /* we always switch to a new timeline after archive recovery */
    assert!(endTLI != newTLI);

    /*
     * Update min recovery point one last time.
     */
    UpdateMinRecoveryPoint(InvalidXLogRecPtr, true);

    /*
     * Calculate the last segment on the old timeline, and the first segment
     * on the new timeline. If the switch happens in the middle of a segment,
     * they are the same, but if the switch happens exactly at a segment
     * boundary, startLogSegNo will be endLogSegNo + 1.
     */
    XLByteToPrevSeg(endOfLog, &mut endLogSegNo, wal_segment_size as uint32);
    XLByteToSeg(endOfLog, &mut startLogSegNo, wal_segment_size as uint32);

    /*
     * Initialize the starting WAL segment for the new timeline. If the switch
     * happens in the middle of a segment, copy data from the last WAL segment
     * of the old timeline up to the switch point, to the starting WAL segment
     * on the new timeline.
     */
    if endLogSegNo == startLogSegNo {
        /*
         * Make a copy of the file on the new timeline.
         *
         * Writing WAL isn't allowed yet, so there are no locking
         * considerations. But we should be just as tense as XLogFileInit to
         * avoid emplacing a bogus file.
         */
        XLogFileCopy(
            newTLI,
            endLogSegNo,
            endTLI,
            endLogSegNo,
            XLogSegmentOffset(endOfLog, wal_segment_size as uint32),
        );
    } else {
        /*
         * The switch happened at a segment boundary, so just create the next
         * segment on the new timeline.
         */
        let fd = XLogFileInit(startLogSegNo, newTLI);

        if libc::close(fd) != 0 {
            let save_errno = *libc::__error();
            XLogFileName(
                xlogfname.as_mut_ptr(),
                newTLI,
                startLogSegNo,
                wal_segment_size as uint32,
            );
            *libc::__error() = save_errno;
            ereport!(
                ERROR,
                errmsg!(
                    "could not close file \"{}\": {}",
                    core::ffi::CStr::from_ptr(xlogfname.as_ptr()).to_string_lossy(),
                    strerror_r()
                )
            );
            /* errcode_for_file_access */
        }
    }

    /*
     * Let's just make real sure there are not .ready or .done flags posted
     * for the new segment.
     */
    XLogFileName(
        xlogfname.as_mut_ptr(),
        newTLI,
        startLogSegNo,
        wal_segment_size as uint32,
    );
    XLogArchiveCleanup(xlogfname.as_ptr());
}

/*
 * Perform cleanup actions at the conclusion of archive recovery.
 */
unsafe fn CleanupAfterArchiveRecovery(
    EndOfLogTLI: TimeLineID,
    EndOfLog: XLogRecPtr,
    newTLI: TimeLineID,
) {
    /*
     * Execute the recovery_end_command, if any.
     */
    if !recoveryEndCommand.is_null()
        && libc::strcmp(recoveryEndCommand, b"\0".as_ptr() as *const c_char) != 0
    {
        ExecuteRecoveryCommand(
            recoveryEndCommand,
            b"recovery_end_command\0".as_ptr() as *const c_char,
            true,
            WAIT_EVENT_RECOVERY_END_COMMAND,
        );
    }

    /*
     * We switched to a new timeline. Clean up segments on the old timeline.
     *
     * If there are any higher-numbered segments on the old timeline, remove
     * them. They might contain valid WAL, but they might also be
     * pre-allocated files containing garbage. In any case, they are not part
     * of the new timeline's history so we don't need them.
     */
    RemoveNonParentXlogFiles(EndOfLog, newTLI);

    /*
     * If the switch happened in the middle of a segment, what to do with the
     * last, partial segment on the old timeline? ... (see C comment)
     * As a compromise, we rename the last segment with the .partial suffix,
     * and archive it.
     */
    if XLogSegmentOffset(EndOfLog, wal_segment_size as uint32) != 0
        && XLogArchivingActive()
    {
        let mut origfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
        let mut endLogSegNo: XLogSegNo = 0;

        XLByteToPrevSeg(EndOfLog, &mut endLogSegNo, wal_segment_size as uint32);
        XLogFileName(
            origfname.as_mut_ptr(),
            EndOfLogTLI,
            endLogSegNo,
            wal_segment_size as uint32,
        );

        if !XLogArchiveIsReadyOrDone(origfname.as_ptr()) {
            let mut origpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
            let mut partialfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
            let mut partialpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];

            /*
             * If we're summarizing WAL, we can't rename the partial file
             * until the summarizer finishes with it, else it will fail.
             */
            if summarize_wal {
                WaitForWalSummarization(EndOfLog);
            }

            XLogFilePath(
                origpath.as_mut_ptr(),
                EndOfLogTLI,
                endLogSegNo,
                wal_segment_size as uint32,
            );
            libc::snprintf(
                partialfname.as_mut_ptr(),
                MAXFNAMELEN,
                b"%s.partial\0".as_ptr() as *const c_char,
                origfname.as_ptr(),
            );
            libc::snprintf(
                partialpath.as_mut_ptr(),
                MAXPGPATH,
                b"%s.partial\0".as_ptr() as *const c_char,
                origpath.as_ptr(),
            );

            /*
             * Make sure there's no .done or .ready file for the .partial
             * file.
             */
            XLogArchiveCleanup(partialfname.as_ptr());

            durable_rename(origpath.as_ptr(), partialpath.as_ptr(), ERROR);
            XLogArchiveNotify(partialfname.as_ptr());
        }
    }
}

/*
 * Check to see if required parameters are set high enough on this server
 * for various aspects of recovery operation.
 */
unsafe fn CheckRequiredParameterValues() {
    /*
     * For archive recovery, the WAL must be generated with at least 'replica'
     * wal_level.
     */
    if ArchiveRecoveryRequested && (*ControlFile).wal_level == WAL_LEVEL_MINIMAL {
        ereport!(
            FATAL,
            errmsg!("WAL was generated with \"wal_level=minimal\", cannot continue recovering")
            /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
               errdetail + errhint also present in C */
        );
    }

    /*
     * For Hot Standby, the WAL must be generated with 'replica' mode, and we
     * must have at least as many backend slots as the primary.
     */
    if ArchiveRecoveryRequested && EnableHotStandby {
        /* We ignore autovacuum_worker_slots when we make this test. */
        RecoveryRequiresIntParameter(
            b"max_connections\0".as_ptr() as *const c_char,
            MaxConnections,
            (*ControlFile).MaxConnections,
        );
        RecoveryRequiresIntParameter(
            b"max_worker_processes\0".as_ptr() as *const c_char,
            max_worker_processes,
            (*ControlFile).max_worker_processes,
        );
        RecoveryRequiresIntParameter(
            b"max_wal_senders\0".as_ptr() as *const c_char,
            max_wal_senders,
            (*ControlFile).max_wal_senders,
        );
        RecoveryRequiresIntParameter(
            b"max_prepared_transactions\0".as_ptr() as *const c_char,
            max_prepared_xacts,
            (*ControlFile).max_prepared_xacts,
        );
        RecoveryRequiresIntParameter(
            b"max_locks_per_transaction\0".as_ptr() as *const c_char,
            max_locks_per_xact,
            (*ControlFile).max_locks_per_xact,
        );
    }
}

/*
 * This must be called ONCE during postmaster or standalone-backend startup
 */
pub unsafe fn StartupXLOG() {
    let Insert: *mut XLogCtlInsert;
    let mut checkPoint: CheckPoint = core::mem::zeroed();
    let mut wasShutdown: bool = false;
    let mut didCrash: bool;
    let mut haveTblspcMap: bool = false;
    let mut haveBackupLabel: bool = false;
    let mut EndOfLog: XLogRecPtr = 0;
    let mut EndOfLogTLI: TimeLineID = 0;
    let mut newTLI: TimeLineID;
    let mut performedWalRecovery: bool;
    let mut endOfRecoveryInfo: *mut EndOfWalRecoveryInfo;
    let mut abortedRecPtr: XLogRecPtr = InvalidXLogRecPtr;
    let mut missingContrecPtr: XLogRecPtr = InvalidXLogRecPtr;
    let mut oldestActiveXID: TransactionId = InvalidTransactionId;
    let mut promoted: bool = false;

    /*
     * We should have an aux process resource owner to use, and we should not
     * be in a transaction that's installed some other resowner.
     */
    assert!(!AuxProcessResourceOwner.is_null());
    assert!(
        CurrentResourceOwner.is_null()
            || CurrentResourceOwner == AuxProcessResourceOwner
    );
    CurrentResourceOwner = AuxProcessResourceOwner;

    /*
     * Check that contents look valid.
     */
    if !XRecOffIsValid((*ControlFile).checkPoint) {
        ereport!(FATAL, errmsg!("control file contains invalid checkpoint location"));
        /* errcode(ERRCODE_DATA_CORRUPTED) */
    }

    match (*ControlFile).state {
        DB_SHUTDOWNED => {
            /*
             * This is the expected case, so don't be chatty in standalone mode
             */
            ereport!(
                if IsPostmasterEnvironment { LOG } else { NOTICE },
                errmsg!("database system was shut down at {}", cstr_to_str(str_time((*ControlFile).time)))
            );
        }
        DB_SHUTDOWNED_IN_RECOVERY => {
            ereport!(
                LOG,
                errmsg!("database system was shut down in recovery at {}", cstr_to_str(str_time((*ControlFile).time)))
            );
        }
        DB_SHUTDOWNING => {
            ereport!(
                LOG,
                errmsg!("database system shutdown was interrupted; last known up at {}", cstr_to_str(str_time((*ControlFile).time)))
            );
        }
        DB_IN_CRASH_RECOVERY => {
            ereport!(
                LOG,
                errmsg!("database system was interrupted while in recovery at {}", cstr_to_str(str_time((*ControlFile).time)))
                /* errhint also in C */
            );
        }
        DB_IN_ARCHIVE_RECOVERY => {
            ereport!(
                LOG,
                errmsg!("database system was interrupted while in recovery at log time {}", cstr_to_str(str_time((*ControlFile).checkPointCopy.time)))
                /* errhint also in C */
            );
        }
        DB_IN_PRODUCTION => {
            ereport!(
                LOG,
                errmsg!("database system was interrupted; last known up at {}", cstr_to_str(str_time((*ControlFile).time)))
            );
        }
        _ => {
            ereport!(FATAL, errmsg!("control file contains invalid database cluster state"));
            /* errcode(ERRCODE_DATA_CORRUPTED) */
        }
    }

    /* This is just to allow attaching to startup process with a debugger */
    // #ifdef XLOG_REPLAY_DELAY -- not compiled in production

    /*
     * Verify that pg_wal, pg_wal/archive_status, and pg_wal/summaries exist.
     */
    ValidateXLOGDirectoryStructure();

    /* Set up timeout handler needed to report startup progress. */
    if !IsBootstrapProcessingMode() {
        RegisterTimeout(
            STARTUP_PROGRESS_TIMEOUT,
            startup_progress_timeout_handler,
        );
    }

    /*
     * If we previously crashed, perform cleanup actions.
     */
    if (*ControlFile).state != DB_SHUTDOWNED
        && (*ControlFile).state != DB_SHUTDOWNED_IN_RECOVERY
    {
        RemoveTempXlogFiles();
        SyncDataDirectory();
        didCrash = true;
    } else {
        didCrash = false;
    }

    /*
     * Prepare for WAL recovery if needed.
     */
    InitWalRecovery(
        ControlFile,
        &mut wasShutdown,
        &mut haveBackupLabel,
        &mut haveTblspcMap,
    );
    checkPoint = (*ControlFile).checkPointCopy;

    /* initialize shared memory variables from the checkpoint record */
    (*TransamVariables).nextXid = checkPoint.nextXid;
    (*TransamVariables).nextOid = checkPoint.nextOid;
    (*TransamVariables).oidCount = 0;
    MultiXactSetNextMXact(checkPoint.nextMulti, checkPoint.nextMultiOffset);
    AdvanceOldestClogXid(checkPoint.oldestXid);
    SetTransactionIdLimit(checkPoint.oldestXid, checkPoint.oldestXidDB);
    SetMultiXactIdLimit(checkPoint.oldestMulti, checkPoint.oldestMultiDB, true);
    SetCommitTsLimit(checkPoint.oldestCommitTsXid, checkPoint.newestCommitTsXid);
    (*XLogCtl).ckptFullXid = checkPoint.nextXid;

    /*
     * Clear out any old relcache cache files.
     */
    RelationCacheInitFileRemove();

    /*
     * Initialize replication slots, before there's a chance to remove
     * required resources.
     */
    StartupReplicationSlots();

    /*
     * Startup logical state, needs to be setup now so we have proper data
     * during crash recovery.
     */
    StartupReorderBuffer();

    /*
     * Startup CLOG.
     */
    StartupCLOG();

    /*
     * Startup MultiXact.
     */
    StartupMultiXact();

    /*
     * Ditto for commit timestamps.
     */
    if (*ControlFile).track_commit_timestamp {
        StartupCommitTs();
    }

    /*
     * Recover knowledge about replay progress of known replication partners.
     */
    StartupReplicationOrigin();

    /*
     * Initialize unlogged LSN.
     */
    if (*ControlFile).state == DB_SHUTDOWNED {
        pg_atomic_write_membarrier_u64(&mut (*XLogCtl).unloggedLSN, (*ControlFile).unloggedLSN);
    } else {
        pg_atomic_write_membarrier_u64(&mut (*XLogCtl).unloggedLSN, FirstNormalUnloggedLSN);
    }

    /*
     * Copy any missing timeline history files between 'now' and the recovery
     * target timeline from archive to pg_wal.
     */
    restoreTimeLineHistoryFiles(checkPoint.ThisTimeLineID, recoveryTargetTLI);

    /*
     * Before running in recovery, scan pg_twophase and fill in its status.
     */
    restoreTwoPhaseData();

    /*
     * When starting with crash recovery, reset pgstat data - it might not be
     * valid.
     */
    if didCrash {
        pgstat_discard_stats();
    } else {
        pgstat_restore_stats();
    }

    lastFullPageWrites = checkPoint.fullPageWrites;

    RedoRecPtr = (*XLogCtl).RedoRecPtr;
    (*XLogCtl).Insert.RedoRecPtr = checkPoint.redo;
    (*XLogCtl).RedoRecPtr = checkPoint.redo;
    RedoRecPtr = checkPoint.redo;
    doPageWrites = lastFullPageWrites;

    /* REDO */
    if InRecovery {
        /* Initialize state for RecoveryInProgress() */
        SpinLockAcquire(&mut (*XLogCtl).info_lck);
        if InArchiveRecovery {
            (*XLogCtl).SharedRecoveryState = RECOVERY_STATE_ARCHIVE;
        } else {
            (*XLogCtl).SharedRecoveryState = RECOVERY_STATE_CRASH;
        }
        SpinLockRelease(&mut (*XLogCtl).info_lck);

        /*
         * Update pg_control to show that we are recovering.
         */
        UpdateControlFile();

        /*
         * If there was a backup label file, it's done its job.
         */
        if haveBackupLabel {
            libc::unlink(BACKUP_LABEL_OLD.as_ptr() as *const c_char);
            durable_rename(
                BACKUP_LABEL_FILE.as_ptr() as *const c_char,
                BACKUP_LABEL_OLD.as_ptr() as *const c_char,
                FATAL,
            );
        }

        /*
         * If there was a tablespace_map file, it's done its job.
         */
        if haveTblspcMap {
            libc::unlink(TABLESPACE_MAP_OLD.as_ptr() as *const c_char);
            durable_rename(
                TABLESPACE_MAP.as_ptr() as *const c_char,
                TABLESPACE_MAP_OLD.as_ptr() as *const c_char,
                FATAL,
            );
        }

        /*
         * Initialize our local copy of minRecoveryPoint.
         */
        if InArchiveRecovery {
            LocalMinRecoveryPoint = (*ControlFile).minRecoveryPoint;
            LocalMinRecoveryPointTLI = (*ControlFile).minRecoveryPointTLI;
        } else {
            LocalMinRecoveryPoint = InvalidXLogRecPtr;
            LocalMinRecoveryPointTLI = 0;
        }

        /* Check that the GUCs used to generate the WAL allow recovery */
        CheckRequiredParameterValues();

        /*
         * We're in recovery, so unlogged relations may be trashed and must be
         * reset.
         */
        ResetUnloggedRelations(UNLOGGED_RELATION_CLEANUP);

        /*
         * Likewise, delete any saved transaction snapshot files.
         */
        DeleteAllExportedSnapshotFiles();

        /*
         * Initialize for Hot Standby, if enabled.
         */
        if ArchiveRecoveryRequested && EnableHotStandby {
            let mut xids: *mut TransactionId = ptr::null_mut();
            let mut nxids: c_int = 0;

            elog!(DEBUG1, "initializing for hot standby");

            InitRecoveryTransactionEnvironment();

            if wasShutdown {
                oldestActiveXID = PrescanPreparedTransactions(&mut xids, &mut nxids);
            } else {
                oldestActiveXID = checkPoint.oldestActiveXid;
            }
            assert!(TransactionIdIsValid(oldestActiveXID));

            /* Tell procarray about the range of xids it has to deal with */
            ProcArrayInitRecovery(XidFromFullTransactionId((*TransamVariables).nextXid));

            /*
             * Startup subtrans only.
             */
            StartupSUBTRANS(oldestActiveXID);

            /*
             * If we're beginning at a shutdown checkpoint, fake-up an empty
             * running-xacts record.
             */
            if wasShutdown {
                let mut running: RunningTransactionsData = core::mem::zeroed();
                let mut latestCompletedXid: TransactionId;

                /* Update pg_subtrans entries for any prepared transactions */
                StandbyRecoverPreparedTransactions();

                running.xcnt = nxids;
                running.subxcnt = 0;
                running.subxid_status = SUBXIDS_IN_SUBTRANS;
                running.nextXid = XidFromFullTransactionId(checkPoint.nextXid);
                running.oldestRunningXid = oldestActiveXID;
                latestCompletedXid = XidFromFullTransactionId(checkPoint.nextXid);
                TransactionIdRetreat(&mut latestCompletedXid);
                assert!(TransactionIdIsNormal(latestCompletedXid));
                running.latestCompletedXid = latestCompletedXid;
                running.xids = xids;

                ProcArrayApplyRecoveryInfo(&mut running);
            }
        }

        /*
         * We're all set for replaying the WAL now. Do it.
         */
        PerformWalRecovery();
        performedWalRecovery = true;
    } else {
        performedWalRecovery = false;
    }

    /*
     * Finish WAL recovery.
     */
    endOfRecoveryInfo = FinishWalRecovery();
    EndOfLog = (*endOfRecoveryInfo).endOfLog;
    EndOfLogTLI = (*endOfRecoveryInfo).endOfLogTLI;
    abortedRecPtr = (*endOfRecoveryInfo).abortedRecPtr;
    missingContrecPtr = (*endOfRecoveryInfo).missingContrecPtr;

    /*
     * Reset ps status display.
     */
    set_ps_display(b"\0".as_ptr() as *const c_char);

    /*
     * When recovering from a backup, complain if we did not roll forward far
     * enough to reach the point where the database is consistent.
     */
    if InRecovery
        && (EndOfLog < LocalMinRecoveryPoint
            || !XLogRecPtrIsInvalid((*ControlFile).backupStartPoint))
    {
        if ArchiveRecoveryRequested || (*ControlFile).backupEndRequired {
            if !XLogRecPtrIsInvalid((*ControlFile).backupStartPoint)
                || (*ControlFile).backupEndRequired
            {
                ereport!(FATAL, errmsg!("WAL ends before end of online backup"));
                /* errcode + errhint also in C */
            } else {
                ereport!(FATAL, errmsg!("WAL ends before consistent recovery point"));
                /* errcode also in C */
            }
        }
    }

    /*
     * Reset unlogged relations to the contents of their INIT fork.
     */
    if InRecovery {
        ResetUnloggedRelations(UNLOGGED_RELATION_INIT);
    }

    /*
     * Pre-scan prepared transactions.
     */
    oldestActiveXID = PrescanPreparedTransactions(ptr::null_mut(), ptr::null_mut());

    /*
     * Allow ordinary WAL segment creation before possibly switching to a new
     * timeline.
     */
    SetInstallXLogFileSegmentActive();

    /*
     * Consider whether we need to assign a new timeline ID.
     */
    newTLI = (*endOfRecoveryInfo).lastRecTLI;
    if ArchiveRecoveryRequested {
        newTLI = findNewestTimeLine(recoveryTargetTLI) + 1;
        ereport!(LOG, errmsg!("selected new timeline ID: {}", newTLI));

        /*
         * Make a writable copy of the last WAL segment.
         */
        XLogInitNewTimeline(EndOfLogTLI, EndOfLog, newTLI);

        /*
         * Remove the signal files out of the way.
         */
        if (*endOfRecoveryInfo).standby_signal_file_found {
            durable_unlink(STANDBY_SIGNAL_FILE.as_ptr() as *const c_char, FATAL);
        }
        if (*endOfRecoveryInfo).recovery_signal_file_found {
            durable_unlink(RECOVERY_SIGNAL_FILE.as_ptr() as *const c_char, FATAL);
        }

        /*
         * Write the timeline history file.
         */
        writeTimeLineHistory(
            newTLI,
            recoveryTargetTLI,
            EndOfLog,
            (*endOfRecoveryInfo).recoveryStopReason,
        );

        ereport!(LOG, errmsg!("archive recovery complete"));
    }

    /* Save the selected TimeLineID in shared memory */
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).InsertTimeLineID = newTLI;
    (*XLogCtl).PrevTimeLineID = (*endOfRecoveryInfo).lastRecTLI;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    /*
     * Actually, if WAL ended in an incomplete record, skip the parts that
     * made it through.
     */
    if !XLogRecPtrIsInvalid(missingContrecPtr) {
        assert!(newTLI == (*endOfRecoveryInfo).lastRecTLI);
        assert!(!XLogRecPtrIsInvalid(abortedRecPtr));
        EndOfLog = missingContrecPtr;
    }

    /*
     * Prepare to write WAL starting at EndOfLog location.
     */
    Insert = &mut (*XLogCtl).Insert;
    (*Insert).PrevBytePos = XLogRecPtrToBytePos((*endOfRecoveryInfo).lastRec);
    (*Insert).CurrBytePos = XLogRecPtrToBytePos(EndOfLog);

    /*
     * Tricky point here: lastPage contains the *last* block that the LastRec
     * record spans.
     */
    if EndOfLog % XLOG_BLCKSZ as u64 != 0 {
        let firstIdx = XLogRecPtrToBufIdx(EndOfLog);
        let len = (EndOfLog - (*endOfRecoveryInfo).lastPageBeginPtr) as usize;
        assert!(len < XLOG_BLCKSZ);

        /* Copy the valid part of the last block, and zero the rest */
        let page = (*XLogCtl).pages.add(firstIdx as usize * XLOG_BLCKSZ);
        ptr::copy_nonoverlapping((*endOfRecoveryInfo).lastPage as *const u8, page as *mut u8, len);
        ptr::write_bytes((page as *mut u8).add(len), 0, XLOG_BLCKSZ - len);

        pg_atomic_write_u64(
            &mut *(*XLogCtl).xlblocks.add(firstIdx as usize),
            (*endOfRecoveryInfo).lastPageBeginPtr + XLOG_BLCKSZ as u64,
        );
        (*XLogCtl).InitializedUpTo =
            (*endOfRecoveryInfo).lastPageBeginPtr + XLOG_BLCKSZ as u64;
    } else {
        /*
         * There is no partial block to copy.
         */
        (*XLogCtl).InitializedUpTo = EndOfLog;
    }

    /*
     * Update local and shared status.
     */
    LogwrtResult.Write = EndOfLog;
    LogwrtResult.Flush = EndOfLog;
    pg_atomic_write_u64(&mut (*XLogCtl).logInsertResult, EndOfLog);
    pg_atomic_write_u64(&mut (*XLogCtl).logWriteResult, EndOfLog);
    pg_atomic_write_u64(&mut (*XLogCtl).logFlushResult, EndOfLog);
    (*XLogCtl).LogwrtRqst.Write = EndOfLog;
    (*XLogCtl).LogwrtRqst.Flush = EndOfLog;

    /*
     * Preallocate additional log files, if wanted.
     */
    PreallocXlogFiles(EndOfLog, newTLI);

    /*
     * Okay, we're officially UP.
     */
    InRecovery = false;

    /* start the archive_timeout timer and LSN running */
    (*XLogCtl).lastSegSwitchTime = libc::time(ptr::null_mut()) as pg_time_t;
    (*XLogCtl).lastSegSwitchLSN = EndOfLog;

    /* also initialize latestCompletedXid, to nextXid - 1 */
    LWLockAcquire(ProcArrayLock as *mut LWLock, LW_EXCLUSIVE);
    (*TransamVariables).latestCompletedXid = (*TransamVariables).nextXid;
    FullTransactionIdRetreat(&mut (*TransamVariables).latestCompletedXid);
    LWLockRelease(ProcArrayLock as *mut LWLock);

    /*
     * Start up subtrans, if not already done for hot standby.
     */
    if standbyState == STANDBY_DISABLED {
        StartupSUBTRANS(oldestActiveXID);
    }

    /*
     * Perform end of recovery actions for any SLRUs that need it.
     */
    TrimCLOG();
    TrimMultiXact();

    /*
     * Reload shared-memory state for prepared transactions.
     */
    RecoverPreparedTransactions();

    /* Shut down xlogreader */
    ShutdownWalRecovery();

    /* Enable WAL writes for this backend only. */
    LocalSetXLogInsertAllowed();

    /* If necessary, write overwrite-contrecord before doing anything else */
    if !XLogRecPtrIsInvalid(abortedRecPtr) {
        assert!(!XLogRecPtrIsInvalid(missingContrecPtr));
        CreateOverwriteContrecordRecord(abortedRecPtr, missingContrecPtr, newTLI);
    }

    /*
     * Update full_page_writes in shared memory and write an XLOG_FPW_CHANGE
     * record.
     */
    (*Insert).fullPageWrites = lastFullPageWrites;
    UpdateFullPageWrites();

    /*
     * Emit checkpoint or end-of-recovery record in XLOG, if required.
     */
    if performedWalRecovery {
        promoted = PerformRecoveryXLogAction();
    }

    /*
     * If any of the critical GUCs have changed, log them before we allow
     * backends to write WAL.
     */
    XLogReportParameters();

    /* If this is archive recovery, perform post-recovery cleanup actions. */
    if ArchiveRecoveryRequested {
        CleanupAfterArchiveRecovery(EndOfLogTLI, EndOfLog, newTLI);
    }

    /*
     * Local WAL inserts enabled, so it's time to finish initialization of
     * commit timestamp.
     */
    CompleteCommitTsInitialization();

    /*
     * All done with end-of-recovery actions.
     */
    LWLockAcquire(ControlFileLock as *mut LWLock, LW_EXCLUSIVE);
    (*ControlFile).state = DB_IN_PRODUCTION;

    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).SharedRecoveryState = RECOVERY_STATE_DONE;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    UpdateControlFile();
    LWLockRelease(ControlFileLock as *mut LWLock);

    /*
     * Shutdown the recovery environment.
     */
    if standbyState != STANDBY_DISABLED {
        ShutdownRecoveryTransactionEnvironment();
    }

    /*
     * If there were cascading standby servers connected to us, nudge any wal
     * sender processes.
     */
    WalSndWakeup(true, true);

    /*
     * If this was a promotion, request an (online) checkpoint now.
     */
    if promoted {
        RequestCheckpoint(CHECKPOINT_FORCE);
    }
}

/*
 * Callback from PerformWalRecovery(), called when we switch from crash
 * recovery to archive recovery mode.
 */
pub unsafe fn SwitchIntoArchiveRecovery(EndRecPtr: XLogRecPtr, replayTLI: TimeLineID) {
    /* initialize minRecoveryPoint to this record */
    LWLockAcquire(ControlFileLock as *mut LWLock, LW_EXCLUSIVE);
    (*ControlFile).state = DB_IN_ARCHIVE_RECOVERY;
    if (*ControlFile).minRecoveryPoint < EndRecPtr {
        (*ControlFile).minRecoveryPoint = EndRecPtr;
        (*ControlFile).minRecoveryPointTLI = replayTLI;
    }
    /* update local copy */
    LocalMinRecoveryPoint = (*ControlFile).minRecoveryPoint;
    LocalMinRecoveryPointTLI = (*ControlFile).minRecoveryPointTLI;

    /*
     * The startup process can update its local copy of minRecoveryPoint from
     * this point.
     */
    updateMinRecoveryPoint = true;

    UpdateControlFile();

    /*
     * We update SharedRecoveryState while holding the lock on ControlFileLock
     * so both states are consistent in shared memory.
     */
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).SharedRecoveryState = RECOVERY_STATE_ARCHIVE;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    LWLockRelease(ControlFileLock as *mut LWLock);
}

/*
 * Callback from PerformWalRecovery(), called when we reach the end of backup.
 */
pub unsafe fn ReachedEndOfBackup(EndRecPtr: XLogRecPtr, tli: TimeLineID) {
    LWLockAcquire(ControlFileLock as *mut LWLock, LW_EXCLUSIVE);

    if (*ControlFile).minRecoveryPoint < EndRecPtr {
        (*ControlFile).minRecoveryPoint = EndRecPtr;
        (*ControlFile).minRecoveryPointTLI = tli;
    }

    (*ControlFile).backupStartPoint = InvalidXLogRecPtr;
    (*ControlFile).backupEndPoint = InvalidXLogRecPtr;
    (*ControlFile).backupEndRequired = false;
    UpdateControlFile();

    LWLockRelease(ControlFileLock as *mut LWLock);
}

/*
 * Perform whatever XLOG actions are necessary at end of REDO.
 */
unsafe fn PerformRecoveryXLogAction() -> bool {
    let mut promoted: bool = false;

    /*
     * Perform a checkpoint to update all our recovery activity to disk.
     */
    if ArchiveRecoveryRequested && IsUnderPostmaster && PromoteIsTriggered() {
        promoted = true;

        /*
         * Insert a special WAL record to mark the end of recovery.
         */
        CreateEndOfRecoveryRecord();
    } else {
        RequestCheckpoint(
            CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_IMMEDIATE | CHECKPOINT_WAIT,
        );
    }

    promoted
}

/*
 * Is the system still in recovery?
 */
pub unsafe fn RecoveryInProgress() -> bool {
    if !LocalRecoveryInProgress {
        return false;
    }

    /*
     * use volatile pointer to make sure we make a fresh read of the
     * shared variable.
     */
    let xlogctl = XLogCtl as *volatile XLogCtlData;
    LocalRecoveryInProgress =
        (*xlogctl).SharedRecoveryState != RECOVERY_STATE_DONE;

    LocalRecoveryInProgress
}

/*
 * Returns current recovery state from shared memory.
 */
pub unsafe fn GetRecoveryState() -> RecoveryState {
    let retval: RecoveryState;
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    retval = (*XLogCtl).SharedRecoveryState;
    SpinLockRelease(&mut (*XLogCtl).info_lck);
    retval
}

/*
 * Is this process allowed to insert new WAL records?
 */
pub unsafe fn XLogInsertAllowed() -> bool {
    /*
     * If value is "unconditionally true" or "unconditionally false", just
     * return it.
     */
    if LocalXLogInsertAllowed >= 0 {
        return LocalXLogInsertAllowed != 0;
    }

    /*
     * Else, must check to see if we're still in recovery.
     */
    if RecoveryInProgress() {
        return false;
    }

    /*
     * On exit from recovery, reset to "unconditionally true".
     */
    LocalXLogInsertAllowed = 1;
    true
}

/*
 * Make XLogInsertAllowed() return true in the current process only.
 *
 * Returns the previous value of LocalXLogInsertAllowed.
 */
unsafe fn LocalSetXLogInsertAllowed() -> c_int {
    let oldXLogAllowed = LocalXLogInsertAllowed;
    LocalXLogInsertAllowed = 1;
    oldXLogAllowed
}

/*
 * Return the current Redo pointer from shared memory.
 */
pub unsafe fn GetRedoRecPtr() -> XLogRecPtr {
    let ptr: XLogRecPtr;

    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    ptr = (*XLogCtl).RedoRecPtr;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    if RedoRecPtr < ptr {
        RedoRecPtr = ptr;
    }

    RedoRecPtr
}

/*
 * Return information needed to decide whether a modified block needs a
 * full-page image.
 */
pub unsafe fn GetFullPageWriteInfo(RedoRecPtr_p: *mut XLogRecPtr, doPageWrites_p: *mut bool) {
    *RedoRecPtr_p = RedoRecPtr;
    *doPageWrites_p = doPageWrites;
}

/*
 * GetInsertRecPtr -- Returns the current insert position.
 *
 * NOTE: The value *actually* returned is the position of the last full
 * xlog page.
 */
pub unsafe fn GetInsertRecPtr() -> XLogRecPtr {
    let recptr: XLogRecPtr;
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    recptr = (*XLogCtl).LogwrtRqst.Write;
    SpinLockRelease(&mut (*XLogCtl).info_lck);
    recptr
}

/*
 * GetFlushRecPtr -- Returns the current flush position.
 */
pub unsafe fn GetFlushRecPtr(insertTLI: *mut TimeLineID) -> XLogRecPtr {
    assert!((*XLogCtl).SharedRecoveryState == RECOVERY_STATE_DONE);

    RefreshXLogWriteResult(&mut LogwrtResult);

    /*
     * If we're writing and flushing WAL, the time line can't be changing, so
     * no lock is required.
     */
    if !insertTLI.is_null() {
        *insertTLI = (*XLogCtl).InsertTimeLineID;
    }

    LogwrtResult.Flush
}

/*
 * GetWALInsertionTimeLine -- Returns the current timeline of a system that
 * is not in recovery.
 */
pub unsafe fn GetWALInsertionTimeLine() -> TimeLineID {
    assert!((*XLogCtl).SharedRecoveryState == RECOVERY_STATE_DONE);
    /* Since the value can't be changing, no lock is required. */
    (*XLogCtl).InsertTimeLineID
}

/*
 * GetWALInsertionTimeLineIfSet -- If the system is not in recovery, returns
 * the WAL insertion timeline; else, returns 0.
 */
pub unsafe fn GetWALInsertionTimeLineIfSet() -> TimeLineID {
    let insertTLI: TimeLineID;
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    insertTLI = (*XLogCtl).InsertTimeLineID;
    SpinLockRelease(&mut (*XLogCtl).info_lck);
    insertTLI
}

/*
 * GetLastImportantRecPtr -- Returns the LSN of the last important record
 * inserted.
 */
pub unsafe fn GetLastImportantRecPtr() -> XLogRecPtr {
    let mut res: XLogRecPtr = InvalidXLogRecPtr;

    for i in 0..NUM_XLOGINSERT_LOCKS as usize {
        /*
         * Need to take a lock to prevent torn reads of the LSN.
         */
        LWLockAcquire(&mut (*WALInsertLocks.add(i)).l.lock, LW_EXCLUSIVE);
        let last_important = (*WALInsertLocks.add(i)).l.lastImportantAt;
        LWLockRelease(&mut (*WALInsertLocks.add(i)).l.lock);

        if res < last_important {
            res = last_important;
        }
    }

    res
}

/*
 * Get the time and LSN of the last xlog segment switch
 */
pub unsafe fn GetLastSegSwitchData(lastSwitchLSN: *mut XLogRecPtr) -> pg_time_t {
    let result: pg_time_t;

    /* Need WALWriteLock, but shared lock is sufficient */
    LWLockAcquire(WALWriteLock as *mut LWLock, LW_SHARED);
    result = (*XLogCtl).lastSegSwitchTime;
    *lastSwitchLSN = (*XLogCtl).lastSegSwitchLSN;
    LWLockRelease(WALWriteLock as *mut LWLock);

    result
}

/*
 * This must be called ONCE during postmaster or standalone-backend shutdown
 */
pub unsafe extern "C" fn ShutdownXLOG(code: c_int, arg: Datum) {
    assert!(!AuxProcessResourceOwner.is_null());
    assert!(
        CurrentResourceOwner.is_null()
            || CurrentResourceOwner == AuxProcessResourceOwner
    );
    CurrentResourceOwner = AuxProcessResourceOwner;

    /* Don't be chatty in standalone mode */
    ereport!(
        if IsPostmasterEnvironment { LOG } else { NOTICE },
        errmsg!("shutting down")
    );

    /*
     * Signal walsenders to move to stopping state.
     */
    WalSndInitStopping();

    /*
     * Wait for WAL senders to be in stopping state.
     */
    WalSndWaitStopping();

    if RecoveryInProgress() {
        CreateRestartPoint(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE);
    } else {
        /*
         * If archiving is enabled, rotate the last XLOG file.
         */
        if XLogArchivingActive() {
            RequestXLogSwitch(false);
        }
        CreateCheckPoint(CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_IMMEDIATE);
    }
}

/*
 * Log start of a checkpoint.
 */
unsafe fn LogCheckpointStart(flags: c_int, restartpoint: bool) {
    if restartpoint {
        /* translator: the placeholders show checkpoint options */
        ereport!(
            LOG,
            errmsg!(
                "restartpoint starting:{}{}{}{}{}{}{}{}",
                if flags & CHECKPOINT_IS_SHUTDOWN != 0 { " shutdown" } else { "" },
                if flags & CHECKPOINT_END_OF_RECOVERY != 0 { " end-of-recovery" } else { "" },
                if flags & CHECKPOINT_IMMEDIATE != 0 { " immediate" } else { "" },
                if flags & CHECKPOINT_FORCE != 0 { " force" } else { "" },
                if flags & CHECKPOINT_WAIT != 0 { " wait" } else { "" },
                if flags & CHECKPOINT_CAUSE_XLOG != 0 { " wal" } else { "" },
                if flags & CHECKPOINT_CAUSE_TIME != 0 { " time" } else { "" },
                if flags & CHECKPOINT_FLUSH_ALL != 0 { " flush-all" } else { "" }
            )
        );
    } else {
        /* translator: the placeholders show checkpoint options */
        ereport!(
            LOG,
            errmsg!(
                "checkpoint starting:{}{}{}{}{}{}{}{}",
                if flags & CHECKPOINT_IS_SHUTDOWN != 0 { " shutdown" } else { "" },
                if flags & CHECKPOINT_END_OF_RECOVERY != 0 { " end-of-recovery" } else { "" },
                if flags & CHECKPOINT_IMMEDIATE != 0 { " immediate" } else { "" },
                if flags & CHECKPOINT_FORCE != 0 { " force" } else { "" },
                if flags & CHECKPOINT_WAIT != 0 { " wait" } else { "" },
                if flags & CHECKPOINT_CAUSE_XLOG != 0 { " wal" } else { "" },
                if flags & CHECKPOINT_CAUSE_TIME != 0 { " time" } else { "" },
                if flags & CHECKPOINT_FLUSH_ALL != 0 { " flush-all" } else { "" }
            )
        );
    }
}

/*
 * Log end of a checkpoint.
 */
unsafe fn LogCheckpointEnd(restartpoint: bool) {
    let write_msecs: i64;
    let sync_msecs: i64;
    let total_msecs: i64;
    let longest_msecs: i64;
    let average_msecs: i64;
    let average_sync_time: u64;

    CheckpointStats.ckpt_end_t = GetCurrentTimestamp();

    write_msecs = TimestampDifferenceMilliseconds(
        CheckpointStats.ckpt_write_t,
        CheckpointStats.ckpt_sync_t,
    );

    sync_msecs = TimestampDifferenceMilliseconds(
        CheckpointStats.ckpt_sync_t,
        CheckpointStats.ckpt_sync_end_t,
    );

    /* Accumulate checkpoint timing summary data, in milliseconds. */
    PendingCheckpointerStats.write_time += write_msecs;
    PendingCheckpointerStats.sync_time += sync_msecs;

    /*
     * All of the published timing statistics are accounted for.  Only
     * continue if a log message is to be written.
     */
    if !log_checkpoints {
        return;
    }

    total_msecs = TimestampDifferenceMilliseconds(
        CheckpointStats.ckpt_start_t,
        CheckpointStats.ckpt_end_t,
    );

    /*
     * Timing values returned from CheckpointStats are in microseconds.
     * Convert to milliseconds for consistent printing.
     */
    longest_msecs = (CheckpointStats.ckpt_longest_sync + 999) / 1000;

    average_sync_time = 0;
    let mut average_sync_time_inner = 0u64;
    if CheckpointStats.ckpt_sync_rels > 0 {
        average_sync_time_inner = CheckpointStats.ckpt_agg_sync_time
            / CheckpointStats.ckpt_sync_rels as u64;
    }
    average_msecs = (average_sync_time_inner as i64 + 999) / 1000;

    /*
     * ControlFileLock is not required to see ControlFile->checkPoint and
     * ->checkPointCopy here as we are the only updator of those variables.
     */
    let (chkpt_hi, chkpt_lo) = LSN_FORMAT_ARGS((*ControlFile).checkPoint);
    let (redo_hi, redo_lo) = LSN_FORMAT_ARGS((*ControlFile).checkPointCopy.redo);
    if restartpoint {
        ereport!(
            LOG,
            errmsg!(
                "restartpoint complete: wrote {} buffers ({:.1}%), \
                 wrote {} SLRU buffers; {} WAL file(s) added, \
                 {} removed, {} recycled; write={}.{:03} s, \
                 sync={}.{:03} s, total={}.{:03} s; sync files={}, \
                 longest={}.{:03} s, average={}.{:03} s; distance={} kB, \
                 estimate={} kB; lsn={}/{}, redo lsn={}/{}",
                CheckpointStats.ckpt_bufs_written,
                CheckpointStats.ckpt_bufs_written as f64 * 100.0 / NBuffers as f64,
                CheckpointStats.ckpt_slru_written,
                CheckpointStats.ckpt_segs_added,
                CheckpointStats.ckpt_segs_removed,
                CheckpointStats.ckpt_segs_recycled,
                write_msecs / 1000, write_msecs % 1000,
                sync_msecs / 1000, sync_msecs % 1000,
                total_msecs / 1000, total_msecs % 1000,
                CheckpointStats.ckpt_sync_rels,
                longest_msecs / 1000, longest_msecs % 1000,
                average_msecs / 1000, average_msecs % 1000,
                (PrevCheckPointDistance / 1024.0) as i64,
                (CheckPointDistanceEstimate / 1024.0) as i64,
                chkpt_hi, chkpt_lo, redo_hi, redo_lo
            )
        );
    } else {
        ereport!(
            LOG,
            errmsg!(
                "checkpoint complete: wrote {} buffers ({:.1}%), \
                 wrote {} SLRU buffers; {} WAL file(s) added, \
                 {} removed, {} recycled; write={}.{:03} s, \
                 sync={}.{:03} s, total={}.{:03} s; sync files={}, \
                 longest={}.{:03} s, average={}.{:03} s; distance={} kB, \
                 estimate={} kB; lsn={}/{}, redo lsn={}/{}",
                CheckpointStats.ckpt_bufs_written,
                CheckpointStats.ckpt_bufs_written as f64 * 100.0 / NBuffers as f64,
                CheckpointStats.ckpt_slru_written,
                CheckpointStats.ckpt_segs_added,
                CheckpointStats.ckpt_segs_removed,
                CheckpointStats.ckpt_segs_recycled,
                write_msecs / 1000, write_msecs % 1000,
                sync_msecs / 1000, sync_msecs % 1000,
                total_msecs / 1000, total_msecs % 1000,
                CheckpointStats.ckpt_sync_rels,
                longest_msecs / 1000, longest_msecs % 1000,
                average_msecs / 1000, average_msecs % 1000,
                (PrevCheckPointDistance / 1024.0) as i64,
                (CheckPointDistanceEstimate / 1024.0) as i64,
                chkpt_hi, chkpt_lo, redo_hi, redo_lo
            )
        );
    }
}

/*
 * Update the estimate of distance between checkpoints.
 */
unsafe fn UpdateCheckPointDistanceEstimate(nbytes: u64) {
    PrevCheckPointDistance = nbytes as f64;
    if CheckPointDistanceEstimate < nbytes as f64 {
        CheckPointDistanceEstimate = nbytes as f64;
    } else {
        CheckPointDistanceEstimate =
            0.90 * CheckPointDistanceEstimate + 0.10 * nbytes as f64;
    }
}

/*
 * Update the ps display for a process running a checkpoint.  Note that
 * this routine should not do any allocations so as it can be called
 * from a critical section.
 */
unsafe fn update_checkpoint_display(flags: c_int, restartpoint: bool, reset: bool) {
    /*
     * The status is reported only for end-of-recovery and shutdown
     * checkpoints or shutdown restartpoints.
     */
    if (flags & (CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_IS_SHUTDOWN)) == 0 {
        return;
    }

    if reset {
        set_ps_display(b"\0".as_ptr() as *const c_char);
    } else {
        let mut activitymsg = [0u8; 128];
        libc::snprintf(
            activitymsg.as_mut_ptr() as *mut c_char,
            128,
            b"performing %s%s%s\0".as_ptr() as *const c_char,
            if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
                b"end-of-recovery \0".as_ptr() as *const c_char
            } else {
                b"\0".as_ptr() as *const c_char
            },
            if flags & CHECKPOINT_IS_SHUTDOWN != 0 {
                b"shutdown \0".as_ptr() as *const c_char
            } else {
                b"\0".as_ptr() as *const c_char
            },
            if restartpoint {
                b"restartpoint\0".as_ptr() as *const c_char
            } else {
                b"checkpoint\0".as_ptr() as *const c_char
            },
        );
        set_ps_display(activitymsg.as_ptr() as *const c_char);
    }
}


/*
 * Perform a checkpoint --- either during shutdown, or on-the-fly
 *
 * Returns true if a new checkpoint was performed, or false if it was skipped
 * because the system was idle.
 */
pub unsafe fn CreateCheckPoint(flags: c_int) -> bool {
    let shutdown: bool;
    let mut checkPoint: CheckPoint = core::mem::zeroed();
    let mut recptr: XLogRecPtr = 0;
    let mut _logSegNo: XLogSegNo = 0;
    let Insert: *mut XLogCtlInsert = &mut (*XLogCtl).Insert;
    let mut freespace: uint32;
    let mut PriorRedoPtr: XLogRecPtr;
    let last_important_lsn: XLogRecPtr;
    let mut vxids: *mut VirtualTransactionId;
    let mut nvxids: c_int = 0;
    let mut oldXLogAllowed: c_int = 0;

    /*
     * An end-of-recovery checkpoint is really a shutdown checkpoint.
     */
    shutdown = (flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY)) != 0;

    /* sanity check */
    if RecoveryInProgress() && (flags & CHECKPOINT_END_OF_RECOVERY) == 0 {
        elog!(ERROR, "can't create a checkpoint during recovery");
    }

    /*
     * Prepare to accumulate statistics.
     */
    MemSet(
        &mut CheckpointStats as *mut CheckpointStatsData as *mut c_void,
        0,
        core::mem::size_of::<CheckpointStatsData>(),
    );
    CheckpointStats.ckpt_start_t = GetCurrentTimestamp();

    /*
     * Let smgr prepare for checkpoint.
     */
    SyncPreCheckpoint();

    /*
     * Use a critical section to force system panic if we have trouble.
     */
    START_CRIT_SECTION!();

    if shutdown {
        LWLockAcquire(ControlFileLock as *mut LWLock, LW_EXCLUSIVE);
        (*ControlFile).state = DB_SHUTDOWNING;
        UpdateControlFile();
        LWLockRelease(ControlFileLock as *mut LWLock);
    }

    /* Begin filling in the checkpoint WAL record */
    MemSet(
        &mut checkPoint as *mut CheckPoint as *mut c_void,
        0,
        core::mem::size_of::<CheckPoint>(),
    );
    checkPoint.time = libc::time(ptr::null_mut()) as pg_time_t;

    /*
     * For Hot Standby, derive the oldestActiveXid before we fix the redo pointer.
     */
    if !shutdown && XLogStandbyInfoActive() {
        checkPoint.oldestActiveXid = GetOldestActiveTransactionId();
    } else {
        checkPoint.oldestActiveXid = InvalidTransactionId;
    }

    /*
     * Get location of last important record.
     */
    last_important_lsn = GetLastImportantRecPtr();

    /*
     * If this isn't a shutdown or forced checkpoint, and no WAL activity,
     * skip it.
     */
    if (flags & (CHECKPOINT_IS_SHUTDOWN | CHECKPOINT_END_OF_RECOVERY | CHECKPOINT_FORCE)) == 0 {
        if last_important_lsn == (*ControlFile).checkPoint {
            END_CRIT_SECTION!();
            elog!(DEBUG1, "checkpoint skipped because system is idle");
            return false;
        }
    }

    /*
     * An end-of-recovery checkpoint is created before anyone is allowed to
     * write WAL.
     */
    if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
        oldXLogAllowed = LocalSetXLogInsertAllowed();
    }

    checkPoint.ThisTimeLineID = (*XLogCtl).InsertTimeLineID;
    if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
        checkPoint.PrevTimeLineID = (*XLogCtl).PrevTimeLineID;
    } else {
        checkPoint.PrevTimeLineID = checkPoint.ThisTimeLineID;
    }

    /*
     * We must block concurrent insertions while examining insert state.
     */
    WALInsertLockAcquireExclusive();

    checkPoint.fullPageWrites = (*Insert).fullPageWrites;
    checkPoint.wal_level = wal_level;

    if shutdown {
        let curInsert = XLogBytePosToRecPtr((*Insert).CurrBytePos);

        /*
         * Compute new REDO record ptr = location of next XLOG record.
         */
        freespace = INSERT_FREESPACE(curInsert);
        if freespace == 0 {
            if XLogSegmentOffset(curInsert, wal_segment_size as uint32) == 0 {
                let new = curInsert + SizeOfXLogLongPHD as XLogRecPtr;
                checkPoint.redo = new;
            } else {
                let new = curInsert + SizeOfXLogShortPHD as XLogRecPtr;
                checkPoint.redo = new;
            }
        } else {
            checkPoint.redo = curInsert;
        }

        /*
         * Here we update the shared RedoRecPtr for future XLogInsert calls.
         */
        RedoRecPtr = (*XLogCtl).Insert.RedoRecPtr;
        (*XLogCtl).Insert.RedoRecPtr = checkPoint.redo;
        RedoRecPtr = checkPoint.redo;
    }

    /*
     * Now we can release the WAL insertion locks.
     */
    WALInsertLockRelease();

    /*
     * If this is an online checkpoint, insert the special XLOG_CHECKPOINT_REDO
     * record.
     */
    if !shutdown {
        /* Include WAL level in record for WAL summarizer's benefit. */
        XLogBeginInsert();
        XLogRegisterData(&mut wal_level as *mut c_int as *mut c_char, core::mem::size_of::<c_int>());
        let _ = XLogInsert(RM_XLOG_ID, XLOG_CHECKPOINT_REDO);

        checkPoint.redo = RedoRecPtr;
    }

    /* Update the info_lck-protected copy of RedoRecPtr as well */
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).RedoRecPtr = checkPoint.redo;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    /*
     * If enabled, log checkpoint start.
     */
    if log_checkpoints {
        LogCheckpointStart(flags, false);
    }

    /* Update the process title */
    update_checkpoint_display(flags, false, false);

    // TRACE_POSTGRESQL_CHECKPOINT_START(flags)

    /*
     * Get the other info we need for the checkpoint record.
     */
    LWLockAcquire(XidGenLock as *mut LWLock, LW_SHARED);
    checkPoint.nextXid = (*TransamVariables).nextXid;
    checkPoint.oldestXid = (*TransamVariables).oldestXid;
    checkPoint.oldestXidDB = (*TransamVariables).oldestXidDB;
    LWLockRelease(XidGenLock as *mut LWLock);

    LWLockAcquire(CommitTsLock as *mut LWLock, LW_SHARED);
    checkPoint.oldestCommitTsXid = (*TransamVariables).oldestCommitTsXid;
    checkPoint.newestCommitTsXid = (*TransamVariables).newestCommitTsXid;
    LWLockRelease(CommitTsLock as *mut LWLock);

    LWLockAcquire(OidGenLock as *mut LWLock, LW_SHARED);
    checkPoint.nextOid = (*TransamVariables).nextOid;
    if !shutdown {
        checkPoint.nextOid += (*TransamVariables).oidCount;
    }
    LWLockRelease(OidGenLock as *mut LWLock);

    MultiXactGetCheckptMulti(
        shutdown,
        &mut checkPoint.nextMulti,
        &mut checkPoint.nextMultiOffset,
        &mut checkPoint.oldestMulti,
        &mut checkPoint.oldestMultiDB,
    );

    /*
     * Having constructed the checkpoint record, ensure all shmem disk buffers
     * and commit-log buffers are flushed to disk.
     */
    END_CRIT_SECTION!();

    /*
     * Wait for any backend currently in commit critical sections.
     */
    vxids = GetVirtualXIDsDelayingChkpt(&mut nvxids, DELAY_CHKPT_START);
    if nvxids > 0 {
        loop {
            /*
             * Keep absorbing fsync requests while we wait.
             */
            AbsorbSyncRequests();
            pgstat_report_wait_start(WAIT_EVENT_CHECKPOINT_DELAY_START);
            pg_usleep(10000);
            pgstat_report_wait_end();
            if !HaveVirtualXIDsDelayingChkpt(vxids, nvxids, DELAY_CHKPT_START) {
                break;
            }
        }
    }
    pfree(vxids as *mut c_void);

    CheckPointGuts(checkPoint.redo, flags);

    vxids = GetVirtualXIDsDelayingChkpt(&mut nvxids, DELAY_CHKPT_COMPLETE);
    if nvxids > 0 {
        loop {
            AbsorbSyncRequests();
            pgstat_report_wait_start(WAIT_EVENT_CHECKPOINT_DELAY_COMPLETE);
            pg_usleep(10000);
            pgstat_report_wait_end();
            if !HaveVirtualXIDsDelayingChkpt(vxids, nvxids, DELAY_CHKPT_COMPLETE) {
                break;
            }
        }
    }
    pfree(vxids as *mut c_void);

    /*
     * Take a snapshot of running transactions and write this to WAL.
     */
    if !shutdown && XLogStandbyInfoActive() {
        LogStandbySnapshot();
    }

    START_CRIT_SECTION!();

    /*
     * Now insert the checkpoint record into XLOG.
     */
    XLogBeginInsert();
    XLogRegisterData(
        &mut checkPoint as *mut CheckPoint as *mut c_char,
        core::mem::size_of::<CheckPoint>(),
    );
    recptr = XLogInsert(
        RM_XLOG_ID,
        if shutdown { XLOG_CHECKPOINT_SHUTDOWN } else { XLOG_CHECKPOINT_ONLINE },
    );

    XLogFlush(recptr);

    /*
     * We mustn't write any new WAL after a shutdown checkpoint.
     */
    if shutdown {
        if flags & CHECKPOINT_END_OF_RECOVERY != 0 {
            LocalXLogInsertAllowed = oldXLogAllowed;
        } else {
            LocalXLogInsertAllowed = 0; /* never again write WAL */
        }
    }

    if shutdown && checkPoint.redo != ProcLastRecPtr {
        ereport!(
            PANIC,
            errmsg!("concurrent write-ahead log activity while database system is shutting down")
        );
    }

    /*
     * Remember the prior checkpoint's redo ptr.
     */
    PriorRedoPtr = (*ControlFile).checkPointCopy.redo;

    /*
     * Update the control file.
     */
    LWLockAcquire(ControlFileLock as *mut LWLock, LW_EXCLUSIVE);
    if shutdown {
        (*ControlFile).state = DB_SHUTDOWNED;
    }
    (*ControlFile).checkPoint = ProcLastRecPtr;
    (*ControlFile).checkPointCopy = checkPoint;
    /* crash recovery should always recover to the end of WAL */
    (*ControlFile).minRecoveryPoint = InvalidXLogRecPtr;
    (*ControlFile).minRecoveryPointTLI = 0;

    /*
     * Persist unloggedLSN value.
     */
    (*ControlFile).unloggedLSN =
        pg_atomic_read_membarrier_u64(&mut (*XLogCtl).unloggedLSN);

    UpdateControlFile();
    LWLockRelease(ControlFileLock as *mut LWLock);

    /* Update shared-memory copy of checkpoint XID/epoch */
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).ckptFullXid = checkPoint.nextXid;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    /*
     * We are now done with critical updates.
     */
    END_CRIT_SECTION!();

    /*
     * WAL summaries end when the next XLOG_CHECKPOINT_REDO or
     * XLOG_CHECKPOINT_SHUTDOWN record is reached.
     */
    WakeupWalSummarizer();

    /*
     * Let smgr do post-checkpoint cleanup.
     */
    SyncPostCheckpoint();

    /*
     * Update the average distance between checkpoints.
     */
    if PriorRedoPtr != InvalidXLogRecPtr {
        UpdateCheckPointDistanceEstimate(RedoRecPtr - PriorRedoPtr);
    }

    // INJECTION_POINT("checkpoint-before-old-wal-removal", NULL)

    /*
     * Delete old log files.
     */
    XLByteToSeg(RedoRecPtr, &mut _logSegNo, wal_segment_size as uint32);
    KeepLogSeg(recptr, &mut _logSegNo);
    if InvalidateObsoleteReplicationSlots(
        RS_INVAL_WAL_REMOVED | RS_INVAL_IDLE_TIMEOUT,
        _logSegNo,
        InvalidOid,
        InvalidTransactionId,
    ) {
        /*
         * Some slots have been invalidated; recalculate the old-segment horizon.
         */
        XLByteToSeg(RedoRecPtr, &mut _logSegNo, wal_segment_size as uint32);
        KeepLogSeg(recptr, &mut _logSegNo);
    }
    _logSegNo -= 1;
    RemoveOldXlogFiles(_logSegNo, RedoRecPtr, recptr, checkPoint.ThisTimeLineID);

    /*
     * Make more log segments if needed.
     */
    if !shutdown {
        PreallocXlogFiles(recptr, checkPoint.ThisTimeLineID);
    }

    /*
     * Truncate pg_subtrans if possible.
     */
    if !RecoveryInProgress() {
        TruncateSUBTRANS(GetOldestTransactionIdConsideredRunning());
    }

    /* Real work is done; log and update stats. */
    LogCheckpointEnd(false);

    /* Reset the process title */
    update_checkpoint_display(flags, false, true);

    // TRACE_POSTGRESQL_CHECKPOINT_DONE(...)

    true
}

/*
 * Mark the end of recovery in WAL though without running a full checkpoint.
 */
unsafe fn CreateEndOfRecoveryRecord() {
    let mut xlrec: xl_end_of_recovery = core::mem::zeroed();
    let recptr: XLogRecPtr;

    /* sanity check */
    if !RecoveryInProgress() {
        elog!(ERROR, "can only be used to end recovery");
    }

    xlrec.end_time = GetCurrentTimestamp();
    xlrec.wal_level = wal_level;

    WALInsertLockAcquireExclusive();
    xlrec.ThisTimeLineID = (*XLogCtl).InsertTimeLineID;
    xlrec.PrevTimeLineID = (*XLogCtl).PrevTimeLineID;
    WALInsertLockRelease();

    START_CRIT_SECTION!();

    XLogBeginInsert();
    XLogRegisterData(
        &mut xlrec as *mut xl_end_of_recovery as *mut c_char,
        core::mem::size_of::<xl_end_of_recovery>(),
    );
    let recptr_val = XLogInsert(RM_XLOG_ID, XLOG_END_OF_RECOVERY);

    XLogFlush(recptr_val);

    /*
     * Update the control file so that crash recovery can follow the timeline
     * changes to this point.
     */
    LWLockAcquire(ControlFileLock as *mut LWLock, LW_EXCLUSIVE);
    (*ControlFile).minRecoveryPoint = recptr_val;
    (*ControlFile).minRecoveryPointTLI = xlrec.ThisTimeLineID;
    UpdateControlFile();
    LWLockRelease(ControlFileLock as *mut LWLock);

    END_CRIT_SECTION!();
}

/*
 * Write an OVERWRITE_CONTRECORD message.
 */
unsafe fn CreateOverwriteContrecordRecord(
    aborted_lsn: XLogRecPtr,
    pagePtr: XLogRecPtr,
    newTLI: TimeLineID,
) -> XLogRecPtr {
    let mut xlrec: xl_overwrite_contrecord = core::mem::zeroed();
    let recptr: XLogRecPtr;
    let pagehdr: *mut XLogPageHeaderData;
    let startPos: XLogRecPtr;

    /* sanity checks */
    if !RecoveryInProgress() {
        elog!(ERROR, "can only be used at end of recovery");
    }
    if pagePtr % XLOG_BLCKSZ as u64 != 0 {
        let (hi, lo) = LSN_FORMAT_ARGS(pagePtr);
        elog!(ERROR, "invalid position for missing continuation record {}/{}", hi, lo);
    }

    /* The current WAL insert position should be right after the page header */
    startPos = pagePtr;
    let startPos = if XLogSegmentOffset(startPos, wal_segment_size as uint32) == 0 {
        startPos + SizeOfXLogLongPHD as u64
    } else {
        startPos + SizeOfXLogShortPHD as u64
    };
    let cur_recptr = GetXLogInsertRecPtr();
    if cur_recptr != startPos {
        let (hi, lo) = LSN_FORMAT_ARGS(cur_recptr);
        elog!(
            ERROR,
            "invalid WAL insert position {}/{} for OVERWRITE_CONTRECORD",
            hi, lo
        );
    }

    START_CRIT_SECTION!();

    /*
     * Initialize the XLOG page header (by GetXLogBuffer), and set the
     * XLP_FIRST_IS_OVERWRITE_CONTRECORD flag.
     */
    WALInsertLockAcquire();
    pagehdr = GetXLogBuffer(pagePtr, newTLI) as *mut XLogPageHeaderData;
    (*pagehdr).xlp_info |= XLP_FIRST_IS_OVERWRITE_CONTRECORD;
    WALInsertLockRelease();

    /*
     * Insert the XLOG_OVERWRITE_CONTRECORD record.
     */
    XLogBeginInsert();
    xlrec.overwritten_lsn = aborted_lsn;
    xlrec.overwrite_time = GetCurrentTimestamp();
    XLogRegisterData(
        &mut xlrec as *mut xl_overwrite_contrecord as *mut c_char,
        core::mem::size_of::<xl_overwrite_contrecord>(),
    );
    let recptr_val = XLogInsert(RM_XLOG_ID, XLOG_OVERWRITE_CONTRECORD);

    /* check that the record was inserted to the right place */
    if ProcLastRecPtr != startPos {
        let (hi, lo) = LSN_FORMAT_ARGS(ProcLastRecPtr);
        elog!(
            ERROR,
            "OVERWRITE_CONTRECORD was inserted to unexpected position {}/{}",
            hi, lo
        );
    }

    XLogFlush(recptr_val);

    END_CRIT_SECTION!();

    recptr_val
}

/*
 * Flush all data in shared memory to disk, and fsync
 *
 * This is the common code shared between regular checkpoints and
 * recovery restartpoints.
 */
unsafe fn CheckPointGuts(checkPointRedo: XLogRecPtr, flags: c_int) {
    CheckPointRelationMap();
    CheckPointReplicationSlots(flags & CHECKPOINT_IS_SHUTDOWN != 0);
    CheckPointSnapBuild();
    CheckPointLogicalRewriteHeap();
    CheckPointReplicationOrigin();

    /* Write out all dirty data in SLRUs and the main buffer pool */
    // TRACE_POSTGRESQL_BUFFER_CHECKPOINT_START(flags)
    CheckpointStats.ckpt_write_t = GetCurrentTimestamp();
    CheckPointCLOG();
    CheckPointCommitTs();
    CheckPointSUBTRANS();
    CheckPointMultiXact();
    CheckPointPredicate();
    CheckPointBuffers(flags);

    /* Perform all queued up fsyncs */
    // TRACE_POSTGRESQL_BUFFER_CHECKPOINT_SYNC_START()
    CheckpointStats.ckpt_sync_t = GetCurrentTimestamp();
    ProcessSyncRequests();
    CheckpointStats.ckpt_sync_end_t = GetCurrentTimestamp();
    // TRACE_POSTGRESQL_BUFFER_CHECKPOINT_DONE()

    /* We deliberately delay 2PC checkpointing as long as possible */
    CheckPointTwoPhase(checkPointRedo);
}

/*
 * Save a checkpoint for recovery restart if appropriate
 */
unsafe fn RecoveryRestartPoint(checkPoint: *const CheckPoint, record: *mut XLogReaderState) {
    /*
     * Also refrain from creating a restartpoint if we have seen any
     * references to non-existent pages.
     */
    if XLogHaveInvalidPages() {
        let (hi, lo) = LSN_FORMAT_ARGS((*checkPoint).redo);
        elog!(
            DEBUG2,
            "could not record restart point at {}/{} because there \
             are unresolved references to invalid pages",
            hi, lo
        );
        return;
    }

    /*
     * Copy the checkpoint record to shared memory.
     */
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).lastCheckPointRecPtr = (*record).ReadRecPtr;
    (*XLogCtl).lastCheckPointEndPtr = (*record).EndRecPtr;
    (*XLogCtl).lastCheckPoint = *checkPoint;
    SpinLockRelease(&mut (*XLogCtl).info_lck);
}

/*
 * Establish a restartpoint if possible.
 *
 * Returns true if a new restartpoint was established.
 */
pub unsafe fn CreateRestartPoint(flags: c_int) -> bool {
    let lastCheckPointRecPtr: XLogRecPtr;
    let lastCheckPointEndPtr: XLogRecPtr;
    let lastCheckPoint: CheckPoint;
    let PriorRedoPtr: XLogRecPtr;
    let receivePtr: XLogRecPtr;
    let replayPtr: XLogRecPtr;
    let mut replayTLI: TimeLineID = 0;
    let endptr: XLogRecPtr;
    let mut _logSegNo: XLogSegNo = 0;
    let xtime: TimestampTz;

    /* Concurrent checkpoint/restartpoint cannot happen */
    assert!(!IsUnderPostmaster || MyBackendType == B_CHECKPOINTER);

    /* Get a local copy of the last safe checkpoint record. */
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    lastCheckPointRecPtr = (*XLogCtl).lastCheckPointRecPtr;
    lastCheckPointEndPtr = (*XLogCtl).lastCheckPointEndPtr;
    lastCheckPoint = (*XLogCtl).lastCheckPoint;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    /*
     * Check that we're still in recovery mode.
     */
    if !RecoveryInProgress() {
        elog!(DEBUG2, "skipping restartpoint, recovery has already ended");
        return false;
    }

    /*
     * If the last checkpoint record we've replayed is already our last
     * restartpoint, we can't perform a new restart point.
     */
    if XLogRecPtrIsInvalid(lastCheckPointRecPtr)
        || lastCheckPoint.redo <= (*ControlFile).checkPointCopy.redo
    {
        let (hi, lo) = LSN_FORMAT_ARGS(lastCheckPoint.redo);
        elog!(DEBUG2, "skipping restartpoint, already performed at {}/{}", hi, lo);

        UpdateMinRecoveryPoint(InvalidXLogRecPtr, true);
        if flags & CHECKPOINT_IS_SHUTDOWN != 0 {
            LWLockAcquire(ControlFileLock as *mut LWLock, LW_EXCLUSIVE);
            (*ControlFile).state = DB_SHUTDOWNED_IN_RECOVERY;
            UpdateControlFile();
            LWLockRelease(ControlFileLock as *mut LWLock);
        }
        return false;
    }

    /*
     * Update the shared RedoRecPtr.
     */
    WALInsertLockAcquireExclusive();
    RedoRecPtr = lastCheckPoint.redo;
    (*XLogCtl).Insert.RedoRecPtr = lastCheckPoint.redo;
    WALInsertLockRelease();

    /* Also update the info_lck-protected copy */
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).RedoRecPtr = lastCheckPoint.redo;
    SpinLockRelease(&mut (*XLogCtl).info_lck);

    /*
     * Prepare to accumulate statistics.
     */
    MemSet(
        &mut CheckpointStats as *mut CheckpointStatsData as *mut c_void,
        0,
        core::mem::size_of::<CheckpointStatsData>(),
    );
    CheckpointStats.ckpt_start_t = GetCurrentTimestamp();

    if log_checkpoints {
        LogCheckpointStart(flags, true);
    }

    /* Update the process title */
    update_checkpoint_display(flags, true, false);

    CheckPointGuts(lastCheckPoint.redo, flags);

    // INJECTION_POINT("create-restart-point", NULL)

    /*
     * Remember the prior checkpoint's redo ptr.
     */
    PriorRedoPtr = (*ControlFile).checkPointCopy.redo;

    /*
     * Update pg_control, using current time.
     */
    LWLockAcquire(ControlFileLock as *mut LWLock, LW_EXCLUSIVE);
    if (*ControlFile).checkPointCopy.redo < lastCheckPoint.redo {
        /*
         * Update the checkpoint information.
         */
        (*ControlFile).checkPoint = lastCheckPointRecPtr;
        (*ControlFile).checkPointCopy = lastCheckPoint;

        /*
         * Ensure minRecoveryPoint is past the checkpoint record.
         */
        if (*ControlFile).state == DB_IN_ARCHIVE_RECOVERY {
            if (*ControlFile).minRecoveryPoint < lastCheckPointEndPtr {
                (*ControlFile).minRecoveryPoint = lastCheckPointEndPtr;
                (*ControlFile).minRecoveryPointTLI = lastCheckPoint.ThisTimeLineID;

                /* update local copy */
                LocalMinRecoveryPoint = (*ControlFile).minRecoveryPoint;
                LocalMinRecoveryPointTLI = (*ControlFile).minRecoveryPointTLI;
            }
            if flags & CHECKPOINT_IS_SHUTDOWN != 0 {
                (*ControlFile).state = DB_SHUTDOWNED_IN_RECOVERY;
            }
        }
        UpdateControlFile();
    }
    LWLockRelease(ControlFileLock as *mut LWLock);

    /*
     * Update the average distance between checkpoints/restartpoints.
     */
    if PriorRedoPtr != InvalidXLogRecPtr {
        UpdateCheckPointDistanceEstimate(RedoRecPtr - PriorRedoPtr);
    }

    /*
     * Delete old log files.
     */
    XLByteToSeg(RedoRecPtr, &mut _logSegNo, wal_segment_size as uint32);

    /*
     * Retreat _logSegNo using the current end of xlog replayed or received,
     * whichever is later.
     */
    receivePtr = GetWalRcvFlushRecPtr(ptr::null_mut(), ptr::null_mut());
    replayPtr = GetXLogReplayRecPtr(&mut replayTLI);
    endptr = if receivePtr < replayPtr { replayPtr } else { receivePtr };
    KeepLogSeg(endptr, &mut _logSegNo);

    // INJECTION_POINT("restartpoint-before-slot-invalidation", NULL)

    if InvalidateObsoleteReplicationSlots(
        RS_INVAL_WAL_REMOVED | RS_INVAL_IDLE_TIMEOUT,
        _logSegNo,
        InvalidOid,
        InvalidTransactionId,
    ) {
        /*
         * Some slots have been invalidated; recalculate the old-segment horizon.
         */
        XLByteToSeg(RedoRecPtr, &mut _logSegNo, wal_segment_size as uint32);
        KeepLogSeg(endptr, &mut _logSegNo);
    }
    _logSegNo -= 1;

    /*
     * Try to recycle segments on a useful timeline.
     */
    if !RecoveryInProgress() {
        replayTLI = (*XLogCtl).InsertTimeLineID;
    }

    RemoveOldXlogFiles(_logSegNo, RedoRecPtr, endptr, replayTLI);

    /*
     * Make more log segments if needed.
     */
    PreallocXlogFiles(endptr, replayTLI);

    /*
     * Truncate pg_subtrans if possible.
     */
    if EnableHotStandby {
        TruncateSUBTRANS(GetOldestTransactionIdConsideredRunning());
    }

    /* Real work is done; log and update stats. */
    LogCheckpointEnd(true);

    /* Reset the process title */
    update_checkpoint_display(flags, true, true);

    xtime = GetLatestXTime();
    let (hi, lo) = LSN_FORMAT_ARGS(lastCheckPoint.redo);
    ereport!(
        if log_checkpoints { LOG } else { DEBUG2 },
        errmsg!("recovery restart point at {}/{}", hi, lo)
        /* xtime ? errdetail(...) : 0 -- omitted as errdetail is conditional */
    );

    /*
     * Finally, execute archive_cleanup_command, if any.
     */
    if !archiveCleanupCommand.is_null()
        && libc::strcmp(archiveCleanupCommand, b"\0".as_ptr() as *const c_char) != 0
    {
        ExecuteRecoveryCommand(
            archiveCleanupCommand,
            b"archive_cleanup_command\0".as_ptr() as *const c_char,
            false,
            WAIT_EVENT_ARCHIVE_CLEANUP_COMMAND,
        );
    }

    true
}

/*
 * Report availability of WAL for the given target LSN
 */
pub unsafe fn GetWALAvailability(targetLSN: XLogRecPtr) -> WALAvailability {
    let currpos: XLogRecPtr;
    let mut currSeg: XLogSegNo = 0;
    let mut targetSeg: XLogSegNo = 0;
    let mut oldestSeg: XLogSegNo;
    let mut oldestSegMaxWalSize: XLogSegNo;
    let mut oldestSlotSeg: XLogSegNo = 0;
    let keepSegs: u64;

    /*
     * slot does not reserve WAL.
     */
    if XLogRecPtrIsInvalid(targetLSN) {
        return WALAVAIL_INVALID_LSN;
    }

    /*
     * Calculate the oldest segment currently reserved by all slots.
     */
    currpos = GetXLogWriteRecPtr();
    XLByteToSeg(currpos, &mut oldestSlotSeg, wal_segment_size as uint32);
    KeepLogSeg(currpos, &mut oldestSlotSeg);

    /*
     * Find the oldest extant segment file.
     */
    oldestSeg = XLogGetLastRemovedSegno() + 1;

    /* calculate oldest segment by max_wal_size */
    XLByteToSeg(currpos, &mut currSeg, wal_segment_size as uint32);
    keepSegs = ConvertToXSegs(max_wal_size_mb, wal_segment_size) + 1;

    if currSeg > keepSegs {
        oldestSegMaxWalSize = currSeg - keepSegs;
    } else {
        oldestSegMaxWalSize = 1;
    }

    /* the segment we care about */
    XLByteToSeg(targetLSN, &mut targetSeg, wal_segment_size as uint32);

    /*
     * No point in returning reserved or extended status values if the
     * targetSeg is known to be lost.
     */
    if targetSeg >= oldestSlotSeg {
        /* show "reserved" when targetSeg is within max_wal_size */
        if targetSeg >= oldestSegMaxWalSize {
            return WALAVAIL_RESERVED;
        }
        /* being retained by slots exceeding max_wal_size */
        return WALAVAIL_EXTENDED;
    }

    /* WAL segments are no longer retained but haven't been removed yet */
    if targetSeg >= oldestSeg {
        return WALAVAIL_UNRESERVED;
    }

    /* Definitely lost */
    WALAVAIL_REMOVED
}

/*
 * Retreat *logSegNo to the last segment that we need to retain.
 */
unsafe fn KeepLogSeg(recptr: XLogRecPtr, logSegNo: *mut XLogSegNo) {
    let mut currSegNo: XLogSegNo = 0;
    let mut segno: XLogSegNo;
    let mut keep: XLogRecPtr;

    XLByteToSeg(recptr, &mut currSegNo, wal_segment_size as uint32);
    segno = currSegNo;

    /* Calculate how many segments are kept by slots. */
    keep = XLogGetReplicationSlotMinimumLSN();
    if keep != InvalidXLogRecPtr && keep < recptr {
        XLByteToSeg(keep, &mut segno, wal_segment_size as uint32);

        /*
         * Account for max_slot_wal_keep_size to avoid keeping more than
         * configured.
         */
        if max_slot_wal_keep_size_mb >= 0 && !IsBinaryUpgrade {
            let slot_keep_segs = ConvertToXSegs(max_slot_wal_keep_size_mb, wal_segment_size);
            if currSegNo - segno > slot_keep_segs {
                segno = currSegNo - slot_keep_segs;
            }
        }
    }

    /*
     * If WAL summarization is in use, don't remove WAL that has yet to be
     * summarized.
     */
    keep = GetOldestUnsummarizedLSN(ptr::null_mut(), ptr::null_mut());
    if keep != InvalidXLogRecPtr {
        let mut unsummarized_segno: XLogSegNo = 0;
        XLByteToSeg(keep, &mut unsummarized_segno, wal_segment_size as uint32);
        if unsummarized_segno < segno {
            segno = unsummarized_segno;
        }
    }

    /* but, keep at least wal_keep_size if that's set */
    if wal_keep_size_mb > 0 {
        let keep_segs = ConvertToXSegs(wal_keep_size_mb, wal_segment_size);
        if currSegNo - segno < keep_segs {
            /* avoid underflow, don't go below 1 */
            if currSegNo <= keep_segs {
                segno = 1;
            } else {
                segno = currSegNo - keep_segs;
            }
        }
    }

    /* don't delete WAL segments newer than the calculated segment */
    if segno < *logSegNo {
        *logSegNo = segno;
    }
}

/*
 * Write a NEXTOID log record
 */
pub unsafe fn XLogPutNextOid(nextOid: Oid) {
    XLogBeginInsert();
    XLogRegisterData(&nextOid as *const Oid as *mut c_char, core::mem::size_of::<Oid>());
    let _ = XLogInsert(RM_XLOG_ID, XLOG_NEXTOID);

    /*
     * We need not flush the NEXTOID record immediately.
     */
}

/*
 * Write an XLOG SWITCH record.
 *
 * The return value is either the end+1 address of the switch record,
 * or the end+1 address of the prior segment if we did not need to
 * write a switch record because we are already at segment start.
 */
pub unsafe fn RequestXLogSwitch(mark_unimportant: bool) -> XLogRecPtr {
    /* XLOG SWITCH has no data */
    XLogBeginInsert();

    if mark_unimportant {
        XLogSetRecordFlags(XLOG_MARK_UNIMPORTANT);
    }
    XLogInsert(RM_XLOG_ID, XLOG_SWITCH)
}

/*
 * Write a RESTORE POINT record
 */
pub unsafe fn XLogRestorePoint(rpName: *const c_char) -> XLogRecPtr {
    let mut xlrec: xl_restore_point = core::mem::zeroed();

    xlrec.rp_time = GetCurrentTimestamp();
    libc::strlcpy(xlrec.rp_name.as_mut_ptr(), rpName, MAXFNAMELEN);

    XLogBeginInsert();
    XLogRegisterData(
        &mut xlrec as *mut xl_restore_point as *mut c_char,
        core::mem::size_of::<xl_restore_point>(),
    );

    let RecPtr = XLogInsert(RM_XLOG_ID, XLOG_RESTORE_POINT);

    let (hi, lo) = LSN_FORMAT_ARGS(RecPtr);
    ereport!(
        LOG,
        errmsg!(
            "restore point \"{}\" created at {}/{}",
            core::ffi::CStr::from_ptr(rpName).to_string_lossy(),
            hi, lo
        )
    );

    RecPtr
}

/*
 * Check if any of the GUC parameters that are critical for hot standby
 * have changed, and update the value in pg_control file if necessary.
 */
unsafe fn XLogReportParameters() {
    if wal_level != (*ControlFile).wal_level
        || wal_log_hints != (*ControlFile).wal_log_hints
        || MaxConnections != (*ControlFile).MaxConnections
        || max_worker_processes != (*ControlFile).max_worker_processes
        || max_wal_senders != (*ControlFile).max_wal_senders
        || max_prepared_xacts != (*ControlFile).max_prepared_xacts
        || max_locks_per_xact != (*ControlFile).max_locks_per_xact
        || track_commit_timestamp != (*ControlFile).track_commit_timestamp
    {
        /*
         * The change in number of backend slots doesn't need to be WAL-logged
         * if archiving is not enabled.
         */
        if wal_level != (*ControlFile).wal_level || XLogIsNeeded() {
            let mut xlrec: xl_parameter_change = core::mem::zeroed();
            let recptr: XLogRecPtr;

            xlrec.MaxConnections = MaxConnections;
            xlrec.max_worker_processes = max_worker_processes;
            xlrec.max_wal_senders = max_wal_senders;
            xlrec.max_prepared_xacts = max_prepared_xacts;
            xlrec.max_locks_per_xact = max_locks_per_xact;
            xlrec.wal_level = wal_level;
            xlrec.wal_log_hints = wal_log_hints;
            xlrec.track_commit_timestamp = track_commit_timestamp;

            XLogBeginInsert();
            XLogRegisterData(
                &mut xlrec as *mut xl_parameter_change as *mut c_char,
                core::mem::size_of::<xl_parameter_change>(),
            );

            let recptr = XLogInsert(RM_XLOG_ID, XLOG_PARAMETER_CHANGE);
            XLogFlush(recptr);
        }

        LWLockAcquire(ControlFileLock as *mut LWLock, LW_EXCLUSIVE);

        (*ControlFile).MaxConnections = MaxConnections;
        (*ControlFile).max_worker_processes = max_worker_processes;
        (*ControlFile).max_wal_senders = max_wal_senders;
        (*ControlFile).max_prepared_xacts = max_prepared_xacts;
        (*ControlFile).max_locks_per_xact = max_locks_per_xact;
        (*ControlFile).wal_level = wal_level;
        (*ControlFile).wal_log_hints = wal_log_hints;
        (*ControlFile).track_commit_timestamp = track_commit_timestamp;
        UpdateControlFile();

        LWLockRelease(ControlFileLock as *mut LWLock);
    }
}

/*
 * Update full_page_writes in shared memory, and write an
 * XLOG_FPW_CHANGE record if necessary.
 */
pub unsafe fn UpdateFullPageWrites() {
    let Insert: *mut XLogCtlInsert = &mut (*XLogCtl).Insert;
    let recoveryInProgress: bool;

    /*
     * Do nothing if full_page_writes has not been changed.
     */
    if fullPageWrites == (*Insert).fullPageWrites {
        return;
    }

    /*
     * Perform this outside critical section.
     */
    recoveryInProgress = RecoveryInProgress();

    START_CRIT_SECTION!();

    /*
     * If we're setting full_page_writes to true, first set it true and then
     * write the WAL record.
     */
    if fullPageWrites {
        WALInsertLockAcquireExclusive();
        (*Insert).fullPageWrites = true;
        WALInsertLockRelease();
    }

    /*
     * Write an XLOG_FPW_CHANGE record.
     */
    if XLogStandbyInfoActive() && !recoveryInProgress {
        XLogBeginInsert();
        XLogRegisterData(&mut fullPageWrites as *mut bool as *mut c_char, core::mem::size_of::<bool>());
        XLogInsert(RM_XLOG_ID, XLOG_FPW_CHANGE);
    }

    if !fullPageWrites {
        WALInsertLockAcquireExclusive();
        (*Insert).fullPageWrites = false;
        WALInsertLockRelease();
    }
    END_CRIT_SECTION!();
}

/*
 * XLOG resource manager's routines
 */
pub unsafe fn xlog_redo(record: *mut XLogReaderState) {
    let info: uint8 = (XLogRecGetInfo(record) & !XLR_INFO_MASK) as uint8;
    let lsn: XLogRecPtr = (*record).EndRecPtr;

    /*
     * In XLOG rmgr, backup blocks are only used by XLOG_FPI and
     * XLOG_FPI_FOR_HINT records.
     */
    assert!(
        info == XLOG_FPI || info == XLOG_FPI_FOR_HINT
            || !XLogRecHasAnyBlockRefs(record)
    );

    if info == XLOG_NEXTOID {
        let mut nextOid: Oid = 0;
        /*
         * We used to try to take the maximum of TransamVariables->nextOid and
         * the recorded nextOid, but that fails if the OID counter wraps around.
         */
        ptr::copy_nonoverlapping(
            XLogRecGetData(record) as *const u8,
            &mut nextOid as *mut Oid as *mut u8,
            core::mem::size_of::<Oid>(),
        );
        LWLockAcquire(OidGenLock as *mut LWLock, LW_EXCLUSIVE);
        (*TransamVariables).nextOid = nextOid;
        (*TransamVariables).oidCount = 0;
        LWLockRelease(OidGenLock as *mut LWLock);
    } else if info == XLOG_CHECKPOINT_SHUTDOWN {
        let mut checkPoint: CheckPoint = core::mem::zeroed();
        let replayTLI: TimeLineID;

        ptr::copy_nonoverlapping(
            XLogRecGetData(record) as *const u8,
            &mut checkPoint as *mut CheckPoint as *mut u8,
            core::mem::size_of::<CheckPoint>(),
        );
        /* In a SHUTDOWN checkpoint, believe the counters exactly */
        LWLockAcquire(XidGenLock as *mut LWLock, LW_EXCLUSIVE);
        (*TransamVariables).nextXid = checkPoint.nextXid;
        LWLockRelease(XidGenLock as *mut LWLock);
        LWLockAcquire(OidGenLock as *mut LWLock, LW_EXCLUSIVE);
        (*TransamVariables).nextOid = checkPoint.nextOid;
        (*TransamVariables).oidCount = 0;
        LWLockRelease(OidGenLock as *mut LWLock);
        MultiXactSetNextMXact(checkPoint.nextMulti, checkPoint.nextMultiOffset);
        MultiXactAdvanceOldest(checkPoint.oldestMulti, checkPoint.oldestMultiDB);

        /*
         * No need to set oldestClogXid here as well.
         */
        SetTransactionIdLimit(checkPoint.oldestXid, checkPoint.oldestXidDB);

        /*
         * If we see a shutdown checkpoint while waiting for an end-of-backup
         * record, the backup was canceled.
         */
        if ArchiveRecoveryRequested
            && !XLogRecPtrIsInvalid((*ControlFile).backupStartPoint)
            && XLogRecPtrIsInvalid((*ControlFile).backupEndPoint)
        {
            ereport!(PANIC, errmsg!("online backup was canceled, recovery cannot continue"));
        }

        /*
         * If we see a shutdown checkpoint, we know that nothing was running
         * on the primary at this point.
         */
        if standbyState >= STANDBY_INITIALIZED {
            let mut xids: *mut TransactionId = ptr::null_mut();
            let mut nxids: c_int = 0;
            let mut oldestActiveXID: TransactionId;
            let mut latestCompletedXid: TransactionId;
            let mut running: RunningTransactionsData = core::mem::zeroed();

            oldestActiveXID = PrescanPreparedTransactions(&mut xids, &mut nxids);

            /* Update pg_subtrans entries for any prepared transactions */
            StandbyRecoverPreparedTransactions();

            running.xcnt = nxids;
            running.subxcnt = 0;
            running.subxid_status = SUBXIDS_IN_SUBTRANS;
            running.nextXid = XidFromFullTransactionId(checkPoint.nextXid);
            running.oldestRunningXid = oldestActiveXID;
            latestCompletedXid = XidFromFullTransactionId(checkPoint.nextXid);
            TransactionIdRetreat(&mut latestCompletedXid);
            assert!(TransactionIdIsNormal(latestCompletedXid));
            running.latestCompletedXid = latestCompletedXid;
            running.xids = xids;

            ProcArrayApplyRecoveryInfo(&mut running);
        }

        /* ControlFile->checkPointCopy always tracks the latest ckpt XID */
        LWLockAcquire(ControlFileLock as *mut LWLock, LW_EXCLUSIVE);
        (*ControlFile).checkPointCopy.nextXid = checkPoint.nextXid;
        LWLockRelease(ControlFileLock as *mut LWLock);

        /* Update shared-memory copy of checkpoint XID/epoch */
        SpinLockAcquire(&mut (*XLogCtl).info_lck);
        (*XLogCtl).ckptFullXid = checkPoint.nextXid;
        SpinLockRelease(&mut (*XLogCtl).info_lck);

        /*
         * We should've already switched to the new TLI before replaying this record.
         */
        let mut replayTLI_inner: TimeLineID = 0;
        let _ = GetCurrentReplayRecPtr(&mut replayTLI_inner);
        if checkPoint.ThisTimeLineID != replayTLI_inner {
            ereport!(
                PANIC,
                errmsg!(
                    "unexpected timeline ID {} (should be {}) in shutdown checkpoint record",
                    checkPoint.ThisTimeLineID, replayTLI_inner
                )
            );
        }

        RecoveryRestartPoint(&checkPoint, record);

        /*
         * After replaying a checkpoint record, free all smgr objects.
         */
        smgrdestroyall();
    } else if info == XLOG_CHECKPOINT_ONLINE {
        let mut checkPoint: CheckPoint = core::mem::zeroed();

        ptr::copy_nonoverlapping(
            XLogRecGetData(record) as *const u8,
            &mut checkPoint as *mut CheckPoint as *mut u8,
            core::mem::size_of::<CheckPoint>(),
        );
        /* In an ONLINE checkpoint, treat the XID counter as a minimum */
        LWLockAcquire(XidGenLock as *mut LWLock, LW_EXCLUSIVE);
        if FullTransactionIdPrecedes((*TransamVariables).nextXid, checkPoint.nextXid) {
            (*TransamVariables).nextXid = checkPoint.nextXid;
        }
        LWLockRelease(XidGenLock as *mut LWLock);

        /*
         * We ignore the nextOid counter in an ONLINE checkpoint.
         */

        /* Handle multixact */
        MultiXactAdvanceNextMXact(checkPoint.nextMulti, checkPoint.nextMultiOffset);

        /*
         * NB: This may perform multixact truncation.
         */
        MultiXactAdvanceOldest(checkPoint.oldestMulti, checkPoint.oldestMultiDB);
        if TransactionIdPrecedes((*TransamVariables).oldestXid, checkPoint.oldestXid) {
            SetTransactionIdLimit(checkPoint.oldestXid, checkPoint.oldestXidDB);
        }
        /* ControlFile->checkPointCopy always tracks the latest ckpt XID */
        LWLockAcquire(ControlFileLock as *mut LWLock, LW_EXCLUSIVE);
        (*ControlFile).checkPointCopy.nextXid = checkPoint.nextXid;
        LWLockRelease(ControlFileLock as *mut LWLock);

        /* Update shared-memory copy of checkpoint XID/epoch */
        SpinLockAcquire(&mut (*XLogCtl).info_lck);
        (*XLogCtl).ckptFullXid = checkPoint.nextXid;
        SpinLockRelease(&mut (*XLogCtl).info_lck);

        /* TLI should not change in an on-line checkpoint */
        let mut replayTLI_inner: TimeLineID = 0;
        let _ = GetCurrentReplayRecPtr(&mut replayTLI_inner);
        if checkPoint.ThisTimeLineID != replayTLI_inner {
            ereport!(
                PANIC,
                errmsg!(
                    "unexpected timeline ID {} (should be {}) in online checkpoint record",
                    checkPoint.ThisTimeLineID, replayTLI_inner
                )
            );
        }

        RecoveryRestartPoint(&checkPoint, record);

        /*
         * After replaying a checkpoint record, free all smgr objects.
         */
        smgrdestroyall();
    } else if info == XLOG_OVERWRITE_CONTRECORD {
        /* nothing to do here, handled in xlogrecovery_redo() */
    } else if info == XLOG_END_OF_RECOVERY {
        let mut xlrec: xl_end_of_recovery = core::mem::zeroed();

        ptr::copy_nonoverlapping(
            XLogRecGetData(record) as *const u8,
            &mut xlrec as *mut xl_end_of_recovery as *mut u8,
            core::mem::size_of::<xl_end_of_recovery>(),
        );

        /*
         * For Hot Standby, we could treat this like a Shutdown Checkpoint,
         * but this case is rarer and harder to test.
         */

        /*
         * We should've already switched to the new TLI before replaying this record.
         */
        let mut replayTLI_inner: TimeLineID = 0;
        let _ = GetCurrentReplayRecPtr(&mut replayTLI_inner);
        if xlrec.ThisTimeLineID != replayTLI_inner {
            ereport!(
                PANIC,
                errmsg!(
                    "unexpected timeline ID {} (should be {}) in end-of-recovery record",
                    xlrec.ThisTimeLineID, replayTLI_inner
                )
            );
        }
    } else if info == XLOG_NOOP {
        /* nothing to do here */
    } else if info == XLOG_SWITCH {
        /* nothing to do here */
    } else if info == XLOG_RESTORE_POINT {
        /* nothing to do here, handled in xlogrecovery.c */
    } else if info == XLOG_FPI || info == XLOG_FPI_FOR_HINT {
        /*
         * XLOG_FPI records contain nothing else but one or more block
         * references. Every block reference must include a full-page image.
         *
         * XLOG_FPI_FOR_HINT records are generated when a page needs to be
         * WAL-logged because of a hint bit update. They may include no
         * full-page images if full_page_writes was disabled when generated.
         *
         * No recovery conflicts are generated by these generic records.
         */
        let mut block_id: uint8 = 0;
        while block_id <= XLogRecMaxBlockId(record) {
            let mut buffer: Buffer = InvalidBuffer;

            if !XLogRecHasBlockImage(record, block_id) {
                if info == XLOG_FPI {
                    elog!(ERROR, "XLOG_FPI record did not contain a full-page image");
                }
                block_id += 1;
                continue;
            }

            if XLogReadBufferForRedo(record, block_id, &mut buffer) != BLK_RESTORED {
                elog!(ERROR, "unexpected XLogReadBufferForRedo result when restoring backup block");
            }
            UnlockReleaseBuffer(buffer);
            block_id += 1;
        }
    } else if info == XLOG_BACKUP_END {
        /* nothing to do here, handled in xlogrecovery_redo() */
    } else if info == XLOG_PARAMETER_CHANGE {
        let mut xlrec: xl_parameter_change = core::mem::zeroed();

        /* Update our copy of the parameters in pg_control */
        ptr::copy_nonoverlapping(
            XLogRecGetData(record) as *const u8,
            &mut xlrec as *mut xl_parameter_change as *mut u8,
            core::mem::size_of::<xl_parameter_change>(),
        );

        /*
         * Invalidate logical slots if we are in hot standby and the primary
         * does not have a WAL level sufficient for logical decoding.
         */
        if InRecovery
            && InHotStandby
            && xlrec.wal_level < WAL_LEVEL_LOGICAL
            && wal_level >= WAL_LEVEL_LOGICAL
        {
            InvalidateObsoleteReplicationSlots(
                RS_INVAL_WAL_LEVEL,
                0,
                InvalidOid,
                InvalidTransactionId,
            );
        }

        LWLockAcquire(ControlFileLock as *mut LWLock, LW_EXCLUSIVE);
        (*ControlFile).MaxConnections = xlrec.MaxConnections;
        (*ControlFile).max_worker_processes = xlrec.max_worker_processes;
        (*ControlFile).max_wal_senders = xlrec.max_wal_senders;
        (*ControlFile).max_prepared_xacts = xlrec.max_prepared_xacts;
        (*ControlFile).max_locks_per_xact = xlrec.max_locks_per_xact;
        (*ControlFile).wal_level = xlrec.wal_level;
        (*ControlFile).wal_log_hints = xlrec.wal_log_hints;

        /*
         * Update minRecoveryPoint to ensure that if recovery is aborted, we
         * recover back up to this point before allowing hot standby again.
         */
        if InArchiveRecovery {
            LocalMinRecoveryPoint = (*ControlFile).minRecoveryPoint;
            LocalMinRecoveryPointTLI = (*ControlFile).minRecoveryPointTLI;
        }
        if LocalMinRecoveryPoint != InvalidXLogRecPtr && LocalMinRecoveryPoint < lsn {
            let mut replayTLI_inner: TimeLineID = 0;
            let _ = GetCurrentReplayRecPtr(&mut replayTLI_inner);
            (*ControlFile).minRecoveryPoint = lsn;
            (*ControlFile).minRecoveryPointTLI = replayTLI_inner;
        }

        CommitTsParameterChange(
            xlrec.track_commit_timestamp,
            (*ControlFile).track_commit_timestamp,
        );
        (*ControlFile).track_commit_timestamp = xlrec.track_commit_timestamp;

        UpdateControlFile();
        LWLockRelease(ControlFileLock as *mut LWLock);

        /* Check to see if any parameter change gives a problem on recovery */
        CheckRequiredParameterValues();
    } else if info == XLOG_FPW_CHANGE {
        let mut fpw: bool = false;

        ptr::copy_nonoverlapping(
            XLogRecGetData(record) as *const u8,
            &mut fpw as *mut bool as *mut u8,
            core::mem::size_of::<bool>(),
        );

        /*
         * Update the LSN of the last replayed XLOG_FPW_CHANGE record.
         */
        if !fpw {
            SpinLockAcquire(&mut (*XLogCtl).info_lck);
            if (*XLogCtl).lastFpwDisableRecPtr < (*record).ReadRecPtr {
                (*XLogCtl).lastFpwDisableRecPtr = (*record).ReadRecPtr;
            }
            SpinLockRelease(&mut (*XLogCtl).info_lck);
        }

        /* Keep track of full_page_writes */
        lastFullPageWrites = fpw;
    } else if info == XLOG_CHECKPOINT_REDO {
        /* nothing to do here, just for informational purposes */
    }
}

/*
 * Return the extra open flags used for opening a file, depending on the
 * value of the GUCs wal_sync_method, fsync and debug_io_direct.
 */
unsafe fn get_sync_bit(method: c_int) -> c_int {
    let mut o_direct_flag: c_int = 0;

    /*
     * Use O_DIRECT if requested, except in walreceiver process.
     */
    if (io_direct_flags & IO_DIRECT_WAL) != 0 && !AmWalReceiverProcess() {
        o_direct_flag = PG_O_DIRECT;
    }

    /* If fsync is disabled, never open in sync mode */
    if !enableFsync {
        return o_direct_flag;
    }

    match method {
        /*
         * enum values for all sync options are defined even if they are
         * not supported on the current platform.
         */
        WAL_SYNC_METHOD_FSYNC
        | WAL_SYNC_METHOD_FSYNC_WRITETHROUGH
        | WAL_SYNC_METHOD_FDATASYNC => o_direct_flag,
        WAL_SYNC_METHOD_OPEN => {
            #[cfg(target_os = "linux")]
            { libc::O_SYNC | o_direct_flag }
            #[cfg(not(target_os = "linux"))]
            { o_direct_flag }
        }
        WAL_SYNC_METHOD_OPEN_DSYNC => {
            #[cfg(target_os = "linux")]
            { libc::O_DSYNC | o_direct_flag }
            #[cfg(not(target_os = "linux"))]
            { o_direct_flag }
        }
        _ => {
            /* can't happen (unless we are out of sync with option array) */
            elog!(ERROR, "unrecognized \"wal_sync_method\": {}", method);
            0 /* silence warning */
        }
    }
}

/*
 * GUC support
 */
pub unsafe fn assign_wal_sync_method(new_wal_sync_method: c_int, _extra: *mut c_void) {
    if wal_sync_method != new_wal_sync_method {
        /*
         * To ensure that no blocks escape unsynced, force an fsync on the
         * currently open log segment (if any).  Also, if the open flag is
         * changing, close the log file so it will be reopened at next use.
         */
        if openLogFile >= 0 {
            pgstat_report_wait_start(WAIT_EVENT_WAL_SYNC_METHOD_ASSIGN);
            if pg_fsync(openLogFile) != 0 {
                let mut xlogfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
                let save_errno = *libc::__error();
                XLogFileName(
                    xlogfname.as_mut_ptr(),
                    openLogTLI,
                    openLogSegNo,
                    wal_segment_size as uint32,
                );
                *libc::__error() = save_errno;
                ereport!(
                    PANIC,
                    errmsg!(
                        "could not fsync file \"{}\": {}",
                        core::ffi::CStr::from_ptr(xlogfname.as_ptr()).to_string_lossy(),
                        strerror_r()
                    )
                );
                /* errcode_for_file_access */
            }
            pgstat_report_wait_end();
            if get_sync_bit(wal_sync_method) != get_sync_bit(new_wal_sync_method) {
                XLogFileClose();
            }
        }
    }
}


/*
 * Issue appropriate kind of fsync (if any) for an XLOG output file.
 *
 * 'fd' is a file descriptor for the XLOG file to be fsync'd.
 * 'segno' is for error reporting purposes.
 */
pub unsafe fn issue_xlog_fsync(fd: c_int, segno: XLogSegNo, tli: TimeLineID) {
    let mut msg: *const c_char = ptr::null();
    let start: instr_time;

    assert!(tli != 0);

    /*
     * Quick exit if fsync is disabled or write() has already synced the WAL file.
     */
    if !enableFsync
        || wal_sync_method == WAL_SYNC_METHOD_OPEN
        || wal_sync_method == WAL_SYNC_METHOD_OPEN_DSYNC
    {
        return;
    }

    /*
     * Measure I/O timing to sync the WAL file for pg_stat_io.
     */
    let start = pgstat_prepare_io_time(track_wal_io_timing);

    pgstat_report_wait_start(WAIT_EVENT_WAL_SYNC);
    match wal_sync_method {
        WAL_SYNC_METHOD_FSYNC => {
            if pg_fsync_no_writethrough(fd) != 0 {
                msg = b"could not fsync file \"%s\": %m\0".as_ptr() as *const c_char;
            }
        }
        WAL_SYNC_METHOD_FSYNC_WRITETHROUGH => {
            if pg_fsync_writethrough(fd) != 0 {
                msg = b"could not fsync write-through file \"%s\": %m\0".as_ptr() as *const c_char;
            }
        }
        WAL_SYNC_METHOD_FDATASYNC => {
            if pg_fdatasync(fd) != 0 {
                msg = b"could not fdatasync file \"%s\": %m\0".as_ptr() as *const c_char;
            }
        }
        WAL_SYNC_METHOD_OPEN | WAL_SYNC_METHOD_OPEN_DSYNC => {
            /* not reachable */
            assert!(false);
        }
        _ => {
            ereport!(
                PANIC,
                errmsg!("unrecognized \"wal_sync_method\": {}", wal_sync_method)
                /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
            );
        }
    }

    /* PANIC if failed to fsync */
    if !msg.is_null() {
        let mut xlogfname: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
        let save_errno = *libc::__error();
        XLogFileName(xlogfname.as_mut_ptr(), tli, segno, wal_segment_size as uint32);
        *libc::__error() = save_errno;
        ereport!(
            PANIC,
            errmsg!(
                "{}: {}",
                core::ffi::CStr::from_ptr(msg).to_string_lossy(),
                core::ffi::CStr::from_ptr(xlogfname.as_ptr()).to_string_lossy()
            )
            /* errcode_for_file_access */
        );
    }

    pgstat_report_wait_end();

    pgstat_count_io_op_time(IOOBJECT_WAL, IOCONTEXT_NORMAL, IOOP_FSYNC, start, 1, 0);
}

/*
 * do_pg_backup_start is the workhorse of the user-visible pg_backup_start()
 * function. It creates the necessary starting checkpoint and constructs the
 * backup state and tablespace map.
 *
 * Input parameters are "state" (the backup state), "fast" (if true, we do
 * the checkpoint in immediate mode to make it faster), and "tablespaces"
 * (if non-NULL, indicates a list of tablespaceinfo structs describing the
 * cluster's tablespaces.).
 *
 * The tablespace map contents are appended to passed-in parameter
 * tablespace_map and the caller is responsible for including it in the backup
 * archive as 'tablespace_map'. The tablespace_map file is required mainly for
 * tar format in windows as native windows utilities are not able to create
 * symlinks while extracting files from tar. However for consistency and
 * platform-independence, we do it the same way everywhere.
 *
 * It fills in "state" with the information required for the backup, such
 * as the minimum WAL location that must be present to restore from this
 * backup (starttli) and the corresponding timeline ID (starttli).
 *
 * Every successfully started backup must be stopped by calling
 * do_pg_backup_stop() or do_pg_abort_backup(). There can be many
 * backups active at the same time.
 *
 * It is the responsibility of the caller of this function to verify the
 * permissions of the calling user!
 */
pub unsafe fn do_pg_backup_start(
    backupidstr: *const c_char,
    fast: bool,
    tablespaces: *mut *mut List,
    state: *mut BackupState,
    tblspcmapfile: *mut StringInfoData,
) {
    let mut backup_started_in_recovery: bool;

    assert!(!state.is_null());
    backup_started_in_recovery = RecoveryInProgress();

    /*
     * During recovery, we don't need to check WAL level. Because, if WAL
     * level is not sufficient, it's impossible to get here during recovery.
     */
    if !backup_started_in_recovery && !XLogIsNeeded() {
        ereport!(
            ERROR,
            errcode!(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!("WAL level not sufficient for making an online backup"),
            errhint!("\"wal_level\" must be set to \"replica\" or \"logical\" at server start.")
            /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
        );
    }

    if libc::strlen(backupidstr) > MAXPGPATH {
        ereport!(
            ERROR,
            errcode!(ERRCODE_INVALID_PARAMETER_VALUE),
            errmsg!("backup label too long (max {} bytes)", MAXPGPATH)
            /* errcode(ERRCODE_INVALID_PARAMETER_VALUE) */
        );
    }

    libc::strlcpy(
        (*state).name.as_mut_ptr(),
        backupidstr,
        core::mem::size_of_val(&(*state).name),
    );

    /*
     * Mark backup active in shared memory.  We must do full-page WAL writes
     * during an on-line backup even if not doing so at other times, because
     * it's quite possible for the backup dump to obtain a "torn" (partially
     * written) copy of a database page if it reads the page concurrently with
     * our write to the same page.  This can be fixed as long as the first
     * write to the page in the WAL sequence is a full-page write. Hence, we
     * increment runningBackups then force a CHECKPOINT, to ensure there are
     * no dirty pages in shared memory that might get dumped while the backup
     * is in progress without having a corresponding WAL record.  (Once the
     * backup is complete, we need not force full-page writes anymore, since
     * we expect that any pages not modified during the backup interval must
     * have been correctly captured by the backup.)
     *
     * Note that forcing full-page writes has no effect during an online
     * backup from the standby.
     *
     * We must hold all the insertion locks to change the value of
     * runningBackups, to ensure adequate interlocking against
     * XLogInsertRecord().
     */
    WALInsertLockAcquireExclusive();
    (*XLogCtl).Insert.runningBackups += 1;
    WALInsertLockRelease();

    /*
     * Ensure we decrement runningBackups if we fail below. NB -- for this to
     * work correctly, it is critical that sessionBackupState is only updated
     * after this block is over.
     */
    PG_ENSURE_ERROR_CLEANUP!(do_pg_abort_backup, DatumGetBool(true));
    {
        let mut gotUniqueStartpoint: bool = false;
        let mut tblspcdir: *mut DIR;
        let mut de: *mut dirent;
        let mut ti: *mut tablespaceinfo;
        let datadirpathlen: usize;

        /*
         * Force an XLOG file switch before the checkpoint, to ensure that the
         * WAL segment the checkpoint is written to doesn't contain pages with
         * old timeline IDs.  That would otherwise happen if you called
         * pg_backup_start() right after restoring from a PITR archive: the
         * first WAL segment containing the startup checkpoint has pages in
         * the beginning with the old timeline ID.  That can cause trouble at
         * recovery: we won't have a history file covering the old timeline if
         * pg_wal directory was not included in the base backup and the WAL
         * archive was cleared too before starting the backup.
         *
         * This also ensures that we have emitted a WAL page header that has
         * XLP_BKP_REMOVABLE off before we emit the checkpoint record.
         * Therefore, if a WAL archiver (such as pglesslog) is trying to
         * compress out removable backup blocks, it won't remove any that
         * occur after this point.
         *
         * During recovery, we skip forcing XLOG file switch, which means that
         * the backup taken during recovery is not available for the special
         * recovery case described above.
         */
        if !backup_started_in_recovery {
            RequestXLogSwitch(false);
        }

        loop {
            let mut checkpointfpw: bool;

            /*
             * Force a CHECKPOINT.  Aside from being necessary to prevent torn
             * page problems, this guarantees that two successive backup runs
             * will have different checkpoint positions and hence different
             * history file names, even if nothing happened in between.
             *
             * During recovery, establish a restartpoint if possible. We use
             * the last restartpoint as the backup starting checkpoint. This
             * means that two successive backup runs can have same checkpoint
             * positions.
             *
             * Since the fact that we are executing do_pg_backup_start()
             * during recovery means that checkpointer is running, we can use
             * RequestCheckpoint() to establish a restartpoint.
             *
             * We use CHECKPOINT_IMMEDIATE only if requested by user (via
             * passing fast = true).  Otherwise this can take awhile.
             */
            RequestCheckpoint(
                CHECKPOINT_FORCE
                    | CHECKPOINT_WAIT
                    | (if fast { CHECKPOINT_IMMEDIATE } else { 0 }),
            );

            /*
             * Now we need to fetch the checkpoint record location, and also
             * its REDO pointer.  The oldest point in WAL that would be needed
             * to restore starting from the checkpoint is precisely the REDO
             * pointer.
             */
            LWLockAcquire(ControlFileLock, LW_SHARED);
            (*state).checkpointloc = (*ControlFile).checkPoint;
            (*state).startpoint = (*ControlFile).checkPointCopy.redo;
            (*state).starttli = (*ControlFile).checkPointCopy.ThisTimeLineID;
            checkpointfpw = (*ControlFile).checkPointCopy.fullPageWrites;
            LWLockRelease(ControlFileLock);

            if backup_started_in_recovery {
                let mut recptr: XLogRecPtr;

                /*
                 * Check to see if all WAL replayed during online backup
                 * (i.e., since last restartpoint used as backup starting
                 * checkpoint) contain full-page writes.
                 */
                SpinLockAcquire(&mut (*XLogCtl).info_lck);
                recptr = (*XLogCtl).lastFpwDisableRecPtr;
                SpinLockRelease(&mut (*XLogCtl).info_lck);

                if !checkpointfpw || (*state).startpoint <= recptr {
                    ereport!(
                        ERROR,
                        errcode!(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                        errmsg!("WAL generated with \"full_page_writes=off\" was replayed since last restartpoint"),
                        errhint!("This means that the backup being taken on the standby is corrupt and should not be used. Enable \"full_page_writes\" and run CHECKPOINT on the primary, and then try an online backup again.")
                        /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
                    );
                }

                /*
                 * During recovery, since we don't use the end-of-backup WAL
                 * record and don't write the backup history file, the
                 * starting WAL location doesn't need to be unique. This means
                 * that two successive backup runs can have same checkpoint
                 * positions.
                 */
                gotUniqueStartpoint = true;
            }

            /*
             * If two base backups are started at the same time (in WAL sender
             * processes), we need to make sure that they use different
             * checkpoints as starting locations, because we use the starting
             * WAL location as a unique identifier for the base backup in the
             * end-of-backup WAL record and when we write the backup history
             * file. Perhaps it would be better generate a separate unique ID
             * for each backup instead of forcing another checkpoint, but
             * taking a checkpoint right after another is not that expensive
             * either because only few buffers have been dirtied yet.
             */
            WALInsertLockAcquireExclusive();
            if (*XLogCtl).Insert.lastBackupStart < (*state).startpoint {
                (*XLogCtl).Insert.lastBackupStart = (*state).startpoint;
                gotUniqueStartpoint = true;
            }
            WALInsertLockRelease();

            if gotUniqueStartpoint {
                break;
            }
        } /* loop */

        /*
         * Construct tablespace_map file.
         */
        datadirpathlen = libc::strlen(DataDir);

        /* Collect information about all tablespaces */
        tblspcdir = AllocateDir(PG_TBLSPC_DIR.as_ptr() as *const c_char);
        loop {
            de = ReadDir(tblspcdir, PG_TBLSPC_DIR.as_ptr() as *const c_char);
            if de.is_null() {
                break;
            }
            let mut fullpath: [c_char; MAXPGPATH + PG_TBLSPC_DIR.len()] =
                [0; MAXPGPATH + PG_TBLSPC_DIR.len()];
            let mut linkpath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
            let mut relpath: *mut c_char = ptr::null_mut();
            let mut de_type: PGFileType;
            let mut badp: *mut c_char;
            let mut tsoid: Oid;

            /*
             * Try to parse the directory name as an unsigned integer.
             *
             * Tablespace directories should be positive integers that can be
             * represented in 32 bits, with no leading zeroes or trailing
             * garbage. If we come across a name that doesn't meet those
             * criteria, skip it.
             */
            if (*de).d_name[0] < b'1' as c_char || (*de).d_name[1] > b'9' as c_char {
                continue;
            }
            *libc::__error() = 0;
            tsoid = libc::strtoul((*de).d_name.as_ptr(), &mut badp, 10) as Oid;
            if *badp != 0 || *libc::__error() == libc::EINVAL || *libc::__error() == libc::ERANGE {
                continue;
            }

            libc::snprintf(
                fullpath.as_mut_ptr(),
                fullpath.len(),
                b"%s/%s\0".as_ptr() as *const c_char,
                PG_TBLSPC_DIR.as_ptr(),
                (*de).d_name.as_ptr(),
            );

            de_type = get_dirent_type(fullpath.as_ptr(), de, false, ERROR);

            if de_type == PGFILETYPE_LNK {
                let mut escapedpath: StringInfoData = core::mem::zeroed();
                let mut rllen: c_int;
                let mut s: *mut c_char;

                rllen = libc::readlink(
                    fullpath.as_ptr(),
                    linkpath.as_mut_ptr(),
                    linkpath.len(),
                ) as c_int;
                if rllen < 0 {
                    ereport!(
                        WARNING,
                        errmsg!("could not read symbolic link \"{}\": {}",
                            core::ffi::CStr::from_ptr(fullpath.as_ptr()).to_string_lossy(),
                            core::ffi::CStr::from_ptr(libc::strerror(*libc::__error())).to_string_lossy()
                        )
                        /* errmsg("could not read symbolic link \"%s\": %m", fullpath) */
                    );
                    continue;
                } else if rllen >= linkpath.len() as c_int {
                    ereport!(
                        WARNING,
                        errmsg!("symbolic link \"{}\" target is too long",
                            core::ffi::CStr::from_ptr(fullpath.as_ptr()).to_string_lossy()
                        )
                        /* errmsg("symbolic link \"%s\" target is too long", fullpath) */
                    );
                    continue;
                }
                linkpath[rllen as usize] = 0;

                /*
                 * Relpath holds the relative path of the tablespace directory
                 * when it's located within PGDATA, or NULL if it's located
                 * elsewhere.
                 */
                if rllen as usize > datadirpathlen
                    && libc::strncmp(linkpath.as_ptr(), DataDir, datadirpathlen) == 0
                    && IS_DIR_SEP!(linkpath[datadirpathlen] as u8)
                {
                    relpath = pstrdup(linkpath.as_ptr().add(datadirpathlen + 1));
                }

                /*
                 * Add a backslash-escaped version of the link path to the
                 * tablespace map file.
                 */
                initStringInfo(&mut escapedpath);
                s = linkpath.as_mut_ptr();
                while *s != 0 {
                    if *s == b'\n' as c_char || *s == b'\r' as c_char || *s == b'\\' as c_char {
                        appendStringInfoChar(&mut escapedpath, b'\\' as c_char);
                    }
                    appendStringInfoChar(&mut escapedpath, *s);
                    s = s.add(1);
                }
                appendStringInfo(
                    tblspcmapfile,
                    b"%s %s\n\0".as_ptr() as *const c_char,
                    (*de).d_name.as_ptr(),
                    escapedpath.data,
                );
                pfree(escapedpath.data as *mut c_void);
            } else if de_type == PGFILETYPE_DIR {
                /*
                 * It's possible to use allow_in_place_tablespaces to create
                 * directories directly under pg_tblspc, for testing purposes
                 * only.
                 *
                 * In this case, we store a relative path rather than an
                 * absolute path into the tablespaceinfo.
                 */
                libc::snprintf(
                    linkpath.as_mut_ptr(),
                    linkpath.len(),
                    b"%s/%s\0".as_ptr() as *const c_char,
                    PG_TBLSPC_DIR.as_ptr(),
                    (*de).d_name.as_ptr(),
                );
                relpath = pstrdup(linkpath.as_ptr());
            } else {
                /* Skip any other file type that appears here. */
                continue;
            }

            ti = palloc(core::mem::size_of::<tablespaceinfo>()) as *mut tablespaceinfo;
            (*ti).oid = tsoid;
            (*ti).path = pstrdup(linkpath.as_ptr());
            (*ti).rpath = relpath;
            (*ti).size = -1;

            if !tablespaces.is_null() {
                *tablespaces = lappend(*tablespaces, ti as *mut c_void);
            }
        } /* loop ReadDir */
        FreeDir(tblspcdir);

        (*state).starttime = libc::time(ptr::null_mut()) as pg_time_t;
    }
    PG_END_ENSURE_ERROR_CLEANUP!(do_pg_abort_backup, DatumGetBool(true));

    (*state).started_in_recovery = backup_started_in_recovery;

    /*
     * Mark that the start phase has correctly finished for the backup.
     */
    sessionBackupState = SESSION_BACKUP_RUNNING;
}

/*
 * Utility routine to fetch the session-level status of a backup running.
 */
pub unsafe fn get_backup_status() -> SessionBackupState {
    sessionBackupState
}

/*
 * do_pg_backup_stop
 *
 * Utility function called at the end of an online backup.  It creates history
 * file (if required), resets sessionBackupState and so on.  It can optionally
 * wait for WAL segments to be archived.
 *
 * "state" is filled with the information necessary to restore from this
 * backup with its stop LSN (stoppoint), its timeline ID (stoptli), etc.
 *
 * It is the responsibility of the caller of this function to verify the
 * permissions of the calling user!
 */
pub unsafe fn do_pg_backup_stop(state: *mut BackupState, waitforarchive: bool) {
    let mut backup_stopped_in_recovery: bool = false;
    let mut histfilepath: [c_char; MAXPGPATH] = [0; MAXPGPATH];
    let mut lastxlogfilename: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let mut histfilename: [c_char; MAXFNAMELEN] = [0; MAXFNAMELEN];
    let mut _logSegNo: XLogSegNo = 0;
    let mut fp: *mut FILE;
    let mut seconds_before_warning: c_int;
    let mut waits: c_int = 0;
    let mut reported_waiting: bool = false;

    assert!(!state.is_null());

    backup_stopped_in_recovery = RecoveryInProgress();

    /*
     * During recovery, we don't need to check WAL level. Because, if WAL
     * level is not sufficient, it's impossible to get here during recovery.
     */
    if !backup_stopped_in_recovery && !XLogIsNeeded() {
        ereport!(
            ERROR,
            errcode!(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!("WAL level not sufficient for making an online backup"),
            errhint!("\"wal_level\" must be set to \"replica\" or \"logical\" at server start.")
            /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
        );
    }

    /*
     * OK to update backup counter and session-level lock.
     *
     * Note that CHECK_FOR_INTERRUPTS() must not occur while updating them,
     * otherwise they can be updated inconsistently, which might cause
     * do_pg_abort_backup() to fail.
     */
    WALInsertLockAcquireExclusive();

    /*
     * It is expected that each do_pg_backup_start() call is matched by
     * exactly one do_pg_backup_stop() call.
     */
    assert!((*XLogCtl).Insert.runningBackups > 0);
    (*XLogCtl).Insert.runningBackups -= 1;

    /*
     * Clean up session-level lock.
     *
     * You might think that WALInsertLockRelease() can be called before
     * cleaning up session-level lock because session-level lock doesn't need
     * to be protected with WAL insertion lock. But since
     * CHECK_FOR_INTERRUPTS() can occur in it, session-level lock must be
     * cleaned up before it.
     */
    sessionBackupState = SESSION_BACKUP_NONE;

    WALInsertLockRelease();

    /*
     * If we are taking an online backup from the standby, we confirm that the
     * standby has not been promoted during the backup.
     */
    if (*state).started_in_recovery && !backup_stopped_in_recovery {
        ereport!(
            ERROR,
            errcode!(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
            errmsg!("the standby was promoted during online backup"),
            errhint!("This means that the backup being taken is corrupt and should not be used. Try taking another online backup.")
            /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
        );
    }

    /*
     * During recovery, we don't write an end-of-backup record. We assume that
     * pg_control was backed up last and its minimum recovery point can be
     * available as the backup end location. Since we don't have an
     * end-of-backup record, we use the pg_control value to check whether
     * we've reached the end of backup when starting recovery from this
     * backup. We have no way of checking if pg_control wasn't backed up last
     * however.
     *
     * We don't force a switch to new WAL file but it is still possible to
     * wait for all the required files to be archived if waitforarchive is
     * true. This is okay if we use the backup to start a standby and fetch
     * the missing WAL using streaming replication. But in the case of an
     * archive recovery, a user should set waitforarchive to true and wait for
     * them to be archived to ensure that all the required files are
     * available.
     *
     * We return the current minimum recovery point as the backup end
     * location. Note that it can be greater than the exact backup end
     * location if the minimum recovery point is updated after the backup of
     * pg_control. This is harmless for current uses.
     *
     * XXX currently a backup history file is for informational and debug
     * purposes only. It's not essential for an online backup. Furthermore,
     * even if it's created, it will not be archived during recovery because
     * an archiver is not invoked. So it doesn't seem worthwhile to write a
     * backup history file during recovery.
     */
    if backup_stopped_in_recovery {
        let mut recptr: XLogRecPtr;

        /*
         * Check to see if all WAL replayed during online backup contain
         * full-page writes.
         */
        SpinLockAcquire(&mut (*XLogCtl).info_lck);
        recptr = (*XLogCtl).lastFpwDisableRecPtr;
        SpinLockRelease(&mut (*XLogCtl).info_lck);

        if (*state).startpoint <= recptr {
            ereport!(
                ERROR,
                errcode!(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
                errmsg!("WAL generated with \"full_page_writes=off\" was replayed during online backup"),
                errhint!("This means that the backup being taken on the standby is corrupt and should not be used. Enable \"full_page_writes\" and run CHECKPOINT on the primary, and then try an online backup again.")
                /* errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE) */
            );
        }

        LWLockAcquire(ControlFileLock, LW_SHARED);
        (*state).stoppoint = (*ControlFile).minRecoveryPoint;
        (*state).stoptli = (*ControlFile).minRecoveryPointTLI;
        LWLockRelease(ControlFileLock);
    } else {
        let mut history_file: *mut c_char;

        /*
         * Write the backup-end xlog record
         */
        XLogBeginInsert();
        XLogRegisterData(
            &mut (*state).startpoint as *mut XLogRecPtr as *const c_char,
            core::mem::size_of::<XLogRecPtr>() as c_int,
        );
        (*state).stoppoint = XLogInsert(RM_XLOG_ID, XLOG_BACKUP_END);

        /*
         * Given that we're not in recovery, InsertTimeLineID is set and can't
         * change, so we can read it without a lock.
         */
        (*state).stoptli = (*XLogCtl).InsertTimeLineID;

        /*
         * Force a switch to a new xlog segment file, so that the backup is
         * valid as soon as archiver moves out the current segment file.
         */
        RequestXLogSwitch(false);

        (*state).stoptime = libc::time(ptr::null_mut()) as pg_time_t;

        /*
         * Write the backup history file
         */
        XLByteToSeg!((*state).startpoint, _logSegNo, wal_segment_size as XLogSegNo);
        BackupHistoryFilePath(
            histfilepath.as_mut_ptr(),
            (*state).stoptli,
            _logSegNo,
            (*state).startpoint,
            wal_segment_size as uint32,
        );
        fp = AllocateFile(
            histfilepath.as_ptr(),
            b"w\0".as_ptr() as *const c_char,
        );
        if fp.is_null() {
            ereport!(
                ERROR,
                errmsg!("could not create file \"{}\": {}",
                    core::ffi::CStr::from_ptr(histfilepath.as_ptr()).to_string_lossy(),
                    core::ffi::CStr::from_ptr(libc::strerror(*libc::__error())).to_string_lossy()
                )
                /* errcode_for_file_access,
                   errmsg("could not create file \"%s\": %m", histfilepath) */
            );
        }

        /* Build and save the contents of the backup history file */
        history_file = build_backup_content(state, true);
        libc::fprintf(fp, b"%s\0".as_ptr() as *const c_char, history_file);
        pfree(history_file as *mut c_void);

        if libc::fflush(fp) != 0 || libc::ferror(fp) != 0 || FreeFile(fp) != 0 {
            ereport!(
                ERROR,
                errmsg!("could not write file \"{}\"",
                    core::ffi::CStr::from_ptr(histfilepath.as_ptr()).to_string_lossy()
                )
                /* errcode_for_file_access,
                   errmsg("could not write file \"%s\": %m", histfilepath) */
            );
        }

        /*
         * Clean out any no-longer-needed history files.  As a side effect,
         * this will post a .ready file for the newly created history file,
         * notifying the archiver that history file may be archived
         * immediately.
         */
        CleanupBackupHistory();
    }

    /*
     * If archiving is enabled, wait for all the required WAL files to be
     * archived before returning. If archiving isn't enabled, the required WAL
     * needs to be transported via streaming replication (hopefully with
     * wal_keep_size set high enough), or some more exotic mechanism like
     * polling and copying files from pg_wal with script. We have no knowledge
     * of those mechanisms, so it's up to the user to ensure that he gets all
     * the required WAL.
     *
     * We wait until both the last WAL file filled during backup and the
     * history file have been archived, and assume that the alphabetic sorting
     * property of the WAL files ensures any earlier WAL files are safely
     * archived as well.
     *
     * We wait forever, since archive_command is supposed to work and we
     * assume the admin wanted his backup to work completely. If you don't
     * wish to wait, then either waitforarchive should be passed in as false,
     * or you can set statement_timeout.  Also, some notices are issued to
     * clue in anyone who might be doing this interactively.
     */

    if waitforarchive
        && ((!backup_stopped_in_recovery && XLogArchivingActive())
            || (backup_stopped_in_recovery && XLogArchivingAlways()))
    {
        XLByteToPrevSeg!((*state).stoppoint, _logSegNo, wal_segment_size as XLogSegNo);
        XLogFileName(
            lastxlogfilename.as_mut_ptr(),
            (*state).stoptli,
            _logSegNo,
            wal_segment_size as uint32,
        );

        XLByteToSeg!((*state).startpoint, _logSegNo, wal_segment_size as XLogSegNo);
        BackupHistoryFileName(
            histfilename.as_mut_ptr(),
            (*state).stoptli,
            _logSegNo,
            (*state).startpoint,
            wal_segment_size as uint32,
        );

        seconds_before_warning = 60;
        waits = 0;

        while XLogArchiveIsBusy(lastxlogfilename.as_ptr()) != 0
            || XLogArchiveIsBusy(histfilename.as_ptr()) != 0
        {
            CHECK_FOR_INTERRUPTS!();

            if !reported_waiting && waits > 5 {
                ereport!(
                    NOTICE,
                    errmsg!("base backup done, waiting for required WAL segments to be archived")
                    /* errmsg("base backup done, waiting for required WAL segments to be archived") */
                );
                reported_waiting = true;
            }

            let _ = WaitLatch(
                MyLatch,
                WL_LATCH_SET | WL_TIMEOUT | WL_EXIT_ON_PM_DEATH,
                1000,
                WAIT_EVENT_BACKUP_WAIT_WAL_ARCHIVE,
            );
            ResetLatch(MyLatch);

            waits += 1;
            if waits >= seconds_before_warning {
                seconds_before_warning *= 2; /* This wraps in >10 years... */
                ereport!(
                    WARNING,
                    errmsg!("still waiting for all required WAL segments to be archived ({} seconds elapsed)",
                        waits
                    ),
                    errhint!("Check that your \"archive_command\" is executing properly.  You can safely cancel this backup, but the database backup will not be usable without all the WAL segments.")
                    /* errmsg(...), errhint(...) */
                );
            }
        }

        ereport!(
            NOTICE,
            errmsg!("all required WAL segments have been archived")
        );
    } else if waitforarchive {
        ereport!(
            NOTICE,
            errmsg!("WAL archiving is not enabled; you must ensure that all required WAL segments are copied through other means to complete the backup")
        );
    }
}

/*
 * do_pg_abort_backup: abort a running backup
 *
 * This does just the most basic steps of do_pg_backup_stop(), by taking the
 * system out of backup mode, thus making it a lot more safe to call from
 * an error handler.
 *
 * 'arg' indicates that it's being called during backup setup; so
 * sessionBackupState has not been modified yet, but runningBackups has
 * already been incremented.  When it's false, then it's invoked as a
 * before_shmem_exit handler, and therefore we must not change state
 * unless sessionBackupState indicates that a backup is actually running.
 *
 * NB: This gets used as a PG_ENSURE_ERROR_CLEANUP callback and
 * before_shmem_exit handler, hence the odd-looking signature.
 */
pub unsafe extern "C" fn do_pg_abort_backup(code: c_int, arg: Datum) {
    let during_backup_start: bool = DatumGetBool(arg);

    /* If called during backup start, there shouldn't be one already running */
    assert!(!during_backup_start || sessionBackupState == SESSION_BACKUP_NONE);

    if during_backup_start || sessionBackupState != SESSION_BACKUP_NONE {
        WALInsertLockAcquireExclusive();
        assert!((*XLogCtl).Insert.runningBackups > 0);
        (*XLogCtl).Insert.runningBackups -= 1;

        sessionBackupState = SESSION_BACKUP_NONE;
        WALInsertLockRelease();

        if !during_backup_start {
            ereport!(
                WARNING,
                errmsg!("aborting backup due to backend exiting before pg_backup_stop was called")
            );
        }
    }
}

/*
 * Register a handler that will warn about unterminated backups at end of
 * session, unless this has already been done.
 */
pub unsafe fn register_persistent_abort_backup_handler() {
    static mut already_done: bool = false;

    if already_done {
        return;
    }
    before_shmem_exit(do_pg_abort_backup, DatumGetBool(false));
    already_done = true;
}

/*
 * Get latest WAL insert pointer
 */
pub unsafe fn GetXLogInsertRecPtr() -> XLogRecPtr {
    let Insert: *mut XLogCtlInsert = &mut (*XLogCtl).Insert;
    let current_bytepos: uint64;

    SpinLockAcquire(&mut (*Insert).insertpos_lck);
    current_bytepos = (*Insert).CurrBytePos;
    SpinLockRelease(&mut (*Insert).insertpos_lck);

    XLogBytePosToRecPtr(current_bytepos)
}

/*
 * Get latest WAL write pointer
 */
pub unsafe fn GetXLogWriteRecPtr() -> XLogRecPtr {
    RefreshXLogWriteResult!(LogwrtResult);

    LogwrtResult.Write
}

/*
 * Returns the redo pointer of the last checkpoint or restartpoint. This is
 * the oldest point in WAL that we still need, if we have to restart recovery.
 */
pub unsafe fn GetOldestRestartPoint(oldrecptr: *mut XLogRecPtr, oldtli: *mut TimeLineID) {
    LWLockAcquire(ControlFileLock, LW_SHARED);
    *oldrecptr = (*ControlFile).checkPointCopy.redo;
    *oldtli = (*ControlFile).checkPointCopy.ThisTimeLineID;
    LWLockRelease(ControlFileLock);
}

/* Thin wrapper around ShutdownWalRcv(). */
pub unsafe fn XLogShutdownWalRcv() {
    ShutdownWalRcv();
    ResetInstallXLogFileSegmentActive();
}

/* Enable WAL file recycling and preallocation. */
pub unsafe fn SetInstallXLogFileSegmentActive() {
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    (*XLogCtl).InstallXLogFileSegmentActive = true;
    LWLockRelease(ControlFileLock);
}

/* Disable WAL file recycling and preallocation. */
pub unsafe fn ResetInstallXLogFileSegmentActive() {
    LWLockAcquire(ControlFileLock, LW_EXCLUSIVE);
    (*XLogCtl).InstallXLogFileSegmentActive = false;
    LWLockRelease(ControlFileLock);
}

pub unsafe fn IsInstallXLogFileSegmentActive() -> bool {
    let mut result: bool;

    LWLockAcquire(ControlFileLock, LW_SHARED);
    result = (*XLogCtl).InstallXLogFileSegmentActive;
    LWLockRelease(ControlFileLock);

    result
}

/*
 * Update the WalWriterSleeping flag.
 */
pub unsafe fn SetWalWriterSleeping(sleeping: bool) {
    SpinLockAcquire(&mut (*XLogCtl).info_lck);
    (*XLogCtl).WalWriterSleeping = sleeping;
    SpinLockRelease(&mut (*XLogCtl).info_lck);
}
