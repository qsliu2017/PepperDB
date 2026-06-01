// ----------
// wait_event.c
//	  Wait event reporting infrastructure.
//
// Copyright (c) 2001-2025, PostgreSQL Global Development Group
//
//
// IDENTIFICATION
//	  src/backend/utils/activity/wait_event.c
//
// NOTES
//
// To make pgstat_report_wait_start() and pgstat_report_wait_end() as
// lightweight as possible, they do not check if shared memory (MyProc
// specifically, where the wait event is stored) is already available. Instead
// we initially set my_wait_event_info to a process local variable, which then
// is redirected to shared memory using pgstat_set_wait_event_storage(). For
// the same reason pgstat_track_activities is not checked - the check adds
// more work than it saves.
//
// ----------

use crate::prelude::*;

use std::ffi::{c_char, c_int, c_long, c_void};

// #include "storage/lmgr.h"   /* for GetLockNameFromTagType */
// #include "storage/lwlock.h" /* for GetLWLockIdentifier */
// #include "storage/spin.h"
// #include "utils/wait_event.h"
use crate::storage::lmgr::s_lock::slock_t;
use crate::storage::spin::{SpinLockAcquire, SpinLockInit, SpinLockRelease};
use crate::utils::hash::dynahash::{
    hash_estimate_size, hash_get_num_entries, hash_search, hash_seq_init, hash_seq_search,
    HASHACTION, HASHCTL, HASH_BLOBS, HASH_ELEM, HASH_SEQ_STATUS, HASH_STRINGS, HTAB,
};
use crate::utils::wait_classes::{
    PG_WAIT_ACTIVITY, PG_WAIT_BUFFERPIN, PG_WAIT_CLIENT, PG_WAIT_EXTENSION, PG_WAIT_INJECTIONPOINT,
    PG_WAIT_IO, PG_WAIT_IPC, PG_WAIT_LOCK, PG_WAIT_LWLOCK, PG_WAIT_TIMEOUT,
};

use crate::pg_config_manual::NAMEDATALEN;

extern "C" {
    fn strlen(s: *const c_char) -> usize;
}

// The wait event enums (WaitEventActivity, WaitEventBufferPin, WaitEventClient,
// WaitEventIPC, WaitEventTimeout, WaitEventIO) are produced at build time into
// the generated header "utils/wait_event_types.h".  That generated header is
// not part of the port yet; faithful aliases to uint32 stand in for them.
pub type WaitEventActivity = uint32;
pub type WaitEventBufferPin = uint32;
pub type WaitEventClient = uint32;
pub type WaitEventIPC = uint32;
pub type WaitEventTimeout = uint32;
pub type WaitEventIO = uint32;

// ----------------------------------------------------------------
// hash table entry structs and shared counter (from wait_event.c)
// ----------------------------------------------------------------

#[allow(non_upper_case_globals)]
static mut local_my_wait_event_info: uint32 = 0;
pub static mut my_wait_event_info: *mut uint32 = &raw mut local_my_wait_event_info;

const WAIT_EVENT_CLASS_MASK: uint32 = 0xFF000000;
const WAIT_EVENT_ID_MASK: uint32 = 0x0000FFFF;

/*
 * Hash tables for storing custom wait event ids and their names in
 * shared memory.
 *
 * WaitEventCustomHashByInfo is used to find the name from wait event
 * information.  Any backend can search it to find custom wait events.
 *
 * WaitEventCustomHashByName is used to find the wait event information from a
 * name.  It is used to ensure that no duplicated entries are registered.
 *
 * For simplicity, we use the same ID counter across types of custom events.
 * We could end that anytime the need arises.
 *
 * The size of the hash table is based on the assumption that
 * WAIT_EVENT_CUSTOM_HASH_INIT_SIZE is enough for most cases, and it seems
 * unlikely that the number of entries will reach
 * WAIT_EVENT_CUSTOM_HASH_MAX_SIZE.
 */
static mut WaitEventCustomHashByInfo: *mut HTAB = null_mut(); /* find names from infos */
static mut WaitEventCustomHashByName: *mut HTAB = null_mut(); /* find infos from names */

const WAIT_EVENT_CUSTOM_HASH_INIT_SIZE: c_long = 16;
const WAIT_EVENT_CUSTOM_HASH_MAX_SIZE: c_int = 128;

/* hash table entries */
#[repr(C)]
struct WaitEventCustomEntryByInfo {
    wait_event_info: uint32,                    /* hash key */
    wait_event_name: [c_char; NAMEDATALEN as usize], /* custom wait event name */
}

#[repr(C)]
struct WaitEventCustomEntryByName {
    wait_event_name: [c_char; NAMEDATALEN as usize], /* hash key */
    wait_event_info: uint32,
}

/* dynamic allocation counter for custom wait events */
#[repr(C)]
struct WaitEventCustomCounterData {
    nextId: c_int,    /* next ID to assign */
    mutex: slock_t,   /* protects the counter */
}

/* pointer to the shared memory */
static mut WaitEventCustomCounter: *mut WaitEventCustomCounterData = null_mut();

/* first event ID of custom wait events */
const WAIT_EVENT_CUSTOM_INITIAL_ID: c_int = 1;

/*
 *  Return the space for dynamic shared hash tables and dynamic allocation counter.
 */
pub unsafe fn WaitEventCustomShmemSize() -> Size {
    let mut sz: Size;

    sz = MAXALIGN(std::mem::size_of::<WaitEventCustomCounterData>());
    sz = add_size(
        sz,
        hash_estimate_size(
            WAIT_EVENT_CUSTOM_HASH_MAX_SIZE as c_long,
            std::mem::size_of::<WaitEventCustomEntryByInfo>(),
        ),
    );
    sz = add_size(
        sz,
        hash_estimate_size(
            WAIT_EVENT_CUSTOM_HASH_MAX_SIZE as c_long,
            std::mem::size_of::<WaitEventCustomEntryByName>(),
        ),
    );
    sz
}

/*
 * Allocate shmem space for dynamic shared hash and dynamic allocation counter.
 */
pub unsafe fn WaitEventCustomShmemInit() {
    let mut found: bool = false;
    let mut info: HASHCTL = std::mem::zeroed();

    WaitEventCustomCounter = ShmemInitStruct(
        c"WaitEventCustomCounterData".as_ptr(),
        std::mem::size_of::<WaitEventCustomCounterData>(),
        &mut found,
    ) as *mut WaitEventCustomCounterData;

    if !found {
        /* initialize the allocation counter and its spinlock. */
        (*WaitEventCustomCounter).nextId = WAIT_EVENT_CUSTOM_INITIAL_ID;
        SpinLockInit(&mut (*WaitEventCustomCounter).mutex);
    }

    /* initialize or attach the hash tables to store custom wait events */
    info.keysize = std::mem::size_of::<uint32>();
    info.entrysize = std::mem::size_of::<WaitEventCustomEntryByInfo>();
    WaitEventCustomHashByInfo = ShmemInitHash(
        c"WaitEventCustom hash by wait event information".as_ptr(),
        WAIT_EVENT_CUSTOM_HASH_INIT_SIZE,
        WAIT_EVENT_CUSTOM_HASH_MAX_SIZE as c_long,
        &mut info,
        HASH_ELEM | HASH_BLOBS,
    );

    /* key is a NULL-terminated string */
    info.keysize = std::mem::size_of::<[c_char; NAMEDATALEN as usize]>();
    info.entrysize = std::mem::size_of::<WaitEventCustomEntryByName>();
    WaitEventCustomHashByName = ShmemInitHash(
        c"WaitEventCustom hash by name".as_ptr(),
        WAIT_EVENT_CUSTOM_HASH_INIT_SIZE,
        WAIT_EVENT_CUSTOM_HASH_MAX_SIZE as c_long,
        &mut info,
        HASH_ELEM | HASH_STRINGS,
    );
}

/*
 * Allocate a new event ID and return the wait event info.
 *
 * If the wait event name is already defined, this does not allocate a new
 * entry; it returns the wait event information associated to the name.
 */
pub unsafe fn WaitEventExtensionNew(wait_event_name: *const c_char) -> uint32 {
    WaitEventCustomNew(PG_WAIT_EXTENSION, wait_event_name)
}

pub unsafe fn WaitEventInjectionPointNew(wait_event_name: *const c_char) -> uint32 {
    WaitEventCustomNew(PG_WAIT_INJECTIONPOINT, wait_event_name)
}

unsafe fn WaitEventCustomNew(classId: uint32, wait_event_name: *const c_char) -> uint32 {
    let eventId: uint16;
    let mut found: bool = false;
    let mut entry_by_name: *mut WaitEventCustomEntryByName;
    let entry_by_info: *mut WaitEventCustomEntryByInfo;
    let wait_event_info: uint32;

    /* Check the limit of the length of the event name */
    if strlen(wait_event_name) >= NAMEDATALEN as usize {
        elog!(
            ERROR,
            "cannot use custom wait event string longer than {} characters",
            NAMEDATALEN - 1
        );
    }

    /*
     * Check if the wait event info associated to the name is already defined,
     * and return it if so.
     */
    LWLockAcquire(WaitEventCustomLock, LW_SHARED);
    entry_by_name = hash_search(
        WaitEventCustomHashByName,
        wait_event_name as *const c_void,
        HASHACTION::HASH_FIND,
        &mut found,
    ) as *mut WaitEventCustomEntryByName;
    LWLockRelease(WaitEventCustomLock);
    if found {
        let oldClassId: uint32;

        oldClassId = (*entry_by_name).wait_event_info & WAIT_EVENT_CLASS_MASK;
        if oldClassId != classId {
            ereport!(ERROR, "wait event already exists in another type");
            unreachable!();
        }
        return (*entry_by_name).wait_event_info;
    }

    /*
     * Allocate and register a new wait event.  Recheck if the event name
     * exists, as it could be possible that a concurrent process has inserted
     * one with the same name since the LWLock acquired again here was
     * previously released.
     */
    LWLockAcquire(WaitEventCustomLock, LW_EXCLUSIVE);
    entry_by_name = hash_search(
        WaitEventCustomHashByName,
        wait_event_name as *const c_void,
        HASHACTION::HASH_FIND,
        &mut found,
    ) as *mut WaitEventCustomEntryByName;
    if found {
        let oldClassId: uint32;

        LWLockRelease(WaitEventCustomLock);
        oldClassId = (*entry_by_name).wait_event_info & WAIT_EVENT_CLASS_MASK;
        if oldClassId != classId {
            ereport!(ERROR, "wait event already exists in another type");
            unreachable!();
        }
        return (*entry_by_name).wait_event_info;
    }

    /* Allocate a new event Id */
    SpinLockAcquire(&mut (*WaitEventCustomCounter).mutex);

    if (*WaitEventCustomCounter).nextId >= WAIT_EVENT_CUSTOM_HASH_MAX_SIZE {
        SpinLockRelease(&mut (*WaitEventCustomCounter).mutex);
        ereport!(ERROR, "too many custom wait events");
        unreachable!();
    }

    eventId = (*WaitEventCustomCounter).nextId as uint16;
    (*WaitEventCustomCounter).nextId += 1;

    SpinLockRelease(&mut (*WaitEventCustomCounter).mutex);

    /* Register the new wait event */
    wait_event_info = classId | eventId as uint32;
    entry_by_info = hash_search(
        WaitEventCustomHashByInfo,
        &wait_event_info as *const uint32 as *const c_void,
        HASHACTION::HASH_ENTER,
        &mut found,
    ) as *mut WaitEventCustomEntryByInfo;
    Assert!(!found);
    crate::port::strlcpy::strlcpy(
        (*entry_by_info).wait_event_name.as_mut_ptr(),
        wait_event_name,
        std::mem::size_of_val(&(*entry_by_info).wait_event_name),
    );

    entry_by_name = hash_search(
        WaitEventCustomHashByName,
        wait_event_name as *const c_void,
        HASHACTION::HASH_ENTER,
        &mut found,
    ) as *mut WaitEventCustomEntryByName;
    Assert!(!found);
    (*entry_by_name).wait_event_info = wait_event_info;

    LWLockRelease(WaitEventCustomLock);

    wait_event_info
}

/*
 * Return the name of a custom wait event information.
 */
unsafe fn GetWaitEventCustomIdentifier(wait_event_info: uint32) -> *const c_char {
    let mut found: bool = false;
    let entry: *mut WaitEventCustomEntryByInfo;

    /* Built-in event? */
    if wait_event_info == PG_WAIT_EXTENSION {
        return c"Extension".as_ptr();
    }

    /* It is a user-defined wait event, so lookup hash table. */
    LWLockAcquire(WaitEventCustomLock, LW_SHARED);
    entry = hash_search(
        WaitEventCustomHashByInfo,
        &wait_event_info as *const uint32 as *const c_void,
        HASHACTION::HASH_FIND,
        &mut found,
    ) as *mut WaitEventCustomEntryByInfo;
    LWLockRelease(WaitEventCustomLock);

    if entry.is_null() {
        elog!(
            ERROR,
            "could not find custom name for wait event information {}",
            wait_event_info
        );
    }

    (*entry).wait_event_name.as_ptr()
}

/*
 * Returns a list of currently defined custom wait event names.  The result is
 * a palloc'd array, with the number of elements saved in *nwaitevents.
 */
pub unsafe fn GetWaitEventCustomNames(classId: uint32, nwaitevents: *mut c_int) -> *mut *mut c_char {
    let waiteventnames: *mut *mut c_char;
    let mut hentry: *mut WaitEventCustomEntryByName;
    let mut hash_seq: HASH_SEQ_STATUS = std::mem::zeroed();
    let mut index: c_int;
    let els: c_int;

    LWLockAcquire(WaitEventCustomLock, LW_SHARED);

    /* Now we can safely count the number of entries */
    els = hash_get_num_entries(WaitEventCustomHashByName) as c_int;

    /* Allocate enough space for all entries */
    waiteventnames =
        palloc(els as Size * std::mem::size_of::<*mut c_char>()) as *mut *mut c_char;

    /* Now scan the hash table to copy the data */
    hash_seq_init(&mut hash_seq, WaitEventCustomHashByName);

    index = 0;
    loop {
        hentry = hash_seq_search(&mut hash_seq) as *mut WaitEventCustomEntryByName;
        if hentry.is_null() {
            break;
        }
        if ((*hentry).wait_event_info & WAIT_EVENT_CLASS_MASK) != classId {
            continue;
        }
        *waiteventnames.offset(index as isize) = pstrdup((*hentry).wait_event_name.as_ptr());
        index += 1;
    }

    LWLockRelease(WaitEventCustomLock);

    *nwaitevents = index;
    waiteventnames
}

/*
 * Configure wait event reporting to report wait events to *wait_event_info.
 * *wait_event_info needs to be valid until pgstat_reset_wait_event_storage()
 * is called.
 *
 * Expected to be called during backend startup, to point my_wait_event_info
 * into shared memory.
 */
pub unsafe fn pgstat_set_wait_event_storage(wait_event_info: *mut uint32) {
    my_wait_event_info = wait_event_info;
}

/*
 * Reset wait event storage location.
 *
 * Expected to be called during backend shutdown, before the location set up
 * pgstat_set_wait_event_storage() becomes invalid.
 */
pub unsafe fn pgstat_reset_wait_event_storage() {
    my_wait_event_info = &raw mut local_my_wait_event_info;
}

/* ----------
 * pgstat_get_wait_event_type() -
 *
 *	Return a string representing the current wait event type, backend is
 *	waiting on.
 */
pub unsafe fn pgstat_get_wait_event_type(wait_event_info: uint32) -> *const c_char {
    let classId: uint32;
    let event_type: *const c_char;

    /* report process as not waiting. */
    if wait_event_info == 0 {
        return null();
    }

    classId = wait_event_info & WAIT_EVENT_CLASS_MASK;

    if classId == PG_WAIT_LWLOCK {
        event_type = c"LWLock".as_ptr();
    } else if classId == PG_WAIT_LOCK {
        event_type = c"Lock".as_ptr();
    } else if classId == PG_WAIT_BUFFERPIN {
        event_type = c"BufferPin".as_ptr();
    } else if classId == PG_WAIT_ACTIVITY {
        event_type = c"Activity".as_ptr();
    } else if classId == PG_WAIT_CLIENT {
        event_type = c"Client".as_ptr();
    } else if classId == PG_WAIT_EXTENSION {
        event_type = c"Extension".as_ptr();
    } else if classId == PG_WAIT_IPC {
        event_type = c"IPC".as_ptr();
    } else if classId == PG_WAIT_TIMEOUT {
        event_type = c"Timeout".as_ptr();
    } else if classId == PG_WAIT_IO {
        event_type = c"IO".as_ptr();
    } else if classId == PG_WAIT_INJECTIONPOINT {
        event_type = c"InjectionPoint".as_ptr();
    } else {
        event_type = c"???".as_ptr();
    }

    event_type
}

/* ----------
 * pgstat_get_wait_event() -
 *
 *	Return a string representing the current wait event, backend is
 *	waiting on.
 */
pub unsafe fn pgstat_get_wait_event(wait_event_info: uint32) -> *const c_char {
    let classId: uint32;
    let eventId: uint16;
    let event_name: *const c_char;

    /* report process as not waiting. */
    if wait_event_info == 0 {
        return null();
    }

    classId = wait_event_info & WAIT_EVENT_CLASS_MASK;
    eventId = (wait_event_info & WAIT_EVENT_ID_MASK) as uint16;

    if classId == PG_WAIT_LWLOCK {
        event_name = GetLWLockIdentifier(classId, eventId);
    } else if classId == PG_WAIT_LOCK {
        event_name = GetLockNameFromTagType(eventId);
    } else if classId == PG_WAIT_EXTENSION || classId == PG_WAIT_INJECTIONPOINT {
        event_name = GetWaitEventCustomIdentifier(wait_event_info);
    } else if classId == PG_WAIT_BUFFERPIN {
        let w: WaitEventBufferPin = wait_event_info as WaitEventBufferPin;

        event_name = pgstat_get_wait_bufferpin(w);
    } else if classId == PG_WAIT_ACTIVITY {
        let w: WaitEventActivity = wait_event_info as WaitEventActivity;

        event_name = pgstat_get_wait_activity(w);
    } else if classId == PG_WAIT_CLIENT {
        let w: WaitEventClient = wait_event_info as WaitEventClient;

        event_name = pgstat_get_wait_client(w);
    } else if classId == PG_WAIT_IPC {
        let w: WaitEventIPC = wait_event_info as WaitEventIPC;

        event_name = pgstat_get_wait_ipc(w);
    } else if classId == PG_WAIT_TIMEOUT {
        let w: WaitEventTimeout = wait_event_info as WaitEventTimeout;

        event_name = pgstat_get_wait_timeout(w);
    } else if classId == PG_WAIT_IO {
        let w: WaitEventIO = wait_event_info as WaitEventIO;

        event_name = pgstat_get_wait_io(w);
    } else {
        event_name = c"unknown wait event".as_ptr();
    }

    event_name
}

// ----------------------------------------------------------------
// The C source ends with `#include "utils/pgstat_wait_event.c"`, a build-time
// generated file (from wait_event_names.txt by generate-wait_event_types.pl)
// that defines the six pgstat_get_wait_*() lookup functions.  That generated
// translation unit is not part of the port yet; faithful stubs stand in for it.
// TODO: include generated pgstat_get_wait_* bodies from wait_event_names.txt.
// ----------------------------------------------------------------

unsafe fn pgstat_get_wait_activity(_w: WaitEventActivity) -> *const c_char {
    unimplemented!() // TODO: utils/pgstat_wait_event.c (generated)
}

unsafe fn pgstat_get_wait_bufferpin(_w: WaitEventBufferPin) -> *const c_char {
    unimplemented!() // TODO: utils/pgstat_wait_event.c (generated)
}

unsafe fn pgstat_get_wait_client(_w: WaitEventClient) -> *const c_char {
    unimplemented!() // TODO: utils/pgstat_wait_event.c (generated)
}

unsafe fn pgstat_get_wait_ipc(_w: WaitEventIPC) -> *const c_char {
    unimplemented!() // TODO: utils/pgstat_wait_event.c (generated)
}

unsafe fn pgstat_get_wait_timeout(_w: WaitEventTimeout) -> *const c_char {
    unimplemented!() // TODO: utils/pgstat_wait_event.c (generated)
}

unsafe fn pgstat_get_wait_io(_w: WaitEventIO) -> *const c_char {
    unimplemented!() // TODO: utils/pgstat_wait_event.c (generated)
}

// ---- Stubs for not-yet-ported called functions ----

// LWLock mode (storage/lwlock.h).
#[allow(non_camel_case_types)]
type LWLockMode = c_int;
const LW_EXCLUSIVE: LWLockMode = 0;
const LW_SHARED: LWLockMode = 1;

// Named LWLock pointer for custom wait events (storage/lwlock.h, lwlocklist).
// TODO: storage/lmgr/lwlock.c - real WaitEventCustomLock pointer.
const WaitEventCustomLock: *mut c_void = null_mut();

// TODO: storage/lmgr/lwlock.c
unsafe fn LWLockAcquire(_lock: *mut c_void, _mode: LWLockMode) -> bool {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}

// TODO: storage/lmgr/lwlock.c
unsafe fn LWLockRelease(_lock: *mut c_void) {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}

// TODO: storage/lmgr/lwlock.c
unsafe fn GetLWLockIdentifier(_classId: uint32, _eventId: uint16) -> *const c_char {
    unimplemented!() // TODO: storage/lmgr/lwlock.c
}

// TODO: storage/lmgr/lmgr.c
unsafe fn GetLockNameFromTagType(_locktag_type: uint16) -> *const c_char {
    unimplemented!() // TODO: storage/lmgr/lmgr.c
}

// TODO: storage/ipc/shmem.c
unsafe fn ShmemInitStruct(_name: *const c_char, _size: Size, _foundPtr: *mut bool) -> *mut c_void {
    unimplemented!() // TODO: storage/ipc/shmem.c
}

// TODO: storage/ipc/shmem.c
unsafe fn ShmemInitHash(
    _name: *const c_char,
    _init_size: c_long,
    _max_size: c_long,
    _infoP: *mut HASHCTL,
    _hash_flags: c_int,
) -> *mut HTAB {
    unimplemented!() // TODO: storage/ipc/shmem.c
}

// TODO: common/shmem.h (add_size)
unsafe fn add_size(s1: Size, s2: Size) -> Size {
    s1 + s2 // faithful: add_size with overflow check elided
}
