//-------------------------------------------------------------------------
//
// pthread_barrier_wait.rs
//    Implementation of pthread_barrier_t support for platforms lacking it.
//
// Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//
// IDENTIFICATION
//    src/port/pthread_barrier_wait.rs
//
// Ported 1:1 from postgres/src/port/pthread_barrier_wait.c and the
// pthread_barrier_t declaration in postgres/src/include/port/pg_pthread.h.
//
// This is a fallback sense-reversal barrier (for platforms such as macOS that
// lack pthread_barrier_wait) built on pthread_mutex + pthread_cond.
//
// NOTE: No #[cfg(test)] here -- exercising a barrier requires multiple threads,
// which is out of scope for a pure-logic unit test.
//-------------------------------------------------------------------------

use crate::prelude::*;

// PTHREAD_BARRIER_SERIAL_THREAD: returned to exactly one (the serial) thread.
pub const PTHREAD_BARRIER_SERIAL_THREAD: c_int = -1;

// Opaque system pthread types, sized per platform (see CONVENTIONS).
// macOS:  pthread_mutex_t = 64 bytes, pthread_cond_t = 48 bytes
// linux:  pthread_mutex_t = 40 bytes, pthread_cond_t = 48 bytes
#[cfg(target_os = "macos")]
#[repr(C)]
pub struct pthread_mutex_t {
    __opaque: [u8; 64],
}
#[cfg(target_os = "macos")]
#[repr(C)]
pub struct pthread_cond_t {
    __opaque: [u8; 48],
}

#[cfg(not(target_os = "macos"))]
#[repr(C)]
pub struct pthread_mutex_t {
    __opaque: [u8; 40],
}
#[cfg(not(target_os = "macos"))]
#[repr(C)]
pub struct pthread_cond_t {
    __opaque: [u8; 48],
}

// typedef struct pg_pthread_barrier { ... } pthread_barrier_t;
//
// Field order mirrors pg_pthread.h exactly:
//   bool            sense;    /* we only need a one bit phase */
//   int             count;    /* number of threads expected */
//   int             arrived;  /* number of threads that have arrived */
//   pthread_mutex_t mutex;
//   pthread_cond_t  cond;
#[repr(C)]
pub struct pthread_barrier_t {
    pub sense: bool,
    pub count: c_int,
    pub arrived: c_int,
    pub mutex: pthread_mutex_t,
    pub cond: pthread_cond_t,
}

extern "C" {
    fn pthread_mutex_init(mutex: *mut pthread_mutex_t, attr: *const c_void) -> c_int;
    fn pthread_mutex_lock(mutex: *mut pthread_mutex_t) -> c_int;
    fn pthread_mutex_unlock(mutex: *mut pthread_mutex_t) -> c_int;
    fn pthread_mutex_destroy(mutex: *mut pthread_mutex_t) -> c_int;

    fn pthread_cond_init(cond: *mut pthread_cond_t, attr: *const c_void) -> c_int;
    fn pthread_cond_wait(cond: *mut pthread_cond_t, mutex: *mut pthread_mutex_t) -> c_int;
    fn pthread_cond_broadcast(cond: *mut pthread_cond_t) -> c_int;
    fn pthread_cond_destroy(cond: *mut pthread_cond_t) -> c_int;
}

#[no_mangle]
pub unsafe extern "C" fn pthread_barrier_init(
    barrier: *mut pthread_barrier_t,
    _attr: *const c_void,
    count: c_int,
) -> c_int {
    let mut error: c_int;

    (*barrier).sense = false;
    (*barrier).count = count;
    (*barrier).arrived = 0;

    error = pthread_cond_init(&mut (*barrier).cond, null());
    if error != 0 {
        return error;
    }

    error = pthread_mutex_init(&mut (*barrier).mutex, null());
    if error != 0 {
        pthread_cond_destroy(&mut (*barrier).cond);
        return error;
    }

    0
}

#[no_mangle]
pub unsafe extern "C" fn pthread_barrier_wait(barrier: *mut pthread_barrier_t) -> c_int {
    let initial_sense: bool;

    pthread_mutex_lock(&mut (*barrier).mutex);

    // We have arrived at the barrier.
    (*barrier).arrived += 1;
    // Assert(barrier->arrived <= barrier->count);
    debug_assert!((*barrier).arrived <= (*barrier).count);

    // If we were the last to arrive, release the others and return.
    if (*barrier).arrived == (*barrier).count {
        (*barrier).arrived = 0;
        (*barrier).sense = !(*barrier).sense;
        pthread_mutex_unlock(&mut (*barrier).mutex);
        pthread_cond_broadcast(&mut (*barrier).cond);

        return PTHREAD_BARRIER_SERIAL_THREAD;
    }

    // Wait for someone else to flip the sense.
    initial_sense = (*barrier).sense;
    loop {
        pthread_cond_wait(&mut (*barrier).cond, &mut (*barrier).mutex);
        if (*barrier).sense != initial_sense {
            break;
        }
    }

    pthread_mutex_unlock(&mut (*barrier).mutex);

    0
}

#[no_mangle]
pub unsafe extern "C" fn pthread_barrier_destroy(barrier: *mut pthread_barrier_t) -> c_int {
    pthread_cond_destroy(&mut (*barrier).cond);
    pthread_mutex_destroy(&mut (*barrier).mutex);
    0
}
