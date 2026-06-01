//! port/pg_pthread.h - declarations for missing POSIX thread components (pthread_barrier_t).
//
// Currently this supplies an implementation of pthread_barrier_t for the
// benefit of macOS, which lacks it.  These declarations are not in port.h,
// because that'd require <pthread.h> to be included by every translation unit.
//
// The C header is compiled only when !HAVE_PTHREAD_BARRIER_WAIT (i.e. on
// platforms such as macOS that lack a native pthread_barrier_wait).  The
// canonical Rust definitions of the type, the constant, and the three
// barrier functions live in the .c-translation module
// `crate::port::pthread_barrier_wait` (from postgres/src/port/pthread_barrier_wait.c).
// To avoid duplicate definitions in the same crate, this header module simply
// re-exports those canonical symbols, mirroring the header's declarations.

#![allow(unused_imports)]

// #define PTHREAD_BARRIER_SERIAL_THREAD (-1)
//
// typedef struct pg_pthread_barrier { ... } pthread_barrier_t;
//
// extern int pthread_barrier_init(pthread_barrier_t *, const void *attr, int count);
// extern int pthread_barrier_wait(pthread_barrier_t *);
// extern int pthread_barrier_destroy(pthread_barrier_t *);
pub use crate::port::pthread_barrier_wait::{
    pthread_barrier_destroy, pthread_barrier_init, pthread_barrier_wait, pthread_barrier_t,
    PTHREAD_BARRIER_SERIAL_THREAD,
};
