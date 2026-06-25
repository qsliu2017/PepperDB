//! Tombstone: src/include/port/atomics.h
//!
//! PG's atomic abstraction (pg_atomic_uint32/uint64/flag and the pg_atomic_*
//! operations) is replaced wholesale by `core::sync::atomic` (AtomicU32/AtomicU64/
//! AtomicBool with Ordering). Callers use the std types directly. The child
//! arch/generic modules below are themselves tombstones (kept only so the module
//! tree resolves).

// === scaffold: child modules (Phase 0) ===
#[path = "atomics/arch-arm.rs"]
pub mod arch_arm;
#[path = "atomics/arch-ppc.rs"]
pub mod arch_ppc;
#[path = "atomics/arch-x86.rs"]
pub mod arch_x86;
pub mod fallback;
pub mod generic;
#[path = "atomics/generic-gcc.rs"]
pub mod generic_gcc;
#[path = "atomics/generic-msvc.rs"]
pub mod generic_msvc;
#[path = "atomics/generic-sunpro.rs"]
pub mod generic_sunpro;
// === end scaffold ===
