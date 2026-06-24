//! Translated from PostgreSQL src/include/port/atomics.h

// === scaffold: child modules (Phase 0) ===
#[path = "atomics/arch-arm.rs"]
pub mod arch_arm;
#[path = "atomics/arch-ppc.rs"]
pub mod arch_ppc;
#[path = "atomics/arch-x86.rs"]
pub mod arch_x86;
#[path = "atomics/fallback.rs"]
pub mod fallback;
#[path = "atomics/generic.rs"]
pub mod generic;
#[path = "atomics/generic-gcc.rs"]
pub mod generic_gcc;
#[path = "atomics/generic-msvc.rs"]
pub mod generic_msvc;
#[path = "atomics/generic-sunpro.rs"]
pub mod generic_sunpro;
// === end scaffold ===
