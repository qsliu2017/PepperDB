//! jit/SectionMemoryManager.h - section-based memory manager for MCJIT/RtDyld (LLVM C++ backport)
//!
//! PROVENANCE: This is a C++ header (`-*- C++ -*-`), a copy of LLVM source modified by the
//! PostgreSQL project (see SectionMemoryManager.cpp). It declares the C++ class
//! `llvm::backport::SectionMemoryManager`, which derives from `RTDyldMemoryManager` and
//! overrides virtual methods. The whole construct is C++-only: virtual inheritance, pure
//! virtual interfaces, `std::unique_ptr`, `std::error_code`, `std::string`, deleted copy
//! ctor/assignment, and `llvm::SmallVector` template members. None of these have a faithful
//! `#[repr(C)]` Rust FFI form. PepperDB never instantiates this from Rust; the C++ object is
//! built and used entirely on the LLVM/C++ side. Below we translate the CONCRETE, non-virtual
//! pieces (the AllocationPurpose enum, the private POD-ish helper structs) with LLVM/system
//! types stubbed locally as opaque aliases, and document the C++ class itself.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(dead_code)]

use crate::c::{uint32, uint8};
use std::ffi::{c_uint, c_void};

// ---------------------------------------------------------------------------
// Local stubs for LLVM / C++ / system types referenced by this header.
// These have no PepperDB Rust definitions; the real types live in LLVM C++ and
// in libstdc++. We stub them as opaque aliases so the concrete symbols below
// have something to name. TODO: dedup if an LLVM-C binding module appears.
// ---------------------------------------------------------------------------

/// C `uintptr_t` (from <cstdint>).
pub type uintptr_t = usize;
/// C `size_t`.
pub type size_t = crate::c::Size;

/// `llvm::sys::MemoryBlock` - opaque LLVM C++ type (Support/Memory.h).
pub type sys_MemoryBlock = c_void;
/// `llvm::Align` - opaque LLVM C++ type (Support/Alignment.h).
pub type Align = c_void;
/// `llvm::StringRef` - opaque LLVM C++ type (ADT/StringRef.h).
pub type StringRef = c_void;
/// `std::error_code` - opaque libstdc++ type (<system_error>).
pub type std_error_code = c_void;
/// `std::string` - opaque libstdc++ type (<string>).
pub type std_string = c_void;
/// `llvm::backport::SectionMemoryManager` - the opaque C++ class (see doc below).
pub type SectionMemoryManager = c_void;
/// `llvm::backport::SectionMemoryManager::MemoryMapper` - opaque C++ abstract interface.
pub type MemoryMapper = c_void;
/// `llvm::RTDyldMemoryManager` - opaque LLVM C++ base class.
pub type RTDyldMemoryManager = c_void;

// ---------------------------------------------------------------------------
// AllocationPurpose: C++ `enum class AllocationPurpose { Code, ROData, RWData };`
// nested in SectionMemoryManager. A scoped enum is a plain integral type; we
// model it as a c_int alias with const variants, matching the C-enum convention.
// ---------------------------------------------------------------------------

pub type AllocationPurpose = std::ffi::c_int;
pub const AllocationPurpose_Code: AllocationPurpose = 0;
pub const AllocationPurpose_ROData: AllocationPurpose = 1;
pub const AllocationPurpose_RWData: AllocationPurpose = 2;

// ---------------------------------------------------------------------------
// Private helper structs of SectionMemoryManager.
//
// FreeMemBlock and MemoryGroup are C++ structs. FreeMemBlock is POD-like and has
// a clean concrete translation (its `sys::MemoryBlock Free` member is by-value,
// stubbed opaquely above so we hold it behind a pointer). MemoryGroup embeds
// `llvm::SmallVector<...>` template instantiations which are C++-only with no
// stable C layout; we translate it but mark the vector members as opaque.
// These are not crate-public in C++ (private nested) but emitted here for
// completeness per the 1:1 translation rule.
// ---------------------------------------------------------------------------

/// C++:
/// ```text
/// struct FreeMemBlock {
///   sys::MemoryBlock Free;
///   unsigned PendingPrefixIndex;
/// };
/// ```
/// `sys::MemoryBlock` is a by-value C++ object with no stable C layout; held here
/// as a raw pointer stub so the struct names every member.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct FreeMemBlock {
    /// The actual block of free memory.
    pub Free: *mut sys_MemoryBlock,
    /// Index in PendingMem of a pending allocation right before this block, if any.
    pub PendingPrefixIndex: c_uint,
}

/// C++:
/// ```text
/// struct MemoryGroup {
///   SmallVector<sys::MemoryBlock, 16> PendingMem;
///   SmallVector<FreeMemBlock, 16> FreeMem;
///   SmallVector<sys::MemoryBlock, 16> AllocatedMem;
///   sys::MemoryBlock Near;
/// };
/// ```
/// The `SmallVector<T, 16>` members are LLVM C++ templates with no stable C
/// layout; stubbed here as opaque pointers. `Near` is a by-value MemoryBlock,
/// likewise held behind a pointer stub.
#[repr(C)]
#[derive(Clone, Copy)]
pub struct MemoryGroup {
    /// `SmallVector<sys::MemoryBlock, 16>` - blocks given out but not yet permission-applied.
    pub PendingMem: *mut c_void,
    /// `SmallVector<FreeMemBlock, 16>` - free blocks, neither permission-applied nor given out.
    pub FreeMem: *mut c_void,
    /// `SmallVector<sys::MemoryBlock, 16>` - all memory requested from the system.
    pub AllocatedMem: *mut c_void,
    /// `sys::MemoryBlock Near` - by-value LLVM object, stubbed opaque.
    pub Near: *mut sys_MemoryBlock,
}

// ---------------------------------------------------------------------------
// C++ class members documented but NOT translatable to Rust FFI.
//
// class SectionMemoryManager : public RTDyldMemoryManager {
//   enum class AllocationPurpose { Code, ROData, RWData };            // -> above
//   class MemoryMapper { ... pure virtual interface ... };           // C++ abstract class
//
//   // ctors / dtor (C++ object lifetime, vtable):
//   SectionMemoryManager(MemoryMapper *MM = nullptr, bool ReserveAlloc = false);
//   SectionMemoryManager(const SectionMemoryManager &) = delete;     // deleted copy
//   void operator=(const SectionMemoryManager &) = delete;           // deleted assign
//   ~SectionMemoryManager() override;
//
//   // virtual overrides of RTDyldMemoryManager (dispatched via C++ vtable):
//   bool needsToReserveAllocationSpace() override;
//   void reserveAllocationSpace(uintptr_t CodeSize, Align CodeAlign,
//                               uintptr_t RODataSize, Align RODataAlign,
//                               uintptr_t RWDataSize, Align RWDataAlign) override;
//     (pre-LLVM-16 variant takes uint32_t aligns instead of Align)
//   uint8_t *allocateCodeSection(uintptr_t Size, unsigned Alignment,
//                                unsigned SectionID, StringRef SectionName) override;
//   uint8_t *allocateDataSection(uintptr_t Size, unsigned Alignment,
//                                unsigned SectionID, StringRef SectionName,
//                                bool IsReadOnly) override;
//   bool finalizeMemory(std::string *ErrMsg = nullptr) override;
//   virtual void invalidateInstructionCache();
//
//   // private members:
//   uint8_t *allocateSection(AllocationPurpose, uintptr_t Size, unsigned Alignment);
//   std::error_code applyMemoryGroupPermissions(MemoryGroup &, unsigned Permissions);
//   bool hasSpace(const MemoryGroup &, uintptr_t Size) const;
//   void anchor() override;
//   MemoryGroup CodeMem, RWDataMem, RODataMem;
//   MemoryMapper *MMapper;
//   std::unique_ptr<MemoryMapper> OwnedMMapper;
//   bool ReserveAllocation;
// };
//
// These rely on C++ virtual dispatch / inheritance / std types and are exercised
// only from the C++ side of the JIT. Rust never calls them; no FFI stub is
// meaningful. The free-standing method SIGNATURES are reproduced below as
// prototype stubs (using the stubbed opaque types) purely to record their
// existence, taking the receiver `this: *mut SectionMemoryManager` explicitly.
// ---------------------------------------------------------------------------

/// `MemoryMapper::allocateMappedMemory` (pure virtual) - returns sys::MemoryBlock by value.
pub unsafe fn MemoryMapper_allocateMappedMemory(
    this: *mut MemoryMapper,
    Purpose: AllocationPurpose,
    NumBytes: size_t,
    NearBlock: *const sys_MemoryBlock,
    Flags: c_uint,
    EC: *mut std_error_code,
) -> *mut sys_MemoryBlock {
    unimplemented!()
}

/// `MemoryMapper::protectMappedMemory` (pure virtual).
pub unsafe fn MemoryMapper_protectMappedMemory(
    this: *mut MemoryMapper,
    Block: *const sys_MemoryBlock,
    Flags: c_uint,
) -> *mut std_error_code {
    unimplemented!()
}

/// `MemoryMapper::releaseMappedMemory` (pure virtual).
pub unsafe fn MemoryMapper_releaseMappedMemory(
    this: *mut MemoryMapper,
    M: *mut sys_MemoryBlock,
) -> *mut std_error_code {
    unimplemented!()
}

/// `SectionMemoryManager::needsToReserveAllocationSpace` override.
pub unsafe fn SectionMemoryManager_needsToReserveAllocationSpace(
    this: *mut SectionMemoryManager,
) -> bool {
    unimplemented!()
}

/// `SectionMemoryManager::reserveAllocationSpace` override (LLVM >= 16, Align params).
pub unsafe fn SectionMemoryManager_reserveAllocationSpace(
    this: *mut SectionMemoryManager,
    CodeSize: uintptr_t,
    CodeAlign: *mut Align,
    RODataSize: uintptr_t,
    RODataAlign: *mut Align,
    RWDataSize: uintptr_t,
    RWDataAlign: *mut Align,
) {
    unimplemented!()
}

/// `SectionMemoryManager::allocateCodeSection` override.
pub unsafe fn SectionMemoryManager_allocateCodeSection(
    this: *mut SectionMemoryManager,
    Size: uintptr_t,
    Alignment: c_uint,
    SectionID: c_uint,
    SectionName: *mut StringRef,
) -> *mut uint8 {
    unimplemented!()
}

/// `SectionMemoryManager::allocateDataSection` override.
pub unsafe fn SectionMemoryManager_allocateDataSection(
    this: *mut SectionMemoryManager,
    Size: uintptr_t,
    Alignment: c_uint,
    SectionID: c_uint,
    SectionName: *mut StringRef,
    IsReadOnly: bool,
) -> *mut uint8 {
    unimplemented!()
}

/// `SectionMemoryManager::finalizeMemory` override.
pub unsafe fn SectionMemoryManager_finalizeMemory(
    this: *mut SectionMemoryManager,
    ErrMsg: *mut std_string,
) -> bool {
    unimplemented!()
}

/// `SectionMemoryManager::invalidateInstructionCache` (virtual).
pub unsafe fn SectionMemoryManager_invalidateInstructionCache(this: *mut SectionMemoryManager) {
    unimplemented!()
}

/// `SectionMemoryManager::allocateSection` (private).
pub unsafe fn SectionMemoryManager_allocateSection(
    this: *mut SectionMemoryManager,
    Purpose: AllocationPurpose,
    Size: uintptr_t,
    Alignment: c_uint,
) -> *mut uint8 {
    unimplemented!()
}

/// `SectionMemoryManager::applyMemoryGroupPermissions` (private).
pub unsafe fn SectionMemoryManager_applyMemoryGroupPermissions(
    this: *mut SectionMemoryManager,
    MemGroup: *mut MemoryGroup,
    Permissions: c_uint,
) -> *mut std_error_code {
    unimplemented!()
}

/// `SectionMemoryManager::hasSpace` (private, const).
pub unsafe fn SectionMemoryManager_hasSpace(
    this: *const SectionMemoryManager,
    MemGroup: *const MemoryGroup,
    Size: uintptr_t,
) -> bool {
    unimplemented!()
}

// Silence unused-import warnings for type aliases used only in doc/positions.
const _: () = {
    let _ = core::mem::size_of::<uint32>();
};
