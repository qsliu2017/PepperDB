//! Core part of the LLVM JIT provider.
//!
//! Translation of postgres/src/backend/jit/llvm/llvmjit.c
//!
//! Copyright (c) 2016-2025, PostgreSQL Global Development Group

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_variables)]

use crate::prelude::*;

use core::ffi::CStr;
use std::ffi::{c_char, c_int, c_void};

use crate::c::{uint32, uint64, Size};
use crate::nodes::pg_list::{lfirst, List, NIL};
use crate::nodes::list::{lappend, list_free};
use crate::portability::instr_time::{
    instr_time, INSTR_TIME_ACCUM_DIFF, INSTR_TIME_GET_DOUBLE, INSTR_TIME_SET_CURRENT,
};
use crate::storage::ipc::ipc::{on_proc_exit, proc_exit_inprogress};
use crate::utils::fmgr::{fmgr_symbol, FunctionCallInfo};
use crate::utils::resowner::resowner::{
    ResourceOwner, ResourceOwnerDesc, ResourceOwnerEnlarge, ResourceOwnerForget,
    ResourceOwnerRemember, CurrentResourceOwner, RELEASE_PRIO_JIT_CONTEXTS,
    RESOURCE_RELEASE_BEFORE_LOCKS,
};
use crate::pg_config_manual::MAXPGPATH;
use crate::miscadmin::{pkglib_path, MyProcPid};

use crate::jit::jit::{
    JitContext, JitProviderCallbacks, PGJIT_INLINE, PGJIT_OPT3,
};
use crate::jit::llvmjit_backport::USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER;
use crate::jit::llvmjit_emit::{
    l_load, l_ptr_const, LLVMBasicBlockRef, LLVMBuilderRef, LLVMContextRef, LLVMModuleRef,
    LLVMTypeRef, LLVMValueRef,
};

// Globals referenced from jit.c (GUCs).
use crate::jit::jit::{jit_debugging_support, jit_dump_bitcode, jit_profiling_support};

// ---------------------------------------------------------------------------
// libc helpers used directly in this module.
// ---------------------------------------------------------------------------
extern "C" {
    fn snprintf(s: *mut c_char, n: usize, fmt: *const c_char, ...) -> c_int;
    fn strncmp(s1: *const c_char, s2: *const c_char, n: usize) -> c_int;
    fn strlen(s: *const c_char) -> usize;
    fn rindex(s: *const c_char, c: c_int) -> *mut c_char;
}

// ---------------------------------------------------------------------------
// Additional LLVM-C / ORC opaque type stubs (from <llvm-c/*.h>). No Rust LLVM-C
// binding exists in this tree; alias each as an opaque pointer. The core
// LLVM*Ref aliases (LLVMValueRef, LLVMTypeRef, ...) come from llvmjit_emit.
// TODO(pg-port): dedup with a real LLVM-C binding module when one exists.
// ---------------------------------------------------------------------------
pub type LLVMErrorRef = *mut c_void;
pub type LLVMAttributeRef = *mut c_void;
pub type LLVMMemoryBufferRef = *mut c_void;
pub type LLVMTargetRef = *mut c_void;
pub type LLVMTargetMachineRef = *mut c_void;
pub type LLVMPassBuilderOptionsRef = *mut c_void;
pub type LLVMJITEventListenerRef = *mut c_void;
pub type LLVMOrcThreadSafeContextRef = *mut c_void;
pub type LLVMOrcThreadSafeModuleRef = *mut c_void;
pub type LLVMOrcLLJITRef = *mut c_void;
pub type LLVMOrcLLJITBuilderRef = *mut c_void;
pub type LLVMOrcJITTargetMachineBuilderRef = *mut c_void;
pub type LLVMOrcResourceTrackerRef = *mut c_void;
pub type LLVMOrcExecutionSessionRef = *mut c_void;
pub type LLVMOrcSymbolStringPoolRef = *mut c_void;
pub type LLVMOrcSymbolStringPoolEntryRef = *mut c_void;
pub type LLVMOrcJITDylibRef = *mut c_void;
pub type LLVMOrcObjectLayerRef = *mut c_void;
pub type LLVMOrcDefinitionGeneratorRef = *mut c_void;
pub type LLVMOrcLookupStateRef = *mut c_void;
pub type LLVMOrcMaterializationUnitRef = *mut c_void;
pub type LLVMOrcJITTargetAddress = uint64;
pub type LLVMOrcLookupKind = c_int;
pub type LLVMOrcJITDylibLookupFlags = c_int;

pub const LLVMErrorSuccess: c_int = 0;

// LLVMCodeGenOptLevel
pub const LLVMCodeGenLevelNone: c_int = 0;
pub const LLVMCodeGenLevelAggressive: c_int = 3;
// LLVMRelocMode
pub const LLVMRelocDefault: c_int = 0;
// LLVMCodeModel
pub const LLVMCodeModelJITDefault: c_int = 1;
// LLVMTypeKind
pub const LLVMVoidTypeKind: c_int = 0;
// LLVMLinkage
pub const LLVMPrivateLinkage: c_int = 9;
// Attribute index sentinels.
pub const LLVMAttributeReturnIndex: uint32 = 0;
pub const LLVMAttributeFunctionIndex: uint32 = !0; // (unsigned) -1
// LLVMJITSymbolGenericFlags
pub const LLVMJITSymbolGenericFlagsExported: u8 = 2;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct LLVMJITSymbolFlags {
    pub GenericFlags: u8,
    pub TargetFlags: u8,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct LLVMJITEvaluatedSymbol {
    pub Address: LLVMOrcJITTargetAddress,
    pub Flags: LLVMJITSymbolFlags,
}

#[repr(C)]
#[derive(Clone, Copy)]
pub struct LLVMOrcCSymbolMapPair {
    pub Name: LLVMOrcSymbolStringPoolEntryRef,
    pub Sym: LLVMJITEvaluatedSymbol,
}

pub type LLVMOrcCSymbolMapPairs = *mut LLVMOrcCSymbolMapPair;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct LLVMOrcCLookupSetElement {
    pub Name: LLVMOrcSymbolStringPoolEntryRef,
    pub LookupFlags: c_int,
}

pub type LLVMOrcCLookupSet = *mut LLVMOrcCLookupSetElement;

pub type LLVMOrcLLJITBuilderObjectLinkingLayerCreatorFunction = Option<
    unsafe extern "C" fn(
        Ctx: *mut c_void,
        ES: LLVMOrcExecutionSessionRef,
        Triple: *const c_char,
    ) -> LLVMOrcObjectLayerRef,
>;

pub type LLVMOrcErrorReporterFunction =
    Option<unsafe extern "C" fn(Ctx: *mut c_void, Err: LLVMErrorRef)>;

pub type LLVMOrcCAPIDefinitionGeneratorTryToGenerateFunction = Option<
    unsafe extern "C" fn(
        GeneratorObj: LLVMOrcDefinitionGeneratorRef,
        Ctx: *mut c_void,
        LookupState: *mut LLVMOrcLookupStateRef,
        Kind: LLVMOrcLookupKind,
        JD: LLVMOrcJITDylibRef,
        JDLookupFlags: LLVMOrcJITDylibLookupFlags,
        LookupSet: LLVMOrcCLookupSet,
        LookupSetSize: usize,
    ) -> LLVMErrorRef,
>;

pub type LLVMOrcDisposeCAPIDefinitionGeneratorFunction =
    Option<unsafe extern "C" fn(Ctx: *mut c_void)>;

// ---------------------------------------------------------------------------
// LLVM-C API entry points used by this module (declared extern; the real
// symbols are provided by the linked LLVM library).
// ---------------------------------------------------------------------------
extern "C" {
    fn LLVMContextCreate() -> LLVMContextRef;
    fn LLVMContextDispose(C: LLVMContextRef);
    fn LLVMModuleCreateWithNameInContext(ModuleID: *const c_char, C: LLVMContextRef) -> LLVMModuleRef;
    fn LLVMDisposeModule(M: LLVMModuleRef);
    fn LLVMSetTarget(M: LLVMModuleRef, Triple: *const c_char);
    fn LLVMSetDataLayout(M: LLVMModuleRef, DataLayoutStr: *const c_char);
    fn LLVMGetTarget(M: LLVMModuleRef) -> *const c_char;
    fn LLVMGetDataLayoutStr(M: LLVMModuleRef) -> *const c_char;
    fn LLVMGetNamedGlobal(M: LLVMModuleRef, Name: *const c_char) -> LLVMValueRef;
    fn LLVMGetNamedFunction(M: LLVMModuleRef, Name: *const c_char) -> LLVMValueRef;
    fn LLVMAddFunction(M: LLVMModuleRef, Name: *const c_char, FunctionTy: LLVMTypeRef) -> LLVMValueRef;
    fn LLVMAddGlobal(M: LLVMModuleRef, Ty: LLVMTypeRef, Name: *const c_char) -> LLVMValueRef;
    fn LLVMSetInitializer(GlobalVar: LLVMValueRef, ConstantVal: LLVMValueRef);
    fn LLVMSetGlobalConstant(GlobalVar: LLVMValueRef, IsConstant: c_int);
    fn LLVMSetLinkage(Global: LLVMValueRef, Linkage: c_int);
    fn LLVMSetUnnamedAddr(Global: LLVMValueRef, HasUnnamedAddr: c_int);
    fn LLVMGlobalGetValueType(Global: LLVMValueRef) -> LLVMTypeRef;
    fn LLVMGetFunctionType(Fn: LLVMValueRef) -> LLVMTypeRef;
    fn LLVMGetFunctionReturnType(Fn: LLVMValueRef) -> LLVMTypeRef;
    fn LLVMGetTypeKind(Ty: LLVMTypeRef) -> c_int;
    fn LLVMCountParams(Fn: LLVMValueRef) -> uint32;
    fn LLVMGetAttributeCountAtIndex(F: LLVMValueRef, Idx: uint32) -> c_int;
    fn LLVMGetAttributesAtIndex(F: LLVMValueRef, Idx: uint32, Attrs: *mut LLVMAttributeRef);
    fn LLVMAddAttributeAtIndex(F: LLVMValueRef, Idx: uint32, A: LLVMAttributeRef);
    fn LLVMGetFirstFunction(M: LLVMModuleRef) -> LLVMValueRef;
    fn LLVMGetNextFunction(Fn: LLVMValueRef) -> LLVMValueRef;
    fn LLVMWriteBitcodeToFile(M: LLVMModuleRef, Path: *const c_char) -> c_int;
    fn LLVMCreateMemoryBufferWithContentsOfFile(
        Path: *const c_char,
        OutMemBuf: *mut LLVMMemoryBufferRef,
        OutMessage: *mut *mut c_char,
    ) -> c_int;
    fn LLVMParseBitcodeInContext2(
        ContextRef: LLVMContextRef,
        MemBuf: LLVMMemoryBufferRef,
        OutModule: *mut LLVMModuleRef,
    ) -> c_int;
    fn LLVMDisposeMemoryBuffer(MemBuf: LLVMMemoryBufferRef);
    fn LLVMDisposeMessage(Message: *mut c_char);

    fn LLVMInitializeNativeTarget() -> c_int;
    fn LLVMInitializeNativeAsmPrinter() -> c_int;
    fn LLVMInitializeNativeAsmParser() -> c_int;
    fn LLVMGetTargetFromTriple(
        Triple: *const c_char,
        T: *mut LLVMTargetRef,
        ErrorMessage: *mut *mut c_char,
    ) -> c_int;
    fn LLVMGetHostCPUName() -> *mut c_char;
    fn LLVMGetHostCPUFeatures() -> *mut c_char;
    fn LLVMCreateTargetMachine(
        T: LLVMTargetRef,
        Triple: *const c_char,
        CPU: *const c_char,
        Features: *const c_char,
        Level: c_int,
        Reloc: c_int,
        CodeModel: c_int,
    ) -> LLVMTargetMachineRef;
    fn LLVMLoadLibraryPermanently(Filename: *const c_char) -> c_int;
    fn LLVMSearchForAddressOfSymbol(symbolName: *const c_char) -> *mut c_void;

    fn LLVMGetErrorMessage(Err: LLVMErrorRef) -> *mut c_char;
    fn LLVMDisposeErrorMessage(ErrMsg: *mut c_char);

    fn LLVMCreatePassBuilderOptions() -> LLVMPassBuilderOptionsRef;
    fn LLVMDisposePassBuilderOptions(Options: LLVMPassBuilderOptionsRef);
    fn LLVMPassBuilderOptionsSetInlinerThreshold(Options: LLVMPassBuilderOptionsRef, Threshold: c_int);
    fn LLVMRunPasses(
        M: LLVMModuleRef,
        Passes: *const c_char,
        TM: LLVMTargetMachineRef,
        Options: LLVMPassBuilderOptionsRef,
    ) -> LLVMErrorRef;

    fn LLVMCreateGDBRegistrationListener() -> LLVMJITEventListenerRef;
    fn LLVMCreatePerfJITEventListener() -> LLVMJITEventListenerRef;

    fn LLVMOrcCreateNewThreadSafeContext() -> LLVMOrcThreadSafeContextRef;
    fn LLVMOrcDisposeThreadSafeContext(TSCtx: LLVMOrcThreadSafeContextRef);
    fn LLVMOrcCreateNewThreadSafeModule(
        M: LLVMModuleRef,
        TSCtx: LLVMOrcThreadSafeContextRef,
    ) -> LLVMOrcThreadSafeModuleRef;

    fn LLVMOrcCreateLLJITBuilder() -> LLVMOrcLLJITBuilderRef;
    fn LLVMOrcJITTargetMachineBuilderCreateFromTargetMachine(
        TM: LLVMTargetMachineRef,
    ) -> LLVMOrcJITTargetMachineBuilderRef;
    fn LLVMOrcLLJITBuilderSetJITTargetMachineBuilder(
        Builder: LLVMOrcLLJITBuilderRef,
        JTMB: LLVMOrcJITTargetMachineBuilderRef,
    );
    fn LLVMOrcLLJITBuilderSetObjectLinkingLayerCreator(
        Builder: LLVMOrcLLJITBuilderRef,
        F: LLVMOrcLLJITBuilderObjectLinkingLayerCreatorFunction,
        Ctx: *mut c_void,
    );
    fn LLVMOrcCreateLLJIT(Result: *mut LLVMOrcLLJITRef, Builder: LLVMOrcLLJITBuilderRef)
        -> LLVMErrorRef;
    fn LLVMOrcDisposeLLJIT(J: LLVMOrcLLJITRef) -> LLVMErrorRef;
    fn LLVMOrcLLJITGetExecutionSession(J: LLVMOrcLLJITRef) -> LLVMOrcExecutionSessionRef;
    fn LLVMOrcLLJITGetMainJITDylib(J: LLVMOrcLLJITRef) -> LLVMOrcJITDylibRef;
    fn LLVMOrcLLJITGetGlobalPrefix(J: LLVMOrcLLJITRef) -> c_char;
    fn LLVMOrcLLJITLookup(
        J: LLVMOrcLLJITRef,
        Result: *mut LLVMOrcJITTargetAddress,
        Name: *const c_char,
    ) -> LLVMErrorRef;
    fn LLVMOrcLLJITAddLLVMIRModuleWithRT(
        J: LLVMOrcLLJITRef,
        RT: LLVMOrcResourceTrackerRef,
        TSM: LLVMOrcThreadSafeModuleRef,
    ) -> LLVMErrorRef;

    fn LLVMOrcExecutionSessionSetErrorReporter(
        ES: LLVMOrcExecutionSessionRef,
        ReportError: LLVMOrcErrorReporterFunction,
        Ctx: *mut c_void,
    );
    fn LLVMOrcExecutionSessionGetSymbolStringPool(
        ES: LLVMOrcExecutionSessionRef,
    ) -> LLVMOrcSymbolStringPoolRef;
    fn LLVMOrcSymbolStringPoolClearDeadEntries(SSP: LLVMOrcSymbolStringPoolRef);
    fn LLVMOrcSymbolStringPoolEntryStr(S: LLVMOrcSymbolStringPoolEntryRef) -> *const c_char;
    fn LLVMOrcRetainSymbolStringPoolEntry(S: LLVMOrcSymbolStringPoolEntryRef);

    fn LLVMOrcJITDylibCreateResourceTracker(JD: LLVMOrcJITDylibRef) -> LLVMOrcResourceTrackerRef;
    fn LLVMOrcResourceTrackerRemove(RT: LLVMOrcResourceTrackerRef) -> LLVMErrorRef;
    fn LLVMOrcReleaseResourceTracker(RT: LLVMOrcResourceTrackerRef);

    fn LLVMOrcAbsoluteSymbols(
        Syms: LLVMOrcCSymbolMapPairs,
        NumPairs: usize,
    ) -> LLVMOrcMaterializationUnitRef;
    fn LLVMOrcJITDylibDefine(JD: LLVMOrcJITDylibRef, MU: LLVMOrcMaterializationUnitRef)
        -> LLVMErrorRef;
    fn LLVMOrcDisposeMaterializationUnit(MU: LLVMOrcMaterializationUnitRef);
    fn LLVMOrcJITDylibAddGenerator(JD: LLVMOrcJITDylibRef, DG: LLVMOrcDefinitionGeneratorRef);
    fn LLVMOrcCreateDynamicLibrarySearchGeneratorForProcess(
        Result: *mut LLVMOrcDefinitionGeneratorRef,
        GlobalPrefx: c_char,
        Filter: Option<unsafe extern "C" fn(Ctx: *mut c_void, Sym: LLVMOrcSymbolStringPoolEntryRef) -> c_int>,
        FilterCtx: *mut c_void,
    ) -> LLVMErrorRef;
    fn LLVMOrcCreateCustomCAPIDefinitionGenerator(
        F: LLVMOrcCAPIDefinitionGeneratorTryToGenerateFunction,
        Ctx: *mut c_void,
        Dispose: LLVMOrcDisposeCAPIDefinitionGeneratorFunction,
    ) -> LLVMOrcDefinitionGeneratorRef;

    fn LLVMOrcCreateRTDyldObjectLinkingLayerWithSafeSectionMemoryManager(
        ES: LLVMOrcExecutionSessionRef,
    ) -> LLVMOrcObjectLayerRef;
    fn LLVMOrcCreateRTDyldObjectLinkingLayerWithSectionMemoryManager(
        ES: LLVMOrcExecutionSessionRef,
    ) -> LLVMOrcObjectLayerRef;
    fn LLVMOrcRTDyldObjectLinkingLayerRegisterJITEventListener(
        RTDyldObjLinkingLayer: LLVMOrcObjectLayerRef,
        Listener: LLVMJITEventListenerRef,
    );
}

const LLVMJIT_LLVM_CONTEXT_REUSE_MAX: usize = 100;

// ---------------------------------------------------------------------------
// Stubs for functions defined in other .c files (faithful signatures).
// ---------------------------------------------------------------------------

// llvmjit_inline.cpp
unsafe fn llvm_inline(_mod: LLVMModuleRef) {
    unimplemented!() // TODO(pg-port): jit/llvm/llvmjit_inline.cpp
}
unsafe fn llvm_inline_reset_caches() {
    unimplemented!() // TODO(pg-port): jit/llvm/llvmjit_inline.cpp
}

// llvmjit_error.cpp
pub unsafe fn llvm_enter_fatal_on_oom() {
    unimplemented!() // TODO(pg-port): jit/llvm/llvmjit_error.cpp
}
pub unsafe fn llvm_leave_fatal_on_oom() {
    unimplemented!() // TODO(pg-port): jit/llvm/llvmjit_error.cpp
}
pub unsafe fn llvm_in_fatal_on_oom() -> bool {
    unimplemented!() // TODO(pg-port): jit/llvm/llvmjit_error.cpp
}
pub unsafe fn llvm_reset_after_error() {
    unimplemented!() // TODO(pg-port): jit/llvm/llvmjit_error.cpp
}
pub unsafe fn llvm_assert_in_fatal_section() {
    unimplemented!() // TODO(pg-port): jit/llvm/llvmjit_error.cpp
}

// llvmjit_expr.c
pub unsafe fn llvm_compile_expr(_state: *mut crate::jit::jit::ExprState) -> bool {
    unimplemented!() // TODO(pg-port): jit/llvm/llvmjit_expr.c
}

// jit.c
unsafe fn jit_release_context(_context: *mut JitContext) {
    unimplemented!() // TODO(pg-port): jit/jit.c
}

// utils/fmgr/dfmgr.c
unsafe fn load_external_function(
    _filename: *const c_char,
    _funcname: *const c_char,
    _signalNotFound: bool,
    _filehandle: *mut *mut c_void,
) -> *mut c_void {
    unimplemented!() // TODO(pg-port): utils/fmgr/dfmgr.c
}

// utils/mmgr/mcxt.c - psprintf has no real home yet.
unsafe fn psprintf(_fmt: *const c_char) -> *mut c_char {
    unimplemented!() // TODO(pg-port): utils/mmgr/mcxt.c (variadic)
}

// ---------------------------------------------------------------------------
// LLVMJitContext (jit/llvmjit.h)
// ---------------------------------------------------------------------------

/// Handle of a module emitted via ORC JIT
#[repr(C)]
pub struct LLVMJitHandle {
    pub lljit: LLVMOrcLLJITRef,
    pub resource_tracker: LLVMOrcResourceTrackerRef,
}

#[repr(C)]
pub struct LLVMJitContext {
    pub base: JitContext,

    /* used to ensure cleanup of context */
    pub resowner: ResourceOwner,

    /* number of modules created */
    pub module_generation: Size,

    /* The LLVM Context used by this JIT context. */
    pub llvm_context: LLVMContextRef,

    /* current, "open for write", module */
    pub module: LLVMModuleRef,

    /* is there any pending code that needs to be emitted */
    pub compiled: bool,

    /* # of objects emitted, used to generate non-conflicting names */
    pub counter: c_int,

    /* list of handles for code emitted via Orc */
    pub handles: *mut List,
}

// ---------------------------------------------------------------------------
// types & functions commonly needed for JITing
// ---------------------------------------------------------------------------
#[no_mangle]
pub static mut TypeSizeT: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut TypeParamBool: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut TypeStorageBool: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut TypePGFunction: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructNullableDatum: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructHeapTupleData: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructMinimalTupleData: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructTupleDescData: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructTupleTableSlot: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructHeapTupleHeaderData: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructHeapTupleTableSlot: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructMinimalTupleTableSlot: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructMemoryContextData: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructFunctionCallInfoData: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructExprContext: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructExprEvalStep: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructExprState: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructAggState: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructAggStatePerGroupData: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructAggStatePerTransData: LLVMTypeRef = null_mut();
#[no_mangle]
pub static mut StructPlanState: LLVMTypeRef = null_mut();

#[no_mangle]
pub static mut AttributeTemplate: LLVMValueRef = null_mut();
#[no_mangle]
pub static mut ExecEvalSubroutineTemplate: LLVMValueRef = null_mut();
#[no_mangle]
pub static mut ExecEvalBoolSubroutineTemplate: LLVMValueRef = null_mut();

static mut llvm_types_module: LLVMModuleRef = null_mut();

static mut llvm_session_initialized: bool = false;
static mut llvm_generation: Size = 0;

/* number of LLVMJitContexts that currently are in use */
static mut llvm_jit_context_in_use_count: Size = 0;

/* how many times has the current LLVMContextRef been used */
static mut llvm_llvm_context_reuse_count: Size = 0;
static mut llvm_triple: *const c_char = null();
static mut llvm_layout: *const c_char = null();
static mut llvm_context: LLVMContextRef = null_mut();

static mut llvm_targetref: LLVMTargetRef = null_mut();
static mut llvm_ts_context: LLVMOrcThreadSafeContextRef = null_mut();
static mut llvm_opt0_orc: LLVMOrcLLJITRef = null_mut();
static mut llvm_opt3_orc: LLVMOrcLLJITRef = null_mut();

// ---------------------------------------------------------------------------
// ResourceOwner callbacks to hold JitContexts
// ---------------------------------------------------------------------------
static jit_resowner_desc: ResourceOwnerDesc = ResourceOwnerDesc {
    name: c"LLVM JIT context".as_ptr(),
    release_phase: RESOURCE_RELEASE_BEFORE_LOCKS,
    release_priority: RELEASE_PRIO_JIT_CONTEXTS,
    ReleaseResource: ResOwnerReleaseJitContext,
    DebugPrint: None, /* the default message is fine */
};

/* Convenience wrappers over ResourceOwnerRemember/Forget */
#[inline]
unsafe fn ResourceOwnerRememberJIT(owner: ResourceOwner, handle: *mut LLVMJitContext) {
    ResourceOwnerRemember(owner, PointerGetDatum(handle as *const c_void), &jit_resowner_desc);
}
#[inline]
unsafe fn ResourceOwnerForgetJIT(owner: ResourceOwner, handle: *mut LLVMJitContext) {
    ResourceOwnerForget(owner, PointerGetDatum(handle as *const c_void), &jit_resowner_desc);
}

/*
 * Initialize LLVM JIT provider.
 */
#[no_mangle]
pub unsafe extern "C" fn _PG_jit_provider_init(cb: *mut JitProviderCallbacks) {
    (*cb).reset_after_error = Some(llvm_reset_after_error_c);
    (*cb).release_context = Some(llvm_release_context_c);
    (*cb).compile_expr = Some(llvm_compile_expr_c);
}

// extern "C" thunks for the provider callbacks (JitProviderCallbacks uses
// extern "C" fn pointers).
unsafe extern "C" fn llvm_reset_after_error_c() {
    llvm_reset_after_error();
}
unsafe extern "C" fn llvm_release_context_c(context: *mut JitContext) {
    llvm_release_context(context);
}
unsafe extern "C" fn llvm_compile_expr_c(state: *mut crate::jit::jit::ExprState) -> bool {
    llvm_compile_expr(state)
}

/*
 * Every now and then create a new LLVMContextRef. Unfortunately, during every
 * round of inlining, types may "leak" (they can still be found/used via the
 * context, but new types will be created the next time in inlining is
 * performed). To prevent that from slowly accumulating problematic amounts of
 * memory, recreate the LLVMContextRef we use. We don't want to do so too
 * often, as that implies some overhead (particularly re-loading the module
 * summaries / modules is fairly expensive). A future TODO would be to make
 * this more finegrained and only drop/recreate the LLVMContextRef when we know
 * there has been inlining. If we can get the size of the context from LLVM
 * then that might be a better way to determine when to drop/recreate rather
 * then the usagecount heuristic currently employed.
 */
unsafe fn llvm_recreate_llvm_context() {
    if llvm_context.is_null() {
        elog!(ERROR, "Trying to recreate a non-existing context");
    }

    /*
     * We can only safely recreate the LLVM context if no other code is being
     * JITed, otherwise we'd release the types in use for that.
     */
    if llvm_jit_context_in_use_count > 0 {
        llvm_llvm_context_reuse_count += 1;
        return;
    }

    if llvm_llvm_context_reuse_count <= LLVMJIT_LLVM_CONTEXT_REUSE_MAX {
        llvm_llvm_context_reuse_count += 1;
        return;
    }

    /*
     * Need to reset the modules that the inlining code caches before
     * disposing of the context. LLVM modules exist within a specific LLVM
     * context, therefore disposing of the context before resetting the cache
     * would lead to dangling pointers to modules.
     */
    llvm_inline_reset_caches();

    LLVMContextDispose(llvm_context);
    llvm_context = LLVMContextCreate();
    llvm_llvm_context_reuse_count = 0;

    /*
     * Re-build cached type information, so code generation code can rely on
     * that information to be present (also prevents the variables to be
     * dangling references).
     */
    llvm_create_types();
}

/*
 * Create a context for JITing work.
 *
 * The context, including subsidiary resources, will be cleaned up either when
 * the context is explicitly released, or when the lifetime of
 * CurrentResourceOwner ends (usually the end of the current [sub]xact).
 */
pub unsafe fn llvm_create_context(jitFlags: c_int) -> *mut LLVMJitContext {
    let context: *mut LLVMJitContext;

    llvm_assert_in_fatal_section();

    llvm_session_initialize();

    llvm_recreate_llvm_context();

    ResourceOwnerEnlarge(CurrentResourceOwner);

    context = MemoryContextAllocZero(
        TopMemoryContext,
        core::mem::size_of::<LLVMJitContext>(),
    ) as *mut LLVMJitContext;
    (*context).base.flags = jitFlags;

    /* ensure cleanup */
    (*context).resowner = CurrentResourceOwner;
    ResourceOwnerRememberJIT(CurrentResourceOwner, context);

    llvm_jit_context_in_use_count += 1;

    context
}

/*
 * Release resources required by one llvm context.
 */
unsafe fn llvm_release_context(context: *mut JitContext) {
    let llvm_jit_context: *mut LLVMJitContext = context as *mut LLVMJitContext;
    let mut lc: *mut crate::nodes::pg_list::ListCell;

    /*
     * Consider as cleaned up even if we skip doing so below, that way we can
     * verify the tracking is correct (see llvm_shutdown()).
     */
    llvm_jit_context_in_use_count -= 1;

    /*
     * When this backend is exiting, don't clean up LLVM. As an error might
     * have occurred from within LLVM, we do not want to risk reentering. All
     * resource cleanup is going to happen through process exit.
     */
    if proc_exit_inprogress {
        return;
    }

    llvm_enter_fatal_on_oom();

    if !(*llvm_jit_context).module.is_null() {
        LLVMDisposeModule((*llvm_jit_context).module);
        (*llvm_jit_context).module = null_mut();
    }

    foreach!(lc, (*llvm_jit_context).handles, {
        let jit_handle: *mut LLVMJitHandle = lfirst(crate::current_cell!(lc)) as *mut LLVMJitHandle;

        {
            let ee: LLVMOrcExecutionSessionRef;
            let sp: LLVMOrcSymbolStringPoolRef;

            LLVMOrcResourceTrackerRemove((*jit_handle).resource_tracker);
            LLVMOrcReleaseResourceTracker((*jit_handle).resource_tracker);

            /*
             * Without triggering cleanup of the string pool, we'd leak
             * memory. It'd be sufficient to do this far less often, but in
             * experiments the required time was small enough to just always
             * do it.
             */
            ee = LLVMOrcLLJITGetExecutionSession((*jit_handle).lljit);
            sp = LLVMOrcExecutionSessionGetSymbolStringPool(ee);
            LLVMOrcSymbolStringPoolClearDeadEntries(sp);
        }

        pfree(jit_handle as *mut c_void);
    });
    list_free((*llvm_jit_context).handles);
    (*llvm_jit_context).handles = NIL;

    llvm_leave_fatal_on_oom();

    if !(*llvm_jit_context).resowner.is_null() {
        ResourceOwnerForgetJIT((*llvm_jit_context).resowner, llvm_jit_context);
    }
}

/*
 * Return module which may be modified, e.g. by creating new functions.
 */
pub unsafe fn llvm_mutable_module(context: *mut LLVMJitContext) -> LLVMModuleRef {
    llvm_assert_in_fatal_section();

    /*
     * If there's no in-progress module, create a new one.
     */
    if (*context).module.is_null() {
        (*context).compiled = false;
        (*context).module_generation = llvm_generation;
        llvm_generation += 1;
        (*context).module = LLVMModuleCreateWithNameInContext(c"pg".as_ptr(), llvm_context);
        LLVMSetTarget((*context).module, llvm_triple);
        LLVMSetDataLayout((*context).module, llvm_layout);
    }

    (*context).module
}

/*
 * Expand function name to be non-conflicting. This should be used by code
 * generating code, when adding new externally visible function definitions to
 * a Module.
 */
pub unsafe fn llvm_expand_funcname(
    context: *mut LLVMJitContext,
    basename: *const c_char,
) -> *mut c_char {
    Assert!(!(*context).module.is_null());

    (*context).base.instr.created_functions += 1;

    /*
     * Previously we used dots to separate, but turns out some tools, e.g.
     * GDB, don't like that and truncate name.
     */
    let r = psprintf(c"%s_%zu_%d".as_ptr());
    (*context).counter += 1;
    let _ = (basename, (*context).module_generation);
    r
}

/*
 * Return pointer to function funcname, which has to exist. If there's pending
 * code to be optimized and emitted, do so first.
 */
pub unsafe fn llvm_get_function(
    context: *mut LLVMJitContext,
    funcname: *const c_char,
) -> *mut c_void {
    let mut lc: *mut crate::nodes::pg_list::ListCell;

    llvm_assert_in_fatal_section();

    /*
     * If there is a pending / not emitted module, compile and emit now.
     * Otherwise we might not find the [correct] function.
     */
    if !(*context).compiled {
        llvm_compile_module(context);
    }

    /*
     * ORC's symbol table is of *unmangled* symbols. Therefore we don't need
     * to mangle here.
     */

    foreach!(lc, (*context).handles, {
        let handle: *mut LLVMJitHandle = lfirst(crate::current_cell!(lc)) as *mut LLVMJitHandle;
        let mut starttime: instr_time = instr_time::default();
        let mut endtime: instr_time = instr_time::default();
        let error: LLVMErrorRef;
        let mut addr: LLVMOrcJITTargetAddress;

        INSTR_TIME_SET_CURRENT(&mut starttime);

        addr = 0;
        error = LLVMOrcLLJITLookup((*handle).lljit, &mut addr, funcname);
        if !error.is_null() {
            elog!(
                ERROR,
                "failed to look up symbol \"{}\": {}",
                CStr::from_ptr(funcname).to_string_lossy(),
                CStr::from_ptr(llvm_error_message(error)).to_string_lossy()
            );
        }

        /*
         * LLJIT only actually emits code the first time a symbol is
         * referenced. Thus add lookup time to emission time. That's counting
         * a bit more than with older LLVM versions, but unlikely to ever
         * matter.
         */
        INSTR_TIME_SET_CURRENT(&mut endtime);
        INSTR_TIME_ACCUM_DIFF(
            &mut (*context).base.instr.emission_counter,
            endtime,
            starttime,
        );

        if addr != 0 {
            return addr as usize as *mut c_void;
        }
    });

    elog!(ERROR, "failed to JIT: {}", CStr::from_ptr(funcname).to_string_lossy());

    #[allow(unreachable_code)]
    null_mut()
}

/*
 * Return type of a variable in llvmjit_types.c. This is useful to keep types
 * in sync between plain C and JIT related code.
 */
pub unsafe fn llvm_pg_var_type(varname: *const c_char) -> LLVMTypeRef {
    let v_srcvar: LLVMValueRef;
    let typ: LLVMTypeRef;

    /* this'll return a *pointer* to the global */
    v_srcvar = LLVMGetNamedGlobal(llvm_types_module, varname);
    if v_srcvar.is_null() {
        elog!(
            ERROR,
            "variable {} not in llvmjit_types.c",
            CStr::from_ptr(varname).to_string_lossy()
        );
    }

    typ = LLVMGlobalGetValueType(v_srcvar);

    typ
}

/*
 * Return function type of a variable in llvmjit_types.c. This is useful to
 * keep function types in sync between C and JITed code.
 */
pub unsafe fn llvm_pg_var_func_type(varname: *const c_char) -> LLVMTypeRef {
    let v_srcvar: LLVMValueRef;
    let typ: LLVMTypeRef;

    v_srcvar = LLVMGetNamedFunction(llvm_types_module, varname);
    if v_srcvar.is_null() {
        elog!(
            ERROR,
            "function {} not in llvmjit_types.c",
            CStr::from_ptr(varname).to_string_lossy()
        );
    }

    typ = LLVMGetFunctionType(v_srcvar);

    typ
}

/*
 * Return declaration for a function referenced in llvmjit_types.c, adding it
 * to the module if necessary.
 *
 * This is used to make functions discovered via llvm_create_types() known to
 * the module that's currently being worked on.
 */
pub unsafe fn llvm_pg_func(mod_: LLVMModuleRef, funcname: *const c_char) -> LLVMValueRef {
    let v_srcfn: LLVMValueRef;
    let mut v_fn: LLVMValueRef;

    /* don't repeatedly add function */
    v_fn = LLVMGetNamedFunction(mod_, funcname);
    if !v_fn.is_null() {
        return v_fn;
    }

    v_srcfn = LLVMGetNamedFunction(llvm_types_module, funcname);

    if v_srcfn.is_null() {
        elog!(
            ERROR,
            "function {} not in llvmjit_types.c",
            CStr::from_ptr(funcname).to_string_lossy()
        );
    }

    v_fn = LLVMAddFunction(mod_, funcname, LLVMGetFunctionType(v_srcfn));
    llvm_copy_attributes(v_srcfn, v_fn);

    v_fn
}

/*
 * Copy attributes from one function to another, for a specific index (an
 * index can reference return value, function and parameter attributes).
 */
unsafe fn llvm_copy_attributes_at_index(v_from: LLVMValueRef, v_to: LLVMValueRef, index: uint32) {
    let num_attributes: c_int;
    let attrs: *mut LLVMAttributeRef;

    num_attributes = LLVMGetAttributeCountAtIndex(v_from, index);

    if num_attributes == 0 {
        return;
    }

    attrs = palloc(core::mem::size_of::<LLVMAttributeRef>() * num_attributes as usize)
        as *mut LLVMAttributeRef;
    LLVMGetAttributesAtIndex(v_from, index, attrs);

    let mut attno: c_int = 0;
    while attno < num_attributes {
        LLVMAddAttributeAtIndex(v_to, index, *attrs.add(attno as usize));
        attno += 1;
    }

    pfree(attrs as *mut c_void);
}

/*
 * Copy all attributes from one function to another. I.e. function, return and
 * parameters will be copied.
 */
pub unsafe fn llvm_copy_attributes(v_from: LLVMValueRef, v_to: LLVMValueRef) {
    let param_count: uint32;

    /* copy function attributes */
    llvm_copy_attributes_at_index(v_from, v_to, LLVMAttributeFunctionIndex);

    if LLVMGetTypeKind(LLVMGetFunctionReturnType(v_to)) != LLVMVoidTypeKind {
        /* and the return value attributes */
        llvm_copy_attributes_at_index(v_from, v_to, LLVMAttributeReturnIndex);
    }

    /* and each function parameter's attribute */
    param_count = LLVMCountParams(v_from);

    let mut paramidx: uint32 = 1;
    while paramidx <= param_count {
        llvm_copy_attributes_at_index(v_from, v_to, paramidx);
        paramidx += 1;
    }
}

/*
 * Return a callable LLVMValueRef for fcinfo.
 */
pub unsafe fn llvm_function_reference(
    context: *mut LLVMJitContext,
    builder: LLVMBuilderRef,
    mod_: LLVMModuleRef,
    fcinfo: FunctionCallInfo,
) -> LLVMValueRef {
    let mut modname: *mut c_char = null_mut();
    let mut basename: *mut c_char = null_mut();
    let funcname: *mut c_char;

    let mut v_fn: LLVMValueRef;

    fmgr_symbol((*(*fcinfo).flinfo).fn_oid, &mut modname, &mut basename);

    if !modname.is_null() && !basename.is_null() {
        /* external function in loadable library */
        funcname = psprintf(c"pgextern.%s.%s".as_ptr());
        let _ = (modname, basename);
    } else if !basename.is_null() {
        /* internal function */
        funcname = pstrdup(basename);
    } else {
        /*
         * Function we don't know to handle, return pointer. We do so by
         * creating a global constant containing a pointer to the function.
         * Makes IR more readable.
         */
        let v_fn_addr: LLVMValueRef;

        funcname = psprintf(c"pgoidextern.%u".as_ptr());
        let _ = (*(*fcinfo).flinfo).fn_oid;
        v_fn = LLVMGetNamedGlobal(mod_, funcname);
        if v_fn as usize != 0 {
            return l_load(builder, TypePGFunction, v_fn, c"".as_ptr());
        }

        v_fn_addr = l_ptr_const(
            (*(*fcinfo).flinfo).fn_addr.map(|f| f as *mut c_void).unwrap_or(null_mut()),
            TypePGFunction,
        );

        v_fn = LLVMAddGlobal(mod_, TypePGFunction, funcname);
        LLVMSetInitializer(v_fn, v_fn_addr);
        LLVMSetGlobalConstant(v_fn, true as c_int);
        LLVMSetLinkage(v_fn, LLVMPrivateLinkage);
        LLVMSetUnnamedAddr(v_fn, true as c_int);

        return l_load(builder, TypePGFunction, v_fn, c"".as_ptr());
    }

    /* check if function already has been added */
    v_fn = LLVMGetNamedFunction(mod_, funcname);
    if v_fn as usize != 0 {
        return v_fn;
    }

    v_fn = LLVMAddFunction(mod_, funcname, LLVMGetFunctionType(AttributeTemplate));

    v_fn
}

/*
 * Optimize code in module using the flags set in context.
 */
unsafe fn llvm_optimize_module(context: *mut LLVMJitContext, module: LLVMModuleRef) {
    // LLVM_VERSION_MAJOR >= 17 path (only this branch is translated; the older
    // legacy PassManagerBuilder path is gated out on modern LLVM).
    let options: LLVMPassBuilderOptionsRef;
    let err: LLVMErrorRef;
    let passes: *const c_char;

    if ((*context).base.flags & PGJIT_OPT3) != 0 {
        passes = c"default<O3>".as_ptr();
    } else if ((*context).base.flags & PGJIT_INLINE) != 0 {
        /* if doing inlining, but no expensive optimization, add inline pass */
        passes = c"default<O0>,mem2reg,inline".as_ptr();
    } else {
        /* default<O0> includes always-inline pass */
        passes = c"default<O0>,mem2reg".as_ptr();
    }

    options = LLVMCreatePassBuilderOptions();

    // #ifdef LLVM_PASS_DEBUG -> LLVMPassBuilderOptionsSetDebugLogging
    // #ifdef USE_ASSERT_CHECKING -> LLVMPassBuilderOptionsSetVerifyEach
    // (both disabled in this build configuration)

    LLVMPassBuilderOptionsSetInlinerThreshold(options, 512);

    err = LLVMRunPasses(module, passes, null_mut(), options);

    if !err.is_null() {
        elog!(
            ERROR,
            "failed to JIT module: {}",
            CStr::from_ptr(llvm_error_message(err)).to_string_lossy()
        );
    }

    LLVMDisposePassBuilderOptions(options);
}

/*
 * Emit code for the currently pending module.
 */
unsafe fn llvm_compile_module(context: *mut LLVMJitContext) {
    let handle: *mut LLVMJitHandle;
    let oldcontext: MemoryContext;
    let mut starttime: instr_time = instr_time::default();
    let mut endtime: instr_time = instr_time::default();
    let compile_orc: LLVMOrcLLJITRef;

    if ((*context).base.flags & PGJIT_OPT3) != 0 {
        compile_orc = llvm_opt3_orc;
    } else {
        compile_orc = llvm_opt0_orc;
    }

    /* perform inlining */
    if ((*context).base.flags & PGJIT_INLINE) != 0 {
        INSTR_TIME_SET_CURRENT(&mut starttime);
        llvm_inline((*context).module);
        INSTR_TIME_SET_CURRENT(&mut endtime);
        INSTR_TIME_ACCUM_DIFF(
            &mut (*context).base.instr.inlining_counter,
            endtime,
            starttime,
        );
    }

    if jit_dump_bitcode {
        let filename: *mut c_char;

        filename = psprintf(c"%d.%zu.bc".as_ptr());
        let _ = (MyProcPid, (*context).module_generation);
        LLVMWriteBitcodeToFile((*context).module, filename);
        pfree(filename as *mut c_void);
    }

    /* optimize according to the chosen optimization settings */
    INSTR_TIME_SET_CURRENT(&mut starttime);
    llvm_optimize_module(context, (*context).module);
    INSTR_TIME_SET_CURRENT(&mut endtime);
    INSTR_TIME_ACCUM_DIFF(
        &mut (*context).base.instr.optimization_counter,
        endtime,
        starttime,
    );

    if jit_dump_bitcode {
        let filename: *mut c_char;

        filename = psprintf(c"%d.%zu.optimized.bc".as_ptr());
        let _ = (MyProcPid, (*context).module_generation);
        LLVMWriteBitcodeToFile((*context).module, filename);
        pfree(filename as *mut c_void);
    }

    handle = MemoryContextAlloc(TopMemoryContext, core::mem::size_of::<LLVMJitHandle>())
        as *mut LLVMJitHandle;

    /*
     * Emit the code. Note that this can, depending on the optimization
     * settings, take noticeable resources as code emission executes low-level
     * instruction combining/selection passes etc. Without optimization a
     * faster instruction selection mechanism is used.
     */
    INSTR_TIME_SET_CURRENT(&mut starttime);
    {
        let ts_module: LLVMOrcThreadSafeModuleRef;
        let error: LLVMErrorRef;
        let jd: LLVMOrcJITDylibRef = LLVMOrcLLJITGetMainJITDylib(compile_orc);

        ts_module = LLVMOrcCreateNewThreadSafeModule((*context).module, llvm_ts_context);

        (*handle).lljit = compile_orc;
        (*handle).resource_tracker = LLVMOrcJITDylibCreateResourceTracker(jd);

        /*
         * NB: This doesn't actually emit code. That happens lazily the first
         * time a symbol defined in the module is requested. Due to that
         * llvm_get_function() also accounts for emission time.
         */

        (*context).module = null_mut(); /* will be owned by LLJIT */
        error = LLVMOrcLLJITAddLLVMIRModuleWithRT(
            compile_orc,
            (*handle).resource_tracker,
            ts_module,
        );

        if !error.is_null() {
            elog!(
                ERROR,
                "failed to JIT module: {}",
                CStr::from_ptr(llvm_error_message(error)).to_string_lossy()
            );
        }

        /* LLVMOrcLLJITAddLLVMIRModuleWithRT takes ownership of the module */
    }

    INSTR_TIME_SET_CURRENT(&mut endtime);
    INSTR_TIME_ACCUM_DIFF(
        &mut (*context).base.instr.emission_counter,
        endtime,
        starttime,
    );

    (*context).module = null_mut();
    (*context).compiled = true;

    /* remember emitted code for cleanup and lookups */
    oldcontext = MemoryContextSwitchTo(TopMemoryContext);
    (*context).handles = lappend((*context).handles, handle as *mut c_void);
    MemoryContextSwitchTo(oldcontext);

    // C also: errhidestmt(true), errhidecontext(true)
    ereport!(
        DEBUG1,
        errmsg!(
            "time to inline: {:.3}s, opt: {:.3}s, emit: {:.3}s",
            INSTR_TIME_GET_DOUBLE((*context).base.instr.inlining_counter),
            INSTR_TIME_GET_DOUBLE((*context).base.instr.optimization_counter),
            INSTR_TIME_GET_DOUBLE((*context).base.instr.emission_counter)
        )
    );
}

/*
 * Per session initialization.
 */
unsafe fn llvm_session_initialize() {
    let oldcontext: MemoryContext;
    let mut error: *mut c_char = null_mut();
    let mut cpu: *mut c_char;
    let mut features: *mut c_char;
    let mut opt0_tm: LLVMTargetMachineRef;
    let mut opt3_tm: LLVMTargetMachineRef;

    if llvm_session_initialized {
        return;
    }

    oldcontext = MemoryContextSwitchTo(TopMemoryContext);

    LLVMInitializeNativeTarget();
    LLVMInitializeNativeAsmPrinter();
    LLVMInitializeNativeAsmParser();

    if llvm_context.is_null() {
        llvm_context = LLVMContextCreate();

        llvm_jit_context_in_use_count = 0;
        llvm_llvm_context_reuse_count = 0;
    }

    /*
     * Synchronize types early, as that also includes inferring the target
     * triple.
     */
    llvm_create_types();

    /*
     * Extract target information from loaded module.
     */
    llvm_set_target();

    if LLVMGetTargetFromTriple(llvm_triple, &mut llvm_targetref, &mut error) != 0 {
        elog!(
            FATAL,
            "failed to query triple {}",
            CStr::from_ptr(error).to_string_lossy()
        );
    }

    /*
     * We want the generated code to use all available features. Therefore
     * grab the host CPU string and detect features of the current CPU. The
     * latter is needed because some CPU architectures default to enabling
     * features not all CPUs have (weird, huh).
     */
    cpu = LLVMGetHostCPUName();
    features = LLVMGetHostCPUFeatures();
    elog!(
        DEBUG2,
        "LLVMJIT detected CPU \"{}\", with features \"{}\"",
        CStr::from_ptr(cpu).to_string_lossy(),
        CStr::from_ptr(features).to_string_lossy()
    );

    opt0_tm = LLVMCreateTargetMachine(
        llvm_targetref,
        llvm_triple,
        cpu,
        features,
        LLVMCodeGenLevelNone,
        LLVMRelocDefault,
        LLVMCodeModelJITDefault,
    );
    opt3_tm = LLVMCreateTargetMachine(
        llvm_targetref,
        llvm_triple,
        cpu,
        features,
        LLVMCodeGenLevelAggressive,
        LLVMRelocDefault,
        LLVMCodeModelJITDefault,
    );

    LLVMDisposeMessage(cpu);
    cpu = null_mut();
    LLVMDisposeMessage(features);
    features = null_mut();
    let _ = (cpu, features);

    /* force symbols in main binary to be loaded */
    LLVMLoadLibraryPermanently(null());

    {
        llvm_ts_context = LLVMOrcCreateNewThreadSafeContext();

        llvm_opt0_orc = llvm_create_jit_instance(opt0_tm);
        opt0_tm = null_mut();

        llvm_opt3_orc = llvm_create_jit_instance(opt3_tm);
        opt3_tm = null_mut();
        let _ = (opt0_tm, opt3_tm);
    }

    on_proc_exit(llvm_shutdown, 0 as Datum);

    llvm_session_initialized = true;

    MemoryContextSwitchTo(oldcontext);
}

unsafe extern "C" fn llvm_shutdown(code: c_int, arg: Datum) {
    /*
     * If llvm_shutdown() is reached while in a fatal-on-oom section an error
     * has occurred in the middle of LLVM code. It is not safe to call back
     * into LLVM (which is why a FATAL error was thrown).
     *
     * We do need to shutdown LLVM in other shutdown cases, otherwise e.g.
     * profiling data won't be written out.
     */
    if llvm_in_fatal_on_oom() {
        Assert!(proc_exit_inprogress);
        return;
    }

    if llvm_jit_context_in_use_count != 0 {
        elog!(
            PANIC,
            "LLVMJitContext in use count not 0 at exit (is {})",
            llvm_jit_context_in_use_count
        );
    }

    {
        if !llvm_opt3_orc.is_null() {
            LLVMOrcDisposeLLJIT(llvm_opt3_orc);
            llvm_opt3_orc = null_mut();
        }
        if !llvm_opt0_orc.is_null() {
            LLVMOrcDisposeLLJIT(llvm_opt0_orc);
            llvm_opt0_orc = null_mut();
        }
        if !llvm_ts_context.is_null() {
            LLVMOrcDisposeThreadSafeContext(llvm_ts_context);
            llvm_ts_context = null_mut();
        }
    }
}

/* helper for llvm_create_types, returning a function's return type */
unsafe fn load_return_type(mod_: LLVMModuleRef, name: *const c_char) -> LLVMTypeRef {
    let value: LLVMValueRef;
    let typ: LLVMTypeRef;

    /* this'll return a *pointer* to the function */
    value = LLVMGetNamedFunction(mod_, name);
    if value.is_null() {
        elog!(
            ERROR,
            "function {} is unknown",
            CStr::from_ptr(name).to_string_lossy()
        );
    }

    typ = LLVMGetFunctionReturnType(value); /* in llvmjit_wrap.cpp */

    typ
}

/*
 * Load triple & layout from clang emitted file so we're guaranteed to be
 * compatible.
 */
unsafe fn llvm_set_target() {
    if llvm_types_module.is_null() {
        elog!(
            ERROR,
            "failed to extract target information, llvmjit_types.c not loaded"
        );
    }

    if llvm_triple.is_null() {
        llvm_triple = pstrdup(LLVMGetTarget(llvm_types_module));
    }

    if llvm_layout.is_null() {
        llvm_layout = pstrdup(LLVMGetDataLayoutStr(llvm_types_module));
    }
}

/*
 * Load required information, types, function signatures from llvmjit_types.c
 * and make them available in global variables.
 *
 * Those global variables are then used while emitting code.
 */
unsafe fn llvm_create_types() {
    let mut path = [0 as c_char; MAXPGPATH as usize];
    let mut buf: LLVMMemoryBufferRef = null_mut();
    let mut msg: *mut c_char = null_mut();

    snprintf(
        path.as_mut_ptr(),
        MAXPGPATH as usize,
        c"%s/%s".as_ptr(),
        pkglib_path.as_ptr(),
        c"llvmjit_types.bc".as_ptr(),
    );

    /* open file */
    if LLVMCreateMemoryBufferWithContentsOfFile(path.as_ptr(), &mut buf, &mut msg) != 0 {
        elog!(
            ERROR,
            "LLVMCreateMemoryBufferWithContentsOfFile({}) failed: {}",
            CStr::from_ptr(path.as_ptr()).to_string_lossy(),
            CStr::from_ptr(msg).to_string_lossy()
        );
    }

    /* eagerly load contents, going to need it all */
    if LLVMParseBitcodeInContext2(llvm_context, buf, &mut llvm_types_module) != 0 {
        elog!(
            ERROR,
            "LLVMParseBitcodeInContext2 of {} failed",
            CStr::from_ptr(path.as_ptr()).to_string_lossy()
        );
    }
    LLVMDisposeMemoryBuffer(buf);

    TypeSizeT = llvm_pg_var_type(c"TypeSizeT".as_ptr());
    TypeParamBool = load_return_type(llvm_types_module, c"FunctionReturningBool".as_ptr());
    TypeStorageBool = llvm_pg_var_type(c"TypeStorageBool".as_ptr());
    TypePGFunction = llvm_pg_var_type(c"TypePGFunction".as_ptr());
    StructNullableDatum = llvm_pg_var_type(c"StructNullableDatum".as_ptr());
    StructExprContext = llvm_pg_var_type(c"StructExprContext".as_ptr());
    StructExprEvalStep = llvm_pg_var_type(c"StructExprEvalStep".as_ptr());
    StructExprState = llvm_pg_var_type(c"StructExprState".as_ptr());
    StructFunctionCallInfoData = llvm_pg_var_type(c"StructFunctionCallInfoData".as_ptr());
    StructMemoryContextData = llvm_pg_var_type(c"StructMemoryContextData".as_ptr());
    StructTupleTableSlot = llvm_pg_var_type(c"StructTupleTableSlot".as_ptr());
    StructHeapTupleTableSlot = llvm_pg_var_type(c"StructHeapTupleTableSlot".as_ptr());
    StructMinimalTupleTableSlot = llvm_pg_var_type(c"StructMinimalTupleTableSlot".as_ptr());
    StructHeapTupleData = llvm_pg_var_type(c"StructHeapTupleData".as_ptr());
    StructHeapTupleHeaderData = llvm_pg_var_type(c"StructHeapTupleHeaderData".as_ptr());
    StructTupleDescData = llvm_pg_var_type(c"StructTupleDescData".as_ptr());
    StructAggState = llvm_pg_var_type(c"StructAggState".as_ptr());
    StructAggStatePerGroupData = llvm_pg_var_type(c"StructAggStatePerGroupData".as_ptr());
    StructAggStatePerTransData = llvm_pg_var_type(c"StructAggStatePerTransData".as_ptr());
    StructPlanState = llvm_pg_var_type(c"StructPlanState".as_ptr());
    StructMinimalTupleData = llvm_pg_var_type(c"StructMinimalTupleData".as_ptr());

    AttributeTemplate = LLVMGetNamedFunction(llvm_types_module, c"AttributeTemplate".as_ptr());
    ExecEvalSubroutineTemplate =
        LLVMGetNamedFunction(llvm_types_module, c"ExecEvalSubroutineTemplate".as_ptr());
    ExecEvalBoolSubroutineTemplate =
        LLVMGetNamedFunction(llvm_types_module, c"ExecEvalBoolSubroutineTemplate".as_ptr());
}

/*
 * Split a symbol into module / function parts.  If the function is in the
 * main binary (or an external library) *modname will be NULL.
 */
pub unsafe fn llvm_split_symbol_name(
    name: *const c_char,
    modname: *mut *mut c_char,
    funcname: *mut *mut c_char,
) {
    *modname = null_mut();
    *funcname = null_mut();

    /*
     * Module function names are pgextern.$module.$funcname
     */
    if strncmp(name, c"pgextern.".as_ptr(), strlen(c"pgextern.".as_ptr())) == 0 {
        /*
         * Symbol names cannot contain a ., therefore we can split based on
         * first and last occurrence of one.
         */
        *funcname = rindex(name, '.' as c_int);
        *funcname = (*funcname).add(1); /* jump over . */

        *modname = pnstrdup(
            name.add(strlen(c"pgextern.".as_ptr())),
            ((*funcname as isize) - (name as isize)) as usize - strlen(c"pgextern.".as_ptr()) - 1,
        );
        Assert!(!funcname.is_null());

        *funcname = pstrdup(*funcname);
    } else {
        *modname = null_mut();
        *funcname = pstrdup(name);
    }
}

/*
 * Attempt to resolve symbol, so LLVM can emit a reference to it.
 */
unsafe fn llvm_resolve_symbol(symname: *const c_char, ctx: *mut c_void) -> uint64 {
    let addr: usize;
    let mut funcname: *mut c_char = null_mut();
    let mut modname: *mut c_char = null_mut();

    /*
     * macOS prefixes all object level symbols with an underscore. But neither
     * dlsym() nor PG's inliner expect that. So undo.
     */
    let mut symname = symname;
    #[cfg(target_os = "macos")]
    {
        if *symname != b'_' as c_char {
            elog!(
                ERROR,
                "expected prefixed symbol name, but got \"{}\"",
                CStr::from_ptr(symname).to_string_lossy()
            );
        }
        symname = symname.add(1);
    }

    llvm_split_symbol_name(symname, &mut modname, &mut funcname);

    /* functions that aren't resolved to names shouldn't ever get here */
    Assert!(!funcname.is_null());

    if !modname.is_null() {
        addr = load_external_function(modname, funcname, true, null_mut()) as usize;
    } else {
        addr = LLVMSearchForAddressOfSymbol(symname) as usize;
    }

    pfree(funcname as *mut c_void);
    if !modname.is_null() {
        pfree(modname as *mut c_void);
    }

    /* let LLVM will error out - should never happen */
    if addr == 0 {
        elog!(
            WARNING,
            "failed to resolve name {}",
            CStr::from_ptr(symname).to_string_lossy()
        );
    }

    addr as uint64
}

unsafe extern "C" fn llvm_resolve_symbols(
    GeneratorObj: LLVMOrcDefinitionGeneratorRef,
    Ctx: *mut c_void,
    LookupState: *mut LLVMOrcLookupStateRef,
    Kind: LLVMOrcLookupKind,
    JD: LLVMOrcJITDylibRef,
    JDLookupFlags: LLVMOrcJITDylibLookupFlags,
    LookupSet: LLVMOrcCLookupSet,
    LookupSetSize: usize,
) -> LLVMErrorRef {
    // LLVM_VERSION_MAJOR > 14
    let symbols: LLVMOrcCSymbolMapPairs = palloc0(
        core::mem::size_of::<LLVMOrcCSymbolMapPair>() * LookupSetSize,
    ) as LLVMOrcCSymbolMapPairs;
    let error: LLVMErrorRef;
    let mu: LLVMOrcMaterializationUnitRef;

    let mut i: usize = 0;
    while i < LookupSetSize {
        let name: *const c_char = LLVMOrcSymbolStringPoolEntryStr((*LookupSet.add(i)).Name);

        LLVMOrcRetainSymbolStringPoolEntry((*LookupSet.add(i)).Name);
        (*symbols.add(i)).Name = (*LookupSet.add(i)).Name;
        (*symbols.add(i)).Sym.Address = llvm_resolve_symbol(name, null_mut());
        (*symbols.add(i)).Sym.Flags.GenericFlags = LLVMJITSymbolGenericFlagsExported;
        i += 1;
    }

    mu = LLVMOrcAbsoluteSymbols(symbols, LookupSetSize);
    error = LLVMOrcJITDylibDefine(JD, mu);
    if error as usize != LLVMErrorSuccess as usize {
        LLVMOrcDisposeMaterializationUnit(mu);
    }

    pfree(symbols as *mut c_void);

    error
}

/*
 * We cannot throw errors through LLVM (without causing a FATAL at least), so
 * just use WARNING here. That's OK anyway, as the error is also reported at
 * the top level action (with less detail) and there might be multiple
 * invocations of errors with details.
 *
 * This doesn't really happen during normal operation, but in cases like
 * symbol resolution breakage. So just using elog(WARNING) is fine.
 */
unsafe extern "C" fn llvm_log_jit_error(ctx: *mut c_void, error: LLVMErrorRef) {
    elog!(
        WARNING,
        "error during JITing: {}",
        CStr::from_ptr(llvm_error_message(error)).to_string_lossy()
    );
}

/*
 * Create our own object layer, so we can add event listeners.
 */
unsafe extern "C" fn llvm_create_object_layer(
    Ctx: *mut c_void,
    ES: LLVMOrcExecutionSessionRef,
    Triple: *const c_char,
) -> LLVMOrcObjectLayerRef {
    let objlayer: LLVMOrcObjectLayerRef = if USE_LLVM_BACKPORT_SECTION_MEMORY_MANAGER {
        LLVMOrcCreateRTDyldObjectLinkingLayerWithSafeSectionMemoryManager(ES)
    } else {
        LLVMOrcCreateRTDyldObjectLinkingLayerWithSectionMemoryManager(ES)
    };

    // HAVE_DECL_LLVMCREATEGDBREGISTRATIONLISTENER
    if jit_debugging_support {
        let l: LLVMJITEventListenerRef = LLVMCreateGDBRegistrationListener();

        LLVMOrcRTDyldObjectLinkingLayerRegisterJITEventListener(objlayer, l);
    }

    // HAVE_DECL_LLVMCREATEPERFJITEVENTLISTENER
    if jit_profiling_support {
        let l: LLVMJITEventListenerRef = LLVMCreatePerfJITEventListener();

        if !l.is_null() {
            LLVMOrcRTDyldObjectLinkingLayerRegisterJITEventListener(objlayer, l);
        }
    }

    objlayer
}

/*
 * Create LLJIT instance, using the passed in target machine. Note that the
 * target machine afterwards is owned by the LLJIT instance.
 */
unsafe fn llvm_create_jit_instance(tm: LLVMTargetMachineRef) -> LLVMOrcLLJITRef {
    let mut lljit: LLVMOrcLLJITRef = null_mut();
    let tm_builder: LLVMOrcJITTargetMachineBuilderRef;
    let lljit_builder: LLVMOrcLLJITBuilderRef;
    let mut error: LLVMErrorRef;
    let mut main_gen: LLVMOrcDefinitionGeneratorRef = null_mut();
    let ref_gen: LLVMOrcDefinitionGeneratorRef;

    lljit_builder = LLVMOrcCreateLLJITBuilder();
    tm_builder = LLVMOrcJITTargetMachineBuilderCreateFromTargetMachine(tm);
    LLVMOrcLLJITBuilderSetJITTargetMachineBuilder(lljit_builder, tm_builder);

    LLVMOrcLLJITBuilderSetObjectLinkingLayerCreator(
        lljit_builder,
        Some(llvm_create_object_layer),
        null_mut(),
    );

    error = LLVMOrcCreateLLJIT(&mut lljit, lljit_builder);
    if !error.is_null() {
        elog!(
            ERROR,
            "failed to create lljit instance: {}",
            CStr::from_ptr(llvm_error_message(error)).to_string_lossy()
        );
    }

    LLVMOrcExecutionSessionSetErrorReporter(
        LLVMOrcLLJITGetExecutionSession(lljit),
        Some(llvm_log_jit_error),
        null_mut(),
    );

    /*
     * Symbol resolution support for symbols in the postgres binary /
     * libraries already loaded.
     */
    error = LLVMOrcCreateDynamicLibrarySearchGeneratorForProcess(
        &mut main_gen,
        LLVMOrcLLJITGetGlobalPrefix(lljit),
        None,
        null_mut(),
    );
    if !error.is_null() {
        elog!(
            ERROR,
            "failed to create generator: {}",
            CStr::from_ptr(llvm_error_message(error)).to_string_lossy()
        );
    }
    LLVMOrcJITDylibAddGenerator(LLVMOrcLLJITGetMainJITDylib(lljit), main_gen);

    /*
     * Symbol resolution support for "special" functions, e.g. a call into an
     * SQL callable function.
     */
    // LLVM_VERSION_MAJOR > 14
    ref_gen = LLVMOrcCreateCustomCAPIDefinitionGenerator(
        Some(llvm_resolve_symbols),
        null_mut(),
        None,
    );
    LLVMOrcJITDylibAddGenerator(LLVMOrcLLJITGetMainJITDylib(lljit), ref_gen);

    lljit
}

unsafe fn llvm_error_message(error: LLVMErrorRef) -> *mut c_char {
    let orig: *mut c_char = LLVMGetErrorMessage(error);
    let msg: *mut c_char = pstrdup(orig);

    LLVMDisposeErrorMessage(orig);

    msg
}

/*
 * ResourceOwner callbacks
 */
unsafe fn ResOwnerReleaseJitContext(res: Datum) {
    let context: *mut LLVMJitContext = DatumGetPointer(res) as *mut LLVMJitContext;

    (*context).resowner = null_mut();
    jit_release_context(&mut (*context).base);
}
