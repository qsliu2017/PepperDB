//! jit/llvmjit_emit.h - Helpers to make emitting LLVM IR a bit more concise and pgindent proof.
//!
//! This header is gated behind `#ifdef USE_LLVM` in C; its contents are LLVM-C
//! `static inline` helpers. There is no Rust LLVM-C binding in this tree, so the
//! LLVM-C reference types are stubbed locally as opaque `*mut c_void` aliases and
//! the helper bodies are emitted as `unimplemented!()` faithful 1:1 prototypes.
//! (Per translation rules: function-like / inline helpers -> `#[inline] pub fn`.)

#![allow(non_snake_case)]
#![allow(unused_variables)]

use std::ffi::{c_int, c_void};
use crate::c::{int16, int32, int64, int8, Size};

// ---------------------------------------------------------------------------
// LLVM-C opaque type stubs (from <llvm-c/Core.h> / <llvm-c/Target.h>).
// No Rust LLVM-C binding exists in this tree; alias each as an opaque pointer.
// TODO: dedup with a real LLVM-C binding module when one exists.
// ---------------------------------------------------------------------------
pub type LLVMValueRef = *mut c_void;
pub type LLVMTypeRef = *mut c_void;
pub type LLVMContextRef = *mut c_void;
pub type LLVMBuilderRef = *mut c_void;
pub type LLVMModuleRef = *mut c_void;
pub type LLVMBasicBlockRef = *mut c_void;
pub type LLVMAttributeRef = *mut c_void;

// ---------------------------------------------------------------------------
// Globals referenced from llvmjit.h (jit/llvmjit.c module-level LLVM types).
// These are LLVM `LLVMTypeRef` / `LLVMValueRef` globals defined in C in
// llvmjit.c; stubbed here as the corresponding opaque types so the helper
// bodies type-check against the same names used in C.
// TODO: dedup with src/jit/llvmjit.rs once that module defines these.
// ---------------------------------------------------------------------------

/*
 * Emit a non-LLVM pointer as an LLVM constant.
 *
 *   LLVMValueRef c = LLVMConstInt(TypeSizeT, (uintptr_t) ptr, false);
 *   return LLVMConstIntToPtr(c, type);
 */
#[inline]
pub unsafe fn l_ptr_const(ptr: *mut c_void, type_: LLVMTypeRef) -> LLVMValueRef {
    unimplemented!()
}

/*
 * Emit pointer.
 *   return LLVMPointerType(t, 0);
 */
#[inline]
pub unsafe fn l_ptr(t: LLVMTypeRef) -> LLVMTypeRef {
    unimplemented!()
}

/*
 * Emit constant integer.
 *   return LLVMConstInt(LLVMInt8TypeInContext(lc), i, false);
 */
#[inline]
pub unsafe fn l_int8_const(lc: LLVMContextRef, i: int8) -> LLVMValueRef {
    unimplemented!()
}

/*
 * Emit constant integer.
 *   return LLVMConstInt(LLVMInt16TypeInContext(lc), i, false);
 */
#[inline]
pub unsafe fn l_int16_const(lc: LLVMContextRef, i: int16) -> LLVMValueRef {
    unimplemented!()
}

/*
 * Emit constant integer.
 *   return LLVMConstInt(LLVMInt32TypeInContext(lc), i, false);
 */
#[inline]
pub unsafe fn l_int32_const(lc: LLVMContextRef, i: int32) -> LLVMValueRef {
    unimplemented!()
}

/*
 * Emit constant integer.
 *   return LLVMConstInt(LLVMInt64TypeInContext(lc), i, false);
 */
#[inline]
pub unsafe fn l_int64_const(lc: LLVMContextRef, i: int64) -> LLVMValueRef {
    unimplemented!()
}

/*
 * Emit constant integer.
 *   return LLVMConstInt(TypeSizeT, i, false);
 */
#[inline]
pub unsafe fn l_sizet_const(i: Size) -> LLVMValueRef {
    unimplemented!()
}

/*
 * Emit constant boolean, as used for storage (e.g. global vars, structs).
 *   return LLVMConstInt(TypeStorageBool, (int) i, false);
 */
#[inline]
pub unsafe fn l_sbool_const(i: bool) -> LLVMValueRef {
    unimplemented!()
}

/*
 * Emit constant boolean, as used for parameters (e.g. function parameters).
 *   return LLVMConstInt(TypeParamBool, (int) i, false);
 */
#[inline]
pub unsafe fn l_pbool_const(i: bool) -> LLVMValueRef {
    unimplemented!()
}

/*
 *   return LLVMBuildStructGEP2(b, t, v, idx, "");
 */
#[inline]
pub unsafe fn l_struct_gep(
    b: LLVMBuilderRef,
    t: LLVMTypeRef,
    v: LLVMValueRef,
    idx: int32,
    name: *const c_char,
) -> LLVMValueRef {
    unimplemented!()
}

/*
 *   return LLVMBuildGEP2(b, t, v, indices, nindices, name);
 */
#[inline]
pub unsafe fn l_gep(
    b: LLVMBuilderRef,
    t: LLVMTypeRef,
    v: LLVMValueRef,
    indices: *mut LLVMValueRef,
    nindices: int32,
    name: *const c_char,
) -> LLVMValueRef {
    unimplemented!()
}

/*
 *   return LLVMBuildLoad2(b, t, v, name);
 */
#[inline]
pub unsafe fn l_load(
    b: LLVMBuilderRef,
    t: LLVMTypeRef,
    v: LLVMValueRef,
    name: *const c_char,
) -> LLVMValueRef {
    unimplemented!()
}

/*
 *   return LLVMBuildCall2(b, t, fn, args, nargs, name);
 */
#[inline]
pub unsafe fn l_call(
    b: LLVMBuilderRef,
    t: LLVMTypeRef,
    fn_: LLVMValueRef,
    args: *mut LLVMValueRef,
    nargs: int32,
    name: *const c_char,
) -> LLVMValueRef {
    unimplemented!()
}

/*
 * Load a pointer member idx from a struct.
 *   return l_load(b,
 *                 LLVMStructGetTypeAtIndex(t, idx),
 *                 l_struct_gep(b, t, v, idx, ""),
 *                 name);
 */
#[inline]
pub unsafe fn l_load_struct_gep(
    b: LLVMBuilderRef,
    t: LLVMTypeRef,
    v: LLVMValueRef,
    idx: int32,
    name: *const c_char,
) -> LLVMValueRef {
    unimplemented!()
}

/*
 * Load value of a pointer, after applying one index operation.
 *   return l_load(b, t, l_gep(b, t, v, &idx, 1, ""), name);
 */
#[inline]
pub unsafe fn l_load_gep1(
    b: LLVMBuilderRef,
    t: LLVMTypeRef,
    v: LLVMValueRef,
    idx: LLVMValueRef,
    name: *const c_char,
) -> LLVMValueRef {
    unimplemented!()
}

/*
 * Insert a new basic block, just before r, the name being determined by fmt
 * and arguments.  In C this is a varargs function (pg_attribute_printf(2, 3));
 * Rust has no stable C-varargs in safe defs, so the variadic tail is dropped
 * and the body stubbed.
 */
#[inline]
pub unsafe fn l_bb_before_v(r: LLVMBasicBlockRef, fmt: *const c_char) -> LLVMBasicBlockRef {
    unimplemented!()
}

/*
 * Insert a new basic block after previous basic blocks, the name being
 * determined by fmt and arguments.  C varargs; tail dropped (see above).
 */
#[inline]
pub unsafe fn l_bb_append_v(f: LLVMValueRef, fmt: *const c_char) -> LLVMBasicBlockRef {
    unimplemented!()
}

/*
 * Mark a callsite as readonly.
 */
#[inline]
pub unsafe fn l_callsite_ro(f: LLVMValueRef) {
    unimplemented!()
}

/*
 * Mark a callsite as alwaysinline.
 */
#[inline]
pub unsafe fn l_callsite_alwaysinline(f: LLVMValueRef) {
    unimplemented!()
}

/*
 * Emit code to switch memory context.
 */
#[inline]
pub unsafe fn l_mcxt_switch(
    mod_: LLVMModuleRef,
    b: LLVMBuilderRef,
    nc: LLVMValueRef,
) -> LLVMValueRef {
    unimplemented!()
}

/*
 * Return pointer to the argno'th argument nullness.
 * Uses FIELDNO_FUNCTIONCALLINFODATA_ARGS, FIELDNO_NULLABLE_DATUM_ISNULL.
 */
#[inline]
pub unsafe fn l_funcnullp(
    b: LLVMBuilderRef,
    v_fcinfo: LLVMValueRef,
    argno: Size,
) -> LLVMValueRef {
    unimplemented!()
}

/*
 * Return pointer to the argno'th argument datum.
 * Uses FIELDNO_FUNCTIONCALLINFODATA_ARGS, FIELDNO_NULLABLE_DATUM_DATUM.
 */
#[inline]
pub unsafe fn l_funcvaluep(
    b: LLVMBuilderRef,
    v_fcinfo: LLVMValueRef,
    argno: Size,
) -> LLVMValueRef {
    unimplemented!()
}

/*
 * Return argno'th argument nullness.
 *   return l_load(b, TypeStorageBool, l_funcnullp(b, v_fcinfo, argno), "");
 */
#[inline]
pub unsafe fn l_funcnull(
    b: LLVMBuilderRef,
    v_fcinfo: LLVMValueRef,
    argno: Size,
) -> LLVMValueRef {
    unimplemented!()
}

/*
 * Return argno'th argument datum.
 *   return l_load(b, TypeSizeT, l_funcvaluep(b, v_fcinfo, argno), "");
 */
#[inline]
pub unsafe fn l_funcvalue(
    b: LLVMBuilderRef,
    v_fcinfo: LLVMValueRef,
    argno: Size,
) -> LLVMValueRef {
    unimplemented!()
}

// c_char import placed at bottom-adjacent use site for clarity; std::ffi.
use std::ffi::c_char;
