//! jit/llvm/llvmjit_expr.c - JIT compile expressions.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California
//!
//! IDENTIFICATION
//!   src/backend/jit/llvm/llvmjit_expr.c
//!
//! 1:1 translation. The bulk of this file emits LLVM IR through the LLVM-C API
//! (<llvm-c/Core.h>). No Rust LLVM-C binding exists in this tree, so the LLVM-C
//! entry points used below are declared as `extern "C"` prototypes and the
//! PostgreSQL-side helpers (l_*, llvm_*, build_*) come from the sibling jit
//! modules. PG executor/node struct field accessors that have no Rust home yet
//! are provided as TODO(pg-port) stubs near the bottom.

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_variables)]
#![allow(unused_mut)]
#![allow(dead_code)]

use core::ptr::{null, null_mut};
use std::ffi::{c_char, c_int, c_void};

use crate::c::{int16, int32, int64, int8, uint32, Size};
use crate::nodes::pg_list::lengthof_helper as lengthof; // not used; see lengthof! below
use crate::utils::fmgr::FunctionCallInfo;
use crate::utils::palloc::{palloc, palloc0, pfree};

use crate::jit::jit::{
    CheckExprStillValid, ExprContext, ExprState, JitContext, PGJIT_DEFORM,
};
use crate::portability::instr_time::{
    instr_time, INSTR_TIME_ACCUM_DIFF, INSTR_TIME_SET_CURRENT,
};

use crate::jit::llvm::llvmjit::{
    llvm_copy_attributes, llvm_create_context, llvm_enter_fatal_on_oom, llvm_expand_funcname,
    llvm_function_reference, llvm_get_function, llvm_leave_fatal_on_oom, llvm_mutable_module,
    llvm_pg_func, llvm_pg_var_func_type, llvm_pg_var_type, slot_compile_deform, AttributeTemplate,
    ExecEvalBoolSubroutineTemplate, ExecEvalSubroutineTemplate, LLVMJitContext, StructAggState,
    StructAggStatePerGroupData, StructAggStatePerTransData, StructExprContext, StructExprEvalStep,
    StructExprState, StructFunctionCallInfoData, StructMemoryContextData, StructNullableDatum,
    StructTupleTableSlot, TypeParamBool, TypeSizeT, TypeStorageBool,
};

use crate::jit::llvmjit_emit::{
    l_bb_append_v, l_bb_before_v, l_call, l_funcnull, l_funcnullp, l_funcvalue, l_funcvaluep, l_gep,
    l_int16_const, l_int32_const, l_int64_const, l_int8_const, l_load, l_load_gep1,
    l_load_struct_gep, l_mcxt_switch, l_ptr, l_ptr_const, l_sbool_const, l_sizet_const, l_struct_gep,
    LLVMBasicBlockRef, LLVMBuilderRef, LLVMContextRef, LLVMModuleRef, LLVMTypeRef, LLVMValueRef,
};

// ---------------------------------------------------------------------------
// LLVM-C entry points used here. <llvm-c/Core.h> / <llvm-c/Target.h>.
// No Rust LLVM-C binding exists in this tree; declared as opaque externs.
// ---------------------------------------------------------------------------
pub type LLVMIntPredicate = c_int;
pub const LLVMIntEQ: LLVMIntPredicate = 32;
pub const LLVMIntUGE: LLVMIntPredicate = 37;
pub const LLVMIntSLT: LLVMIntPredicate = 40;
pub const LLVMIntSLE: LLVMIntPredicate = 41;
pub const LLVMIntSGT: LLVMIntPredicate = 38;
pub const LLVMIntSGE: LLVMIntPredicate = 39;

pub const LLVMExternalLinkage: c_int = 0;
pub const LLVMDefaultVisibility: c_int = 0;
pub const LLVMCCallConv: c_uint = 0;

use std::ffi::c_uint;

extern "C" {
    pub fn LLVMGetModuleContext(m: LLVMModuleRef) -> LLVMContextRef;
    pub fn LLVMCreateBuilderInContext(c: LLVMContextRef) -> LLVMBuilderRef;
    pub fn LLVMDisposeBuilder(b: LLVMBuilderRef);
    pub fn LLVMAddFunction(m: LLVMModuleRef, name: *const c_char, ty: LLVMTypeRef) -> LLVMValueRef;
    pub fn LLVMSetLinkage(global: LLVMValueRef, linkage: c_int);
    pub fn LLVMSetVisibility(global: LLVMValueRef, viz: c_int);
    pub fn LLVMAppendBasicBlockInContext(
        c: LLVMContextRef,
        f: LLVMValueRef,
        name: *const c_char,
    ) -> LLVMBasicBlockRef;
    pub fn LLVMGetParam(f: LLVMValueRef, index: c_uint) -> LLVMValueRef;
    pub fn LLVMPositionBuilderAtEnd(b: LLVMBuilderRef, block: LLVMBasicBlockRef);
    pub fn LLVMBuildBr(b: LLVMBuilderRef, dest: LLVMBasicBlockRef) -> LLVMValueRef;
    pub fn LLVMBuildCondBr(
        b: LLVMBuilderRef,
        if_: LLVMValueRef,
        then: LLVMBasicBlockRef,
        else_: LLVMBasicBlockRef,
    ) -> LLVMValueRef;
    pub fn LLVMBuildRet(b: LLVMBuilderRef, v: LLVMValueRef) -> LLVMValueRef;
    pub fn LLVMBuildStore(b: LLVMBuilderRef, val: LLVMValueRef, ptr: LLVMValueRef) -> LLVMValueRef;
    pub fn LLVMBuildICmp(
        b: LLVMBuilderRef,
        op: LLVMIntPredicate,
        lhs: LLVMValueRef,
        rhs: LLVMValueRef,
        name: *const c_char,
    ) -> LLVMValueRef;
    pub fn LLVMBuildAnd(
        b: LLVMBuilderRef,
        lhs: LLVMValueRef,
        rhs: LLVMValueRef,
        name: *const c_char,
    ) -> LLVMValueRef;
    pub fn LLVMBuildOr(
        b: LLVMBuilderRef,
        lhs: LLVMValueRef,
        rhs: LLVMValueRef,
        name: *const c_char,
    ) -> LLVMValueRef;
    pub fn LLVMBuildXor(
        b: LLVMBuilderRef,
        lhs: LLVMValueRef,
        rhs: LLVMValueRef,
        name: *const c_char,
    ) -> LLVMValueRef;
    pub fn LLVMBuildShl(
        b: LLVMBuilderRef,
        lhs: LLVMValueRef,
        rhs: LLVMValueRef,
        name: *const c_char,
    ) -> LLVMValueRef;
    pub fn LLVMBuildLShr(
        b: LLVMBuilderRef,
        lhs: LLVMValueRef,
        rhs: LLVMValueRef,
        name: *const c_char,
    ) -> LLVMValueRef;
    pub fn LLVMBuildZExt(
        b: LLVMBuilderRef,
        val: LLVMValueRef,
        dest_ty: LLVMTypeRef,
        name: *const c_char,
    ) -> LLVMValueRef;
    pub fn LLVMBuildTrunc(
        b: LLVMBuilderRef,
        val: LLVMValueRef,
        dest_ty: LLVMTypeRef,
        name: *const c_char,
    ) -> LLVMValueRef;
    pub fn LLVMBuildSelect(
        b: LLVMBuilderRef,
        if_: LLVMValueRef,
        then: LLVMValueRef,
        else_: LLVMValueRef,
        name: *const c_char,
    ) -> LLVMValueRef;
    pub fn LLVMBuildBitCast(
        b: LLVMBuilderRef,
        val: LLVMValueRef,
        dest_ty: LLVMTypeRef,
        name: *const c_char,
    ) -> LLVMValueRef;
    pub fn LLVMBuildPtrToInt(
        b: LLVMBuilderRef,
        val: LLVMValueRef,
        dest_ty: LLVMTypeRef,
        name: *const c_char,
    ) -> LLVMValueRef;
    pub fn LLVMBuildPhi(b: LLVMBuilderRef, ty: LLVMTypeRef, name: *const c_char) -> LLVMValueRef;
    pub fn LLVMAddIncoming(
        phi: LLVMValueRef,
        incoming_values: *mut LLVMValueRef,
        incoming_blocks: *mut LLVMBasicBlockRef,
        count: c_uint,
    );
    pub fn LLVMBuildSwitch(
        b: LLVMBuilderRef,
        v: LLVMValueRef,
        else_: LLVMBasicBlockRef,
        numcases: c_uint,
    ) -> LLVMValueRef;
    pub fn LLVMAddCase(switch: LLVMValueRef, onval: LLVMValueRef, dest: LLVMBasicBlockRef);
    pub fn LLVMBuildUnreachable(b: LLVMBuilderRef) -> LLVMValueRef;
    pub fn LLVMGetFunctionType(f: LLVMValueRef) -> LLVMTypeRef;
    pub fn LLVMCountParams(f: LLVMValueRef) -> c_uint;
    pub fn LLVMInt32TypeInContext(c: LLVMContextRef) -> LLVMTypeRef;
    pub fn LLVMInt8TypeInContext(c: LLVMContextRef) -> LLVMTypeRef;
    pub fn LLVMInt64TypeInContext(c: LLVMContextRef) -> LLVMTypeRef;
    pub fn LLVMVoidTypeInContext(c: LLVMContextRef) -> LLVMTypeRef;
    pub fn LLVMFunctionType(
        ret: LLVMTypeRef,
        param_types: *mut LLVMTypeRef,
        param_count: c_uint,
        is_var_arg: c_int,
    ) -> LLVMTypeRef;
    pub fn LLVMGetNamedFunction(m: LLVMModuleRef, name: *const c_char) -> LLVMValueRef;
    pub fn LLVMSetFunctionCallConv(f: LLVMValueRef, cc: c_uint);
    pub fn LLVMGetIntrinsicID(f: LLVMValueRef) -> c_uint;
}

struct CompiledExprState {
    context: *mut LLVMJitContext,
    funcname: *const c_char,
}

// macro making it easier to call ExecEval* functions
//   build_EvalXFunc(b, mod, funcname, v_state, op, ...)
// -> build_EvalXFuncInt(b, mod, funcname, v_state, op, nargs, &v_args)
macro_rules! build_EvalXFunc {
    ($b:expr, $mod:expr, $funcname:expr, $v_state:expr, $op:expr $(, $arg:expr)* $(,)?) => {{
        let mut v_args: [LLVMValueRef; { 0usize $(+ { let _ = &$arg; 1usize })* }] =
            [$($arg),*];
        let natts = v_args.len() as c_int;
        let _ptr = if v_args.is_empty() {
            null_mut()
        } else {
            v_args.as_mut_ptr()
        };
        build_EvalXFuncInt($b, $mod, $funcname, $v_state, $op, natts, _ptr)
    }};
}

// lengthof for fixed-count arg arrays.
macro_rules! lengthof {
    ($arr:expr) => {
        ($arr).len() as c_int
    };
}
