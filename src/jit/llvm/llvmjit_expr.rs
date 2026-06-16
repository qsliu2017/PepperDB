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
use crate::postgres::{Datum, NullableDatum};
use crate::utils::fmgr::{FunctionCallInfo, FIELDNO_FUNCTIONCALLINFODATA_ISNULL};
use crate::utils::palloc::{palloc, palloc0, pfree};
use crate::utils::elog::ERROR;
use crate::{castNode, elog, Assert};

use crate::access::cmptype::{
    CompareType, COMPARE_GE, COMPARE_GT, COMPARE_LE, COMPARE_LT,
};

use crate::executor::execExpr::{
    ExprEvalOp, ExprEvalOp::*, ExprEvalStep,
};
use crate::executor::execExprInterp::ExecEvalStepOp;
use crate::executor::execTuples::TTSOpsVirtual;
use crate::executor::tuptable::{
    TupleTableSlotOps, FIELDNO_TUPLETABLESLOT_ISNULL, FIELDNO_TUPLETABLESLOT_NVALID,
    FIELDNO_TUPLETABLESLOT_VALUES,
};
use crate::access::common::tupdesc::TupleDesc;

use crate::nodes::execnodes::{
    AggState, AggStatePerTrans, EState, ExprContext, ExprState, ExprStateEvalFunc,
    JsonExprState, PlanState, WindowFuncExprState,
    FIELDNO_AGGSTATE_ALL_PERGROUPS, FIELDNO_AGGSTATE_CURAGGCONTEXT,
    FIELDNO_AGGSTATE_CURPERTRANS, FIELDNO_AGGSTATE_CURRENT_SET,
    FIELDNO_EXPRCONTEXT_AGGNULLS, FIELDNO_EXPRCONTEXT_AGGVALUES,
    FIELDNO_EXPRCONTEXT_CASEDATUM, FIELDNO_EXPRCONTEXT_CASENULL,
    FIELDNO_EXPRCONTEXT_DOMAINDATUM, FIELDNO_EXPRCONTEXT_DOMAINNULL,
    FIELDNO_EXPRCONTEXT_INNERTUPLE, FIELDNO_EXPRCONTEXT_NEWTUPLE,
    FIELDNO_EXPRCONTEXT_OLDTUPLE, FIELDNO_EXPRCONTEXT_OUTERTUPLE,
    FIELDNO_EXPRCONTEXT_SCANTUPLE, FIELDNO_EXPRSTATE_FLAGS, FIELDNO_EXPRSTATE_PARENT,
    FIELDNO_EXPRSTATE_RESNULL, FIELDNO_EXPRSTATE_RESULTSLOT, FIELDNO_EXPRSTATE_RESVALUE,
};
use crate::postgres::FIELDNO_NULLABLE_DATUM_ISNULL;

use crate::jit::jit::{
    CheckExprStillValid, JitContext, PGJIT_DEFORM,
};
use crate::jit::llvm::llvmjit_deform::slot_compile_deform;
use crate::nodes::nodes::T_AggState;
use crate::portability::instr_time::{
    instr_time, INSTR_TIME_ACCUM_DIFF, INSTR_TIME_SET_CURRENT,
};

// nodeAgg.h field accessors for AggStatePerGroupData; not yet ported into the
// shared headers. TODO(pg-port): move to nodes/execnodes.rs alongside the other
// FIELDNO_* constants when AggStatePerGroupData gains its full definition.
const FIELDNO_AGGSTATEPERGROUPDATA_TRANSVALUE: usize = 0;
const FIELDNO_AGGSTATEPERGROUPDATA_TRANSVALUEISNULL: usize = 1;
const FIELDNO_AGGSTATEPERGROUPDATA_NOTRANSVALUE: usize = 2;

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

// ---------------------------------------------------------------------------
// llvm_compile_expr - JIT compile a single expression.
// ---------------------------------------------------------------------------
pub unsafe fn llvm_compile_expr(state: *mut ExprState) -> bool {
    let parent: *mut PlanState = (*state).parent;
    let funcname: *mut c_char;

    let mut context: *mut LLVMJitContext = null_mut();

    let b: LLVMBuilderRef;
    let mod_: LLVMModuleRef;
    let lc: LLVMContextRef;
    let eval_fn: LLVMValueRef;
    let entry: LLVMBasicBlockRef;
    let opblocks: *mut LLVMBasicBlockRef;

    /* state itself */
    let v_state: LLVMValueRef;
    let v_econtext: LLVMValueRef;
    let v_parent: LLVMValueRef;

    /* returnvalue */
    let v_isnullp: LLVMValueRef;

    /* tmp vars in state */
    let v_tmpvaluep: LLVMValueRef;
    let v_tmpisnullp: LLVMValueRef;

    /* slots */
    let v_innerslot: LLVMValueRef;
    let v_outerslot: LLVMValueRef;
    let v_scanslot: LLVMValueRef;
    let v_oldslot: LLVMValueRef;
    let v_newslot: LLVMValueRef;
    let v_resultslot: LLVMValueRef;

    /* nulls/values of slots */
    let v_innervalues: LLVMValueRef;
    let v_innernulls: LLVMValueRef;
    let v_outervalues: LLVMValueRef;
    let v_outernulls: LLVMValueRef;
    let v_scanvalues: LLVMValueRef;
    let v_scannulls: LLVMValueRef;
    let v_oldvalues: LLVMValueRef;
    let v_oldnulls: LLVMValueRef;
    let v_newvalues: LLVMValueRef;
    let v_newnulls: LLVMValueRef;
    let v_resultvalues: LLVMValueRef;
    let v_resultnulls: LLVMValueRef;

    /* stuff in econtext */
    let v_aggvalues: LLVMValueRef;
    let v_aggnulls: LLVMValueRef;

    let mut starttime: instr_time = instr_time::default();
    let mut deform_starttime: instr_time = instr_time::default();
    let mut endtime: instr_time = instr_time::default();
    let mut deform_endtime: instr_time = instr_time::default();

    llvm_enter_fatal_on_oom();

    /*
     * Right now we don't support compiling expressions without a parent, as
     * we need access to the EState.
     */
    Assert!(!parent.is_null());

    /* get or create JIT context */
    if !(*(*parent).state).es_jit.is_null() {
        context = (*(*parent).state).es_jit as *mut LLVMJitContext;
    } else {
        context = llvm_create_context((*(*parent).state).es_jit_flags);
        (*(*parent).state).es_jit = &raw mut (*context).base;
    }

    INSTR_TIME_SET_CURRENT(&mut starttime);

    mod_ = llvm_mutable_module(context);
    lc = LLVMGetModuleContext(mod_);

    b = LLVMCreateBuilderInContext(lc);

    funcname = llvm_expand_funcname(context, c"evalexpr".as_ptr());

    /* create function */
    eval_fn = LLVMAddFunction(
        mod_,
        funcname,
        llvm_pg_var_func_type(c"ExecInterpExprStillValid".as_ptr()),
    );
    LLVMSetLinkage(eval_fn, LLVMExternalLinkage);
    LLVMSetVisibility(eval_fn, LLVMDefaultVisibility);
    llvm_copy_attributes(AttributeTemplate, eval_fn);

    entry = LLVMAppendBasicBlockInContext(lc, eval_fn, c"entry".as_ptr());

    /* build state */
    v_state = LLVMGetParam(eval_fn, 0);
    v_econtext = LLVMGetParam(eval_fn, 1);
    v_isnullp = LLVMGetParam(eval_fn, 2);

    LLVMPositionBuilderAtEnd(b, entry);

    v_tmpvaluep = l_struct_gep(
        b,
        StructExprState,
        v_state,
        FIELDNO_EXPRSTATE_RESVALUE as i32,
        c"v.state.resvalue".as_ptr(),
    );
    v_tmpisnullp = l_struct_gep(
        b,
        StructExprState,
        v_state,
        FIELDNO_EXPRSTATE_RESNULL as i32,
        c"v.state.resnull".as_ptr(),
    );
    v_parent = l_load_struct_gep(
        b,
        StructExprState,
        v_state,
        FIELDNO_EXPRSTATE_PARENT as i32,
        c"v.state.parent".as_ptr(),
    );

    /* build global slots */
    v_scanslot = l_load_struct_gep(
        b,
        StructExprContext,
        v_econtext,
        FIELDNO_EXPRCONTEXT_SCANTUPLE as i32,
        c"v_scanslot".as_ptr(),
    );
    v_innerslot = l_load_struct_gep(
        b,
        StructExprContext,
        v_econtext,
        FIELDNO_EXPRCONTEXT_INNERTUPLE as i32,
        c"v_innerslot".as_ptr(),
    );
    v_outerslot = l_load_struct_gep(
        b,
        StructExprContext,
        v_econtext,
        FIELDNO_EXPRCONTEXT_OUTERTUPLE as i32,
        c"v_outerslot".as_ptr(),
    );
    v_oldslot = l_load_struct_gep(
        b,
        StructExprContext,
        v_econtext,
        FIELDNO_EXPRCONTEXT_OLDTUPLE as i32,
        c"v_oldslot".as_ptr(),
    );
    v_newslot = l_load_struct_gep(
        b,
        StructExprContext,
        v_econtext,
        FIELDNO_EXPRCONTEXT_NEWTUPLE as i32,
        c"v_newslot".as_ptr(),
    );
    v_resultslot = l_load_struct_gep(
        b,
        StructExprState,
        v_state,
        FIELDNO_EXPRSTATE_RESULTSLOT as i32,
        c"v_resultslot".as_ptr(),
    );

    /* build global values/isnull pointers */
    v_scanvalues = l_load_struct_gep(
        b,
        StructTupleTableSlot,
        v_scanslot,
        FIELDNO_TUPLETABLESLOT_VALUES as i32,
        c"v_scanvalues".as_ptr(),
    );
    v_scannulls = l_load_struct_gep(
        b,
        StructTupleTableSlot,
        v_scanslot,
        FIELDNO_TUPLETABLESLOT_ISNULL as i32,
        c"v_scannulls".as_ptr(),
    );
    v_innervalues = l_load_struct_gep(
        b,
        StructTupleTableSlot,
        v_innerslot,
        FIELDNO_TUPLETABLESLOT_VALUES as i32,
        c"v_innervalues".as_ptr(),
    );
    v_innernulls = l_load_struct_gep(
        b,
        StructTupleTableSlot,
        v_innerslot,
        FIELDNO_TUPLETABLESLOT_ISNULL as i32,
        c"v_innernulls".as_ptr(),
    );
    v_outervalues = l_load_struct_gep(
        b,
        StructTupleTableSlot,
        v_outerslot,
        FIELDNO_TUPLETABLESLOT_VALUES as i32,
        c"v_outervalues".as_ptr(),
    );
    v_outernulls = l_load_struct_gep(
        b,
        StructTupleTableSlot,
        v_outerslot,
        FIELDNO_TUPLETABLESLOT_ISNULL as i32,
        c"v_outernulls".as_ptr(),
    );
    v_oldvalues = l_load_struct_gep(
        b,
        StructTupleTableSlot,
        v_oldslot,
        FIELDNO_TUPLETABLESLOT_VALUES as i32,
        c"v_oldvalues".as_ptr(),
    );
    v_oldnulls = l_load_struct_gep(
        b,
        StructTupleTableSlot,
        v_oldslot,
        FIELDNO_TUPLETABLESLOT_ISNULL as i32,
        c"v_oldnulls".as_ptr(),
    );
    v_newvalues = l_load_struct_gep(
        b,
        StructTupleTableSlot,
        v_newslot,
        FIELDNO_TUPLETABLESLOT_VALUES as i32,
        c"v_newvalues".as_ptr(),
    );
    v_newnulls = l_load_struct_gep(
        b,
        StructTupleTableSlot,
        v_newslot,
        FIELDNO_TUPLETABLESLOT_ISNULL as i32,
        c"v_newnulls".as_ptr(),
    );
    v_resultvalues = l_load_struct_gep(
        b,
        StructTupleTableSlot,
        v_resultslot,
        FIELDNO_TUPLETABLESLOT_VALUES as i32,
        c"v_resultvalues".as_ptr(),
    );
    v_resultnulls = l_load_struct_gep(
        b,
        StructTupleTableSlot,
        v_resultslot,
        FIELDNO_TUPLETABLESLOT_ISNULL as i32,
        c"v_resultnulls".as_ptr(),
    );

    /* aggvalues/aggnulls */
    v_aggvalues = l_load_struct_gep(
        b,
        StructExprContext,
        v_econtext,
        FIELDNO_EXPRCONTEXT_AGGVALUES as i32,
        c"v.econtext.aggvalues".as_ptr(),
    );
    v_aggnulls = l_load_struct_gep(
        b,
        StructExprContext,
        v_econtext,
        FIELDNO_EXPRCONTEXT_AGGNULLS as i32,
        c"v.econtext.aggnulls".as_ptr(),
    );

    /* allocate blocks for each op upfront, so we can do jumps easily */
    opblocks = palloc(core::mem::size_of::<LLVMBasicBlockRef>() * (*state).steps_len as usize)
        as *mut LLVMBasicBlockRef;
    for opno in 0..(*state).steps_len {
        // C: l_bb_append_v(eval_fn, "b.op.%d.start", opno)
        *opblocks.add(opno as usize) = l_bb_append_v(eval_fn, c"b.op.%d.start".as_ptr());
    }

    /* jump from entry to first block */
    LLVMBuildBr(b, *opblocks.add(0));

    for opno in 0..(*state).steps_len {
        let op: *mut ExprEvalStep;
        let opcode: ExprEvalOp;
        let v_resvaluep: LLVMValueRef;
        let v_resnullp: LLVMValueRef;

        LLVMPositionBuilderAtEnd(b, *opblocks.add(opno as usize));

        op = (*state).steps.add(opno as usize);
        opcode = ExecEvalStepOp(state, op);

        v_resvaluep = l_ptr_const((*op).resvalue as *mut c_void, l_ptr(TypeSizeT));
        v_resnullp = l_ptr_const((*op).resnull as *mut c_void, l_ptr(TypeStorageBool));

        match opcode {
            EEOP_DONE_RETURN => {
                let v_tmpisnull: LLVMValueRef;
                let v_tmpvalue: LLVMValueRef;

                v_tmpvalue = l_load(b, TypeSizeT, v_tmpvaluep, c"".as_ptr());
                v_tmpisnull = l_load(b, TypeStorageBool, v_tmpisnullp, c"".as_ptr());

                LLVMBuildStore(b, v_tmpisnull, v_isnullp);

                LLVMBuildRet(b, v_tmpvalue);
            }

            EEOP_DONE_NO_RETURN => {
                LLVMBuildRet(b, l_sizet_const(0));
            }

            EEOP_INNER_FETCHSOME | EEOP_OUTER_FETCHSOME | EEOP_SCAN_FETCHSOME
            | EEOP_OLD_FETCHSOME | EEOP_NEW_FETCHSOME => {
                let mut desc: TupleDesc = null_mut();
                let v_slot: LLVMValueRef;
                let b_fetch: LLVMBasicBlockRef;
                let v_nvalid: LLVMValueRef;
                let mut l_jit_deform: LLVMValueRef = null_mut();
                let mut tts_ops: *const TupleTableSlotOps = null();

                b_fetch = l_bb_before_v(*opblocks.add((opno + 1) as usize), c"op.%d.fetch".as_ptr());

                if !(*op).d.fetch.known_desc.is_null() {
                    desc = (*op).d.fetch.known_desc;
                }

                if (*op).d.fetch.fixed {
                    tts_ops = (*op).d.fetch.kind;
                }

                /* step should not have been generated */
                Assert!(tts_ops != &raw const TTSOpsVirtual);

                if opcode == EEOP_INNER_FETCHSOME {
                    v_slot = v_innerslot;
                } else if opcode == EEOP_OUTER_FETCHSOME {
                    v_slot = v_outerslot;
                } else if opcode == EEOP_SCAN_FETCHSOME {
                    v_slot = v_scanslot;
                } else if opcode == EEOP_OLD_FETCHSOME {
                    v_slot = v_oldslot;
                } else {
                    v_slot = v_newslot;
                }

                /*
                 * Check if all required attributes are available, or
                 * whether deforming is required.
                 */
                v_nvalid = l_load_struct_gep(
                    b,
                    StructTupleTableSlot,
                    v_slot,
                    FIELDNO_TUPLETABLESLOT_NVALID as i32,
                    c"".as_ptr(),
                );
                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(
                        b,
                        LLVMIntUGE,
                        v_nvalid,
                        l_int16_const(lc, (*op).d.fetch.last_var as int16),
                        c"".as_ptr(),
                    ),
                    *opblocks.add((opno + 1) as usize),
                    b_fetch,
                );

                LLVMPositionBuilderAtEnd(b, b_fetch);

                /*
                 * If the tupledesc of the to-be-deformed tuple is known,
                 * and JITing of deforming is enabled, build deform
                 * function specific to tupledesc and the exact number of
                 * to-be-extracted attributes.
                 */
                if !tts_ops.is_null()
                    && !desc.is_null()
                    && ((*context).base.flags & PGJIT_DEFORM) != 0
                {
                    INSTR_TIME_SET_CURRENT(&mut deform_starttime);
                    l_jit_deform =
                        slot_compile_deform(context, desc, tts_ops, (*op).d.fetch.last_var);
                    INSTR_TIME_SET_CURRENT(&mut deform_endtime);
                    INSTR_TIME_ACCUM_DIFF(
                        &mut (*context).base.instr.deform_counter,
                        deform_endtime,
                        deform_starttime,
                    );
                }

                if !l_jit_deform.is_null() {
                    let mut params: [LLVMValueRef; 1] = [null_mut(); 1];

                    params[0] = v_slot;

                    l_call(
                        b,
                        LLVMGetFunctionType(l_jit_deform),
                        l_jit_deform,
                        params.as_mut_ptr(),
                        lengthof!(params),
                        c"".as_ptr(),
                    );
                } else {
                    let mut params: [LLVMValueRef; 2] = [null_mut(); 2];

                    params[0] = v_slot;
                    params[1] = l_int32_const(lc, (*op).d.fetch.last_var);

                    l_call(
                        b,
                        llvm_pg_var_func_type(c"slot_getsomeattrs_int".as_ptr()),
                        llvm_pg_func(mod_, c"slot_getsomeattrs_int".as_ptr()),
                        params.as_mut_ptr(),
                        lengthof!(params),
                        c"".as_ptr(),
                    );
                }

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_INNER_VAR | EEOP_OUTER_VAR | EEOP_SCAN_VAR | EEOP_OLD_VAR | EEOP_NEW_VAR => {
                let value: LLVMValueRef;
                let isnull: LLVMValueRef;
                let v_attnum: LLVMValueRef;
                let v_values: LLVMValueRef;
                let v_nulls: LLVMValueRef;

                if opcode == EEOP_INNER_VAR {
                    v_values = v_innervalues;
                    v_nulls = v_innernulls;
                } else if opcode == EEOP_OUTER_VAR {
                    v_values = v_outervalues;
                    v_nulls = v_outernulls;
                } else if opcode == EEOP_SCAN_VAR {
                    v_values = v_scanvalues;
                    v_nulls = v_scannulls;
                } else if opcode == EEOP_OLD_VAR {
                    v_values = v_oldvalues;
                    v_nulls = v_oldnulls;
                } else {
                    v_values = v_newvalues;
                    v_nulls = v_newnulls;
                }

                v_attnum = l_int32_const(lc, (*op).d.var.attnum);
                value = l_load_gep1(b, TypeSizeT, v_values, v_attnum, c"".as_ptr());
                isnull = l_load_gep1(b, TypeStorageBool, v_nulls, v_attnum, c"".as_ptr());
                LLVMBuildStore(b, value, v_resvaluep);
                LLVMBuildStore(b, isnull, v_resnullp);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_INNER_SYSVAR | EEOP_OUTER_SYSVAR | EEOP_SCAN_SYSVAR | EEOP_OLD_SYSVAR
            | EEOP_NEW_SYSVAR => {
                let v_slot: LLVMValueRef;

                if opcode == EEOP_INNER_SYSVAR {
                    v_slot = v_innerslot;
                } else if opcode == EEOP_OUTER_SYSVAR {
                    v_slot = v_outerslot;
                } else if opcode == EEOP_SCAN_SYSVAR {
                    v_slot = v_scanslot;
                } else if opcode == EEOP_OLD_SYSVAR {
                    v_slot = v_oldslot;
                } else {
                    v_slot = v_newslot;
                }

                build_EvalXFunc!(
                    b,
                    mod_,
                    c"ExecEvalSysVar".as_ptr(),
                    v_state,
                    op,
                    v_econtext,
                    v_slot
                );

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_WHOLEROW => {
                build_EvalXFunc!(b, mod_, c"ExecEvalWholeRowVar".as_ptr(), v_state, op, v_econtext);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_ASSIGN_INNER_VAR | EEOP_ASSIGN_OUTER_VAR | EEOP_ASSIGN_SCAN_VAR
            | EEOP_ASSIGN_OLD_VAR | EEOP_ASSIGN_NEW_VAR => {
                let v_value: LLVMValueRef;
                let v_isnull: LLVMValueRef;
                let v_rvaluep: LLVMValueRef;
                let v_risnullp: LLVMValueRef;
                let v_attnum: LLVMValueRef;
                let mut v_resultnum: LLVMValueRef;
                let v_values: LLVMValueRef;
                let v_nulls: LLVMValueRef;

                if opcode == EEOP_ASSIGN_INNER_VAR {
                    v_values = v_innervalues;
                    v_nulls = v_innernulls;
                } else if opcode == EEOP_ASSIGN_OUTER_VAR {
                    v_values = v_outervalues;
                    v_nulls = v_outernulls;
                } else if opcode == EEOP_ASSIGN_SCAN_VAR {
                    v_values = v_scanvalues;
                    v_nulls = v_scannulls;
                } else if opcode == EEOP_ASSIGN_OLD_VAR {
                    v_values = v_oldvalues;
                    v_nulls = v_oldnulls;
                } else {
                    v_values = v_newvalues;
                    v_nulls = v_newnulls;
                }

                /* load data */
                v_attnum = l_int32_const(lc, (*op).d.assign_var.attnum);
                v_value = l_load_gep1(b, TypeSizeT, v_values, v_attnum, c"".as_ptr());
                v_isnull = l_load_gep1(b, TypeStorageBool, v_nulls, v_attnum, c"".as_ptr());

                /* compute addresses of targets */
                v_resultnum = l_int32_const(lc, (*op).d.assign_var.resultnum);
                v_rvaluep = l_gep(b, TypeSizeT, v_resultvalues, &mut v_resultnum, 1, c"".as_ptr());
                v_risnullp =
                    l_gep(b, TypeStorageBool, v_resultnulls, &mut v_resultnum, 1, c"".as_ptr());

                /* and store */
                LLVMBuildStore(b, v_value, v_rvaluep);
                LLVMBuildStore(b, v_isnull, v_risnullp);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_ASSIGN_TMP | EEOP_ASSIGN_TMP_MAKE_RO => {
                let mut v_value: LLVMValueRef;
                let v_isnull: LLVMValueRef;
                let v_rvaluep: LLVMValueRef;
                let v_risnullp: LLVMValueRef;
                let mut v_resultnum: LLVMValueRef;
                let resultnum: usize = (*op).d.assign_tmp.resultnum as usize;

                /* load data */
                v_value = l_load(b, TypeSizeT, v_tmpvaluep, c"".as_ptr());
                v_isnull = l_load(b, TypeStorageBool, v_tmpisnullp, c"".as_ptr());

                /* compute addresses of targets */
                v_resultnum = l_int32_const(lc, resultnum as int32);
                v_rvaluep = l_gep(b, TypeSizeT, v_resultvalues, &mut v_resultnum, 1, c"".as_ptr());
                v_risnullp =
                    l_gep(b, TypeStorageBool, v_resultnulls, &mut v_resultnum, 1, c"".as_ptr());

                /* store nullness */
                LLVMBuildStore(b, v_isnull, v_risnullp);

                /* make value readonly if necessary */
                if opcode == EEOP_ASSIGN_TMP_MAKE_RO {
                    let b_notnull: LLVMBasicBlockRef;
                    let mut v_params: [LLVMValueRef; 1] = [null_mut(); 1];

                    b_notnull = l_bb_before_v(
                        *opblocks.add((opno + 1) as usize),
                        c"op.%d.assign_tmp.notnull".as_ptr(),
                    );

                    /* check if value is NULL */
                    LLVMBuildCondBr(
                        b,
                        LLVMBuildICmp(b, LLVMIntEQ, v_isnull, l_sbool_const(false), c"".as_ptr()),
                        b_notnull,
                        *opblocks.add((opno + 1) as usize),
                    );

                    /* if value is not null, convert to RO datum */
                    LLVMPositionBuilderAtEnd(b, b_notnull);
                    v_params[0] = v_value;
                    v_value = l_call(
                        b,
                        llvm_pg_var_func_type(c"MakeExpandedObjectReadOnlyInternal".as_ptr()),
                        llvm_pg_func(mod_, c"MakeExpandedObjectReadOnlyInternal".as_ptr()),
                        v_params.as_mut_ptr(),
                        lengthof!(v_params),
                        c"".as_ptr(),
                    );

                    /*
                     * Falling out of the if () with builder in b_notnull,
                     * which is fine - the null is already stored above.
                     */
                }

                /* and finally store result */
                LLVMBuildStore(b, v_value, v_rvaluep);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_CONST => {
                let v_constvalue: LLVMValueRef;
                let v_constnull: LLVMValueRef;

                v_constvalue = l_sizet_const((*op).d.constval.value as Size);
                v_constnull = l_sbool_const((*op).d.constval.isnull);

                LLVMBuildStore(b, v_constvalue, v_resvaluep);
                LLVMBuildStore(b, v_constnull, v_resnullp);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_FUNCEXPR | EEOP_FUNCEXPR_STRICT | EEOP_FUNCEXPR_STRICT_1
            | EEOP_FUNCEXPR_STRICT_2 => {
                let fcinfo: FunctionCallInfo = (*op).d.func.fcinfo_data;
                let mut v_fcinfo_isnull: LLVMValueRef = null_mut();
                let v_retval: LLVMValueRef;

                if opcode == EEOP_FUNCEXPR_STRICT
                    || opcode == EEOP_FUNCEXPR_STRICT_1
                    || opcode == EEOP_FUNCEXPR_STRICT_2
                {
                    let b_nonull: LLVMBasicBlockRef;
                    let b_checkargnulls: *mut LLVMBasicBlockRef;
                    let v_fcinfo: LLVMValueRef;

                    /*
                     * Block for the actual function call, if args are
                     * non-NULL.
                     */
                    b_nonull = l_bb_before_v(
                        *opblocks.add((opno + 1) as usize),
                        c"b.%d.no-null-args".as_ptr(),
                    );

                    /* should make sure they're optimized beforehand */
                    if (*op).d.func.nargs == 0 {
                        elog!(ERROR, "argumentless strict functions are pointless");
                    }

                    v_fcinfo =
                        l_ptr_const(fcinfo as *mut c_void, l_ptr(StructFunctionCallInfoData));

                    /*
                     * set resnull to true, if the function is actually
                     * called, it'll be reset
                     */
                    LLVMBuildStore(b, l_sbool_const(true), v_resnullp);

                    /* create blocks for checking args, one for each */
                    b_checkargnulls = palloc(
                        core::mem::size_of::<LLVMBasicBlockRef>()
                            * (*op).d.func.nargs as usize,
                    ) as *mut LLVMBasicBlockRef;
                    for argno in 0..(*op).d.func.nargs {
                        *b_checkargnulls.add(argno as usize) =
                            l_bb_before_v(b_nonull, c"b.%d.isnull.%d".as_ptr());
                    }

                    /* jump to check of first argument */
                    LLVMBuildBr(b, *b_checkargnulls.add(0));

                    /* check each arg for NULLness */
                    for argno in 0..(*op).d.func.nargs {
                        let v_argisnull: LLVMValueRef;
                        let b_argnotnull: LLVMBasicBlockRef;

                        LLVMPositionBuilderAtEnd(b, *b_checkargnulls.add(argno as usize));

                        /*
                         * Compute block to jump to if argument is not
                         * null.
                         */
                        if argno + 1 == (*op).d.func.nargs {
                            b_argnotnull = b_nonull;
                        } else {
                            b_argnotnull = *b_checkargnulls.add((argno + 1) as usize);
                        }

                        /* and finally load & check NULLness of arg */
                        v_argisnull = l_funcnull(b, v_fcinfo, argno as Size);
                        LLVMBuildCondBr(
                            b,
                            LLVMBuildICmp(
                                b,
                                LLVMIntEQ,
                                v_argisnull,
                                l_sbool_const(true),
                                c"".as_ptr(),
                            ),
                            *opblocks.add((opno + 1) as usize),
                            b_argnotnull,
                        );
                    }

                    LLVMPositionBuilderAtEnd(b, b_nonull);
                }

                v_retval = BuildV1Call(context, b, mod_, fcinfo, &mut v_fcinfo_isnull);
                LLVMBuildStore(b, v_retval, v_resvaluep);
                LLVMBuildStore(b, v_fcinfo_isnull, v_resnullp);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_FUNCEXPR_FUSAGE => {
                build_EvalXFunc!(
                    b,
                    mod_,
                    c"ExecEvalFuncExprFusage".as_ptr(),
                    v_state,
                    op,
                    v_econtext
                );
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_FUNCEXPR_STRICT_FUSAGE => {
                build_EvalXFunc!(
                    b,
                    mod_,
                    c"ExecEvalFuncExprStrictFusage".as_ptr(),
                    v_state,
                    op,
                    v_econtext
                );
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            /*
             * Treat them the same for now, optimizer can remove
             * redundancy. Could be worthwhile to optimize during emission
             * though.
             */
            EEOP_BOOL_AND_STEP_FIRST | EEOP_BOOL_AND_STEP | EEOP_BOOL_AND_STEP_LAST => {
                let v_boolvalue: LLVMValueRef;
                let v_boolnull: LLVMValueRef;
                let v_boolanynullp: LLVMValueRef;
                let v_boolanynull: LLVMValueRef;
                let b_boolisnull: LLVMBasicBlockRef;
                let b_boolcheckfalse: LLVMBasicBlockRef;
                let b_boolisfalse: LLVMBasicBlockRef;
                let b_boolcont: LLVMBasicBlockRef;
                let b_boolisanynull: LLVMBasicBlockRef;

                b_boolisnull =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"b.%d.boolisnull".as_ptr());
                b_boolcheckfalse = l_bb_before_v(
                    *opblocks.add((opno + 1) as usize),
                    c"b.%d.boolcheckfalse".as_ptr(),
                );
                b_boolisfalse =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"b.%d.boolisfalse".as_ptr());
                b_boolisanynull = l_bb_before_v(
                    *opblocks.add((opno + 1) as usize),
                    c"b.%d.boolisanynull".as_ptr(),
                );
                b_boolcont =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"b.%d.boolcont".as_ptr());

                v_boolanynullp =
                    l_ptr_const((*op).d.boolexpr.anynull as *mut c_void, l_ptr(TypeStorageBool));

                if opcode == EEOP_BOOL_AND_STEP_FIRST {
                    LLVMBuildStore(b, l_sbool_const(false), v_boolanynullp);
                }

                v_boolnull = l_load(b, TypeStorageBool, v_resnullp, c"".as_ptr());
                v_boolvalue = l_load(b, TypeSizeT, v_resvaluep, c"".as_ptr());

                /* check if current input is NULL */
                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_boolnull, l_sbool_const(true), c"".as_ptr()),
                    b_boolisnull,
                    b_boolcheckfalse,
                );

                /* build block that sets anynull */
                LLVMPositionBuilderAtEnd(b, b_boolisnull);
                /* set boolanynull to true */
                LLVMBuildStore(b, l_sbool_const(true), v_boolanynullp);
                /* and jump to next block */
                LLVMBuildBr(b, b_boolcont);

                /* build block checking for false */
                LLVMPositionBuilderAtEnd(b, b_boolcheckfalse);
                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_boolvalue, l_sizet_const(0), c"".as_ptr()),
                    b_boolisfalse,
                    b_boolcont,
                );

                /*
                 * Build block handling FALSE. Value is false, so short
                 * circuit.
                 */
                LLVMPositionBuilderAtEnd(b, b_boolisfalse);
                /* result is already set to FALSE, need not change it */
                /* and jump to the end of the AND expression */
                LLVMBuildBr(b, *opblocks.add((*op).d.boolexpr.jumpdone as usize));

                /* Build block that continues if bool is TRUE. */
                LLVMPositionBuilderAtEnd(b, b_boolcont);

                v_boolanynull = l_load(b, TypeStorageBool, v_boolanynullp, c"".as_ptr());

                /* set value to NULL if any previous values were NULL */
                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_boolanynull, l_sbool_const(false), c"".as_ptr()),
                    *opblocks.add((opno + 1) as usize),
                    b_boolisanynull,
                );

                LLVMPositionBuilderAtEnd(b, b_boolisanynull);
                /* set resnull to true */
                LLVMBuildStore(b, l_sbool_const(true), v_resnullp);
                /* reset resvalue */
                LLVMBuildStore(b, l_sizet_const(0), v_resvaluep);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            /*
             * Treat them the same for now, optimizer can remove
             * redundancy. Could be worthwhile to optimize during emission
             * though.
             */
            EEOP_BOOL_OR_STEP_FIRST | EEOP_BOOL_OR_STEP | EEOP_BOOL_OR_STEP_LAST => {
                let v_boolvalue: LLVMValueRef;
                let v_boolnull: LLVMValueRef;
                let v_boolanynullp: LLVMValueRef;
                let v_boolanynull: LLVMValueRef;

                let b_boolisnull: LLVMBasicBlockRef;
                let b_boolchecktrue: LLVMBasicBlockRef;
                let b_boolistrue: LLVMBasicBlockRef;
                let b_boolcont: LLVMBasicBlockRef;
                let b_boolisanynull: LLVMBasicBlockRef;

                b_boolisnull =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"b.%d.boolisnull".as_ptr());
                b_boolchecktrue = l_bb_before_v(
                    *opblocks.add((opno + 1) as usize),
                    c"b.%d.boolchecktrue".as_ptr(),
                );
                b_boolistrue =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"b.%d.boolistrue".as_ptr());
                b_boolisanynull = l_bb_before_v(
                    *opblocks.add((opno + 1) as usize),
                    c"b.%d.boolisanynull".as_ptr(),
                );
                b_boolcont =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"b.%d.boolcont".as_ptr());

                v_boolanynullp =
                    l_ptr_const((*op).d.boolexpr.anynull as *mut c_void, l_ptr(TypeStorageBool));

                if opcode == EEOP_BOOL_OR_STEP_FIRST {
                    LLVMBuildStore(b, l_sbool_const(false), v_boolanynullp);
                }
                v_boolnull = l_load(b, TypeStorageBool, v_resnullp, c"".as_ptr());
                v_boolvalue = l_load(b, TypeSizeT, v_resvaluep, c"".as_ptr());

                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_boolnull, l_sbool_const(true), c"".as_ptr()),
                    b_boolisnull,
                    b_boolchecktrue,
                );

                /* build block that sets anynull */
                LLVMPositionBuilderAtEnd(b, b_boolisnull);
                /* set boolanynull to true */
                LLVMBuildStore(b, l_sbool_const(true), v_boolanynullp);
                /* and jump to next block */
                LLVMBuildBr(b, b_boolcont);

                /* build block checking for true */
                LLVMPositionBuilderAtEnd(b, b_boolchecktrue);
                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_boolvalue, l_sizet_const(1), c"".as_ptr()),
                    b_boolistrue,
                    b_boolcont,
                );

                /*
                 * Build block handling True. Value is true, so short
                 * circuit.
                 */
                LLVMPositionBuilderAtEnd(b, b_boolistrue);
                /* result is already set to TRUE, need not change it */
                /* and jump to the end of the OR expression */
                LLVMBuildBr(b, *opblocks.add((*op).d.boolexpr.jumpdone as usize));

                /* build block that continues if bool is FALSE */
                LLVMPositionBuilderAtEnd(b, b_boolcont);

                v_boolanynull = l_load(b, TypeStorageBool, v_boolanynullp, c"".as_ptr());

                /* set value to NULL if any previous values were NULL */
                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_boolanynull, l_sbool_const(false), c"".as_ptr()),
                    *opblocks.add((opno + 1) as usize),
                    b_boolisanynull,
                );

                LLVMPositionBuilderAtEnd(b, b_boolisanynull);
                /* set resnull to true */
                LLVMBuildStore(b, l_sbool_const(true), v_resnullp);
                /* reset resvalue */
                LLVMBuildStore(b, l_sizet_const(0), v_resvaluep);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_BOOL_NOT_STEP => {
                let v_boolvalue: LLVMValueRef;
                let v_negbool: LLVMValueRef;

                /* compute !boolvalue */
                v_boolvalue = l_load(b, TypeSizeT, v_resvaluep, c"".as_ptr());
                v_negbool = LLVMBuildZExt(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_boolvalue, l_sizet_const(0), c"".as_ptr()),
                    TypeSizeT,
                    c"".as_ptr(),
                );

                /*
                 * Store it back in resvalue.  We can ignore resnull here;
                 * if it was true, it stays true, and the value we store
                 * in resvalue doesn't matter.
                 */
                LLVMBuildStore(b, v_negbool, v_resvaluep);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_QUAL => {
                let v_resnull: LLVMValueRef;
                let v_resvalue: LLVMValueRef;
                let v_nullorfalse: LLVMValueRef;
                let b_qualfail: LLVMBasicBlockRef;

                b_qualfail =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"op.%d.qualfail".as_ptr());

                v_resvalue = l_load(b, TypeSizeT, v_resvaluep, c"".as_ptr());
                v_resnull = l_load(b, TypeStorageBool, v_resnullp, c"".as_ptr());

                v_nullorfalse = LLVMBuildOr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_resnull, l_sbool_const(true), c"".as_ptr()),
                    LLVMBuildICmp(b, LLVMIntEQ, v_resvalue, l_sizet_const(0), c"".as_ptr()),
                    c"".as_ptr(),
                );

                LLVMBuildCondBr(b, v_nullorfalse, b_qualfail, *opblocks.add((opno + 1) as usize));

                /* build block handling NULL or false */
                LLVMPositionBuilderAtEnd(b, b_qualfail);
                /* set resnull to false */
                LLVMBuildStore(b, l_sbool_const(false), v_resnullp);
                /* set resvalue to false */
                LLVMBuildStore(b, l_sizet_const(0), v_resvaluep);
                /* and jump out */
                LLVMBuildBr(b, *opblocks.add((*op).d.qualexpr.jumpdone as usize));
            }

            EEOP_JUMP => {
                LLVMBuildBr(b, *opblocks.add((*op).d.jump.jumpdone as usize));
            }

            EEOP_JUMP_IF_NULL => {
                let v_resnull: LLVMValueRef;

                /* Transfer control if current result is null */

                v_resnull = l_load(b, TypeStorageBool, v_resnullp, c"".as_ptr());

                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_resnull, l_sbool_const(true), c"".as_ptr()),
                    *opblocks.add((*op).d.jump.jumpdone as usize),
                    *opblocks.add((opno + 1) as usize),
                );
            }

            EEOP_JUMP_IF_NOT_NULL => {
                let v_resnull: LLVMValueRef;

                /* Transfer control if current result is non-null */

                v_resnull = l_load(b, TypeStorageBool, v_resnullp, c"".as_ptr());

                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_resnull, l_sbool_const(false), c"".as_ptr()),
                    *opblocks.add((*op).d.jump.jumpdone as usize),
                    *opblocks.add((opno + 1) as usize),
                );
            }

            EEOP_JUMP_IF_NOT_TRUE => {
                let v_resnull: LLVMValueRef;
                let v_resvalue: LLVMValueRef;
                let v_nullorfalse: LLVMValueRef;

                /* Transfer control if current result is null or false */

                v_resvalue = l_load(b, TypeSizeT, v_resvaluep, c"".as_ptr());
                v_resnull = l_load(b, TypeStorageBool, v_resnullp, c"".as_ptr());

                v_nullorfalse = LLVMBuildOr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_resnull, l_sbool_const(true), c"".as_ptr()),
                    LLVMBuildICmp(b, LLVMIntEQ, v_resvalue, l_sizet_const(0), c"".as_ptr()),
                    c"".as_ptr(),
                );

                LLVMBuildCondBr(
                    b,
                    v_nullorfalse,
                    *opblocks.add((*op).d.jump.jumpdone as usize),
                    *opblocks.add((opno + 1) as usize),
                );
            }

            EEOP_NULLTEST_ISNULL => {
                let v_resnull: LLVMValueRef = l_load(b, TypeStorageBool, v_resnullp, c"".as_ptr());
                let v_resvalue: LLVMValueRef;

                v_resvalue = LLVMBuildSelect(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_resnull, l_sbool_const(true), c"".as_ptr()),
                    l_sizet_const(1),
                    l_sizet_const(0),
                    c"".as_ptr(),
                );
                LLVMBuildStore(b, v_resvalue, v_resvaluep);
                LLVMBuildStore(b, l_sbool_const(false), v_resnullp);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_NULLTEST_ISNOTNULL => {
                let v_resnull: LLVMValueRef = l_load(b, TypeStorageBool, v_resnullp, c"".as_ptr());
                let v_resvalue: LLVMValueRef;

                v_resvalue = LLVMBuildSelect(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_resnull, l_sbool_const(true), c"".as_ptr()),
                    l_sizet_const(0),
                    l_sizet_const(1),
                    c"".as_ptr(),
                );
                LLVMBuildStore(b, v_resvalue, v_resvaluep);
                LLVMBuildStore(b, l_sbool_const(false), v_resnullp);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_NULLTEST_ROWISNULL => {
                build_EvalXFunc!(b, mod_, c"ExecEvalRowNull".as_ptr(), v_state, op, v_econtext);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_NULLTEST_ROWISNOTNULL => {
                build_EvalXFunc!(b, mod_, c"ExecEvalRowNotNull".as_ptr(), v_state, op, v_econtext);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_BOOLTEST_IS_TRUE | EEOP_BOOLTEST_IS_NOT_FALSE | EEOP_BOOLTEST_IS_FALSE
            | EEOP_BOOLTEST_IS_NOT_TRUE => {
                let b_isnull: LLVMBasicBlockRef;
                let b_notnull: LLVMBasicBlockRef;
                let v_resnull: LLVMValueRef = l_load(b, TypeStorageBool, v_resnullp, c"".as_ptr());

                b_isnull =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"op.%d.isnull".as_ptr());
                b_notnull =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"op.%d.isnotnull".as_ptr());

                /* check if value is NULL */
                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_resnull, l_sbool_const(true), c"".as_ptr()),
                    b_isnull,
                    b_notnull,
                );

                /* if value is NULL, return false */
                LLVMPositionBuilderAtEnd(b, b_isnull);

                /* result is not null */
                LLVMBuildStore(b, l_sbool_const(false), v_resnullp);

                if opcode == EEOP_BOOLTEST_IS_TRUE || opcode == EEOP_BOOLTEST_IS_FALSE {
                    LLVMBuildStore(b, l_sizet_const(0), v_resvaluep);
                } else {
                    LLVMBuildStore(b, l_sizet_const(1), v_resvaluep);
                }

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));

                LLVMPositionBuilderAtEnd(b, b_notnull);

                if opcode == EEOP_BOOLTEST_IS_TRUE || opcode == EEOP_BOOLTEST_IS_NOT_FALSE {
                    /*
                     * if value is not null NULL, return value (already
                     * set)
                     */
                } else {
                    let mut v_value: LLVMValueRef = l_load(b, TypeSizeT, v_resvaluep, c"".as_ptr());

                    v_value = LLVMBuildZExt(
                        b,
                        LLVMBuildICmp(b, LLVMIntEQ, v_value, l_sizet_const(0), c"".as_ptr()),
                        TypeSizeT,
                        c"".as_ptr(),
                    );
                    LLVMBuildStore(b, v_value, v_resvaluep);
                }
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_PARAM_EXEC => {
                build_EvalXFunc!(b, mod_, c"ExecEvalParamExec".as_ptr(), v_state, op, v_econtext);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_PARAM_EXTERN => {
                build_EvalXFunc!(b, mod_, c"ExecEvalParamExtern".as_ptr(), v_state, op, v_econtext);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_PARAM_CALLBACK => {
                let v_func: LLVMValueRef;
                let mut v_params: [LLVMValueRef; 3] = [null_mut(); 3];

                v_func = l_ptr_const(
                    (*op).d.cparam.paramfunc as *mut c_void,
                    llvm_pg_var_type(c"TypeExecEvalSubroutine".as_ptr()),
                );

                v_params[0] = v_state;
                v_params[1] = l_ptr_const(op as *mut c_void, l_ptr(StructExprEvalStep));
                v_params[2] = v_econtext;
                l_call(
                    b,
                    LLVMGetFunctionType(ExecEvalSubroutineTemplate),
                    v_func,
                    v_params.as_mut_ptr(),
                    lengthof!(v_params),
                    c"".as_ptr(),
                );

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_PARAM_SET => {
                build_EvalXFunc!(b, mod_, c"ExecEvalParamSet".as_ptr(), v_state, op, v_econtext);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_SBSREF_SUBSCRIPTS => {
                let jumpdone: c_int = (*op).d.sbsref_subscript.jumpdone;
                let v_func: LLVMValueRef;
                let mut v_params: [LLVMValueRef; 3] = [null_mut(); 3];
                let mut v_ret: LLVMValueRef;

                v_func = l_ptr_const(
                    (*op).d.sbsref_subscript.subscriptfunc as *mut c_void,
                    llvm_pg_var_type(c"TypeExecEvalBoolSubroutine".as_ptr()),
                );

                v_params[0] = v_state;
                v_params[1] = l_ptr_const(op as *mut c_void, l_ptr(StructExprEvalStep));
                v_params[2] = v_econtext;
                v_ret = l_call(
                    b,
                    LLVMGetFunctionType(ExecEvalBoolSubroutineTemplate),
                    v_func,
                    v_params.as_mut_ptr(),
                    lengthof!(v_params),
                    c"".as_ptr(),
                );
                v_ret = LLVMBuildZExt(b, v_ret, TypeStorageBool, c"".as_ptr());

                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_ret, l_sbool_const(true), c"".as_ptr()),
                    *opblocks.add((opno + 1) as usize),
                    *opblocks.add(jumpdone as usize),
                );
            }

            EEOP_SBSREF_OLD | EEOP_SBSREF_ASSIGN | EEOP_SBSREF_FETCH => {
                let v_func: LLVMValueRef;
                let mut v_params: [LLVMValueRef; 3] = [null_mut(); 3];

                v_func = l_ptr_const(
                    (*op).d.sbsref.subscriptfunc as *mut c_void,
                    llvm_pg_var_type(c"TypeExecEvalSubroutine".as_ptr()),
                );

                v_params[0] = v_state;
                v_params[1] = l_ptr_const(op as *mut c_void, l_ptr(StructExprEvalStep));
                v_params[2] = v_econtext;
                l_call(
                    b,
                    LLVMGetFunctionType(ExecEvalSubroutineTemplate),
                    v_func,
                    v_params.as_mut_ptr(),
                    lengthof!(v_params),
                    c"".as_ptr(),
                );

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_CASE_TESTVAL => {
                let v_casevaluep: LLVMValueRef;
                let v_casevalue: LLVMValueRef;
                let v_casenullp: LLVMValueRef;
                let v_casenull: LLVMValueRef;

                v_casevaluep =
                    l_ptr_const((*op).d.casetest.value as *mut c_void, l_ptr(TypeSizeT));
                v_casenullp =
                    l_ptr_const((*op).d.casetest.isnull as *mut c_void, l_ptr(TypeStorageBool));

                v_casevalue = l_load(b, TypeSizeT, v_casevaluep, c"".as_ptr());
                v_casenull = l_load(b, TypeStorageBool, v_casenullp, c"".as_ptr());
                LLVMBuildStore(b, v_casevalue, v_resvaluep);
                LLVMBuildStore(b, v_casenull, v_resnullp);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_CASE_TESTVAL_EXT => {
                let v_casevalue: LLVMValueRef;
                let v_casenull: LLVMValueRef;

                v_casevalue = l_load_struct_gep(
                    b,
                    StructExprContext,
                    v_econtext,
                    FIELDNO_EXPRCONTEXT_CASEDATUM as i32,
                    c"".as_ptr(),
                );
                v_casenull = l_load_struct_gep(
                    b,
                    StructExprContext,
                    v_econtext,
                    FIELDNO_EXPRCONTEXT_CASENULL as i32,
                    c"".as_ptr(),
                );
                LLVMBuildStore(b, v_casevalue, v_resvaluep);
                LLVMBuildStore(b, v_casenull, v_resnullp);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_MAKE_READONLY => {
                let b_notnull: LLVMBasicBlockRef;
                let mut v_params: [LLVMValueRef; 1] = [null_mut(); 1];
                let v_ret: LLVMValueRef;
                let v_nullp: LLVMValueRef;
                let v_valuep: LLVMValueRef;
                let v_null: LLVMValueRef;
                let v_value: LLVMValueRef;

                b_notnull = l_bb_before_v(
                    *opblocks.add((opno + 1) as usize),
                    c"op.%d.readonly.notnull".as_ptr(),
                );

                v_nullp = l_ptr_const(
                    (*op).d.make_readonly.isnull as *mut c_void,
                    l_ptr(TypeStorageBool),
                );

                v_null = l_load(b, TypeStorageBool, v_nullp, c"".as_ptr());

                /* store null isnull value in result */
                LLVMBuildStore(b, v_null, v_resnullp);

                /* check if value is NULL */
                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_null, l_sbool_const(true), c"".as_ptr()),
                    *opblocks.add((opno + 1) as usize),
                    b_notnull,
                );

                /* if value is not null, convert to RO datum */
                LLVMPositionBuilderAtEnd(b, b_notnull);

                v_valuep =
                    l_ptr_const((*op).d.make_readonly.value as *mut c_void, l_ptr(TypeSizeT));

                v_value = l_load(b, TypeSizeT, v_valuep, c"".as_ptr());

                v_params[0] = v_value;
                v_ret = l_call(
                    b,
                    llvm_pg_var_func_type(c"MakeExpandedObjectReadOnlyInternal".as_ptr()),
                    llvm_pg_func(mod_, c"MakeExpandedObjectReadOnlyInternal".as_ptr()),
                    v_params.as_mut_ptr(),
                    lengthof!(v_params),
                    c"".as_ptr(),
                );
                LLVMBuildStore(b, v_ret, v_resvaluep);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_IOCOERCE => {
                let fcinfo_out: FunctionCallInfo;
                let fcinfo_in: FunctionCallInfo;
                let v_fn_out: LLVMValueRef;
                let v_fn_in: LLVMValueRef;
                let v_fcinfo_out: LLVMValueRef;
                let v_fcinfo_in: LLVMValueRef;
                let v_fcinfo_in_isnullp: LLVMValueRef;
                let v_retval: LLVMValueRef;
                let v_resvalue: LLVMValueRef;
                let v_resnull: LLVMValueRef;

                let v_output_skip: LLVMValueRef;
                let mut v_output: LLVMValueRef;

                let b_skipoutput: LLVMBasicBlockRef;
                let b_calloutput: LLVMBasicBlockRef;
                let b_input: LLVMBasicBlockRef;
                let b_inputcall: LLVMBasicBlockRef;

                fcinfo_out = (*op).d.iocoerce.fcinfo_data_out;
                fcinfo_in = (*op).d.iocoerce.fcinfo_data_in;

                b_skipoutput = l_bb_before_v(
                    *opblocks.add((opno + 1) as usize),
                    c"op.%d.skipoutputnull".as_ptr(),
                );
                b_calloutput =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"op.%d.calloutput".as_ptr());
                b_input =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"op.%d.input".as_ptr());
                b_inputcall =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"op.%d.inputcall".as_ptr());

                v_fn_out = llvm_function_reference(context, b, mod_, fcinfo_out);
                v_fn_in = llvm_function_reference(context, b, mod_, fcinfo_in);
                v_fcinfo_out =
                    l_ptr_const(fcinfo_out as *mut c_void, l_ptr(StructFunctionCallInfoData));
                v_fcinfo_in =
                    l_ptr_const(fcinfo_in as *mut c_void, l_ptr(StructFunctionCallInfoData));

                v_fcinfo_in_isnullp = l_struct_gep(
                    b,
                    StructFunctionCallInfoData,
                    v_fcinfo_in,
                    FIELDNO_FUNCTIONCALLINFODATA_ISNULL as i32,
                    c"v_fcinfo_in_isnull".as_ptr(),
                );

                /* output functions are not called on nulls */
                v_resnull = l_load(b, TypeStorageBool, v_resnullp, c"".as_ptr());
                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_resnull, l_sbool_const(true), c"".as_ptr()),
                    b_skipoutput,
                    b_calloutput,
                );

                LLVMPositionBuilderAtEnd(b, b_skipoutput);
                v_output_skip = l_sizet_const(0);
                LLVMBuildBr(b, b_input);

                LLVMPositionBuilderAtEnd(b, b_calloutput);
                v_resvalue = l_load(b, TypeSizeT, v_resvaluep, c"".as_ptr());

                /* set arg[0] */
                LLVMBuildStore(b, v_resvalue, l_funcvaluep(b, v_fcinfo_out, 0));
                LLVMBuildStore(b, l_sbool_const(false), l_funcnullp(b, v_fcinfo_out, 0));
                /* and call output function (can never return NULL) */
                v_output = l_call(
                    b,
                    LLVMGetFunctionType(v_fn_out),
                    v_fn_out,
                    &mut v_fcinfo_out,
                    1,
                    c"funccall_coerce_out".as_ptr(),
                );
                LLVMBuildBr(b, b_input);

                /* build block handling input function call */
                LLVMPositionBuilderAtEnd(b, b_input);

                /* phi between resnull and output function call branches */
                {
                    let mut incoming_values: [LLVMValueRef; 2] = [null_mut(); 2];
                    let mut incoming_blocks: [LLVMBasicBlockRef; 2] = [null_mut(); 2];

                    incoming_values[0] = v_output_skip;
                    incoming_blocks[0] = b_skipoutput;

                    incoming_values[1] = v_output;
                    incoming_blocks[1] = b_calloutput;

                    v_output = LLVMBuildPhi(b, TypeSizeT, c"output".as_ptr());
                    LLVMAddIncoming(
                        v_output,
                        incoming_values.as_mut_ptr(),
                        incoming_blocks.as_mut_ptr(),
                        lengthof!(incoming_blocks) as c_uint,
                    );
                }

                /*
                 * If input function is strict, skip if input string is
                 * NULL.
                 */
                if (*(*op).d.iocoerce.finfo_in).fn_strict {
                    LLVMBuildCondBr(
                        b,
                        LLVMBuildICmp(b, LLVMIntEQ, v_output, l_sizet_const(0), c"".as_ptr()),
                        *opblocks.add((opno + 1) as usize),
                        b_inputcall,
                    );
                } else {
                    LLVMBuildBr(b, b_inputcall);
                }

                LLVMPositionBuilderAtEnd(b, b_inputcall);
                /* set arguments */
                /* arg0: output */
                LLVMBuildStore(b, v_output, l_funcvaluep(b, v_fcinfo_in, 0));
                LLVMBuildStore(b, v_resnull, l_funcnullp(b, v_fcinfo_in, 0));

                /* arg1: ioparam: preset in execExpr.c */
                /* arg2: typmod: preset in execExpr.c  */

                /* reset fcinfo_in->isnull */
                LLVMBuildStore(b, l_sbool_const(false), v_fcinfo_in_isnullp);
                /* and call function */
                v_retval = l_call(
                    b,
                    LLVMGetFunctionType(v_fn_in),
                    v_fn_in,
                    &mut v_fcinfo_in,
                    1,
                    c"funccall_iocoerce_in".as_ptr(),
                );

                LLVMBuildStore(b, v_retval, v_resvaluep);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_IOCOERCE_SAFE => {
                build_EvalXFunc!(b, mod_, c"ExecEvalCoerceViaIOSafe".as_ptr(), v_state, op);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_DISTINCT | EEOP_NOT_DISTINCT => {
                let fcinfo: FunctionCallInfo = (*op).d.func.fcinfo_data;

                let v_fcinfo: LLVMValueRef;
                let mut v_fcinfo_isnull: LLVMValueRef = null_mut();

                let v_argnull0: LLVMValueRef;
                let v_argisnull0: LLVMValueRef;
                let v_argnull1: LLVMValueRef;
                let v_argisnull1: LLVMValueRef;

                let v_anyargisnull: LLVMValueRef;
                let v_bothargisnull: LLVMValueRef;

                let mut v_result: LLVMValueRef;

                let b_noargnull: LLVMBasicBlockRef;
                let b_checkbothargnull: LLVMBasicBlockRef;
                let b_bothargnull: LLVMBasicBlockRef;
                let b_anyargnull: LLVMBasicBlockRef;

                b_noargnull =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"op.%d.noargnull".as_ptr());
                b_checkbothargnull = l_bb_before_v(
                    *opblocks.add((opno + 1) as usize),
                    c"op.%d.checkbothargnull".as_ptr(),
                );
                b_bothargnull = l_bb_before_v(
                    *opblocks.add((opno + 1) as usize),
                    c"op.%d.bothargnull".as_ptr(),
                );
                b_anyargnull =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"op.%d.anyargnull".as_ptr());

                v_fcinfo = l_ptr_const(fcinfo as *mut c_void, l_ptr(StructFunctionCallInfoData));

                /* load args[0|1].isnull for both arguments */
                v_argnull0 = l_funcnull(b, v_fcinfo, 0);
                v_argisnull0 =
                    LLVMBuildICmp(b, LLVMIntEQ, v_argnull0, l_sbool_const(true), c"".as_ptr());
                v_argnull1 = l_funcnull(b, v_fcinfo, 1);
                v_argisnull1 =
                    LLVMBuildICmp(b, LLVMIntEQ, v_argnull1, l_sbool_const(true), c"".as_ptr());

                v_anyargisnull = LLVMBuildOr(b, v_argisnull0, v_argisnull1, c"".as_ptr());
                v_bothargisnull = LLVMBuildAnd(b, v_argisnull0, v_argisnull1, c"".as_ptr());

                /*
                 * Check function arguments for NULLness: If either is
                 * NULL, we check if both args are NULL. Otherwise call
                 * comparator.
                 */
                LLVMBuildCondBr(b, v_anyargisnull, b_checkbothargnull, b_noargnull);

                /*
                 * build block checking if any arg is null
                 */
                LLVMPositionBuilderAtEnd(b, b_checkbothargnull);
                LLVMBuildCondBr(b, v_bothargisnull, b_bothargnull, b_anyargnull);

                /* Both NULL? Then is not distinct... */
                LLVMPositionBuilderAtEnd(b, b_bothargnull);
                LLVMBuildStore(b, l_sbool_const(false), v_resnullp);
                if opcode == EEOP_NOT_DISTINCT {
                    LLVMBuildStore(b, l_sizet_const(1), v_resvaluep);
                } else {
                    LLVMBuildStore(b, l_sizet_const(0), v_resvaluep);
                }

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));

                /* Only one is NULL? Then is distinct... */
                LLVMPositionBuilderAtEnd(b, b_anyargnull);
                LLVMBuildStore(b, l_sbool_const(false), v_resnullp);
                if opcode == EEOP_NOT_DISTINCT {
                    LLVMBuildStore(b, l_sizet_const(0), v_resvaluep);
                } else {
                    LLVMBuildStore(b, l_sizet_const(1), v_resvaluep);
                }
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));

                /* neither argument is null: compare */
                LLVMPositionBuilderAtEnd(b, b_noargnull);

                v_result = BuildV1Call(context, b, mod_, fcinfo, &mut v_fcinfo_isnull);

                if opcode == EEOP_DISTINCT {
                    /* Must invert result of "=" */
                    v_result = LLVMBuildZExt(
                        b,
                        LLVMBuildICmp(b, LLVMIntEQ, v_result, l_sizet_const(0), c"".as_ptr()),
                        TypeSizeT,
                        c"".as_ptr(),
                    );
                }

                LLVMBuildStore(b, v_fcinfo_isnull, v_resnullp);
                LLVMBuildStore(b, v_result, v_resvaluep);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_NULLIF => {
                let fcinfo: FunctionCallInfo = (*op).d.func.fcinfo_data;

                let v_fcinfo: LLVMValueRef;
                let mut v_fcinfo_isnull: LLVMValueRef = null_mut();
                let v_argnull0: LLVMValueRef;
                let v_argnull1: LLVMValueRef;
                let v_anyargisnull: LLVMValueRef;
                let v_arg0: LLVMValueRef;
                let b_hasnull: LLVMBasicBlockRef;
                let b_nonull: LLVMBasicBlockRef;
                let b_argsequal: LLVMBasicBlockRef;
                let v_retval: LLVMValueRef;
                let v_argsequal: LLVMValueRef;

                b_hasnull =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"b.%d.null-args".as_ptr());
                b_nonull = l_bb_before_v(
                    *opblocks.add((opno + 1) as usize),
                    c"b.%d.no-null-args".as_ptr(),
                );
                b_argsequal =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"b.%d.argsequal".as_ptr());

                v_fcinfo = l_ptr_const(fcinfo as *mut c_void, l_ptr(StructFunctionCallInfoData));

                /* save original arg[0] */
                v_arg0 = l_funcvalue(b, v_fcinfo, 0);

                /* if either argument is NULL they can't be equal */
                v_argnull0 = l_funcnull(b, v_fcinfo, 0);
                v_argnull1 = l_funcnull(b, v_fcinfo, 1);

                v_anyargisnull = LLVMBuildOr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_argnull0, l_sbool_const(true), c"".as_ptr()),
                    LLVMBuildICmp(b, LLVMIntEQ, v_argnull1, l_sbool_const(true), c"".as_ptr()),
                    c"".as_ptr(),
                );

                LLVMBuildCondBr(b, v_anyargisnull, b_hasnull, b_nonull);

                /* one (or both) of the arguments are null, return arg[0] */
                LLVMPositionBuilderAtEnd(b, b_hasnull);
                LLVMBuildStore(b, v_argnull0, v_resnullp);
                LLVMBuildStore(b, v_arg0, v_resvaluep);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));

                /* build block to invoke function and check result */
                LLVMPositionBuilderAtEnd(b, b_nonull);

                /*
                 * If first argument is of varlena type, it might be an
                 * expanded datum.  We need to ensure that the value
                 * passed to the comparison function is a read-only
                 * pointer.  However, if we end by returning the first
                 * argument, that will be the original read-write pointer
                 * if it was read-write.
                 */
                if (*op).d.func.make_ro {
                    let mut v_params: [LLVMValueRef; 1] = [null_mut(); 1];
                    let v_arg0_ro: LLVMValueRef;

                    v_params[0] = v_arg0;
                    v_arg0_ro = l_call(
                        b,
                        llvm_pg_var_func_type(c"MakeExpandedObjectReadOnlyInternal".as_ptr()),
                        llvm_pg_func(mod_, c"MakeExpandedObjectReadOnlyInternal".as_ptr()),
                        v_params.as_mut_ptr(),
                        lengthof!(v_params),
                        c"".as_ptr(),
                    );
                    LLVMBuildStore(b, v_arg0_ro, l_funcvaluep(b, v_fcinfo, 0));
                }

                v_retval = BuildV1Call(context, b, mod_, fcinfo, &mut v_fcinfo_isnull);

                /*
                 * If result not null and arguments are equal return null,
                 * else return arg[0] (same result as if there'd been
                 * NULLs, hence reuse b_hasnull).
                 */
                v_argsequal = LLVMBuildAnd(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_fcinfo_isnull, l_sbool_const(false), c"".as_ptr()),
                    LLVMBuildICmp(b, LLVMIntEQ, v_retval, l_sizet_const(1), c"".as_ptr()),
                    c"".as_ptr(),
                );
                LLVMBuildCondBr(b, v_argsequal, b_argsequal, b_hasnull);

                /* build block setting result to NULL, if args are equal */
                LLVMPositionBuilderAtEnd(b, b_argsequal);
                LLVMBuildStore(b, l_sbool_const(true), v_resnullp);
                LLVMBuildStore(b, l_sizet_const(0), v_resvaluep);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_SQLVALUEFUNCTION => {
                build_EvalXFunc!(b, mod_, c"ExecEvalSQLValueFunction".as_ptr(), v_state, op);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_CURRENTOFEXPR => {
                build_EvalXFunc!(b, mod_, c"ExecEvalCurrentOfExpr".as_ptr(), v_state, op);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_NEXTVALUEEXPR => {
                build_EvalXFunc!(b, mod_, c"ExecEvalNextValueExpr".as_ptr(), v_state, op);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_RETURNINGEXPR => {
                let b_isnull: LLVMBasicBlockRef;
                let v_flagsp: LLVMValueRef;
                let v_flags: LLVMValueRef;
                let v_nullflag: LLVMValueRef;

                b_isnull =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"op.%d.row.isnull".as_ptr());

                /*
                 * The next op actually evaluates the expression.  If the
                 * OLD/NEW row doesn't exist, skip that and return NULL.
                 */
                v_flagsp = l_struct_gep(
                    b,
                    StructExprState,
                    v_state,
                    FIELDNO_EXPRSTATE_FLAGS as i32,
                    c"v.state.flags".as_ptr(),
                );
                v_flags = l_load(b, TypeStorageBool, v_flagsp, c"".as_ptr());

                v_nullflag = l_int8_const(lc, (*op).d.returningexpr.nullflag as int8);

                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(
                        b,
                        LLVMIntEQ,
                        LLVMBuildAnd(b, v_flags, v_nullflag, c"".as_ptr()),
                        l_sbool_const(false),
                        c"".as_ptr(),
                    ),
                    *opblocks.add((opno + 1) as usize),
                    b_isnull,
                );

                LLVMPositionBuilderAtEnd(b, b_isnull);

                LLVMBuildStore(b, l_sizet_const(0), v_resvaluep);
                LLVMBuildStore(b, l_sbool_const(true), v_resnullp);

                LLVMBuildBr(b, *opblocks.add((*op).d.returningexpr.jumpdone as usize));
            }

            EEOP_ARRAYEXPR => {
                build_EvalXFunc!(b, mod_, c"ExecEvalArrayExpr".as_ptr(), v_state, op);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_ARRAYCOERCE => {
                build_EvalXFunc!(b, mod_, c"ExecEvalArrayCoerce".as_ptr(), v_state, op, v_econtext);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_ROW => {
                build_EvalXFunc!(b, mod_, c"ExecEvalRow".as_ptr(), v_state, op);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_ROWCOMPARE_STEP => {
                let fcinfo: FunctionCallInfo = (*op).d.rowcompare_step.fcinfo_data;
                let mut v_fcinfo_isnull: LLVMValueRef = null_mut();
                let b_null: LLVMBasicBlockRef;
                let b_compare: LLVMBasicBlockRef;
                let b_compare_result: LLVMBasicBlockRef;

                let v_retval: LLVMValueRef;

                b_null =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"op.%d.row-null".as_ptr());
                b_compare = l_bb_before_v(
                    *opblocks.add((opno + 1) as usize),
                    c"op.%d.row-compare".as_ptr(),
                );
                b_compare_result = l_bb_before_v(
                    *opblocks.add((opno + 1) as usize),
                    c"op.%d.row-compare-result".as_ptr(),
                );

                /*
                 * If function is strict, and either arg is null, we're
                 * done.
                 */
                if (*(*op).d.rowcompare_step.finfo).fn_strict {
                    let v_fcinfo: LLVMValueRef;
                    let v_argnull0: LLVMValueRef;
                    let v_argnull1: LLVMValueRef;
                    let v_anyargisnull: LLVMValueRef;

                    v_fcinfo =
                        l_ptr_const(fcinfo as *mut c_void, l_ptr(StructFunctionCallInfoData));

                    v_argnull0 = l_funcnull(b, v_fcinfo, 0);
                    v_argnull1 = l_funcnull(b, v_fcinfo, 1);

                    v_anyargisnull = LLVMBuildOr(
                        b,
                        LLVMBuildICmp(b, LLVMIntEQ, v_argnull0, l_sbool_const(true), c"".as_ptr()),
                        LLVMBuildICmp(b, LLVMIntEQ, v_argnull1, l_sbool_const(true), c"".as_ptr()),
                        c"".as_ptr(),
                    );

                    LLVMBuildCondBr(b, v_anyargisnull, b_null, b_compare);
                } else {
                    LLVMBuildBr(b, b_compare);
                }

                /* build block invoking comparison function */
                LLVMPositionBuilderAtEnd(b, b_compare);

                /* call function */
                v_retval = BuildV1Call(context, b, mod_, fcinfo, &mut v_fcinfo_isnull);
                LLVMBuildStore(b, v_retval, v_resvaluep);

                /* if result of function is NULL, force NULL result */
                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_fcinfo_isnull, l_sbool_const(false), c"".as_ptr()),
                    b_compare_result,
                    b_null,
                );

                /* build block analyzing the !NULL comparator result */
                LLVMPositionBuilderAtEnd(b, b_compare_result);

                /* if results equal, compare next, otherwise done */
                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_retval, l_sizet_const(0), c"".as_ptr()),
                    *opblocks.add((opno + 1) as usize),
                    *opblocks.add((*op).d.rowcompare_step.jumpdone as usize),
                );

                /*
                 * Build block handling NULL input or NULL comparator
                 * result.
                 */
                LLVMPositionBuilderAtEnd(b, b_null);
                LLVMBuildStore(b, l_sbool_const(true), v_resnullp);
                LLVMBuildBr(b, *opblocks.add((*op).d.rowcompare_step.jumpnull as usize));
            }

            EEOP_ROWCOMPARE_FINAL => {
                let cmptype: CompareType = (*op).d.rowcompare_final.cmptype;

                let v_cmpresult: LLVMValueRef;
                let mut v_result: LLVMValueRef;
                let predicate: LLVMIntPredicate;

                /*
                 * Btree comparators return 32 bit results, need to be
                 * careful about sign (used as a 64 bit value it's
                 * otherwise wrong).
                 */
                v_cmpresult = LLVMBuildTrunc(
                    b,
                    l_load(b, TypeSizeT, v_resvaluep, c"".as_ptr()),
                    LLVMInt32TypeInContext(lc),
                    c"".as_ptr(),
                );

                match cmptype {
                    COMPARE_LT => {
                        predicate = LLVMIntSLT;
                    }
                    COMPARE_LE => {
                        predicate = LLVMIntSLE;
                    }
                    COMPARE_GT => {
                        predicate = LLVMIntSGT;
                    }
                    COMPARE_GE => {
                        predicate = LLVMIntSGE;
                    }
                    _ => {
                        /* EQ and NE cases aren't allowed here */
                        Assert!(false);
                        predicate = 0; /* prevent compiler warning */
                    }
                }

                v_result =
                    LLVMBuildICmp(b, predicate, v_cmpresult, l_int32_const(lc, 0), c"".as_ptr());
                v_result = LLVMBuildZExt(b, v_result, TypeSizeT, c"".as_ptr());

                LLVMBuildStore(b, l_sbool_const(false), v_resnullp);
                LLVMBuildStore(b, v_result, v_resvaluep);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_MINMAX => {
                build_EvalXFunc!(b, mod_, c"ExecEvalMinMax".as_ptr(), v_state, op);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_FIELDSELECT => {
                build_EvalXFunc!(b, mod_, c"ExecEvalFieldSelect".as_ptr(), v_state, op, v_econtext);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_FIELDSTORE_DEFORM => {
                build_EvalXFunc!(
                    b,
                    mod_,
                    c"ExecEvalFieldStoreDeForm".as_ptr(),
                    v_state,
                    op,
                    v_econtext
                );
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_FIELDSTORE_FORM => {
                build_EvalXFunc!(
                    b,
                    mod_,
                    c"ExecEvalFieldStoreForm".as_ptr(),
                    v_state,
                    op,
                    v_econtext
                );
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_DOMAIN_TESTVAL => {
                let v_casevaluep: LLVMValueRef;
                let v_casevalue: LLVMValueRef;
                let v_casenullp: LLVMValueRef;
                let v_casenull: LLVMValueRef;

                v_casevaluep =
                    l_ptr_const((*op).d.casetest.value as *mut c_void, l_ptr(TypeSizeT));
                v_casenullp =
                    l_ptr_const((*op).d.casetest.isnull as *mut c_void, l_ptr(TypeStorageBool));

                v_casevalue = l_load(b, TypeSizeT, v_casevaluep, c"".as_ptr());
                v_casenull = l_load(b, TypeStorageBool, v_casenullp, c"".as_ptr());
                LLVMBuildStore(b, v_casevalue, v_resvaluep);
                LLVMBuildStore(b, v_casenull, v_resnullp);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_DOMAIN_TESTVAL_EXT => {
                let v_casevalue: LLVMValueRef;
                let v_casenull: LLVMValueRef;

                v_casevalue = l_load_struct_gep(
                    b,
                    StructExprContext,
                    v_econtext,
                    FIELDNO_EXPRCONTEXT_DOMAINDATUM as i32,
                    c"".as_ptr(),
                );
                v_casenull = l_load_struct_gep(
                    b,
                    StructExprContext,
                    v_econtext,
                    FIELDNO_EXPRCONTEXT_DOMAINNULL as i32,
                    c"".as_ptr(),
                );
                LLVMBuildStore(b, v_casevalue, v_resvaluep);
                LLVMBuildStore(b, v_casenull, v_resnullp);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_DOMAIN_NOTNULL => {
                build_EvalXFunc!(b, mod_, c"ExecEvalConstraintNotNull".as_ptr(), v_state, op);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_DOMAIN_CHECK => {
                build_EvalXFunc!(b, mod_, c"ExecEvalConstraintCheck".as_ptr(), v_state, op);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_HASHDATUM_SET_INITVAL => {
                let v_initvalue: LLVMValueRef;

                v_initvalue = l_sizet_const((*op).d.hashdatum_initvalue.init_value as Size);

                LLVMBuildStore(b, v_initvalue, v_resvaluep);
                LLVMBuildStore(b, l_sbool_const(false), v_resnullp);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_HASHDATUM_FIRST | EEOP_HASHDATUM_FIRST_STRICT | EEOP_HASHDATUM_NEXT32
            | EEOP_HASHDATUM_NEXT32_STRICT => {
                let fcinfo: FunctionCallInfo = (*op).d.hashdatum.fcinfo_data;
                let v_fcinfo: LLVMValueRef;
                let mut v_fcinfo_isnull: LLVMValueRef = null_mut();
                let mut v_retval: LLVMValueRef;
                let b_checkargnull: LLVMBasicBlockRef;
                let b_ifnotnull: LLVMBasicBlockRef;
                let b_ifnullblock: LLVMBasicBlockRef;
                let v_argisnull: LLVMValueRef;
                let mut v_prevhash: LLVMValueRef = null_mut();

                /*
                 * When performing the next hash and not in strict mode we
                 * perform a rotation of the previously stored hash value
                 * before doing the NULL check.  We want to do this even
                 * when we receive a NULL Datum to hash.  In strict mode,
                 * we do this after the NULL check so as not to waste the
                 * effort of rotating the bits when we're going to throw
                 * away the hash value and return NULL.
                 */
                if opcode == EEOP_HASHDATUM_NEXT32 {
                    let v_tmp1: LLVMValueRef;
                    let v_tmp2: LLVMValueRef;
                    let tmp: LLVMValueRef;

                    tmp = l_ptr_const(
                        &raw mut (*(*op).d.hashdatum.iresult).value as *mut c_void,
                        l_ptr(TypeSizeT),
                    );

                    /*
                     * Fetch the previously hashed value from where the
                     * previous hash operation stored it.
                     */
                    v_prevhash = l_load(b, TypeSizeT, tmp, c"prevhash".as_ptr());

                    /*
                     * Rotate bits left by 1 bit.  Be careful not to
                     * overflow uint32 when working with size_t.
                     */
                    let v_tmp1a = LLVMBuildShl(b, v_prevhash, l_sizet_const(1), c"".as_ptr());
                    v_tmp1 = LLVMBuildAnd(b, v_tmp1a, l_sizet_const(0xffffffff), c"".as_ptr());
                    v_tmp2 = LLVMBuildLShr(b, v_prevhash, l_sizet_const(31), c"".as_ptr());
                    v_prevhash = LLVMBuildOr(b, v_tmp1, v_tmp2, c"rotatedhash".as_ptr());
                }

                /*
                 * Block for the actual function call, if args are
                 * non-NULL.
                 */
                b_ifnotnull =
                    l_bb_before_v(*opblocks.add((opno + 1) as usize), c"b.%d.ifnotnull".as_ptr());

                /* we expect the hash function to have 1 argument */
                if (*fcinfo).nargs != 1 {
                    elog!(ERROR, "incorrect number of function arguments");
                }

                v_fcinfo = l_ptr_const(fcinfo as *mut c_void, l_ptr(StructFunctionCallInfoData));

                b_checkargnull = l_bb_before_v(b_ifnotnull, c"b.%d.isnull.0".as_ptr());

                LLVMBuildBr(b, b_checkargnull);

                /*
                 * Determine what to do if we find the argument to be
                 * NULL.
                 */
                if opcode == EEOP_HASHDATUM_FIRST_STRICT
                    || opcode == EEOP_HASHDATUM_NEXT32_STRICT
                {
                    b_ifnullblock = l_bb_before_v(b_ifnotnull, c"b.%d.strictnull".as_ptr());

                    LLVMPositionBuilderAtEnd(b, b_ifnullblock);

                    /*
                     * In strict node, NULL inputs result in NULL.  Save
                     * the NULL result and goto jumpdone.
                     */
                    LLVMBuildStore(b, l_sbool_const(true), v_resnullp);
                    LLVMBuildStore(b, l_sizet_const(0), v_resvaluep);
                    LLVMBuildBr(b, *opblocks.add((*op).d.hashdatum.jumpdone as usize));
                } else {
                    b_ifnullblock = l_bb_before_v(b_ifnotnull, c"b.%d.null".as_ptr());

                    LLVMPositionBuilderAtEnd(b, b_ifnullblock);

                    LLVMBuildStore(b, l_sbool_const(false), v_resnullp);

                    if opcode == EEOP_HASHDATUM_NEXT32 {
                        Assert!(!v_prevhash.is_null());

                        /*
                         * Save the rotated hash value and skip to the
                         * next op.
                         */
                        LLVMBuildStore(b, v_prevhash, v_resvaluep);
                    } else {
                        Assert!(opcode == EEOP_HASHDATUM_FIRST);

                        /*
                         * Store a zero Datum when the Datum to hash is
                         * NULL
                         */
                        LLVMBuildStore(b, l_sizet_const(0), v_resvaluep);
                    }

                    LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
                }

                LLVMPositionBuilderAtEnd(b, b_checkargnull);

                /* emit code to check if the input parameter is NULL */
                v_argisnull = l_funcnull(b, v_fcinfo, 0);
                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_argisnull, l_sbool_const(true), c"".as_ptr()),
                    b_ifnullblock,
                    b_ifnotnull,
                );

                LLVMPositionBuilderAtEnd(b, b_ifnotnull);

                /*
                 * Rotate the previously stored hash value when performing
                 * NEXT32 in strict mode.  In non-strict mode we already
                 * did this before checking for NULLs.
                 */
                if opcode == EEOP_HASHDATUM_NEXT32_STRICT {
                    let v_tmp1: LLVMValueRef;
                    let v_tmp2: LLVMValueRef;
                    let tmp: LLVMValueRef;

                    tmp = l_ptr_const(
                        &raw mut (*(*op).d.hashdatum.iresult).value as *mut c_void,
                        l_ptr(TypeSizeT),
                    );

                    /*
                     * Fetch the previously hashed value from where the
                     * previous hash operation stored it.
                     */
                    v_prevhash = l_load(b, TypeSizeT, tmp, c"prevhash".as_ptr());

                    /*
                     * Rotate bits left by 1 bit.  Be careful not to
                     * overflow uint32 when working with size_t.
                     */
                    let v_tmp1a = LLVMBuildShl(b, v_prevhash, l_sizet_const(1), c"".as_ptr());
                    v_tmp1 = LLVMBuildAnd(b, v_tmp1a, l_sizet_const(0xffffffff), c"".as_ptr());
                    v_tmp2 = LLVMBuildLShr(b, v_prevhash, l_sizet_const(31), c"".as_ptr());
                    v_prevhash = LLVMBuildOr(b, v_tmp1, v_tmp2, c"rotatedhash".as_ptr());
                }

                /* call the hash function */
                v_retval = BuildV1Call(context, b, mod_, fcinfo, &mut v_fcinfo_isnull);

                /*
                 * For NEXT32 ops, XOR (^) the returned hash value with
                 * the existing hash value.
                 */
                if opcode == EEOP_HASHDATUM_NEXT32 || opcode == EEOP_HASHDATUM_NEXT32_STRICT {
                    v_retval = LLVMBuildXor(b, v_prevhash, v_retval, c"xorhash".as_ptr());
                }

                LLVMBuildStore(b, v_retval, v_resvaluep);
                LLVMBuildStore(b, l_sbool_const(false), v_resnullp);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_CONVERT_ROWTYPE => {
                build_EvalXFunc!(
                    b,
                    mod_,
                    c"ExecEvalConvertRowtype".as_ptr(),
                    v_state,
                    op,
                    v_econtext
                );
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_SCALARARRAYOP => {
                build_EvalXFunc!(b, mod_, c"ExecEvalScalarArrayOp".as_ptr(), v_state, op);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_HASHED_SCALARARRAYOP => {
                build_EvalXFunc!(
                    b,
                    mod_,
                    c"ExecEvalHashedScalarArrayOp".as_ptr(),
                    v_state,
                    op,
                    v_econtext
                );
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_XMLEXPR => {
                build_EvalXFunc!(b, mod_, c"ExecEvalXmlExpr".as_ptr(), v_state, op);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_JSON_CONSTRUCTOR => {
                build_EvalXFunc!(
                    b,
                    mod_,
                    c"ExecEvalJsonConstructor".as_ptr(),
                    v_state,
                    op,
                    v_econtext
                );
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_IS_JSON => {
                build_EvalXFunc!(b, mod_, c"ExecEvalJsonIsPredicate".as_ptr(), v_state, op);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_JSONEXPR_PATH => {
                let jsestate: *mut JsonExprState = (*op).d.jsonexpr.jsestate;
                let v_ret: LLVMValueRef;

                /*
                 * Call ExecEvalJsonExprPath().  It returns the address of
                 * the step to perform next.
                 */
                v_ret = build_EvalXFunc!(
                    b,
                    mod_,
                    c"ExecEvalJsonExprPath".as_ptr(),
                    v_state,
                    op,
                    v_econtext
                );

                /*
                 * Build a switch to map the return value (v_ret above),
                 * which is a runtime value of the step address to perform
                 * next, to either jump_empty, jump_error,
                 * jump_eval_coercion, or jump_end.
                 */
                if (*jsestate).jump_empty >= 0
                    || (*jsestate).jump_error >= 0
                    || (*jsestate).jump_eval_coercion >= 0
                {
                    let v_jump_empty: LLVMValueRef;
                    let v_jump_error: LLVMValueRef;
                    let v_jump_coercion: LLVMValueRef;
                    let v_switch: LLVMValueRef;
                    let b_done: LLVMBasicBlockRef;
                    let b_empty: LLVMBasicBlockRef;
                    let b_error: LLVMBasicBlockRef;
                    let b_coercion: LLVMBasicBlockRef;

                    b_empty = l_bb_before_v(
                        *opblocks.add((opno + 1) as usize),
                        c"op.%d.jsonexpr_empty".as_ptr(),
                    );
                    b_error = l_bb_before_v(
                        *opblocks.add((opno + 1) as usize),
                        c"op.%d.jsonexpr_error".as_ptr(),
                    );
                    b_coercion = l_bb_before_v(
                        *opblocks.add((opno + 1) as usize),
                        c"op.%d.jsonexpr_coercion".as_ptr(),
                    );
                    b_done = l_bb_before_v(
                        *opblocks.add((opno + 1) as usize),
                        c"op.%d.jsonexpr_done".as_ptr(),
                    );

                    v_switch = LLVMBuildSwitch(b, v_ret, b_done, 3);
                    /* Returned jsestate->jump_empty? */
                    if (*jsestate).jump_empty >= 0 {
                        v_jump_empty = l_int32_const(lc, (*jsestate).jump_empty);
                        LLVMAddCase(v_switch, v_jump_empty, b_empty);
                    }
                    /* ON EMPTY code */
                    LLVMPositionBuilderAtEnd(b, b_empty);
                    if (*jsestate).jump_empty >= 0 {
                        LLVMBuildBr(b, *opblocks.add((*jsestate).jump_empty as usize));
                    } else {
                        LLVMBuildUnreachable(b);
                    }

                    /* Returned jsestate->jump_error? */
                    if (*jsestate).jump_error >= 0 {
                        v_jump_error = l_int32_const(lc, (*jsestate).jump_error);
                        LLVMAddCase(v_switch, v_jump_error, b_error);
                    }
                    /* ON ERROR code */
                    LLVMPositionBuilderAtEnd(b, b_error);
                    if (*jsestate).jump_error >= 0 {
                        LLVMBuildBr(b, *opblocks.add((*jsestate).jump_error as usize));
                    } else {
                        LLVMBuildUnreachable(b);
                    }

                    /* Returned jsestate->jump_eval_coercion? */
                    if (*jsestate).jump_eval_coercion >= 0 {
                        v_jump_coercion = l_int32_const(lc, (*jsestate).jump_eval_coercion);
                        LLVMAddCase(v_switch, v_jump_coercion, b_coercion);
                    }
                    /* jump_eval_coercion code */
                    LLVMPositionBuilderAtEnd(b, b_coercion);
                    if (*jsestate).jump_eval_coercion >= 0 {
                        LLVMBuildBr(b, *opblocks.add((*jsestate).jump_eval_coercion as usize));
                    } else {
                        LLVMBuildUnreachable(b);
                    }

                    LLVMPositionBuilderAtEnd(b, b_done);
                }

                LLVMBuildBr(b, *opblocks.add((*jsestate).jump_end as usize));
            }

            EEOP_JSONEXPR_COERCION => {
                build_EvalXFunc!(b, mod_, c"ExecEvalJsonCoercion".as_ptr(), v_state, op, v_econtext);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_JSONEXPR_COERCION_FINISH => {
                build_EvalXFunc!(b, mod_, c"ExecEvalJsonCoercionFinish".as_ptr(), v_state, op);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_AGGREF => {
                let v_aggno: LLVMValueRef;
                let value: LLVMValueRef;
                let isnull: LLVMValueRef;

                v_aggno = l_int32_const(lc, (*op).d.aggref.aggno);

                /* load agg value / null */
                value = l_load_gep1(b, TypeSizeT, v_aggvalues, v_aggno, c"aggvalue".as_ptr());
                isnull = l_load_gep1(b, TypeStorageBool, v_aggnulls, v_aggno, c"aggnull".as_ptr());

                /* and store result */
                LLVMBuildStore(b, value, v_resvaluep);
                LLVMBuildStore(b, isnull, v_resnullp);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_GROUPING_FUNC => {
                build_EvalXFunc!(b, mod_, c"ExecEvalGroupingFunc".as_ptr(), v_state, op);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_WINDOW_FUNC => {
                let wfunc: *mut WindowFuncExprState = (*op).d.window_func.wfstate;
                let v_wfuncnop: LLVMValueRef;
                let v_wfuncno: LLVMValueRef;
                let value: LLVMValueRef;
                let isnull: LLVMValueRef;

                /*
                 * At this point aggref->wfuncno is not yet set (it's set
                 * up in ExecInitWindowAgg() after initializing the
                 * expression). So load it from memory each time round.
                 */
                v_wfuncnop = l_ptr_const(
                    &raw mut (*wfunc).wfuncno as *mut c_void,
                    l_ptr(LLVMInt32TypeInContext(lc)),
                );
                v_wfuncno =
                    l_load(b, LLVMInt32TypeInContext(lc), v_wfuncnop, c"v_wfuncno".as_ptr());

                /* load window func value / null */
                value = l_load_gep1(b, TypeSizeT, v_aggvalues, v_wfuncno, c"windowvalue".as_ptr());
                isnull =
                    l_load_gep1(b, TypeStorageBool, v_aggnulls, v_wfuncno, c"windownull".as_ptr());

                LLVMBuildStore(b, value, v_resvaluep);
                LLVMBuildStore(b, isnull, v_resnullp);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_MERGE_SUPPORT_FUNC => {
                build_EvalXFunc!(
                    b,
                    mod_,
                    c"ExecEvalMergeSupportFunc".as_ptr(),
                    v_state,
                    op,
                    v_econtext
                );
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_SUBPLAN => {
                build_EvalXFunc!(b, mod_, c"ExecEvalSubPlan".as_ptr(), v_state, op, v_econtext);
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_AGG_STRICT_DESERIALIZE | EEOP_AGG_DESERIALIZE => {
                let aggstate: *mut AggState;
                let mut fcinfo: FunctionCallInfo = (*op).d.agg_deserialize.fcinfo_data;

                let v_retval: LLVMValueRef;
                let mut v_fcinfo_isnull: LLVMValueRef = null_mut();
                let v_tmpcontext: LLVMValueRef;
                let v_oldcontext: LLVMValueRef;

                if opcode == EEOP_AGG_STRICT_DESERIALIZE {
                    let v_fcinfo: LLVMValueRef;
                    let v_argnull0: LLVMValueRef;
                    let b_deserialize: LLVMBasicBlockRef;

                    b_deserialize = l_bb_before_v(
                        *opblocks.add((opno + 1) as usize),
                        c"op.%d.deserialize".as_ptr(),
                    );

                    v_fcinfo =
                        l_ptr_const(fcinfo as *mut c_void, l_ptr(StructFunctionCallInfoData));
                    v_argnull0 = l_funcnull(b, v_fcinfo, 0);

                    LLVMBuildCondBr(
                        b,
                        LLVMBuildICmp(b, LLVMIntEQ, v_argnull0, l_sbool_const(true), c"".as_ptr()),
                        *opblocks.add((*op).d.agg_deserialize.jumpnull as usize),
                        b_deserialize,
                    );
                    LLVMPositionBuilderAtEnd(b, b_deserialize);
                }

                aggstate = castNode!(AggState, T_AggState, (*state).parent);
                fcinfo = (*op).d.agg_deserialize.fcinfo_data;

                v_tmpcontext = l_ptr_const(
                    (*(*aggstate).tmpcontext).ecxt_per_tuple_memory as *mut c_void,
                    l_ptr(StructMemoryContextData),
                );
                v_oldcontext = l_mcxt_switch(mod_, b, v_tmpcontext);
                v_retval = BuildV1Call(context, b, mod_, fcinfo, &mut v_fcinfo_isnull);
                l_mcxt_switch(mod_, b, v_oldcontext);

                LLVMBuildStore(b, v_retval, v_resvaluep);
                LLVMBuildStore(b, v_fcinfo_isnull, v_resnullp);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_AGG_STRICT_INPUT_CHECK_ARGS | EEOP_AGG_STRICT_INPUT_CHECK_ARGS_1
            | EEOP_AGG_STRICT_INPUT_CHECK_NULLS => {
                let nargs: c_int = (*op).d.agg_strict_input_check.nargs;
                let args: *mut NullableDatum = (*op).d.agg_strict_input_check.args;
                let nulls: *mut bool = (*op).d.agg_strict_input_check.nulls;
                let jumpnull: c_int;

                let v_argsp: LLVMValueRef;
                let v_nullsp: LLVMValueRef;
                let b_checknulls: *mut LLVMBasicBlockRef;

                Assert!(nargs > 0);

                jumpnull = (*op).d.agg_strict_input_check.jumpnull;
                v_argsp = l_ptr_const(args as *mut c_void, l_ptr(StructNullableDatum));
                v_nullsp = l_ptr_const(nulls as *mut c_void, l_ptr(TypeStorageBool));

                /* create blocks for checking args */
                b_checknulls = palloc(
                    core::mem::size_of::<LLVMBasicBlockRef>() * nargs as usize,
                ) as *mut LLVMBasicBlockRef;
                for argno in 0..nargs {
                    *b_checknulls.add(argno as usize) = l_bb_before_v(
                        *opblocks.add((opno + 1) as usize),
                        c"op.%d.check-null.%d".as_ptr(),
                    );
                }

                LLVMBuildBr(b, *b_checknulls.add(0));

                /* strict function, check for NULL args */
                for argno in 0..nargs {
                    let v_argno: LLVMValueRef = l_int32_const(lc, argno);
                    let v_argisnull: LLVMValueRef;
                    let b_argnotnull: LLVMBasicBlockRef;

                    LLVMPositionBuilderAtEnd(b, *b_checknulls.add(argno as usize));

                    if argno + 1 == nargs {
                        b_argnotnull = *opblocks.add((opno + 1) as usize);
                    } else {
                        b_argnotnull = *b_checknulls.add((argno + 1) as usize);
                    }

                    if opcode == EEOP_AGG_STRICT_INPUT_CHECK_NULLS {
                        v_argisnull =
                            l_load_gep1(b, TypeStorageBool, v_nullsp, v_argno, c"".as_ptr());
                    } else {
                        let mut v_argno_m = v_argno;
                        let v_argn: LLVMValueRef;

                        v_argn = l_gep(
                            b,
                            StructNullableDatum,
                            v_argsp,
                            &mut v_argno_m,
                            1,
                            c"".as_ptr(),
                        );
                        v_argisnull = l_load_struct_gep(
                            b,
                            StructNullableDatum,
                            v_argn,
                            FIELDNO_NULLABLE_DATUM_ISNULL as i32,
                            c"".as_ptr(),
                        );
                    }

                    LLVMBuildCondBr(
                        b,
                        LLVMBuildICmp(b, LLVMIntEQ, v_argisnull, l_sbool_const(true), c"".as_ptr()),
                        *opblocks.add(jumpnull as usize),
                        b_argnotnull,
                    );
                }
            }

            EEOP_AGG_PLAIN_PERGROUP_NULLCHECK => {
                let jumpnull: c_int;
                let v_aggstatep: LLVMValueRef;
                let v_allpergroupsp: LLVMValueRef;
                let v_pergroup_allaggs: LLVMValueRef;
                let v_setoff: LLVMValueRef;

                jumpnull = (*op).d.agg_plain_pergroup_nullcheck.jumpnull;

                /*
                 * pergroup_allaggs = aggstate->all_pergroups
                 * [op->d.agg_plain_pergroup_nullcheck.setoff];
                 */
                v_aggstatep = LLVMBuildBitCast(b, v_parent, l_ptr(StructAggState), c"".as_ptr());

                v_allpergroupsp = l_load_struct_gep(
                    b,
                    StructAggState,
                    v_aggstatep,
                    FIELDNO_AGGSTATE_ALL_PERGROUPS as i32,
                    c"aggstate.all_pergroups".as_ptr(),
                );

                v_setoff = l_int32_const(lc, (*op).d.agg_plain_pergroup_nullcheck.setoff);

                v_pergroup_allaggs = l_load_gep1(
                    b,
                    l_ptr(StructAggStatePerGroupData),
                    v_allpergroupsp,
                    v_setoff,
                    c"".as_ptr(),
                );

                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(
                        b,
                        LLVMIntEQ,
                        LLVMBuildPtrToInt(b, v_pergroup_allaggs, TypeSizeT, c"".as_ptr()),
                        l_sizet_const(0),
                        c"".as_ptr(),
                    ),
                    *opblocks.add(jumpnull as usize),
                    *opblocks.add((opno + 1) as usize),
                );
            }

            EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL | EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL
            | EEOP_AGG_PLAIN_TRANS_BYVAL | EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF
            | EEOP_AGG_PLAIN_TRANS_STRICT_BYREF | EEOP_AGG_PLAIN_TRANS_BYREF => {
                let aggstate: *mut AggState;
                let pertrans: AggStatePerTrans;
                let fcinfo: FunctionCallInfo;

                let v_aggstatep: LLVMValueRef;
                let v_fcinfo: LLVMValueRef;
                let mut v_fcinfo_isnull: LLVMValueRef = null_mut();

                let v_transvaluep: LLVMValueRef;
                let v_transnullp: LLVMValueRef;

                let v_setoff: LLVMValueRef;
                let mut v_transno: LLVMValueRef;

                let mut v_aggcontext: LLVMValueRef;

                let v_allpergroupsp: LLVMValueRef;
                let v_current_setp: LLVMValueRef;
                let v_current_pertransp: LLVMValueRef;
                let v_curaggcontext: LLVMValueRef;

                let v_pertransp: LLVMValueRef;

                let v_pergroupp: LLVMValueRef;

                let v_retval: LLVMValueRef;

                let v_tmpcontext: LLVMValueRef;
                let v_oldcontext: LLVMValueRef;

                aggstate = castNode!(AggState, T_AggState, (*state).parent);
                pertrans = (*op).d.agg_trans.pertrans;

                fcinfo = (*pertrans).transfn_fcinfo;

                v_aggstatep = LLVMBuildBitCast(b, v_parent, l_ptr(StructAggState), c"".as_ptr());
                v_pertransp =
                    l_ptr_const(pertrans as *mut c_void, l_ptr(StructAggStatePerTransData));

                /*
                 * pergroup = &aggstate->all_pergroups
                 * [op->d.agg_trans.setoff] [op->d.agg_trans.transno];
                 */
                v_allpergroupsp = l_load_struct_gep(
                    b,
                    StructAggState,
                    v_aggstatep,
                    FIELDNO_AGGSTATE_ALL_PERGROUPS as i32,
                    c"aggstate.all_pergroups".as_ptr(),
                );
                v_setoff = l_int32_const(lc, (*op).d.agg_trans.setoff);
                v_transno = l_int32_const(lc, (*op).d.agg_trans.transno);
                v_pergroupp = l_gep(
                    b,
                    StructAggStatePerGroupData,
                    l_load_gep1(
                        b,
                        l_ptr(StructAggStatePerGroupData),
                        v_allpergroupsp,
                        v_setoff,
                        c"".as_ptr(),
                    ),
                    &mut v_transno,
                    1,
                    c"".as_ptr(),
                );

                if opcode == EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL
                    || opcode == EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF
                {
                    let v_notransvalue: LLVMValueRef;
                    let b_init: LLVMBasicBlockRef;
                    let b_no_init: LLVMBasicBlockRef;

                    v_notransvalue = l_load_struct_gep(
                        b,
                        StructAggStatePerGroupData,
                        v_pergroupp,
                        FIELDNO_AGGSTATEPERGROUPDATA_NOTRANSVALUE as i32,
                        c"notransvalue".as_ptr(),
                    );

                    b_init =
                        l_bb_before_v(*opblocks.add((opno + 1) as usize), c"op.%d.inittrans".as_ptr());
                    b_no_init = l_bb_before_v(
                        *opblocks.add((opno + 1) as usize),
                        c"op.%d.no_inittrans".as_ptr(),
                    );

                    LLVMBuildCondBr(
                        b,
                        LLVMBuildICmp(b, LLVMIntEQ, v_notransvalue, l_sbool_const(true), c"".as_ptr()),
                        b_init,
                        b_no_init,
                    );

                    /* block to init the transition value if necessary */
                    {
                        let mut params: [LLVMValueRef; 4] = [null_mut(); 4];

                        LLVMPositionBuilderAtEnd(b, b_init);

                        v_aggcontext = l_ptr_const(
                            (*op).d.agg_trans.aggcontext as *mut c_void,
                            l_ptr(StructExprContext),
                        );

                        params[0] = v_aggstatep;
                        params[1] = v_pertransp;
                        params[2] = v_pergroupp;
                        params[3] = v_aggcontext;

                        l_call(
                            b,
                            llvm_pg_var_func_type(c"ExecAggInitGroup".as_ptr()),
                            llvm_pg_func(mod_, c"ExecAggInitGroup".as_ptr()),
                            params.as_mut_ptr(),
                            lengthof!(params),
                            c"".as_ptr(),
                        );

                        LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
                    }

                    LLVMPositionBuilderAtEnd(b, b_no_init);
                }

                if opcode == EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYVAL
                    || opcode == EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF
                    || opcode == EEOP_AGG_PLAIN_TRANS_STRICT_BYVAL
                    || opcode == EEOP_AGG_PLAIN_TRANS_STRICT_BYREF
                {
                    let v_transnull: LLVMValueRef;
                    let b_strictpass: LLVMBasicBlockRef;

                    b_strictpass = l_bb_before_v(
                        *opblocks.add((opno + 1) as usize),
                        c"op.%d.strictpass".as_ptr(),
                    );
                    v_transnull = l_load_struct_gep(
                        b,
                        StructAggStatePerGroupData,
                        v_pergroupp,
                        FIELDNO_AGGSTATEPERGROUPDATA_TRANSVALUEISNULL as i32,
                        c"transnull".as_ptr(),
                    );

                    LLVMBuildCondBr(
                        b,
                        LLVMBuildICmp(b, LLVMIntEQ, v_transnull, l_sbool_const(true), c"".as_ptr()),
                        *opblocks.add((opno + 1) as usize),
                        b_strictpass,
                    );

                    LLVMPositionBuilderAtEnd(b, b_strictpass);
                }

                v_fcinfo = l_ptr_const(fcinfo as *mut c_void, l_ptr(StructFunctionCallInfoData));
                v_aggcontext = l_ptr_const(
                    (*op).d.agg_trans.aggcontext as *mut c_void,
                    l_ptr(StructExprContext),
                );

                v_current_setp = l_struct_gep(
                    b,
                    StructAggState,
                    v_aggstatep,
                    FIELDNO_AGGSTATE_CURRENT_SET as i32,
                    c"aggstate.current_set".as_ptr(),
                );
                v_curaggcontext = l_struct_gep(
                    b,
                    StructAggState,
                    v_aggstatep,
                    FIELDNO_AGGSTATE_CURAGGCONTEXT as i32,
                    c"aggstate.curaggcontext".as_ptr(),
                );
                v_current_pertransp = l_struct_gep(
                    b,
                    StructAggState,
                    v_aggstatep,
                    FIELDNO_AGGSTATE_CURPERTRANS as i32,
                    c"aggstate.curpertrans".as_ptr(),
                );

                /* set aggstate globals */
                LLVMBuildStore(b, v_aggcontext, v_curaggcontext);
                LLVMBuildStore(b, l_int32_const(lc, (*op).d.agg_trans.setno), v_current_setp);
                LLVMBuildStore(b, v_pertransp, v_current_pertransp);

                /* invoke transition function in per-tuple context */
                v_tmpcontext = l_ptr_const(
                    (*(*aggstate).tmpcontext).ecxt_per_tuple_memory as *mut c_void,
                    l_ptr(StructMemoryContextData),
                );
                v_oldcontext = l_mcxt_switch(mod_, b, v_tmpcontext);

                /* store transvalue in fcinfo->args[0] */
                v_transvaluep = l_struct_gep(
                    b,
                    StructAggStatePerGroupData,
                    v_pergroupp,
                    FIELDNO_AGGSTATEPERGROUPDATA_TRANSVALUE as i32,
                    c"transvalue".as_ptr(),
                );
                v_transnullp = l_struct_gep(
                    b,
                    StructAggStatePerGroupData,
                    v_pergroupp,
                    FIELDNO_AGGSTATEPERGROUPDATA_TRANSVALUEISNULL as i32,
                    c"transnullp".as_ptr(),
                );
                LLVMBuildStore(
                    b,
                    l_load(b, TypeSizeT, v_transvaluep, c"transvalue".as_ptr()),
                    l_funcvaluep(b, v_fcinfo, 0),
                );
                LLVMBuildStore(
                    b,
                    l_load(b, TypeStorageBool, v_transnullp, c"transnull".as_ptr()),
                    l_funcnullp(b, v_fcinfo, 0),
                );

                /* and invoke transition function */
                v_retval = BuildV1Call(context, b, mod_, fcinfo, &mut v_fcinfo_isnull);

                /*
                 * For pass-by-ref datatype, must copy the new value into
                 * aggcontext and free the prior transValue.  But if
                 * transfn returned a pointer to its first input, we don't
                 * need to do anything.  Also, if transfn returned a
                 * pointer to a R/W expanded object that is already a
                 * child of the aggcontext, assume we can adopt that value
                 * without copying it.
                 */
                if opcode == EEOP_AGG_PLAIN_TRANS_INIT_STRICT_BYREF
                    || opcode == EEOP_AGG_PLAIN_TRANS_STRICT_BYREF
                    || opcode == EEOP_AGG_PLAIN_TRANS_BYREF
                {
                    let b_call: LLVMBasicBlockRef;
                    let b_nocall: LLVMBasicBlockRef;
                    let v_fn: LLVMValueRef;
                    let v_transvalue: LLVMValueRef;
                    let v_transnull: LLVMValueRef;
                    let v_newval: LLVMValueRef;
                    let mut params: [LLVMValueRef; 6] = [null_mut(); 6];

                    b_call =
                        l_bb_before_v(*opblocks.add((opno + 1) as usize), c"op.%d.transcall".as_ptr());
                    b_nocall = l_bb_before_v(
                        *opblocks.add((opno + 1) as usize),
                        c"op.%d.transnocall".as_ptr(),
                    );

                    v_transvalue = l_load(b, TypeSizeT, v_transvaluep, c"".as_ptr());
                    v_transnull = l_load(b, TypeStorageBool, v_transnullp, c"".as_ptr());

                    /*
                     * DatumGetPointer(newVal) !=
                     * DatumGetPointer(pergroup->transValue))
                     */
                    LLVMBuildCondBr(
                        b,
                        LLVMBuildICmp(b, LLVMIntEQ, v_transvalue, v_retval, c"".as_ptr()),
                        b_nocall,
                        b_call,
                    );

                    /* returned datum not passed datum, reparent */
                    LLVMPositionBuilderAtEnd(b, b_call);

                    params[0] = v_aggstatep;
                    params[1] = v_pertransp;
                    params[2] = v_retval;
                    params[3] = LLVMBuildTrunc(b, v_fcinfo_isnull, TypeParamBool, c"".as_ptr());
                    params[4] = v_transvalue;
                    params[5] = LLVMBuildTrunc(b, v_transnull, TypeParamBool, c"".as_ptr());

                    v_fn = llvm_pg_func(mod_, c"ExecAggCopyTransValue".as_ptr());
                    v_newval = l_call(
                        b,
                        LLVMGetFunctionType(v_fn),
                        v_fn,
                        params.as_mut_ptr(),
                        lengthof!(params),
                        c"".as_ptr(),
                    );

                    /* store trans value */
                    LLVMBuildStore(b, v_newval, v_transvaluep);
                    LLVMBuildStore(b, v_fcinfo_isnull, v_transnullp);

                    l_mcxt_switch(mod_, b, v_oldcontext);
                    LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));

                    /* returned datum passed datum, no need to reparent */
                    LLVMPositionBuilderAtEnd(b, b_nocall);
                }

                /* store trans value */
                LLVMBuildStore(b, v_retval, v_transvaluep);
                LLVMBuildStore(b, v_fcinfo_isnull, v_transnullp);

                l_mcxt_switch(mod_, b, v_oldcontext);

                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_AGG_PRESORTED_DISTINCT_SINGLE => {
                let aggstate: *mut AggState = castNode!(AggState, T_AggState, (*state).parent);
                let pertrans: AggStatePerTrans = (*op).d.agg_presorted_distinctcheck.pertrans;
                let jumpdistinct: c_int = (*op).d.agg_presorted_distinctcheck.jumpdistinct;

                let v_fn: LLVMValueRef =
                    llvm_pg_func(mod_, c"ExecEvalPreOrderedDistinctSingle".as_ptr());
                let mut v_args: [LLVMValueRef; 2] = [null_mut(); 2];
                let mut v_ret: LLVMValueRef;

                v_args[0] = l_ptr_const(aggstate as *mut c_void, l_ptr(StructAggState));
                v_args[1] =
                    l_ptr_const(pertrans as *mut c_void, l_ptr(StructAggStatePerTransData));

                v_ret = l_call(b, LLVMGetFunctionType(v_fn), v_fn, v_args.as_mut_ptr(), 2, c"".as_ptr());
                v_ret = LLVMBuildZExt(b, v_ret, TypeStorageBool, c"".as_ptr());

                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_ret, l_sbool_const(true), c"".as_ptr()),
                    *opblocks.add((opno + 1) as usize),
                    *opblocks.add(jumpdistinct as usize),
                );
            }

            EEOP_AGG_PRESORTED_DISTINCT_MULTI => {
                let aggstate: *mut AggState = castNode!(AggState, T_AggState, (*state).parent);
                let pertrans: AggStatePerTrans = (*op).d.agg_presorted_distinctcheck.pertrans;
                let jumpdistinct: c_int = (*op).d.agg_presorted_distinctcheck.jumpdistinct;

                let v_fn: LLVMValueRef =
                    llvm_pg_func(mod_, c"ExecEvalPreOrderedDistinctMulti".as_ptr());
                let mut v_args: [LLVMValueRef; 2] = [null_mut(); 2];
                let mut v_ret: LLVMValueRef;

                v_args[0] = l_ptr_const(aggstate as *mut c_void, l_ptr(StructAggState));
                v_args[1] =
                    l_ptr_const(pertrans as *mut c_void, l_ptr(StructAggStatePerTransData));

                v_ret = l_call(b, LLVMGetFunctionType(v_fn), v_fn, v_args.as_mut_ptr(), 2, c"".as_ptr());
                v_ret = LLVMBuildZExt(b, v_ret, TypeStorageBool, c"".as_ptr());

                LLVMBuildCondBr(
                    b,
                    LLVMBuildICmp(b, LLVMIntEQ, v_ret, l_sbool_const(true), c"".as_ptr()),
                    *opblocks.add((opno + 1) as usize),
                    *opblocks.add(jumpdistinct as usize),
                );
            }

            EEOP_AGG_ORDERED_TRANS_DATUM => {
                build_EvalXFunc!(
                    b,
                    mod_,
                    c"ExecEvalAggOrderedTransDatum".as_ptr(),
                    v_state,
                    op,
                    v_econtext
                );
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_AGG_ORDERED_TRANS_TUPLE => {
                build_EvalXFunc!(
                    b,
                    mod_,
                    c"ExecEvalAggOrderedTransTuple".as_ptr(),
                    v_state,
                    op,
                    v_econtext
                );
                LLVMBuildBr(b, *opblocks.add((opno + 1) as usize));
            }

            EEOP_LAST => {
                Assert!(false);
            }

            _ => {}
        }
    }

    LLVMDisposeBuilder(b);

    /*
     * Don't immediately emit function, instead do so the first time the
     * expression is actually evaluated. That allows to emit a lot of
     * functions together, avoiding a lot of repeated llvm and memory
     * remapping overhead.
     */
    {
        let cstate: *mut CompiledExprState =
            palloc0(core::mem::size_of::<CompiledExprState>()) as *mut CompiledExprState;

        (*cstate).context = context;
        (*cstate).funcname = funcname;

        (*state).evalfunc = Some(ExecRunCompiledExpr);
        (*state).evalfunc_private = cstate as *mut c_void;
    }

    llvm_leave_fatal_on_oom();

    INSTR_TIME_SET_CURRENT(&mut endtime);
    INSTR_TIME_ACCUM_DIFF(
        &mut (*context).base.instr.generation_counter,
        endtime,
        starttime,
    );

    true
}

/*
 * Run compiled expression.
 *
 * This will only be called the first time a JITed expression is called. We
 * first make sure the expression is still up-to-date, and then get a pointer to
 * the emitted function. The latter can be the first thing that triggers
 * optimizing and emitting all the generated functions.
 */
unsafe fn ExecRunCompiledExpr(
    state: *mut ExprState,
    econtext: *mut ExprContext,
    isNull: *mut bool,
) -> Datum {
    let cstate: *mut CompiledExprState = (*state).evalfunc_private as *mut CompiledExprState;
    // ExprStateEvalFunc = Option<unsafe fn(...) -> Datum>; the inner fn type.
    type ExprStateEvalFuncInner =
        unsafe fn(*mut ExprState, *mut ExprContext, *mut bool) -> Datum;
    let func: ExprStateEvalFuncInner;

    CheckExprStillValid(state, econtext);

    llvm_enter_fatal_on_oom();
    func = core::mem::transmute::<*mut c_void, ExprStateEvalFuncInner>(llvm_get_function(
        (*cstate).context,
        (*cstate).funcname,
    ));
    llvm_leave_fatal_on_oom();
    Assert!((func as *mut c_void).is_null() == false);

    /* remove indirection via this function for future calls */
    (*state).evalfunc = Some(func);

    func(state, econtext, isNull)
}

unsafe fn BuildV1Call(
    context: *mut LLVMJitContext,
    b: LLVMBuilderRef,
    mod_: LLVMModuleRef,
    fcinfo: FunctionCallInfo,
    v_fcinfo_isnull: *mut LLVMValueRef,
) -> LLVMValueRef {
    let lc: LLVMContextRef;
    let v_fn: LLVMValueRef;
    let v_fcinfo_isnullp: LLVMValueRef;
    let v_retval: LLVMValueRef;
    let mut v_fcinfo: LLVMValueRef;

    lc = LLVMGetModuleContext(mod_);

    v_fn = llvm_function_reference(context, b, mod_, fcinfo);

    v_fcinfo = l_ptr_const(fcinfo as *mut c_void, l_ptr(StructFunctionCallInfoData));
    v_fcinfo_isnullp = l_struct_gep(
        b,
        StructFunctionCallInfoData,
        v_fcinfo,
        FIELDNO_FUNCTIONCALLINFODATA_ISNULL as i32,
        c"v_fcinfo_isnull".as_ptr(),
    );
    LLVMBuildStore(b, l_sbool_const(false), v_fcinfo_isnullp);

    v_retval = l_call(
        b,
        LLVMGetFunctionType(AttributeTemplate),
        v_fn,
        &mut v_fcinfo,
        1,
        c"funccall".as_ptr(),
    );

    if !v_fcinfo_isnull.is_null() {
        *v_fcinfo_isnull = l_load(b, TypeStorageBool, v_fcinfo_isnullp, c"".as_ptr());
    }

    /*
     * Add lifetime-end annotation, signaling that writes to memory don't have
     * to be retained (important for inlining potential).
     */
    {
        let v_lifetime: LLVMValueRef = create_LifetimeEnd(mod_);
        let mut params: [LLVMValueRef; 2] = [null_mut(); 2];

        params[0] = l_int64_const(
            lc,
            (core::mem::size_of::<NullableDatum>() as i64) * (*fcinfo).nargs as i64,
        );
        params[1] = l_ptr_const(
            (*fcinfo).args.as_mut_ptr() as *mut c_void,
            l_ptr(LLVMInt8TypeInContext(lc)),
        );
        l_call(
            b,
            LLVMGetFunctionType(v_lifetime),
            v_lifetime,
            params.as_mut_ptr(),
            lengthof!(params),
            c"".as_ptr(),
        );

        params[0] = l_int64_const(lc, core::mem::size_of_val(&(*fcinfo).isnull) as i64);
        params[1] = l_ptr_const(
            &raw mut (*fcinfo).isnull as *mut c_void,
            l_ptr(LLVMInt8TypeInContext(lc)),
        );
        l_call(
            b,
            LLVMGetFunctionType(v_lifetime),
            v_lifetime,
            params.as_mut_ptr(),
            lengthof!(params),
            c"".as_ptr(),
        );
    }

    v_retval
}

/*
 * Implement an expression step by calling the function funcname.
 */
unsafe fn build_EvalXFuncInt(
    b: LLVMBuilderRef,
    mod_: LLVMModuleRef,
    funcname: *const c_char,
    v_state: LLVMValueRef,
    op: *mut ExprEvalStep,
    nargs: c_int,
    v_args: *mut LLVMValueRef,
) -> LLVMValueRef {
    let v_fn: LLVMValueRef = llvm_pg_func(mod_, funcname);
    let params: *mut LLVMValueRef;
    let mut argno: c_int = 0;
    let v_ret: LLVMValueRef;

    /* cheap pre-check as llvm just asserts out */
    if LLVMCountParams(v_fn) != (nargs + 2) as c_uint {
        elog!(
            ERROR,
            "parameter mismatch: {} expects {} passed {}",
            std::ffi::CStr::from_ptr(funcname).to_string_lossy(),
            LLVMCountParams(v_fn),
            nargs + 2
        );
    }

    params = palloc(core::mem::size_of::<LLVMValueRef>() * (2 + nargs) as usize)
        as *mut LLVMValueRef;

    *params.add(argno as usize) = v_state;
    argno += 1;
    *params.add(argno as usize) = l_ptr_const(op as *mut c_void, l_ptr(StructExprEvalStep));
    argno += 1;

    for i in 0..nargs {
        *params.add(argno as usize) = *v_args.add(i as usize);
        argno += 1;
    }

    v_ret = l_call(b, LLVMGetFunctionType(v_fn), v_fn, params, argno, c"".as_ptr());

    pfree(params as *mut c_void);

    v_ret
}

unsafe fn create_LifetimeEnd(mod_: LLVMModuleRef) -> LLVMValueRef {
    let sig: LLVMTypeRef;
    let mut fn_: LLVMValueRef;
    let mut param_types: [LLVMTypeRef; 2] = [null_mut(); 2];
    let lc: LLVMContextRef;

    /* variadic pointer argument */
    let nm: *const c_char = c"llvm.lifetime.end.p0".as_ptr();

    fn_ = LLVMGetNamedFunction(mod_, nm);
    if !fn_.is_null() {
        return fn_;
    }

    lc = LLVMGetModuleContext(mod_);
    param_types[0] = LLVMInt64TypeInContext(lc);
    param_types[1] = l_ptr(LLVMInt8TypeInContext(lc));

    sig = LLVMFunctionType(
        LLVMVoidTypeInContext(lc),
        param_types.as_mut_ptr(),
        lengthof!(param_types) as c_uint,
        0,
    );
    fn_ = LLVMAddFunction(mod_, nm, sig);

    LLVMSetFunctionCallConv(fn_, LLVMCCallConv);

    Assert!(LLVMGetIntrinsicID(fn_) != 0);

    fn_
}
