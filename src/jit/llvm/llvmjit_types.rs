//! Type and function references for JIT inlining (jit/llvm/llvmjit_types.c).
//!
//! NB: This file will not be linked into the server, it's just converted to
//! bitcode (in C). The declarations exist solely so clang/LLVM emit the type
//! and function signatures the JIT needs for inlining. 1:1 translation; the
//! actual LLVM bitcode generation is part of the (unported) JIT backend, so the
//! type-bearing globals are modeled as zeroed markers and the referenced-
//! function table is gated off until the JIT layer is ported.

#![allow(non_upper_case_globals)]
use crate::prelude::*;
use crate::utils::fmgr::FunctionCallInfo;

/*
 * List of types needed for JITing. These have to be non-static, otherwise
 * clang/LLVM will omit them.  As this file will never be linked into anything,
 * that's harmless.  Modeled here as zeroed type markers.
 */
pub static mut TypeSizeT: usize = 0;
pub static mut TypeStorageBool: bool = false;

/*
 * To determine which attributes functions need to have to be compatible for
 * inlining, we copy the attributes of this template function.
 */
pub unsafe fn AttributeTemplate(fcinfo: FunctionCallInfo) -> Datum {
    // AssertVariableIsOfType(&AttributeTemplate, PGFunction);
    let _ = fcinfo;
    crate::PG_RETURN_NULL!(fcinfo)
}

/*
 * More "templates" giving examples of function types corresponding to function
 * pointer types used by the expression evaluator.
 */
pub unsafe fn ExecEvalSubroutineTemplate(
    _state: *mut core::ffi::c_void,   // ExprState *
    _op: *mut core::ffi::c_void,      // struct ExprEvalStep *
    _econtext: *mut core::ffi::c_void, // ExprContext *
) {
    // AssertVariableIsOfType(&ExecEvalSubroutineTemplate, ExecEvalSubroutine);
}

pub unsafe fn ExecEvalBoolSubroutineTemplate(
    _state: *mut core::ffi::c_void,
    _op: *mut core::ffi::c_void,
    _econtext: *mut core::ffi::c_void,
) -> bool {
    // AssertVariableIsOfType(&ExecEvalBoolSubroutineTemplate, ExecEvalBoolSubroutine);
    false
}

/*
 * Clang represents bool returned by functions differently (i1) than stored
 * ones (i8); this template lets the JIT determine the width of a returned int.
 */
pub unsafe fn FunctionReturningBool() -> bool {
    false
}

/*
 * To force signatures of functions used during JITing to be present, the C
 * file references them in a non-static `referenced_functions[]` array. That
 * requires the full ExecEval* family (execExprInterp.c) plus a few helpers to
 * be in scope; it is reconstructed here when the JIT backend is ported.
 *
 * TODO(pg-port): re-enable once the LLVM JIT backend (llvmjit.c/llvmjit_expr.c)
 * is wired, providing every referenced ExecEval* symbol:
 *   ExecAggInitGroup, ExecAggCopyTransValue, ExecEvalPreOrderedDistinctSingle/Multi,
 *   ExecEvalAggOrderedTransDatum/Tuple, ExecEvalArrayCoerce, ExecEvalArrayExpr,
 *   ExecEvalConstraintCheck, ExecEvalConstraintNotNull, ExecEvalConvertRowtype,
 *   ExecEvalCurrentOfExpr, ExecEvalFieldSelect, ExecEvalFieldStoreDeForm/Form,
 *   ExecEvalFuncExprFusage, ExecEvalFuncExprStrictFusage, ExecEvalGroupingFunc,
 *   ExecEvalMergeSupportFunc, ExecEvalMinMax, ExecEvalNextValueExpr,
 *   ExecEvalParamExec, ExecEvalParamExtern, ExecEvalParamSet, ExecEvalRow,
 *   ExecEvalRowNotNull, ExecEvalRowNull, ExecEvalCoerceViaIOSafe,
 *   ExecEvalSQLValueFunction, ExecEvalScalarArrayOp, ExecEvalHashedScalarArrayOp,
 *   ExecEvalSubPlan, ExecEvalSysVar, ExecEvalWholeRowVar, ExecEvalXmlExpr,
 *   ExecEvalJsonConstructor, ExecEvalJsonIsPredicate, ExecEvalJsonCoercion,
 *   ExecEvalJsonCoercionFinish, ExecEvalJsonExprPath,
 *   MakeExpandedObjectReadOnlyInternal, slot_getmissingattrs,
 *   slot_getsomeattrs_int, strlen, varsize_any, ExecInterpExprStillValid.
 */
#[cfg(any())]
pub static referenced_functions: [*mut core::ffi::c_void; 0] = [];
