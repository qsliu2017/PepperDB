//! Translated from PostgreSQL src/include/jit/llvmjit_emit.h
//! LLVM JIT IR-emit helpers. LLVM-C types are out-of-tree, so the LLVM*Ref
//! handles are opaque pointer aliases and all bodies are stubbed. TODO(jit).

// TODO(jit): opaque LLVM-C handle types until the LLVM bindings land.
pub type LLVMValueRef = *mut core::ffi::c_void;
pub type LLVMTypeRef = *mut core::ffi::c_void;
pub type LLVMContextRef = *mut core::ffi::c_void;
pub type LLVMBuilderRef = *mut core::ffi::c_void;
pub type LLVMModuleRef = *mut core::ffi::c_void;
pub type LLVMBasicBlockRef = *mut core::ffi::c_void;

/// `l_ptr_const` - emit a non-LLVM pointer as an LLVM constant. TODO(jit)
pub fn l_ptr_const(_ptr: *mut core::ffi::c_void, _type_: LLVMTypeRef) -> LLVMValueRef {
    unimplemented!()
}

/// `l_ptr` - emit pointer type. TODO(jit)
pub fn l_ptr(_t: LLVMTypeRef) -> LLVMTypeRef {
    unimplemented!()
}

/// `l_int8_const` - emit constant integer. TODO(jit)
pub fn l_int8_const(_lc: LLVMContextRef, _i: i8) -> LLVMValueRef {
    unimplemented!()
}

/// `l_int16_const` - emit constant integer. TODO(jit)
pub fn l_int16_const(_lc: LLVMContextRef, _i: i16) -> LLVMValueRef {
    unimplemented!()
}

/// `l_int32_const` - emit constant integer. TODO(jit)
pub fn l_int32_const(_lc: LLVMContextRef, _i: i32) -> LLVMValueRef {
    unimplemented!()
}

/// `l_int64_const` - emit constant integer. TODO(jit)
pub fn l_int64_const(_lc: LLVMContextRef, _i: i64) -> LLVMValueRef {
    unimplemented!()
}

/// `l_sizet_const` - emit constant size_t integer. TODO(jit)
pub fn l_sizet_const(_i: usize) -> LLVMValueRef {
    unimplemented!()
}

/// `l_sbool_const` - emit storage-bool constant. TODO(jit)
pub fn l_sbool_const(_i: bool) -> LLVMValueRef {
    unimplemented!()
}

/// `l_pbool_const` - emit parameter-bool constant. TODO(jit)
pub fn l_pbool_const(_i: bool) -> LLVMValueRef {
    unimplemented!()
}

/// `l_struct_gep` - struct GEP. TODO(jit)
pub fn l_struct_gep(
    _b: LLVMBuilderRef,
    _t: LLVMTypeRef,
    _v: LLVMValueRef,
    _idx: i32,
    _name: &str,
) -> LLVMValueRef {
    unimplemented!()
}

/// `l_gep` - GEP over indices. TODO(jit)
pub fn l_gep(
    _b: LLVMBuilderRef,
    _t: LLVMTypeRef,
    _v: LLVMValueRef,
    _indices: &mut [LLVMValueRef],
    _name: &str,
) -> LLVMValueRef {
    unimplemented!()
}

/// `l_load` - load. TODO(jit)
pub fn l_load(_b: LLVMBuilderRef, _t: LLVMTypeRef, _v: LLVMValueRef, _name: &str) -> LLVMValueRef {
    unimplemented!()
}

/// `l_call` - call. TODO(jit)
pub fn l_call(
    _b: LLVMBuilderRef,
    _t: LLVMTypeRef,
    _fn_: LLVMValueRef,
    _args: &mut [LLVMValueRef],
    _name: &str,
) -> LLVMValueRef {
    unimplemented!()
}

/// `l_load_struct_gep` - load a member from a struct. TODO(jit)
pub fn l_load_struct_gep(
    _b: LLVMBuilderRef,
    _t: LLVMTypeRef,
    _v: LLVMValueRef,
    _idx: i32,
    _name: &str,
) -> LLVMValueRef {
    unimplemented!()
}

/// `l_load_gep1` - load after one index operation. TODO(jit)
pub fn l_load_gep1(
    _b: LLVMBuilderRef,
    _t: LLVMTypeRef,
    _v: LLVMValueRef,
    _idx: LLVMValueRef,
    _name: &str,
) -> LLVMValueRef {
    unimplemented!()
}

/// `l_bb_before_v` - insert a basic block before `r` (name from format). TODO(jit)
pub fn l_bb_before_v(_r: LLVMBasicBlockRef, _fmt: &str) -> LLVMBasicBlockRef {
    unimplemented!()
}

/// `l_bb_append_v` - append a basic block to `f` (name from format). TODO(jit)
pub fn l_bb_append_v(_f: LLVMValueRef, _fmt: &str) -> LLVMBasicBlockRef {
    unimplemented!()
}

/// `l_callsite_ro` - mark a callsite readonly. TODO(jit)
pub fn l_callsite_ro(_f: LLVMValueRef) {
    unimplemented!()
}

/// `l_callsite_alwaysinline` - mark a callsite alwaysinline. TODO(jit)
pub fn l_callsite_alwaysinline(_f: LLVMValueRef) {
    unimplemented!()
}

/// `l_mcxt_switch` - emit code to switch memory context. TODO(jit)
pub fn l_mcxt_switch(_mod_: LLVMModuleRef, _b: LLVMBuilderRef, _nc: LLVMValueRef) -> LLVMValueRef {
    unimplemented!()
}

/// `l_funcnullp` - pointer to argno'th argument nullness. TODO(jit)
pub fn l_funcnullp(_b: LLVMBuilderRef, _v_fcinfo: LLVMValueRef, _argno: usize) -> LLVMValueRef {
    unimplemented!()
}

/// `l_funcvaluep` - pointer to argno'th argument datum. TODO(jit)
pub fn l_funcvaluep(_b: LLVMBuilderRef, _v_fcinfo: LLVMValueRef, _argno: usize) -> LLVMValueRef {
    unimplemented!()
}

/// `l_funcnull` - argno'th argument nullness. TODO(jit)
pub fn l_funcnull(_b: LLVMBuilderRef, _v_fcinfo: LLVMValueRef, _argno: usize) -> LLVMValueRef {
    unimplemented!()
}

/// `l_funcvalue` - argno'th argument datum. TODO(jit)
pub fn l_funcvalue(_b: LLVMBuilderRef, _v_fcinfo: LLVMValueRef, _argno: usize) -> LLVMValueRef {
    unimplemented!()
}
