//! Translated from PostgreSQL src/include/jit/llvmjit.h
//
// LLVM JIT provider glue. JIT is OUT OF SCOPE for the port; the whole header is
// guarded by `#ifdef USE_LLVM`. We keep this mostly as a tombstone: the LLVM C
// API types (LLVMContextRef/LLVMModuleRef/LLVMTypeRef/LLVMValueRef/...) have no
// Rust equivalent here, so the many `extern PGDLLIMPORT LLVMTypeRef Type*` /
// `Struct*` globals and the LLVM-typed functions are NOT translated. Only the
// public context struct shell and the few non-LLVM-typed entry points are stubbed
// for signature completeness.

use crate::jit::jit::JitContext;

// The LLVM context owns LLVM-C handles (llvm_context/module) that don't exist in
// Rust; modeled as a unit-ish shell. The `handles` List -> Vec when implemented.
// Not on-disk; in-memory only.
pub struct LLVMJitContext {
    pub base: JitContext,
    // resowner: ResourceOwner -> Rust ownership/RAII (dropped); omitted.
    pub module_generation: usize,
    // llvm_context: LLVMContextRef -- omitted (LLVM C handle, out of scope).
    // module: LLVMModuleRef -- omitted (LLVM C handle, out of scope).
    pub compiled: bool,
    pub counter: i32,
    // handles: List of Orc code handles -> Vec when JIT is implemented.
}

// --- Non-LLVM-typed entry points (signatures kept; bodies stubbed) ---

pub fn llvm_enter_fatal_on_oom() {
    unimplemented!()
}
pub fn llvm_leave_fatal_on_oom() {
    unimplemented!()
}
pub fn llvm_in_fatal_on_oom() -> bool {
    unimplemented!()
}
pub fn llvm_reset_after_error() {
    unimplemented!()
}
pub fn llvm_assert_in_fatal_section() {
    unimplemented!()
}

pub fn llvm_create_context(_jit_flags: i32) -> LLVMJitContext {
    unimplemented!()
}

/// `char *llvm_expand_funcname(LLVMJitContext *, const char *basename)`.
pub fn llvm_expand_funcname(_context: &mut LLVMJitContext, _basename: &str) -> String {
    unimplemented!()
}

/// `void *llvm_get_function(...)` -- opaque code pointer; out of scope.
pub fn llvm_get_function(_context: &mut LLVMJitContext, _funcname: &str) {
    unimplemented!()
}

/// `(modname, funcname)` split of a symbol name (C wrote two out-params).
pub fn llvm_split_symbol_name(_name: &str) -> (String, String) {
    unimplemented!()
}

pub fn llvm_inline_reset_caches() {
    unimplemented!()
}

// Omitted (require LLVM-C types, out of scope):
//   TypeParamBool/TypePGFunction/TypeSizeT/TypeStorageBool and all Struct* /
//   AttributeTemplate / *Template globals;
//   llvm_mutable_module, llvm_pg_var_type, llvm_pg_var_func_type, llvm_pg_func,
//   llvm_copy_attributes, llvm_function_reference, llvm_inline,
//   llvm_compile_expr, slot_compile_deform, LLVMGetFunctionReturnType,
//   LLVMGetFunctionType, LLVMOrcCreateRTDyldObjectLinkingLayerWith...().
