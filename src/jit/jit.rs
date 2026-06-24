//! Translated from PostgreSQL src/include/jit/jit.h
//
// Provider-independent JIT infrastructure. JIT is largely out-of-scope for the
// port: types/flags are translated, functions stubbed. In-memory types only
// (no on-disk layout).

use crate::portability::instr_time::InstrTime;

use bitflags::bitflags;

bitflags! {
    /// Flags determining what kind of JIT operations to perform (PGJIT_*).
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct PgJit: i32 {
        const PERFORM = 1 << 0;
        const OPT3    = 1 << 1;
        const INLINE  = 1 << 2;
        const EXPR    = 1 << 3;
        const DEFORM  = 1 << 4;
    }
}

impl PgJit {
    /// PGJIT_NONE
    pub const NONE: Self = Self::empty();
}

#[derive(Debug, Clone, Copy, Default)]
pub struct JitInstrumentation {
    pub created_functions: usize,
    pub generation_counter: InstrTime,
    pub deform_counter: InstrTime,
    pub inlining_counter: InstrTime,
    pub optimization_counter: InstrTime,
    pub emission_counter: InstrTime,
}

/// Accumulated JIT instrumentation across workers. C uses a DSM struct with a
/// FLEXIBLE_ARRAY_MEMBER; under single-process that becomes an owned Vec.
pub struct SharedJitInstrumentation {
    pub num_workers: i32,
    pub jit_instr: Vec<JitInstrumentation>,
}

pub struct JitContext {
    pub flags: PgJit,
    pub instr: JitInstrumentation,
}

// ExprState forward ref: ExprState's real definition is in nodes/execnodes.h;
// the header forward-declares `struct ExprState`.
use crate::nodes::params::ExprState;

// JitProviderCallbacks is a runtime-pluggable vtable (open/extension case): the
// provider is a loadable module chosen at run time, so per routine-struct.md
// appendix B it stays a struct of `fn` pointers mirroring the C vtable (rather
// than a trait), and `_PG_jit_provider_init` fills a caller-provided instance.
pub type JitProviderResetAfterErrorCB = fn();
pub type JitProviderReleaseContextCB = fn(context: &mut JitContext);
pub type JitProviderCompileExprCB = fn(state: &mut ExprState) -> bool;

pub struct JitProviderCallbacks {
    pub reset_after_error: JitProviderResetAfterErrorCB,
    pub release_context: JitProviderReleaseContextCB,
    pub compile_expr: JitProviderCompileExprCB,
}

// C: `void _PG_jit_provider_init(JitProviderCallbacks *cb)` -- provider entry
// point that fills the caller-provided vtable. Stubbed.
pub fn _PG_jit_provider_init(_cb: &mut JitProviderCallbacks) {
    unimplemented!()
}

// GUCs. TODO(global): move into a Session/GUC context.
pub static mut jit_enabled: bool = true;
pub static mut jit_provider: Option<String> = None;
pub static mut jit_debugging_support: bool = false;
pub static mut jit_dump_bitcode: bool = false;
pub static mut jit_expressions: bool = true;
pub static mut jit_profiling_support: bool = false;
pub static mut jit_tuple_deforming: bool = true;
pub static mut jit_above_cost: f64 = 100000.0;
pub static mut jit_inline_above_cost: f64 = 500000.0;
pub static mut jit_optimize_above_cost: f64 = 500000.0;

pub fn jit_reset_after_error() {
    unimplemented!()
}

pub fn jit_release_context(_context: &mut JitContext) {
    unimplemented!()
}

/// Attempt to JIT-compile an expression. May decline (returns false).
pub fn jit_compile_expr(_state: &mut ExprState) -> bool {
    unimplemented!()
}

pub fn InstrJitAgg(_dst: &mut JitInstrumentation, _add: &JitInstrumentation) {
    unimplemented!()
}
