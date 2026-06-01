//! Generate code for deforming a heap tuple.
//!
//! Translation of postgres/src/backend/jit/llvm/llvmjit_deform.c
//!
//! This gains performance benefits over unJITed deforming from compile-time
//! knowledge of the tuple descriptor. Fixed column widths, NOT NULLness, etc
//! can be taken advantage of.
//!
//! Portions Copyright (c) 1996-2025, PostgreSQL Global Development Group
//! Portions Copyright (c) 1994, Regents of the University of California

#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_variables)]
#![allow(unused_assignments)]

use std::ffi::{c_char, c_int, c_void};

use crate::c::{Size, TYPEALIGN};

// access/htup_details.h
use crate::access::htup_details::{
    FIELDNO_HEAPTUPLEDATA_DATA, FIELDNO_HEAPTUPLEHEADERDATA_BITS,
    FIELDNO_HEAPTUPLEHEADERDATA_HOFF, FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK,
    FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK2, HEAP_HASNULL, HEAP_NATTS_MASK,
};

// access/tupdesc.h / access/tupdesc_details.h
use crate::access::common::tupdesc::{
    CompactAttribute, TupleDesc, TupleDescCompactAttr, ATTNULLABLE_VALID,
};

// executor/tuptable.h
use crate::executor::tuptable::{
    TupleTableSlotOps, FIELDNO_HEAPTUPLETABLESLOT_OFF, FIELDNO_HEAPTUPLETABLESLOT_TUPLE,
    FIELDNO_MINIMALTUPLETABLESLOT_OFF, FIELDNO_MINIMALTUPLETABLESLOT_TUPLE,
    FIELDNO_TUPLETABLESLOT_FLAGS, FIELDNO_TUPLETABLESLOT_ISNULL,
    FIELDNO_TUPLETABLESLOT_NVALID, FIELDNO_TUPLETABLESLOT_VALUES, TTS_FLAG_SLOW,
};
use crate::executor::execTuples::{
    TTSOpsBufferHeapTuple, TTSOpsHeapTuple, TTSOpsMinimalTuple, TTSOpsVirtual,
};

// jit/llvmjit.h
use crate::jit::llvm::llvmjit::{
    llvm_copy_attributes, llvm_expand_funcname, llvm_mutable_module, llvm_pg_func,
    llvm_pg_var_func_type, AttributeTemplate, LLVMJitContext, StructHeapTupleData,
    StructHeapTupleHeaderData, StructHeapTupleTableSlot, StructMinimalTupleTableSlot,
    StructTupleTableSlot, TypeSizeT, TypeStorageBool,
};

// jit/llvmjit_emit.h
use crate::jit::llvmjit_emit::{
    l_bb_append_v, l_call, l_callsite_alwaysinline, l_callsite_ro, l_gep, l_int16_const,
    l_int32_const, l_int8_const, l_load, l_load_gep1, l_load_struct_gep, l_ptr,
    l_sizet_const, l_struct_gep, LLVMBasicBlockRef, LLVMBuilderRef, LLVMContextRef,
    LLVMModuleRef, LLVMTypeRef, LLVMValueRef,
};

use crate::utils::palloc::palloc;

// pg_config.h
const MAXIMUM_ALIGNOF: c_int = crate::pg_config::MAXIMUM_ALIGNOF as c_int;

// ---------------------------------------------------------------------------
// LLVM-C bindings (from <llvm-c/Core.h>). No Rust LLVM-C binding module exists
// in this tree; the opaque LLVM*Ref aliases come from llvmjit_emit. Declare the
// subset of the C API used directly here, matching the signatures used by the
// sibling llvmjit.rs module.
// TODO(pg-port): dedup with a real LLVM-C binding module when one exists.
// ---------------------------------------------------------------------------

// LLVMIntPredicate
const LLVMIntEQ: c_int = 32;
const LLVMIntNE: c_int = 33;
const LLVMIntUGE: c_int = 35;
const LLVMIntULT: c_int = 36;

// LLVMLinkage
const LLVMInternalLinkage: c_int = 8;

extern "C" {
    fn LLVMFunctionType(
        ReturnType: LLVMTypeRef,
        ParamTypes: *mut LLVMTypeRef,
        ParamCount: u32,
        IsVarArg: c_int,
    ) -> LLVMTypeRef;
    fn LLVMVoidTypeInContext(C: LLVMContextRef) -> LLVMTypeRef;
    fn LLVMInt8TypeInContext(C: LLVMContextRef) -> LLVMTypeRef;
    fn LLVMInt32TypeInContext(C: LLVMContextRef) -> LLVMTypeRef;
    fn LLVMInt16TypeInContext(C: LLVMContextRef) -> LLVMTypeRef;
    fn LLVMIntTypeInContext(C: LLVMContextRef, NumBits: u32) -> LLVMTypeRef;
    fn LLVMPointerType(ElementType: LLVMTypeRef, AddressSpace: u32) -> LLVMTypeRef;

    fn LLVMGetModuleContext(M: LLVMModuleRef) -> LLVMContextRef;
    fn LLVMAddFunction(M: LLVMModuleRef, Name: *const c_char, FunctionTy: LLVMTypeRef)
        -> LLVMValueRef;
    fn LLVMSetLinkage(Global: LLVMValueRef, Linkage: c_int);
    fn LLVMSetParamAlignment(Arg: LLVMValueRef, Align: u32);
    fn LLVMGetParam(Fn: LLVMValueRef, Index: u32) -> LLVMValueRef;
    fn LLVMGetFunctionType(Fn: LLVMValueRef) -> LLVMTypeRef;

    fn LLVMAppendBasicBlockInContext(
        C: LLVMContextRef,
        Fn: LLVMValueRef,
        Name: *const c_char,
    ) -> LLVMBasicBlockRef;
    fn LLVMCreateBuilderInContext(C: LLVMContextRef) -> LLVMBuilderRef;
    fn LLVMPositionBuilderAtEnd(Builder: LLVMBuilderRef, Block: LLVMBasicBlockRef);
    fn LLVMDisposeBuilder(Builder: LLVMBuilderRef);

    fn LLVMBuildAlloca(B: LLVMBuilderRef, Ty: LLVMTypeRef, Name: *const c_char) -> LLVMValueRef;
    fn LLVMBuildStore(B: LLVMBuilderRef, Val: LLVMValueRef, Ptr: LLVMValueRef) -> LLVMValueRef;
    fn LLVMBuildBitCast(
        B: LLVMBuilderRef,
        Val: LLVMValueRef,
        DestTy: LLVMTypeRef,
        Name: *const c_char,
    ) -> LLVMValueRef;
    fn LLVMBuildPointerCast(
        B: LLVMBuilderRef,
        Val: LLVMValueRef,
        DestTy: LLVMTypeRef,
        Name: *const c_char,
    ) -> LLVMValueRef;
    fn LLVMBuildPtrToInt(
        B: LLVMBuilderRef,
        Val: LLVMValueRef,
        DestTy: LLVMTypeRef,
        Name: *const c_char,
    ) -> LLVMValueRef;
    fn LLVMBuildZExt(
        B: LLVMBuilderRef,
        Val: LLVMValueRef,
        DestTy: LLVMTypeRef,
        Name: *const c_char,
    ) -> LLVMValueRef;
    fn LLVMBuildSExt(
        B: LLVMBuilderRef,
        Val: LLVMValueRef,
        DestTy: LLVMTypeRef,
        Name: *const c_char,
    ) -> LLVMValueRef;
    fn LLVMBuildTrunc(
        B: LLVMBuilderRef,
        Val: LLVMValueRef,
        DestTy: LLVMTypeRef,
        Name: *const c_char,
    ) -> LLVMValueRef;
    fn LLVMBuildAnd(
        B: LLVMBuilderRef,
        LHS: LLVMValueRef,
        RHS: LLVMValueRef,
        Name: *const c_char,
    ) -> LLVMValueRef;
    fn LLVMBuildOr(
        B: LLVMBuilderRef,
        LHS: LLVMValueRef,
        RHS: LLVMValueRef,
        Name: *const c_char,
    ) -> LLVMValueRef;
    fn LLVMBuildAdd(
        B: LLVMBuilderRef,
        LHS: LLVMValueRef,
        RHS: LLVMValueRef,
        Name: *const c_char,
    ) -> LLVMValueRef;
    fn LLVMBuildICmp(
        B: LLVMBuilderRef,
        Op: c_int,
        LHS: LLVMValueRef,
        RHS: LLVMValueRef,
        Name: *const c_char,
    ) -> LLVMValueRef;

    fn LLVMBuildBr(B: LLVMBuilderRef, Dest: LLVMBasicBlockRef) -> LLVMValueRef;
    fn LLVMBuildCondBr(
        B: LLVMBuilderRef,
        If: LLVMValueRef,
        Then: LLVMBasicBlockRef,
        Else: LLVMBasicBlockRef,
    ) -> LLVMValueRef;
    fn LLVMBuildSwitch(
        B: LLVMBuilderRef,
        V: LLVMValueRef,
        Else: LLVMBasicBlockRef,
        NumCases: u32,
    ) -> LLVMValueRef;
    fn LLVMAddCase(Switch: LLVMValueRef, OnVal: LLVMValueRef, Dest: LLVMBasicBlockRef);
    fn LLVMBuildUnreachable(B: LLVMBuilderRef) -> LLVMValueRef;
    fn LLVMBuildRetVoid(B: LLVMBuilderRef) -> LLVMValueRef;
}

// ---------------------------------------------------------------------------
// Stubs for functions defined in other .c files (faithful signatures).
// ---------------------------------------------------------------------------

// executor/execTuples.c - memset tts_isnull for missing columns.
unsafe fn slot_getmissingattrs() {
    unimplemented!() // TODO(pg-port): executor/execTuples.c (referenced by symbol only)
}

/// pg_unreachable() - declares unreachable code (cf. c.h).
#[inline]
unsafe fn pg_unreachable() -> ! {
    unreachable!()
}

/*
 * Create a function that deforms a tuple of type desc up to natts columns.
 */
pub unsafe fn slot_compile_deform(
    context: *mut LLVMJitContext,
    desc: TupleDesc,
    ops: *const TupleTableSlotOps,
    natts: c_int,
) -> LLVMValueRef {
    let funcname: *mut c_char;

    let mod_: LLVMModuleRef;
    let lc: LLVMContextRef;
    let b: LLVMBuilderRef;

    let deform_sig: LLVMTypeRef;
    let v_deform_fn: LLVMValueRef;

    let b_entry: LLVMBasicBlockRef;
    let b_adjust_unavail_cols: LLVMBasicBlockRef;
    let b_find_start: LLVMBasicBlockRef;

    let b_out: LLVMBasicBlockRef;
    let b_dead: LLVMBasicBlockRef;
    let attcheckattnoblocks: *mut LLVMBasicBlockRef;
    let attstartblocks: *mut LLVMBasicBlockRef;
    let attisnullblocks: *mut LLVMBasicBlockRef;
    let attcheckalignblocks: *mut LLVMBasicBlockRef;
    let attalignblocks: *mut LLVMBasicBlockRef;
    let attstoreblocks: *mut LLVMBasicBlockRef;

    let v_offp: LLVMValueRef;

    let v_tupdata_base: LLVMValueRef;
    let v_tts_values: LLVMValueRef;
    let v_tts_nulls: LLVMValueRef;
    let v_slotoffp: LLVMValueRef;
    let v_flagsp: LLVMValueRef;
    let v_nvalidp: LLVMValueRef;
    let v_nvalid: LLVMValueRef;
    let v_maxatt: LLVMValueRef;

    let v_slot: LLVMValueRef;

    let v_tupleheaderp: LLVMValueRef;
    let v_tuplep: LLVMValueRef;
    let v_infomask1: LLVMValueRef;
    let v_infomask2: LLVMValueRef;
    let v_bits: LLVMValueRef;

    let v_hoff: LLVMValueRef;

    let v_hasnulls: LLVMValueRef;

    /* last column (0 indexed) guaranteed to exist */
    let mut guaranteed_column_number: c_int = -1;

    /* current known alignment */
    let mut known_alignment: c_int = 0;

    /* if true, known_alignment describes definite offset of column */
    let mut attguaranteedalign: bool = true;

    let mut attnum: c_int;

    /* virtual tuples never need deforming, so don't generate code */
    if ops == &TTSOpsVirtual as *const TupleTableSlotOps {
        return std::ptr::null_mut();
    }

    /* decline to JIT for slot types we don't know to handle */
    if ops != &TTSOpsHeapTuple as *const TupleTableSlotOps
        && ops != &TTSOpsBufferHeapTuple as *const TupleTableSlotOps
        && ops != &TTSOpsMinimalTuple as *const TupleTableSlotOps
    {
        return std::ptr::null_mut();
    }

    mod_ = llvm_mutable_module(context);
    lc = LLVMGetModuleContext(mod_);

    funcname = llvm_expand_funcname(context, c"deform".as_ptr());

    /*
     * Check which columns have to exist, so we don't have to check the row's
     * natts unnecessarily.
     */
    attnum = 0;
    while attnum < (*desc).natts {
        let att: *mut CompactAttribute = TupleDescCompactAttr(desc, attnum);

        /*
         * If the column is declared NOT NULL then it must be present in every
         * tuple, unless there's a "missing" entry that could provide a
         * non-NULL value for it. That in turn guarantees that the NULL bitmap
         * - if there are any NULLable columns - is at least long enough to
         * cover columns up to attnum.
         *
         * Be paranoid and also check !attisdropped, even though the
         * combination of attisdropped && attnotnull combination shouldn't
         * exist.
         */
        if (*att).attnullability == ATTNULLABLE_VALID
            && !(*att).atthasmissing
            && !(*att).attisdropped
        {
            guaranteed_column_number = attnum;
        }
        attnum += 1;
    }

    /* Create the signature and function */
    {
        let mut param_types: [LLVMTypeRef; 1] = [std::ptr::null_mut(); 1];

        param_types[0] = l_ptr(StructTupleTableSlot);

        deform_sig = LLVMFunctionType(
            LLVMVoidTypeInContext(lc),
            param_types.as_mut_ptr(),
            param_types.len() as u32,
            0,
        );
    }
    v_deform_fn = LLVMAddFunction(mod_, funcname, deform_sig);
    LLVMSetLinkage(v_deform_fn, LLVMInternalLinkage);
    LLVMSetParamAlignment(LLVMGetParam(v_deform_fn, 0), MAXIMUM_ALIGNOF as u32);
    llvm_copy_attributes(AttributeTemplate, v_deform_fn);

    b_entry = LLVMAppendBasicBlockInContext(lc, v_deform_fn, c"entry".as_ptr());
    b_adjust_unavail_cols =
        LLVMAppendBasicBlockInContext(lc, v_deform_fn, c"adjust_unavail_cols".as_ptr());
    b_find_start = LLVMAppendBasicBlockInContext(lc, v_deform_fn, c"find_startblock".as_ptr());
    b_out = LLVMAppendBasicBlockInContext(lc, v_deform_fn, c"outblock".as_ptr());
    b_dead = LLVMAppendBasicBlockInContext(lc, v_deform_fn, c"deadblock".as_ptr());

    b = LLVMCreateBuilderInContext(lc);

    attcheckattnoblocks =
        palloc(std::mem::size_of::<LLVMBasicBlockRef>() * natts as usize) as *mut LLVMBasicBlockRef;
    attstartblocks =
        palloc(std::mem::size_of::<LLVMBasicBlockRef>() * natts as usize) as *mut LLVMBasicBlockRef;
    attisnullblocks =
        palloc(std::mem::size_of::<LLVMBasicBlockRef>() * natts as usize) as *mut LLVMBasicBlockRef;
    attcheckalignblocks =
        palloc(std::mem::size_of::<LLVMBasicBlockRef>() * natts as usize) as *mut LLVMBasicBlockRef;
    attalignblocks =
        palloc(std::mem::size_of::<LLVMBasicBlockRef>() * natts as usize) as *mut LLVMBasicBlockRef;
    attstoreblocks =
        palloc(std::mem::size_of::<LLVMBasicBlockRef>() * natts as usize) as *mut LLVMBasicBlockRef;

    known_alignment = 0;

    LLVMPositionBuilderAtEnd(b, b_entry);

    /* perform allocas first, llvm only converts those to registers */
    v_offp = LLVMBuildAlloca(b, TypeSizeT, c"v_offp".as_ptr());

    v_slot = LLVMGetParam(v_deform_fn, 0);

    v_tts_values = l_load_struct_gep(
        b,
        StructTupleTableSlot,
        v_slot,
        FIELDNO_TUPLETABLESLOT_VALUES as c_int,
        c"tts_values".as_ptr(),
    );
    v_tts_nulls = l_load_struct_gep(
        b,
        StructTupleTableSlot,
        v_slot,
        FIELDNO_TUPLETABLESLOT_ISNULL as c_int,
        c"tts_ISNULL".as_ptr(),
    );
    v_flagsp = l_struct_gep(
        b,
        StructTupleTableSlot,
        v_slot,
        FIELDNO_TUPLETABLESLOT_FLAGS as c_int,
        c"".as_ptr(),
    );
    v_nvalidp = l_struct_gep(
        b,
        StructTupleTableSlot,
        v_slot,
        FIELDNO_TUPLETABLESLOT_NVALID as c_int,
        c"".as_ptr(),
    );

    if ops == &TTSOpsHeapTuple as *const TupleTableSlotOps
        || ops == &TTSOpsBufferHeapTuple as *const TupleTableSlotOps
    {
        let v_heapslot: LLVMValueRef;

        v_heapslot = LLVMBuildBitCast(
            b,
            v_slot,
            l_ptr(StructHeapTupleTableSlot),
            c"heapslot".as_ptr(),
        );
        v_slotoffp = l_struct_gep(
            b,
            StructHeapTupleTableSlot,
            v_heapslot,
            FIELDNO_HEAPTUPLETABLESLOT_OFF as c_int,
            c"".as_ptr(),
        );
        v_tupleheaderp = l_load_struct_gep(
            b,
            StructHeapTupleTableSlot,
            v_heapslot,
            FIELDNO_HEAPTUPLETABLESLOT_TUPLE as c_int,
            c"tupleheader".as_ptr(),
        );
    } else if ops == &TTSOpsMinimalTuple as *const TupleTableSlotOps {
        let v_minimalslot: LLVMValueRef;

        v_minimalslot = LLVMBuildBitCast(
            b,
            v_slot,
            l_ptr(StructMinimalTupleTableSlot),
            c"minimalslot".as_ptr(),
        );
        v_slotoffp = l_struct_gep(
            b,
            StructMinimalTupleTableSlot,
            v_minimalslot,
            FIELDNO_MINIMALTUPLETABLESLOT_OFF as c_int,
            c"".as_ptr(),
        );
        v_tupleheaderp = l_load_struct_gep(
            b,
            StructMinimalTupleTableSlot,
            v_minimalslot,
            FIELDNO_MINIMALTUPLETABLESLOT_TUPLE as c_int,
            c"tupleheader".as_ptr(),
        );
    } else {
        /* should've returned at the start of the function */
        pg_unreachable();
    }

    v_tuplep = l_load_struct_gep(
        b,
        StructHeapTupleData,
        v_tupleheaderp,
        FIELDNO_HEAPTUPLEDATA_DATA as c_int,
        c"tuple".as_ptr(),
    );
    v_bits = LLVMBuildBitCast(
        b,
        l_struct_gep(
            b,
            StructHeapTupleHeaderData,
            v_tuplep,
            FIELDNO_HEAPTUPLEHEADERDATA_BITS as c_int,
            c"".as_ptr(),
        ),
        l_ptr(LLVMInt8TypeInContext(lc)),
        c"t_bits".as_ptr(),
    );
    v_infomask1 = l_load_struct_gep(
        b,
        StructHeapTupleHeaderData,
        v_tuplep,
        FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK as c_int,
        c"infomask1".as_ptr(),
    );
    v_infomask2 = l_load_struct_gep(
        b,
        StructHeapTupleHeaderData,
        v_tuplep,
        FIELDNO_HEAPTUPLEHEADERDATA_INFOMASK2 as c_int,
        c"infomask2".as_ptr(),
    );

    /* t_infomask & HEAP_HASNULL */
    v_hasnulls = LLVMBuildICmp(
        b,
        LLVMIntNE,
        LLVMBuildAnd(
            b,
            l_int16_const(lc, HEAP_HASNULL as i16),
            v_infomask1,
            c"".as_ptr(),
        ),
        l_int16_const(lc, 0),
        c"hasnulls".as_ptr(),
    );

    /* t_infomask2 & HEAP_NATTS_MASK */
    v_maxatt = LLVMBuildAnd(
        b,
        l_int16_const(lc, HEAP_NATTS_MASK as i16),
        v_infomask2,
        c"maxatt".as_ptr(),
    );

    /*
     * Need to zext, as getelementptr otherwise treats hoff as a signed 8bit
     * integer, which'd yield a negative offset for t_hoff > 127.
     */
    v_hoff = LLVMBuildZExt(
        b,
        l_load_struct_gep(
            b,
            StructHeapTupleHeaderData,
            v_tuplep,
            FIELDNO_HEAPTUPLEHEADERDATA_HOFF as c_int,
            c"".as_ptr(),
        ),
        LLVMInt32TypeInContext(lc),
        c"t_hoff".as_ptr(),
    );

    {
        let mut v_hoff_arg = v_hoff;
        v_tupdata_base = l_gep(
            b,
            LLVMInt8TypeInContext(lc),
            LLVMBuildBitCast(
                b,
                v_tuplep,
                l_ptr(LLVMInt8TypeInContext(lc)),
                c"".as_ptr(),
            ),
            &mut v_hoff_arg,
            1,
            c"v_tupdata_base".as_ptr(),
        );
    }

    /*
     * Load tuple start offset from slot. Will be reset below in case there's
     * no existing deformed columns in slot.
     */
    {
        let mut v_off_start: LLVMValueRef;

        v_off_start = l_load(
            b,
            LLVMInt32TypeInContext(lc),
            v_slotoffp,
            c"v_slot_off".as_ptr(),
        );
        v_off_start = LLVMBuildZExt(b, v_off_start, TypeSizeT, c"".as_ptr());
        LLVMBuildStore(b, v_off_start, v_offp);
    }
