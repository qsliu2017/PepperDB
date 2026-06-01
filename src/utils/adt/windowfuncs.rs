//! src/backend/utils/adt/windowfuncs.c
//!
//! Standard window functions defined in SQL spec.
//!
//! Portions Copyright (c) 2000-2025, PostgreSQL Global Development Group

use crate::prelude::*;

use crate::nodes::nodes::Node;
use crate::nodes::plannodes::MonotonicFunction;
use crate::nodes::parsenodes::{
    FRAMEOPTION_END_CURRENT_ROW, FRAMEOPTION_NONDEFAULT, FRAMEOPTION_ROWS,
    FRAMEOPTION_START_UNBOUNDED_PRECEDING,
};
use crate::nodes::supportnodes::{SupportRequestOptimizeWindowClause, SupportRequestWFuncMonotonic};
use crate::utils::fmgr::{get_fn_expr_arg_stable, FunctionCallInfo};
use crate::windowapi::{
    PG_WINDOW_OBJECT, WinGetCurrentPosition, WinGetFuncArgCurrent, WinGetFuncArgInFrame,
    WinGetFuncArgInPartition, WinGetPartitionLocalMemory, WinGetPartitionRowCount,
    WinRowsArePeers, WinSetMarkPosition, WindowObject, WINDOW_SEEK_CURRENT, WINDOW_SEEK_HEAD,
    WINDOW_SEEK_TAIL,
};
use crate::{
    Assert, PG_FUNCTION_ARGS, PG_GETARG_POINTER, PG_RETURN_DATUM, PG_RETURN_FLOAT8,
    PG_RETURN_INT32, PG_RETURN_INT64, PG_RETURN_NULL, PG_RETURN_POINTER,
};

/*
 * ranking process information
 */
#[repr(C)]
struct rank_context {
    rank: int64, /* current rank */
}

/*
 * ntile process information
 */
#[repr(C)]
struct ntile_context {
    ntile: int32,           /* current result */
    rows_per_bucket: int64, /* row number of current bucket */
    boundary: int64,        /* how many rows should be in the bucket */
    remainder: int64,       /* (total rows) % (bucket num) */
}

/*
 * The SupportRequest* NodeTag variants are not yet present in the ported
 * NodeTag enum, so the C `IsA(rawreq, T_...)` checks become these local
 * stub helpers until the tags are added.
 */
unsafe fn IsA_SupportRequestWFuncMonotonic(_rawreq: *mut Node) -> bool {
    unimplemented!() // TODO: nodes/nodes.h (missing T_SupportRequestWFuncMonotonic)
}

unsafe fn IsA_SupportRequestOptimizeWindowClause(_rawreq: *mut Node) -> bool {
    unimplemented!() // TODO: nodes/nodes.h (missing T_SupportRequestOptimizeWindowClause)
}

/* ERRCODE_INVALID_ARGUMENT_FOR_NTILE not yet ported (utils/errcodes.h) */
unsafe fn ERRCODE_INVALID_ARGUMENT_FOR_NTILE() -> c_int {
    unimplemented!() // TODO: utils/errcodes.h
}

/* ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE not yet ported (utils/errcodes.h) */
unsafe fn ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE() -> c_int {
    unimplemented!() // TODO: utils/errcodes.h
}

/*
 * utility routine for *_rank functions.
 */
unsafe fn rank_up(winobj: WindowObject) -> bool {
    let mut up: bool = false; /* should rank increase? */
    let curpos: int64 = WinGetCurrentPosition(winobj);
    let context: *mut rank_context;

    context = WinGetPartitionLocalMemory(winobj, core::mem::size_of::<rank_context>() as Size)
        as *mut rank_context;

    if (*context).rank == 0 {
        /* first call: rank of first row is always 1 */
        Assert!(curpos == 0);
        (*context).rank = 1;
    } else {
        Assert!(curpos > 0);
        /* do current and prior tuples match by ORDER BY clause? */
        if !WinRowsArePeers(winobj, curpos - 1, curpos) {
            up = true;
        }
    }

    /* We can advance the mark, but only *after* access to prior row */
    WinSetMarkPosition(winobj, curpos);

    up
}

/*
 * row_number
 * just increment up from 1 until current partition finishes.
 */
#[no_mangle]
pub unsafe extern "C" fn window_row_number(fcinfo: FunctionCallInfo) -> Datum {
    let winobj: WindowObject = PG_WINDOW_OBJECT(fcinfo);
    let curpos: int64 = WinGetCurrentPosition(winobj);

    WinSetMarkPosition(winobj, curpos);
    PG_RETURN_INT64!(curpos + 1)
}

/*
 * window_row_number_support
 *		prosupport function for window_row_number()
 */
#[no_mangle]
pub unsafe extern "C" fn window_row_number_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq: *mut Node = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;

    if IsA_SupportRequestWFuncMonotonic(rawreq) {
        let req: *mut SupportRequestWFuncMonotonic = rawreq as *mut SupportRequestWFuncMonotonic;

        /* row_number() is monotonically increasing */
        (*req).monotonic = MonotonicFunction::MONOTONICFUNC_INCREASING;
        PG_RETURN_POINTER!(req as *mut c_void)
    } else if IsA_SupportRequestOptimizeWindowClause(rawreq) {
        let req: *mut SupportRequestOptimizeWindowClause =
            rawreq as *mut SupportRequestOptimizeWindowClause;

        /*
         * The frame options can always become "ROWS BETWEEN UNBOUNDED
         * PRECEDING AND CURRENT ROW".  row_number() always just increments by
         * 1 with each row in the partition.  Using ROWS instead of RANGE
         * saves effort checking peer rows during execution.
         */
        (*req).frameOptions = FRAMEOPTION_NONDEFAULT
            | FRAMEOPTION_ROWS
            | FRAMEOPTION_START_UNBOUNDED_PRECEDING
            | FRAMEOPTION_END_CURRENT_ROW;

        PG_RETURN_POINTER!(req as *mut c_void)
    } else {
        PG_RETURN_POINTER!(null_mut::<c_void>())
    }
}

/*
 * rank
 * Rank changes when key columns change.
 * The new rank number is the current row number.
 */
#[no_mangle]
pub unsafe extern "C" fn window_rank(fcinfo: FunctionCallInfo) -> Datum {
    let winobj: WindowObject = PG_WINDOW_OBJECT(fcinfo);
    let context: *mut rank_context;
    let up: bool;

    up = rank_up(winobj);
    context = WinGetPartitionLocalMemory(winobj, core::mem::size_of::<rank_context>() as Size)
        as *mut rank_context;
    if up {
        (*context).rank = WinGetCurrentPosition(winobj) + 1;
    }

    PG_RETURN_INT64!((*context).rank)
}

/*
 * window_rank_support
 *		prosupport function for window_rank()
 */
#[no_mangle]
pub unsafe extern "C" fn window_rank_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq: *mut Node = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;

    if IsA_SupportRequestWFuncMonotonic(rawreq) {
        let req: *mut SupportRequestWFuncMonotonic = rawreq as *mut SupportRequestWFuncMonotonic;

        /* rank() is monotonically increasing */
        (*req).monotonic = MonotonicFunction::MONOTONICFUNC_INCREASING;
        PG_RETURN_POINTER!(req as *mut c_void)
    } else if IsA_SupportRequestOptimizeWindowClause(rawreq) {
        let req: *mut SupportRequestOptimizeWindowClause =
            rawreq as *mut SupportRequestOptimizeWindowClause;

        /*
         * rank() is coded in such a way that it returns "(COUNT (*) OVER
         * (<opt> RANGE UNBOUNDED PRECEDING) - COUNT (*) OVER (<opt> RANGE
         * CURRENT ROW) + 1)" regardless of the frame options.  We'll set the
         * frame options to "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW"
         * so they agree with what window_row_number_support() optimized the
         * frame options to be.  Using ROWS instead of RANGE saves from doing
         * peer row checks during execution.
         */
        (*req).frameOptions = FRAMEOPTION_NONDEFAULT
            | FRAMEOPTION_ROWS
            | FRAMEOPTION_START_UNBOUNDED_PRECEDING
            | FRAMEOPTION_END_CURRENT_ROW;

        PG_RETURN_POINTER!(req as *mut c_void)
    } else {
        PG_RETURN_POINTER!(null_mut::<c_void>())
    }
}

/*
 * dense_rank
 * Rank increases by 1 when key columns change.
 */
#[no_mangle]
pub unsafe extern "C" fn window_dense_rank(fcinfo: FunctionCallInfo) -> Datum {
    let winobj: WindowObject = PG_WINDOW_OBJECT(fcinfo);
    let context: *mut rank_context;
    let up: bool;

    up = rank_up(winobj);
    context = WinGetPartitionLocalMemory(winobj, core::mem::size_of::<rank_context>() as Size)
        as *mut rank_context;
    if up {
        (*context).rank += 1;
    }

    PG_RETURN_INT64!((*context).rank)
}

/*
 * window_dense_rank_support
 *		prosupport function for window_dense_rank()
 */
#[no_mangle]
pub unsafe extern "C" fn window_dense_rank_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq: *mut Node = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;

    if IsA_SupportRequestWFuncMonotonic(rawreq) {
        let req: *mut SupportRequestWFuncMonotonic = rawreq as *mut SupportRequestWFuncMonotonic;

        /* dense_rank() is monotonically increasing */
        (*req).monotonic = MonotonicFunction::MONOTONICFUNC_INCREASING;
        PG_RETURN_POINTER!(req as *mut c_void)
    } else if IsA_SupportRequestOptimizeWindowClause(rawreq) {
        let req: *mut SupportRequestOptimizeWindowClause =
            rawreq as *mut SupportRequestOptimizeWindowClause;

        /*
         * dense_rank() is unaffected by the frame options.  Here we set the
         * frame options to match what's done in row_number's support
         * function.  Using ROWS instead of RANGE (the default) saves the
         * executor from having to check for peer rows.
         */
        (*req).frameOptions = FRAMEOPTION_NONDEFAULT
            | FRAMEOPTION_ROWS
            | FRAMEOPTION_START_UNBOUNDED_PRECEDING
            | FRAMEOPTION_END_CURRENT_ROW;

        PG_RETURN_POINTER!(req as *mut c_void)
    } else {
        PG_RETURN_POINTER!(null_mut::<c_void>())
    }
}

/*
 * percent_rank
 * return fraction between 0 and 1 inclusive,
 * which is described as (RK - 1) / (NR - 1), where RK is the current row's
 * rank and NR is the total number of rows, per spec.
 */
#[no_mangle]
pub unsafe extern "C" fn window_percent_rank(fcinfo: FunctionCallInfo) -> Datum {
    let winobj: WindowObject = PG_WINDOW_OBJECT(fcinfo);
    let context: *mut rank_context;
    let up: bool;
    let totalrows: int64 = WinGetPartitionRowCount(winobj);

    Assert!(totalrows > 0);

    up = rank_up(winobj);
    context = WinGetPartitionLocalMemory(winobj, core::mem::size_of::<rank_context>() as Size)
        as *mut rank_context;
    if up {
        (*context).rank = WinGetCurrentPosition(winobj) + 1;
    }

    /* return zero if there's only one row, per spec */
    if totalrows <= 1 {
        PG_RETURN_FLOAT8!(0.0)
    } else {
        PG_RETURN_FLOAT8!(((*context).rank - 1) as float8 / (totalrows - 1) as float8)
    }
}

/*
 * window_percent_rank_support
 *		prosupport function for window_percent_rank()
 */
#[no_mangle]
pub unsafe extern "C" fn window_percent_rank_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq: *mut Node = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;

    if IsA_SupportRequestWFuncMonotonic(rawreq) {
        let req: *mut SupportRequestWFuncMonotonic = rawreq as *mut SupportRequestWFuncMonotonic;

        /* percent_rank() is monotonically increasing */
        (*req).monotonic = MonotonicFunction::MONOTONICFUNC_INCREASING;
        PG_RETURN_POINTER!(req as *mut c_void)
    } else if IsA_SupportRequestOptimizeWindowClause(rawreq) {
        let req: *mut SupportRequestOptimizeWindowClause =
            rawreq as *mut SupportRequestOptimizeWindowClause;

        /*
         * percent_rank() is unaffected by the frame options.  Here we set the
         * frame options to match what's done in row_number's support
         * function.  Using ROWS instead of RANGE (the default) saves the
         * executor from having to check for peer rows.
         */
        (*req).frameOptions = FRAMEOPTION_NONDEFAULT
            | FRAMEOPTION_ROWS
            | FRAMEOPTION_START_UNBOUNDED_PRECEDING
            | FRAMEOPTION_END_CURRENT_ROW;

        PG_RETURN_POINTER!(req as *mut c_void)
    } else {
        PG_RETURN_POINTER!(null_mut::<c_void>())
    }
}

/*
 * cume_dist
 * return fraction between 0 and 1 inclusive,
 * which is described as NP / NR, where NP is the number of rows preceding or
 * peers to the current row, and NR is the total number of rows, per spec.
 */
#[no_mangle]
pub unsafe extern "C" fn window_cume_dist(fcinfo: FunctionCallInfo) -> Datum {
    let winobj: WindowObject = PG_WINDOW_OBJECT(fcinfo);
    let context: *mut rank_context;
    let up: bool;
    let totalrows: int64 = WinGetPartitionRowCount(winobj);

    Assert!(totalrows > 0);

    up = rank_up(winobj);
    context = WinGetPartitionLocalMemory(winobj, core::mem::size_of::<rank_context>() as Size)
        as *mut rank_context;
    if up || (*context).rank == 1 {
        /*
         * The current row is not peer to prior row or is just the first, so
         * count up the number of rows that are peer to the current.
         */
        let mut row: int64;

        (*context).rank = WinGetCurrentPosition(winobj) + 1;

        /*
         * start from current + 1
         */
        row = (*context).rank;
        while row < totalrows {
            if !WinRowsArePeers(winobj, row - 1, row) {
                break;
            }
            (*context).rank += 1;
            row += 1;
        }
    }

    PG_RETURN_FLOAT8!((*context).rank as float8 / totalrows as float8)
}

/*
 * window_cume_dist_support
 *		prosupport function for window_cume_dist()
 */
#[no_mangle]
pub unsafe extern "C" fn window_cume_dist_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq: *mut Node = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;

    if IsA_SupportRequestWFuncMonotonic(rawreq) {
        let req: *mut SupportRequestWFuncMonotonic = rawreq as *mut SupportRequestWFuncMonotonic;

        /* cume_dist() is monotonically increasing */
        (*req).monotonic = MonotonicFunction::MONOTONICFUNC_INCREASING;
        PG_RETURN_POINTER!(req as *mut c_void)
    } else if IsA_SupportRequestOptimizeWindowClause(rawreq) {
        let req: *mut SupportRequestOptimizeWindowClause =
            rawreq as *mut SupportRequestOptimizeWindowClause;

        /*
         * cume_dist() is unaffected by the frame options.  Here we set the
         * frame options to match what's done in row_number's support
         * function.  Using ROWS instead of RANGE (the default) saves the
         * executor from having to check for peer rows.
         */
        (*req).frameOptions = FRAMEOPTION_NONDEFAULT
            | FRAMEOPTION_ROWS
            | FRAMEOPTION_START_UNBOUNDED_PRECEDING
            | FRAMEOPTION_END_CURRENT_ROW;

        PG_RETURN_POINTER!(req as *mut c_void)
    } else {
        PG_RETURN_POINTER!(null_mut::<c_void>())
    }
}

/*
 * ntile
 * compute an exact numeric value with scale 0 (zero),
 * ranging from 1 (one) to n, per spec.
 */
#[no_mangle]
pub unsafe extern "C" fn window_ntile(fcinfo: FunctionCallInfo) -> Datum {
    let winobj: WindowObject = PG_WINDOW_OBJECT(fcinfo);
    let context: *mut ntile_context;

    context = WinGetPartitionLocalMemory(winobj, core::mem::size_of::<ntile_context>() as Size)
        as *mut ntile_context;

    if (*context).ntile == 0 {
        /* first call */
        let total: int64;
        let nbuckets: int32;
        let mut isnull: bool = false;

        total = WinGetPartitionRowCount(winobj);
        nbuckets = DatumGetInt32(WinGetFuncArgCurrent(winobj, 0, &mut isnull));

        /*
         * per spec: If NT is the null value, then the result is the null
         * value.
         */
        if isnull {
            PG_RETURN_NULL!(fcinfo);
        }

        /*
         * per spec: If NT is less than or equal to 0 (zero), then an
         * exception condition is raised.
         */
        if nbuckets <= 0 {
            let _ = ERRCODE_INVALID_ARGUMENT_FOR_NTILE();
            elog!(ERROR, "argument of ntile must be greater than zero");
            unreachable!();
        }

        (*context).ntile = 1;
        (*context).rows_per_bucket = 0;
        (*context).boundary = total / nbuckets as int64;
        if (*context).boundary <= 0 {
            (*context).boundary = 1;
        } else {
            /*
             * If the total number is not divisible, add 1 row to leading
             * buckets.
             */
            (*context).remainder = total % nbuckets as int64;
            if (*context).remainder != 0 {
                (*context).boundary += 1;
            }
        }
    }

    (*context).rows_per_bucket += 1;
    if (*context).boundary < (*context).rows_per_bucket {
        /* ntile up */
        if (*context).remainder != 0 && (*context).ntile as int64 == (*context).remainder {
            (*context).remainder = 0;
            (*context).boundary -= 1;
        }
        (*context).ntile += 1;
        (*context).rows_per_bucket = 1;
    }

    PG_RETURN_INT32!((*context).ntile)
}

/*
 * window_ntile_support
 *		prosupport function for window_ntile()
 */
#[no_mangle]
pub unsafe extern "C" fn window_ntile_support(fcinfo: FunctionCallInfo) -> Datum {
    let rawreq: *mut Node = PG_GETARG_POINTER!(fcinfo, 0) as *mut Node;

    if IsA_SupportRequestWFuncMonotonic(rawreq) {
        let req: *mut SupportRequestWFuncMonotonic = rawreq as *mut SupportRequestWFuncMonotonic;

        /*
         * ntile() is monotonically increasing as the number of buckets cannot
         * change after the first call
         */
        (*req).monotonic = MonotonicFunction::MONOTONICFUNC_INCREASING;
        PG_RETURN_POINTER!(req as *mut c_void)
    } else if IsA_SupportRequestOptimizeWindowClause(rawreq) {
        let req: *mut SupportRequestOptimizeWindowClause =
            rawreq as *mut SupportRequestOptimizeWindowClause;

        /*
         * ntile() is unaffected by the frame options.  Here we set the frame
         * options to match what's done in row_number's support function.
         * Using ROWS instead of RANGE (the default) saves the executor from
         * having to check for peer rows.
         */
        (*req).frameOptions = FRAMEOPTION_NONDEFAULT
            | FRAMEOPTION_ROWS
            | FRAMEOPTION_START_UNBOUNDED_PRECEDING
            | FRAMEOPTION_END_CURRENT_ROW;

        PG_RETURN_POINTER!(req as *mut c_void)
    } else {
        PG_RETURN_POINTER!(null_mut::<c_void>())
    }
}

/*
 * leadlag_common
 * common operation of lead() and lag()
 * For lead() forward is true, whereas for lag() it is false.
 * withoffset indicates we have an offset second argument.
 * withdefault indicates we have a default third argument.
 */
unsafe fn leadlag_common(
    fcinfo: FunctionCallInfo,
    forward: bool,
    withoffset: bool,
    withdefault: bool,
) -> Datum {
    let winobj: WindowObject = PG_WINDOW_OBJECT(fcinfo);
    let offset: int32;
    let const_offset: bool;
    let mut result: Datum;
    let mut isnull: bool = false;
    let mut isout: bool = false;

    if withoffset {
        offset = DatumGetInt32(WinGetFuncArgCurrent(winobj, 1, &mut isnull));
        if isnull {
            PG_RETURN_NULL!(fcinfo);
        }
        const_offset = get_fn_expr_arg_stable((*fcinfo).flinfo, 1);
    } else {
        offset = 1;
        const_offset = true;
    }

    result = WinGetFuncArgInPartition(
        winobj,
        0,
        if forward { offset } else { -offset },
        WINDOW_SEEK_CURRENT,
        const_offset,
        &mut isnull,
        &mut isout,
    );

    if isout {
        /*
         * target row is out of the partition; supply default value if
         * provided.  otherwise it'll stay NULL
         */
        if withdefault {
            result = WinGetFuncArgCurrent(winobj, 2, &mut isnull);
        }
    }

    if isnull {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_DATUM!(result)
}

/*
 * lag
 * returns the value of VE evaluated on a row that is 1
 * row before the current row within a partition,
 * per spec.
 */
#[no_mangle]
pub unsafe extern "C" fn window_lag(fcinfo: FunctionCallInfo) -> Datum {
    leadlag_common(fcinfo, false, false, false)
}

/*
 * lag_with_offset
 * returns the value of VE evaluated on a row that is OFFSET
 * rows before the current row within a partition,
 * per spec.
 */
#[no_mangle]
pub unsafe extern "C" fn window_lag_with_offset(fcinfo: FunctionCallInfo) -> Datum {
    leadlag_common(fcinfo, false, true, false)
}

/*
 * lag_with_offset_and_default
 * same as lag_with_offset but accepts default value
 * as its third argument.
 */
#[no_mangle]
pub unsafe extern "C" fn window_lag_with_offset_and_default(fcinfo: FunctionCallInfo) -> Datum {
    leadlag_common(fcinfo, false, true, true)
}

/*
 * lead
 * returns the value of VE evaluated on a row that is 1
 * row after the current row within a partition,
 * per spec.
 */
#[no_mangle]
pub unsafe extern "C" fn window_lead(fcinfo: FunctionCallInfo) -> Datum {
    leadlag_common(fcinfo, true, false, false)
}

/*
 * lead_with_offset
 * returns the value of VE evaluated on a row that is OFFSET
 * number of rows after the current row within a partition,
 * per spec.
 */
#[no_mangle]
pub unsafe extern "C" fn window_lead_with_offset(fcinfo: FunctionCallInfo) -> Datum {
    leadlag_common(fcinfo, true, true, false)
}

/*
 * lead_with_offset_and_default
 * same as lead_with_offset but accepts default value
 * as its third argument.
 */
#[no_mangle]
pub unsafe extern "C" fn window_lead_with_offset_and_default(fcinfo: FunctionCallInfo) -> Datum {
    leadlag_common(fcinfo, true, true, true)
}

/*
 * first_value
 * return the value of VE evaluated on the first row of the
 * window frame, per spec.
 */
#[no_mangle]
pub unsafe extern "C" fn window_first_value(fcinfo: FunctionCallInfo) -> Datum {
    let winobj: WindowObject = PG_WINDOW_OBJECT(fcinfo);
    let result: Datum;
    let mut isnull: bool = false;

    result = WinGetFuncArgInFrame(
        winobj,
        0,
        0,
        WINDOW_SEEK_HEAD,
        true,
        &mut isnull,
        null_mut(),
    );
    if isnull {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_DATUM!(result)
}

/*
 * last_value
 * return the value of VE evaluated on the last row of the
 * window frame, per spec.
 */
#[no_mangle]
pub unsafe extern "C" fn window_last_value(fcinfo: FunctionCallInfo) -> Datum {
    let winobj: WindowObject = PG_WINDOW_OBJECT(fcinfo);
    let result: Datum;
    let mut isnull: bool = false;

    result = WinGetFuncArgInFrame(
        winobj,
        0,
        0,
        WINDOW_SEEK_TAIL,
        true,
        &mut isnull,
        null_mut(),
    );
    if isnull {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_DATUM!(result)
}

/*
 * nth_value
 * return the value of VE evaluated on the n-th row from the first
 * row of the window frame, per spec.
 */
#[no_mangle]
pub unsafe extern "C" fn window_nth_value(fcinfo: FunctionCallInfo) -> Datum {
    let winobj: WindowObject = PG_WINDOW_OBJECT(fcinfo);
    let const_offset: bool;
    let result: Datum;
    let mut isnull: bool = false;
    let nth: int32;

    nth = DatumGetInt32(WinGetFuncArgCurrent(winobj, 1, &mut isnull));
    if isnull {
        PG_RETURN_NULL!(fcinfo);
    }
    const_offset = get_fn_expr_arg_stable((*fcinfo).flinfo, 1);

    if nth <= 0 {
        let _ = ERRCODE_INVALID_ARGUMENT_FOR_NTH_VALUE();
        elog!(ERROR, "argument of nth_value must be greater than zero");
        unreachable!();
    }

    result = WinGetFuncArgInFrame(
        winobj,
        0,
        nth - 1,
        WINDOW_SEEK_HEAD,
        const_offset,
        &mut isnull,
        null_mut(),
    );
    if isnull {
        PG_RETURN_NULL!(fcinfo);
    }

    PG_RETURN_DATUM!(result)
}
