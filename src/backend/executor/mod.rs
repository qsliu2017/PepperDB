//! Directory module: src/backend/executor (the .c bodies for the executor).
//!
//! Step 08 stands up the executor spine + expression interpreter for the M1
//! Result/Const projection path. Each dispatcher (ExecInitNode/ExecProcNode/
//! ExecEndNode nodeTag switch, the ExecInterpExpr opcode match, ExecInitExprRec's
//! node switch, ExecutePlan's command arm) is correct-for-reachable: the
//! Result/Const/projection path is COMPLETE; every other arm is a clean
//! `not_yet_reachable` guard that grows in later milestones (rules.md s4).

pub mod execExpr;
pub mod execExprInterp;
pub mod execMain;
pub mod execProcnode;
pub mod execTuples;
pub mod execUtils;
pub mod nodeResult;
