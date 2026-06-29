//! Directory module: src/backend/executor (the .c bodies for the executor).
//!
//! Step 08 stands up the executor spine + expression interpreter for the M1
//! Result/Const projection path; step 18A (M2) adds the SeqScan + ExecScan +
//! ModifyTable(Insert) path. Each dispatcher (ExecInitNode/ExecProcNode/
//! ExecEndNode nodeTag switch, the ExecInterpExpr opcode match, ExecInitExprRec's
//! node switch, ExecutePlan's command arm) is correct-for-reachable: the
//! Result/Const, the forward seqscan + scan-Var projection, and the INSERT paths
//! are COMPLETE; every other arm is a clean `not_yet_reachable` guard that grows
//! in later milestones (rules.md s4).

pub mod execAmi;
pub mod execExpr;
pub mod execExprInterp;
pub mod execMain;
pub mod execProcnode;
pub mod execScan;
pub mod execTuples;
pub mod execUtils;
pub mod nodeAgg;
pub mod nodeBitmapAnd;
pub mod nodeBitmapHeapscan;
pub mod nodeBitmapIndexscan;
pub mod nodeBitmapOr;
pub mod nodeGroup;
pub mod nodeIndexonlyscan;
pub mod nodeIndexscan;
pub mod nodeLimit;
pub mod nodeMaterial;
pub mod nodeModifyTable;
pub mod nodeResult;
pub mod nodeSeqscan;
pub mod nodeSort;
pub mod nodeUnique;
