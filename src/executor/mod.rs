//! Backend executor (postgres/src/backend/executor + postgres/src/include/executor).
//!
//! The TupleTableSlot row-abstraction layer comes first (tuptable + execTuples);
//! the node executors are future work.

pub mod nodeModifyTable;
pub mod nodeAgg;
pub mod execExpr;
pub mod nodeSubplan;
pub mod execIndexing;
pub mod execAsync;
pub mod nodeBitmapAnd;
pub mod nodeBitmapOr;
pub mod nodeGroup;
pub mod nodeLimit;
pub mod nodeMaterial;
pub mod nodeSetOp;
pub mod nodeNamedtuplestorescan;
pub mod nodeSubqueryscan;
pub mod nodeUnique;
pub mod nodeWorktablescan;
pub mod execGrouping;
pub mod execdebug;
pub mod execdesc;
// TODO: executor.h written (src/executor/executor.rs) but deferred from the build
// until executor node types (MemoryContext/ExprState evalfunc) are unified across
// modules - its static-inline fns have ~7 cross-module stub type mismatches.
pub mod executor;
pub mod hashjoin;
pub mod tablefunc;
pub mod execJunk;
pub mod execTuples;
pub mod execScan;
pub mod execUtils;
pub mod instrument;
pub mod spi_priv;
pub mod tstoreReceiver;
pub mod tuptable;

pub mod tqueue;
pub mod nodeResult;
pub mod nodeValuesscan;
pub mod nodeCtescan;
pub mod nodeRecursiveunion;
pub mod nodeProjectSet;
pub mod nodeSeqscan;
pub mod nodeNestloop;
pub mod nodeLockRows;
pub mod nodeCustom;
pub mod nodeSamplescan;
pub mod nodeMergeAppend;
pub mod nodeTidrangescan;
pub mod nodeBitmapIndexscan;
pub mod nodeGather;
pub mod nodeSort;
pub mod nodeForeignscan;
pub mod nodeTableFuncscan;
pub mod nodeTidscan;
pub mod nodeBitmapHeapscan;
pub mod nodeFunctionscan;
pub mod nodeGatherMerge;
pub mod nodeIndexonlyscan;
pub mod nodeMemoize;
pub mod execCurrent;
pub mod execAmi;
pub mod execProcnode;
pub mod execSRF;
pub mod execReplication;
pub mod execExprInterp;
pub mod execMain;
pub mod spi;
pub mod nodeWindowAgg;
pub mod nodeHash;
pub mod execPartition;
pub mod functions;
pub mod nodeIndexscan;
pub mod nodeHashjoin;
pub mod nodeMergejoin;
pub mod nodeIncrementalSort;
