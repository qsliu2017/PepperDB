//! Translated from PostgreSQL src/include/access/stratnum.h

/// Strategy numbers identify operator semantics within an operator class.
pub type StrategyNumber = u16;

pub const INVALID_STRATEGY: StrategyNumber = 0;

/// Strategy numbers for B-tree indexes.
pub const BT_LESS_STRATEGY_NUMBER: StrategyNumber = 1;
pub const BT_LESS_EQUAL_STRATEGY_NUMBER: StrategyNumber = 2;
pub const BT_EQUAL_STRATEGY_NUMBER: StrategyNumber = 3;
pub const BT_GREATER_EQUAL_STRATEGY_NUMBER: StrategyNumber = 4;
pub const BT_GREATER_STRATEGY_NUMBER: StrategyNumber = 5;
pub const BT_MAX_STRATEGY_NUMBER: StrategyNumber = 5;

/// Strategy numbers for hash indexes (only equality).
pub const HT_EQUAL_STRATEGY_NUMBER: StrategyNumber = 1;
pub const HT_MAX_STRATEGY_NUMBER: StrategyNumber = 1;

/// Strategy numbers common to (some) GiST, SP-GiST and BRIN opclasses.
pub const RT_LEFT_STRATEGY_NUMBER: StrategyNumber = 1; // for <<
pub const RT_OVER_LEFT_STRATEGY_NUMBER: StrategyNumber = 2; // for &<
pub const RT_OVERLAP_STRATEGY_NUMBER: StrategyNumber = 3; // for &&
pub const RT_OVER_RIGHT_STRATEGY_NUMBER: StrategyNumber = 4; // for &>
pub const RT_RIGHT_STRATEGY_NUMBER: StrategyNumber = 5; // for >>
pub const RT_SAME_STRATEGY_NUMBER: StrategyNumber = 6; // for ~=
pub const RT_CONTAINS_STRATEGY_NUMBER: StrategyNumber = 7; // for @>
pub const RT_CONTAINED_BY_STRATEGY_NUMBER: StrategyNumber = 8; // for <@
pub const RT_OVER_BELOW_STRATEGY_NUMBER: StrategyNumber = 9; // for &<|
pub const RT_BELOW_STRATEGY_NUMBER: StrategyNumber = 10; // for <<|
pub const RT_ABOVE_STRATEGY_NUMBER: StrategyNumber = 11; // for |>>
pub const RT_OVER_ABOVE_STRATEGY_NUMBER: StrategyNumber = 12; // for |&>
pub const RT_OLD_CONTAINS_STRATEGY_NUMBER: StrategyNumber = 13; // old spelling of @>
pub const RT_OLD_CONTAINED_BY_STRATEGY_NUMBER: StrategyNumber = 14; // old spelling of <@
pub const RT_KNN_SEARCH_STRATEGY_NUMBER: StrategyNumber = 15; // for <-> (distance)
pub const RT_CONTAINS_ELEM_STRATEGY_NUMBER: StrategyNumber = 16; // range types @> elem
pub const RT_ADJACENT_STRATEGY_NUMBER: StrategyNumber = 17; // for -|-
pub const RT_EQUAL_STRATEGY_NUMBER: StrategyNumber = 18; // for =
pub const RT_NOT_EQUAL_STRATEGY_NUMBER: StrategyNumber = 19; // for !=
pub const RT_LESS_STRATEGY_NUMBER: StrategyNumber = 20; // for <
pub const RT_LESS_EQUAL_STRATEGY_NUMBER: StrategyNumber = 21; // for <=
pub const RT_GREATER_STRATEGY_NUMBER: StrategyNumber = 22; // for >
pub const RT_GREATER_EQUAL_STRATEGY_NUMBER: StrategyNumber = 23; // for >=
pub const RT_SUB_STRATEGY_NUMBER: StrategyNumber = 24; // for inet >>
pub const RT_SUB_EQUAL_STRATEGY_NUMBER: StrategyNumber = 25; // for inet <<=
pub const RT_SUPER_STRATEGY_NUMBER: StrategyNumber = 26; // for inet <<
pub const RT_SUPER_EQUAL_STRATEGY_NUMBER: StrategyNumber = 27; // for inet >>=
pub const RT_PREFIX_STRATEGY_NUMBER: StrategyNumber = 28; // for text ^@
pub const RT_OLD_BELOW_STRATEGY_NUMBER: StrategyNumber = 29; // old spelling of <<|
pub const RT_OLD_ABOVE_STRATEGY_NUMBER: StrategyNumber = 30; // old spelling of |>>
pub const RT_MAX_STRATEGY_NUMBER: StrategyNumber = 30;
