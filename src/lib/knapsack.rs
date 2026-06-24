//! Translated from PostgreSQL src/include/lib/knapsack.h
//! 0/1 (discrete) knapsack solver.

use crate::nodes::bitmapset::Bitmapset;

/// Select a subset of items (by index) maximizing total value within
/// `max_weight`. `num_items` is implied by the slice lengths.
pub fn discrete_knapsack(
    _max_weight: i32,
    _item_weights: &[i32],
    _item_values: &[f64],
) -> Bitmapset {
    unimplemented!()
}
