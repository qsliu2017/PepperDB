//! Translated from PostgreSQL src/include/lib/qunique.h

use core::cmp::Ordering;

/// Remove duplicates from a pre-sorted slice, according to `compare`.
/// Returns the new length (deduplicated elements occupy `array[..len]`).
pub fn qunique<T>(array: &mut [T], compare: impl Fn(&T, &T) -> Ordering) -> usize {
    let elements = array.len();
    if elements <= 1 {
        return elements;
    }

    let mut j = 0;
    for i in 1..elements {
        if compare(&array[i], &array[j]) != Ordering::Equal {
            j += 1;
            if j != i {
                array.swap(j, i);
            }
        }
    }
    j + 1
}

/// Like `qunique`, but the comparator carries an extra pass-through argument.
pub fn qunique_arg<T, A>(
    array: &mut [T],
    compare: impl Fn(&T, &T, &A) -> Ordering,
    arg: &A,
) -> usize {
    let elements = array.len();
    if elements <= 1 {
        return elements;
    }

    let mut j = 0;
    for i in 1..elements {
        if compare(&array[i], &array[j], arg) != Ordering::Equal {
            j += 1;
            if j != i {
                array.swap(j, i);
            }
        }
    }
    j + 1
}
