//! Translated from PostgreSQL src/include/lib/sort_template.h
//!
//! The C header is a macro template instantiated per element type to generate a
//! specialized qsort. In Rust one generic function covers every instantiation:
//! the element type is a type parameter `T` and the comparator is a closure.
//! `ST_ELEMENT_TYPE_VOID` (the `element_size` variant) collapses away because a
//! generic already knows its element size.

use core::cmp::Ordering;

/// Median of three (Bentley-McIlroy), returns the chosen index.
fn med3<T>(a: usize, b: usize, c: usize, data: &[T], cmp: &impl Fn(&T, &T) -> Ordering) -> usize {
    if cmp(&data[a], &data[b]) == Ordering::Less {
        if cmp(&data[b], &data[c]) == Ordering::Less {
            b
        } else if cmp(&data[a], &data[c]) == Ordering::Less {
            c
        } else {
            a
        }
    } else if cmp(&data[b], &data[c]) == Ordering::Greater {
        b
    } else if cmp(&data[a], &data[c]) == Ordering::Less {
        a
    } else {
        c
    }
}

/// Sort a slice in place, faithful to PostgreSQL's `ST_SORT` (recurse on the
/// smaller partition, iterate on the larger; presorted-input fast path).
pub fn pg_qsort<T>(data: &mut [T], cmp: impl Fn(&T, &T) -> Ordering) {
    sort_range(data, 0, data.len(), &cmp);
}

/// Like `pg_qsort`, but the comparator carries an extra pass-through argument.
pub fn pg_qsort_arg<T, A>(data: &mut [T], cmp: impl Fn(&T, &T, &A) -> Ordering, arg: &A) {
    pg_qsort(data, |a, b| cmp(a, b, arg));
}

fn sort_range<T>(
    data: &mut [T],
    mut base: usize,
    mut n: usize,
    cmp: &impl Fn(&T, &T) -> Ordering,
) {
    loop {
        if n < 7 {
            // insertion sort
            for pm in (base + 1)..(base + n) {
                let mut pl = pm;
                while pl > base && cmp(&data[pl - 1], &data[pl]) == Ordering::Greater {
                    data.swap(pl, pl - 1);
                    pl -= 1;
                }
            }
            return;
        }

        // presorted check
        let mut presorted = true;
        for pm in (base + 1)..(base + n) {
            if cmp(&data[pm - 1], &data[pm]) == Ordering::Greater {
                presorted = false;
                break;
            }
        }
        if presorted {
            return;
        }

        let mut pm = base + n / 2;
        if n > 7 {
            let mut pl = base;
            let mut pn = base + (n - 1);
            if n > 40 {
                let d = n / 8;
                pl = med3(pl, pl + d, pl + 2 * d, data, cmp);
                pm = med3(pm - d, pm, pm + d, data, cmp);
                pn = med3(pn - 2 * d, pn - d, pn, data, cmp);
            }
            pm = med3(pl, pm, pn, data, cmp);
        }
        data.swap(base, pm);

        let mut pa = base + 1;
        let mut pb = base + 1;
        let mut pc = base + (n - 1);
        let mut pd = base + (n - 1);
        loop {
            while pb <= pc {
                let r = cmp(&data[pb], &data[base]);
                if r == Ordering::Greater {
                    break;
                }
                if r == Ordering::Equal {
                    data.swap(pa, pb);
                    pa += 1;
                }
                pb += 1;
            }
            while pb <= pc {
                let r = cmp(&data[pc], &data[base]);
                if r == Ordering::Less {
                    break;
                }
                if r == Ordering::Equal {
                    data.swap(pc, pd);
                    pd = pd.wrapping_sub(1);
                }
                pc = pc.wrapping_sub(1);
            }
            if pb > pc {
                break;
            }
            data.swap(pb, pc);
            pb += 1;
            pc = pc.wrapping_sub(1);
        }

        let pn = base + n;
        // swap equal-to-pivot elements from the ends into the middle
        let d1 = (pa - base).min(pb - pa);
        swapn(data, base, pb - d1, d1);
        let d1 = (pd - pc).min(pn - pd - 1);
        swapn(data, pb, pn - d1, d1);

        let d1 = pb - pa;
        let d2 = pd - pc;
        if d1 <= d2 {
            if d1 > 1 {
                sort_range(data, base, d1, cmp);
            }
            if d2 > 1 {
                base = pn - d2;
                n = d2;
                continue;
            }
        } else {
            if d2 > 1 {
                sort_range(data, pn - d2, d2, cmp);
            }
            if d1 > 1 {
                n = d1;
                continue;
            }
        }
        return;
    }
}

/// Swap `n` consecutive elements starting at indices `a` and `b`.
fn swapn<T>(data: &mut [T], a: usize, b: usize, n: usize) {
    for i in 0..n {
        data.swap(a + i, b + i);
    }
}
