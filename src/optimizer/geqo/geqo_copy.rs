//! src/backend/optimizer/geqo/geqo_copy.c
//!
//! Routines to copy one GEQO chromosome to another (adopted from D. Whitley's
//! Genitor algorithm).
//!
//! #include mapping:
//!   - "postgres.h"                  -> `use crate::prelude::*;`
//!   - "optimizer/geqo_copy.h"       -> declares geqo_copy; pulls in the
//!                                      Chromosome struct (defined in
//!                                      optimizer/geqo_gene.h). The header has no
//!                                      standalone Rust home in this batch, so
//!                                      the Chromosome decl is MERGED in below.
//!   - Gene / PlannerInfo            -> imported from sibling modules; not
//!                                      redefined here.
//!
//! geqo_copy() is a FULLY REAL 1:1 translation of the C source: it copies the
//! `string` array (element-by-element, like the C loop / a memcpy) and the
//! scalar `worth` from a source Chromosome into a destination Chromosome.

use core::ffi::c_double;

use crate::prelude::*;

// PlannerInfo is only threaded through for signature fidelity with the C
// source (geqo_copy never dereferences it). Gene is the genome element type.
use crate::nodes::pathnodes::PlannerInfo;
use crate::optimizer::geqo::geqo_recombination::Gene;

/// Chromosome (from optimizer/geqo_gene.h).
///
/// "we presume that int instead of Relid is o.k. for Gene; so don't change it!"
/// A Chromosome is a tour (`string`, a heap-allocated array of `Gene`) together
/// with its fitness value (`worth`).
#[repr(C)]
#[derive(Clone, Copy)]
pub struct Chromosome {
    pub string: *mut Gene,
    pub worth: c_double,
}

/// geqo_copy
///
/// Copies one gene (chromosome) to another: the first `string_length` entries
/// of the `string` array plus the scalar `worth`, from `chromo2` (source) into
/// `chromo1` (destination).
///
/// # Safety
/// `chromo1` and `chromo2` must be valid, and each one's `string` must point to
/// at least `string_length` writable/readable `Gene`s. `root` is only passed
/// through (never dereferenced), matching the C source.
pub unsafe fn geqo_copy(
    _root: *mut PlannerInfo,
    chromo1: *mut Chromosome,
    chromo2: *mut Chromosome,
    string_length: c_int,
) {
    let mut i: c_int = 0;
    while i < string_length {
        *(*chromo1).string.offset(i as isize) = *(*chromo2).string.offset(i as isize);
        i += 1;
    }

    (*chromo1).worth = (*chromo2).worth;
}

#[cfg(test)]
mod tests {
    use super::*;

    // geqo_copy must duplicate the full string array and the worth from a
    // source Chromosome into a destination Chromosome. Both chromosomes are
    // hand-built via palloc (the genome arrays are independently allocated, so
    // the copy is a real element-by-element duplication, not aliasing).
    #[test]
    fn geqo_copy_duplicates_string_and_worth() {
        unsafe {
            let string_length: c_int = 8;

            // Source chromosome with a known tour + worth.
            let src_arr =
                palloc((string_length as usize) * core::mem::size_of::<Gene>()) as *mut Gene;
            for i in 0..string_length {
                *src_arr.offset(i as isize) = (i * 3 + 1) as Gene;
            }
            let mut src = Chromosome {
                string: src_arr,
                worth: 42.5_f64 as c_double,
            };

            // Destination chromosome, separately allocated and pre-filled with
            // sentinel values to prove every slot is overwritten.
            let dst_arr =
                palloc((string_length as usize) * core::mem::size_of::<Gene>()) as *mut Gene;
            for i in 0..string_length {
                *dst_arr.offset(i as isize) = -1 as Gene;
            }
            let mut dst = Chromosome {
                string: dst_arr,
                worth: 0.0_f64 as c_double,
            };

            geqo_copy(
                core::ptr::null_mut(),
                &mut dst as *mut Chromosome,
                &mut src as *mut Chromosome,
                string_length,
            );

            // worth copied.
            assert_eq!(dst.worth, 42.5_f64 as c_double);

            // every element copied.
            for i in 0..string_length {
                assert_eq!(
                    *dst.string.offset(i as isize),
                    (i * 3 + 1) as Gene,
                    "string[{}] not copied",
                    i
                );
            }

            // The arrays are distinct allocations: mutating the source after
            // the copy must not disturb the destination.
            *src.string.offset(0) = 999 as Gene;
            assert_eq!(*dst.string.offset(0), 1 as Gene);

            pfree(src_arr as *mut c_void);
            pfree(dst_arr as *mut c_void);
        }
    }
}
