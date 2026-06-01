//! lib/sort_template.h - C template for a specialized qsort (Bentley-McIlroy) instantiated per-caller.
//!
//! This is a PostgreSQL TEMPLATE header. It is `#include`d multiple times, each
//! time with a different set of caller-defined macros that parameterize the
//! generated code:
//!
//!   - ST_SORT                - name of the generated sort function
//!   - ST_ELEMENT_TYPE        - type of the referenced elements
//!   - ST_ELEMENT_TYPE_VOID   - alternative: element type is `void`; the
//!                              generated function gains a runtime
//!                              `element_size` parameter (a traditional qsort)
//!   - ST_DECLARE             - if defined, the functions/types are declared
//!   - ST_DEFINE              - if defined, the functions/types are defined
//!   - ST_SCOPE               - scope (e.g. `extern`, `static inline`)
//!   - ST_CHECK_FOR_INTERRUPTS- if defined, the sort is interruptible
//!   - ST_COMPARE(a, b)              - simple comparison expression, OR
//!   - ST_COMPARE(a, b, arg)        - variant taking an extra pass-through arg, OR
//!   - ST_COMPARE_RUNTIME_POINTER  - sort takes a comparator function pointer
//!   - ST_COMPARE_ARG_TYPE    - type of the extra pass-through argument
//!   - ST_COMPARATOR_TYPE_NAME- name for the generated comparator fn-ptr typedef
//!
//! Because every symbol in this header is built by token-pasting the caller's
//! ST_SORT / ST_ELEMENT_TYPE / etc. macros (ST_MAKE_NAME), there is NO concrete,
//! standalone C declaration to translate 1:1 - and therefore no concrete Rust
//! form either. Rust has no token-pasting preprocessor; the idiomatic Rust
//! equivalent of this header is generics (`fn sort<T, F: Fn(&T,&T)->Ordering>`)
//! or a declarative macro, but per the porting rules we do NOT emit a generic
//! impl here. Each Rust caller that needs a specialized sort will provide its
//! own instantiation.
//!
//! The sole purpose of this file is to document the template and expose any
//! pieces that are concrete regardless of instantiation. There are none in this
//! header: the only `#define` constants (ST_POINTER_STEP, etc.) are themselves
//! macro-parameterized, and every struct/typedef/function is name-mangled per
//! caller. So this module is documentation-only.
//!
//! Below is a faithful Rust pseudo-translation of the generated bodies, written
//! as DOC COMMENTS (not compiled code) so future porters can see exactly what
//! each instantiation produces.

// ---------------------------------------------------------------------------
// Name-mangling macros (concrete C, but produce no symbols on their own):
//
//   ST_MAKE_PREFIX(a)  => CppConcat(a, _)            // "foo"  -> "foo_"
//   ST_MAKE_NAME(a,b)  => ST_MAKE_NAME_(ST_MAKE_PREFIX(a), b)
//   ST_MAKE_NAME_(a,b) => CppConcat(a, b)            // "foo_", "med3" -> "foo_med3"
//
// These exist only to build the per-caller symbol names
//   ST_MED3  = ST_MAKE_NAME(ST_SORT, med3)
//   ST_SWAP  = ST_MAKE_NAME(ST_SORT, swap)
//   ST_SWAPN = ST_MAKE_NAME(ST_SORT, swapn)
// and have no Rust counterpart.
// ---------------------------------------------------------------------------

// ---------------------------------------------------------------------------
// Conditional argument plumbing (all macro-parameterized, no concrete form):
//
//   ST_ELEMENT_TYPE_VOID defined:
//       ST_ELEMENT_TYPE            = void
//       ST_SORT_PROTO_ELEMENT_SIZE = ", size_t element_size"
//       ST_SORT_INVOKE_ELEMENT_SIZE= ", element_size"
//       ST_POINTER_TYPE            = uint8
//       ST_POINTER_STEP            = element_size      // runtime byte step
//   else:
//       ST_POINTER_TYPE            = ST_ELEMENT_TYPE
//       ST_POINTER_STEP            = 1                 // step in elements
//
//   ST_COMPARE_RUNTIME_POINTER defined:
//       typedef int (*ST_COMPARATOR_TYPE_NAME)(const ST_ELEMENT_TYPE *,
//                                               const ST_ELEMENT_TYPE * [, ARG]);
//       sort/med3 take a `compare` fn-pointer parameter.
//   ST_COMPARE_ARG_TYPE defined:
//       sort/med3/compare take an extra `ST_COMPARE_ARG_TYPE *arg`.
//
//   DO_COMPARE(a,b)    -> ST_COMPARE(a, b [, arg])
//   DO_MED3(a,b,c)     -> ST_MED3(a, b, c [, compare] [, arg])
//   DO_SORT(a,n)       -> ST_SORT(a, n [, element_size] [, compare] [, arg])
//   DO_SWAP / DO_SWAPN -> element or byte-wise swap depending on VOID mode
//   DO_CHECK_FOR_INTERRUPTS() -> CHECK_FOR_INTERRUPTS() iff ST_CHECK_FOR_INTERRUPTS
// ---------------------------------------------------------------------------

/// Doc-only faithful sketch of the generated, per-caller bodies.
///
/// The actual C is templated; this is the shape every instantiation expands to.
///
/// ```ignore
/// // ST_DECLARE: comparator typedef (only when ST_COMPARE_RUNTIME_POINTER):
/// pub type ST_COMPARATOR_TYPE_NAME =
///     Option<unsafe extern "C" fn(*const ST_ELEMENT_TYPE,
///                                 *const ST_ELEMENT_TYPE,
///                                 /* arg: */ *mut ST_COMPARE_ARG_TYPE) -> c_int>;
///
/// // ST_DECLARE: the sort prototype (optional args at end):
/// //   ST_SCOPE void ST_SORT(ST_ELEMENT_TYPE *first, size_t n
/// //                         [, size_t element_size]
/// //                         [, ST_COMPARATOR_TYPE_NAME compare]
/// //                         [, ST_COMPARE_ARG_TYPE *arg]);
///
/// // ST_DEFINE: median-of-three (pg_noinline). Comparator inlined here.
/// unsafe fn ST_MED3(a: *mut ST_ELEMENT_TYPE,
///                   b: *mut ST_ELEMENT_TYPE,
///                   c: *mut ST_ELEMENT_TYPE /*, compare, arg */)
///     -> *mut ST_ELEMENT_TYPE
/// {
///     if DO_COMPARE(a, b) < 0 {
///         if DO_COMPARE(b, c) < 0 { b }
///         else if DO_COMPARE(a, c) < 0 { c } else { a }
///     } else {
///         if DO_COMPARE(b, c) > 0 { b }
///         else if DO_COMPARE(a, c) < 0 { a } else { c }
///     }
/// }
///
/// // ST_DEFINE: single-element swap (ST_POINTER_TYPE is element or uint8).
/// unsafe fn ST_SWAP(a: *mut ST_POINTER_TYPE, b: *mut ST_POINTER_TYPE) {
///     let tmp = *a; *a = *b; *b = tmp;
/// }
///
/// // ST_DEFINE: n-wide swap (used for byte-wise void mode and block swaps).
/// unsafe fn ST_SWAPN(a: *mut ST_POINTER_TYPE, b: *mut ST_POINTER_TYPE, n: usize) {
///     for i in 0..n { ST_SWAP(a.add(i), b.add(i)); }
/// }
///
/// // ST_DEFINE: the qsort itself (Bentley-McIlroy, with presorted check and
/// // recurse-on-smaller-partition tail iteration). STEP = ST_POINTER_STEP.
/// unsafe fn ST_SORT(data: *mut ST_ELEMENT_TYPE, mut n: usize
///                   /*, element_size, compare, arg */)
/// {
///     let mut a = data as *mut ST_POINTER_TYPE;
///     // 'loop label:
///     loop {
///         DO_CHECK_FOR_INTERRUPTS();
///         // Insertion sort for small n.
///         if n < 7 {
///             let mut pm = a.add(STEP);
///             while pm < a.add(n * STEP) {
///                 let mut pl = pm;
///                 while pl > a && DO_COMPARE(pl.sub(STEP), pl) > 0 {
///                     DO_SWAP(pl, pl.sub(STEP));
///                     pl = pl.sub(STEP);
///                 }
///                 pm = pm.add(STEP);
///             }
///             return;
///         }
///         // Early-out if already sorted.
///         let mut presorted = 1;
///         { let mut pm = a.add(STEP);
///           while pm < a.add(n * STEP) {
///               DO_CHECK_FOR_INTERRUPTS();
///               if DO_COMPARE(pm.sub(STEP), pm) > 0 { presorted = 0; break; }
///               pm = pm.add(STEP);
///           } }
///         if presorted != 0 { return; }
///
///         // Pivot selection: median, or median-of-medians for large n.
///         let mut pm = a.add((n / 2) * STEP);
///         if n > 7 {
///             let mut pl = a;
///             let mut pn = a.add((n - 1) * STEP);
///             if n > 40 {
///                 let d = (n / 8) * STEP;
///                 pl = DO_MED3(pl, pl.add(d), pl.add(2 * d));
///                 pm = DO_MED3(pm.sub(d), pm, pm.add(d));
///                 pn = DO_MED3(pn.sub(2 * d), pn.sub(d), pn);
///             }
///             pm = DO_MED3(pl, pm, pn);
///         }
///         DO_SWAP(a, pm);
///
///         // 3-way partition (handles equal keys via pa/pd end swaps).
///         let (mut pa, mut pb) = (a.add(STEP), a.add(STEP));
///         let (mut pc, mut pd) = (a.add((n - 1) * STEP), a.add((n - 1) * STEP));
///         loop {
///             let mut r;
///             while pb <= pc && { r = DO_COMPARE(pb, a); r <= 0 } {
///                 if r == 0 { DO_SWAP(pa, pb); pa = pa.add(STEP); }
///                 pb = pb.add(STEP);
///                 DO_CHECK_FOR_INTERRUPTS();
///             }
///             while pb <= pc && { r = DO_COMPARE(pc, a); r >= 0 } {
///                 if r == 0 { DO_SWAP(pc, pd); pd = pd.sub(STEP); }
///                 pc = pc.sub(STEP);
///                 DO_CHECK_FOR_INTERRUPTS();
///             }
///             if pb > pc { break; }
///             DO_SWAP(pb, pc);
///             pb = pb.add(STEP);
///             pc = pc.sub(STEP);
///         }
///
///         // Move equal keys back to the middle.
///         let pn = a.add(n * STEP);
///         let mut d1 = Min(pa - a, pb - pa);
///         DO_SWAPN(a, pb.sub_bytes(d1), d1);
///         d1 = Min(pd - pc, pn - pd - STEP);
///         DO_SWAPN(pb, pn.sub_bytes(d1), d1);
///
///         // Recurse on the smaller partition, iterate on the larger.
///         d1 = pb - pa;
///         let d2 = pd - pc;
///         if d1 <= d2 {
///             if d1 > STEP { DO_SORT(a, d1 / STEP); }
///             if d2 > STEP { a = pn - d2; n = d2 / STEP; continue; }
///         } else {
///             if d2 > STEP { DO_SORT(pn - d2, d2 / STEP); }
///             if d1 > STEP { n = d1 / STEP; continue; }
///         }
///         return;
///     }
/// }
/// ```
///
/// NOTE: pointer arithmetic above is shown in element units for clarity; in the
/// ST_ELEMENT_TYPE_VOID instantiation ST_POINTER_TYPE is `uint8` and STEP is the
/// runtime `element_size`, so all stepping is byte-wise. This module emits no
/// compiled symbols - callers provide their own specialized sort.
pub mod sort_template_doc {}
