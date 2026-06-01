//! SP-GiST access method (postgres/src/backend/access/spgist).
//!
//! So far: the geometric support procedures (`spgproc`), the point
//! opclasses (`spgquadtreeproc`, `spgkdtreeproc`), and the text suffix-tree
//! opclass (`spgtextproc`).

pub mod spgscan;
pub mod spgutils;
pub mod spginsert;
pub mod spgist;
pub mod spgist_private;
pub mod spgkdtreeproc;
pub mod spgproc;
pub mod spgquadtreeproc;
pub mod spgtextproc;
pub mod spgvalidate;
