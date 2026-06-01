//! Precompiled-header aggregators (postgres/src/include/pch).
//!
//! These headers exist only to speed up the C build; each body is a single
//! `#include` and defines no symbols. Translated as faithful doc-only modules.

pub mod c_pch;
pub mod postgres_fe_pch;
pub mod postgres_pch;
