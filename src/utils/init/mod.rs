//! Backend initialization (postgres/src/backend/utils/init).
//!
//! So far: process-global variables (`globals`) and the untrusted-user context
//! switch (`usercontext`).

pub mod postinit;
pub mod globals;
pub mod usercontext;
