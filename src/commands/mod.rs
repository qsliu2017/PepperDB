//! Directory module: src/include/commands

// === scaffold: child modules (Phase 0) ===
pub mod alter;
#[path = "async.rs"]
pub mod r#async;
pub mod cluster;
pub mod collationcmds;
pub mod comment;
pub mod conversioncmds;
pub mod copy;
pub mod copyapi;
pub mod copyfrom_internal;
pub mod createas;
pub mod dbcommands;
pub mod dbcommands_xlog;
pub mod defrem;
pub mod discard;
pub mod event_trigger;
pub mod explain;
pub mod explain_dr;
pub mod explain_format;
pub mod explain_state;
pub mod extension;
pub mod lockcmds;
pub mod matview;
pub mod policy;
pub mod portalcmds;
pub mod prepare;
pub mod proclang;
pub mod progress;
pub mod publicationcmds;
pub mod schemacmds;
pub mod seclabel;
pub mod sequence;
pub mod subscriptioncmds;
pub mod tablecmds;
pub mod tablespace;
pub mod trigger;
pub mod typecmds;
pub mod user;
pub mod vacuum;
pub mod view;
// === end scaffold ===
