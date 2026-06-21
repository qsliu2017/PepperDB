//! Backend command-support subsystem (postgres/src/backend/commands +
//! postgres/src/include/commands).
//!
//! Only the trigger-manager type layer (trigger.h TriggerData + event macros) is
//! present so far; the command implementations are future work.

pub mod analyze;
pub mod copyfrom;
pub mod r#async;
pub mod copyto;
pub mod policy;
pub mod alter;
pub mod amcmds;
pub mod comment;
pub mod seclabel;
pub mod functioncmds;
pub mod opclasscmds;
pub mod conversioncmds;
pub mod schemacmds;
pub mod lockcmds;
pub mod proclang;
pub mod constraint;
pub mod copyapi;
pub mod dbcommands_xlog;
pub mod dbcommands;
pub mod copyfrom_internal;
pub mod defrem;
pub mod define;
pub mod discard;
pub mod explain_dr;
pub mod explain_state;
pub mod progress;
pub mod trigger;
pub mod portalcmds;
pub mod view;
pub mod aggregatecmds;
pub mod operatorcmds;
pub mod prepare;
pub mod createas;
pub mod explain_format;
pub mod statscmds;
pub mod matview;
pub mod collationcmds;
pub mod copy;
pub mod explain;
pub mod typecmds;
pub mod extension;
pub mod indexcmds;
pub mod vacuum;
pub mod vacuumparallel;

pub mod tablecmds;
pub mod sequence;
pub mod tablespace;
