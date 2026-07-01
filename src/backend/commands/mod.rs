//! Directory module: src/backend/commands

pub mod alter;
pub mod analyze;
pub mod cluster;
pub mod collationcmds;
pub mod comment;
pub mod conversioncmds;
pub mod copy;
pub mod copyfrom;
pub mod copyfromparse;
pub mod copyto;
pub mod dbcommands;
pub mod define;
pub mod dropcmds;
pub mod functioncmds;
pub mod indexcmds;
pub mod portalcmds;
pub mod prepare;
pub mod schemacmds;
pub mod sequence;
pub mod tablecmds;
pub mod tablespace;
pub mod trigger;
pub mod typecmds;
pub mod vacuum;
pub mod variable;
pub mod view;

#[cfg(test)]
mod fk_tests;
#[cfg(test)]
mod vacuum_tests;
