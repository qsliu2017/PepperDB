//! Support for communication destinations. Translated from
//! backend/tcop/dest.c (disposition: grow).
//!
//! The destination switch (`CreateDestReceiver`) is correct-for-reachable: the
//! `DestRemote`/`DestRemoteExecute` -> printtup arm and the stateless
//! `DestNone`/`DestDebug` arms are COMPLETE; the SPI/tuplestore/COPY/INTO/queue
//! arms are clean grow guards (rules.md s4). `BeginCommand`/`NullCommand` are
//! faithful; `EndCommand` (CommandComplete) and `ReadyForQuery` reach the wire
//! via the SYNC append / async flush split of step 03 (rules.md s5).

use crate::access::tupdesc::TupleDesc;
use crate::executor::tuptable::TupleTableSlot;
use crate::libpq::protocol::{
    PQMSG_COMMAND_COMPLETE, PQMSG_EMPTY_QUERY_RESPONSE, PQMSG_READY_FOR_QUERY,
};
use crate::nodes::nodes::CmdType;
use crate::tcop::cmdtag::QueryCompletion;
use crate::tcop::cmdtaglist::{CommandTag, COMMAND_TAGS};
use crate::tcop::dest::{CommandDest, DestReceiver};

use crate::backend::access::common::printtup::printtup_create_DR;
use crate::backend::libpq::pqcomm;

/// A do-nothing receiver (C `donothingDR`, the `DestNone` global). Used for
/// `DestNone` and as the initial QueryDesc destination in `PortalStart`.
pub struct NoneReceiver;

impl DestReceiver for NoneReceiver {
    fn receive_slot(&mut self, _slot: &mut TupleTableSlot) -> bool {
        true
    }
    fn r_startup(&mut self, _operation: CmdType, _typeinfo: TupleDesc) {}
    fn r_shutdown(&mut self) {}
    fn mydest(&self) -> CommandDest {
        CommandDest::DestNone
    }
}

/// PG `CreateDestReceiver`: return the receiver function set for `dest`.
///
/// `DestRemote`/`DestRemoteExecute` build a printtup receiver; `DestNone`/
/// `DestDebug` get the stateless do-nothing receiver (debug's interactive
/// printing grows with the standalone backend). The remaining destinations are
/// grow guards.
pub fn create_dest_receiver(dest: CommandDest) -> Box<dyn DestReceiver> {
    match dest {
        CommandDest::DestRemote | CommandDest::DestRemoteExecute => printtup_create_DR(dest),
        CommandDest::DestNone | CommandDest::DestDebug => Box::new(NoneReceiver),
        CommandDest::DestRemoteSimple => unimplemented!("CreateDestReceiver: DestRemoteSimple (printsimple) deferred"),
        CommandDest::DestSPI => unimplemented!("CreateDestReceiver: DestSPI deferred"),
        CommandDest::DestTuplestore => unimplemented!("CreateDestReceiver: DestTuplestore deferred"),
        CommandDest::DestIntoRel => unimplemented!("CreateDestReceiver: DestIntoRel deferred"),
        CommandDest::DestCopyOut => unimplemented!("CreateDestReceiver: DestCopyOut deferred"),
        CommandDest::DestSQLFunction => unimplemented!("CreateDestReceiver: DestSQLFunction deferred"),
        CommandDest::DestTransientRel => unimplemented!("CreateDestReceiver: DestTransientRel deferred"),
        CommandDest::DestTupleQueue => unimplemented!("CreateDestReceiver: DestTupleQueue deferred"),
        CommandDest::DestExplainSerialize => unimplemented!("CreateDestReceiver: DestExplainSerialize deferred"),
    }
}

/// PG `BeginCommand`: initialize the destination at start of command. Nothing to
/// do at present (matches C).
pub fn begin_command(_command_tag: CommandTag, _dest: CommandDest) {}

/// PG `EndCommand`: send the CommandComplete tag to the frontend for the remote
/// destinations. SYNC append (the executor/portal path is sync); the async flush
/// happens in the command loop.
pub fn end_command(qc: &QueryCompletion, dest: CommandDest, force_undecorated_output: bool) {
    match dest {
        CommandDest::DestRemote
        | CommandDest::DestRemoteExecute
        | CommandDest::DestRemoteSimple => {
            let mut tag = build_query_completion_string(qc, force_undecorated_output);
            tag.push('\0'); // C sends the NUL terminator (len + 1)
            pqcomm::pq_putmessage_sync(PQMSG_COMMAND_COMPLETE, tag.as_bytes());
        }
        _ => {}
    }
}

/// PG `EndReplicationCommand`: stripped-down EndCommand for replication. Deferred.
pub fn end_replication_command(_command_tag: &str) {
    unimplemented!("EndReplicationCommand: replication deferred")
}

/// PG `NullCommand`: tell the dest an empty query string was recognized.
pub fn null_command(dest: CommandDest) {
    match dest {
        CommandDest::DestRemote
        | CommandDest::DestRemoteExecute
        | CommandDest::DestRemoteSimple => {
            pqcomm::pq_putmessage_sync(PQMSG_EMPTY_QUERY_RESPONSE, &[]);
        }
        _ => {}
    }
}

/// PG `ReadyForQuery`: tell the dest we are ready for a new query and flush. The
/// 'Z' message carries the transaction-block status byte. ASYNC: it ends the
/// command cycle with a socket flush (rules.md s5).
pub async fn ready_for_query(dest: CommandDest) {
    match dest {
        CommandDest::DestRemote
        | CommandDest::DestRemoteExecute
        | CommandDest::DestRemoteSimple => {
            let status = crate::backend::access::transam::xact::TransactionBlockStatusCode() as u8;
            pqcomm::pq_putmessage_sync(PQMSG_READY_FOR_QUERY, &[status]);
            // Flush output at end of cycle in any case.
            let _ = pqcomm::pq_flush().await;
        }
        _ => {}
    }
}

/// Build the command-completion tag string (C `BuildQueryCompletionString`).
///
/// For tags whose `display_rowcount` is set, the row count is appended:
/// `"SELECT 1"`. Some tags additionally carry an Oid before the count
/// (only the legacy `INSERT 0 N` form); that grows with INSERT. For M1 (SELECT)
/// the form is `"<TAG> <nprocessed>"`.
fn build_query_completion_string(qc: &QueryCompletion, force_undecorated_output: bool) -> String {
    let behavior = tag_behavior(qc.command_tag);
    let name = behavior.name;
    if force_undecorated_output || !behavior.display_rowcount {
        return name.to_string();
    }
    // INSERT prepends a 0 Oid (legacy); other counted tags do not. INSERT grows
    // later; M1 SELECT takes the simple "<name> <n>" form.
    if qc.command_tag == CommandTag::Insert {
        format!("{name} 0 {}", qc.nprocessed)
    } else {
        format!("{name} {}", qc.nprocessed)
    }
}

/// Look up a command tag's behavior row in the generated table.
fn tag_behavior(tag: CommandTag) -> &'static crate::tcop::cmdtaglist::CommandTagBehavior {
    COMMAND_TAGS
        .iter()
        .find(|b| b.tag == tag)
        .unwrap_or_else(|| unreachable!("every CommandTag has a COMMAND_TAGS row"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_completion_tag_has_rowcount() {
        let qc = QueryCompletion { command_tag: CommandTag::Select, nprocessed: 1 };
        assert_eq!(build_query_completion_string(&qc, false), "SELECT 1");
    }

    #[test]
    fn select_completion_tag_42() {
        let qc = QueryCompletion { command_tag: CommandTag::Select, nprocessed: 42 };
        assert_eq!(build_query_completion_string(&qc, false), "SELECT 42");
    }

    #[test]
    fn create_remote_receiver_is_printtup() {
        let dr = create_dest_receiver(CommandDest::DestRemote);
        assert_eq!(dr.mydest(), CommandDest::DestRemote);
    }

    #[test]
    fn create_none_receiver() {
        let dr = create_dest_receiver(CommandDest::DestNone);
        assert_eq!(dr.mydest(), CommandDest::DestNone);
    }
}
