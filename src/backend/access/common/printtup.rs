//! Routines to print out tuples to the destination. Translated from
//! backend/access/common/printtup.c (disposition: full -- the
//! frontend/`DestRemote` path; the standalone-backend `debugtup`/`printatt`
//! interactive path and the binary (`format == 1`) output are grow guards).
//!
//! The receiver builds each protocol message with the SYNC `pqformat` builders
//! and appends it to the connection send buffer with the SYNC
//! [`pqcomm::pq_putmessage_sync`] -- it NEVER `.await`s, because the executor
//! (`ExecutePlan`) calls `receiveSlot` synchronously (rules.md s5). The async
//! socket flush happens later in the command loop. The C per-row `tmpcontext`
//! (memory recovered between rows) is tombstoned: Rust ownership frees the
//! per-row `String`/`PqMsg` at end of scope.

use std::sync::Arc;

use crate::access::tupdesc::TupleDesc;
use crate::catalog::genbki::INT4OID;
use crate::executor::tuptable::{slot_getallattrs, TupleTableSlot};
use crate::libpq::protocol::{PQMSG_DATA_ROW, PQMSG_ROW_DESCRIPTION};
use crate::nodes::nodes::CmdType;
use crate::postgres::Datum;
use crate::tcop::dest::{CommandDest, DestReceiver};

use crate::backend::libpq::pqcomm;
use crate::backend::libpq::pqformat::PqMsg;
use crate::backend::utils::fmgr::fmgr::OidOutputFunctionCall;
use crate::utils::lsyscache::getTypeOutputInfo;

/// Per-attribute output info (C `PrinttupAttrInfo`). `finfo` is folded into a
/// re-lookup via [`OidOutputFunctionCall`] for M1 (matches `debugtup`); caching
/// an `FmgrInfo` is a later optimization.
struct PrinttupAttrInfo {
    /// Oid of the type's text output function (used when `format == 0`).
    typoutput: crate::postgres_ext::Oid,
    /// Is the type varlena (toastable)? Unused on the M1 fixed-len int path.
    #[allow(dead_code, reason = "kept for parity; used once varlena/binary output grows")]
    typisvarlena: bool,
    /// Format code for this column (0 = text). Binary (1) is a grow guard.
    format: i16,
}

/// Private state for a printtup destination object (C `DR_printtup`).
///
/// The C struct carries a `Portal *` back-pointer used only to fetch
/// `portal->formats` and the targetlist for `SendRowDescriptionMessage`. On the
/// M1 SELECT path the targetlist is empty (a const projection has no source
/// table/column, so `resorigtbl`/`resorigcol` are zero) and the formats are all
/// text, so we hold the `formats` directly instead of a raw `Portal` pointer.
pub struct DRprinttup {
    /// CommandDest this receiver serves (DestRemote / DestRemoteExecute).
    mydest: CommandDest,
    /// Send a RowDescription at startup? (true for DestRemote, not Execute.)
    send_descrip: bool,
    /// Per-column format codes (from the portal). Empty => all text.
    formats: Vec<i16>,
    /// The TupleDesc we built `myinfo` for (C `attrinfo`); cached to detect a
    /// mid-stream type change. `None` is the C null sentinel (no info built yet);
    /// identity is compared with [`Arc::ptr_eq`] (C compared the raw pointer).
    attrinfo: Option<TupleDesc>,
    /// Per-column output info (C `myinfo`).
    myinfo: Vec<PrinttupAttrInfo>,
}

/// PG `printtup_create_DR`: construct a DestReceiver for DestRemote /
/// DestRemoteExecute. Sends the RowDescription automatically for DestRemote.
pub fn printtup_create_DR(dest: CommandDest) -> Box<dyn DestReceiver> {
    Box::new(DRprinttup {
        mydest: dest,
        send_descrip: dest == CommandDest::DestRemote,
        formats: Vec::new(),
        attrinfo: None,
        myinfo: Vec::new(),
    })
}

/// PG `SetRemoteDestReceiverParams`: bind a printtup receiver to its portal.
/// Here we transfer the portal's per-column format codes onto the receiver
/// (the only portal state printtup needs on the M1 path).
pub fn set_remote_dest_receiver_params(self_: &mut dyn DestReceiver, formats: &[i16]) {
    let my_state = self_
        .as_any_mut()
        .downcast_mut::<DRprinttup>()
        .unwrap_or_else(|| unreachable!("SetRemoteDestReceiverParams on a non-printtup receiver"));
    crate::assert!(
        my_state.mydest == CommandDest::DestRemote
            || my_state.mydest == CommandDest::DestRemoteExecute
    );
    my_state.formats = formats.to_vec();
}

impl DestReceiver for DRprinttup {
    /// PG `printtup_startup`: if we emit row descriptions, send the tuple
    /// descriptor of the tuples. The C `initStringInfo` / `tmpcontext` setup is
    /// tombstoned (per-message `PqMsg` is owned and dropped per call).
    fn r_startup(&mut self, _operation: CmdType, typeinfo: TupleDesc) {
        if self.send_descrip {
            send_row_description_message(&typeinfo, &self.formats);
        }
    }

    /// PG `printtup`: send one tuple to the client as a DataRow message.
    fn receive_slot(&mut self, slot: &mut TupleTableSlot) -> bool {
        let typeinfo = slot
            .tupleDescriptor
            .as_ref()
            .unwrap_or_else(|| unreachable!("printtup: slot has a tuple descriptor"));
        let natts = typeinfo.natts;

        // Set or update derived attribute info, if needed. C compared the cached
        // descriptor pointer; here the same identity test via Arc::ptr_eq (None =
        // the C null, i.e. nothing built yet).
        let changed = self
            .attrinfo
            .as_ref()
            .is_none_or(|a| !Arc::ptr_eq(a, typeinfo));
        if changed || self.myinfo.len() != natts as usize {
            let typeinfo = Arc::clone(typeinfo);
            self.prepare_info(&typeinfo, natts);
        }

        // Make sure the tuple is fully deconstructed.
        slot_getallattrs(slot);

        // Prepare a DataRow message.
        let mut buf = PqMsg::default();
        buf.begin_message(PQMSG_DATA_ROW);
        buf.send_int16(natts as u16);

        for i in 0..natts as usize {
            let this_state = &self.myinfo[i];
            if slot.isnull[i] {
                buf.send_int32(u32::MAX); // -1 length == SQL NULL
                continue;
            }
            let attr: Datum = slot.values[i];
            if this_state.format == 0 {
                // Text output.
                let outputstr = OidOutputFunctionCall(this_state.typoutput, attr);
                buf.send_counted_text(&outputstr);
            } else {
                // Binary output grows with SendFunctionCall / varlena.
                unimplemented!("printtup binary output (format == 1) deferred");
            }
        }

        pqcomm::pq_putmessage_sync(buf.msgtype, &buf.data);
        true
    }

    /// PG `printtup_shutdown`: drop the cached attr info. The C buffer/context
    /// frees are RAII here.
    fn r_shutdown(&mut self) {
        self.myinfo.clear();
        self.attrinfo = None;
    }

    fn mydest(&self) -> CommandDest {
        self.mydest
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

impl DRprinttup {
    /// PG `printtup_prepare_info`: compute the per-column output info. Caches an
    /// Arc clone of the descriptor it built `myinfo` for (C cached the pointer).
    fn prepare_info(&mut self, typeinfo: &TupleDesc, num_attrs: i32) {
        self.myinfo.clear();
        self.attrinfo = Some(Arc::clone(typeinfo));
        if num_attrs <= 0 {
            return;
        }
        let desc = &**typeinfo;
        self.myinfo = (0..num_attrs as usize)
            .map(|i| {
                let format = self.formats.get(i).copied().unwrap_or(0);
                let attr = desc.attr(i);
                if format == 0 {
                    let (typoutput, typisvarlena) = getTypeOutputInfo(attr.atttypid);
                    PrinttupAttrInfo { typoutput, typisvarlena, format }
                } else {
                    unimplemented!("printtup binary output info (format == 1) deferred");
                }
            })
            .collect();
    }
}

/// PG `SendRowDescriptionMessage`: send a RowDescription ('T') to the frontend.
///
/// The M1 targetlist is empty (a const projection carries no source
/// table/column), so `resorigtbl`/`resorigcol` are sent as zeroes for every
/// column; `formats` empty => format code 0 (text). Domains (the
/// `getBaseTypeAndTypmod` rewrite) are not reachable yet, so the attr's own
/// `atttypid`/`atttypmod` are sent directly.
pub fn send_row_description_message(typeinfo: &TupleDesc, formats: &[i16]) {
    let desc = &**typeinfo;
    let natts = desc.natts;

    let mut buf = PqMsg::default();
    buf.begin_message(PQMSG_ROW_DESCRIPTION);
    buf.send_int16(natts as u16);

    for i in 0..natts as usize {
        let att = desc.attr(i);
        let attname = name_str(&att.attname);
        let format = formats.get(i).copied().unwrap_or(0);

        buf.send_string(attname); // column name (null-terminated)
        buf.send_int32(0); // resorigtbl (table OID)
        buf.send_int16(0); // resorigcol (column attnum)
        buf.send_int32(att.atttypid.0); // type OID
        buf.send_int16(att.attlen as u16); // type length
        buf.send_int32(att.atttypmod as u32); // type modifier
        buf.send_int16(format as u16); // format code
    }

    pqcomm::pq_putmessage_sync(buf.msgtype, &buf.data);
}

/// Decode a NUL-padded `NameData` to its `&str` (up to the first NUL). M1 names
/// are ASCII (e.g. "?column?"), so a lossy decode is faithful.
fn name_str(name: &crate::c::NameData) -> &str {
    let bytes = &name.data;
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    std::str::from_utf8(&bytes[..end]).unwrap_or("")
}

/// Keep `INT4OID` referenced for the M1 result-column sanity it documents; the
/// type-output mapping itself lives in the `getTypeOutputInfo` shim.
#[allow(dead_code, reason = "documents the M1 int4 result column; mapping is in lsyscache shim")]
const _M1_RESULT_TYPE: crate::postgres_ext::Oid = INT4OID;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::backend::executor::execTuples::{exec_type_from_tl, make_tuple_table_slot};
    use crate::backend::libpq::pqcomm::{scope, PqComm};
    use crate::backend::nodes::makefuncs::{make_const, make_target_entry};
    use crate::executor::tuptable::TTSOpsVirtual;
    use crate::nodes::nodes::Node;
    use crate::postgres::Int32GetDatum;
    use crate::postgres_ext::InvalidOid;

    /// A `[Const int4]` targetlist (named "?column?"), mirroring execTuples tests.
    fn const_int4_tlist(values: &[i32]) -> Vec<Node> {
        values
            .iter()
            .enumerate()
            .map(|(i, &v)| {
                let con = make_const(INT4OID, -1, InvalidOid, 4, Int32GetDatum(v), false, true);
                let tle = make_target_entry(
                    Some(Node::Const(Box::new(con))),
                    (i + 1) as i16,
                    Some("?column?".to_string()),
                    false,
                );
                Node::TargetEntry(Box::new(tle))
            })
            .collect()
    }

    /// Drain whatever printtup appended into the send buffer, by flushing over a
    /// duplex and reading the bytes the client sees.
    async fn capture(body: impl FnOnce()) -> Vec<u8> {
        use tokio::io::AsyncReadExt;
        let (server, mut client) = tokio::io::duplex(64 * 1024);
        scope(Arc::new(PqComm::new(server)), async {
            body();
            crate::backend::libpq::pqcomm::pq_flush().await.unwrap();
            let mut buf = vec![0u8; 64 * 1024];
            let n = tokio::time::timeout(
                std::time::Duration::from_millis(200),
                client.read(&mut buf),
            )
            .await
            .unwrap()
            .unwrap();
            buf.truncate(n);
            buf
        })
        .await
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn row_description_bytes_for_one_int4_attr() {
        let desc = exec_type_from_tl(&const_int4_tlist(&[1]));
        let bytes = capture(|| send_row_description_message(&desc, &[])).await;

        // Expected: 'T' | len | natts(1) | "?column?\0" | resorigtbl(0) |
        //           resorigcol(0) | typoid(23) | attlen(4) | typmod(-1) | format(0)
        let mut body = Vec::new();
        body.extend_from_slice(&1u16.to_be_bytes());
        body.extend_from_slice(b"?column?\0");
        body.extend_from_slice(&0u32.to_be_bytes());
        body.extend_from_slice(&0u16.to_be_bytes());
        body.extend_from_slice(&23u32.to_be_bytes());
        body.extend_from_slice(&4u16.to_be_bytes());
        body.extend_from_slice(&(-1i32 as u32).to_be_bytes());
        body.extend_from_slice(&0u16.to_be_bytes());
        let mut expect = vec![PQMSG_ROW_DESCRIPTION];
        expect.extend_from_slice(&((body.len() as u32 + 4).to_be_bytes()));
        expect.extend_from_slice(&body);

        assert_eq!(bytes, expect);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn data_row_for_null_sends_minus_one_length() {
        let desc = exec_type_from_tl(&const_int4_tlist(&[1]));
        let mut slot = make_tuple_table_slot(Some(desc), &TTSOpsVirtual);
        // Mark the single attribute NULL and valid.
        slot.nvalid = 1;
        slot.values[0] = Datum(0);
        slot.isnull[0] = true;

        let mut dr = printtup_create_DR(CommandDest::DestRemoteExecute); // no T msg
        let bytes = capture(|| {
            dr.receive_slot(&mut slot);
        })
        .await;

        // 'D' | len | natts(1) | col0 length = -1
        let mut body = Vec::new();
        body.extend_from_slice(&1u16.to_be_bytes());
        body.extend_from_slice(&(-1i32 as u32).to_be_bytes());
        let mut expect = vec![PQMSG_DATA_ROW];
        expect.extend_from_slice(&((body.len() as u32 + 4).to_be_bytes()));
        expect.extend_from_slice(&body);
        assert_eq!(bytes, expect);
    }
}
