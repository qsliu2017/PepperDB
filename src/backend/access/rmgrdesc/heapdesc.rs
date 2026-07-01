//! rmgr descriptor routines for access/heap/heapam.c. Translated from
//! backend/access/rmgrdesc/heapdesc.c: `heap_desc`/`heap_identify` +
//! `heap2_desc`/`heap2_identify`.

use std::fmt::Write as _;

use super::rmgr_info;
use super::util::{u16_at, u32_at, u8_at};
use crate::access::heapam_xlog::{
    XLHL_KEYS_UPDATED, XLHL_XMAX_EXCL_LOCK, XLHL_XMAX_IS_MULTI, XLHL_XMAX_KEYSHR_LOCK,
    XLHL_XMAX_LOCK_ONLY, XLH_TRUNCATE_CASCADE, XLH_TRUNCATE_RESTART_SEQS, XLOG_HEAP2_LOCK_UPDATED,
    XLOG_HEAP2_MULTI_INSERT, XLOG_HEAP2_NEW_CID, XLOG_HEAP2_PRUNE_ON_ACCESS,
    XLOG_HEAP2_PRUNE_VACUUM_CLEANUP, XLOG_HEAP2_PRUNE_VACUUM_SCAN, XLOG_HEAP2_VISIBLE,
    XLOG_HEAP_CONFIRM, XLOG_HEAP_DELETE, XLOG_HEAP_HOT_UPDATE, XLOG_HEAP_INIT_PAGE,
    XLOG_HEAP_INPLACE, XLOG_HEAP_INSERT, XLOG_HEAP_LOCK, XLOG_HEAP_OPMASK, XLOG_HEAP_TRUNCATE,
    XLOG_HEAP_UPDATE, XLHP_HAS_CONFLICT_HORIZON, XLHP_IS_CATALOG_REL,
};
use crate::access::xlogreader::DecodedXLogRecord;

/// C `infobits_desc`: render an xl-heap-lock infobits byte as `"key: [A, B, ...]"`.
fn infobits_desc(infobits: u8, keyname: &str) -> String {
    let mut parts: Vec<&str> = Vec::new();
    if infobits & XLHL_XMAX_IS_MULTI != 0 {
        parts.push("IS_MULTI");
    }
    if infobits & XLHL_XMAX_LOCK_ONLY != 0 {
        parts.push("LOCK_ONLY");
    }
    if infobits & XLHL_XMAX_EXCL_LOCK != 0 {
        parts.push("EXCL_LOCK");
    }
    if infobits & XLHL_XMAX_KEYSHR_LOCK != 0 {
        parts.push("KEYSHR_LOCK");
    }
    if infobits & crate::access::heapam_xlog::XLHL_KEYS_UPDATED != 0 {
        parts.push("KEYS_UPDATED");
    }
    let _ = XLHL_KEYS_UPDATED;
    format!("{keyname}: [{}]", parts.join(", "))
}

/// C `truncate_flags_desc`: render an xl_heap_truncate flags byte.
fn truncate_flags_desc(flags: u8) -> String {
    let mut parts: Vec<&str> = Vec::new();
    if flags & XLH_TRUNCATE_CASCADE != 0 {
        parts.push("CASCADE");
    }
    if flags & XLH_TRUNCATE_RESTART_SEQS != 0 {
        parts.push("RESTART_SEQS");
    }
    format!("flags: [{}]", parts.join(", "))
}

/// C `heap_desc`.
#[must_use]
pub fn heap_desc(record: &DecodedXLogRecord) -> String {
    let rec = record.get_data().unwrap_or(&[]);
    let info = rmgr_info(record.header.info) & XLOG_HEAP_OPMASK;

    if info == XLOG_HEAP_INSERT {
        // xl_heap_insert { offnum: u16, flags: u8 }
        let offnum = u16_at(rec, 0);
        let flags = u8_at(rec, 2);
        format!("off: {offnum}, flags: 0x{flags:02X}")
    } else if info == XLOG_HEAP_DELETE {
        // xl_heap_delete { xmax: u32, offnum: u16, infobits_set: u8, flags: u8 }
        let xmax = u32_at(rec, 0);
        let offnum = u16_at(rec, 4);
        let infobits = u8_at(rec, 6);
        let flags = u8_at(rec, 7);
        format!(
            "xmax: {xmax}, off: {offnum}, {}, flags: 0x{flags:02X}",
            infobits_desc(infobits, "infobits")
        )
    } else if info == XLOG_HEAP_UPDATE || info == XLOG_HEAP_HOT_UPDATE {
        // xl_heap_update { old_xmax: u32, old_offnum: u16, old_infobits_set: u8,
        //                  flags: u8, new_xmax: u32, new_offnum: u16 }
        let old_xmax = u32_at(rec, 0);
        let old_offnum = u16_at(rec, 4);
        let old_infobits = u8_at(rec, 6);
        let flags = u8_at(rec, 7);
        let new_xmax = u32_at(rec, 8);
        let new_offnum = u16_at(rec, 12);
        format!(
            "old_xmax: {old_xmax}, old_off: {old_offnum}, {}, flags: 0x{flags:02X}, new_xmax: {new_xmax}, new_off: {new_offnum}",
            infobits_desc(old_infobits, "old_infobits")
        )
    } else if info == XLOG_HEAP_TRUNCATE {
        // xl_heap_truncate { dbId: u32, nrelids: u32, flags: u8, relids: [Oid] }
        let nrelids = u32_at(rec, 4);
        let flags = u8_at(rec, 8);
        let mut out = format!("{}, nrelids: {nrelids}", truncate_flags_desc(flags));
        out.push_str(", relids:");
        // relids FAM begins at offset 9 (offsetof flags + 1).
        for i in 0..nrelids as usize {
            let _ = write!(out, " {}", u32_at(rec, 9 + i * 4));
        }
        out
    } else if info == XLOG_HEAP_CONFIRM {
        // xl_heap_confirm { offnum: u16 }
        format!("off: {}", u16_at(rec, 0))
    } else if info == XLOG_HEAP_LOCK {
        // xl_heap_lock { xmax: u32, offnum: u16, infobits_set: u8, flags: u8 }
        let xmax = u32_at(rec, 0);
        let offnum = u16_at(rec, 4);
        let infobits = u8_at(rec, 6);
        let flags = u8_at(rec, 7);
        format!(
            "xmax: {xmax}, off: {offnum}, {}, flags: 0x{flags:02X}",
            infobits_desc(infobits, "infobits")
        )
    } else if info == XLOG_HEAP_INPLACE {
        // xl_heap_inplace { offnum: u16, ... }; inval-message rendering deferred.
        format!("off: {}", u16_at(rec, 0))
    } else {
        String::new()
    }
}

/// C `heap2_desc`.
#[must_use]
pub fn heap2_desc(record: &DecodedXLogRecord) -> String {
    let rec = record.get_data().unwrap_or(&[]);
    let info = rmgr_info(record.header.info) & XLOG_HEAP_OPMASK;

    if info == XLOG_HEAP2_PRUNE_ON_ACCESS
        || info == XLOG_HEAP2_PRUNE_VACUUM_SCAN
        || info == XLOG_HEAP2_PRUNE_VACUUM_CLEANUP
    {
        // xl_heap_prune { reason: u8, flags: u8 } then optional conflict horizon.
        let flags = u8_at(rec, 1);
        let mut out = String::new();
        if flags & XLHP_HAS_CONFLICT_HORIZON != 0 {
            // conflict horizon XID follows SizeOfHeapPrune (2), unaligned.
            let conflict_xid = u32_at(rec, 2);
            let _ = write!(out, "snapshotConflictHorizon: {conflict_xid}");
        }
        let _ = write!(
            out,
            ", isCatalogRel: {}",
            if flags & XLHP_IS_CATALOG_REL != 0 { 'T' } else { 'F' }
        );
        // The per-item arrays (plans/redirected/dead/unused) live in block-0
        // data; their full rendering is deferred (heap2 prune AM not yet ported).
        out
    } else if info == XLOG_HEAP2_VISIBLE {
        // xl_heap_visible { snapshotConflictHorizon: u32, flags: u8 }
        let horizon = u32_at(rec, 0);
        let flags = u8_at(rec, 4);
        format!("snapshotConflictHorizon: {horizon}, flags: 0x{flags:02X}")
    } else if info == XLOG_HEAP2_MULTI_INSERT {
        // xl_heap_multi_insert { flags: u8, ntuples: u16, offsets: [u16] }
        let flags = u8_at(rec, 0);
        let ntuples = u16_at(rec, 2);
        let isinit = (rmgr_info(record.header.info) & XLOG_HEAP_INIT_PAGE) != 0;
        let mut out = format!("ntuples: {ntuples}, flags: 0x{flags:02X}");
        if record.get_block_data(0).is_some() && !isinit {
            out.push_str(", offsets:");
            for i in 0..ntuples as usize {
                let _ = write!(out, " {}", u16_at(rec, 4 + i * 2));
            }
        }
        out
    } else if info == XLOG_HEAP2_LOCK_UPDATED {
        // xl_heap_lock_updated { xmax: u32, offnum: u16, infobits_set: u8, flags: u8 }
        let xmax = u32_at(rec, 0);
        let offnum = u16_at(rec, 4);
        let infobits = u8_at(rec, 6);
        let flags = u8_at(rec, 7);
        format!(
            "xmax: {xmax}, off: {offnum}, {}, flags: 0x{flags:02X}",
            infobits_desc(infobits, "infobits")
        )
    } else if info == XLOG_HEAP2_NEW_CID {
        // xl_heap_new_cid { top_xid: u32, cmin: u32, cmax: u32, combocid: u32,
        //   target_locator: RelFileLocator(3xu32), target_tid: ItemPointerData }
        let cmin = u32_at(rec, 4);
        let cmax = u32_at(rec, 8);
        let combo = u32_at(rec, 12);
        let spc = u32_at(rec, 16);
        let db = u32_at(rec, 20);
        let rel = u32_at(rec, 24);
        // ItemPointerData: block (hi u16, lo u16) + offset u16 at 28.
        let blk_hi = u16_at(rec, 28);
        let blk_lo = u16_at(rec, 30);
        let blk = (u32::from(blk_hi) << 16) | u32::from(blk_lo);
        let off = u16_at(rec, 32);
        format!(
            "rel: {spc}/{db}/{rel}, tid: {blk}/{off}, cmin: {cmin}, cmax: {cmax}, combo: {combo}"
        )
    } else {
        String::new()
    }
}

/// C `heap_identify`.
#[must_use]
pub fn heap_identify(info: u8) -> Option<&'static str> {
    Some(match rmgr_info(info) {
        x if x == XLOG_HEAP_INSERT => "INSERT",
        x if x == XLOG_HEAP_INSERT | XLOG_HEAP_INIT_PAGE => "INSERT+INIT",
        x if x == XLOG_HEAP_DELETE => "DELETE",
        x if x == XLOG_HEAP_UPDATE => "UPDATE",
        x if x == XLOG_HEAP_UPDATE | XLOG_HEAP_INIT_PAGE => "UPDATE+INIT",
        x if x == XLOG_HEAP_HOT_UPDATE => "HOT_UPDATE",
        x if x == XLOG_HEAP_HOT_UPDATE | XLOG_HEAP_INIT_PAGE => "HOT_UPDATE+INIT",
        x if x == XLOG_HEAP_TRUNCATE => "TRUNCATE",
        x if x == XLOG_HEAP_CONFIRM => "HEAP_CONFIRM",
        x if x == XLOG_HEAP_LOCK => "LOCK",
        x if x == XLOG_HEAP_INPLACE => "INPLACE",
        _ => return None,
    })
}

/// C `heap2_identify`.
#[must_use]
pub fn heap2_identify(info: u8) -> Option<&'static str> {
    Some(match rmgr_info(info) {
        x if x == XLOG_HEAP2_PRUNE_ON_ACCESS => "PRUNE_ON_ACCESS",
        x if x == XLOG_HEAP2_PRUNE_VACUUM_SCAN => "PRUNE_VACUUM_SCAN",
        x if x == XLOG_HEAP2_PRUNE_VACUUM_CLEANUP => "PRUNE_VACUUM_CLEANUP",
        x if x == XLOG_HEAP2_VISIBLE => "VISIBLE",
        x if x == XLOG_HEAP2_MULTI_INSERT => "MULTI_INSERT",
        x if x == XLOG_HEAP2_MULTI_INSERT | XLOG_HEAP_INIT_PAGE => "MULTI_INSERT+INIT",
        x if x == XLOG_HEAP2_LOCK_UPDATED => "LOCK_UPDATED",
        x if x == XLOG_HEAP2_NEW_CID => "NEW_CID",
        x if x == crate::access::heapam_xlog::XLOG_HEAP2_REWRITE => "REWRITE",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::access::rmgrdesc::test_util::record_with_data;

    #[test]
    fn heap_insert_renders() {
        // xl_heap_insert { offnum: 5, flags: 0x03 }
        let mut data = Vec::new();
        data.extend_from_slice(&5u16.to_ne_bytes());
        data.push(0x03);
        let rec = record_with_data(10, XLOG_HEAP_INSERT, &data);
        assert_eq!(heap_identify(XLOG_HEAP_INSERT), Some("INSERT"));
        assert_eq!(heap_desc(&rec), "off: 5, flags: 0x03");
    }

    #[test]
    fn heap_delete_renders_infobits() {
        // xl_heap_delete { xmax: 42, offnum: 7, infobits: IS_MULTI, flags: 0x01 }
        let mut data = Vec::new();
        data.extend_from_slice(&42u32.to_ne_bytes());
        data.extend_from_slice(&7u16.to_ne_bytes());
        data.push(XLHL_XMAX_IS_MULTI);
        data.push(0x01);
        let rec = record_with_data(10, XLOG_HEAP_DELETE, &data);
        assert_eq!(heap_identify(XLOG_HEAP_DELETE), Some("DELETE"));
        assert_eq!(
            heap_desc(&rec),
            "xmax: 42, off: 7, infobits: [IS_MULTI], flags: 0x01"
        );
    }

    #[test]
    fn heap2_visible_renders() {
        let mut data = Vec::new();
        data.extend_from_slice(&99u32.to_ne_bytes());
        data.push(0x02);
        let rec = record_with_data(9, XLOG_HEAP2_VISIBLE, &data);
        assert_eq!(heap2_identify(XLOG_HEAP2_VISIBLE), Some("VISIBLE"));
        assert_eq!(
            heap2_desc(&rec),
            "snapshotConflictHorizon: 99, flags: 0x02"
        );
    }

    #[test]
    fn identify_init_page_variant() {
        assert_eq!(
            heap_identify(XLOG_HEAP_INSERT | XLOG_HEAP_INIT_PAGE),
            Some("INSERT+INIT")
        );
    }
}
