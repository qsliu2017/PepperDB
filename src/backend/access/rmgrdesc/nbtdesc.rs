//! rmgr descriptor routines for access/nbtree/nbtxlog.c. Translated from
//! backend/access/rmgrdesc/nbtdesc.c: `btree_desc` / `btree_identify`.

use super::rmgr_info;
use super::util::{u16_at, u32_at};
use crate::access::nbtxlog::{
    XLOG_BTREE_DEDUP, XLOG_BTREE_DELETE, XLOG_BTREE_INSERT_LEAF, XLOG_BTREE_INSERT_META,
    XLOG_BTREE_INSERT_POST, XLOG_BTREE_INSERT_UPPER, XLOG_BTREE_MARK_PAGE_HALFDEAD,
    XLOG_BTREE_META_CLEANUP, XLOG_BTREE_NEWROOT, XLOG_BTREE_REUSE_PAGE, XLOG_BTREE_SPLIT_L,
    XLOG_BTREE_SPLIT_R, XLOG_BTREE_UNLINK_PAGE, XLOG_BTREE_UNLINK_PAGE_META, XLOG_BTREE_VACUUM,
};
use crate::access::xlogreader::DecodedXLogRecord;

/// C `btree_desc`.
#[must_use]
pub fn btree_desc(record: &DecodedXLogRecord) -> String {
    let rec = record.get_data().unwrap_or(&[]);
    let info = rmgr_info(record.header.info);

    match info {
        x if x == XLOG_BTREE_INSERT_LEAF
            || x == XLOG_BTREE_INSERT_UPPER
            || x == XLOG_BTREE_INSERT_META
            || x == XLOG_BTREE_INSERT_POST =>
        {
            // xl_btree_insert { offnum: u16 }
            format!("off: {}", u16_at(rec, 0))
        }
        x if x == XLOG_BTREE_SPLIT_L || x == XLOG_BTREE_SPLIT_R => {
            // xl_btree_split { level: u32, firstrightoff: u16, newitemoff: u16, postingoff: u16 }
            let level = u32_at(rec, 0);
            let firstrightoff = u16_at(rec, 4);
            let newitemoff = u16_at(rec, 6);
            let postingoff = u16_at(rec, 8);
            format!(
                "level: {level}, firstrightoff: {firstrightoff}, newitemoff: {newitemoff}, postingoff: {postingoff}"
            )
        }
        x if x == XLOG_BTREE_DEDUP => {
            // xl_btree_dedup { nintervals: u16 }
            format!("nintervals: {}", u16_at(rec, 0))
        }
        x if x == XLOG_BTREE_VACUUM => {
            // xl_btree_vacuum { ndeleted: u16, nupdated: u16 }
            format!("ndeleted: {}, nupdated: {}", u16_at(rec, 0), u16_at(rec, 2))
        }
        x if x == XLOG_BTREE_DELETE => {
            // xl_btree_delete { snapshotConflictHorizon: u32, ndeleted: u16, nupdated: u16, isCatalogRel: bool }
            let horizon = u32_at(rec, 0);
            let ndeleted = u16_at(rec, 4);
            let nupdated = u16_at(rec, 6);
            let is_catalog = rec.get(8).copied().unwrap_or(0) != 0;
            format!(
                "snapshotConflictHorizon: {horizon}, ndeleted: {ndeleted}, nupdated: {nupdated}, isCatalogRel: {}",
                if is_catalog { 'T' } else { 'F' }
            )
        }
        x if x == XLOG_BTREE_MARK_PAGE_HALFDEAD => {
            // xl_btree_mark_page_halfdead { poffset: u16, leafblk: u32, leftblk: u32, rightblk: u32, topparent: u32 }
            // Rendered order: topparent, leaf, left, right.
            let leafblk = u32_at(rec, 4);
            let leftblk = u32_at(rec, 8);
            let rightblk = u32_at(rec, 12);
            let topparent = u32_at(rec, 16);
            format!("topparent: {topparent}, leaf: {leafblk}, left: {leftblk}, right: {rightblk}")
        }
        x if x == XLOG_BTREE_UNLINK_PAGE_META || x == XLOG_BTREE_UNLINK_PAGE => {
            // xl_btree_unlink_page { leftsib: u32, rightsib: u32, level: u32,
            //   safexid: FullTransactionId(u64), leafleftsib: u32, leafrightsib: u32, leaftopparent: u32 }
            let leftsib = u32_at(rec, 0);
            let rightsib = u32_at(rec, 4);
            let level = u32_at(rec, 8);
            let safexid = super::util::u64_at(rec, 12);
            let epoch = (safexid >> 32) as u32;
            let xid = safexid as u32;
            let leafleft = u32_at(rec, 20);
            let leafright = u32_at(rec, 24);
            let leaftopparent = u32_at(rec, 28);
            format!(
                "left: {leftsib}, right: {rightsib}, level: {level}, safexid: {epoch}:{xid}, leafleft: {leafleft}, leafright: {leafright}, leaftopparent: {leaftopparent}"
            )
        }
        x if x == XLOG_BTREE_NEWROOT => {
            // xl_btree_newroot { rootblk: u32, level: u32 }
            format!("level: {}", u32_at(rec, 4))
        }
        x if x == XLOG_BTREE_REUSE_PAGE => {
            // xl_btree_reuse_page { locator: RelFileLocator(3xu32), block: u32,
            //   snapshotConflictHorizon: FullTransactionId(u64), isCatalogRel: bool }
            let spc = u32_at(rec, 0);
            let db = u32_at(rec, 4);
            let rel = u32_at(rec, 8);
            let horizon = super::util::u64_at(rec, 16);
            let epoch = (horizon >> 32) as u32;
            let xid = horizon as u32;
            let is_catalog = rec.get(24).copied().unwrap_or(0) != 0;
            format!(
                "rel: {spc}/{db}/{rel}, snapshotConflictHorizon: {epoch}:{xid}, isCatalogRel: {}",
                if is_catalog { 'T' } else { 'F' }
            )
        }
        x if x == XLOG_BTREE_META_CLEANUP => {
            // xl_btree_metadata lives in block-0 data; last_cleanup_num_delpages
            // is at offsetof (version:u32, root:u32, level:u32, fastroot:u32,
            // fastlevel:u32, last_cleanup_num_delpages:u32 -> offset 20).
            let block0 = record.get_block_data(0).unwrap_or(&[]);
            format!("last_cleanup_num_delpages: {}", u32_at(block0, 20))
        }
        _ => String::new(),
    }
}

/// C `btree_identify`.
#[must_use]
pub fn btree_identify(info: u8) -> Option<&'static str> {
    Some(match rmgr_info(info) {
        x if x == XLOG_BTREE_INSERT_LEAF => "INSERT_LEAF",
        x if x == XLOG_BTREE_INSERT_UPPER => "INSERT_UPPER",
        x if x == XLOG_BTREE_INSERT_META => "INSERT_META",
        x if x == XLOG_BTREE_SPLIT_L => "SPLIT_L",
        x if x == XLOG_BTREE_SPLIT_R => "SPLIT_R",
        x if x == XLOG_BTREE_INSERT_POST => "INSERT_POST",
        x if x == XLOG_BTREE_DEDUP => "DEDUP",
        x if x == XLOG_BTREE_VACUUM => "VACUUM",
        x if x == XLOG_BTREE_DELETE => "DELETE",
        x if x == XLOG_BTREE_MARK_PAGE_HALFDEAD => "MARK_PAGE_HALFDEAD",
        x if x == XLOG_BTREE_UNLINK_PAGE => "UNLINK_PAGE",
        x if x == XLOG_BTREE_UNLINK_PAGE_META => "UNLINK_PAGE_META",
        x if x == XLOG_BTREE_NEWROOT => "NEWROOT",
        x if x == XLOG_BTREE_REUSE_PAGE => "REUSE_PAGE",
        x if x == XLOG_BTREE_META_CLEANUP => "META_CLEANUP",
        _ => return None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::access::rmgrdesc::test_util::record_with_data;

    const BTREE_RMID: u8 = 11;

    #[test]
    fn insert_leaf_renders_off() {
        let data = 7u16.to_ne_bytes();
        let rec = record_with_data(BTREE_RMID, XLOG_BTREE_INSERT_LEAF, &data);
        assert_eq!(btree_identify(XLOG_BTREE_INSERT_LEAF), Some("INSERT_LEAF"));
        assert_eq!(btree_desc(&rec), "off: 7");
    }

    #[test]
    fn newroot_renders_level() {
        let mut data = Vec::new();
        data.extend_from_slice(&3u32.to_ne_bytes()); // rootblk
        data.extend_from_slice(&2u32.to_ne_bytes()); // level
        let rec = record_with_data(BTREE_RMID, XLOG_BTREE_NEWROOT, &data);
        assert_eq!(btree_identify(XLOG_BTREE_NEWROOT), Some("NEWROOT"));
        assert_eq!(btree_desc(&rec), "level: 2");
    }

    #[test]
    fn split_renders_fields() {
        let mut data = Vec::new();
        data.extend_from_slice(&1u32.to_ne_bytes()); // level
        data.extend_from_slice(&10u16.to_ne_bytes()); // firstrightoff
        data.extend_from_slice(&20u16.to_ne_bytes()); // newitemoff
        data.extend_from_slice(&0u16.to_ne_bytes()); // postingoff
        let rec = record_with_data(BTREE_RMID, XLOG_BTREE_SPLIT_L, &data);
        assert_eq!(btree_identify(XLOG_BTREE_SPLIT_L), Some("SPLIT_L"));
        assert_eq!(
            btree_desc(&rec),
            "level: 1, firstrightoff: 10, newitemoff: 20, postingoff: 0"
        );
    }
}
