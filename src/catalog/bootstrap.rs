//! Bootstrap (initdb) and catalog loading from persistent heap tables.
//!
//! On a fresh data directory, `initdb` creates pg_filenode.map, pg_class (OID 1259)
//! and pg_attribute (OID 1249) as heap files in global/, inserting self-referential
//! catalog rows. On subsequent startups, `load_catalog` reads those heap tables to
//! reconstruct the in-memory Catalog.

use crate::catalog::filenode_map;
use crate::catalog::{Catalog, Column, Table};
use crate::storage::disk::{DiskManager, PAGE_SIZE};
use crate::storage::heap;
use crate::types::{Datum, TypeId, OID};
use std::collections::HashMap;
use std::io;

pub const PG_CLASS_OID: OID = 1259;
pub const PG_ATTRIBUTE_OID: OID = 1249;
const INITIAL_NEXT_OID: OID = 16384;

/// pg_class columns: oid (Int4), relname (Text), relfilenode (Int4), relnatts (Int2)
pub fn pg_class_columns() -> Vec<Column> {
    vec![
        Column {
            name: "oid".into(),
            type_id: TypeId::Int4,
            col_num: 0,
            typmod: -1,
        },
        Column {
            name: "relname".into(),
            type_id: TypeId::Text,
            col_num: 1,
            typmod: -1,
        },
        Column {
            name: "relfilenode".into(),
            type_id: TypeId::Int4,
            col_num: 2,
            typmod: -1,
        },
        Column {
            name: "relnatts".into(),
            type_id: TypeId::Int2,
            col_num: 3,
            typmod: -1,
        },
    ]
}

/// pg_attribute columns: attrelid (Int4), attname (Text), atttypid (Int4), attnum (Int2)
pub fn pg_attribute_columns() -> Vec<Column> {
    vec![
        Column {
            name: "attrelid".into(),
            type_id: TypeId::Int4,
            col_num: 0,
            typmod: -1,
        },
        Column {
            name: "attname".into(),
            type_id: TypeId::Text,
            col_num: 1,
            typmod: -1,
        },
        Column {
            name: "atttypid".into(),
            type_id: TypeId::Int4,
            col_num: 2,
            typmod: -1,
        },
        Column {
            name: "attnum".into(),
            type_id: TypeId::Int2,
            col_num: 3,
            typmod: -1,
        },
    ]
}

/// Initialize a fresh data directory: write pg_filenode.map, create pg_class and
/// pg_attribute heap files, insert self-referential catalog rows.
pub fn initdb(disk: &DiskManager) -> io::Result<()> {
    let map_path = disk.base_path().join("global").join("pg_filenode.map");
    let mappings = vec![
        (PG_CLASS_OID, PG_CLASS_OID),
        (PG_ATTRIBUTE_OID, PG_ATTRIBUTE_OID),
    ];
    filenode_map::write_filenode_map(&map_path, &mappings, INITIAL_NEXT_OID)?;

    disk.create_global_file(PG_CLASS_OID);
    disk.create_global_file(PG_ATTRIBUTE_OID);

    let class_cols = pg_class_columns();
    let attr_cols = pg_attribute_columns();

    // Insert pg_class row for pg_class itself
    let pg_class_row = vec![
        Datum::Int4(PG_CLASS_OID as i32),
        Datum::Text("pg_class".into()),
        Datum::Int4(PG_CLASS_OID as i32),
        Datum::Int2(class_cols.len() as i16),
    ];
    insert_global_tuple(disk, PG_CLASS_OID, &pg_class_row, &class_cols);

    // Insert pg_class row for pg_attribute
    let pg_attr_row = vec![
        Datum::Int4(PG_ATTRIBUTE_OID as i32),
        Datum::Text("pg_attribute".into()),
        Datum::Int4(PG_ATTRIBUTE_OID as i32),
        Datum::Int2(attr_cols.len() as i16),
    ];
    insert_global_tuple(disk, PG_CLASS_OID, &pg_attr_row, &class_cols);

    // Insert pg_attribute rows for pg_class columns
    for (i, col) in class_cols.iter().enumerate() {
        let row = vec![
            Datum::Int4(PG_CLASS_OID as i32),
            Datum::Text(col.name.clone()),
            Datum::Int4(col.type_id.pg_oid() as i32),
            Datum::Int2((i + 1) as i16),
        ];
        insert_global_tuple(disk, PG_ATTRIBUTE_OID, &row, &attr_cols);
    }

    // Insert pg_attribute rows for pg_attribute columns
    for (i, col) in attr_cols.iter().enumerate() {
        let row = vec![
            Datum::Int4(PG_ATTRIBUTE_OID as i32),
            Datum::Text(col.name.clone()),
            Datum::Int4(col.type_id.pg_oid() as i32),
            Datum::Int2((i + 1) as i16),
        ];
        insert_global_tuple(disk, PG_ATTRIBUTE_OID, &row, &attr_cols);
    }

    Ok(())
}

/// Load catalog from persistent heap tables in global/.
pub fn load_catalog(disk: &DiskManager) -> io::Result<Catalog> {
    let map_path = disk.base_path().join("global").join("pg_filenode.map");
    let (_mappings, next_oid) = filenode_map::read_filenode_map(&map_path)?;

    let class_cols = pg_class_columns();
    let attr_cols = pg_attribute_columns();

    // Scan pg_class to get all tables
    let class_rows = scan_global_heap(disk, PG_CLASS_OID, &class_cols);
    // Scan pg_attribute to get all columns
    let attr_rows = scan_global_heap(disk, PG_ATTRIBUTE_OID, &attr_cols);

    // Build attrelid -> Vec<(attname, atttypid, attnum)>
    let mut attr_map: HashMap<OID, Vec<(String, OID, i16)>> = HashMap::new();
    for row in &attr_rows {
        let attrelid = match &row[0] {
            Datum::Int4(v) => *v as OID,
            _ => continue,
        };
        let attname = match &row[1] {
            Datum::Text(s) => s.clone(),
            _ => continue,
        };
        let atttypid = match &row[2] {
            Datum::Int4(v) => *v as OID,
            _ => continue,
        };
        let attnum = match &row[3] {
            Datum::Int2(v) => *v,
            _ => continue,
        };
        attr_map
            .entry(attrelid)
            .or_default()
            .push((attname, atttypid, attnum));
    }

    let mut catalog = Catalog::with_next_oid(next_oid);

    for row in &class_rows {
        let oid = match &row[0] {
            Datum::Int4(v) => *v as OID,
            _ => continue,
        };
        let relname = match &row[1] {
            Datum::Text(s) => s.clone(),
            _ => continue,
        };

        // Skip system catalogs (pg_class, pg_attribute) -- they're not user tables
        if oid == PG_CLASS_OID || oid == PG_ATTRIBUTE_OID {
            continue;
        }

        let mut columns: Vec<Column> = attr_map
            .get(&oid)
            .map(|attrs| {
                attrs
                    .iter()
                    .map(|(name, typid, attnum)| Column {
                        name: name.clone(),
                        type_id: TypeId::from_pg_oid(*typid).unwrap_or(TypeId::Text),
                        col_num: (*attnum - 1) as u16,
                        typmod: -1,
                    })
                    .collect()
            })
            .unwrap_or_default();

        // Sort by attnum
        columns.sort_by_key(|c| c.col_num);

        catalog.insert_table(Table {
            oid,
            name: relname,
            columns,
        });
    }

    Ok(catalog)
}

/// Helper: insert a tuple into a global heap file, allocating pages as needed.
fn insert_global_tuple(disk: &DiskManager, relfilenode: OID, values: &[Datum], columns: &[Column]) {
    let tuple = heap::build_tuple(values, columns);
    let num_pages = disk.num_global_pages(relfilenode);
    let mut page = [0u8; PAGE_SIZE];

    if num_pages == 0 {
        heap::init_page(&mut page);
        heap::insert_tuple(&mut page, &tuple, 0).expect("tuple too large for page");
        disk.write_global_page(relfilenode, 0, &page);
    } else {
        let last = num_pages - 1;
        disk.read_global_page(relfilenode, last, &mut page);
        if heap::insert_tuple(&mut page, &tuple, last).is_ok() {
            disk.write_global_page(relfilenode, last, &page);
        } else {
            let new_id = num_pages;
            heap::init_page(&mut page);
            heap::insert_tuple(&mut page, &tuple, new_id).expect("tuple too large for page");
            disk.write_global_page(relfilenode, new_id, &page);
        }
    }
}

/// Helper: scan all live tuples from a global heap file.
fn scan_global_heap(disk: &DiskManager, relfilenode: OID, columns: &[Column]) -> Vec<Vec<Datum>> {
    let num_pages = disk.num_global_pages(relfilenode);
    let mut tuples = Vec::new();
    let mut page = [0u8; PAGE_SIZE];

    for page_id in 0..num_pages {
        disk.read_global_page(relfilenode, page_id, &mut page);
        let n = heap::num_items(&page);
        for item_idx in 0..n {
            if let Some(datums) = heap::read_tuple(&page, item_idx, columns) {
                tuples.push(datums);
            }
        }
    }
    tuples
}

/// Insert a pg_class row for a user table into the global catalog heap.
pub fn insert_pg_class_row(disk: &DiskManager, oid: OID, name: &str, ncols: i16) {
    let cols = pg_class_columns();
    let row = vec![
        Datum::Int4(oid as i32),
        Datum::Text(name.into()),
        Datum::Int4(oid as i32),
        Datum::Int2(ncols),
    ];
    insert_global_tuple(disk, PG_CLASS_OID, &row, &cols);
}

/// Insert pg_attribute rows for a user table into the global catalog heap.
pub fn insert_pg_attribute_rows(disk: &DiskManager, table_oid: OID, columns: &[Column]) {
    let attr_cols = pg_attribute_columns();
    for (i, col) in columns.iter().enumerate() {
        let row = vec![
            Datum::Int4(table_oid as i32),
            Datum::Text(col.name.clone()),
            Datum::Int4(col.type_id.pg_oid() as i32),
            Datum::Int2((i + 1) as i16),
        ];
        insert_global_tuple(disk, PG_ATTRIBUTE_OID, &row, &attr_cols);
    }
}

/// Mark pg_class and pg_attribute rows dead for a dropped table.
pub fn drop_pg_catalog_rows(disk: &DiskManager, table_oid: OID) {
    // Mark pg_class row dead
    let class_cols = pg_class_columns();
    let num_pages = disk.num_global_pages(PG_CLASS_OID);
    let mut page = [0u8; PAGE_SIZE];
    for page_id in 0..num_pages {
        disk.read_global_page(PG_CLASS_OID, page_id, &mut page);
        let n = heap::num_items(&page);
        let mut modified = false;
        for item_idx in 0..n {
            if let Some(datums) = heap::read_tuple(&page, item_idx, &class_cols) {
                if let Datum::Int4(oid) = &datums[0] {
                    if *oid as OID == table_oid {
                        heap::mark_tuple_dead(&mut page, item_idx);
                        modified = true;
                    }
                }
            }
        }
        if modified {
            disk.write_global_page(PG_CLASS_OID, page_id, &page);
        }
    }

    // Mark pg_attribute rows dead
    let attr_cols = pg_attribute_columns();
    let num_pages = disk.num_global_pages(PG_ATTRIBUTE_OID);
    for page_id in 0..num_pages {
        disk.read_global_page(PG_ATTRIBUTE_OID, page_id, &mut page);
        let n = heap::num_items(&page);
        let mut modified = false;
        for item_idx in 0..n {
            if let Some(datums) = heap::read_tuple(&page, item_idx, &attr_cols) {
                if let Datum::Int4(attrelid) = &datums[0] {
                    if *attrelid as OID == table_oid {
                        heap::mark_tuple_dead(&mut page, item_idx);
                        modified = true;
                    }
                }
            }
        }
        if modified {
            disk.write_global_page(PG_ATTRIBUTE_OID, page_id, &page);
        }
    }
}

/// Update next_oid in pg_filenode.map (preserving existing mappings).
pub fn update_next_oid(disk: &DiskManager, next_oid: OID) -> io::Result<()> {
    let map_path = disk.base_path().join("global").join("pg_filenode.map");
    let (mappings, _old_next_oid) = filenode_map::read_filenode_map(&map_path)?;
    let entries: Vec<(OID, OID)> = mappings.into_iter().collect();
    filenode_map::write_filenode_map(&map_path, &entries, next_oid)
}

#[cfg(test)]
mod test {
    use super::*;

    fn make_disk() -> (tempfile::TempDir, DiskManager) {
        let dir = tempfile::tempdir().unwrap();
        let dm = DiskManager::new(dir.path(), 5);
        (dir, dm)
    }

    #[test]
    fn bootstrap_creates_catalog_files() {
        let (dir, disk) = make_disk();
        initdb(&disk).unwrap();
        assert!(dir.path().join("global/pg_filenode.map").exists());
        assert!(dir.path().join("global/1259").exists());
        assert!(dir.path().join("global/1249").exists());
    }

    #[test]
    fn bootstrap_self_referential() {
        let (_dir, disk) = make_disk();
        initdb(&disk).unwrap();

        let class_cols = pg_class_columns();
        let rows = scan_global_heap(&disk, PG_CLASS_OID, &class_cols);
        // Should have 2 rows: pg_class and pg_attribute
        assert_eq!(rows.len(), 2);

        let names: Vec<&str> = rows
            .iter()
            .filter_map(|r| match &r[1] {
                Datum::Text(s) => Some(s.as_str()),
                _ => None,
            })
            .collect();
        assert!(names.contains(&"pg_class"));
        assert!(names.contains(&"pg_attribute"));

        let attr_cols = pg_attribute_columns();
        let attrs = scan_global_heap(&disk, PG_ATTRIBUTE_OID, &attr_cols);
        // 4 cols for pg_class + 4 cols for pg_attribute = 8
        assert_eq!(attrs.len(), 8);
    }

    #[test]
    fn catalog_survives_restart() {
        let (dir, disk) = make_disk();
        initdb(&disk).unwrap();

        // Create a user table
        let cols = vec![
            Column {
                name: "a".into(),
                type_id: TypeId::Int4,
                col_num: 0,
                typmod: -1,
            },
            Column {
                name: "b".into(),
                type_id: TypeId::Text,
                col_num: 1,
                typmod: -1,
            },
        ];
        let oid: OID = 16384;
        disk.create_heap_file(oid);
        insert_pg_class_row(&disk, oid, "my_table", 2);
        insert_pg_attribute_rows(&disk, oid, &cols);
        update_next_oid(&disk, 16385).unwrap();

        // "Restart": create new DiskManager, load catalog
        let disk2 = DiskManager::new(dir.path(), 5);
        let catalog = load_catalog(&disk2).unwrap();
        let table = catalog.get_table("my_table").unwrap();
        assert_eq!(table.oid, 16384);
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.columns[0].name, "a");
        assert_eq!(table.columns[0].type_id, TypeId::Int4);
        assert_eq!(table.columns[1].name, "b");
        assert_eq!(table.columns[1].type_id, TypeId::Text);
    }

    #[test]
    fn drop_survives_restart() {
        let (dir, disk) = make_disk();
        initdb(&disk).unwrap();

        let cols = vec![Column {
            name: "x".into(),
            type_id: TypeId::Int4,
            col_num: 0,
            typmod: -1,
        }];
        let oid: OID = 16384;
        disk.create_heap_file(oid);
        insert_pg_class_row(&disk, oid, "drop_me", 1);
        insert_pg_attribute_rows(&disk, oid, &cols);
        update_next_oid(&disk, 16385).unwrap();

        // Drop it
        drop_pg_catalog_rows(&disk, oid);

        // Restart
        let disk2 = DiskManager::new(dir.path(), 5);
        let catalog = load_catalog(&disk2).unwrap();
        assert!(catalog.get_table("drop_me").is_none());
    }

    #[test]
    fn typeid_pg_oid_round_trip() {
        for tid in [
            TypeId::Bool,
            TypeId::Int2,
            TypeId::Int4,
            TypeId::Int8,
            TypeId::Float4,
            TypeId::Float8,
            TypeId::Text,
        ] {
            assert_eq!(TypeId::from_pg_oid(tid.pg_oid()), Some(tid));
        }
    }
}
