//! In-memory catalog (simplified pg_class + pg_attribute).
//! Persisted to global/ heap tables via bootstrap module; loaded on restart.

pub mod bootstrap;
pub mod filenode_map;

use std::collections::HashMap;

use crate::types::{TypeId, OID};

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub type_id: TypeId,
    pub col_num: u16,
    /// PostgreSQL atttypmod. -1 = no modifier. For char(n): n (the max length).
    pub typmod: i32,
}

impl Column {
    pub fn new(name: &str, type_id: TypeId, col_num: u16) -> Self {
        Self {
            name: name.into(),
            type_id,
            col_num,
            typmod: -1,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Table {
    pub oid: OID,
    pub name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone)]
pub struct Index {
    pub oid: OID,
    pub name: String,
    pub table_oid: OID,
    pub column_name: String,
    pub key_type: TypeId,
}

pub struct Catalog {
    tables: HashMap<String, Table>,
    indexes: Vec<Index>,
    next_oid: OID,
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
            indexes: Vec::new(),
            next_oid: 16384,
        }
    }

    pub fn with_next_oid(next_oid: OID) -> Self {
        Self {
            tables: HashMap::new(),
            indexes: Vec::new(),
            next_oid,
        }
    }

    pub fn next_oid(&self) -> OID {
        self.next_oid
    }

    pub fn create_table(&mut self, name: &str, columns: Vec<Column>) -> Result<OID, String> {
        if self.tables.contains_key(name) {
            return Err(format!("relation \"{}\" already exists", name));
        }
        let oid = self.next_oid;
        self.next_oid += 1;
        self.tables.insert(
            name.to_owned(),
            Table {
                oid,
                name: name.to_owned(),
                columns,
            },
        );
        Ok(oid)
    }

    /// Insert a pre-built Table (used by catalog loader on restart).
    pub fn insert_table(&mut self, table: Table) {
        self.tables.insert(table.name.clone(), table);
    }

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn all_tables(&self) -> Vec<&Table> {
        self.tables.values().collect()
    }

    pub fn drop_table(&mut self, name: &str) -> Result<OID, String> {
        match self.tables.remove(name) {
            Some(table) => {
                self.indexes.retain(|idx| idx.table_oid != table.oid);
                Ok(table.oid)
            }
            None => Err(format!("table \"{}\" does not exist", name)),
        }
    }

    pub fn create_index(
        &mut self,
        name: &str,
        table_oid: OID,
        column_name: &str,
        key_type: TypeId,
    ) -> Result<OID, String> {
        if self.indexes.iter().any(|i| i.name == name) {
            return Err(format!("relation \"{}\" already exists", name));
        }
        let oid = self.next_oid;
        self.next_oid += 1;
        self.indexes.push(Index {
            oid,
            name: name.to_owned(),
            table_oid,
            column_name: column_name.to_owned(),
            key_type,
        });
        Ok(oid)
    }

    pub fn insert_index(&mut self, index: Index) {
        self.indexes.push(index);
    }

    pub fn get_indexes_for_table(&self, table_oid: OID) -> Vec<&Index> {
        self.indexes
            .iter()
            .filter(|i| i.table_oid == table_oid)
            .collect()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_and_get_table() {
        let mut cat = Catalog::new();
        let cols = vec![
            Column {
                name: "a".into(),
                type_id: TypeId::Int4,
                col_num: 0,
                typmod: -1,
            },
            Column {
                name: "b".into(),
                type_id: TypeId::Int4,
                col_num: 1,
                typmod: -1,
            },
        ];
        let oid = cat.create_table("t", cols).unwrap();
        assert_eq!(oid, 16384);

        let table = cat.get_table("t").unwrap();
        assert_eq!(table.columns.len(), 2);
        assert_eq!(table.columns[0].name, "a");
    }

    #[test]
    fn duplicate_table() {
        let mut cat = Catalog::new();
        let cols = vec![Column {
            name: "a".into(),
            type_id: TypeId::Int4,
            col_num: 0,
            typmod: -1,
        }];
        cat.create_table("t", cols.clone()).unwrap();
        assert!(cat.create_table("t", cols).is_err());
    }

    #[test]
    fn get_nonexistent() {
        let cat = Catalog::new();
        assert!(cat.get_table("nope").is_none());
    }
}
