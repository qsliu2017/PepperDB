// In-memory catalog (simplified pg_class + pg_attribute).
// Stores table metadata. Lost on restart -- acceptable for PoC.

use std::collections::HashMap;

use crate::types::{TypeId, OID};

#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub type_id: TypeId,
    pub col_num: u16,
}

#[derive(Debug, Clone)]
pub struct Table {
    pub oid: OID,
    pub name: String,
    pub columns: Vec<Column>,
}

pub struct Catalog {
    tables: HashMap<String, Table>,
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
            next_oid: 16384,
        }
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

    pub fn get_table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    pub fn all_tables(&self) -> Vec<&Table> {
        self.tables.values().collect()
    }

    pub fn drop_table(&mut self, name: &str) -> Result<OID, String> {
        match self.tables.remove(name) {
            Some(table) => Ok(table.oid),
            None => Err(format!("table \"{}\" does not exist", name)),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn create_and_get_table() {
        let mut cat = Catalog::new();
        let cols = vec![
            Column { name: "a".into(), type_id: TypeId::Int4, col_num: 0 },
            Column { name: "b".into(), type_id: TypeId::Int4, col_num: 1 },
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
        let cols = vec![Column { name: "a".into(), type_id: TypeId::Int4, col_num: 0 }];
        cat.create_table("t", cols.clone()).unwrap();
        assert!(cat.create_table("t", cols).is_err());
    }

    #[test]
    fn get_nonexistent() {
        let cat = Catalog::new();
        assert!(cat.get_table("nope").is_none());
    }
}
