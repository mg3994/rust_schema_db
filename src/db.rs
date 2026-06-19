use anyhow::Result;
use redb::{Database, TableDefinition, ReadableTable, ReadableTableMetadata};
use rkyv::{
    check_archived_root,
    ser::{serializers::AllocSerializer, Serializer},
    AlignedVec,
};
use std::path::Path;
use crate::models::{SchemaNode, ArchivedSchemaNode};

const NODES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("nodes");
const TYPES_INDEX_TABLE: TableDefinition<(&str, &str), ()> = TableDefinition::new("types_index_v2");

pub struct SchemaDb {
    db: Database,
}

impl SchemaDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Database::create(path)?;

        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(NODES_TABLE)?;
            let _ = write_txn.open_table(TYPES_INDEX_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self { db })
    }

    pub fn upsert_batch(&self, nodes: &[SchemaNode]) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut nodes_table = write_txn.open_table(NODES_TABLE)?;
            let mut types_table = write_txn.open_table(TYPES_INDEX_TABLE)?;

            // Reuse serializer buffer to reduce allocations
            let mut serializer = AllocSerializer::<2048>::default();

            for node in nodes {
                serializer.serialize_value(node)
                    .map_err(|e| anyhow::anyhow!("Node serialization failed: {}", e))?;
                let node_bytes = serializer.into_serializer().into_inner();
                nodes_table.insert(node.id.as_str(), node_bytes.as_slice())?;

                for ty in &node.types {
                    types_table.insert((ty.as_str(), node.id.as_str()), ())?;
                }

                // Clear the serializer for the next node, reusing its internal buffer
                serializer = AllocSerializer::<2048>::default();
                // Note: AllocSerializer doesn't have a simple 'clear' that resets the pointer easily
                // in 0.7 without re-creating. But we can reuse the memory if we use a custom serializer.
                // For now, even creating a new one is fast, but we'll try to keep it efficient.
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn with_node<F, R>(&self, id: &str, f: F) -> Result<Option<R>>
    where F: FnOnce(&ArchivedSchemaNode) -> R
    {
        let read_txn = self.db.begin_read()?;
        let nodes_table = read_txn.open_table(NODES_TABLE)?;
        let result = nodes_table.get(id)?;

        if let Some(access) = result {
            let bytes = access.value();
            if bytes.as_ptr() as usize % 8 == 0 {
                let archived = check_archived_root::<SchemaNode>(bytes)
                    .map_err(|e| anyhow::anyhow!("Validation failed: {}", e))?;
                Ok(Some(f(archived)))
            } else {
                let mut aligned = AlignedVec::new();
                aligned.extend_from_slice(bytes);
                let archived = check_archived_root::<SchemaNode>(&aligned)
                    .map_err(|e| anyhow::anyhow!("Validation failed: {}", e))?;
                Ok(Some(f(archived)))
            }
        } else {
            Ok(None)
        }
    }

    pub fn for_each_by_type<F>(&self, ty: &str, mut f: F) -> Result<()>
    where F: FnMut(&ArchivedSchemaNode)
    {
        let read_txn = self.db.begin_read()?;
        let types_table = read_txn.open_table(TYPES_INDEX_TABLE)?;
        let nodes_table = read_txn.open_table(NODES_TABLE)?;

        let range = (ty, "")..=(ty, "\u{10FFFF}");
        for entry in types_table.range(range)? {
            let (key, _) = entry?;
            let (_, id) = key.value();

            if let Some(access) = nodes_table.get(id)? {
                let bytes = access.value();
                if bytes.as_ptr() as usize % 8 == 0 {
                    let archived = check_archived_root::<SchemaNode>(bytes)
                        .map_err(|e| anyhow::anyhow!("Validation failed: {}", e))?;
                    f(archived);
                } else {
                    let mut aligned = AlignedVec::new();
                    aligned.extend_from_slice(bytes);
                    let archived = check_archived_root::<SchemaNode>(&aligned)
                        .map_err(|e| anyhow::anyhow!("Validation failed: {}", e))?;
                    f(archived);
                }
            }
        }
        Ok(())
    }

    pub fn get_ids_by_type(&self, ty: &str) -> Result<Vec<String>> {
        let read_txn = self.db.begin_read()?;
        let types_table = read_txn.open_table(TYPES_INDEX_TABLE)?;

        let mut ids = Vec::new();
        let range = (ty, "")..=(ty, "\u{10FFFF}");
        for entry in types_table.range(range)? {
            let (key, _) = entry?;
            let (_, id) = key.value();
            ids.push(id.to_string());
        }
        Ok(ids)
    }

    pub fn list_types(&self) -> Result<Vec<String>> {
        let read_txn = self.db.begin_read()?;
        let types_table = read_txn.open_table(TYPES_INDEX_TABLE)?;
        let mut types = std::collections::HashSet::new();
        for entry in types_table.iter()? {
            let (key, _) = entry?;
            let (ty, _) = key.value();
            types.insert(ty.to_string());
        }
        let mut types_vec: Vec<String> = types.into_iter().collect();
        types_vec.sort();
        Ok(types_vec)
    }

    pub fn count_nodes(&self) -> Result<usize> {
        let read_txn = self.db.begin_read()?;
        let nodes_table = read_txn.open_table(NODES_TABLE)?;
        Ok(nodes_table.len()? as usize)
    }

    pub fn remove(&self, id: &str) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut nodes_table = write_txn.open_table(NODES_TABLE)?;

            let types = {
                let result = nodes_table.get(id)?;
                if let Some(access) = result {
                    let bytes = access.value();
                    if bytes.as_ptr() as usize % 8 == 0 {
                        let archived = check_archived_root::<SchemaNode>(bytes)
                            .map_err(|e| anyhow::anyhow!("Validation failed: {}", e))?;
                        archived.types.iter().map(|s| s.to_string()).collect::<Vec<String>>()
                    } else {
                        let mut aligned = AlignedVec::new();
                        aligned.extend_from_slice(bytes);
                        let archived = check_archived_root::<SchemaNode>(&aligned)
                            .map_err(|e| anyhow::anyhow!("Validation failed: {}", e))?;
                        archived.types.iter().map(|s| s.to_string()).collect::<Vec<String>>()
                    }
                } else {
                    return Ok(());
                }
            };

            nodes_table.remove(id)?;

            let mut types_table = write_txn.open_table(TYPES_INDEX_TABLE)?;
            for ty in types {
                types_table.remove((ty.as_str(), id))?;
            }
        }
        write_txn.commit()?;
        Ok(())
    }
}
