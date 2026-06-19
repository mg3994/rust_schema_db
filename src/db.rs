use anyhow::Result;
use redb::{Database, TableDefinition, ReadableTable, ReadableTableMetadata};
use rkyv::{
    check_archived_root,
    ser::{serializers::AllocSerializer, Serializer},
    AlignedVec,
};
use std::path::Path;
use crate::models::{SchemaNode, ArchivedSchemaNode};
use std::collections::HashMap;

const NODES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("nodes");
const TYPES_INDEX_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("types_index");

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

            let mut type_updates: HashMap<String, Vec<String>> = HashMap::new();

            for node in nodes {
                let mut serializer = AllocSerializer::<1024>::default();
                serializer.serialize_value(node)
                    .map_err(|e| anyhow::anyhow!("Node serialization failed: {}", e))?;
                let node_bytes = serializer.into_serializer().into_inner();
                nodes_table.insert(node.id.as_str(), node_bytes.as_slice())?;

                for ty in &node.types {
                    type_updates.entry(ty.to_string()).or_default().push(node.id.to_string());
                }
            }

            for (ty, new_ids) in type_updates {
                let mut ids = if let Some(access) = types_table.get(ty.as_str())? {
                    let bytes = access.value();
                    if bytes.as_ptr() as usize % 8 == 0 {
                        let archived = check_archived_root::<Vec<String>>(bytes)
                            .map_err(|e| anyhow::anyhow!("Type index validation failed: {}", e))?;
                        archived.iter().map(|s| s.to_string()).collect::<Vec<String>>()
                    } else {
                        let mut aligned = AlignedVec::new();
                        aligned.extend_from_slice(bytes);
                        let archived = check_archived_root::<Vec<String>>(&aligned)
                            .map_err(|e| anyhow::anyhow!("Type index validation failed: {}", e))?;
                        archived.iter().map(|s| s.to_string()).collect::<Vec<String>>()
                    }
                } else {
                    Vec::new()
                };

                let mut changed = false;
                for id in new_ids {
                    if !ids.contains(&id) {
                        ids.push(id);
                        changed = true;
                    }
                }

                if changed {
                    let mut serializer = AllocSerializer::<1024>::default();
                    serializer.serialize_value(&ids)
                        .map_err(|e| anyhow::anyhow!("Type index serialization failed: {}", e))?;
                    let ids_bytes = serializer.into_serializer().into_inner();
                    types_table.insert(ty.as_str(), ids_bytes.as_slice())?;
                }
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

    pub fn get_ids_by_type(&self, ty: &str) -> Result<Vec<String>> {
        let read_txn = self.db.begin_read()?;
        let types_table = read_txn.open_table(TYPES_INDEX_TABLE)?;
        if let Some(access) = types_table.get(ty)? {
            let bytes = access.value();
            if bytes.as_ptr() as usize % 8 == 0 {
                let archived = check_archived_root::<Vec<String>>(bytes)
                    .map_err(|e| anyhow::anyhow!("Type index validation failed: {}", e))?;
                Ok(archived.iter().map(|s| s.to_string()).collect())
            } else {
                let mut aligned = AlignedVec::new();
                aligned.extend_from_slice(bytes);
                let archived = check_archived_root::<Vec<String>>(&aligned)
                    .map_err(|e| anyhow::anyhow!("Type index validation failed: {}", e))?;
                Ok(archived.iter().map(|s| s.to_string()).collect())
            }
        } else {
            Ok(Vec::new())
        }
    }

    pub fn list_types(&self) -> Result<Vec<String>> {
        let read_txn = self.db.begin_read()?;
        let types_table = read_txn.open_table(TYPES_INDEX_TABLE)?;
        let mut types = Vec::new();
        for entry in types_table.iter()? {
            let (key, _) = entry?;
            types.push(key.value().to_string());
        }
        Ok(types)
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
                let ids_to_update = if let Some(access) = types_table.get(ty.as_str())? {
                    let bytes = access.value();
                    let mut ids = if bytes.as_ptr() as usize % 8 == 0 {
                        let archived = check_archived_root::<Vec<String>>(bytes)
                            .map_err(|e| anyhow::anyhow!("Type index validation failed: {}", e))?;
                        archived.iter().map(|s| s.to_string()).collect::<Vec<String>>()
                    } else {
                        let mut aligned = AlignedVec::new();
                        aligned.extend_from_slice(bytes);
                        let archived = check_archived_root::<Vec<String>>(&aligned)
                            .map_err(|e| anyhow::anyhow!("Type index validation failed: {}", e))?;
                        archived.iter().map(|s| s.to_string()).collect::<Vec<String>>()
                    };

                    ids.retain(|x| x != id);
                    Some(ids)
                } else {
                    None
                };

                if let Some(ids) = ids_to_update {
                    if ids.is_empty() {
                        types_table.remove(ty.as_str())?;
                    } else {
                        let mut serializer = AllocSerializer::<1024>::default();
                        serializer.serialize_value(&ids)
                            .map_err(|e| anyhow::anyhow!("Type index serialization failed: {}", e))?;
                        let ids_bytes = serializer.into_serializer().into_inner();
                        types_table.insert(ty.as_str(), ids_bytes.as_slice())?;
                    }
                }
            }
        }
        write_txn.commit()?;
        Ok(())
    }
}
