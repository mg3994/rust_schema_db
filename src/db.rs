use anyhow::Result;
use redb::{Database, TableDefinition, ReadableTable};
use rkyv::{
    check_archived_root,
    ser::{serializers::AllocSerializer, Serializer},
    AlignedVec,
};
use std::path::Path;
use crate::models::{SchemaNode, ArchivedSchemaNode};

const NODES_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("nodes");
const TYPES_INDEX_TABLE: TableDefinition<&str, &[u8]> = TableDefinition::new("types_index");

pub struct SchemaDb {
    db: Database,
}

pub struct ArchivedNodeGuard<'a> {
    _data: AlignedVec,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a> ArchivedNodeGuard<'a> {
    pub fn get(&self) -> &ArchivedSchemaNode {
        unsafe { rkyv::archived_root::<SchemaNode>(&self._data) }
    }
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

            for node in nodes {
                let mut serializer = AllocSerializer::<1024>::default();
                serializer.serialize_value(node)
                    .map_err(|e| anyhow::anyhow!("Node serialization failed: {}", e))?;
                let node_bytes = serializer.into_serializer().into_inner();
                nodes_table.insert(node.id.as_str(), node_bytes.as_slice())?;

                for ty in &node.types {
                    let ids_bytes = {
                        let mut ids = if let Some(access) = types_table.get(ty.as_str())? {
                            let bytes = access.value();
                            let mut aligned = AlignedVec::new();
                            aligned.extend_from_slice(bytes);
                            let archived = check_archived_root::<Vec<String>>(&aligned)
                                .map_err(|e| anyhow::anyhow!("Type index validation failed: {}", e))?;
                            archived.iter().map(|s| s.to_string()).collect::<Vec<String>>()
                        } else {
                            Vec::new()
                        };

                        if !ids.contains(&node.id.to_string()) {
                            ids.push(node.id.to_string());
                            let mut serializer = AllocSerializer::<1024>::default();
                            serializer.serialize_value(&ids)
                                .map_err(|e| anyhow::anyhow!("Type index serialization failed: {}", e))?;
                            Some(serializer.into_serializer().into_inner())
                        } else {
                            None
                        }
                    };

                    if let Some(bytes) = ids_bytes {
                        types_table.insert(ty.as_str(), bytes.as_slice())?;
                    }
                }
            }
        }
        write_txn.commit()?;
        Ok(())
    }

    pub fn get_by_id(&self, id: &str) -> Result<Option<ArchivedNodeGuard<'_>>> {
        let read_txn = self.db.begin_read()?;
        let nodes_table = read_txn.open_table(NODES_TABLE)?;
        let result = nodes_table.get(id)?;

        if let Some(access) = result {
            let mut aligned = AlignedVec::new();
            aligned.extend_from_slice(access.value());
            check_archived_root::<SchemaNode>(&aligned)
                .map_err(|e| anyhow::anyhow!("Validation failed: {}", e))?;

            Ok(Some(ArchivedNodeGuard {
                _data: aligned,
                _marker: std::marker::PhantomData,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn get_ids_by_type(&self, ty: &str) -> Result<Vec<String>> {
        let read_txn = self.db.begin_read()?;
        let types_table = read_txn.open_table(TYPES_INDEX_TABLE)?;
        if let Some(access) = types_table.get(ty)? {
            let mut aligned = AlignedVec::new();
            aligned.extend_from_slice(access.value());
            let archived = check_archived_root::<Vec<String>>(&aligned)
                .map_err(|e| anyhow::anyhow!("Type index validation failed: {}", e))?;
            Ok(archived.iter().map(|s| s.to_string()).collect())
        } else {
            Ok(Vec::new())
        }
    }
}
