use anyhow::Result;
use redb::{Database, TableDefinition, ReadableTable, ReadableTableMetadata};
use rkyv::{
    check_archived_root,
    ser::{serializers::AllocSerializer, Serializer},
    AlignedVec,
    Deserialize,
};
use std::path::Path;
use crate::models::{SchemaNode, ArchivedSchemaNode, SchemaValue};
use std::collections::{HashSet, VecDeque};

const NODES_TABLE: TableDefinition<u64, &[u8]> = TableDefinition::new("nodes_v2");
const ID_TO_U64: TableDefinition<&str, u64> = TableDefinition::new("id_to_u64");
const U64_TO_ID: TableDefinition<u64, &str> = TableDefinition::new("u64_to_id");
const STRING_TO_U32: TableDefinition<&str, u32> = TableDefinition::new("string_to_u32");
const U32_TO_STRING: TableDefinition<u32, &str> = TableDefinition::new("u32_to_string");
const TYPES_INDEX_TABLE: TableDefinition<(u32, u64), ()> = TableDefinition::new("types_index_v4");
const PROPERTY_INDEX_TABLE: TableDefinition<(u32, u64), ()> = TableDefinition::new("property_index_v3");
const VALUE_INDEX_TABLE: TableDefinition<(u32, &[u8], u64), ()> = TableDefinition::new("value_index");
const NUMERIC_INDEX_TABLE: TableDefinition<(u32, i64, u64), ()> = TableDefinition::new("numeric_index_v2");
const FTS_INDEX_TABLE: TableDefinition<(&str, u64), ()> = TableDefinition::new("fts_index");
const INBOUND_REFS_TABLE: TableDefinition<(u64, u64), ()> = TableDefinition::new("inbound_refs");
const CARDINALITY_TABLE: TableDefinition<(u8, &str), u64> = TableDefinition::new("cardinality");
const COUNTER_TABLE: TableDefinition<&str, u64> = TableDefinition::new("counter");

pub const KIND_TYPE: u8 = 1;
pub const KIND_PROP: u8 = 2;
pub const KIND_FTS: u8 = 3;

pub struct SchemaDb {
    db: Database,
}

#[derive(Default, Clone)]
pub struct Query {
    pub r#type: Option<String>,
    pub property: Option<String>,
    pub keyword: Option<String>,
}

impl SchemaDb {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = Database::create(path)?;

        let write_txn = db.begin_write()?;
        {
            let _ = write_txn.open_table(NODES_TABLE)?;
            let _ = write_txn.open_table(ID_TO_U64)?;
            let _ = write_txn.open_table(U64_TO_ID)?;
            let _ = write_txn.open_table(STRING_TO_U32)?;
            let _ = write_txn.open_table(U32_TO_STRING)?;
            let _ = write_txn.open_table(TYPES_INDEX_TABLE)?;
            let _ = write_txn.open_table(PROPERTY_INDEX_TABLE)?;
            let _ = write_txn.open_table(VALUE_INDEX_TABLE)?;
            let _ = write_txn.open_table(NUMERIC_INDEX_TABLE)?;
            let _ = write_txn.open_table(FTS_INDEX_TABLE)?;
            let _ = write_txn.open_table(INBOUND_REFS_TABLE)?;
            let _ = write_txn.open_table(CARDINALITY_TABLE)?;
            let _ = write_txn.open_table(COUNTER_TABLE)?;
        }
        write_txn.commit()?;

        Ok(Self { db })
    }

    fn tokenize(text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| s.len() > 2)
            .map(|s| s.to_string())
            .collect()
    }

    pub fn upsert_batch(&self, nodes: &[SchemaNode]) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut nodes_table = write_txn.open_table(NODES_TABLE)?;
            let mut id_map = write_txn.open_table(ID_TO_U64)?;
            let mut u64_map = write_txn.open_table(U64_TO_ID)?;
            let mut s2u_map = write_txn.open_table(STRING_TO_U32)?;
            let mut u2s_map = write_txn.open_table(U32_TO_STRING)?;
            let mut types_table = write_txn.open_table(TYPES_INDEX_TABLE)?;
            let mut property_table = write_txn.open_table(PROPERTY_INDEX_TABLE)?;
            let mut value_table = write_txn.open_table(VALUE_INDEX_TABLE)?;
            let mut numeric_table = write_txn.open_table(NUMERIC_INDEX_TABLE)?;
            let mut fts_table = write_txn.open_table(FTS_INDEX_TABLE)?;
            let mut inbound_refs = write_txn.open_table(INBOUND_REFS_TABLE)?;
            let mut card_table = write_txn.open_table(CARDINALITY_TABLE)?;
            let mut counter_table = write_txn.open_table(COUNTER_TABLE)?;

            let mut current_id_counter = counter_table.get("id_counter")?.map(|v| v.value()).unwrap_or(0);
            let mut current_str_counter = counter_table.get("str_counter")?.map(|v| v.value()).unwrap_or(0) as u32;

            for node in nodes {
                let u64_id = {
                    let mut existing_id = None;
                    if let Some(access) = id_map.get(node.id.as_str())? {
                        existing_id = Some(access.value());
                    }
                    if let Some(v) = existing_id {
                        v
                    } else {
                        current_id_counter += 1;
                        id_map.insert(node.id.as_str(), current_id_counter)?;
                        u64_map.insert(current_id_counter, node.id.as_str())?;
                        current_id_counter
                    }
                };

                // Serialization with fixed-size serializer and explicit scratch to reduce allocations if possible
                // (though AllocSerializer is quite optimized already in 0.7)
                let mut serializer = AllocSerializer::<2048>::default();
                serializer.serialize_value(node)
                    .map_err(|e| anyhow::anyhow!("Node serialization failed: {}", e))?;
                let node_bytes = serializer.into_serializer().into_inner();
                nodes_table.insert(u64_id, node_bytes.as_slice())?;

                for ty in &node.types {
                    let u32_ty = Self::intern_string(ty.as_str(), &mut s2u_map, &mut u2s_map, &mut current_str_counter)?;
                    if types_table.get((u32_ty, u64_id))?.is_none() {
                        types_table.insert((u32_ty, u64_id), ())?;
                        Self::increment_cardinality(&mut card_table, KIND_TYPE, ty.as_str())?;
                    }
                }

                for prop in &node.properties {
                    let u32_prop = Self::intern_string(prop.name.as_str(), &mut s2u_map, &mut u2s_map, &mut current_str_counter)?;
                    if property_table.get((u32_prop, u64_id))?.is_none() {
                        property_table.insert((u32_prop, u64_id), ())?;
                        Self::increment_cardinality(&mut card_table, KIND_PROP, prop.name.as_str())?;
                    }

                    for r in &prop.references {
                        let target_u64 = {
                            let mut tid = None;
                            if let Some(access) = id_map.get(r.as_str())? {
                                tid = Some(access.value());
                            }
                            if let Some(v) = tid {
                                v
                            } else {
                                current_id_counter += 1;
                                id_map.insert(r.as_str(), current_id_counter)?;
                                u64_map.insert(current_id_counter, r.as_str())?;
                                current_id_counter
                            }
                        };
                        inbound_refs.insert((target_u64, u64_id), ())?;
                    }

                    for val in &prop.values {
                        if let SchemaValue::String(s) = val {
                            for token in Self::tokenize(s.as_str()) {
                                if fts_table.get((token.as_str(), u64_id))?.is_none() {
                                    fts_table.insert((token.as_str(), u64_id), ())?;
                                    Self::increment_cardinality(&mut card_table, KIND_FTS, token.as_str())?;
                                }
                            }
                        }

                        let mut val_serializer = AllocSerializer::<256>::default();
                        val_serializer.serialize_value(val)
                            .map_err(|e| anyhow::anyhow!("Value serialization failed: {}", e))?;
                        let val_bytes = val_serializer.into_serializer().into_inner();
                        value_table.insert((u32_prop, val_bytes.as_slice(), u64_id), ())?;

                        match val {
                            SchemaValue::Integer(i) => {
                                numeric_table.insert((u32_prop, *i, u64_id), ())?;
                            }
                            SchemaValue::Float(f) => {
                                numeric_table.insert((u32_prop, *f as i64, u64_id), ())?;
                            }
                            _ => {}
                        }
                    }
                }
            }
            counter_table.insert("id_counter", current_id_counter)?;
            counter_table.insert("str_counter", current_str_counter as u64)?;
        }
        write_txn.commit()?;
        Ok(())
    }

    fn increment_cardinality(table: &mut redb::Table<(u8, &str), u64>, kind: u8, key: &str) -> Result<()> {
        let mut count = 0;
        if let Some(access) = table.get((kind, key))? {
            count = access.value();
        }
        table.insert((kind, key), count + 1)?;
        Ok(())
    }

    fn decrement_cardinality(table: &mut redb::Table<(u8, &str), u64>, kind: u8, key: &str) -> Result<()> {
        let mut count = 0;
        let mut exists = false;
        if let Some(access) = table.get((kind, key))? {
            count = access.value();
            exists = true;
        }
        if exists {
            if count <= 1 {
                table.remove((kind, key))?;
            } else {
                table.insert((kind, key), count - 1)?;
            }
        }
        Ok(())
    }

    pub fn get_cardinality(&self, kind: u8, key: &str) -> Result<u64> {
        let read_txn = self.db.begin_read()?;
        let table = read_txn.open_table(CARDINALITY_TABLE)?;
        Ok(table.get((kind, key))?.map(|v| v.value()).unwrap_or(0))
    }

    fn intern_string(s: &str, s2u: &mut redb::Table<&str, u32>, u2s: &mut redb::Table<u32, &str>, counter: &mut u32) -> Result<u32> {
        let mut id = None;
        if let Some(access) = s2u.get(s)? {
            id = Some(access.value());
        }
        if let Some(v) = id {
            Ok(v)
        } else {
            *counter += 1;
            s2u.insert(s, *counter)?;
            u2s.insert(*counter, s)?;
            Ok(*counter)
        }
    }

    fn with_validated_node<F, R>(bytes: &[u8], f: F) -> Result<R>
    where F: FnOnce(&ArchivedSchemaNode) -> R
    {
        if bytes.as_ptr() as usize % 8 == 0 {
            let archived = check_archived_root::<SchemaNode>(bytes)
                .map_err(|e| anyhow::anyhow!("Validation failed: {}", e))?;
            Ok(f(archived))
        } else {
            let mut aligned = AlignedVec::new();
            aligned.extend_from_slice(bytes);
            let archived = check_archived_root::<SchemaNode>(&aligned)
                .map_err(|e| anyhow::anyhow!("Validation failed: {}", e))?;
            Ok(f(archived))
        }
    }

    pub fn with_node<F, R>(&self, id: &str, f: F) -> Result<Option<R>>
    where F: FnOnce(&ArchivedSchemaNode) -> R
    {
        let read_txn = self.db.begin_read()?;
        let id_map = read_txn.open_table(ID_TO_U64)?;
        let u64_id = match id_map.get(id)? {
            Some(v) => v.value(),
            None => return Ok(None),
        };

        let nodes_table = read_txn.open_table(NODES_TABLE)?;
        let result = nodes_table.get(u64_id)?;

        if let Some(access) = result {
            Ok(Some(Self::with_validated_node(access.value(), f)?))
        } else {
            Ok(None)
        }
    }

    pub fn search<F>(&self, query: Query, mut f: F) -> Result<()>
    where F: FnMut(&ArchivedSchemaNode)
    {
        let read_txn = self.db.begin_read()?;
        let s2u_map = read_txn.open_table(STRING_TO_U32)?;
        let card_table = read_txn.open_table(CARDINALITY_TABLE)?;

        #[derive(Debug)]
        enum QueryStep {
            Type(String, u64),
            Property(String, u64),
            Keyword(String, u64),
        }

        let mut steps = Vec::new();

        if let Some(ty) = &query.r#type {
            let card = card_table.get((KIND_TYPE, ty.as_str()))?.map(|v| v.value()).unwrap_or(0);
            steps.push(QueryStep::Type(ty.clone(), card));
        }
        if let Some(prop) = &query.property {
            let card = card_table.get((KIND_PROP, prop.as_str()))?.map(|v| v.value()).unwrap_or(0);
            steps.push(QueryStep::Property(prop.clone(), card));
        }
        if let Some(kw) = &query.keyword {
            let card = card_table.get((KIND_FTS, kw.as_str()))?.map(|v| v.value()).unwrap_or(0);
            steps.push(QueryStep::Keyword(kw.clone(), card));
        }

        steps.sort_by_key(|s| match s {
            QueryStep::Type(_, c) | QueryStep::Property(_, c) | QueryStep::Keyword(_, c) => *c,
        });

        let mut results: Option<HashSet<u64>> = None;

        for step in steps {
            let mut step_ids = HashSet::new();
            match step {
                QueryStep::Type(ty, _) => {
                    if let Some(access) = s2u_map.get(ty.as_str())? {
                        let u32_ty = access.value();
                        let types_table = read_txn.open_table(TYPES_INDEX_TABLE)?;
                        for entry in types_table.range((u32_ty, 0)..(u32_ty, u64::MAX))? {
                            step_ids.insert(entry?.0.value().1);
                        }
                    } else { return Ok(()); }
                }
                QueryStep::Property(prop, _) => {
                    if let Some(access) = s2u_map.get(prop.as_str())? {
                        let u32_prop = access.value();
                        let property_table = read_txn.open_table(PROPERTY_INDEX_TABLE)?;
                        for entry in property_table.range((u32_prop, 0)..(u32_prop, u64::MAX))? {
                            step_ids.insert(entry?.0.value().1);
                        }
                    } else { return Ok(()); }
                }
                QueryStep::Keyword(kw, _) => {
                    let fts_table = read_txn.open_table(FTS_INDEX_TABLE)?;
                    let kw_lower = kw.to_lowercase();
                    for entry in fts_table.range((kw_lower.as_str(), 0)..(kw_lower.as_str(), u64::MAX))? {
                        step_ids.insert(entry?.0.value().1);
                    }
                    if step_ids.is_empty() { return Ok(()); }
                }
            }

            if let Some(mut current) = results {
                current.retain(|id| step_ids.contains(id));
                results = Some(current);
            } else {
                results = Some(step_ids);
            }

            if let Some(ref r) = results {
                if r.is_empty() { return Ok(()); }
            }
        }

        if let Some(ids) = results {
            let nodes_table = read_txn.open_table(NODES_TABLE)?;
            for id in ids {
                if let Some(access) = nodes_table.get(id)? {
                    Self::with_validated_node(access.value(), |node| f(node))?;
                }
            }
        }

        Ok(())
    }

    pub fn bfs<F>(&self, start_id: &str, depth: usize, mut f: F) -> Result<()>
    where F: FnMut(&ArchivedSchemaNode, usize)
    {
        let read_txn = self.db.begin_read()?;
        let id_map = read_txn.open_table(ID_TO_U64)?;
        let nodes_table = read_txn.open_table(NODES_TABLE)?;

        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        if let Some(access) = id_map.get(start_id)? {
            let start_u64 = access.value();
            queue.push_back((start_u64, 0));
            visited.insert(start_u64);
        }

        while let Some((u64_id, current_depth)) = queue.pop_front() {
            if current_depth > depth { continue; }

            if let Some(access) = nodes_table.get(u64_id)? {
                Self::with_validated_node(access.value(), |node| {
                    f(node, current_depth);

                    if current_depth < depth {
                        for p in node.properties.iter() {
                            for r in p.references.iter() {
                                if let Ok(Some(target_access)) = id_map.get(r.as_str()) {
                                    let target_u64 = target_access.value();
                                    if !visited.contains(&target_u64) {
                                        visited.insert(target_u64);
                                        queue.push_back((target_u64, current_depth + 1));
                                    }
                                }
                            }
                        }
                    }
                })?;
            }
        }

        Ok(())
    }

    pub fn for_each_inbound_ref<F>(&self, target_id: &str, mut f: F) -> Result<()>
    where F: FnMut(&ArchivedSchemaNode)
    {
        let read_txn = self.db.begin_read()?;
        let id_map = read_txn.open_table(ID_TO_U64)?;
        let target_u64 = match id_map.get(target_id)? {
            Some(v) => v.value(),
            None => return Ok(()),
        };

        let inbound_table = read_txn.open_table(INBOUND_REFS_TABLE)?;
        let nodes_table = read_txn.open_table(NODES_TABLE)?;

        for entry in inbound_table.range((target_u64, 0)..(target_u64, u64::MAX))? {
            let (key, _) = entry?;
            let (_, source_u64) = key.value();
            if let Some(access) = nodes_table.get(source_u64)? {
                Self::with_validated_node(access.value(), |node| f(node))?;
            }
        }
        Ok(())
    }

    pub fn get_ids_by_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let read_txn = self.db.begin_read()?;
        let id_map = read_txn.open_table(ID_TO_U64)?;
        let mut ids = Vec::new();
        let end = format!("{}\u{10FFFF}", prefix);
        for entry in id_map.range(prefix..=end.as_str())? {
            let (key, _) = entry?;
            ids.push(key.value().to_string());
        }
        Ok(ids)
    }

    pub fn for_each_by_keyword<F>(&self, keyword: &str, mut f: F) -> Result<()>
    where F: FnMut(&ArchivedSchemaNode)
    {
        let read_txn = self.db.begin_read()?;
        let fts_table = read_txn.open_table(FTS_INDEX_TABLE)?;
        let nodes_table = read_txn.open_table(NODES_TABLE)?;

        let keyword = keyword.to_lowercase();
        let range = (keyword.as_str(), 0)..(keyword.as_str(), u64::MAX);
        for entry in fts_table.range(range)? {
            let (key, _) = entry?;
            let (_, u64_id) = key.value();
            if let Some(access) = nodes_table.get(u64_id)? {
                Self::with_validated_node(access.value(), |node| f(node))?;
            }
        }
        Ok(())
    }

    pub fn for_each_by_value<F>(&self, prop_name: &str, value: &SchemaValue, mut f: F) -> Result<()>
    where F: FnMut(&ArchivedSchemaNode)
    {
        let read_txn = self.db.begin_read()?;
        let s2u_map = read_txn.open_table(STRING_TO_U32)?;
        let u32_prop = match s2u_map.get(prop_name)? {
            Some(v) => v.value(),
            None => return Ok(()),
        };

        let mut val_serializer = AllocSerializer::<256>::default();
        val_serializer.serialize_value(value)
            .map_err(|e| anyhow::anyhow!("Value serialization failed: {}", e))?;
        let val_bytes = val_serializer.into_serializer().into_inner();

        let value_table = read_txn.open_table(VALUE_INDEX_TABLE)?;
        let nodes_table = read_txn.open_table(NODES_TABLE)?;

        let range = (u32_prop, val_bytes.as_slice(), 0)..(u32_prop, val_bytes.as_slice(), u64::MAX);
        for entry in value_table.range(range)? {
            let (key, _) = entry?;
            let (_, _, u64_id) = key.value();

            if let Some(access) = nodes_table.get(u64_id)? {
                Self::with_validated_node(access.value(), |node| f(node))?;
            }
        }
        Ok(())
    }

    pub fn for_each_by_numeric_range<F>(&self, prop_name: &str, min: i64, max: i64, mut f: F) -> Result<()>
    where F: FnMut(&ArchivedSchemaNode)
    {
        let read_txn = self.db.begin_read()?;
        let s2u_map = read_txn.open_table(STRING_TO_U32)?;
        let u32_prop = match s2u_map.get(prop_name)? {
            Some(v) => v.value(),
            None => return Ok(()),
        };

        let numeric_table = read_txn.open_table(NUMERIC_INDEX_TABLE)?;
        let nodes_table = read_txn.open_table(NODES_TABLE)?;

        let range = (u32_prop, min, 0)..(u32_prop, max, u64::MAX);
        for entry in numeric_table.range(range)? {
            let (key, _) = entry?;
            let (_, _, u64_id) = key.value();

            if let Some(access) = nodes_table.get(u64_id)? {
                Self::with_validated_node(access.value(), |node| f(node))?;
            }
        }
        Ok(())
    }

    pub fn for_each_by_type<F>(&self, ty: &str, mut f: F) -> Result<()>
    where F: FnMut(&ArchivedSchemaNode)
    {
        let read_txn = self.db.begin_read()?;
        let s2u_map = read_txn.open_table(STRING_TO_U32)?;
        let u32_ty = match s2u_map.get(ty)? {
            Some(v) => v.value(),
            None => return Ok(()),
        };

        let types_table = read_txn.open_table(TYPES_INDEX_TABLE)?;
        let nodes_table = read_txn.open_table(NODES_TABLE)?;

        let range = (u32_ty, 0)..(u32_ty, u64::MAX);
        for entry in types_table.range(range)? {
            let (key, _) = entry?;
            let (_, u64_id) = key.value();

            if let Some(access) = nodes_table.get(u64_id)? {
                Self::with_validated_node(access.value(), |node| f(node))?;
            }
        }
        Ok(())
    }

    pub fn for_each_by_property<F>(&self, prop_name: &str, mut f: F) -> Result<()>
    where F: FnMut(&ArchivedSchemaNode)
    {
        let read_txn = self.db.begin_read()?;
        let s2u_map = read_txn.open_table(STRING_TO_U32)?;
        let u32_prop = match s2u_map.get(prop_name)? {
            Some(v) => v.value(),
            None => return Ok(()),
        };

        let property_table = read_txn.open_table(PROPERTY_INDEX_TABLE)?;
        let nodes_table = read_txn.open_table(NODES_TABLE)?;

        let range = (u32_prop, 0)..(u32_prop, u64::MAX);
        for entry in property_table.range(range)? {
            let (key, _) = entry?;
            let (_, u64_id) = key.value();

            if let Some(access) = nodes_table.get(u64_id)? {
                Self::with_validated_node(access.value(), |node| f(node))?;
            }
        }
        Ok(())
    }

    pub fn get_ids_by_property(&self, prop_name: &str) -> Result<Vec<String>> {
        let read_txn = self.db.begin_read()?;
        let s2u_map = read_txn.open_table(STRING_TO_U32)?;
        let u32_prop = match s2u_map.get(prop_name)? {
            Some(v) => v.value(),
            None => return Ok(Vec::new()),
        };

        let property_table = read_txn.open_table(PROPERTY_INDEX_TABLE)?;
        let u64_to_id_table = read_txn.open_table(U64_TO_ID)?;

        let mut ids = Vec::new();
        let range = (u32_prop, 0)..(u32_prop, u64::MAX);
        for entry in property_table.range(range)? {
            let (key, _) = entry?;
            let (_, u64_id) = key.value();
            if let Some(id_access) = u64_to_id_table.get(u64_id)? {
                ids.push(id_access.value().to_string());
            }
        }
        Ok(ids)
    }

    pub fn get_ids_by_type(&self, ty: &str) -> Result<Vec<String>> {
        let read_txn = self.db.begin_read()?;
        let s2u_map = read_txn.open_table(STRING_TO_U32)?;
        let u32_ty = match s2u_map.get(ty)? {
            Some(v) => v.value(),
            None => return Ok(Vec::new()),
        };

        let types_table = read_txn.open_table(TYPES_INDEX_TABLE)?;
        let u64_to_id_table = read_txn.open_table(U64_TO_ID)?;

        let mut ids = Vec::new();
        let range = (u32_ty, 0)..(u32_ty, u64::MAX);
        for entry in types_table.range(range)? {
            let (key, _) = entry?;
            let (_, u64_id) = key.value();
            if let Some(id_access) = u64_to_id_table.get(u64_id)? {
                ids.push(id_access.value().to_string());
            }
        }
        Ok(ids)
    }

    pub fn list_types(&self) -> Result<Vec<String>> {
        let read_txn = self.db.begin_read()?;
        let types_table = read_txn.open_table(TYPES_INDEX_TABLE)?;
        let u2s_map = read_txn.open_table(U32_TO_STRING)?;
        let mut types = std::collections::HashSet::new();
        for entry in types_table.iter()? {
            let (key, _) = entry?;
            let (u32_ty, _) = key.value();
            if let Some(s) = u2s_map.get(u32_ty)? {
                types.insert(s.value().to_string());
            }
        }
        let mut types_vec: Vec<String> = types.into_iter().collect();
        types_vec.sort();
        Ok(types_vec)
    }

    pub fn list_properties(&self) -> Result<Vec<String>> {
        let read_txn = self.db.begin_read()?;
        let property_table = read_txn.open_table(PROPERTY_INDEX_TABLE)?;
        let u2s_map = read_txn.open_table(U32_TO_STRING)?;
        let mut props = std::collections::HashSet::new();
        for entry in property_table.iter()? {
            let (key, _) = entry?;
            let (u32_prop, _) = key.value();
            if let Some(s) = u2s_map.get(u32_prop)? {
                props.insert(s.value().to_string());
            }
        }
        let mut props_vec: Vec<String> = props.into_iter().collect();
        props_vec.sort();
        Ok(props_vec)
    }

    pub fn count_nodes(&self) -> Result<usize> {
        let read_txn = self.db.begin_read()?;
        let nodes_table = read_txn.open_table(NODES_TABLE)?;
        Ok(nodes_table.len()? as usize)
    }

    pub fn resolve_references<F>(&self, ids: &[&str], mut f: F) -> Result<()>
    where F: FnMut(&ArchivedSchemaNode)
    {
        let read_txn = self.db.begin_read()?;
        let id_map = read_txn.open_table(ID_TO_U64)?;
        let nodes_table = read_txn.open_table(NODES_TABLE)?;

        for id in ids {
            if let Some(u64_id_access) = id_map.get(*id)? {
                let u64_id = u64_id_access.value();
                if let Some(access) = nodes_table.get(u64_id)? {
                    Self::with_validated_node(access.value(), |node| f(node))?;
                }
            }
        }
        Ok(())
    }

    pub fn remove(&self, id: &str) -> Result<()> {
        let write_txn = self.db.begin_write()?;
        {
            let mut nodes_table = write_txn.open_table(NODES_TABLE)?;
            let mut id_map = write_txn.open_table(ID_TO_U64)?;
            let mut u64_map = write_txn.open_table(U64_TO_ID)?;
            let mut s2u_map = write_txn.open_table(STRING_TO_U32)?;
            let mut types_table = write_txn.open_table(TYPES_INDEX_TABLE)?;
            let mut property_table = write_txn.open_table(PROPERTY_INDEX_TABLE)?;
            let mut value_table = write_txn.open_table(VALUE_INDEX_TABLE)?;
            let mut numeric_table = write_txn.open_table(NUMERIC_INDEX_TABLE)?;
            let mut fts_table = write_txn.open_table(FTS_INDEX_TABLE)?;
            let mut inbound_refs = write_txn.open_table(INBOUND_REFS_TABLE)?;
            let mut card_table = write_txn.open_table(CARDINALITY_TABLE)?;

            let u64_id = {
                let mut uid = None;
                if let Some(access) = id_map.get(id)? {
                    uid = Some(access.value());
                }
                match uid {
                    Some(v) => v,
                    None => return Ok(()),
                }
            };

            let (types, props_with_vals, refs) = {
                let result = nodes_table.get(u64_id)?;
                if let Some(access) = result {
                    Self::with_validated_node(access.value(), |archived| {
                        let ts = archived.types.iter().map(|s| s.to_string()).collect::<Vec<String>>();
                        let mut pvs = Vec::new();
                        let mut rs = Vec::new();
                        for p in archived.properties.iter() {
                            let p_name = p.name.to_string();
                            let mut vals = Vec::new();
                            for v in p.values.iter() {
                                let v_owned: SchemaValue = v.deserialize(&mut rkyv::Infallible).unwrap();
                                let mut val_serializer = AllocSerializer::<256>::default();
                                val_serializer.serialize_value(&v_owned).unwrap();
                                vals.push((v_owned, val_serializer.into_serializer().into_inner()));
                            }
                            pvs.push((p_name, vals));
                            for r in p.references.iter() {
                                rs.push(r.to_string());
                            }
                        }
                        (ts, pvs, rs)
                    })?
                } else {
                    return Ok(());
                }
            };

            nodes_table.remove(u64_id)?;
            id_map.remove(id)?;
            u64_map.remove(u64_id)?;

            for ty in types {
                if let Some(access) = s2u_map.get(ty.as_str())? {
                    types_table.remove((access.value(), u64_id))?;
                    Self::decrement_cardinality(&mut card_table, KIND_TYPE, ty.as_str())?;
                }
            }

            for r in refs {
                 if let Some(access) = id_map.get(r.as_str())? {
                     inbound_refs.remove((access.value(), u64_id))?;
                 }
            }

            for (prop_name, vals) in props_with_vals {
                if let Some(access) = s2u_map.get(prop_name.as_str())? {
                    let pid = access.value();
                    property_table.remove((pid, u64_id))?;
                    Self::decrement_cardinality(&mut card_table, KIND_PROP, prop_name.as_str())?;

                    for (v_owned, v_bytes) in vals {
                        value_table.remove((pid, v_bytes.as_slice(), u64_id))?;

                        if let SchemaValue::String(s) = &v_owned {
                            for token in Self::tokenize(s.as_str()) {
                                fts_table.remove((token.as_str(), u64_id))?;
                                Self::decrement_cardinality(&mut card_table, KIND_FTS, token.as_str())?;
                            }
                        }

                        match v_owned {
                            SchemaValue::Integer(i) => {
                                numeric_table.remove((pid, i, u64_id))?;
                            }
                            SchemaValue::Float(f) => {
                                numeric_table.remove((pid, f as i64, u64_id))?;
                            }
                            _ => {}
                        }
                    }
                }
            }
        }
        write_txn.commit()?;
        Ok(())
    }
}
