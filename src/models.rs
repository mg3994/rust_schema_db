use rkyv::{Archive, Deserialize, Serialize};
use compact_str::CompactString;

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[archive(check_bytes)]
pub struct Property {
    pub name: CompactString,
    pub values: Vec<SchemaValue>,
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[archive(check_bytes)]
pub enum SchemaValue {
    Null,
    Bool(bool),
    Integer(i64),
    Float(f64),
    String(CompactString),
}

#[derive(Archive, Deserialize, Serialize, Debug, Clone)]
#[archive(check_bytes)]
pub struct SchemaNode {
    pub id: CompactString,
    pub types: Vec<CompactString>,
    pub properties: Vec<Property>,
}

impl SchemaNode {
    pub fn new(id: CompactString) -> Self {
        Self {
            id,
            types: Vec::new(),
            properties: Vec::new(),
        }
    }
}
