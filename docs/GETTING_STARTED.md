# Getting Started with SchemaDb

## Installation

Add the following to your `Cargo.toml`:

```toml
[dependencies]
redb = "2.1.1"
rkyv = { version = "0.7.45", features = ["validation"] }
simd_json = "0.13"
compact_str = { version = "0.8.0", features = ["rkyv"] }
anyhow = "1.0"
```

## Basic Usage

### 1. Opening the Database
```rust
let db = SchemaDb::open("my_schema.redb")?;
```

### 2. Ingesting Data
```rust
let node = SchemaNode::new(CompactString::new("https://example.org/node1"));
// Add types and properties...
db.upsert_batch(&[node])?;
```

### 3. High-Performance Zero-Copy Read
```rust
db.with_node("https://example.org/node1", |archived_node| {
    println!("Node ID: {}", archived_node.id);
    // Access properties without allocation...
})?;
```

### 4. Querying by Type
```rust
db.for_each_by_type("Person", |person| {
    println!("Found person: {}", person.id);
})?;
```

### 5. Numeric Range Filtering
```rust
db.for_each_by_numeric_range("ratingValue", 4, 5, |node| {
    println!("High rated entity: {}", node.id);
})?;
```

## Best Practices
- **Batching**: Always use `upsert_batch` for multiple nodes to minimize transaction overhead.
- **Zero-Copy**: Perform as much logic as possible inside the closures provided by `with_node` or `for_each_*` to avoid copying data.
- **String Interning**: The database automatically interns type and property names for storage efficiency.
- **Alignment**: The database handles 8-byte alignment automatically. If you are extremely performance-sensitive, try to ensure your `redb` values start at 8-byte aligned offsets (though this is managed by the OS/redb usually).
