# Usage Examples: SchemaDb

## 1. Finding Entities by Keyword (Full-Text Search)
Search for any entity containing a specific word in its string properties.

```rust
db.for_each_by_keyword("person", |node| {
    println!("Found node: {}", node.id);
})?;
```

## 2. Advanced Intersectional Search
Find all entities that are of type `Class` AND contain the property `rdfs:label` AND mention the word "Creative".

```rust
let query = Query {
    r#type: Some("rdfs:Class".to_string()),
    property: Some("rdfs:label".to_string()),
    keyword: Some("Creative".to_string()),
};

db.search(query, |node| {
    println!("Matching node: {}", node.id);
})?;
```

## 3. Numeric Range Filtering
Find entities with a specific rating or count.

```rust
db.for_each_by_numeric_range("ratingValue", 4, 5, |node| {
    println!("Highly rated: {}", node.id);
})?;
```

## 4. Graph Traversal
Efficiently resolve a list of references found in a node.

```rust
db.with_node("https://schema.org/Person", |person| {
    let refs: Vec<&str> = person.properties.iter()
        .flat_map(|p| p.references.iter().map(|s| s.as_str()))
        .collect();

    db.resolve_references(&refs, |related_node| {
        println!("Related: {}", related_node.id);
    }).unwrap();
})?;
```

## 5. Listing Schema Metadata
Explore the available types and properties in the database.

```rust
let types = db.list_types()?;
println!("Available types: {:?}", types);

let props = db.list_properties()?;
println!("Available properties: {:?}", props);
```
