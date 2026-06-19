# SchemaDb API Reference

## Core API

### `SchemaDb::open(path: P) -> Result<Self>`
Opens or creates a `redb` database at the specified path and initializes the necessary tables.

### `SchemaDb::upsert_batch(&self, nodes: &[SchemaNode]) -> Result<()>`
Atomically inserts or updates a batch of nodes. Correctly maintains all indices (Primary, Type, Property, Value, Numeric).

### `SchemaDb::with_node<F, R>(&self, id: &str, f: F) -> Result<Option<R>>`
True zero-copy read. Fetches a node by its `@id` and executes a closure with a reference to the archived node.

### `SchemaDb::remove(&self, id: &str) -> Result<()>`
Removes a node and prunes it from all secondary indices.

## Query & Discovery

### `SchemaDb::for_each_by_type<F>(&self, ty: &str, mut f: F) -> Result<()>`
Iterates over all nodes of a specific type. Zero-copy access within the closure.

### `SchemaDb::for_each_by_property<F>(&self, prop_name: &str, mut f: F) -> Result<()>`
Iterates over all nodes containing a specific property.

### `SchemaDb::for_each_by_value<F>(&self, prop_name: &str, value: &SchemaValue, mut f: F) -> Result<()>`
Iterates over nodes where a specific property has an exact matching value.

### `SchemaDb::for_each_by_numeric_range<F>(&self, prop_name: &str, min: i64, max: i64, mut f: F) -> Result<()>`
Finds nodes with numeric property values within a range.

### `SchemaDb::resolve_references<F>(&self, ids: &[&str], mut f: F) -> Result<()>`
Efficiently retrieves multiple nodes by their IDs in a single pass, ideal for resolving JSON-LD graph links.

### `SchemaDb::list_types(&self) -> Result<Vec<String>>`
Returns a list of all unique Schema.org types indexed in the database.

### `SchemaDb::list_properties(&self) -> Result<Vec<String>>`
Returns a list of all unique Schema.org properties indexed in the database.

### `SchemaDb::count_nodes(&self) -> Result<usize>`
Returns the total number of nodes in the database.
