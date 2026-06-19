# SchemaDb Internals & Optimizations

## Cardinality-Aware Query Optimizer
SchemaDb implements a smart query optimizer for its `search` method:
- **Tracking**: The database maintains an internal `cardinality` table that tracks how many entities are associated with each type, property, and keyword.
- **Selectivity**: When a complex query (e.g., Type=X AND Keyword=Y) is executed, the optimizer reorders the execution steps to start with the most selective criteria (the one with the lowest cardinality).
- **Efficiency**: This reordering drastically reduces the size of intermediate results and minimizes the work required for set intersections, leading to faster response times for complex queries.

## Zero-Copy Graph Traversal (BFS)
The database supports high-performance graph exploration:
- **Safe Traversal**: The `bfs` method walks the graph starting from any entity up to a specified depth.
- **Cycle Detection**: It uses a `HashSet` of internal `u64` IDs to efficiently handle circular references in the JSON-LD graph.
- **Performance**: Every node encountered during the walk is accessed via a zero-copy reference, ensuring that the traversal itself is CPU-efficient.

## Data Interning & Numeric Mapping
- **Entity IDs**: Full Schema.org URIs are mapped to `u64` values.
- **Metadata Interning**: Type and Property names are interned into `u32` IDs.
- **Index Compaction**: All secondary indices store only numeric IDs, significantly reducing the B-Tree size and improving cache locality compared to string-heavy indices.
