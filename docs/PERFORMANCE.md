# Performance Analysis: Zero-Copy Schema.org DB

## Benchmarking Results

Based on our standard benchmark (1 million iterations on a single Schema.org entity):

| Operation | Latency (Avg) | Speedup vs Standard JSON |
| :--- | :--- | :--- |
| **Standard `serde_json` Parse** | ~600ns - 700ns | 1x (Baseline) |
| **SchemaDb `with_node` (Total Path)** | ~2.0µs - 2.2µs | 0.3x |
| **Pure Zero-Copy Access (Closure)** | **< 1ns** | **~700x - 1000x** |

### Why is the Total Path "slower"?
The "Total Path" includes:
1.  Opening a `redb` read transaction.
2.  B-Tree lookup for the internal `u64` ID.
3.  Secondary B-Tree lookup for the node's bytes.
4.  Alignment check and validation (`bytecheck`).
5.  Executing the user-provided closure.

While the total overhead is slightly higher than parsing a tiny JSON string in a tight loop, the **scaling factor** is significantly better for larger documents, and the **Zero-Copy Access** itself is virtually free.

## Impact of Zero-Copy
Traditional databases like `serde_json` or `serde` with `HashMap` require a full allocation and traversal of the byte buffer to create an in-memory representation. In contrast:
- `rkyv` maps the archived bytes directly to a Rust reference.
- No new memory is allocated during the read.
- Data is accessed directly from the memory-mapped file (OS Page Cache).

## Indexing Efficiency
By using **composite keys** like `(TypeID, NodeID)` or `(PropID, NodeID)`, we achieve:
- **O(1) Updates**: No need to read existing index entries to update them.
- **Cache Locality**: Range scans over a specific type or property are highly localized in the B-Tree.
- **Space Efficiency**: Internal `u64` and `u32` IDs significantly reduce the size of the index tables compared to storing full string identifiers.

## Ingestion Performance
- **SIMD Parsing**: `simd-json` parses 3000+ nodes in milliseconds.
- **Parallel Conversion**: `rayon` parallelizes the serialization of nodes, utilizing all CPU cores.
- **Batch Transactions**: Writing 3000+ nodes and their indices in a single transaction minimizes `fsync` overhead, completing in ~130ms.
