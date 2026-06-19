# Architecture: Hyper-Optimized Schema.org Database

## Overview
This database is designed for ultra-high-performance storage and retrieval of Schema.org JSON-LD data. It utilizes a zero-copy architecture to achieve sub-microsecond read latencies.

## Core Components

### 1. Storage: `redb`
- **Embedded Key-Value Store**: Pure Rust, memory-mapped storage.
- **Transactions**: ACID compliant with ACID properties.
- **Composite Keys**: Used for efficient indexing without read-modify-write overhead.

### 2. Serialization: `rkyv`
- **Zero-Copy**: Data is archived into a format that can be cast directly back to Rust types without allocation or deserialization.
- **Validation**: Uses `bytecheck` to ensure memory safety before casting archived bytes.

### 3. Parsing: `simd-json`
- **Hardware Acceleration**: Leverages SIMD instructions for rapid JSON-LD ingestion.
- **In-place Mutation**: Mutates the input buffer to minimize memory usage.

### 4. String Handling: `compact_str`
- **Efficient Strings**: O(1) clones for short strings, which are common in Schema.org @ids and properties.

## Optimization Strategies

### Internal Numeric IDs (u64)
All entities are assigned an internal `u64` ID. Mapping tables (`id_to_u64` and `u64_to_id`) handle the translation. This reduces the size of keys in secondary indices and accelerates comparisons.

### String Interning
Type and property names are interned into `u32` IDs. This eliminates string duplication in the indices and allows for extremely fast range scans.

### Multi-Level Indexing
1.  **Primary Index**: `u64` -> `rkyv` bytes.
2.  **Type Index**: `(u32_type, u64_id)` -> `()`.
3.  **Property Index**: `(u32_prop, u64_id)` -> `()`.
4.  **Value Index**: `(u32_prop, archived_value, u64_id)` -> `()`.
5.  **Numeric Index**: `(u32_prop, f64_val, u64_id)` -> `()`.

## Data Alignment
The database handles 8-byte alignment requirements for zero-copy casting of `i64` and `f64` types. It checks pointer alignment from memory-mapped files and provides a safe fallback (with a small copy cost) only when necessary.
