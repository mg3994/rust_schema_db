mod models;
mod db;

use anyhow::Result;
use compact_str::CompactString;
use db::{SchemaDb, Query};
use models::{Property, SchemaNode, SchemaValue};
use rkyv::Deserialize;
use simd_json::prelude::*;
use std::fs::File;
use std::io::Read;
use std::time::{Instant, Duration};
use rayon::prelude::*;

fn main() -> Result<()> {
    let db_path = "schema_db.redb";
    let _ = std::fs::remove_file(db_path);

    let db = SchemaDb::open(db_path)?;

    println!("Downloading/Reading Schema.org JSON-LD...");
    let schema_url = "https://schema.org/version/latest/schemaorg-current-https.jsonld";
    let mut bytes = match reqwest::blocking::get(schema_url) {
        Ok(resp) => resp.bytes()?.to_vec(),
        Err(_) => {
            println!("Failed to download, looking for local file...");
            let mut f = File::open("schemaorg-current-https.jsonld")?;
            let mut b = Vec::new();
            f.read_to_end(&mut b)?;
            b
        }
    };

    println!("Parsing with simd-json...");
    let tape = simd_json::to_borrowed_value(&mut bytes)
        .map_err(|e| anyhow::anyhow!("simd-json parse failed: {}", e))?;

    let nodes_json = if let Some(graph) = tape.get("@graph") {
        graph.as_array().unwrap()
    } else {
        println!("@graph not found, treating as single object or list");
        if tape.is_array() {
            tape.as_array().unwrap()
        } else {
            std::slice::from_ref(&tape)
        }
    };

    println!("Converting {} nodes in parallel...", nodes_json.len());
    let start_conv = Instant::now();
    let schema_nodes: Vec<SchemaNode> = nodes_json.par_iter().filter_map(|node_val| {
        if let Some(node_obj) = node_val.as_object() {
            let id = node_obj.get("@id")
                .and_then(|v| v.as_str())
                .unwrap_or_default();

            if id.is_empty() { return None; }

            let mut node = SchemaNode::new(CompactString::new(id));

            if let Some(types_val) = node_obj.get("@type") {
                if let Some(s) = types_val.as_str() {
                    node.types.push(CompactString::new(s));
                } else if let Some(arr) = types_val.as_array() {
                    for t in arr {
                        if let Some(s) = t.as_str() {
                            node.types.push(CompactString::new(s));
                        }
                    }
                }
            }

            for (key, val) in node_obj {
                if key == "@id" || key == "@type" || key == "@context" {
                    continue;
                }

                let (values, refs) = convert_simd_value(val);
                node.properties.push(Property {
                    name: CompactString::new(key.as_ref()),
                    values,
                    references: refs,
                });
            }
            Some(node)
        } else {
            None
        }
    }).collect();
    println!("Parallel conversion took: {:?}", start_conv.elapsed());

    println!("Ingesting nodes into DB...");
    let start_ingest = Instant::now();
    db.upsert_batch(&schema_nodes)?;
    println!("DB Ingestion took: {:?}", start_ingest.elapsed());

    let test_id = schema_nodes.iter().find(|n| n.id.contains("Person")).map(|n| n.id.to_string()).unwrap_or_else(|| schema_nodes[0].id.to_string());

    println!("Total nodes in DB: {}", db.count_nodes()?);
    let types = db.list_types()?;
    println!("Total unique types indexed: {}", types.len());
    let props = db.list_properties()?;
    println!("Total unique properties indexed: {}", props.len());

    // Basic CRUD Verification
    println!("\n--- CRUD Verification ---");
    let test_node = SchemaNode {
        id: CompactString::new("http://test.org/alpha"),
        types: vec![CompactString::new("TestType")],
        properties: vec![Property {
            name: CompactString::new("testProp"),
            values: vec![SchemaValue::Integer(123), SchemaValue::String(CompactString::new("Searching for needles in haystacks"))],
            references: Vec::new(),
        }],
    };
    db.upsert_batch(&[test_node.clone()])?;
    assert!(db.with_node("http://test.org/alpha", |_| ())?.is_some());
    println!("Upsert verified.");

    // FTS Verification
    println!("\n--- FTS Verification ---");
    let mut found_fts = Vec::new();
    db.for_each_by_keyword("needles", |node| {
        found_fts.push(node.id.to_string());
    })?;
    assert!(found_fts.contains(&"http://test.org/alpha".to_string()));
    println!("FTS verified.");

    // Advanced Query Verification
    println!("\n--- Advanced Query Verification ---");
    let query = Query {
        r#type: Some("TestType".to_string()),
        property: Some("testProp".to_string()),
        keyword: Some("haystacks".to_string()),
    };
    let mut search_results = Vec::new();
    db.search(query, |node| {
        search_results.push(node.id.to_string());
    })?;
    assert!(search_results.contains(&"http://test.org/alpha".to_string()));
    println!("Intersectional Search verified.");

    db.remove("http://test.org/alpha")?;
    assert!(db.with_node("http://test.org/alpha", |_| ())?.is_none());
    println!("Remove and Index pruning verified.");

    // Benchmarking
    println!("\n--- Benchmarking ID: {} ---", test_id);

    let mut label_val = SchemaValue::Null;
    db.with_node(&test_id, |node| {
        println!("Found node: {} with {} types", node.id, node.types.len());
        for p in node.properties.iter() {
            if p.name == "rdfs:label" {
                if let Some(v) = p.values.first() {
                    label_val = v.deserialize(&mut rkyv::Infallible).unwrap();
                }
            }
        }
    })?.expect("Test node not found");

    let iters = 1_000_000;

    // Benchmark rkyv zero-copy read (Total path)
    let start = Instant::now();
    for _ in 0..iters {
        let _ = db.with_node(&test_id, |archived| {
            std::hint::black_box(&archived.id);
            std::hint::black_box(&archived.types);
        })?;
    }
    let duration_total_read = start.elapsed();
    println!("Total SchemaDb read (txn + fetch + validation + closure) ({} iters): {:?}", iters, duration_total_read);
    println!("Average total read latency: {:?}", duration_total_read / iters);

    // Pure zero-copy access
    let mut duration_pure_access = Duration::default();
    let _ = db.with_node(&test_id, |node_ref| {
        let start = Instant::now();
        for _ in 0..iters {
            std::hint::black_box(&node_ref.id);
            std::hint::black_box(&node_ref.types);
        }
        duration_pure_access = start.elapsed();
        Ok::<(), anyhow::Error>(())
    })?.unwrap();
    println!("Pure zero-copy access ({} iters): {:?}", iters, duration_pure_access);
    println!("Average pure access latency: {:?}", duration_pure_access / iters);

    // Compare with serde_json
    let json_node = serde_json::json!({
        "@id": test_id,
        "@type": "Class",
        "rdfs:comment": "Sample comment for benchmark comparison.",
        "rdfs:label": "Sample Label"
    });
    let json_str = serde_json::to_string(&json_node)?;

    let start = Instant::now();
    for _ in 0..iters {
        let val: serde_json::Value = serde_json::from_str(&json_str)?;
        std::hint::black_box(val.get("@id").and_then(|v| v.as_str()));
    }
    let duration_serde = start.elapsed();
    println!("serde_json parse ({} iters): {:?}", iters, duration_serde);
    println!("Average serde_json latency: {:?}", duration_serde / iters);

    println!("\nSpeedup (Pure Access vs Serde): {:.2}x", duration_serde.as_secs_f64() / duration_pure_access.as_secs_f64());

    Ok(())
}

fn convert_simd_value(val: &simd_json::BorrowedValue) -> (Vec<SchemaValue>, Vec<CompactString>) {
    let mut values = Vec::new();
    let mut refs = Vec::new();

    match val {
        simd_json::BorrowedValue::Static(s) => match s {
            simd_json::StaticNode::Bool(b) => values.push(SchemaValue::Bool(*b)),
            simd_json::StaticNode::Null => values.push(SchemaValue::Null),
            simd_json::StaticNode::I64(i) => values.push(SchemaValue::Integer(*i)),
            simd_json::StaticNode::F64(f) => values.push(SchemaValue::Float(*f)),
            _ => values.push(SchemaValue::Null),
        },
        simd_json::BorrowedValue::String(s) => values.push(SchemaValue::String(CompactString::new(s.as_ref()))),
        simd_json::BorrowedValue::Array(arr) => {
            for v in arr {
                let (mut vs, mut rs) = convert_simd_value(v);
                values.append(&mut vs);
                refs.append(&mut rs);
            }
        }
        simd_json::BorrowedValue::Object(obj) => {
            if let Some(id) = obj.get("@id").and_then(|v| v.as_str()) {
                refs.push(CompactString::new(id));
            } else {
                values.push(SchemaValue::String(CompactString::new("[Nested Object]")));
            }
        }
    }
    (values, refs)
}
