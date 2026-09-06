use serde_json::{Map, Value, json};
use std::collections::HashSet;

use crate::db::Database;
use crate::model::{Triple, Urn};

/// Controls how triples are rendered.
#[derive(Debug, Clone, Copy, Default)]
pub struct OutputOpts {
    /// Render each value as an object carrying source, confidence, and created_at.
    pub provenance: bool,
}

/// Render one triple's object as a JSON value, honoring `opts`.
fn value_of(triple: &Triple, opts: OutputOpts) -> Value {
    if !opts.provenance {
        return if triple.is_link {
            json!({"@id": triple.object})
        } else {
            json!(triple.object)
        };
    }
    let mut m = Map::new();
    if triple.is_link {
        m.insert("@id".into(), json!(triple.object));
    } else {
        m.insert("@value".into(), json!(triple.object));
    }
    if let Some(s) = &triple.source {
        m.insert("source".into(), json!(s));
    }
    if let Some(c) = triple.confidence {
        m.insert("confidence".into(), json!(c));
    }
    m.insert("created_at".into(), json!(triple.created_at));
    Value::Object(m)
}

/// Insert `val` under `predicate`, promoting to an array on the second value.
fn push_predicate(predicates: &mut Map<String, Value>, predicate: &str, val: Value) {
    match predicates.get_mut(predicate) {
        Some(Value::Array(arr)) => arr.push(val),
        Some(existing) => {
            let prev = existing.take();
            *existing = json!([prev, val]);
        }
        None => {
            predicates.insert(predicate.to_string(), val);
        }
    }
}

/// Attach provenance fields (source, confidence, created_at) onto an already-built
/// child object, which is expected to be a JSON object carrying at least `@id`.
fn attach_provenance(mut child: Value, triple: &Triple) -> Value {
    if let Value::Object(m) = &mut child {
        if let Some(s) = &triple.source {
            m.insert("source".into(), json!(s));
        }
        if let Some(c) = triple.confidence {
            m.insert("confidence".into(), json!(c));
        }
        m.insert("created_at".into(), json!(triple.created_at));
    }
    child
}

/// Build a JSON-LD object for an entity from its triples.
pub fn entity_to_jsonld(subject: &str, triples: &[Triple], opts: OutputOpts) -> Value {
    let mut map = Map::new();

    // @context
    map.insert("@context".to_string(), json!({"urn": "urn:"}));

    // @id
    map.insert("@id".to_string(), json!(subject));

    // @type from URN
    if let Ok(urn) = Urn::parse(subject) {
        map.insert("@type".to_string(), json!(urn.entity_type));
    }

    // Group triples by predicate
    let mut predicates: Map<String, Value> = Map::new();
    for triple in triples {
        let val = value_of(triple, opts);
        push_predicate(&mut predicates, &triple.predicate, val);
    }

    map.extend(predicates);
    Value::Object(map)
}

/// Build a JSON-LD object showing only a specific predicate's values on an entity.
pub fn predicate_to_jsonld(
    subject: &str,
    predicate: &str,
    triples: &[Triple],
    opts: OutputOpts,
) -> Value {
    let mut map = Map::new();
    map.insert("@id".to_string(), json!(subject));

    let values: Vec<Value> = triples.iter().map(|t| value_of(t, opts)).collect();

    let val = match values.len() {
        0 => Value::Null,
        1 => values.into_iter().next().unwrap(),
        _ => Value::Array(values),
    };

    map.insert(predicate.to_string(), val);
    Value::Object(map)
}

/// Build a fully expanded JSON-LD object, recursively resolving links.
/// Uses a visited set to break cycles.
pub fn entity_to_jsonld_expanded(
    db: &Database,
    subject: &str,
    visited: &mut HashSet<String>,
    opts: OutputOpts,
) -> Value {
    // If already visited, emit just a reference
    if visited.contains(subject) {
        return json!({"@id": subject});
    }
    visited.insert(subject.to_string());

    let triples = match db.get_triples_by_subject(subject) {
        Ok(t) => t,
        Err(_) => return json!({"@id": subject}),
    };

    let mut map = Map::new();
    map.insert("@context".to_string(), json!({"urn": "urn:"}));
    map.insert("@id".to_string(), json!(subject));

    if let Ok(urn) = Urn::parse(subject) {
        map.insert("@type".to_string(), json!(urn.entity_type));
    }

    // Group triples by predicate
    let mut predicates: Map<String, Value> = Map::new();
    for triple in &triples {
        let val = if triple.is_link {
            // Recursively expand
            let child = entity_to_jsonld_expanded_inner(db, &triple.object, visited, opts);
            if opts.provenance {
                attach_provenance(child, triple)
            } else {
                child
            }
        } else {
            value_of(triple, opts)
        };

        push_predicate(&mut predicates, &triple.predicate, val);
    }

    map.extend(predicates);
    Value::Object(map)
}

/// Inner expand without @context (for nested entities).
fn entity_to_jsonld_expanded_inner(
    db: &Database,
    subject: &str,
    visited: &mut HashSet<String>,
    opts: OutputOpts,
) -> Value {
    if visited.contains(subject) {
        return json!({"@id": subject});
    }
    visited.insert(subject.to_string());

    let triples = match db.get_triples_by_subject(subject) {
        Ok(t) => t,
        Err(_) => return json!({"@id": subject}),
    };

    if triples.is_empty() {
        return json!({"@id": subject});
    }

    let mut map = Map::new();
    map.insert("@id".to_string(), json!(subject));

    if let Ok(urn) = Urn::parse(subject) {
        map.insert("@type".to_string(), json!(urn.entity_type));
    }

    let mut predicates: Map<String, Value> = Map::new();
    for triple in &triples {
        let val = if triple.is_link {
            let child = entity_to_jsonld_expanded_inner(db, &triple.object, visited, opts);
            if opts.provenance {
                attach_provenance(child, triple)
            } else {
                child
            }
        } else {
            value_of(triple, opts)
        };

        push_predicate(&mut predicates, &triple.predicate, val);
    }

    map.extend(predicates);
    Value::Object(map)
}

/// Build a JSON-LD array of entity summaries from search results.
/// Groups triples by subject and returns an array of entities.
pub fn triples_to_entity_summaries(triples: &[Triple], opts: OutputOpts) -> Value {
    let mut subjects: Vec<String> = Vec::new();
    let mut subject_triples: std::collections::HashMap<String, Vec<&Triple>> =
        std::collections::HashMap::new();

    for triple in triples {
        subject_triples
            .entry(triple.subject.clone())
            .or_default()
            .push(triple);
        if !subjects.contains(&triple.subject) {
            subjects.push(triple.subject.clone());
        }
    }

    let entities: Vec<Value> = subjects
        .iter()
        .map(|subject| {
            let ts: Vec<Triple> = subject_triples[subject]
                .iter()
                .map(|t| (*t).clone())
                .collect();
            entity_to_jsonld(subject, &ts, opts)
        })
        .collect();

    Value::Array(entities)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_triple(subject: &str, predicate: &str, object: &str, is_link: bool) -> Triple {
        Triple {
            id: 0,
            subject: subject.to_string(),
            predicate: predicate.to_string(),
            object: object.to_string(),
            is_link,
            source: None,
            confidence: None,
            created_at: String::new(),
        }
    }

    fn make_triple_with(
        subject: &str,
        predicate: &str,
        object: &str,
        is_link: bool,
        source: Option<&str>,
        confidence: Option<f64>,
    ) -> Triple {
        Triple {
            id: 1,
            subject: subject.to_string(),
            predicate: predicate.to_string(),
            object: object.to_string(),
            is_link,
            source: source.map(str::to_string),
            confidence,
            created_at: "2026-09-06T12:00:00.000Z".to_string(),
        }
    }

    #[test]
    fn provenance_wraps_literals_in_value_objects() {
        let triples = vec![make_triple_with(
            "urn:person:alice-example",
            "urn:prop:age",
            "35",
            false,
            Some("public records"),
            Some(0.9),
        )];
        let json = entity_to_jsonld(
            "urn:person:alice-example",
            &triples,
            OutputOpts { provenance: true },
        );
        let v = &json["urn:prop:age"];
        assert_eq!(v["@value"], "35");
        assert_eq!(v["source"], "public records");
        assert_eq!(v["confidence"], 0.9);
        assert_eq!(v["created_at"], "2026-09-06T12:00:00.000Z");
    }

    #[test]
    fn provenance_adds_fields_to_link_objects() {
        let triples = vec![make_triple_with(
            "urn:person:alice-example",
            "urn:rel:knows",
            "urn:person:bob-example",
            true,
            Some("interview"),
            None,
        )];
        let json = entity_to_jsonld(
            "urn:person:alice-example",
            &triples,
            OutputOpts { provenance: true },
        );
        let v = &json["urn:rel:knows"];
        assert_eq!(v["@id"], "urn:person:bob-example");
        assert_eq!(v["source"], "interview");
        assert!(
            v.get("confidence").is_none(),
            "absent provenance fields are omitted"
        );
    }

    #[test]
    fn default_output_is_unchanged_without_provenance() {
        let triples = vec![make_triple_with(
            "urn:person:alice-example",
            "urn:prop:age",
            "35",
            false,
            Some("public records"),
            Some(0.9),
        )];
        let json = entity_to_jsonld("urn:person:alice-example", &triples, OutputOpts::default());
        assert_eq!(json["urn:prop:age"], "35");
    }

    #[test]
    fn single_literal() {
        let triples = vec![make_triple(
            "urn:person:alice",
            "urn:firstname",
            "Alice",
            false,
        )];
        let json = entity_to_jsonld("urn:person:alice", &triples, OutputOpts::default());
        assert_eq!(json["@id"], "urn:person:alice");
        assert_eq!(json["@type"], "person");
        assert_eq!(json["urn:firstname"], "Alice");
    }

    #[test]
    fn multi_valued_literal() {
        let triples = vec![
            make_triple("urn:person:alice", "urn:phone", "+1-555-0123", false),
            make_triple("urn:person:alice", "urn:phone", "15550123", false),
        ];
        let json = entity_to_jsonld("urn:person:alice", &triples, OutputOpts::default());
        let phones = json["urn:phone"].as_array().unwrap();
        assert_eq!(phones.len(), 2);
    }

    #[test]
    fn single_link() {
        let triples = vec![make_triple(
            "urn:person:alice",
            "urn:knows",
            "urn:person:jane",
            true,
        )];
        let json = entity_to_jsonld("urn:person:alice", &triples, OutputOpts::default());
        assert_eq!(json["urn:knows"]["@id"], "urn:person:jane");
    }

    #[test]
    fn mixed_literals_and_links() {
        let triples = vec![
            make_triple("urn:person:alice", "urn:firstname", "Alice", false),
            make_triple("urn:person:alice", "urn:knows", "urn:person:jane", true),
        ];
        let json = entity_to_jsonld("urn:person:alice", &triples, OutputOpts::default());
        assert_eq!(json["urn:firstname"], "Alice");
        assert_eq!(json["urn:knows"]["@id"], "urn:person:jane");
    }

    #[test]
    fn mutation_return() {
        let triples = vec![
            make_triple("urn:person:alice", "urn:phone", "+1-555-0123", false),
            make_triple("urn:person:alice", "urn:phone", "15550123", false),
        ];
        let json = predicate_to_jsonld(
            "urn:person:alice",
            "urn:phone",
            &triples,
            OutputOpts::default(),
        );
        assert_eq!(json["@id"], "urn:person:alice");
        let phones = json["urn:phone"].as_array().unwrap();
        assert_eq!(phones.len(), 2);
    }

    #[test]
    fn empty_entity() {
        let json = entity_to_jsonld("urn:person:alice", &[], OutputOpts::default());
        assert_eq!(json["@id"], "urn:person:alice");
        assert_eq!(json["@type"], "person");
    }

    #[test]
    fn context_present() {
        let json = entity_to_jsonld("urn:person:alice", &[], OutputOpts::default());
        assert!(json.get("@context").is_some());
    }
}
