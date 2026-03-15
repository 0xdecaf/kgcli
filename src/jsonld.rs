use serde_json::{Map, Value, json};
use std::collections::HashSet;

use crate::db::Database;
use crate::model::{Triple, Urn};

/// Build a JSON-LD object for an entity from its triples.
pub fn entity_to_jsonld(subject: &str, triples: &[Triple]) -> Value {
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
        let val = if triple.is_link {
            json!({"@id": triple.object})
        } else {
            json!(triple.object)
        };

        match predicates.get_mut(&triple.predicate) {
            Some(existing) => {
                // Convert to array if not already
                if existing.is_array() {
                    existing.as_array_mut().unwrap().push(val);
                } else {
                    let prev = existing.clone();
                    *existing = json!([prev, val]);
                }
            }
            None => {
                predicates.insert(triple.predicate.clone(), val);
            }
        }
    }

    map.extend(predicates);
    Value::Object(map)
}

/// Build a JSON-LD object showing only a specific predicate's values on an entity.
pub fn predicate_to_jsonld(subject: &str, predicate: &str, triples: &[Triple]) -> Value {
    let mut map = Map::new();
    map.insert("@id".to_string(), json!(subject));

    let values: Vec<Value> = triples
        .iter()
        .map(|t| {
            if t.is_link {
                json!({"@id": t.object})
            } else {
                json!(t.object)
            }
        })
        .collect();

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
            entity_to_jsonld_expanded_inner(db, &triple.object, visited)
        } else {
            json!(triple.object)
        };

        match predicates.get_mut(&triple.predicate) {
            Some(existing) => {
                if existing.is_array() {
                    existing.as_array_mut().unwrap().push(val);
                } else {
                    let prev = existing.clone();
                    *existing = json!([prev, val]);
                }
            }
            None => {
                predicates.insert(triple.predicate.clone(), val);
            }
        }
    }

    map.extend(predicates);
    Value::Object(map)
}

/// Inner expand without @context (for nested entities).
fn entity_to_jsonld_expanded_inner(
    db: &Database,
    subject: &str,
    visited: &mut HashSet<String>,
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
            entity_to_jsonld_expanded_inner(db, &triple.object, visited)
        } else {
            json!(triple.object)
        };

        match predicates.get_mut(&triple.predicate) {
            Some(existing) => {
                if existing.is_array() {
                    existing.as_array_mut().unwrap().push(val);
                } else {
                    let prev = existing.clone();
                    *existing = json!([prev, val]);
                }
            }
            None => {
                predicates.insert(triple.predicate.clone(), val);
            }
        }
    }

    map.extend(predicates);
    Value::Object(map)
}

/// Build a JSON-LD array of entity summaries from search results.
/// Groups triples by subject and returns an array of entities.
pub fn triples_to_entity_summaries(triples: &[Triple]) -> Value {
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
            entity_to_jsonld(subject, &ts)
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

    #[test]
    fn single_literal() {
        let triples = vec![make_triple("urn:person:tony", "urn:firstname", "Tony", false)];
        let json = entity_to_jsonld("urn:person:tony", &triples);
        assert_eq!(json["@id"], "urn:person:tony");
        assert_eq!(json["@type"], "person");
        assert_eq!(json["urn:firstname"], "Tony");
    }

    #[test]
    fn multi_valued_literal() {
        let triples = vec![
            make_triple("urn:person:tony", "urn:phone", "+1-555-0123", false),
            make_triple("urn:person:tony", "urn:phone", "15550123", false),
        ];
        let json = entity_to_jsonld("urn:person:tony", &triples);
        let phones = json["urn:phone"].as_array().unwrap();
        assert_eq!(phones.len(), 2);
    }

    #[test]
    fn single_link() {
        let triples = vec![make_triple("urn:person:tony", "urn:knows", "urn:person:jane", true)];
        let json = entity_to_jsonld("urn:person:tony", &triples);
        assert_eq!(json["urn:knows"]["@id"], "urn:person:jane");
    }

    #[test]
    fn mixed_literals_and_links() {
        let triples = vec![
            make_triple("urn:person:tony", "urn:firstname", "Tony", false),
            make_triple("urn:person:tony", "urn:knows", "urn:person:jane", true),
        ];
        let json = entity_to_jsonld("urn:person:tony", &triples);
        assert_eq!(json["urn:firstname"], "Tony");
        assert_eq!(json["urn:knows"]["@id"], "urn:person:jane");
    }

    #[test]
    fn mutation_return() {
        let triples = vec![
            make_triple("urn:person:tony", "urn:phone", "+1-555-0123", false),
            make_triple("urn:person:tony", "urn:phone", "15550123", false),
        ];
        let json = predicate_to_jsonld("urn:person:tony", "urn:phone", &triples);
        assert_eq!(json["@id"], "urn:person:tony");
        let phones = json["urn:phone"].as_array().unwrap();
        assert_eq!(phones.len(), 2);
    }

    #[test]
    fn empty_entity() {
        let json = entity_to_jsonld("urn:person:tony", &[]);
        assert_eq!(json["@id"], "urn:person:tony");
        assert_eq!(json["@type"], "person");
    }

    #[test]
    fn context_present() {
        let json = entity_to_jsonld("urn:person:tony", &[]);
        assert!(json.get("@context").is_some());
    }
}
