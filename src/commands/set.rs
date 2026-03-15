use anyhow::{Result, bail};

use crate::db::Database;
use crate::jsonld::predicate_to_jsonld;
use crate::model::Urn;

pub fn run(
    db: &Database,
    subject: &str,
    predicate: &str,
    value: &str,
    source: Option<&str>,
    confidence: Option<f64>,
) -> Result<()> {
    Urn::parse(subject)?;
    Urn::parse(predicate)?;

    if value.is_empty() {
        bail!("empty value not allowed");
    }

    db.insert_triple(subject, predicate, value, false, source, confidence)?;

    // Return all values of this predicate on the entity
    let triples = db.get_triples_by_subject_predicate(subject, predicate)?;
    let json = predicate_to_jsonld(subject, predicate, &triples);
    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}
