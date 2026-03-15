use anyhow::Result;

use crate::db::Database;
use crate::jsonld::predicate_to_jsonld;
use crate::model::Urn;

pub fn run(db: &Database, subject: &str, predicate: &str, target: &str) -> Result<()> {
    Urn::parse(subject)?;
    Urn::parse(predicate)?;
    Urn::parse(target)?;

    // No-op if doesn't exist
    db.delete_triple(subject, predicate, target)?;

    // Return remaining link targets
    let triples = db.get_triples_by_subject_predicate(subject, predicate)?;
    let json = predicate_to_jsonld(subject, predicate, &triples);
    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}
