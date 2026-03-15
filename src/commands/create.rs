use anyhow::{Result, bail};

use crate::db::Database;
use crate::jsonld::entity_to_jsonld;
use crate::model::Urn;

pub fn run(
    db: &Database,
    subject: &str,
    predicates: &[(String, String)],
    source: Option<&str>,
    confidence: Option<f64>,
) -> Result<()> {
    let urn = Urn::parse(subject)?;

    // Upsert: apply any predicates regardless of whether entity exists
    for (pred, val) in predicates {
        if val.is_empty() {
            bail!("empty value not allowed for predicate {pred}");
        }
        Urn::parse(pred)?; // Validate predicate is a URN
        db.insert_triple(&urn.full, pred, val, false, source, confidence)?;
    }

    // If no predicates and entity doesn't exist, we still need to be able to
    // return it. We don't insert a "marker" triple — an entity with no triples
    // is valid. But `get` will return an empty entity.

    let triples = db.get_triples_by_subject(&urn.full)?;
    let json = entity_to_jsonld(&urn.full, &triples);
    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}
