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

    if predicates.is_empty() {
        bail!(
            "at least one <predicate-urn>=<value> property is required; \
             an entity exists only through its triples (use `kg link` to attach a bare target)"
        );
    }

    for (pred, val) in predicates {
        if val.is_empty() {
            bail!("empty value not allowed for predicate {pred}");
        }
        Urn::parse(pred)?;
        db.insert_triple(&urn.full, pred, val, false, source, confidence)?;
    }

    let triples = db.get_triples_by_subject(&urn.full)?;
    let json = entity_to_jsonld(&urn.full, &triples);
    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}
