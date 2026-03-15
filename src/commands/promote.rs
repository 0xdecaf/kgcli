use anyhow::{Result, bail};

use crate::db::Database;
use crate::jsonld::predicate_to_jsonld;
use crate::model::Urn;

/// Promote a literal value to a link.
/// Deletes the literal triple (subject, predicate, value) and inserts a link triple
/// (subject, predicate, target_urn) with is_link=true.
pub fn run(
    db: &Database,
    subject: &str,
    predicate: &str,
    value: &str,
    target: &str,
) -> Result<()> {
    Urn::parse(subject)?;
    Urn::parse(predicate)?;
    Urn::parse(target)?;

    // Verify the literal triple exists
    let triples = db.get_triples_by_subject_predicate(subject, predicate)?;
    let exists = triples.iter().any(|t| !t.is_link && t.object == value);
    if !exists {
        bail!("literal triple not found: {subject} {predicate} {value}");
    }

    // Delete the literal and insert the link
    db.delete_triple(subject, predicate, value)?;
    db.insert_triple(subject, predicate, target, true, None, None)?;

    let remaining = db.get_triples_by_subject_predicate(subject, predicate)?;
    let json = predicate_to_jsonld(subject, predicate, &remaining);
    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}
