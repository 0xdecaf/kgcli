use anyhow::Result;

use crate::db::Database;
use crate::jsonld::{OutputOpts, predicate_to_jsonld};
use crate::model::Urn;

/// Promote a literal value to a link.
/// Deletes the literal triple (subject, predicate, value) and inserts a link triple
/// (subject, predicate, target_urn) with is_link=true, preserving the literal's
/// source and confidence.
pub fn run(
    db: &Database,
    subject: &str,
    predicate: &str,
    value: &str,
    target: &str,
    opts: OutputOpts,
) -> Result<()> {
    Urn::parse(subject)?;
    Urn::parse(predicate)?;
    Urn::parse(target)?;

    db.promote_literal(subject, predicate, value, target)?;

    let remaining = db.get_triples_by_subject_predicate(subject, predicate)?;
    let json = predicate_to_jsonld(subject, predicate, &remaining, opts);
    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}
