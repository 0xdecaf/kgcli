use anyhow::Result;

use crate::db::Database;
use crate::jsonld::{entity_to_jsonld, predicate_to_jsonld};
use crate::model::Urn;

pub fn run(
    db: &Database,
    subject: &str,
    predicate: Option<&str>,
    value: Option<&str>,
) -> Result<()> {
    Urn::parse(subject)?;

    match (predicate, value) {
        (None, _) => {
            // Delete entire entity — warn about inbound links
            let inbound = db.find_inbound_links(subject)?;
            let count = db.delete_entity(subject)?;

            if !inbound.is_empty() {
                let sources: Vec<String> = inbound.iter().map(|t| t.subject.clone()).collect();
                eprintln!(
                    "warning: {} dangling inbound link(s) from: {}",
                    inbound.len(),
                    sources.join(", ")
                );
            }

            let json = serde_json::json!({
                "deleted": subject,
                "triples_removed": count
            });
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        (Some(pred), None) => {
            Urn::parse(pred)?;
            db.delete_predicate(subject, pred)?;
            let remaining = db.get_triples_by_subject(subject)?;
            let json = entity_to_jsonld(subject, &remaining);
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
        (Some(pred), Some(val)) => {
            Urn::parse(pred)?;
            db.delete_triple(subject, pred, val)?;
            let remaining = db.get_triples_by_subject_predicate(subject, pred)?;
            let json = predicate_to_jsonld(subject, pred, &remaining);
            println!("{}", serde_json::to_string_pretty(&json)?);
        }
    }

    Ok(())
}
