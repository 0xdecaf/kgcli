use anyhow::{Result, bail};

use crate::db::Database;
use crate::model::Urn;

pub fn run(db: &Database, subject: &str, direction: &str) -> Result<()> {
    Urn::parse(subject)?;

    if !db.entity_exists(subject)? {
        bail!("entity not found: {subject}");
    }

    let mut neighbors = Vec::new();

    if direction == "out" || direction == "both" {
        for t in db.get_outbound_links(subject)? {
            neighbors.push(serde_json::json!({
                "direction": "out",
                "predicate": t.predicate,
                "entity": t.object,
            }));
        }
    }

    if direction == "in" || direction == "both" {
        for t in db.find_inbound_links(subject)? {
            neighbors.push(serde_json::json!({
                "direction": "in",
                "predicate": t.predicate,
                "entity": t.subject,
            }));
        }
    }

    println!("{}", serde_json::to_string_pretty(&neighbors)?);
    Ok(())
}
