use anyhow::{Result, bail};

use crate::Direction;
use crate::db::Database;
use crate::model::Urn;

pub fn run(db: &Database, subject: &str, direction: Direction) -> Result<()> {
    Urn::parse(subject)?;

    if !db.entity_exists(subject)? {
        bail!("entity not found: {subject}");
    }

    let mut neighbors = Vec::new();

    if matches!(direction, Direction::Out | Direction::Both) {
        for t in db.get_outbound_links(subject)? {
            neighbors.push(serde_json::json!({
                "direction": "out",
                "predicate": t.predicate,
                "entity": t.object,
            }));
        }
    }

    if matches!(direction, Direction::In | Direction::Both) {
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
