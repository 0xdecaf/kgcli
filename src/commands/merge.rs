use anyhow::{Result, bail};

use crate::db::Database;
use crate::jsonld::entity_to_jsonld;
use crate::model::Urn;

pub fn run(db: &Database, source: &str, target: &str) -> Result<()> {
    Urn::parse(source)?;
    Urn::parse(target)?;

    if source == target {
        bail!("cannot merge entity into itself");
    }

    if !db.entity_exists(source)? {
        bail!("source entity not found: {source}");
    }

    let count = db.merge_entity(source, target)?;

    eprintln!("merged {count} triple(s) from {source} → {target}");

    let triples = db.get_triples_by_subject(target)?;
    let json = entity_to_jsonld(target, &triples);
    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}
