use anyhow::{Result, bail};
use std::collections::HashSet;

use crate::db::Database;
use crate::jsonld::{entity_to_jsonld, entity_to_jsonld_expanded};
use crate::model::Urn;

pub fn run(db: &Database, subject: &str, expand: bool) -> Result<()> {
    Urn::parse(subject)?;

    let triples = db.get_triples_by_subject(subject)?;
    if triples.is_empty() && !db.entity_exists(subject)? {
        bail!("entity not found: {subject}");
    }

    let json = if expand {
        let mut visited = HashSet::new();
        entity_to_jsonld_expanded(db, subject, &mut visited)
    } else {
        entity_to_jsonld(subject, &triples)
    };

    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}
