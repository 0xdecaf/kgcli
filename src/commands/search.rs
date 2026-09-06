use anyhow::Result;

use crate::db::Database;
use crate::jsonld::{OutputOpts, triples_to_entity_summaries};

pub fn run(db: &Database, query: &str, opts: OutputOpts) -> Result<()> {
    let triples = db.fts_search(query)?;

    if triples.is_empty() {
        println!("[]");
        return Ok(());
    }

    let json = triples_to_entity_summaries(&triples, opts);
    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}
