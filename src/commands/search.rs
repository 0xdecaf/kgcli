use anyhow::Result;

use crate::db::Database;
use crate::jsonld::triples_to_entity_summaries;

pub fn run(db: &Database, query: &str) -> Result<()> {
    let triples = db.fts_search(query)?;

    if triples.is_empty() {
        println!("[]");
        return Ok(());
    }

    let json = triples_to_entity_summaries(&triples);
    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}
