use anyhow::Result;

use crate::db::Database;

pub fn run(db: &Database, entity_type: &str) -> Result<()> {
    let predicates = db.schema_for_type(entity_type)?;

    if predicates.is_empty() {
        println!("[]");
        return Ok(());
    }

    let json: Vec<serde_json::Value> = predicates
        .iter()
        .map(|(p, count)| serde_json::json!({"predicate": p, "count": count}))
        .collect();
    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}
