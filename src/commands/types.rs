use anyhow::Result;

use crate::db::Database;

pub fn run(db: &Database) -> Result<()> {
    let types = db.list_types()?;

    if types.is_empty() {
        println!("[]");
        return Ok(());
    }

    let json: Vec<serde_json::Value> = types
        .iter()
        .map(|(t, count)| serde_json::json!({"type": t, "count": count}))
        .collect();
    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}
