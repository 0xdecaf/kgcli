use anyhow::Result;

use crate::db::Database;
use crate::model::Urn;

pub fn run(db: &Database, predicate: &str, value: Option<&str>) -> Result<()> {
    Urn::parse(predicate)?;

    let subjects = db.query_by_predicate(predicate, value)?;
    let json = serde_json::json!(subjects);
    println!("{}", serde_json::to_string_pretty(&json)?);
    Ok(())
}
