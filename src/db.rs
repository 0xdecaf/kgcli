use anyhow::{Context, Result, bail};
use rusqlite::{Connection, params};
use std::path::{Path, PathBuf};

use crate::model::Triple;

const SCHEMA_SQL: &str = "
CREATE TABLE IF NOT EXISTS triples (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    subject     TEXT NOT NULL,
    predicate   TEXT NOT NULL,
    object      TEXT NOT NULL,
    is_link     BOOLEAN NOT NULL DEFAULT 0,
    source      TEXT,
    confidence  REAL,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_subject ON triples(subject);
CREATE INDEX IF NOT EXISTS idx_subject_predicate ON triples(subject, predicate);
CREATE INDEX IF NOT EXISTS idx_object_links ON triples(object) WHERE is_link = 1;
CREATE INDEX IF NOT EXISTS idx_predicate ON triples(predicate);
CREATE UNIQUE INDEX IF NOT EXISTS idx_spo ON triples(subject, predicate, object);

CREATE VIRTUAL TABLE IF NOT EXISTS triples_fts USING fts5(
    subject, predicate, object,
    content='triples',
    content_rowid='id'
);

CREATE TRIGGER IF NOT EXISTS triples_ai AFTER INSERT ON triples BEGIN
    INSERT INTO triples_fts(rowid, subject, predicate, object)
    VALUES (new.id, new.subject, new.predicate, new.object);
END;

CREATE TRIGGER IF NOT EXISTS triples_ad AFTER DELETE ON triples BEGIN
    INSERT INTO triples_fts(triples_fts, rowid, subject, predicate, object)
    VALUES ('delete', old.id, old.subject, old.predicate, old.object);
END;

CREATE TRIGGER IF NOT EXISTS triples_au AFTER UPDATE ON triples BEGIN
    INSERT INTO triples_fts(triples_fts, rowid, subject, predicate, object)
    VALUES ('delete', old.id, old.subject, old.predicate, old.object);
    INSERT INTO triples_fts(rowid, subject, predicate, object)
    VALUES (new.id, new.subject, new.predicate, new.object);
END;
";

/// Resolve the database path from the --graph flag.
///
/// - None → `.ont/graph.db` in cwd
/// - Some(name) with no path separators → `.ont/<name>.db` in cwd
/// - Some(path) with path separators or absolute → use as-is
pub fn resolve_db_path(graph: Option<&str>) -> Result<PathBuf> {
    match graph {
        None => {
            let dir = Path::new(".ont");
            std::fs::create_dir_all(dir)
                .context("failed to create .ont directory")?;
            Ok(dir.join("graph.db"))
        }
        Some(name) => {
            let path = Path::new(name);
            if path.is_absolute() || name.contains('/') || name.contains('\\') {
                // Absolute or relative path — use as-is, ensure parent exists
                if let Some(parent) = path.parent() {
                    if !parent.as_os_str().is_empty() && !parent.exists() {
                        bail!("parent directory does not exist: {}", parent.display());
                    }
                }
                Ok(path.to_path_buf())
            } else {
                // Named graph — use .ont/<name>.db
                let dir = Path::new(".ont");
                std::fs::create_dir_all(dir)
                    .context("failed to create .ont directory")?;
                Ok(dir.join(format!("{name}.db")))
            }
        }
    }
}

pub struct Database {
    pub conn: Connection,
}

impl Database {
    pub fn open(path: &Path) -> Result<Self> {
        let conn = Connection::open(path)
            .with_context(|| format!("failed to open database: {}", path.display()))?;
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA foreign_keys=ON;")
            .context("failed to set pragmas")?;
        conn.execute_batch(SCHEMA_SQL)
            .context("failed to initialize schema")?;
        Ok(Self { conn })
    }

    /// Insert a triple. Returns Ok(true) if inserted, Ok(false) if duplicate.
    pub fn insert_triple(
        &self,
        subject: &str,
        predicate: &str,
        object: &str,
        is_link: bool,
        source: Option<&str>,
        confidence: Option<f64>,
    ) -> Result<bool> {
        let result = self.conn.execute(
            "INSERT OR IGNORE INTO triples (subject, predicate, object, is_link, source, confidence)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![subject, predicate, object, is_link, source, confidence],
        )?;
        Ok(result > 0)
    }

    /// Get all triples for a subject.
    pub fn get_triples_by_subject(&self, subject: &str) -> Result<Vec<Triple>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, subject, predicate, object, is_link, source, confidence, created_at
             FROM triples WHERE subject = ?1 ORDER BY predicate, id",
        )?;
        let rows = stmt.query_map(params![subject], |row| {
            Ok(Triple {
                id: row.get(0)?,
                subject: row.get(1)?,
                predicate: row.get(2)?,
                object: row.get(3)?,
                is_link: row.get(4)?,
                source: row.get(5)?,
                confidence: row.get(6)?,
                created_at: row.get(7)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Get triples for a subject filtered by predicate.
    pub fn get_triples_by_subject_predicate(
        &self,
        subject: &str,
        predicate: &str,
    ) -> Result<Vec<Triple>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, subject, predicate, object, is_link, source, confidence, created_at
             FROM triples WHERE subject = ?1 AND predicate = ?2 ORDER BY id",
        )?;
        let rows = stmt.query_map(params![subject, predicate], |row| {
            Ok(Triple {
                id: row.get(0)?,
                subject: row.get(1)?,
                predicate: row.get(2)?,
                object: row.get(3)?,
                is_link: row.get(4)?,
                source: row.get(5)?,
                confidence: row.get(6)?,
                created_at: row.get(7)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Check if any triples exist for a subject.
    pub fn entity_exists(&self, subject: &str) -> Result<bool> {
        let count: i64 = self.conn.query_row(
            "SELECT COUNT(*) FROM triples WHERE subject = ?1",
            params![subject],
            |row| row.get(0),
        )?;
        Ok(count > 0)
    }

    /// Delete all triples for a subject. Returns count of deleted rows.
    pub fn delete_entity(&self, subject: &str) -> Result<usize> {
        let count = self.conn.execute(
            "DELETE FROM triples WHERE subject = ?1",
            params![subject],
        )?;
        Ok(count)
    }

    /// Delete triples matching subject + predicate.
    pub fn delete_predicate(&self, subject: &str, predicate: &str) -> Result<usize> {
        let count = self.conn.execute(
            "DELETE FROM triples WHERE subject = ?1 AND predicate = ?2",
            params![subject, predicate],
        )?;
        Ok(count)
    }

    /// Delete a specific triple.
    pub fn delete_triple(&self, subject: &str, predicate: &str, object: &str) -> Result<usize> {
        let count = self.conn.execute(
            "DELETE FROM triples WHERE subject = ?1 AND predicate = ?2 AND object = ?3",
            params![subject, predicate, object],
        )?;
        Ok(count)
    }

    /// Find inbound links (triples where this entity is the object and is_link=true).
    pub fn find_inbound_links(&self, object: &str) -> Result<Vec<Triple>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, subject, predicate, object, is_link, source, confidence, created_at
             FROM triples WHERE object = ?1 AND is_link = 1",
        )?;
        let rows = stmt.query_map(params![object], |row| {
            Ok(Triple {
                id: row.get(0)?,
                subject: row.get(1)?,
                predicate: row.get(2)?,
                object: row.get(3)?,
                is_link: row.get(4)?,
                source: row.get(5)?,
                confidence: row.get(6)?,
                created_at: row.get(7)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Full-text search across all triple fields.
    pub fn fts_search(&self, query: &str) -> Result<Vec<Triple>> {
        let mut stmt = self.conn.prepare(
            "SELECT t.id, t.subject, t.predicate, t.object, t.is_link, t.source, t.confidence, t.created_at
             FROM triples_fts f
             JOIN triples t ON f.rowid = t.id
             WHERE triples_fts MATCH ?1
             ORDER BY rank",
        )?;
        let rows = stmt.query_map(params![query], |row| {
            Ok(Triple {
                id: row.get(0)?,
                subject: row.get(1)?,
                predicate: row.get(2)?,
                object: row.get(3)?,
                is_link: row.get(4)?,
                source: row.get(5)?,
                confidence: row.get(6)?,
                created_at: row.get(7)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Find entities by predicate and optional value match.
    pub fn query_by_predicate(
        &self,
        predicate: &str,
        value: Option<&str>,
    ) -> Result<Vec<String>> {
        match value {
            Some(val) => {
                let mut stmt = self.conn.prepare(
                    "SELECT DISTINCT subject FROM triples WHERE predicate = ?1 AND object = ?2",
                )?;
                let rows = stmt.query_map(params![predicate, val], |row| row.get(0))?;
                rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
            }
            None => {
                let mut stmt = self.conn.prepare(
                    "SELECT DISTINCT subject FROM triples WHERE predicate = ?1",
                )?;
                let rows = stmt.query_map(params![predicate], |row| row.get(0))?;
                rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
            }
        }
    }

    /// Get all outbound links from a subject (is_link=true).
    pub fn get_outbound_links(&self, subject: &str) -> Result<Vec<Triple>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, subject, predicate, object, is_link, source, confidence, created_at
             FROM triples WHERE subject = ?1 AND is_link = 1",
        )?;
        let rows = stmt.query_map(params![subject], |row| {
            Ok(Triple {
                id: row.get(0)?,
                subject: row.get(1)?,
                predicate: row.get(2)?,
                object: row.get(3)?,
                is_link: row.get(4)?,
                source: row.get(5)?,
                confidence: row.get(6)?,
                created_at: row.get(7)?,
            })
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// List all entity types with counts.
    pub fn list_types(&self) -> Result<Vec<(String, i64)>> {
        // Extract the type from URN subjects: urn:<type>:<id>
        let mut stmt = self.conn.prepare(
            "SELECT
                SUBSTR(subject, 5, INSTR(SUBSTR(subject, 5), ':') - 1) AS entity_type,
                COUNT(DISTINCT subject) AS cnt
             FROM triples
             WHERE subject LIKE 'urn:%'
             GROUP BY entity_type
             ORDER BY cnt DESC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }

    /// Merge source entity into target: re-point all triples from source to target.
    /// Returns the number of triples moved.
    pub fn merge_entity(&self, source: &str, target: &str) -> Result<usize> {
        let triples = self.get_triples_by_subject(source)?;
        let mut moved = 0;
        for t in &triples {
            let inserted = self.insert_triple(
                target,
                &t.predicate,
                &t.object,
                t.is_link,
                t.source.as_deref(),
                t.confidence,
            )?;
            if inserted {
                moved += 1;
            }
        }
        // Also re-point inbound links from source to target
        let inbound = self.find_inbound_links(source)?;
        for t in &inbound {
            let inserted = self.insert_triple(
                &t.subject,
                &t.predicate,
                target,
                true,
                t.source.as_deref(),
                t.confidence,
            )?;
            if inserted {
                moved += 1;
            }
        }
        // Delete old source entity and its inbound references
        self.delete_entity(source)?;
        for t in &inbound {
            self.delete_triple(&t.subject, &t.predicate, source)?;
        }
        Ok(moved)
    }

    /// Show predicates used for a given entity type with counts.
    pub fn schema_for_type(&self, entity_type: &str) -> Result<Vec<(String, i64)>> {
        let pattern = format!("urn:{entity_type}:%");
        let mut stmt = self.conn.prepare(
            "SELECT predicate, COUNT(*) as cnt
             FROM triples
             WHERE subject LIKE ?1
             GROUP BY predicate
             ORDER BY cnt DESC",
        )?;
        let rows = stmt.query_map(params![pattern], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        rows.collect::<Result<Vec<_>, _>>().map_err(Into::into)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_db() -> Database {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch(SCHEMA_SQL).unwrap();
        Database { conn }
    }

    #[test]
    fn schema_init() {
        let db = test_db();
        // Verify tables exist
        let count: i64 = db.conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='triples'",
            [], |row| row.get(0),
        ).unwrap();
        assert_eq!(count, 1);
    }

    #[test]
    fn schema_idempotent() {
        let db = test_db();
        db.conn.execute_batch(SCHEMA_SQL).unwrap();
    }

    #[test]
    fn insert_literal() {
        let db = test_db();
        let inserted = db.insert_triple("urn:person:tony", "urn:firstname", "Tony", false, None, None).unwrap();
        assert!(inserted);
        let triples = db.get_triples_by_subject("urn:person:tony").unwrap();
        assert_eq!(triples.len(), 1);
        assert!(!triples[0].is_link);
    }

    #[test]
    fn insert_link() {
        let db = test_db();
        db.insert_triple("urn:person:tony", "urn:knows", "urn:person:jane", true, None, None).unwrap();
        let triples = db.get_triples_by_subject("urn:person:tony").unwrap();
        assert_eq!(triples.len(), 1);
        assert!(triples[0].is_link);
    }

    #[test]
    fn unique_constraint() {
        let db = test_db();
        db.insert_triple("urn:person:tony", "urn:firstname", "Tony", false, None, None).unwrap();
        let inserted = db.insert_triple("urn:person:tony", "urn:firstname", "Tony", false, None, None).unwrap();
        assert!(!inserted);
    }

    #[test]
    fn get_by_subject() {
        let db = test_db();
        db.insert_triple("urn:person:tony", "urn:firstname", "Tony", false, None, None).unwrap();
        db.insert_triple("urn:person:tony", "urn:lastname", "Moulton", false, None, None).unwrap();
        db.insert_triple("urn:person:tony", "urn:age", "35", false, None, None).unwrap();
        let triples = db.get_triples_by_subject("urn:person:tony").unwrap();
        assert_eq!(triples.len(), 3);
    }

    #[test]
    fn get_by_subject_predicate() {
        let db = test_db();
        db.insert_triple("urn:person:tony", "urn:firstname", "Tony", false, None, None).unwrap();
        db.insert_triple("urn:person:tony", "urn:lastname", "Moulton", false, None, None).unwrap();
        let triples = db.get_triples_by_subject_predicate("urn:person:tony", "urn:firstname").unwrap();
        assert_eq!(triples.len(), 1);
        assert_eq!(triples[0].object, "Tony");
    }

    #[test]
    fn delete_entity() {
        let db = test_db();
        db.insert_triple("urn:person:tony", "urn:firstname", "Tony", false, None, None).unwrap();
        db.insert_triple("urn:person:tony", "urn:lastname", "Moulton", false, None, None).unwrap();
        let count = db.delete_entity("urn:person:tony").unwrap();
        assert_eq!(count, 2);
        assert!(!db.entity_exists("urn:person:tony").unwrap());
    }

    #[test]
    fn delete_predicate() {
        let db = test_db();
        db.insert_triple("urn:person:tony", "urn:firstname", "Tony", false, None, None).unwrap();
        db.insert_triple("urn:person:tony", "urn:lastname", "Moulton", false, None, None).unwrap();
        db.delete_predicate("urn:person:tony", "urn:firstname").unwrap();
        let triples = db.get_triples_by_subject("urn:person:tony").unwrap();
        assert_eq!(triples.len(), 1);
        assert_eq!(triples[0].predicate, "urn:lastname");
    }

    #[test]
    fn delete_specific_triple() {
        let db = test_db();
        db.insert_triple("urn:person:tony", "urn:phone", "+1-555-0123", false, None, None).unwrap();
        db.insert_triple("urn:person:tony", "urn:phone", "15550123", false, None, None).unwrap();
        db.delete_triple("urn:person:tony", "urn:phone", "+1-555-0123").unwrap();
        let triples = db.get_triples_by_subject_predicate("urn:person:tony", "urn:phone").unwrap();
        assert_eq!(triples.len(), 1);
        assert_eq!(triples[0].object, "15550123");
    }

    #[test]
    fn fts_insert_sync() {
        let db = test_db();
        db.insert_triple("urn:person:tony", "urn:firstname", "Tony", false, None, None).unwrap();
        let results = db.fts_search("Tony").unwrap();
        assert_eq!(results.len(), 1);
    }

    #[test]
    fn fts_delete_sync() {
        let db = test_db();
        db.insert_triple("urn:person:tony", "urn:firstname", "Tony", false, None, None).unwrap();
        db.delete_entity("urn:person:tony").unwrap();
        let results = db.fts_search("Tony").unwrap();
        assert_eq!(results.len(), 0);
    }

    #[test]
    fn get_links_only() {
        let db = test_db();
        db.insert_triple("urn:person:tony", "urn:firstname", "Tony", false, None, None).unwrap();
        db.insert_triple("urn:person:tony", "urn:knows", "urn:person:jane", true, None, None).unwrap();
        let links = db.get_outbound_links("urn:person:tony").unwrap();
        assert_eq!(links.len(), 1);
        assert_eq!(links[0].object, "urn:person:jane");
    }

    #[test]
    fn provenance_stored() {
        let db = test_db();
        db.insert_triple("urn:person:tony", "urn:age", "35", false, Some("public records"), Some(0.9)).unwrap();
        let triples = db.get_triples_by_subject("urn:person:tony").unwrap();
        assert_eq!(triples[0].source.as_deref(), Some("public records"));
        assert_eq!(triples[0].confidence, Some(0.9));
    }

    #[test]
    fn find_inbound_links() {
        let db = test_db();
        db.insert_triple("urn:person:tony", "urn:knows", "urn:person:jane", true, None, None).unwrap();
        db.insert_triple("urn:person:bob", "urn:knows", "urn:person:jane", true, None, None).unwrap();
        let inbound = db.find_inbound_links("urn:person:jane").unwrap();
        assert_eq!(inbound.len(), 2);
    }

    #[test]
    fn list_types() {
        let db = test_db();
        db.insert_triple("urn:person:tony", "urn:name", "Tony", false, None, None).unwrap();
        db.insert_triple("urn:person:jane", "urn:name", "Jane", false, None, None).unwrap();
        db.insert_triple("urn:org:acme", "urn:name", "Acme", false, None, None).unwrap();
        let types = db.list_types().unwrap();
        assert_eq!(types.len(), 2);
    }

    #[test]
    fn schema_for_type() {
        let db = test_db();
        db.insert_triple("urn:person:tony", "urn:firstname", "Tony", false, None, None).unwrap();
        db.insert_triple("urn:person:tony", "urn:lastname", "Moulton", false, None, None).unwrap();
        db.insert_triple("urn:person:jane", "urn:firstname", "Jane", false, None, None).unwrap();
        let schema = db.schema_for_type("person").unwrap();
        assert_eq!(schema.len(), 2);
        // firstname should have count 2
        let firstname = schema.iter().find(|(p, _)| p == "urn:firstname").unwrap();
        assert_eq!(firstname.1, 2);
    }

    #[test]
    fn resolve_default_path() {
        let dir = tempfile::tempdir().unwrap();
        let _guard = std::env::set_current_dir(dir.path());
        // We can't easily test resolve_db_path without changing cwd
        // so just test the logic paths directly
        let path = resolve_db_path(None).unwrap();
        assert!(path.to_str().unwrap().contains("graph.db"));
    }
}
