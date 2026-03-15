use anyhow::{bail, Result};

/// A parsed URN with type and id segments.
/// Format: `urn:<type>:<id>` where id may contain additional colons.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Urn {
    pub full: String,
    pub entity_type: String,
    pub id: String,
}

impl Urn {
    pub fn parse(input: &str) -> Result<Self> {
        if input.is_empty() {
            bail!("empty URN");
        }
        if !input.starts_with("urn:") {
            bail!("not a valid URN (must start with 'urn:'): {input}");
        }
        let rest = &input[4..];
        let colon_pos = rest
            .find(':')
            .ok_or_else(|| anyhow::anyhow!("incomplete URN (missing id segment): {input}"))?;
        let entity_type = &rest[..colon_pos];
        let id = &rest[colon_pos + 1..];

        if entity_type.is_empty() {
            bail!("empty type segment in URN: {input}");
        }
        if id.is_empty() {
            bail!("empty id segment in URN: {input}");
        }
        if id.contains(char::is_whitespace) {
            bail!("whitespace not allowed in URN id: {input}");
        }

        Ok(Self {
            full: input.to_string(),
            entity_type: entity_type.to_string(),
            id: id.to_string(),
        })
    }
}

impl std::fmt::Display for Urn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.full)
    }
}

/// A triple stored in the database.
#[derive(Debug, Clone)]
pub struct Triple {
    pub id: i64,
    pub subject: String,
    pub predicate: String,
    pub object: String,
    pub is_link: bool,
    pub source: Option<String>,
    pub confidence: Option<f64>,
    pub created_at: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn valid_urn() {
        let u = Urn::parse("urn:person:tony-moulton").unwrap();
        assert_eq!(u.entity_type, "person");
        assert_eq!(u.id, "tony-moulton");
        assert_eq!(u.full, "urn:person:tony-moulton");
    }

    #[test]
    fn valid_urn_with_dots() {
        let u = Urn::parse("urn:domain:example.com").unwrap();
        assert_eq!(u.entity_type, "domain");
        assert_eq!(u.id, "example.com");
    }

    #[test]
    fn valid_urn_with_nested_colons() {
        let u = Urn::parse("urn:hash:sha256:abc123").unwrap();
        assert_eq!(u.entity_type, "hash");
        assert_eq!(u.id, "sha256:abc123");
    }

    #[test]
    fn missing_prefix() {
        assert!(Urn::parse("person:tony").is_err());
    }

    #[test]
    fn empty_type() {
        assert!(Urn::parse("urn::tony").is_err());
    }

    #[test]
    fn empty_id() {
        assert!(Urn::parse("urn:person:").is_err());
    }

    #[test]
    fn just_urn_prefix() {
        assert!(Urn::parse("urn:").is_err());
    }

    #[test]
    fn empty_string() {
        assert!(Urn::parse("").is_err());
    }

    #[test]
    fn unicode_in_id() {
        let u = Urn::parse("urn:person:müller").unwrap();
        assert_eq!(u.entity_type, "person");
        assert_eq!(u.id, "müller");
    }

    #[test]
    fn whitespace_in_id() {
        assert!(Urn::parse("urn:person:tony moulton").is_err());
    }

    #[test]
    fn case_preserved() {
        let u = Urn::parse("urn:Person:Tony").unwrap();
        assert_eq!(u.entity_type, "Person");
        assert_eq!(u.id, "Tony");
    }

    #[test]
    fn type_extraction_person() {
        let u = Urn::parse("urn:person:tony-moulton").unwrap();
        assert_eq!(u.entity_type, "person");
    }

    #[test]
    fn type_extraction_org() {
        let u = Urn::parse("urn:org:acme-corp").unwrap();
        assert_eq!(u.entity_type, "org");
    }
}
