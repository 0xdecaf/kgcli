mod commands;
mod db;
mod jsonld;
mod model;

use anyhow::Result;
use clap::{Parser, Subcommand};

use db::{Database, resolve_db_path};

#[derive(Clone, Copy, Debug, clap::ValueEnum)]
pub enum Direction {
    In,
    Out,
    Both,
}

fn parse_confidence(s: &str) -> Result<f64, String> {
    let v: f64 = s.parse().map_err(|_| format!("`{s}` is not a number"))?;
    if (0.0..=1.0).contains(&v) {
        Ok(v)
    } else {
        Err(format!("confidence must be between 0 and 1, got {v}"))
    }
}

#[derive(Parser)]
#[command(
    name = "kg",
    version,
    about = "Graph database CLI for OSINT investigations",
    long_about = "kg stores an investigation as subject-predicate-object triples in a local \
SQLite file. Subjects, predicates, and link targets are URNs of the form \
urn:<type>:<id> (predicates too, e.g. urn:prop:name). Literal values are plain strings.\n\n\
Quick start:\n  \
kg create urn:person:alice-example urn:prop:name=Alice --source osint --confidence 0.8\n  \
kg link urn:person:alice-example urn:rel:works-at urn:org:acme-example\n  \
kg get urn:person:alice-example --expand"
)]
struct Cli {
    /// Named graph or path to database file
    #[arg(long, global = true)]
    graph: Option<String>,

    /// Include source, confidence, and created_at on every value
    #[arg(long, global = true)]
    provenance: bool,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create an entity with optional key=value properties
    Create {
        /// Entity URN (e.g. urn:person:alice-example)
        subject: String,
        /// Properties as <predicate-urn>=<value> pairs (e.g. urn:prop:name=Alice)
        props: Vec<String>,
        #[arg(long)]
        source: Option<String>,
        #[arg(long, value_parser = parse_confidence)]
        confidence: Option<f64>,
    },
    /// Set a literal property on an entity
    Set {
        subject: String,
        predicate: String,
        value: String,
        #[arg(long)]
        source: Option<String>,
        #[arg(long, value_parser = parse_confidence)]
        confidence: Option<f64>,
    },
    /// Get an entity and its properties
    Get {
        subject: String,
        /// Recursively expand linked entities
        #[arg(long)]
        expand: bool,
    },
    /// Delete an entity, a predicate, or a specific triple
    Delete {
        subject: String,
        predicate: Option<String>,
        value: Option<String>,
    },
    /// Create a link between two entities
    Link {
        subject: String,
        predicate: String,
        target: String,
        #[arg(long)]
        source: Option<String>,
        #[arg(long, value_parser = parse_confidence)]
        confidence: Option<f64>,
    },
    /// Remove a link between two entities
    Unlink {
        subject: String,
        predicate: String,
        target: String,
    },
    /// Full-text search across all entities
    Search { query: String },
    /// Find entities by predicate and optional value
    Query {
        predicate: String,
        value: Option<String>,
    },
    /// List all entity types with counts
    Types,
    /// Show predicates used for a given entity type
    Schema { entity_type: String },
    /// Show inbound and/or outbound links for an entity
    Neighbors {
        subject: String,
        /// Direction: in, out, or both
        #[arg(long, value_enum, default_value_t = Direction::Both)]
        direction: Direction,
    },
    /// Merge source entity into target entity
    Merge { source: String, target: String },
    /// Find shortest path between two entities
    Path {
        from: String,
        to: String,
        /// Maximum search depth
        #[arg(long, default_value = "6")]
        max_depth: usize,
    },
    /// Promote a literal value to a link
    Promote {
        subject: String,
        predicate: String,
        value: String,
        target: String,
    },
}

impl Command {
    fn is_read_only(&self) -> bool {
        matches!(
            self,
            Command::Get { .. }
                | Command::Search { .. }
                | Command::Query { .. }
                | Command::Types
                | Command::Schema { .. }
                | Command::Neighbors { .. }
                | Command::Path { .. }
        )
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let read_only = cli.command.is_read_only();
    let db_path = resolve_db_path(cli.graph.as_deref(), !read_only)?;
    let db = if read_only {
        Database::open_read_only(&db_path)?
    } else {
        Database::open(&db_path)?
    };
    let opts = jsonld::OutputOpts {
        provenance: cli.provenance,
    };

    match cli.command {
        Command::Create {
            subject,
            props,
            source,
            confidence,
        } => {
            let predicates: Vec<(String, String)> = props
                .iter()
                .map(|p| {
                    let (k, v) = p.split_once('=').ok_or_else(|| {
                        anyhow::anyhow!("invalid property (expected key=value): {p}")
                    })?;
                    Ok((k.to_string(), v.to_string()))
                })
                .collect::<Result<Vec<_>>>()?;
            commands::create::run(
                &db,
                &subject,
                &predicates,
                source.as_deref(),
                confidence,
                opts,
            )
        }
        Command::Set {
            subject,
            predicate,
            value,
            source,
            confidence,
        } => commands::set::run(
            &db,
            &subject,
            &predicate,
            &value,
            source.as_deref(),
            confidence,
            opts,
        ),
        Command::Get { subject, expand } => commands::get::run(&db, &subject, expand, opts),
        Command::Delete {
            subject,
            predicate,
            value,
        } => commands::delete::run(&db, &subject, predicate.as_deref(), value.as_deref(), opts),
        Command::Link {
            subject,
            predicate,
            target,
            source,
            confidence,
        } => commands::link::run(
            &db,
            &subject,
            &predicate,
            &target,
            source.as_deref(),
            confidence,
            opts,
        ),
        Command::Unlink {
            subject,
            predicate,
            target,
        } => commands::unlink::run(&db, &subject, &predicate, &target, opts),
        Command::Search { query } => commands::search::run(&db, &query, opts),
        Command::Query { predicate, value } => {
            commands::query::run(&db, &predicate, value.as_deref())
        }
        Command::Types => commands::types::run(&db),
        Command::Schema { entity_type } => commands::schema::run(&db, &entity_type),
        Command::Neighbors { subject, direction } => {
            commands::neighbors::run(&db, &subject, direction)
        }
        Command::Merge { source, target } => commands::merge::run(&db, &source, &target, opts),
        Command::Path {
            from,
            to,
            max_depth,
        } => commands::path::run(&db, &from, &to, max_depth),
        Command::Promote {
            subject,
            predicate,
            value,
            target,
        } => commands::promote::run(&db, &subject, &predicate, &value, &target, opts),
    }
}
