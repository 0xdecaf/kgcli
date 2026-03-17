mod commands;
mod db;
mod jsonld;
mod model;

use anyhow::Result;
use clap::{Parser, Subcommand};

use db::{Database, resolve_db_path};

#[derive(Parser)]
#[command(name = "kg", about = "Graph database CLI for OSINT investigations")]
struct Cli {
    /// Named graph or path to database file
    #[arg(long, global = true)]
    graph: Option<String>,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Create an entity with optional key=value properties
    Create {
        /// Entity URN (e.g. urn:person:tony-moulton)
        subject: String,
        /// Properties as key=value pairs (e.g. urn:name=Tony)
        #[arg(trailing_var_arg = true)]
        props: Vec<String>,
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
        confidence: Option<f64>,
    },
    /// Set a literal property on an entity
    Set {
        subject: String,
        predicate: String,
        value: String,
        #[arg(long)]
        source: Option<String>,
        #[arg(long)]
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
        #[arg(long)]
        confidence: Option<f64>,
    },
    /// Remove a link between two entities
    Unlink {
        subject: String,
        predicate: String,
        target: String,
    },
    /// Full-text search across all entities
    Search {
        query: String,
    },
    /// Find entities by predicate and optional value
    Query {
        predicate: String,
        value: Option<String>,
    },
    /// List all entity types with counts
    Types,
    /// Show predicates used for a given entity type
    Schema {
        entity_type: String,
    },
    /// Show inbound and/or outbound links for an entity
    Neighbors {
        subject: String,
        /// Direction: in, out, or both
        #[arg(long, default_value = "both")]
        direction: String,
    },
    /// Merge source entity into target entity
    Merge {
        source: String,
        target: String,
    },
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

fn main() -> Result<()> {
    let cli = Cli::parse();
    let db_path = resolve_db_path(cli.graph.as_deref())?;
    let db = Database::open(&db_path)?;

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
                    let (k, v) = p
                        .split_once('=')
                        .ok_or_else(|| anyhow::anyhow!("invalid property (expected key=value): {p}"))?;
                    Ok((k.to_string(), v.to_string()))
                })
                .collect::<Result<Vec<_>>>()?;
            commands::create::run(&db, &subject, &predicates, source.as_deref(), confidence)
        }
        Command::Set {
            subject,
            predicate,
            value,
            source,
            confidence,
        } => commands::set::run(&db, &subject, &predicate, &value, source.as_deref(), confidence),
        Command::Get { subject, expand } => commands::get::run(&db, &subject, expand),
        Command::Delete {
            subject,
            predicate,
            value,
        } => commands::delete::run(&db, &subject, predicate.as_deref(), value.as_deref()),
        Command::Link {
            subject,
            predicate,
            target,
            source,
            confidence,
        } => commands::link::run(&db, &subject, &predicate, &target, source.as_deref(), confidence),
        Command::Unlink {
            subject,
            predicate,
            target,
        } => commands::unlink::run(&db, &subject, &predicate, &target),
        Command::Search { query } => commands::search::run(&db, &query),
        Command::Query { predicate, value } => {
            commands::query::run(&db, &predicate, value.as_deref())
        }
        Command::Types => commands::types::run(&db),
        Command::Schema { entity_type } => commands::schema::run(&db, &entity_type),
        Command::Neighbors { subject, direction } => {
            commands::neighbors::run(&db, &subject, &direction)
        }
        Command::Merge { source, target } => commands::merge::run(&db, &source, &target),
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
        } => commands::promote::run(&db, &subject, &predicate, &value, &target),
    }
}
