use anyhow::{Result, bail};
use std::collections::{HashMap, HashSet, VecDeque};

use crate::db::Database;
use crate::model::Urn;

pub fn run(db: &Database, from: &str, to: &str, max_depth: usize) -> Result<()> {
    Urn::parse(from)?;
    Urn::parse(to)?;

    if from == to {
        let json = serde_json::json!([from]);
        println!("{}", serde_json::to_string_pretty(&json)?);
        return Ok(());
    }

    // BFS to find shortest path through links
    let mut visited = HashSet::new();
    let mut parent: HashMap<String, (String, String)> = HashMap::new(); // child -> (parent, predicate)
    let mut queue = VecDeque::new();

    visited.insert(from.to_string());
    queue.push_back(from.to_string());

    let mut found = false;

    while let Some(current) = queue.pop_front() {
        // Check depth
        let depth = {
            let mut d = 0;
            let mut node = current.as_str();
            while let Some((p, _)) = parent.get(node) {
                d += 1;
                node = p.as_str();
            }
            d
        };

        if depth >= max_depth {
            continue;
        }

        // Outbound links
        for t in db.get_outbound_links(&current)? {
            if !visited.contains(&t.object) {
                visited.insert(t.object.clone());
                parent.insert(t.object.clone(), (current.clone(), t.predicate.clone()));
                if t.object == to {
                    found = true;
                    break;
                }
                queue.push_back(t.object);
            }
        }

        if found {
            break;
        }

        // Inbound links
        for t in db.find_inbound_links(&current)? {
            if !visited.contains(&t.subject) {
                visited.insert(t.subject.clone());
                parent.insert(t.subject.clone(), (current.clone(), t.predicate.clone()));
                if t.subject == to {
                    found = true;
                    break;
                }
                queue.push_back(t.subject);
            }
        }

        if found {
            break;
        }
    }

    if !found {
        bail!("no path found between {from} and {to} within depth {max_depth}");
    }

    // Reconstruct path
    let mut path = Vec::new();
    let mut node = to.to_string();
    while let Some((p, predicate)) = parent.get(&node) {
        path.push(serde_json::json!({
            "from": p,
            "predicate": predicate,
            "to": node,
        }));
        node = p.clone();
    }
    path.reverse();

    println!("{}", serde_json::to_string_pretty(&path)?);
    Ok(())
}
