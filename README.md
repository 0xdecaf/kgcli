# kgcli

[![CI](https://github.com/0xdecaf/kgcli/actions/workflows/ci.yml/badge.svg)](https://github.com/0xdecaf/kgcli/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

`kg` is a single-binary command-line knowledge graph for OSINT investigations.
It stores facts as subject-predicate-object triples in a local SQLite file,
records where each fact came from and how much you trust it, and answers
questions like "how is this domain connected to that person?" without a
server, a schema migration, or a browser.

## Why

Investigation notes rot in text files. Graph databases are heavy for a case
that fits in one SQLite file. `kg` sits in between: every fact has provenance,
every entity is addressable by URN, and the whole graph is a file you can
`cp`, encrypt, or delete.

## Install

```bash
cargo install --git https://github.com/0xdecaf/kgcli
# or download a release tarball from the Releases page
```

## Sixty-second walkthrough

```bash
kg create urn:person:alice-example urn:prop:name=Alice --source osint --confidence 0.8
kg set urn:person:alice-example urn:prop:employer "Acme Corp" --source linkedin --confidence 0.6
kg create urn:org:acme-example urn:prop:name="Acme Corp" --source opencorporates
kg promote urn:person:alice-example urn:prop:employer "Acme Corp" urn:org:acme-example
kg link urn:org:acme-example urn:rel:owns urn:domain:example.com --source whois
kg get urn:person:alice-example --expand --provenance
kg path urn:person:alice-example urn:domain:example.com
kg search acme
```

```json
{
  "@context": {
    "urn": "urn:"
  },
  "@id": "urn:person:alice-example",
  "@type": "person",
  "urn:prop:employer": {
    "@id": "urn:org:acme-example",
    "@type": "org",
    "confidence": 0.6,
    "created_at": "2026-09-06T18:03:03.475Z",
    "source": "linkedin",
    "urn:prop:name": {
      "@value": "Acme Corp",
      "created_at": "2026-09-06T18:03:03.469Z",
      "source": "opencorporates"
    },
    "urn:rel:owns": {
      "@id": "urn:domain:example.com",
      "created_at": "2026-09-06T18:03:03.480Z",
      "source": "whois"
    }
  },
  ...
}
```

## Data model

- **Entities** are URNs: `urn:<type>:<id>`. The type segment is required and
  is used by `kg types` and `kg schema`. Ids may contain further colons
  (`urn:hash:sha256:abc…`) but not whitespace.
- **Predicates** are also URNs (`urn:prop:name`, `urn:rel:knows`). The prefix
  is a convention, not enforced; pick one and stay consistent.
- **Literals** are plain strings. **Links** point at another entity URN and are
  created with `kg link` or by promoting a literal with `kg promote`.
- **Provenance**: every triple carries `--source`, `--confidence` (0 to 1),
  and a UTC `created_at`. Show them with `--provenance` on any command.
  Databases created before this version keep their older timestamp format
  for existing rows.
- **Existence** is defined by triples: an entity exists once something is
  asserted about it. `kg create` therefore requires at least one property.
- **Dangling links** are allowed. `kg link` may point at an entity that has no
  triples yet; `kg delete` warns when inbound links are left behind.

## Commands

| Command | Purpose |
|---|---|
| `create <urn> <pred>=<val>…` | Assert one or more literal properties |
| `set <urn> <pred> <val>` | Add a literal value (multi-valued predicates append) |
| `link <urn> <pred> <target-urn>` | Add a link |
| `unlink`, `delete` | Remove a link, a triple, a predicate, or an entity |
| `get <urn> [--expand]` | Show an entity, optionally with linked entities inlined |
| `promote <urn> <pred> <val> <target>` | Turn a literal into a link, keeping provenance |
| `merge <source> <target>` | Fold one entity into another (atomic, no self-loops) |
| `neighbors <urn> [--direction in|out|both]` | Adjacent entities |
| `path <from> <to> [--max-depth N]` | Shortest link path |
| `search <text>` | Full-text search over all triples (literal, per-token) |
| `query <pred> [<val>]` | Entities having a predicate (and value) |
| `types`, `schema <type>` | What is in the graph |

Global flags: `--graph <name|path>` (default `.kg/graph.db` in the current
directory), `--provenance`.

## Output

Every command prints JSON. Entity output uses JSON-LD-style keys (`@id`,
`@type`) so it is easy to post-process with `jq`; it is not a full JSON-LD
document. Read commands never create or modify the database.

## Handling sensitive data

The graph is a plain SQLite file created with mode 0600. It is not encrypted;
put it on an encrypted volume if the investigation warrants it. Add `.kg/` to
your project's `.gitignore` so a graph is never committed by accident.

## Development

```bash
cargo test
cargo clippy --all-targets -- -D warnings
```

## License

MIT
