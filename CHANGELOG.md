# Changelog

## [Unreleased]

### Added
- `--provenance` flag exposing `source`, `confidence`, and `created_at`.
- `--version`.
- README, CHANGELOG, integration test suite (`assert_cmd`).
- MSRV and coverage CI jobs.

### Changed
- `create` now requires at least one property.
- Read commands open the database read-only and no longer create `.kg/`.
- `created_at` is RFC 3339 UTC with a `Z` suffix.
- New database files are created with mode 0600.
- `merge` and `promote` are atomic; `merge` no longer creates self-loops;
  `promote` keeps the literal's provenance.

### Fixed
- Flags may now follow `create` properties (`--source` after `k=v`).
- `--confidence` must be within 0 to 1; `--direction` is validated.
- `search` treats input literally instead of as FTS5 syntax.
- CI was failing on formatting and a dead-code warning.
