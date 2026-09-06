use assert_cmd::Command;
use predicates::prelude::*;
use std::path::Path;

/// Build a `kg` command that runs inside `dir` so `.kg/graph.db` is scoped to the test.
pub fn kg(dir: &Path) -> Command {
    let mut cmd = Command::cargo_bin("kg").expect("kg binary built");
    cmd.current_dir(dir);
    cmd
}

#[test]
fn version_flag_prints_name_and_version() {
    let dir = tempfile::tempdir().unwrap();
    kg(dir.path())
        .arg("--version")
        .assert()
        .success()
        .stdout(predicate::str::starts_with("kg 0.1.0"));
}

#[test]
fn help_quick_start_example_actually_runs() {
    let dir = tempfile::tempdir().unwrap();
    kg(dir.path())
        .args([
            "create",
            "urn:person:alice-example",
            "urn:prop:name=Alice",
            "--source",
            "osint",
            "--confidence",
            "0.8",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"urn:prop:name\": \"Alice\""));
}

#[test]
fn create_without_properties_is_an_error() {
    let dir = tempfile::tempdir().unwrap();
    kg(dir.path())
        .args(["create", "urn:person:alice-example"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("required").or(predicate::str::contains("at least one")));
    kg(dir.path())
        .args(["get", "urn:person:alice-example"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no graph database found"));
}

#[test]
fn provenance_flag_exposes_source_and_confidence() {
    let dir = tempfile::tempdir().unwrap();
    kg(dir.path())
        .args([
            "set",
            "urn:person:alice-example",
            "urn:prop:age",
            "35",
            "--source",
            "public-records",
            "--confidence",
            "0.9",
        ])
        .assert()
        .success();
    kg(dir.path())
        .args(["get", "urn:person:alice-example", "--provenance"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"source\": \"public-records\""))
        .stdout(predicate::str::contains("\"confidence\": 0.9"))
        .stdout(predicate::str::contains("\"created_at\""));
    kg(dir.path())
        .args(["get", "urn:person:alice-example"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"urn:prop:age\": \"35\""));
}

#[test]
fn confidence_outside_unit_interval_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    kg(dir.path())
        .args([
            "set",
            "urn:person:alice-example",
            "urn:prop:age",
            "35",
            "--confidence",
            "7",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("between 0 and 1"));
}

#[test]
fn neighbors_rejects_unknown_direction() {
    let dir = tempfile::tempdir().unwrap();
    kg(dir.path())
        .args([
            "neighbors",
            "urn:person:alice-example",
            "--direction",
            "sideways",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("possible values"));
}

#[test]
fn read_commands_do_not_create_a_database() {
    let dir = tempfile::tempdir().unwrap();
    kg(dir.path())
        .args(["types"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("no graph database found"));
    assert!(
        !dir.path().join(".kg").exists(),
        ".kg directory must not be created by a read command"
    );
}

#[cfg(unix)]
#[test]
fn new_database_is_private_to_the_user() {
    use std::os::unix::fs::PermissionsExt;
    let dir = tempfile::tempdir().unwrap();
    kg(dir.path())
        .args(["set", "urn:person:alice-example", "urn:prop:age", "35"])
        .assert()
        .success();
    let mode = std::fs::metadata(dir.path().join(".kg/graph.db"))
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(mode, 0o600, "mode was {mode:o}");
}
