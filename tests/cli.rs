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
        .stderr(predicate::str::contains("at least one"));
    kg(dir.path())
        .args(["get", "urn:person:alice-example"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("entity not found"));
}
