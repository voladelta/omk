use std::io::Write;
use std::process::{Command, Output, Stdio};

use omk::SCHEMA_VERSION;
use serde_json::Value;

mod contracts;
mod idempotency;

fn omk(db: &std::path::Path, args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_omk"))
        .arg("--db")
        .arg(db)
        .args(args)
        .output()
        .unwrap()
}

fn success_json(db: &std::path::Path, args: &[&str]) -> Value {
    let output = omk(db, args);
    assert!(
        output.status.success(),
        "command failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}

fn omk_with_stdin(db: &std::path::Path, args: &[&str], input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_omk"))
        .arg("--db")
        .arg(db)
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    child.wait_with_output().unwrap()
}
