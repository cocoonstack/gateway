use std::process::Command;

#[test]
fn version_flag_prints_the_version_and_exits() {
    let out = Command::new(env!("CARGO_BIN_EXE_gw"))
        .arg("--version")
        .output()
        .expect("run gw");
    assert!(out.status.success());
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        format!("gw {}", env!("CARGO_PKG_VERSION"))
    );
}

#[test]
fn an_unknown_argument_is_refused_with_usage() {
    let out = Command::new(env!("CARGO_BIN_EXE_gw"))
        .arg("--bogus")
        .output()
        .expect("run gw");
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("usage: gw"));
}
