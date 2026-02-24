use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

struct TestEnv {
    _tmp: TempDir,
    home: PathBuf,
    state: PathBuf,
    data: PathBuf,
    conf: PathBuf,
}

impl TestEnv {
    fn new() -> Self {
        let tmp = TempDir::new().expect("tempdir");
        let root = tmp.path().to_path_buf();
        let home = root.join("home");
        let state = root.join("state");
        let data = root.join("data");
        let conf = root.join("conf");
        fs::create_dir_all(&home).unwrap();
        fs::create_dir_all(&state).unwrap();
        fs::create_dir_all(&data).unwrap();
        fs::create_dir_all(&conf).unwrap();
        Self {
            _tmp: tmp,
            home,
            state,
            data,
            conf,
        }
    }

    fn budom_path() -> PathBuf {
        PathBuf::from(assert_cmd::cargo::cargo_bin!("budom"))
    }

    fn cmd(&self) -> Command {
        let mut c = Command::new(Self::budom_path());
        c.env("HOME", &self.home);
        c.env("XDG_STATE_HOME", &self.state);
        c.env("XDG_DATA_HOME", &self.data);
        c.env("XDG_CONFIG_HOME", &self.conf);
        c
    }

    fn run(&self, args: &[&str]) -> Output {
        let mut c = self.cmd();
        c.args(args);
        c.output().expect("run budom")
    }

    fn run_ok(&self, args: &[&str]) -> String {
        let out = self.run(args);
        if !out.status.success() {
            panic!(
                "command failed {:?}\nstdout={}\nstderr={}",
                args,
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        String::from_utf8_lossy(&out.stdout).to_string()
    }

    fn inspect_json(&self, r: &str) -> Value {
        let out = self.run(&["inspect", r, "--json"]);
        if !out.status.success() {
            panic!(
                "inspect failed {}\nstdout={}\nstderr={}",
                r,
                String::from_utf8_lossy(&out.stdout),
                String::from_utf8_lossy(&out.stderr)
            );
        }
        serde_json::from_slice(&out.stdout).expect("json")
    }

    fn wait_for<F: FnMut() -> bool>(&self, timeout: Duration, mut f: F) {
        let start = Instant::now();
        while start.elapsed() < timeout {
            if f() {
                return;
            }
            thread::sleep(Duration::from_millis(50));
        }
        panic!("timeout waiting for condition");
    }

    fn job_dir(&self, id: &str) -> PathBuf {
        self.data.join("budom").join("jobs").join(id)
    }

    fn supervisor_pid_path(&self) -> PathBuf {
        self.state.join("budom").join("run").join("supervisor.pid")
    }
}

fn parse_run_id(stdout: &str) -> String {
    stdout.lines().next().unwrap_or_default().trim().to_string()
}

fn flip_first_alpha_case(s: &str) -> Option<String> {
    let mut out = String::with_capacity(s.len());
    let mut flipped = false;
    for ch in s.chars() {
        if !flipped && ch.is_ascii_alphabetic() {
            if ch.is_ascii_lowercase() {
                out.push(ch.to_ascii_uppercase());
            } else {
                out.push(ch.to_ascii_lowercase());
            }
            flipped = true;
        } else {
            out.push(ch);
        }
    }
    if flipped { Some(out) } else { None }
}

fn write_json(path: &Path, value: &Value) {
    fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

#[test]
fn lifecycle_start_ps_stop_rm() {
    let env = TestEnv::new();

    env.run_ok(&["run", "--name", "life", "--", "/bin/sh", "-c", "sleep 30"]);

    let ps = env.run_ok(&["ps", "--json"]);
    let rows: Value = serde_json::from_str(&ps).unwrap();
    let has_life = rows
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r.get("name") == Some(&Value::String("life".to_string())));
    assert!(has_life, "life job should appear in ps");

    env.run_ok(&["stop", "life", "--timeout", "2s"]);

    env.wait_for(Duration::from_secs(5), || {
        let v = env.inspect_json("life");
        v["status"]["state"] == Value::String("exited".to_string())
    });

    env.run_ok(&["rm", "life"]);
    let inspect = env.run(&["inspect", "life"]);
    assert_eq!(inspect.status.code(), Some(3));
}

#[test]
fn restart_policy_increments_restart_count() {
    let env = TestEnv::new();

    env.run_ok(&[
        "run",
        "--name",
        "rp",
        "--restart",
        "on-failure",
        "--max-retries",
        "2",
        "--backoff",
        "20,80",
        "--",
        "/bin/sh",
        "-c",
        "exit 1",
    ]);

    env.wait_for(Duration::from_secs(5), || {
        let v = env.inspect_json("rp");
        v["status"]["state"] == Value::String("exited".to_string())
            && v["status"]["restart_count"].as_u64().unwrap_or_default() >= 2
    });

    let v = env.inspect_json("rp");
    assert_eq!(v["status"]["restart_count"].as_u64(), Some(2));
    assert_eq!(v["status"]["last_exit"]["code"].as_i64(), Some(1));

    env.run_ok(&["rm", "rp", "--force"]);
}

#[test]
fn raw_log_bytes_are_preserved() {
    let env = TestEnv::new();

    let stdout = env.run_ok(&[
        "run",
        "--name",
        "raw",
        "--",
        "/bin/sh",
        "-c",
        "printf 'A\\000B\\n'; printf 'ERR\\n' 1>&2",
    ]);
    let id = parse_run_id(&stdout);

    env.wait_for(Duration::from_secs(3), || {
        let v = env.inspect_json("raw");
        v["status"]["state"] == Value::String("exited".to_string())
    });

    let out_path = env.job_dir(&id).join("stdout.log");
    let err_path = env.job_dir(&id).join("stderr.log");

    assert_eq!(fs::read(out_path).unwrap(), b"A\0B\n");
    assert_eq!(fs::read(err_path).unwrap(), b"ERR\n");

    env.run_ok(&["rm", "raw"]);
}

#[test]
fn log_rotation_creates_numbered_files() {
    let env = TestEnv::new();

    let stdout = env.run_ok(&[
        "run",
        "--name",
        "rot",
        "--log-max-size-mb",
        "1",
        "--log-max-files",
        "2",
        "--",
        "/bin/sh",
        "-c",
        "head -c 1300000 /dev/zero",
    ]);
    let id = parse_run_id(&stdout);

    env.wait_for(Duration::from_secs(5), || {
        let v = env.inspect_json("rot");
        v["status"]["state"] == Value::String("exited".to_string())
    });

    let dir = env.job_dir(&id);
    let active = dir.join("stdout.log");
    let rotated = dir.join("stdout.log.1");

    assert!(active.exists(), "active stdout.log should exist");
    assert!(rotated.exists(), "rotated stdout.log.1 should exist");
    assert!(fs::metadata(rotated).unwrap().len() > 0);

    env.run_ok(&["rm", "rot"]);
}

#[test]
fn stale_state_is_reported_after_supervisor_restart() {
    let env = TestEnv::new();

    let first = env.run_ok(&["run", "--name", "stale", "--", "/bin/sh", "-c", "sleep 30"]);
    let first_id = parse_run_id(&first);

    let pid_text = fs::read_to_string(env.supervisor_pid_path()).unwrap();
    let sup_pid: i32 = pid_text.trim().parse().unwrap();
    Command::new("kill")
        .args(["-9", &sup_pid.to_string()])
        .status()
        .unwrap();

    let desired_path = env.job_dir(&first_id).join("desired.json");
    let mut desired: Value = serde_json::from_slice(&fs::read(&desired_path).unwrap()).unwrap();
    desired["desired"] = Value::String("stopped".to_string());
    desired["updated_at"] = Value::String("2026-02-14T00:00:00Z".to_string());
    write_json(&desired_path, &desired);

    env.run_ok(&["run", "--name", "kick", "--", "/bin/true"]);

    env.wait_for(Duration::from_secs(5), || {
        let v = env.inspect_json("stale");
        v["status"]["state"] == Value::String("stale".to_string())
    });

    let stale_v = env.inspect_json("stale");
    if let Some(pid) = stale_v["status"]["pid"].as_i64() {
        let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();
    }

    env.run_ok(&["rm", "stale", "--force"]);
    env.run_ok(&["rm", "kick", "--force"]);
}

#[test]
fn second_run_works_while_supervisor_is_already_running() {
    let env = TestEnv::new();

    env.run_ok(&["run", "--name", "keeper", "--", "/bin/sh", "-c", "sleep 30"]);

    let second = env.run(&["run", "--", "/bin/true"]);
    assert_eq!(
        second.status.code(),
        Some(0),
        "second run should succeed\\nstdout={}\\nstderr={}",
        String::from_utf8_lossy(&second.stdout),
        String::from_utf8_lossy(&second.stderr)
    );

    let second_id = String::from_utf8_lossy(&second.stdout).trim().to_string();
    assert!(!second_id.is_empty(), "second run should print an id");

    env.wait_for(Duration::from_secs(3), || {
        let out = env.run(&["inspect", &second_id, "--json"]);
        out.status.success()
    });

    env.run_ok(&["stop", "keeper"]);
    env.run_ok(&["rm", "keeper", "--force"]);
    env.run_ok(&["rm", &second_id, "--force"]);
}

#[test]
fn ps_json_exits_cleanly_on_broken_pipe() {
    let env = TestEnv::new();

    env.run_ok(&["run", "--name", "bp", "--", "/bin/sh", "-c", "sleep 30"]);

    let mut cmd = env.cmd();
    cmd.args(["ps", "--json"]);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());
    let mut child = cmd.spawn().expect("spawn ps");

    drop(child.stdout.take());
    let out = child.wait_with_output().expect("wait ps");

    assert!(
        out.status.success(),
        "ps should exit cleanly on broken pipe\\nstdout={}\\nstderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let stderr = String::from_utf8_lossy(&out.stderr);
    assert!(
        !stderr.contains("failed printing to stdout"),
        "panic output should not be present: {stderr}"
    );
    assert!(
        !stderr.contains("Broken pipe"),
        "broken pipe should be handled quietly: {stderr}"
    );

    env.run_ok(&["stop", "bp", "--timeout", "2s"]);
    env.run_ok(&["rm", "bp", "--force"]);
}

#[test]
fn running_job_status_file_does_not_churn_at_idle() {
    let env = TestEnv::new();

    let out = env.run_ok(&["run", "--name", "idle", "--", "/bin/sh", "-c", "sleep 30"]);
    let id = parse_run_id(&out);

    env.wait_for(Duration::from_secs(3), || {
        let v = env.inspect_json("idle");
        v["status"]["state"] == Value::String("running".to_string())
    });

    let status_path = env.job_dir(&id).join("status.json");
    let first = fs::read(&status_path).expect("read first status");
    thread::sleep(Duration::from_millis(700));
    let second = fs::read(&status_path).expect("read second status");

    assert_eq!(
        first, second,
        "idle status file should remain unchanged over short interval"
    );

    env.run_ok(&["stop", "idle", "--timeout", "2s"]);
    env.run_ok(&["rm", "idle", "--force"]);
}

#[test]
fn ps_reconciles_running_state_against_real_process_liveness() {
    let env = TestEnv::new();

    let out = env.run_ok(&["run", "--name", "ghost", "--", "/bin/sh", "-c", "sleep 30"]);
    let id = parse_run_id(&out);
    let v = env.inspect_json("ghost");
    let pid = v["status"]["pid"].as_i64().expect("pid") as i32;

    let sup_pid_text = fs::read_to_string(env.supervisor_pid_path()).unwrap();
    let sup_pid: i32 = sup_pid_text.trim().parse().unwrap();

    let desired_path = env.job_dir(&id).join("desired.json");
    let mut desired: Value = serde_json::from_slice(&fs::read(&desired_path).unwrap()).unwrap();
    desired["desired"] = Value::String("stopped".to_string());
    desired["updated_at"] = Value::String("2026-02-20T00:00:00Z".to_string());
    write_json(&desired_path, &desired);

    let _ = Command::new("kill")
        .args(["-9", &sup_pid.to_string()])
        .status();
    let _ = Command::new("kill").args(["-9", &pid.to_string()]).status();

    thread::sleep(Duration::from_millis(150));

    let ps = env.run_ok(&["ps", "--json"]);
    let rows: Value = serde_json::from_str(&ps).unwrap();
    let listed_in_default = rows
        .as_array()
        .unwrap()
        .iter()
        .any(|r| r["id"] == Value::String(id.clone()));
    assert!(
        !listed_in_default,
        "stale/dead job should not appear in default ps output: {rows}"
    );

    let ps_all = env.run_ok(&["ps", "--all", "--json"]);
    let rows_all: Value = serde_json::from_str(&ps_all).unwrap();
    let row = rows_all
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["id"] == Value::String(id.clone()))
        .expect("job should still be listed in --all");
    assert_eq!(row["state"], Value::String("stale".to_string()));
    assert_eq!(row["pid"], Value::Null);

    env.run_ok(&["rm", "ghost", "--force"]);
}

#[test]
fn recover_restarts_desired_running_jobs_after_supervisor_restart() {
    let env = TestEnv::new();

    let out = env.run_ok(&["run", "--name", "boot", "--", "/bin/sh", "-c", "sleep 30"]);
    let _id = parse_run_id(&out);
    let v = env.inspect_json("boot");
    let old_pid = v["status"]["pid"].as_i64().expect("pid") as i32;

    let sup_pid_text = fs::read_to_string(env.supervisor_pid_path()).unwrap();
    let sup_pid: i32 = sup_pid_text.trim().parse().unwrap();
    let _ = Command::new("kill")
        .args(["-9", &sup_pid.to_string()])
        .status();
    let _ = Command::new("kill")
        .args(["-9", &old_pid.to_string()])
        .status();

    thread::sleep(Duration::from_millis(150));

    let recover_out = env.run_ok(&["recover", "--json"]);
    let rec: Value = serde_json::from_str(&recover_out).unwrap();
    assert_eq!(rec["desired_running"].as_u64(), Some(1));
    assert_eq!(rec["failed"].as_u64(), Some(0));
    assert_eq!(
        rec["started"].as_u64().unwrap_or(0) + rec["already_running"].as_u64().unwrap_or(0),
        1
    );

    env.wait_for(Duration::from_secs(5), || {
        let v = env.inspect_json("boot");
        v["status"]["state"] == Value::String("running".to_string())
            && v["status"]["pid"].as_i64().is_some()
    });

    let v2 = env.inspect_json("boot");
    assert_ne!(
        v2["status"]["pid"].as_i64().unwrap_or_default(),
        i64::from(old_pid),
        "pid should be replaced after restart"
    );

    env.run_ok(&["stop", "boot", "--timeout", "2s"]);
    env.run_ok(&["rm", "boot", "--force"]);
}

#[test]
fn recover_if_supervisor_down_skips_when_supervisor_is_running() {
    let env = TestEnv::new();

    env.run_ok(&["run", "--name", "noop", "--", "/bin/sh", "-c", "sleep 30"]);

    let out = env.run_ok(&["recover", "--if-supervisor-down", "--json"]);
    let payload: Value = serde_json::from_str(&out).unwrap();
    assert_eq!(payload["skipped"], Value::Bool(true));
    assert_eq!(
        payload["reason"],
        Value::String("supervisor_running".to_string())
    );

    env.run_ok(&["stop", "noop", "--timeout", "2s"]);
    env.run_ok(&["rm", "noop", "--force"]);
}

#[test]
fn stop_accepts_multiple_refs() {
    let env = TestEnv::new();

    env.run_ok(&["run", "--name", "a", "--", "/bin/sh", "-c", "sleep 30"]);
    env.run_ok(&["run", "--name", "b", "--", "/bin/sh", "-c", "sleep 30"]);

    env.run_ok(&["stop", "a", "b", "--timeout", "2s"]);

    env.wait_for(Duration::from_secs(5), || {
        let a = env.inspect_json("a");
        let b = env.inspect_json("b");
        a["status"]["state"] == Value::String("exited".to_string())
            && b["status"]["state"] == Value::String("exited".to_string())
    });

    env.run_ok(&["rm", "a", "--force"]);
    env.run_ok(&["rm", "b", "--force"]);
}

#[test]
fn start_accepts_multiple_refs() {
    let env = TestEnv::new();

    env.run_ok(&["run", "--name", "sa", "--", "/bin/sh", "-c", "sleep 30"]);
    env.run_ok(&["run", "--name", "sb", "--", "/bin/sh", "-c", "sleep 30"]);
    env.run_ok(&["stop", "sa", "sb", "--timeout", "2s"]);

    env.wait_for(Duration::from_secs(5), || {
        let a = env.inspect_json("sa");
        let b = env.inspect_json("sb");
        a["status"]["state"] == Value::String("exited".to_string())
            && b["status"]["state"] == Value::String("exited".to_string())
    });

    env.run_ok(&["start", "sa", "sb"]);

    env.wait_for(Duration::from_secs(5), || {
        let a = env.inspect_json("sa");
        let b = env.inspect_json("sb");
        a["status"]["state"] == Value::String("running".to_string())
            && b["status"]["state"] == Value::String("running".to_string())
            && a["desired"]["desired"] == Value::String("running".to_string())
            && b["desired"]["desired"] == Value::String("running".to_string())
    });

    env.run_ok(&["stop", "sa", "sb", "--timeout", "2s"]);
    env.run_ok(&["rm", "sa", "--force"]);
    env.run_ok(&["rm", "sb", "--force"]);
}

#[test]
fn id_is_base58_len12_and_one_char_prefix_resolves_when_unique() {
    let env = TestEnv::new();

    let out_a = env.run_ok(&["run", "--", "/bin/sh", "-c", "sleep 30"]);
    let id_a = parse_run_id(&out_a);
    let out_b = env.run_ok(&["run", "--", "/bin/sh", "-c", "sleep 30"]);
    let id_b = parse_run_id(&out_b);

    assert_eq!(id_a.len(), 12, "id must be 12 chars");
    assert!(
        id_a.chars()
            .all(|c| "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".contains(c)),
        "id should be base58: {id_a}"
    );
    assert_ne!(id_a, id_b, "generated ids should be distinct");

    let prefix = &id_a[..1];
    let inspect = env.run(&["inspect", prefix, "--json"]);
    assert_eq!(
        inspect.status.code(),
        Some(0),
        "1-char prefix should resolve uniquely\\nstdout={}\\nstderr={}",
        String::from_utf8_lossy(&inspect.stdout),
        String::from_utf8_lossy(&inspect.stderr)
    );
    let payload: Value = serde_json::from_slice(&inspect.stdout).unwrap();
    assert_eq!(payload["id"], Value::String(id_a.clone()));

    env.run_ok(&["stop", &id_a, "--timeout", "2s"]);
    env.run_ok(&["rm", &id_a, "--force"]);
    env.run_ok(&["stop", &id_b, "--timeout", "2s"]);
    env.run_ok(&["rm", &id_b, "--force"]);
}

#[test]
fn id_resolution_is_case_sensitive() {
    let env = TestEnv::new();

    let out = env.run_ok(&["run", "--", "/bin/sh", "-c", "sleep 30"]);
    let id = parse_run_id(&out);
    let flipped = match flip_first_alpha_case(&id) {
        Some(v) => v,
        None => {
            env.run_ok(&["stop", &id, "--timeout", "2s"]);
            env.run_ok(&["rm", &id, "--force"]);
            return;
        }
    };

    let inspect = env.run(&["inspect", &flipped, "--json"]);
    assert_eq!(
        inspect.status.code(),
        Some(3),
        "case-changed id should not resolve\\nstdout={}\\nstderr={}",
        String::from_utf8_lossy(&inspect.stdout),
        String::from_utf8_lossy(&inspect.stderr)
    );

    env.run_ok(&["stop", &id, "--timeout", "2s"]);
    env.run_ok(&["rm", &id, "--force"]);
}

#[test]
fn rm_accepts_multiple_refs() {
    let env = TestEnv::new();

    env.run_ok(&["run", "--name", "ra", "--", "/bin/sh", "-c", "sleep 30"]);
    env.run_ok(&["run", "--name", "rb", "--", "/bin/sh", "-c", "sleep 30"]);

    env.run_ok(&["stop", "ra", "rb", "--timeout", "2s"]);
    env.run_ok(&["rm", "ra", "rb", "--force"]);

    let a = env.run(&["inspect", "ra"]);
    let b = env.run(&["inspect", "rb"]);
    assert_eq!(a.status.code(), Some(3));
    assert_eq!(b.status.code(), Some(3));
}

#[test]
fn replace_graceful_sends_term_before_removal() {
    let env = TestEnv::new();
    let marker = env.data.join("term-graceful.txt");
    let script = format!(
        "trap 'echo term >> {} ; exit 0' TERM; while true; do sleep 1; done",
        marker.display()
    );

    env.run_ok(&["run", "--name", "svc", "--", "/bin/sh", "-c", &script]);

    env.wait_for(Duration::from_secs(3), || {
        let v = env.inspect_json("svc");
        v["status"]["state"] == Value::String("running".to_string())
    });

    env.run_ok(&[
        "run",
        "--name",
        "svc",
        "--replace",
        "--replace-timeout",
        "2s",
        "--",
        "/bin/true",
    ]);

    env.wait_for(Duration::from_secs(3), || marker.exists());
    let text = fs::read_to_string(&marker).unwrap_or_default();
    assert!(
        text.contains("term"),
        "graceful replace should deliver TERM, marker content: {text}"
    );

    env.run_ok(&["rm", "svc", "--force"]);
}

#[test]
fn replace_force_skips_graceful_term() {
    let env = TestEnv::new();
    let marker = env.data.join("term-force.txt");
    let script = format!(
        "trap 'echo term >> {} ; exit 0' TERM; while true; do sleep 1; done",
        marker.display()
    );

    env.run_ok(&["run", "--name", "svc", "--", "/bin/sh", "-c", &script]);

    env.wait_for(Duration::from_secs(3), || {
        let v = env.inspect_json("svc");
        v["status"]["state"] == Value::String("running".to_string())
    });

    env.run_ok(&[
        "run",
        "--name",
        "svc",
        "--replace",
        "--force",
        "--",
        "/bin/true",
    ]);

    thread::sleep(Duration::from_millis(600));
    assert!(
        !marker.exists(),
        "forced replace should not allow TERM trap marker"
    );

    env.run_ok(&["rm", "svc", "--force"]);
}

#[test]
fn tags_can_filter_ps_and_target_stop_rm() {
    let env = TestEnv::new();

    env.run_ok(&[
        "run", "--name", "ta", "--tag", "api", "--tag", "blue", "--", "/bin/sh", "-c", "sleep 30",
    ]);
    env.run_ok(&[
        "run", "--name", "tb", "--tag", "api", "--tag", "red", "--", "/bin/sh", "-c", "sleep 30",
    ]);
    env.run_ok(&[
        "run", "--name", "tc", "--tag", "worker", "--", "/bin/sh", "-c", "sleep 30",
    ]);

    let ps_api = env.run_ok(&["ps", "--json", "--tag", "api"]);
    let rows_api: Value = serde_json::from_str(&ps_api).unwrap();
    assert_eq!(rows_api.as_array().unwrap().len(), 2);
    for row in rows_api.as_array().unwrap() {
        let tags = row["tags"]
            .as_array()
            .expect("ps row should include tags array");
        assert!(
            tags.iter().any(|t| t == "api"),
            "filtered ps row should include api tag: {row}"
        );
    }

    env.run_ok(&["stop", "--tag", "api", "--tag", "blue", "--timeout", "2s"]);
    env.wait_for(Duration::from_secs(5), || {
        let ta = env.inspect_json("ta");
        ta["status"]["state"] == Value::String("exited".to_string())
    });

    env.run_ok(&["rm", "tag:api", "--force"]);
    assert_eq!(env.run(&["inspect", "ta"]).status.code(), Some(3));
    assert_eq!(env.run(&["inspect", "tb"]).status.code(), Some(3));

    let tc = env.inspect_json("tc");
    assert_eq!(
        tc["meta"]["tags"],
        Value::Array(vec![Value::String("worker".to_string())])
    );

    env.run_ok(&["stop", "tc", "--timeout", "2s"]);
    env.run_ok(&["rm", "tc", "--force"]);
}

#[test]
fn start_can_target_tags() {
    let env = TestEnv::new();

    env.run_ok(&[
        "run", "--name", "sta", "--tag", "api", "--tag", "blue", "--", "/bin/sh", "-c", "sleep 30",
    ]);
    env.run_ok(&[
        "run", "--name", "stb", "--tag", "api", "--tag", "red", "--", "/bin/sh", "-c", "sleep 30",
    ]);
    env.run_ok(&[
        "run", "--name", "stc", "--tag", "worker", "--", "/bin/sh", "-c", "sleep 30",
    ]);

    env.run_ok(&["stop", "--tag", "api", "--timeout", "2s"]);
    env.wait_for(Duration::from_secs(5), || {
        let a = env.inspect_json("sta");
        let b = env.inspect_json("stb");
        a["status"]["state"] == Value::String("exited".to_string())
            && b["status"]["state"] == Value::String("exited".to_string())
    });

    env.run_ok(&["start", "--tag", "api"]);

    env.wait_for(Duration::from_secs(5), || {
        let a = env.inspect_json("sta");
        let b = env.inspect_json("stb");
        a["status"]["state"] == Value::String("running".to_string())
            && b["status"]["state"] == Value::String("running".to_string())
    });

    env.run_ok(&["stop", "sta", "stb", "stc", "--timeout", "2s"]);
    env.run_ok(&["rm", "sta", "--force"]);
    env.run_ok(&["rm", "stb", "--force"]);
    env.run_ok(&["rm", "stc", "--force"]);
}

#[test]
fn logs_can_target_tags_with_interleaved_colored_output() {
    let env = TestEnv::new();

    env.run_ok(&[
        "run",
        "--name",
        "la",
        "--tag",
        "api",
        "--",
        "/bin/sh",
        "-c",
        "echo la-1; echo la-2",
    ]);
    env.run_ok(&[
        "run",
        "--name",
        "lb",
        "--tag",
        "api",
        "--",
        "/bin/sh",
        "-c",
        "echo lb-1; echo lb-2",
    ]);

    env.wait_for(Duration::from_secs(5), || {
        let a = env.inspect_json("la");
        let b = env.inspect_json("lb");
        a["status"]["state"] == Value::String("exited".to_string())
            && b["status"]["state"] == Value::String("exited".to_string())
    });

    let out = env.run(&["logs", "--tag", "api", "--tail", "1"]);
    assert!(
        out.status.success(),
        "logs by tag should succeed\\nstdout={}\\nstderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("\u{1b}[38;2;"),
        "expected ansi truecolor prefixes: {text}"
    );
    assert!(text.contains("[la]"), "expected la prefix: {text}");
    assert!(text.contains("[lb]"), "expected lb prefix: {text}");
    assert!(text.contains("la-2"), "expected la tail output: {text}");
    assert!(text.contains("lb-2"), "expected lb tail output: {text}");

    env.run_ok(&["rm", "la", "--force"]);
    env.run_ok(&["rm", "lb", "--force"]);
}

#[test]
fn logs_help_includes_follow_long_flag() {
    let env = TestEnv::new();
    let out = env.run(&["logs", "--help"]);
    assert!(
        out.status.success(),
        "logs --help should succeed\\nstdout={}\\nstderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8_lossy(&out.stdout);
    assert!(
        text.contains("--follow"),
        "expected --follow in help output: {text}"
    );
}
