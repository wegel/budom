use anyhow::{Context, Result, anyhow, bail};
use chrono::{SecondsFormat, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use fs2::FileExt;
use nix::errno::Errno;
use nix::sys::signal::{Signal, killpg};
use nix::unistd::{Pid, setsid};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::os::unix::fs::symlink;
use std::os::unix::net::UnixStream;
use std::os::unix::process::ExitStatusExt;
use std::path::{Path, PathBuf};
use std::process::{self, Command as StdCommand, Stdio};
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream as TokioUnixStream};
use tokio::process::{Child, Command as TokioCommand};
use ulid::Ulid;

const EXIT_OK: i32 = 0;
const EXIT_USAGE: i32 = 2;
const EXIT_NOT_FOUND: i32 = 3;
const EXIT_CONFLICT: i32 = 4;
const EXIT_INVALID_STATE: i32 = 5;
const EXIT_OS: i32 = 6;

#[derive(Debug, Clone, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum RestartPolicy {
    No,
    Always,
    OnFailure,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum StatusState {
    Starting,
    Running,
    Stopping,
    Exited,
    Backoff,
    Error,
    Stale,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum DesiredState {
    Running,
    Stopped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BackoffConfig {
    kind: String,
    initial_ms: u64,
    max_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RestartConfig {
    policy: RestartPolicy,
    max_retries: u32,
    backoff: BackoffConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LoggingConfig {
    merge_streams: bool,
    max_size_mb: u64,
    max_files: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct UserInfo {
    uid: u32,
    gid: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Meta {
    schema: u32,
    id: String,
    name: Option<String>,
    created_at: String,
    cwd: String,
    argv: Vec<String>,
    env: HashMap<String, String>,
    inherit_env: bool,
    stdin: String,
    user: UserInfo,
    logging: LoggingConfig,
    restart: RestartConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DesiredFile {
    schema: u32,
    desired: DesiredState,
    updated_at: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ExitStatusFile {
    at: String,
    code: Option<i32>,
    signal: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct StatusFile {
    schema: u32,
    state: StatusState,
    pid: Option<i32>,
    started_at: Option<String>,
    last_seen_at: Option<String>,
    restart_count: u32,
    last_exit: Option<ExitStatusFile>,
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct ConfigFile {
    #[serde(default)]
    logging: ConfigLogging,
    #[serde(default)]
    restart: ConfigRestart,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConfigLogging {
    max_size_mb: Option<u64>,
    max_files: Option<u32>,
    merge_streams: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ConfigRestart {
    policy: Option<RestartPolicy>,
    max_retries: Option<u32>,
    backoff_initial_ms: Option<u64>,
    backoff_max_ms: Option<u64>,
}

impl Default for ConfigLogging {
    fn default() -> Self {
        Self {
            max_size_mb: Some(50),
            max_files: Some(5),
            merge_streams: Some(false),
        }
    }
}

impl Default for ConfigRestart {
    fn default() -> Self {
        Self {
            policy: Some(RestartPolicy::No),
            max_retries: Some(0),
            backoff_initial_ms: Some(200),
            backoff_max_ms: Some(30000),
        }
    }
}

#[derive(Debug, Clone)]
struct EffectiveDefaults {
    logging: LoggingConfig,
    restart: RestartConfig,
}

#[derive(Debug, Clone)]
struct Paths {
    state_dir: PathBuf,
    data_dir: PathBuf,
    conf_dir: PathBuf,
}

impl Paths {
    fn detect() -> Result<Self> {
        let home = std::env::var_os("HOME").ok_or_else(|| anyhow!("HOME is not set"))?;
        let home = PathBuf::from(home);

        let state_home = std::env::var_os("XDG_STATE_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".local/state"));
        let data_home = std::env::var_os("XDG_DATA_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".local/share"));
        let conf_home = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| home.join(".config"));

        Ok(Self {
            state_dir: state_home.join("budom"),
            data_dir: data_home.join("budom"),
            conf_dir: conf_home.join("budom"),
        })
    }

    fn ensure_dirs(&self) -> Result<()> {
        fs::create_dir_all(self.run_dir())?;
        fs::create_dir_all(self.jobs_dir())?;
        fs::create_dir_all(self.names_dir())?;
        fs::create_dir_all(&self.conf_dir)?;
        Ok(())
    }

    fn run_dir(&self) -> PathBuf {
        self.state_dir.join("run")
    }

    fn socket_path(&self) -> PathBuf {
        self.run_dir().join("budom.sock")
    }

    fn supervisor_pid_path(&self) -> PathBuf {
        self.run_dir().join("supervisor.pid")
    }

    fn supervisor_lock_path(&self) -> PathBuf {
        self.run_dir().join("supervisor.lock")
    }

    fn supervisor_log_path(&self) -> PathBuf {
        self.run_dir().join("supervisor.log")
    }

    fn jobs_dir(&self) -> PathBuf {
        self.data_dir.join("jobs")
    }

    fn names_dir(&self) -> PathBuf {
        self.data_dir.join("names")
    }

    fn job_dir(&self, id: &str) -> PathBuf {
        self.jobs_dir().join(id)
    }

    fn meta_path(&self, id: &str) -> PathBuf {
        self.job_dir(id).join("meta.json")
    }

    fn desired_path(&self, id: &str) -> PathBuf {
        self.job_dir(id).join("desired.json")
    }

    fn status_path(&self, id: &str) -> PathBuf {
        self.job_dir(id).join("status.json")
    }

    fn pid_path(&self, id: &str) -> PathBuf {
        self.job_dir(id).join("pid")
    }

    fn exit_path(&self, id: &str) -> PathBuf {
        self.job_dir(id).join("exit.json")
    }

    fn lock_path(&self, id: &str) -> PathBuf {
        self.job_dir(id).join("lock")
    }

    fn stdout_path(&self, id: &str) -> PathBuf {
        self.job_dir(id).join("stdout.log")
    }

    fn stderr_path(&self, id: &str) -> PathBuf {
        self.job_dir(id).join("stderr.log")
    }

    fn config_path(&self) -> PathBuf {
        self.conf_dir.join("config.toml")
    }

    fn name_path(&self, name: &str) -> PathBuf {
        self.names_dir().join(name)
    }
}

#[derive(Parser, Debug)]
#[command(name = "budom")]
#[command(version = "0.1.0")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Run {
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        cwd: Option<PathBuf>,
        #[arg(long = "env")]
        env_vars: Vec<String>,
        #[arg(long)]
        clean_env: bool,
        #[arg(long)]
        merge_streams: bool,
        #[arg(long, value_enum)]
        restart: Option<RestartPolicy>,
        #[arg(long)]
        max_retries: Option<u32>,
        #[arg(long)]
        backoff: Option<String>,
        #[arg(long)]
        log_max_size_mb: Option<u64>,
        #[arg(long)]
        log_max_files: Option<u32>,
        #[arg(long)]
        replace: bool,
        #[arg(required = true, trailing_var_arg = true)]
        cmd: Vec<String>,
    },
    Ps {
        #[arg(long)]
        all: bool,
        #[arg(long)]
        json: bool,
    },
    Inspect {
        r#ref: String,
        #[arg(long)]
        json: bool,
    },
    Logs {
        r#ref: String,
        #[arg(short = 'f')]
        follow: bool,
        #[arg(long)]
        stdout: bool,
        #[arg(long)]
        stderr: bool,
        #[arg(long)]
        tail: Option<usize>,
    },
    Stop {
        #[arg(required = true, num_args = 1..)]
        refs: Vec<String>,
        #[arg(long)]
        signal: Option<String>,
        #[arg(long)]
        timeout: Option<String>,
    },
    Rm {
        r#ref: String,
        #[arg(long)]
        force: bool,
    },
    Gc,
    #[command(name = "__supervise", hide = true)]
    Supervise,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
enum IpcRequest {
    Hello {
        proto: u32,
    },
    Run {
        id: String,
    },
    Ps {
        all: bool,
    },
    Inspect {
        r#ref: String,
    },
    Stop {
        r#ref: String,
        signal: String,
        timeout_ms: u64,
    },
    Rm {
        r#ref: String,
        force: bool,
    },
    LogsPath {
        r#ref: String,
    },
    Gc,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct IpcResponse {
    ok: bool,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    data: Value,
}

impl IpcResponse {
    fn ok(data: Value) -> Self {
        Self {
            ok: true,
            code: None,
            message: None,
            data,
        }
    }

    fn err(code: &str, message: impl Into<String>) -> Self {
        Self {
            ok: false,
            code: Some(code.to_string()),
            message: Some(message.into()),
            data: Value::Null,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PsRow {
    id: String,
    name: Option<String>,
    desired: DesiredState,
    state: StatusState,
    pid: Option<i32>,
    restart_count: u32,
}

struct RunningProc {
    child: Child,
    pid: i32,
}

struct ManagedJob {
    meta: Meta,
    desired: DesiredFile,
    status: StatusFile,
    runtime: Option<RunningProc>,
    backoff_until: Option<Instant>,
}

struct Supervisor {
    paths: Paths,
    jobs: HashMap<String, ManagedJob>,
    _lock: File,
}

fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

fn current_uid_gid() -> (u32, u32) {
    unsafe { (nix::libc::getuid(), nix::libc::getgid()) }
}

fn parse_backoff(input: &str) -> Result<(u64, u64)> {
    let (a, b) = input
        .split_once(',')
        .ok_or_else(|| anyhow!("backoff must be initial_ms,max_ms"))?;
    let initial = a
        .parse::<u64>()
        .with_context(|| format!("invalid backoff initial_ms: {a}"))?;
    let max = b
        .parse::<u64>()
        .with_context(|| format!("invalid backoff max_ms: {b}"))?;
    Ok((initial, max))
}

fn parse_timeout_ms(input: Option<&str>) -> Result<u64> {
    let Some(s) = input else {
        return Ok(10_000);
    };
    if let Some(ms) = s.strip_suffix("ms") {
        return Ok(ms.parse::<u64>()?);
    }
    if let Some(sec) = s.strip_suffix('s') {
        return Ok(sec.parse::<u64>()? * 1000);
    }
    Ok(s.parse::<u64>()?)
}

fn parse_env_vars(pairs: &[String]) -> Result<HashMap<String, String>> {
    let mut out = HashMap::new();
    for p in pairs {
        let (k, v) = p
            .split_once('=')
            .ok_or_else(|| anyhow!("invalid --env '{}', expected KEY=VAL", p))?;
        if k.is_empty() {
            bail!("invalid --env '{}', key is empty", p);
        }
        out.insert(k.to_string(), v.to_string());
    }
    Ok(out)
}

fn is_valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.is_empty() || bytes.len() > 64 {
        return false;
    }
    let first = bytes[0] as char;
    if !first.is_ascii_alphanumeric() {
        return false;
    }
    bytes.iter().all(|b| {
        let c = *b as char;
        c.is_ascii_alphanumeric() || c == '_' || c == '.' || c == '-'
    })
}

fn load_config(paths: &Paths) -> Result<EffectiveDefaults> {
    let defaults = ConfigFile::default();
    let cfg = if paths.config_path().exists() {
        let raw = fs::read_to_string(paths.config_path())?;
        let parsed = toml::from_str::<ConfigFile>(&raw)?;
        merge_config(defaults, parsed)
    } else {
        defaults
    };

    Ok(EffectiveDefaults {
        logging: LoggingConfig {
            merge_streams: cfg.logging.merge_streams.unwrap_or(false),
            max_size_mb: cfg.logging.max_size_mb.unwrap_or(50),
            max_files: cfg.logging.max_files.unwrap_or(5),
        },
        restart: RestartConfig {
            policy: cfg.restart.policy.unwrap_or(RestartPolicy::No),
            max_retries: cfg.restart.max_retries.unwrap_or(0),
            backoff: BackoffConfig {
                kind: "exponential".to_string(),
                initial_ms: cfg.restart.backoff_initial_ms.unwrap_or(200),
                max_ms: cfg.restart.backoff_max_ms.unwrap_or(30000),
            },
        },
    })
}

fn merge_config(base: ConfigFile, other: ConfigFile) -> ConfigFile {
    ConfigFile {
        logging: ConfigLogging {
            max_size_mb: other.logging.max_size_mb.or(base.logging.max_size_mb),
            max_files: other.logging.max_files.or(base.logging.max_files),
            merge_streams: other.logging.merge_streams.or(base.logging.merge_streams),
        },
        restart: ConfigRestart {
            policy: other.restart.policy.or(base.restart.policy),
            max_retries: other.restart.max_retries.or(base.restart.max_retries),
            backoff_initial_ms: other
                .restart
                .backoff_initial_ms
                .or(base.restart.backoff_initial_ms),
            backoff_max_ms: other.restart.backoff_max_ms.or(base.restart.backoff_max_ms),
        },
    }
}

fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("missing parent for {}", path.display()))?;
    fs::create_dir_all(parent)?;
    let tmp = parent.join(format!(
        ".{}.tmp.{}.{}",
        path.file_name().and_then(|x| x.to_str()).unwrap_or("budom"),
        process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(tmp, path)?;
    Ok(())
}

fn write_json_atomic<T: Serialize>(path: &Path, val: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(val)?;
    write_atomic(path, &bytes)
}

fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let raw = fs::read(path)?;
    Ok(serde_json::from_slice(&raw)?)
}

fn read_text(path: &Path) -> Result<String> {
    Ok(fs::read_to_string(path)?)
}

fn job_lock(paths: &Paths, id: &str) -> Result<File> {
    let lock_path = paths.lock_path(id);
    let f = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(lock_path)?;
    f.lock_exclusive()?;
    Ok(f)
}

fn resolve_name_to_id(paths: &Paths, name: &str) -> Result<Option<String>> {
    let p = paths.name_path(name);
    if !p.exists() {
        return Ok(None);
    }
    let metadata = fs::symlink_metadata(&p)?;
    if metadata.file_type().is_symlink() {
        let target = fs::read_link(&p)?;
        if let Some(s) = target.file_name().and_then(|x| x.to_str()) {
            return Ok(Some(s.to_string()));
        }
        return Ok(Some(target.to_string_lossy().to_string()));
    }
    let s = fs::read_to_string(&p)?;
    Ok(Some(s.trim().to_string()))
}

fn resolve_meta_name_to_id(paths: &Paths, name: &str) -> Result<Option<String>> {
    for id in list_job_ids(paths)? {
        if let Ok(meta) = load_meta(paths, &id)
            && meta.name.as_deref() == Some(name)
        {
            return Ok(Some(id));
        }
    }
    Ok(None)
}

fn link_name(paths: &Paths, name: &str, id: &str) -> Result<()> {
    let p = paths.name_path(name);
    if p.exists() {
        fs::remove_file(&p)?;
    }
    let target = PathBuf::from(id);
    if symlink(&target, &p).is_err() {
        write_atomic(&p, id.as_bytes())?;
    }
    Ok(())
}

fn unlink_name(paths: &Paths, name: &str) -> Result<()> {
    let p = paths.name_path(name);
    if p.exists() {
        fs::remove_file(p)?;
    }
    Ok(())
}

fn list_job_ids(paths: &Paths) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    if !paths.jobs_dir().exists() {
        return Ok(ids);
    }
    for ent in fs::read_dir(paths.jobs_dir())? {
        let ent = ent?;
        if ent.file_type()?.is_dir()
            && let Some(id) = ent.file_name().to_str()
        {
            ids.push(id.to_string());
        }
    }
    ids.sort();
    Ok(ids)
}

fn resolve_ref(paths: &Paths, r: &str) -> Result<String> {
    if let Some(id) = resolve_meta_name_to_id(paths, r)? {
        return Ok(id);
    }

    if let Some(id) = resolve_name_to_id(paths, r)?
        && paths.job_dir(&id).exists()
    {
        return Ok(id);
    }

    let ids = list_job_ids(paths)?;
    let r_lower = r.to_ascii_lowercase();

    for id in &ids {
        if id.eq_ignore_ascii_case(&r_lower) || id.eq_ignore_ascii_case(r) {
            return Ok(id.clone());
        }
    }

    let matches: Vec<String> = ids
        .into_iter()
        .filter(|id| id.to_ascii_lowercase().starts_with(&r_lower))
        .collect();
    if matches.len() == 1 {
        return Ok(matches[0].clone());
    }
    if matches.is_empty() {
        bail!("not found: {r}");
    }
    bail!("ambiguous short id '{}': {} matches", r, matches.len())
}

fn load_meta(paths: &Paths, id: &str) -> Result<Meta> {
    read_json(&paths.meta_path(id))
}

fn load_desired(paths: &Paths, id: &str) -> Result<DesiredFile> {
    if paths.desired_path(id).exists() {
        read_json(&paths.desired_path(id))
    } else {
        Ok(DesiredFile {
            schema: 1,
            desired: DesiredState::Stopped,
            updated_at: now_rfc3339(),
        })
    }
}

fn load_status(paths: &Paths, id: &str) -> Result<StatusFile> {
    if paths.status_path(id).exists() {
        read_json(&paths.status_path(id))
    } else {
        Ok(StatusFile {
            schema: 1,
            state: StatusState::Exited,
            pid: None,
            started_at: None,
            last_seen_at: None,
            restart_count: 0,
            last_exit: None,
            error: None,
        })
    }
}

fn save_desired(paths: &Paths, id: &str, desired: &DesiredFile) -> Result<()> {
    write_json_atomic(&paths.desired_path(id), desired)
}

fn save_status(paths: &Paths, id: &str, status: &StatusFile) -> Result<()> {
    write_json_atomic(&paths.status_path(id), status)
}

fn save_pid(paths: &Paths, id: &str, pid: i32) -> Result<()> {
    write_atomic(&paths.pid_path(id), pid.to_string().as_bytes())
}

fn remove_pid(paths: &Paths, id: &str) -> Result<()> {
    let p = paths.pid_path(id);
    if p.exists() {
        fs::remove_file(p)?;
    }
    Ok(())
}

fn parse_pid(paths: &Paths, id: &str) -> Result<Option<i32>> {
    let p = paths.pid_path(id);
    if !p.exists() {
        return Ok(None);
    }
    let s = read_text(&p)?;
    Ok(Some(s.trim().parse::<i32>()?))
}

fn should_show_default(status: &StatusState) -> bool {
    !matches!(status, StatusState::Exited | StatusState::Stale)
}

fn stream_base_name(stdout: bool) -> &'static str {
    if stdout { "stdout.log" } else { "stderr.log" }
}

fn rotate_if_needed(paths: &Paths, id: &str, stdout: bool, logging: &LoggingConfig) -> Result<()> {
    let _lk = job_lock(paths, id)?;
    let active = if stdout {
        paths.stdout_path(id)
    } else {
        paths.stderr_path(id)
    };

    if !active.exists() {
        return Ok(());
    }

    let max_size = logging.max_size_mb.saturating_mul(1024 * 1024);
    if max_size == 0 || logging.max_files == 0 {
        return Ok(());
    }

    let len = fs::metadata(&active)?.len();
    if len <= max_size {
        return Ok(());
    }

    let name = stream_base_name(stdout);
    for n in (2..=logging.max_files).rev() {
        let from = paths.job_dir(id).join(format!("{}.{}", name, n - 1));
        let to = paths.job_dir(id).join(format!("{}.{}", name, n));
        if from.exists() {
            let _ = fs::remove_file(&to);
            fs::rename(from, to)?;
        }
    }

    let first = paths.job_dir(id).join(format!("{}.1", name));
    let _ = fs::remove_file(&first);
    fs::rename(&active, &first)?;
    File::create(active)?;
    Ok(())
}

fn append_bytes(
    paths: &Paths,
    id: &str,
    stdout: bool,
    bytes: &[u8],
    logging: &LoggingConfig,
) -> Result<()> {
    let p = if stdout {
        paths.stdout_path(id)
    } else {
        paths.stderr_path(id)
    };
    let mut f = OpenOptions::new().create(true).append(true).open(p)?;
    f.write_all(bytes)?;
    f.flush()?;
    rotate_if_needed(paths, id, stdout, logging)?;
    Ok(())
}

async fn pump_stream<R: AsyncRead + Unpin>(
    paths: Paths,
    id: String,
    mut reader: R,
    stdout: bool,
    logging: LoggingConfig,
) {
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                let chunk = buf[..n].to_vec();
                let p2 = paths.clone();
                let id2 = id.clone();
                let lg2 = logging.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    append_bytes(&p2, &id2, stdout, &chunk, &lg2)
                })
                .await;
            }
            Err(_) => break,
        }
    }
}

fn parse_signal(name: &str) -> Result<Signal> {
    match name.to_ascii_uppercase().as_str() {
        "TERM" | "SIGTERM" => Ok(Signal::SIGTERM),
        "INT" | "SIGINT" => Ok(Signal::SIGINT),
        "KILL" | "SIGKILL" => Ok(Signal::SIGKILL),
        other => bail!("unsupported signal: {other}"),
    }
}

fn to_signal_name(sig: Option<i32>) -> Option<String> {
    sig.and_then(|n| Signal::try_from(n).ok())
        .map(|s| format!("{s:?}").trim_start_matches("SIG").to_string())
}

fn kill_pgroup(pid: i32, signal: Signal) -> Result<()> {
    let pgid = Pid::from_raw(pid);
    match killpg(pgid, signal) {
        Ok(()) => Ok(()),
        Err(Errno::ESRCH) => Ok(()),
        Err(e) => Err(anyhow!(e)),
    }
}

impl Supervisor {
    fn load_job_from_disk(&mut self, id: &str, mark_stale_on_pid: bool) -> Result<()> {
        if self.jobs.contains_key(id) {
            return Ok(());
        }
        if !self.paths.job_dir(id).exists() {
            bail!("unknown job id: {id}");
        }

        let meta = load_meta(&self.paths, id)?;
        let desired = load_desired(&self.paths, id)?;
        let mut status = load_status(&self.paths, id)?;
        let pid_on_disk = parse_pid(&self.paths, id)?;

        if mark_stale_on_pid && (pid_on_disk.is_some() || status.pid.is_some()) {
            status.state = StatusState::Stale;
            status.pid = pid_on_disk.or(status.pid);
            status.error = Some("stale from previous supervisor".to_string());
            save_status(&self.paths, id, &status)?;
        }

        self.jobs.insert(
            id.to_string(),
            ManagedJob {
                meta,
                desired,
                status,
                runtime: None,
                backoff_until: None,
            },
        );
        Ok(())
    }

    async fn load(paths: Paths, lock_file: File) -> Result<Self> {
        let mut sup = Self {
            paths,
            jobs: HashMap::new(),
            _lock: lock_file,
        };

        for id in list_job_ids(&sup.paths)? {
            if sup.load_job_from_disk(&id, true).is_err() {
                continue;
            }
        }

        Ok(sup)
    }

    async fn start_if_needed(&mut self, id: &str) -> Result<()> {
        let Some(job) = self.jobs.get_mut(id) else {
            bail!("unknown job id: {id}");
        };
        if job.runtime.is_some() {
            return Ok(());
        }
        if job.desired.desired != DesiredState::Running {
            return Ok(());
        }
        if let Some(until) = job.backoff_until
            && Instant::now() < until
        {
            return Ok(());
        }

        job.status.state = StatusState::Starting;
        job.status.error = None;
        save_status(&self.paths, id, &job.status)?;

        if job.meta.argv.is_empty() {
            job.status.state = StatusState::Error;
            job.status.error = Some("empty argv".to_string());
            save_status(&self.paths, id, &job.status)?;
            return Ok(());
        }

        let mut cmd = TokioCommand::new(&job.meta.argv[0]);
        if job.meta.argv.len() > 1 {
            cmd.args(&job.meta.argv[1..]);
        }
        cmd.current_dir(&job.meta.cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());

        if !job.meta.inherit_env {
            cmd.env_clear();
        }
        cmd.envs(job.meta.env.clone());

        unsafe {
            cmd.pre_exec(|| {
                if nix::libc::setpgid(0, 0) != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }

        let mut child = match cmd.spawn() {
            Ok(c) => c,
            Err(e) => {
                job.status.state = StatusState::Error;
                job.status.error = Some(format!("spawn failed: {e}"));
                save_status(&self.paths, id, &job.status)?;
                return Ok(());
            }
        };

        let pid = child.id().map(|x| x as i32);
        let Some(pid) = pid else {
            job.status.state = StatusState::Error;
            job.status.error = Some("spawned child has no pid".to_string());
            save_status(&self.paths, id, &job.status)?;
            return Ok(());
        };

        let out = child.stdout.take();
        let err = child.stderr.take();

        let logging = job.meta.logging.clone();
        if let Some(stdout_pipe) = out {
            tokio::spawn(pump_stream(
                self.paths.clone(),
                id.to_string(),
                stdout_pipe,
                true,
                logging.clone(),
            ));
        }
        if let Some(stderr_pipe) = err {
            tokio::spawn(pump_stream(
                self.paths.clone(),
                id.to_string(),
                stderr_pipe,
                job.meta.logging.merge_streams,
                logging,
            ));
        }

        save_pid(&self.paths, id, pid)?;
        job.status.state = StatusState::Running;
        job.status.pid = Some(pid);
        job.status.started_at = Some(now_rfc3339());
        job.status.last_seen_at = Some(now_rfc3339());
        job.status.error = None;
        save_status(&self.paths, id, &job.status)?;

        job.runtime = Some(RunningProc { child, pid });
        job.backoff_until = None;
        Ok(())
    }

    fn record_exit_common(
        &mut self,
        id: &str,
        code: Option<i32>,
        signal: Option<String>,
        now: String,
    ) -> Result<()> {
        let Some(job) = self.jobs.get_mut(id) else {
            return Ok(());
        };

        let exit = ExitStatusFile {
            at: now,
            code,
            signal,
        };
        write_json_atomic(&self.paths.exit_path(id), &exit)?;
        remove_pid(&self.paths, id)?;

        job.status.pid = None;
        job.status.last_seen_at = Some(now_rfc3339());
        job.status.last_exit = Some(exit.clone());

        let should_restart = if job.desired.desired != DesiredState::Running {
            false
        } else {
            match job.meta.restart.policy {
                RestartPolicy::No => false,
                RestartPolicy::Always => true,
                RestartPolicy::OnFailure => exit.code.unwrap_or(1) != 0 || exit.signal.is_some(),
            }
        };

        if should_restart {
            if job.meta.restart.max_retries != 0
                && job.status.restart_count >= job.meta.restart.max_retries
            {
                job.status.state = StatusState::Exited;
                job.status.error = None;
                job.runtime = None;
                save_status(&self.paths, id, &job.status)?;
                return Ok(());
            }

            let k = job.status.restart_count;
            let raw = job
                .meta
                .restart
                .backoff
                .initial_ms
                .saturating_mul(2u64.saturating_pow(k));
            let delay = raw.min(job.meta.restart.backoff.max_ms);
            job.status.restart_count = job.status.restart_count.saturating_add(1);
            job.status.state = StatusState::Backoff;
            job.status.error = None;
            job.backoff_until = Some(Instant::now() + Duration::from_millis(delay));
            job.runtime = None;
            save_status(&self.paths, id, &job.status)?;
            return Ok(());
        }

        job.status.state = StatusState::Exited;
        job.status.error = None;
        job.runtime = None;
        save_status(&self.paths, id, &job.status)?;
        Ok(())
    }

    async fn tick(&mut self) -> Result<()> {
        let ids: Vec<String> = self.jobs.keys().cloned().collect();
        for id in ids {
            let mut exited: Option<(Option<i32>, Option<String>)> = None;
            let mut maybe_restart_from_backoff = false;
            if let Some(job) = self.jobs.get_mut(&id) {
                if let Some(runtime) = job.runtime.as_mut() {
                    match runtime.child.try_wait() {
                        Ok(Some(st)) => {
                            exited = Some((st.code(), to_signal_name(st.signal())));
                        }
                        Ok(None) => {
                            job.status.last_seen_at = Some(now_rfc3339());
                            let _ = save_status(&self.paths, &id, &job.status);
                        }
                        Err(e) => {
                            job.status.state = StatusState::Error;
                            job.status.error = Some(format!("wait failed: {e}"));
                            job.runtime = None;
                            let _ = save_status(&self.paths, &id, &job.status);
                        }
                    }
                } else if job.desired.desired == DesiredState::Running
                    && matches!(job.status.state, StatusState::Backoff)
                    && let Some(until) = job.backoff_until
                    && Instant::now() >= until
                {
                    maybe_restart_from_backoff = true;
                }
            }

            if let Some((code, signal)) = exited {
                self.record_exit_common(&id, code, signal, now_rfc3339())?;
            }

            if maybe_restart_from_backoff {
                self.start_if_needed(&id).await?;
            }
        }

        Ok(())
    }

    fn desired_running_count(&self) -> usize {
        self.jobs
            .values()
            .filter(|j| j.desired.desired == DesiredState::Running)
            .count()
    }

    async fn handle_stop(&mut self, id: &str, signal: Signal, timeout: u64) -> Result<()> {
        let Some(job) = self.jobs.get_mut(id) else {
            bail!("not found");
        };

        job.desired.desired = DesiredState::Stopped;
        job.desired.updated_at = now_rfc3339();
        save_desired(&self.paths, id, &job.desired)?;

        if let Some(runtime) = job.runtime.as_mut() {
            job.status.state = StatusState::Stopping;
            save_status(&self.paths, id, &job.status)?;

            kill_pgroup(runtime.pid, signal)?;
            let deadline = Instant::now() + Duration::from_millis(timeout);
            let mut exited = None;
            while Instant::now() < deadline {
                if let Some(st) = runtime.child.try_wait()? {
                    exited = Some((st.code(), to_signal_name(st.signal())));
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }

            if exited.is_none() && signal != Signal::SIGKILL {
                kill_pgroup(runtime.pid, Signal::SIGKILL)?;
                let deadline2 = Instant::now() + Duration::from_secs(2);
                while Instant::now() < deadline2 {
                    if let Some(st) = runtime.child.try_wait()? {
                        exited = Some((st.code(), to_signal_name(st.signal())));
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }

            if let Some((code, sig)) = exited {
                self.record_exit_common(id, code, sig, now_rfc3339())?;
            } else {
                job.status.state = StatusState::Exited;
                job.status.pid = None;
                remove_pid(&self.paths, id)?;
                save_status(&self.paths, id, &job.status)?;
                job.runtime = None;
            }
        } else {
            job.status.state = StatusState::Exited;
            job.status.pid = None;
            save_status(&self.paths, id, &job.status)?;
        }

        Ok(())
    }

    async fn handle_rm(&mut self, id: &str, force: bool) -> Result<()> {
        let (running, name) = {
            let Some(job) = self.jobs.get(id) else {
                bail!("not found");
            };
            let running = job.runtime.is_some()
                || matches!(
                    job.status.state,
                    StatusState::Running
                        | StatusState::Starting
                        | StatusState::Stopping
                        | StatusState::Backoff
                );
            (running, job.meta.name.clone())
        };

        if running && !force {
            bail!("job is running; use --force");
        }

        if running {
            let _ = self.handle_stop(id, Signal::SIGKILL, 500).await;
        }

        if force && let Some(pid) = parse_pid(&self.paths, id)? {
            let _ = kill_pgroup(pid, Signal::SIGKILL);
        }

        if let Some(name) = &name {
            let _ = unlink_name(&self.paths, name);
        }

        let job_dir = self.paths.job_dir(id);
        if job_dir.exists() {
            fs::remove_dir_all(job_dir)?;
        }
        self.jobs.remove(id);
        Ok(())
    }

    async fn process_request(&mut self, req: IpcRequest) -> IpcResponse {
        match req {
            IpcRequest::Hello { proto } => {
                if proto != 1 {
                    return IpcResponse::err("usage", "unsupported proto");
                }
                IpcResponse::ok(json!({"proto":1,"version":"budom 0.1.0","pid":process::id()}))
            }
            IpcRequest::Run { id } => match self.start_if_needed(&id).await {
                Ok(()) => {
                    let Some(job) = self.jobs.get(&id) else {
                        return IpcResponse::err("not_found", "job missing");
                    };
                    IpcResponse::ok(json!({"id":id,"name":job.meta.name,"pid":job.status.pid}))
                }
                Err(e) if e.to_string().contains("unknown job id") => {
                    if let Err(load_err) = self.load_job_from_disk(&id, false) {
                        return IpcResponse::err("not_found", load_err.to_string());
                    }
                    match self.start_if_needed(&id).await {
                        Ok(()) => {
                            let Some(job) = self.jobs.get(&id) else {
                                return IpcResponse::err("not_found", "job missing");
                            };
                            IpcResponse::ok(
                                json!({"id":id,"name":job.meta.name,"pid":job.status.pid}),
                            )
                        }
                        Err(inner) => IpcResponse::err("os", inner.to_string()),
                    }
                }
                Err(e) => IpcResponse::err("os", e.to_string()),
            },
            IpcRequest::Ps { all } => {
                let mut rows = Vec::new();
                for (id, job) in &self.jobs {
                    if all || should_show_default(&job.status.state) {
                        rows.push(PsRow {
                            id: id.clone(),
                            name: job.meta.name.clone(),
                            desired: job.desired.desired.clone(),
                            state: job.status.state.clone(),
                            pid: job.status.pid,
                            restart_count: job.status.restart_count,
                        });
                    }
                }
                IpcResponse::ok(json!({"rows": rows}))
            }
            IpcRequest::Inspect { r#ref } => match resolve_ref(&self.paths, &r#ref) {
                Ok(id) => {
                    let Some(job) = self.jobs.get(&id) else {
                        return IpcResponse::err("not_found", "job missing");
                    };
                    IpcResponse::ok(
                        json!({"id":id,"meta":job.meta,"desired":job.desired,"status":job.status}),
                    )
                }
                Err(_) => IpcResponse::err("not_found", format!("not found: {}", r#ref)),
            },
            IpcRequest::Stop {
                r#ref,
                signal,
                timeout_ms,
            } => {
                let signal = match parse_signal(&signal) {
                    Ok(s) => s,
                    Err(e) => return IpcResponse::err("usage", e.to_string()),
                };
                let id = match resolve_ref(&self.paths, &r#ref) {
                    Ok(id) => id,
                    Err(_) => {
                        return IpcResponse::err("not_found", format!("not found: {}", r#ref));
                    }
                };
                match self.handle_stop(&id, signal, timeout_ms).await {
                    Ok(()) => IpcResponse::ok(json!({"id":id})),
                    Err(e) => IpcResponse::err("os", e.to_string()),
                }
            }
            IpcRequest::Rm { r#ref, force } => {
                let id = match resolve_ref(&self.paths, &r#ref) {
                    Ok(id) => id,
                    Err(_) => {
                        return IpcResponse::err("not_found", format!("not found: {}", r#ref));
                    }
                };
                match self.handle_rm(&id, force).await {
                    Ok(()) => IpcResponse::ok(json!({"id":id})),
                    Err(e) => {
                        if e.to_string().contains("--force") {
                            IpcResponse::err("invalid_state", e.to_string())
                        } else {
                            IpcResponse::err("os", e.to_string())
                        }
                    }
                }
            }
            IpcRequest::LogsPath { r#ref } => {
                let id = match resolve_ref(&self.paths, &r#ref) {
                    Ok(id) => id,
                    Err(_) => {
                        return IpcResponse::err("not_found", format!("not found: {}", r#ref));
                    }
                };
                match load_meta(&self.paths, &id) {
                    Ok(meta) => IpcResponse::ok(json!({
                        "id": id,
                        "stdout": self.paths.stdout_path(&id),
                        "stderr": self.paths.stderr_path(&id),
                        "merge_streams": meta.logging.merge_streams,
                    })),
                    Err(e) => IpcResponse::err("os", e.to_string()),
                }
            }
            IpcRequest::Gc => match gc_impl(&self.paths) {
                Ok(removed) => IpcResponse::ok(json!({"removed":removed})),
                Err(e) => IpcResponse::err("os", e.to_string()),
            },
        }
    }
}

async fn read_frame_async(stream: &mut TokioUnixStream) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;
    Ok(body)
}

async fn write_frame_async(stream: &mut TokioUnixStream, body: &[u8]) -> Result<()> {
    let len = body.len() as u32;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(body).await?;
    Ok(())
}

fn read_frame(stream: &mut UnixStream) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body)?;
    Ok(body)
}

fn write_frame(stream: &mut UnixStream, body: &[u8]) -> Result<()> {
    let len = body.len() as u32;
    stream.write_all(&len.to_le_bytes())?;
    stream.write_all(body)?;
    Ok(())
}

async fn supervisor_main(paths: Paths) -> Result<()> {
    paths.ensure_dirs()?;
    let _ = setsid();

    let lock_file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(paths.supervisor_lock_path())?;

    if lock_file.try_lock_exclusive().is_err() {
        return Ok(());
    }

    write_atomic(
        &paths.supervisor_pid_path(),
        process::id().to_string().as_bytes(),
    )?;

    if paths.socket_path().exists() {
        let _ = fs::remove_file(paths.socket_path());
    }

    let listener = UnixListener::bind(paths.socket_path())?;
    let mut sup = Supervisor::load(paths.clone(), lock_file).await?;

    let ids: Vec<String> = sup.jobs.keys().cloned().collect();
    for id in ids {
        if let Some(job) = sup.jobs.get(&id)
            && job.desired.desired == DesiredState::Running
        {
            let _ = sup.start_if_needed(&id).await;
        }
    }

    let grace_until = Instant::now() + Duration::from_secs(2);
    let mut ticker = tokio::time::interval(Duration::from_millis(100));

    loop {
        tokio::select! {
            _ = ticker.tick() => {
                let _ = sup.tick().await;
            }
            accept = listener.accept() => {
                if let Ok((mut stream, _)) = accept {
                    let req = match read_frame_async(&mut stream).await {
                        Ok(bytes) => bytes,
                        Err(_) => continue,
                    };
                    let req = match serde_json::from_slice::<IpcRequest>(&req) {
                        Ok(req) => req,
                        Err(e) => {
                            let resp = IpcResponse::err("usage", e.to_string());
                            let body =
                                serde_json::to_vec(&resp).unwrap_or_else(|_| b"{\"ok\":false}".to_vec());
                            let _ = write_frame_async(&mut stream, &body).await;
                            continue;
                        }
                    };
                    let resp = sup.process_request(req).await;
                    let body = serde_json::to_vec(&resp)?;
                    let _ = write_frame_async(&mut stream, &body).await;
                }
            }
        }

        if Instant::now() > grace_until && sup.desired_running_count() == 0 {
            break;
        }
    }

    let _ = fs::remove_file(paths.socket_path());
    let _ = fs::remove_file(paths.supervisor_pid_path());
    Ok(())
}

fn start_supervisor_detached(paths: &Paths) -> Result<()> {
    paths.ensure_dirs()?;
    let exe = std::env::current_exe()?;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(paths.supervisor_log_path())?;
    let log2 = log.try_clone()?;

    StdCommand::new(exe)
        .arg("__supervise")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log2))
        .spawn()?;
    Ok(())
}

fn connect_ipc(paths: &Paths) -> Result<UnixStream> {
    let sock = paths.socket_path();
    let stream = UnixStream::connect(sock)?;
    Ok(stream)
}

fn send_ipc(paths: &Paths, req: &IpcRequest) -> Result<IpcResponse> {
    let mut stream = connect_ipc(paths)?;
    let body = serde_json::to_vec(req)?;
    write_frame(&mut stream, &body)?;
    let resp = read_frame(&mut stream)?;
    Ok(serde_json::from_slice(&resp)?)
}

fn ensure_supervisor(paths: &Paths) -> Result<()> {
    if connect_ipc(paths).is_ok() {
        return Ok(());
    }

    start_supervisor_detached(paths)?;
    let start = Instant::now();
    let mut delay = Duration::from_millis(30);

    while start.elapsed() < Duration::from_secs(2) {
        if connect_ipc(paths).is_ok() {
            return Ok(());
        }
        std::thread::sleep(delay);
        delay = (delay * 2).min(Duration::from_millis(250));
    }

    bail!("supervisor did not start within 2s")
}

struct NewJobSpec {
    name: Option<String>,
    cwd: PathBuf,
    env_map: HashMap<String, String>,
    inherit_env: bool,
    cmd: Vec<String>,
    logging: LoggingConfig,
    restart: RestartConfig,
}

fn create_job(paths: &Paths, spec: NewJobSpec) -> Result<String> {
    let id = Ulid::new().to_string().to_ascii_lowercase();
    let created_at = now_rfc3339();

    fs::create_dir_all(paths.job_dir(&id))?;
    File::create(paths.lock_path(&id))?;

    let (uid, gid) = current_uid_gid();
    let meta = Meta {
        schema: 1,
        id: id.clone(),
        name: spec.name.clone(),
        created_at: created_at.clone(),
        cwd: spec.cwd.to_string_lossy().to_string(),
        argv: spec.cmd,
        env: spec.env_map,
        inherit_env: spec.inherit_env,
        stdin: "null".to_string(),
        user: UserInfo { uid, gid },
        logging: spec.logging,
        restart: spec.restart,
    };

    let desired = DesiredFile {
        schema: 1,
        desired: DesiredState::Running,
        updated_at: created_at,
    };

    let status = StatusFile {
        schema: 1,
        state: StatusState::Starting,
        pid: None,
        started_at: None,
        last_seen_at: None,
        restart_count: 0,
        last_exit: None,
        error: None,
    };

    write_json_atomic(&paths.meta_path(&id), &meta)?;
    write_json_atomic(&paths.desired_path(&id), &desired)?;
    write_json_atomic(&paths.status_path(&id), &status)?;

    if let Some(name) = spec.name {
        link_name(paths, &name, &id)?;
    }

    Ok(id)
}

fn load_all_rows(paths: &Paths, all: bool) -> Result<Vec<PsRow>> {
    let mut rows = Vec::new();
    for id in list_job_ids(paths)? {
        let meta = load_meta(paths, &id)?;
        let desired = load_desired(paths, &id)?;
        let status = load_status(paths, &id)?;
        if all || should_show_default(&status.state) {
            rows.push(PsRow {
                id,
                name: meta.name,
                desired: desired.desired,
                state: status.state,
                pid: status.pid,
                restart_count: status.restart_count,
            });
        }
    }
    Ok(rows)
}

fn print_ps(rows: &[PsRow]) {
    println!("ID\tNAME\tDESIRED\tSTATE\tPID\tRESTARTS");
    for r in rows {
        println!(
            "{}\t{}\t{:?}\t{:?}\t{}\t{}",
            &r.id[..8.min(r.id.len())],
            r.name.clone().unwrap_or_else(|| "-".to_string()),
            r.desired,
            r.state,
            r.pid
                .map(|p| p.to_string())
                .unwrap_or_else(|| "-".to_string()),
            r.restart_count
        );
    }
}

fn load_inspect(paths: &Paths, r: &str) -> Result<Value> {
    let id = resolve_ref(paths, r)?;
    let meta = load_meta(paths, &id)?;
    let desired = load_desired(paths, &id)?;
    let status = load_status(paths, &id)?;
    Ok(json!({"id":id,"meta":meta,"desired":desired,"status":status}))
}

fn get_logs_paths(paths: &Paths, r: &str) -> Result<(PathBuf, PathBuf, bool)> {
    if let Ok(resp) = send_ipc(
        paths,
        &IpcRequest::LogsPath {
            r#ref: r.to_string(),
        },
    ) && resp.ok
    {
        let stdout = PathBuf::from(
            resp.data
                .get("stdout")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("missing stdout path"))?,
        );
        let stderr = PathBuf::from(
            resp.data
                .get("stderr")
                .and_then(Value::as_str)
                .ok_or_else(|| anyhow!("missing stderr path"))?,
        );
        let merge = resp
            .data
            .get("merge_streams")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        return Ok((stdout, stderr, merge));
    }

    let id = resolve_ref(paths, r)?;
    let meta = load_meta(paths, &id)?;
    Ok((
        paths.stdout_path(&id),
        paths.stderr_path(&id),
        meta.logging.merge_streams,
    ))
}

fn read_all(path: &Path) -> Result<Vec<u8>> {
    if !path.exists() {
        return Ok(Vec::new());
    }
    Ok(fs::read(path)?)
}

fn tail_lines(path: &Path, n: usize) -> Result<Vec<u8>> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let bytes = read_all(path)?;
    let mut count = 0usize;
    let mut idx = bytes.len();
    while idx > 0 {
        idx -= 1;
        if bytes[idx] == b'\n' {
            count += 1;
            if count > n {
                idx += 1;
                break;
            }
        }
    }
    Ok(bytes[idx..].to_vec())
}

fn print_file(path: &Path, tail: Option<usize>) -> Result<()> {
    let out = if let Some(n) = tail {
        tail_lines(path, n)?
    } else {
        read_all(path)?
    };
    io::stdout().write_all(&out)?;
    io::stdout().flush()?;
    Ok(())
}

fn follow_file(path: &Path) -> Result<()> {
    let mut offset = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    loop {
        std::thread::sleep(Duration::from_millis(200));
        let size = fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        if size < offset {
            offset = 0;
        }
        if size > offset {
            let mut f = File::open(path)?;
            use std::io::Seek;
            use std::io::SeekFrom;
            f.seek(SeekFrom::Start(offset))?;
            let mut buf = Vec::new();
            f.read_to_end(&mut buf)?;
            io::stdout().write_all(&buf)?;
            io::stdout().flush()?;
            offset = size;
        }
    }
}

fn map_ipc_error_to_exit(resp: &IpcResponse) -> i32 {
    match resp.code.as_deref() {
        Some("usage") => EXIT_USAGE,
        Some("not_found") => EXIT_NOT_FOUND,
        Some("conflict") => EXIT_CONFLICT,
        Some("invalid_state") => EXIT_INVALID_STATE,
        _ => EXIT_OS,
    }
}

fn rm_offline(paths: &Paths, r: &str, force: bool) -> Result<()> {
    let id = resolve_ref(paths, r)?;
    let meta = load_meta(paths, &id)?;
    let status = load_status(paths, &id)?;

    if !force
        && matches!(
            status.state,
            StatusState::Running
                | StatusState::Starting
                | StatusState::Stopping
                | StatusState::Backoff
        )
    {
        bail!("job is running; use --force");
    }

    if force && let Some(pid) = parse_pid(paths, &id)? {
        let _ = kill_pgroup(pid, Signal::SIGKILL);
    }

    if let Some(name) = meta.name {
        let _ = unlink_name(paths, &name);
    }

    let dir = paths.job_dir(&id);
    if dir.exists() {
        fs::remove_dir_all(dir)?;
    }
    Ok(())
}

fn stop_offline(paths: &Paths, r: &str, signal: Signal, timeout_ms: u64) -> Result<()> {
    let id = resolve_ref(paths, r)?;
    let mut desired = load_desired(paths, &id)?;
    desired.desired = DesiredState::Stopped;
    desired.updated_at = now_rfc3339();
    save_desired(paths, &id, &desired)?;

    if let Some(pid) = parse_pid(paths, &id)? {
        let _ = kill_pgroup(pid, signal);
        std::thread::sleep(Duration::from_millis(timeout_ms.min(2000)));
        if signal != Signal::SIGKILL {
            let _ = kill_pgroup(pid, Signal::SIGKILL);
        }
    }

    let mut status = load_status(paths, &id)?;
    status.state = StatusState::Exited;
    status.pid = None;
    save_status(paths, &id, &status)?;
    let _ = remove_pid(paths, &id);
    Ok(())
}

fn gc_impl(paths: &Paths) -> Result<usize> {
    let mut removed = 0usize;
    if !paths.names_dir().exists() {
        return Ok(0);
    }
    for ent in fs::read_dir(paths.names_dir())? {
        let ent = ent?;
        let p = ent.path();
        if !p.is_file() && !p.is_symlink() {
            continue;
        }
        let name = ent.file_name().to_string_lossy().to_string();
        let Some(id) = resolve_name_to_id(paths, &name)? else {
            continue;
        };
        if !paths.job_dir(&id).exists() {
            fs::remove_file(p)?;
            removed += 1;
        }
    }
    Ok(removed)
}

fn maybe_replace_name(paths: &Paths, name: &str) -> Result<()> {
    if let Some(existing) = resolve_name_to_id(paths, name)?
        && paths.job_dir(&existing).exists()
    {
        if let Ok(resp) = send_ipc(
            paths,
            &IpcRequest::Rm {
                r#ref: existing.clone(),
                force: true,
            },
        ) {
            if !resp.ok {
                bail!(resp.message.unwrap_or_else(|| "replace failed".to_string()));
            }
            return Ok(());
        }
        rm_offline(paths, &existing, true)?;
    }
    Ok(())
}

fn run_cli(cli: Cli) -> i32 {
    let paths = match Paths::detect() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("{e:#}");
            return EXIT_OS;
        }
    };

    if let Err(e) = paths.ensure_dirs() {
        eprintln!("{e:#}");
        return EXIT_OS;
    }

    match cli.command {
        Commands::Supervise => {
            let rt = match tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
            {
                Ok(rt) => rt,
                Err(e) => {
                    eprintln!("{e:#}");
                    return EXIT_OS;
                }
            };
            match rt.block_on(supervisor_main(paths)) {
                Ok(()) => EXIT_OK,
                Err(e) => {
                    eprintln!("{e:#}");
                    EXIT_OS
                }
            }
        }
        Commands::Run {
            name,
            cwd,
            env_vars,
            clean_env,
            merge_streams,
            restart,
            max_retries,
            backoff,
            log_max_size_mb,
            log_max_files,
            replace,
            cmd,
        } => {
            if let Some(n) = &name {
                if !is_valid_name(n) {
                    eprintln!("invalid name; regex: [A-Za-z0-9][A-Za-z0-9_.-]{{0,63}}");
                    return EXIT_USAGE;
                }
                match resolve_name_to_id(&paths, n) {
                    Ok(Some(existing)) => {
                        if paths.job_dir(&existing).exists() {
                            if !replace {
                                eprintln!("name conflict: {n}");
                                return EXIT_CONFLICT;
                            }
                            if let Err(e) = maybe_replace_name(&paths, n) {
                                eprintln!("{e:#}");
                                return EXIT_OS;
                            }
                        }
                    }
                    Ok(None) => {}
                    Err(e) => {
                        eprintln!("{e:#}");
                        return EXIT_OS;
                    }
                }
            }

            let defaults = match load_config(&paths) {
                Ok(d) => d,
                Err(e) => {
                    eprintln!("{e:#}");
                    return EXIT_OS;
                }
            };

            let env_map = match parse_env_vars(&env_vars) {
                Ok(v) => v,
                Err(e) => {
                    eprintln!("{e:#}");
                    return EXIT_USAGE;
                }
            };

            let (initial_ms, max_ms) = if let Some(ref b) = backoff {
                match parse_backoff(b) {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("{e:#}");
                        return EXIT_USAGE;
                    }
                }
            } else {
                (
                    defaults.restart.backoff.initial_ms,
                    defaults.restart.backoff.max_ms,
                )
            };

            let cwd = cwd
                .unwrap_or_else(|| std::env::current_dir().unwrap_or_else(|_| PathBuf::from(".")));
            let cwd = match fs::canonicalize(cwd) {
                Ok(p) => p,
                Err(e) => {
                    eprintln!("invalid --cwd: {e}");
                    return EXIT_USAGE;
                }
            };

            let logging = LoggingConfig {
                merge_streams: if merge_streams {
                    true
                } else {
                    defaults.logging.merge_streams
                },
                max_size_mb: log_max_size_mb.unwrap_or(defaults.logging.max_size_mb),
                max_files: log_max_files.unwrap_or(defaults.logging.max_files),
            };

            let restart_cfg = RestartConfig {
                policy: restart.unwrap_or(defaults.restart.policy),
                max_retries: max_retries.unwrap_or(defaults.restart.max_retries),
                backoff: BackoffConfig {
                    kind: "exponential".to_string(),
                    initial_ms,
                    max_ms,
                },
            };

            let id = match create_job(
                &paths,
                NewJobSpec {
                    name,
                    cwd,
                    env_map,
                    inherit_env: !clean_env,
                    cmd,
                    logging,
                    restart: restart_cfg,
                },
            ) {
                Ok(id) => id,
                Err(e) => {
                    eprintln!("{e:#}");
                    return EXIT_OS;
                }
            };

            if let Err(e) = ensure_supervisor(&paths) {
                eprintln!("{e:#}");
                return EXIT_OS;
            }

            match send_ipc(&paths, &IpcRequest::Run { id: id.clone() }) {
                Ok(resp) if resp.ok => {
                    println!("{id}");
                    EXIT_OK
                }
                Ok(resp) => {
                    eprintln!(
                        "{}",
                        resp.message
                            .clone()
                            .unwrap_or_else(|| "run failed".to_string())
                    );
                    map_ipc_error_to_exit(&resp)
                }
                Err(e) => {
                    eprintln!("{e:#}");
                    EXIT_OS
                }
            }
        }
        Commands::Ps {
            all,
            json: out_json,
        } => {
            let rows = match send_ipc(&paths, &IpcRequest::Ps { all }) {
                Ok(resp) if resp.ok => serde_json::from_value::<Vec<PsRow>>(
                    resp.data
                        .get("rows")
                        .cloned()
                        .unwrap_or_else(|| Value::Array(vec![])),
                )
                .unwrap_or_default(),
                _ => match load_all_rows(&paths, all) {
                    Ok(rows) => rows,
                    Err(e) => {
                        eprintln!("{e:#}");
                        return EXIT_OS;
                    }
                },
            };

            if out_json {
                match serde_json::to_string_pretty(&rows) {
                    Ok(s) => println!("{s}"),
                    Err(e) => {
                        eprintln!("{e:#}");
                        return EXIT_OS;
                    }
                }
            } else {
                print_ps(&rows);
            }
            EXIT_OK
        }
        Commands::Inspect {
            r#ref,
            json: out_json,
        } => {
            let payload = match send_ipc(
                &paths,
                &IpcRequest::Inspect {
                    r#ref: r#ref.clone(),
                },
            ) {
                Ok(resp) if resp.ok => resp.data,
                Ok(resp) => {
                    eprintln!(
                        "{}",
                        resp.message
                            .clone()
                            .unwrap_or_else(|| "inspect failed".to_string())
                    );
                    return map_ipc_error_to_exit(&resp);
                }
                Err(_) => match load_inspect(&paths, &r#ref) {
                    Ok(v) => v,
                    Err(e) => {
                        eprintln!("{e:#}");
                        return EXIT_NOT_FOUND;
                    }
                },
            };

            if out_json {
                match serde_json::to_string_pretty(&payload) {
                    Ok(s) => println!("{s}"),
                    Err(e) => {
                        eprintln!("{e:#}");
                        return EXIT_OS;
                    }
                }
            } else {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".to_string())
                );
            }
            EXIT_OK
        }
        Commands::Logs {
            r#ref,
            follow,
            stdout,
            stderr,
            tail,
        } => {
            let (stdout_path, stderr_path, merge) = match get_logs_paths(&paths, &r#ref) {
                Ok(x) => x,
                Err(e) => {
                    eprintln!("{e:#}");
                    return EXIT_NOT_FOUND;
                }
            };

            let mut selected = Vec::new();
            if stdout {
                selected.push(stdout_path.clone());
            }
            if stderr {
                selected.push(if merge {
                    stdout_path.clone()
                } else {
                    stderr_path.clone()
                });
            }
            if !stdout && !stderr {
                selected.push(stdout_path.clone());
            }

            for p in &selected {
                if let Err(e) = print_file(p, tail) {
                    eprintln!("{e:#}");
                    return EXIT_OS;
                }
            }

            if follow {
                if selected.len() > 1 {
                    eprintln!("-f with multiple streams is not supported in v1");
                    return EXIT_USAGE;
                }
                if let Err(e) = follow_file(&selected[0]) {
                    eprintln!("{e:#}");
                    return EXIT_OS;
                }
            }

            EXIT_OK
        }
        Commands::Stop {
            refs,
            signal,
            timeout,
        } => {
            let signal_name = signal.unwrap_or_else(|| "TERM".to_string());
            let timeout_ms = match parse_timeout_ms(timeout.as_deref()) {
                Ok(ms) => ms,
                Err(e) => {
                    eprintln!("{e:#}");
                    return EXIT_USAGE;
                }
            };

            let sig = match parse_signal(&signal_name) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("{e:#}");
                    return EXIT_USAGE;
                }
            };

            let mut rc = EXIT_OK;
            for r in refs {
                match send_ipc(
                    &paths,
                    &IpcRequest::Stop {
                        r#ref: r.clone(),
                        signal: signal_name.clone(),
                        timeout_ms,
                    },
                ) {
                    Ok(resp) if resp.ok => {}
                    Ok(resp) => {
                        eprintln!(
                            "{}: {}",
                            r,
                            resp.message
                                .clone()
                                .unwrap_or_else(|| "stop failed".to_string())
                        );
                        if rc == EXIT_OK {
                            rc = map_ipc_error_to_exit(&resp);
                        }
                    }
                    Err(_) => match stop_offline(&paths, &r, sig, timeout_ms) {
                        Ok(()) => {}
                        Err(e) => {
                            eprintln!("{}: {e:#}", r);
                            if rc == EXIT_OK {
                                rc = EXIT_NOT_FOUND;
                            }
                        }
                    },
                }
            }

            rc
        }
        Commands::Rm { r#ref, force } => match send_ipc(
            &paths,
            &IpcRequest::Rm {
                r#ref: r#ref.clone(),
                force,
            },
        ) {
            Ok(resp) if resp.ok => EXIT_OK,
            Ok(resp) => {
                eprintln!(
                    "{}",
                    resp.message
                        .clone()
                        .unwrap_or_else(|| "rm failed".to_string())
                );
                map_ipc_error_to_exit(&resp)
            }
            Err(_) => match rm_offline(&paths, &r#ref, force) {
                Ok(()) => EXIT_OK,
                Err(e) => {
                    let msg = e.to_string();
                    eprintln!("{msg}");
                    if msg.contains("--force") {
                        EXIT_INVALID_STATE
                    } else {
                        EXIT_NOT_FOUND
                    }
                }
            },
        },
        Commands::Gc => match send_ipc(&paths, &IpcRequest::Gc) {
            Ok(resp) if resp.ok => EXIT_OK,
            Ok(resp) => {
                eprintln!(
                    "{}",
                    resp.message
                        .clone()
                        .unwrap_or_else(|| "gc failed".to_string())
                );
                map_ipc_error_to_exit(&resp)
            }
            Err(_) => match gc_impl(&paths) {
                Ok(_) => EXIT_OK,
                Err(e) => {
                    eprintln!("{e:#}");
                    EXIT_OS
                }
            },
        },
    }
}

fn main() {
    let cli = Cli::parse();
    process::exit(run_cli(cli));
}
