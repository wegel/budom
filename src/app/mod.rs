use anyhow::{Context, Result, anyhow, bail};
use chrono::{SecondsFormat, Utc};
use clap::{Parser, Subcommand, ValueEnum};
use fs2::FileExt;
use nix::errno::Errno;
use nix::sys::signal::{Signal, killpg};
use nix::unistd::{Pid, setsid};
use rand::RngCore;
use rand::rngs::OsRng;
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
    #[serde(default)]
    tags: Vec<String>,
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
#[command(
    name = "budom",
    about = "Per-user daemon manager for background processes",
    long_about = None
)]
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
        #[arg(long, requires = "replace")]
        force: bool,
        #[arg(long, requires = "replace")]
        replace_timeout: Option<String>,
        #[arg(long = "tag")]
        tags: Vec<String>,
        #[arg(required = true, trailing_var_arg = true)]
        cmd: Vec<String>,
    },
    Ps {
        #[arg(long)]
        all: bool,
        #[arg(long)]
        json: bool,
        #[arg(long = "tag")]
        tags: Vec<String>,
    },
    Inspect {
        r#ref: String,
        #[arg(long)]
        json: bool,
    },
    Logs {
        #[arg(value_name = "REF", num_args = 0..)]
        refs: Vec<String>,
        #[arg(long = "tag")]
        tags: Vec<String>,
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
        #[arg(value_name = "REF", num_args = 0..)]
        refs: Vec<String>,
        #[arg(long = "tag")]
        tags: Vec<String>,
        #[arg(long)]
        signal: Option<String>,
        #[arg(long)]
        timeout: Option<String>,
    },
    Rm {
        #[arg(value_name = "REF", num_args = 0..)]
        refs: Vec<String>,
        #[arg(long = "tag")]
        tags: Vec<String>,
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

mod cli;
mod ops;
mod supervisor;

pub fn entrypoint() {
    let cli = Cli::parse();
    process::exit(self::cli::run_cli(cli));
}
