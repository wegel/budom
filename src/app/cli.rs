use super::ops::*;
use super::supervisor::{NewJobSpec, ensure_supervisor, send_ipc, supervisor_main};
use super::*;

const ID_ALPHABET_BASE58: &[u8] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
const ID_LEN: usize = 12;
const MAX_ID_RETRIES: usize = 128;

fn random_base58_id(len: usize) -> String {
    let mut out = String::with_capacity(len);
    let mut rng = OsRng;
    let mut buf = [0u8; 32];

    while out.len() < len {
        rng.fill_bytes(&mut buf);
        for b in &buf {
            // Rejection sampling keeps character distribution uniform over 58 symbols.
            if *b < 232 {
                out.push(ID_ALPHABET_BASE58[(b % 58) as usize] as char);
                if out.len() == len {
                    break;
                }
            }
        }
    }

    out
}

fn reserve_job_id(paths: &Paths) -> Result<String> {
    for _ in 0..MAX_ID_RETRIES {
        let id = random_base58_id(ID_LEN);
        match fs::create_dir(paths.job_dir(&id)) {
            Ok(()) => return Ok(id),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e.into()),
        }
    }
    bail!(
        "failed to allocate unique job id after {} attempts",
        MAX_ID_RETRIES
    )
}

fn create_job(paths: &Paths, spec: NewJobSpec) -> Result<String> {
    let id = reserve_job_id(paths)?;
    let created_at = now_rfc3339();

    File::create(paths.lock_path(&id))?;

    let (uid, gid) = current_uid_gid();
    let meta = Meta {
        schema: 1,
        id: id.clone(),
        name: spec.name.clone(),
        tags: spec.tags.clone(),
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
        let mut status = load_status(paths, &id)?;
        if reconcile_status_liveness(paths, &id, &mut status)? {
            let _ = save_status(paths, &id, &status);
        }
        if all || should_show_default(&status.state) {
            rows.push(PsRow {
                id,
                name: meta.name,
                tags: meta.tags,
                desired: desired.desired,
                state: status.state,
                pid: status.pid,
                restart_count: status.restart_count,
            });
        }
    }
    Ok(rows)
}

fn desired_display(desired: &DesiredState) -> &'static str {
    match desired {
        DesiredState::Running => "Running",
        DesiredState::Stopped => "Stopped",
    }
}

fn status_display(state: &StatusState) -> &'static str {
    match state {
        StatusState::Starting => "Starting",
        StatusState::Running => "Running",
        StatusState::Stopping => "Stopping",
        StatusState::Exited => "Exited",
        StatusState::Backoff => "Backoff",
        StatusState::Error => "Error",
        StatusState::Stale => "Stale",
    }
}

fn format_ps_rows(rows: &[PsRow]) -> String {
    let mut data = Vec::with_capacity(rows.len());
    let mut w_id = "ID".len();
    let mut w_name = "NAME".len();
    let mut w_desired = "DESIRED".len();
    let mut w_state = "STATE".len();
    let mut w_pid = "PID".len();
    let mut w_restarts = "RESTARTS".len();
    let mut w_tags = "TAGS".len();

    for r in rows {
        let id = r.id[..8.min(r.id.len())].to_string();
        let name = r.name.clone().unwrap_or_else(|| "-".to_string());
        let tags = if r.tags.is_empty() {
            "-".to_string()
        } else {
            r.tags.join(",")
        };
        let desired = desired_display(&r.desired).to_string();
        let state = status_display(&r.state).to_string();
        let pid = r
            .pid
            .map(|p| p.to_string())
            .unwrap_or_else(|| "-".to_string());
        let restarts = r.restart_count.to_string();

        w_id = w_id.max(id.len());
        w_name = w_name.max(name.len());
        w_desired = w_desired.max(desired.len());
        w_state = w_state.max(state.len());
        w_pid = w_pid.max(pid.len());
        w_restarts = w_restarts.max(restarts.len());
        w_tags = w_tags.max(tags.len());

        data.push((id, name, desired, state, pid, restarts, tags));
    }

    let mut out = String::new();
    out.push_str(&format!(
        "{:<w_id$}  {:<w_name$}  {:<w_desired$}  {:<w_state$}  {:<w_pid$}  {:<w_restarts$}  {:<w_tags$}\n",
        "ID", "NAME", "DESIRED", "STATE", "PID", "RESTARTS", "TAGS"
    ));
    for (id, name, desired, state, pid, restarts, tags) in data {
        out.push_str(&format!(
            "{:<w_id$}  {:<w_name$}  {:<w_desired$}  {:<w_state$}  {:<w_pid$}  {:<w_restarts$}  {:<w_tags$}\n",
            id, name, desired, state, pid, restarts, tags
        ));
    }
    out
}

fn write_stdout_all(bytes: &[u8]) -> Result<bool> {
    let mut out = io::stdout().lock();
    match out.write_all(bytes) {
        Ok(()) => {}
        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => return Ok(true),
        Err(e) => return Err(e.into()),
    }
    match out.flush() {
        Ok(()) => Ok(false),
        Err(e) if e.kind() == io::ErrorKind::BrokenPipe => Ok(true),
        Err(e) => Err(e.into()),
    }
}

fn write_stdout_line(s: &str) -> Result<bool> {
    let mut line = String::with_capacity(s.len() + 1);
    line.push_str(s);
    line.push('\n');
    write_stdout_all(line.as_bytes())
}

fn print_ps(rows: &[PsRow]) -> Result<bool> {
    write_stdout_all(format_ps_rows(rows).as_bytes())
}

const CATPPUCCIN_MOCHA_COLORS: &[(u8, u8, u8)] = &[
    (245, 224, 220), // rosewater
    (242, 205, 205), // flamingo
    (245, 194, 231), // pink
    (203, 166, 247), // mauve
    (243, 139, 168), // red
    (235, 160, 172), // maroon
    (250, 179, 135), // peach
    (249, 226, 175), // yellow
    (166, 227, 161), // green
    (148, 226, 213), // teal
    (137, 220, 235), // sky
    (116, 199, 236), // sapphire
    (137, 180, 250), // blue
    (180, 190, 254), // lavender
];

#[derive(Clone)]
struct LogSource {
    label: String,
    path: PathBuf,
    color: (u8, u8, u8),
    offset: u64,
    partial: Vec<u8>,
}

fn color_for_id(id: &str) -> (u8, u8, u8) {
    let mut hash: u64 = 1469598103934665603; // fnv offset basis
    for b in id.as_bytes() {
        hash ^= u64::from(*b);
        hash = hash.wrapping_mul(1099511628211);
    }
    CATPPUCCIN_MOCHA_COLORS[(hash as usize) % CATPPUCCIN_MOCHA_COLORS.len()]
}

fn label_for_id(paths: &Paths, id: &str) -> String {
    if let Ok(meta) = load_meta(paths, id)
        && let Some(name) = meta.name
    {
        return name;
    }
    id[..8.min(id.len())].to_string()
}

fn write_colored_prefixed_line(
    label: &str,
    color: (u8, u8, u8),
    line: &[u8],
    include_newline: bool,
) -> Result<bool> {
    let prefix = format!(
        "\x1b[38;2;{};{};{}m[{}]\x1b[0m ",
        color.0, color.1, color.2, label
    );
    let mut out = Vec::with_capacity(prefix.len() + line.len() + 1);
    out.extend_from_slice(prefix.as_bytes());
    out.extend_from_slice(line);
    if include_newline {
        out.push(b'\n');
    }
    write_stdout_all(&out)
}

fn drain_complete_lines(buf: &mut Vec<u8>) -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    let mut start = 0usize;
    for (i, b) in buf.iter().enumerate() {
        if *b == b'\n' {
            out.push(buf[start..i].to_vec());
            start = i + 1;
        }
    }
    if start > 0 {
        buf.drain(0..start);
    }
    out
}

fn read_log_bytes(path: &Path, tail: Option<usize>) -> Result<Vec<u8>> {
    if let Some(n) = tail {
        tail_lines(path, n)
    } else {
        read_all(path)
    }
}

fn print_interleaved_snapshot(sources: &mut [LogSource], tail: Option<usize>) -> Result<bool> {
    let mut lines_by_source = Vec::with_capacity(sources.len());

    for s in sources.iter_mut() {
        let bytes = read_log_bytes(&s.path, tail).unwrap_or_default();
        s.offset = bytes.len() as u64;
        s.partial = bytes;
        let lines = drain_complete_lines(&mut s.partial);
        lines_by_source.push(lines);
    }

    let mut printed = true;
    while printed {
        printed = false;
        for (idx, lines) in lines_by_source.iter_mut().enumerate() {
            if lines.is_empty() {
                continue;
            }
            printed = true;
            let line = lines.remove(0);
            if write_colored_prefixed_line(&sources[idx].label, sources[idx].color, &line, true)? {
                return Ok(true);
            }
        }
    }

    for s in sources.iter_mut() {
        if !s.partial.is_empty() {
            let partial = std::mem::take(&mut s.partial);
            if write_colored_prefixed_line(&s.label, s.color, &partial, true)? {
                return Ok(true);
            }
        }
    }

    Ok(false)
}

fn follow_interleaved_sources(sources: &mut [LogSource]) -> Result<()> {
    loop {
        std::thread::sleep(Duration::from_millis(200));
        for s in sources.iter_mut() {
            let size = fs::metadata(&s.path).map(|m| m.len()).unwrap_or(0);
            if size < s.offset {
                s.offset = 0;
                s.partial.clear();
            }
            if size <= s.offset {
                continue;
            }
            let mut f = match File::open(&s.path) {
                Ok(v) => v,
                Err(_) => continue,
            };
            use std::io::Seek;
            use std::io::SeekFrom;
            f.seek(SeekFrom::Start(s.offset))?;
            let mut buf = Vec::new();
            f.read_to_end(&mut buf)?;
            s.offset = size;
            s.partial.extend_from_slice(&buf);

            for line in drain_complete_lines(&mut s.partial) {
                if write_colored_prefixed_line(&s.label, s.color, &line, true)? {
                    return Ok(());
                }
            }
        }
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

fn collect_log_sources(
    paths: &Paths,
    ids: &[String],
    stdout: bool,
    stderr: bool,
) -> Result<Vec<LogSource>> {
    let include_stdout = stdout || !stderr;
    let include_stderr = stderr;
    let mut out = Vec::new();

    for id in ids {
        let (stdout_path, stderr_path, merge) = get_logs_paths(paths, id)?;
        let base_label = label_for_id(paths, id);
        let color = color_for_id(id);

        if include_stdout {
            let label = if include_stderr && !merge {
                format!("{base_label}:stdout")
            } else {
                base_label.clone()
            };
            out.push(LogSource {
                label,
                path: stdout_path.clone(),
                color,
                offset: 0,
                partial: Vec::new(),
            });
        }

        if include_stderr {
            let path = if merge {
                stdout_path.clone()
            } else {
                stderr_path
            };
            let already_added = include_stdout && merge;
            if !already_added {
                let label = if merge {
                    base_label.clone()
                } else {
                    format!("{base_label}:stderr")
                };
                out.push(LogSource {
                    label,
                    path,
                    color,
                    offset: 0,
                    partial: Vec::new(),
                });
            }
        }
    }

    Ok(out)
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
    if write_stdout_all(&out)? {
        return Ok(());
    }
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
            if write_stdout_all(&buf)? {
                return Ok(());
            }
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

fn map_target_resolve_error(msg: &str) -> i32 {
    if msg.contains("invalid tag") || msg.contains("invalid tag selector") {
        EXIT_USAGE
    } else {
        EXIT_NOT_FOUND
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

pub(super) fn gc_impl(paths: &Paths) -> Result<usize> {
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

fn replace_existing_id(paths: &Paths, existing: &str, force: bool, timeout_ms: u64) -> Result<()> {
    if !paths.job_dir(existing).exists() {
        return Ok(());
    }

    if !force {
        if let Ok(resp) = send_ipc(
            paths,
            &IpcRequest::Stop {
                r#ref: existing.to_string(),
                signal: "TERM".to_string(),
                timeout_ms,
            },
        ) {
            if !resp.ok {
                bail!(
                    "{}",
                    resp.message
                        .unwrap_or_else(|| "replace stop failed".to_string())
                );
            }
        } else {
            stop_offline(paths, existing, Signal::SIGTERM, timeout_ms)?;
        }
    }

    if let Ok(resp) = send_ipc(
        paths,
        &IpcRequest::Rm {
            r#ref: existing.to_string(),
            force: true,
        },
    ) {
        if !resp.ok {
            bail!(
                "{}",
                resp.message
                    .unwrap_or_else(|| "replace rm failed".to_string())
            );
        }
        return Ok(());
    }

    rm_offline(paths, existing, true)
}

pub(super) fn run_cli(cli: Cli) -> i32 {
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
            force,
            replace_timeout,
            tags,
            cmd,
        } => {
            let replace_timeout_ms = match parse_timeout_ms(replace_timeout.as_deref()) {
                Ok(ms) => ms,
                Err(e) => {
                    eprintln!("{e:#}");
                    return EXIT_USAGE;
                }
            };

            if let Some(n) = &name {
                if !is_valid_name(n) {
                    eprintln!("invalid name; regex: [A-Za-z0-9][A-Za-z0-9_.-]{{0,63}}");
                    return EXIT_USAGE;
                }
                match list_named_job_ids(&paths, n) {
                    Ok(existing_ids) => {
                        if !existing_ids.is_empty() {
                            if !replace {
                                eprintln!("name conflict: {n}");
                                return EXIT_CONFLICT;
                            }
                            for existing in existing_ids {
                                if let Err(e) = replace_existing_id(
                                    &paths,
                                    &existing,
                                    force,
                                    replace_timeout_ms,
                                ) {
                                    eprintln!("{e:#}");
                                    return EXIT_OS;
                                }
                            }
                        }
                    }
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

            for t in &tags {
                if !is_valid_tag(t) {
                    eprintln!(
                        "invalid tag '{}'; regex: [A-Za-z0-9][A-Za-z0-9_.-]{{0,63}}",
                        t
                    );
                    return EXIT_USAGE;
                }
            }

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
                    tags,
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
                Ok(resp) if resp.ok => match write_stdout_line(&id) {
                    Ok(_) => EXIT_OK,
                    Err(e) => {
                        eprintln!("{e:#}");
                        EXIT_OS
                    }
                },
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
            tags,
        } => {
            for t in &tags {
                if !is_valid_tag(t) {
                    eprintln!(
                        "invalid tag '{}'; regex: [A-Za-z0-9][A-Za-z0-9_.-]{{0,63}}",
                        t
                    );
                    return EXIT_USAGE;
                }
            }

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
            let rows: Vec<PsRow> = if tags.is_empty() {
                rows
            } else {
                rows.into_iter()
                    .filter(|r| {
                        if let Ok(meta) = load_meta(&paths, &r.id) {
                            matches_all_tags(&meta.tags, &tags)
                        } else {
                            false
                        }
                    })
                    .collect()
            };

            if out_json {
                match serde_json::to_string_pretty(&rows) {
                    Ok(s) => {
                        if let Err(e) = write_stdout_line(&s) {
                            eprintln!("{e:#}");
                            return EXIT_OS;
                        }
                    }
                    Err(e) => {
                        eprintln!("{e:#}");
                        return EXIT_OS;
                    }
                }
            } else if let Err(e) = print_ps(&rows) {
                eprintln!("{e:#}");
                return EXIT_OS;
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
                    Ok(s) => {
                        if let Err(e) = write_stdout_line(&s) {
                            eprintln!("{e:#}");
                            return EXIT_OS;
                        }
                    }
                    Err(e) => {
                        eprintln!("{e:#}");
                        return EXIT_OS;
                    }
                }
            } else {
                let s = serde_json::to_string_pretty(&payload).unwrap_or_else(|_| "{}".to_string());
                if let Err(e) = write_stdout_line(&s) {
                    eprintln!("{e:#}");
                    return EXIT_OS;
                }
            }
            EXIT_OK
        }
        Commands::Logs {
            refs,
            tags,
            follow,
            stdout,
            stderr,
            tail,
        } => {
            let targets = match resolve_targets(&paths, &refs, &tags) {
                Ok(t) => t,
                Err(e) => {
                    let msg = e.to_string();
                    eprintln!("{msg}");
                    return map_target_resolve_error(&msg);
                }
            };
            if targets.is_empty() {
                eprintln!("no targets provided");
                return EXIT_USAGE;
            }

            let single_plain = tags.is_empty() && refs.len() == 1 && !refs[0].starts_with("tag:");
            if single_plain {
                let (stdout_path, stderr_path, merge) = match get_logs_paths(&paths, &targets[0]) {
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

                return EXIT_OK;
            }

            let mut sources = match collect_log_sources(&paths, &targets, stdout, stderr) {
                Ok(s) => s,
                Err(e) => {
                    eprintln!("{e:#}");
                    return EXIT_NOT_FOUND;
                }
            };
            if sources.is_empty() {
                eprintln!("no log streams selected");
                return EXIT_USAGE;
            }

            match print_interleaved_snapshot(&mut sources, tail) {
                Ok(_) => {}
                Err(e) => {
                    eprintln!("{e:#}");
                    return EXIT_OS;
                }
            }

            if follow && let Err(e) = follow_interleaved_sources(&mut sources) {
                eprintln!("{e:#}");
                return EXIT_OS;
            }

            EXIT_OK
        }
        Commands::Stop {
            refs,
            tags,
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

            let targets = match resolve_targets(&paths, &refs, &tags) {
                Ok(t) => t,
                Err(e) => {
                    let msg = e.to_string();
                    eprintln!("{msg}");
                    return map_target_resolve_error(&msg);
                }
            };
            if targets.is_empty() {
                eprintln!("no targets provided");
                return EXIT_USAGE;
            }

            let mut rc = EXIT_OK;
            for r in targets {
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
        Commands::Rm { refs, tags, force } => {
            let targets = match resolve_targets(&paths, &refs, &tags) {
                Ok(t) => t,
                Err(e) => {
                    let msg = e.to_string();
                    eprintln!("{msg}");
                    return map_target_resolve_error(&msg);
                }
            };
            if targets.is_empty() {
                eprintln!("no targets provided");
                return EXIT_USAGE;
            }

            let mut rc = EXIT_OK;
            for r in targets {
                match send_ipc(
                    &paths,
                    &IpcRequest::Rm {
                        r#ref: r.clone(),
                        force,
                    },
                ) {
                    Ok(resp) if resp.ok => {}
                    Ok(resp) => {
                        eprintln!(
                            "{}: {}",
                            r,
                            resp.message
                                .clone()
                                .unwrap_or_else(|| "rm failed".to_string())
                        );
                        if rc == EXIT_OK {
                            rc = map_ipc_error_to_exit(&resp);
                        }
                    }
                    Err(_) => match rm_offline(&paths, &r, force) {
                        Ok(()) => {}
                        Err(e) => {
                            let msg = e.to_string();
                            eprintln!("{r}: {msg}");
                            if rc == EXIT_OK {
                                rc = if msg.contains("--force") {
                                    EXIT_INVALID_STATE
                                } else {
                                    EXIT_NOT_FOUND
                                };
                            }
                        }
                    },
                }
            }
            rc
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn random_base58_id_has_expected_len_and_charset() {
        let id = random_base58_id(ID_LEN);
        assert_eq!(id.len(), ID_LEN);
        assert!(
            id.chars()
                .all(|c| "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz".contains(c))
        );
    }

    #[test]
    fn random_base58_id_is_not_constant() {
        let a = random_base58_id(ID_LEN);
        let b = random_base58_id(ID_LEN);
        assert_ne!(a, b);
    }

    #[test]
    fn ps_output_is_aligned_with_missing_names() {
        let rows = vec![
            PsRow {
                id: "01abcdef1234567890".to_string(),
                name: None,
                tags: vec!["api".to_string(), "blue".to_string()],
                desired: DesiredState::Running,
                state: StatusState::Running,
                pid: Some(12),
                restart_count: 0,
            },
            PsRow {
                id: "01b".to_string(),
                name: Some("longer-name".to_string()),
                tags: vec![],
                desired: DesiredState::Stopped,
                state: StatusState::Exited,
                pid: None,
                restart_count: 12,
            },
        ];

        let out = format_ps_rows(&rows);
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 3);

        let header = lines[0];
        let name_col = header.find("NAME").expect("NAME column");
        let tags_col = header.find("TAGS").expect("TAGS column");
        let desired_col = header.find("DESIRED").expect("DESIRED column");
        let state_col = header.find("STATE").expect("STATE column");
        let pid_col = header.find("PID").expect("PID column");
        let restarts_col = header.find("RESTARTS").expect("RESTARTS column");

        for line in &lines[1..] {
            assert_ne!(line.as_bytes()[name_col], b' ');
            assert_ne!(line.as_bytes()[tags_col], b' ');
            assert_ne!(line.as_bytes()[desired_col], b' ');
            assert_ne!(line.as_bytes()[state_col], b' ');
            assert_ne!(line.as_bytes()[pid_col], b' ');
            assert_ne!(line.as_bytes()[restarts_col], b' ');
        }
    }
}
