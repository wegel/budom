use super::ops::*;
use super::supervisor::{NewJobSpec, ensure_supervisor, send_ipc, supervisor_main};
use super::*;

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

    for r in rows {
        let id = r.id[..8.min(r.id.len())].to_string();
        let name = r.name.clone().unwrap_or_else(|| "-".to_string());
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

        data.push((id, name, desired, state, pid, restarts));
    }

    let mut out = String::new();
    out.push_str(&format!(
        "{:<w_id$}  {:<w_name$}  {:<w_desired$}  {:<w_state$}  {:<w_pid$}  {:<w_restarts$}\n",
        "ID", "NAME", "DESIRED", "STATE", "PID", "RESTARTS"
    ));
    for (id, name, desired, state, pid, restarts) in data {
        out.push_str(&format!(
            "{:<w_id$}  {:<w_name$}  {:<w_desired$}  {:<w_state$}  {:<w_pid$}  {:<w_restarts$}\n",
            id, name, desired, state, pid, restarts
        ));
    }
    out
}

fn print_ps(rows: &[PsRow]) {
    print!("{}", format_ps_rows(rows));
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
        Commands::Rm { refs, force } => {
            let mut rc = EXIT_OK;
            for r in refs {
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
    fn ps_output_is_aligned_with_missing_names() {
        let rows = vec![
            PsRow {
                id: "01abcdef1234567890".to_string(),
                name: None,
                desired: DesiredState::Running,
                state: StatusState::Running,
                pid: Some(12),
                restart_count: 0,
            },
            PsRow {
                id: "01b".to_string(),
                name: Some("longer-name".to_string()),
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
        let desired_col = header.find("DESIRED").expect("DESIRED column");
        let state_col = header.find("STATE").expect("STATE column");
        let pid_col = header.find("PID").expect("PID column");
        let restarts_col = header.find("RESTARTS").expect("RESTARTS column");

        for line in &lines[1..] {
            assert_ne!(line.as_bytes()[name_col], b' ');
            assert_ne!(line.as_bytes()[desired_col], b' ');
            assert_ne!(line.as_bytes()[state_col], b' ');
            assert_ne!(line.as_bytes()[pid_col], b' ');
            assert_ne!(line.as_bytes()[restarts_col], b' ');
        }
    }
}
