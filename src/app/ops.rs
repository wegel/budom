use super::*;

pub(super) fn now_rfc3339() -> String {
    Utc::now().to_rfc3339_opts(SecondsFormat::Secs, true)
}

pub(super) fn current_uid_gid() -> (u32, u32) {
    unsafe { (nix::libc::getuid(), nix::libc::getgid()) }
}

pub(super) fn parse_backoff(input: &str) -> Result<(u64, u64)> {
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

pub(super) fn parse_timeout_ms(input: Option<&str>) -> Result<u64> {
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

pub(super) fn parse_env_vars(pairs: &[String]) -> Result<HashMap<String, String>> {
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

pub(super) fn is_valid_name(name: &str) -> bool {
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

pub(super) fn is_valid_tag(tag: &str) -> bool {
    is_valid_name(tag)
}

pub(super) fn load_config(paths: &Paths) -> Result<EffectiveDefaults> {
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

pub(super) fn merge_config(base: ConfigFile, other: ConfigFile) -> ConfigFile {
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

pub(super) fn write_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
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

pub(super) fn write_json_atomic<T: Serialize>(path: &Path, val: &T) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(val)?;
    write_atomic(path, &bytes)
}

pub(super) fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> Result<T> {
    let raw = fs::read(path)?;
    Ok(serde_json::from_slice(&raw)?)
}

pub(super) fn read_text(path: &Path) -> Result<String> {
    Ok(fs::read_to_string(path)?)
}

pub(super) fn job_lock(paths: &Paths, id: &str) -> Result<File> {
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

pub(super) fn resolve_name_to_id(paths: &Paths, name: &str) -> Result<Option<String>> {
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

pub(super) fn resolve_meta_name_to_id(paths: &Paths, name: &str) -> Result<Option<String>> {
    Ok(list_named_job_ids(paths, name)?.into_iter().next())
}

pub(super) fn list_named_job_ids(paths: &Paths, name: &str) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    for id in list_job_ids(paths)? {
        if let Ok(meta) = load_meta(paths, &id)
            && meta.name.as_deref() == Some(name)
        {
            ids.push(id);
        }
    }
    ids.sort();
    Ok(ids)
}

pub(super) fn matches_all_tags(job_tags: &[String], required_tags: &[String]) -> bool {
    required_tags
        .iter()
        .all(|t| job_tags.iter().any(|jt| jt == t))
}

pub(super) fn list_job_ids_by_tags(paths: &Paths, required_tags: &[String]) -> Result<Vec<String>> {
    if required_tags.is_empty() {
        return list_job_ids(paths);
    }
    let mut ids = Vec::new();
    for id in list_job_ids(paths)? {
        if let Ok(meta) = load_meta(paths, &id)
            && matches_all_tags(&meta.tags, required_tags)
        {
            ids.push(id);
        }
    }
    ids.sort();
    Ok(ids)
}

pub(super) fn resolve_targets(
    paths: &Paths,
    refs: &[String],
    tag_filters: &[String],
) -> Result<Vec<String>> {
    use std::collections::BTreeSet;
    let mut out = BTreeSet::new();

    for r in refs {
        if let Some(tag) = r.strip_prefix("tag:") {
            if !is_valid_tag(tag) {
                bail!("invalid tag selector '{}'", r);
            }
            let ids = list_job_ids_by_tags(paths, &[tag.to_string()])?;
            if ids.is_empty() {
                bail!("not found: {}", r);
            }
            for id in ids {
                out.insert(id);
            }
            continue;
        }

        out.insert(resolve_ref(paths, r)?);
    }

    if !tag_filters.is_empty() {
        for t in tag_filters {
            if !is_valid_tag(t) {
                bail!("invalid tag '{}'", t);
            }
        }
        let ids = list_job_ids_by_tags(paths, tag_filters)?;
        if ids.is_empty() && out.is_empty() {
            bail!("no jobs match tags");
        }
        for id in ids {
            out.insert(id);
        }
    }

    Ok(out.into_iter().collect())
}

pub(super) fn link_name(paths: &Paths, name: &str, id: &str) -> Result<()> {
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

pub(super) fn unlink_name(paths: &Paths, name: &str) -> Result<()> {
    let p = paths.name_path(name);
    if p.exists() {
        fs::remove_file(p)?;
    }
    Ok(())
}

pub(super) fn list_job_ids(paths: &Paths) -> Result<Vec<String>> {
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

pub(super) fn resolve_ref(paths: &Paths, r: &str) -> Result<String> {
    if let Some(id) = resolve_meta_name_to_id(paths, r)? {
        return Ok(id);
    }

    if let Some(id) = resolve_name_to_id(paths, r)?
        && paths.job_dir(&id).exists()
    {
        return Ok(id);
    }

    let ids = list_job_ids(paths)?;

    for id in &ids {
        if id == r {
            return Ok(id.clone());
        }
    }

    let matches: Vec<String> = ids.into_iter().filter(|id| id.starts_with(r)).collect();
    if matches.len() == 1 {
        return Ok(matches[0].clone());
    }
    if matches.is_empty() {
        bail!("not found: {r}");
    }
    bail!("ambiguous short id '{}': {} matches", r, matches.len())
}

pub(super) fn load_meta(paths: &Paths, id: &str) -> Result<Meta> {
    read_json(&paths.meta_path(id))
}

pub(super) fn load_desired(paths: &Paths, id: &str) -> Result<DesiredFile> {
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

pub(super) fn load_status(paths: &Paths, id: &str) -> Result<StatusFile> {
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

pub(super) fn save_desired(paths: &Paths, id: &str, desired: &DesiredFile) -> Result<()> {
    write_json_atomic(&paths.desired_path(id), desired)
}

pub(super) fn save_status(paths: &Paths, id: &str, status: &StatusFile) -> Result<()> {
    write_json_atomic(&paths.status_path(id), status)
}

pub(super) fn save_pid(paths: &Paths, id: &str, pid: i32) -> Result<()> {
    write_atomic(&paths.pid_path(id), pid.to_string().as_bytes())
}

pub(super) fn remove_pid(paths: &Paths, id: &str) -> Result<()> {
    let p = paths.pid_path(id);
    if p.exists() {
        fs::remove_file(p)?;
    }
    Ok(())
}

pub(super) fn parse_pid(paths: &Paths, id: &str) -> Result<Option<i32>> {
    let p = paths.pid_path(id);
    if !p.exists() {
        return Ok(None);
    }
    let s = read_text(&p)?;
    Ok(Some(s.trim().parse::<i32>()?))
}

pub(super) fn pid_is_alive(pid: i32) -> bool {
    let p = Pid::from_raw(pid);
    match nix::sys::signal::kill(p, None) {
        Ok(()) => true,
        Err(Errno::EPERM) => true,
        Err(Errno::ESRCH) => false,
        Err(_) => false,
    }
}

pub(super) fn reconcile_status_liveness(
    paths: &Paths,
    id: &str,
    status: &mut StatusFile,
) -> Result<bool> {
    let active = matches!(
        status.state,
        StatusState::Starting | StatusState::Running | StatusState::Stopping | StatusState::Backoff
    );
    if !active {
        return Ok(false);
    }

    let pid_on_disk = parse_pid(paths, id)?;
    let pid = status.pid.or(pid_on_disk);
    let alive = pid.map(pid_is_alive).unwrap_or(false);
    if alive {
        return Ok(false);
    }

    if pid.is_some() {
        let _ = remove_pid(paths, id);
    }

    let mut changed = false;
    if status.state != StatusState::Stale {
        status.state = StatusState::Stale;
        changed = true;
    }
    if status.pid.is_some() {
        status.pid = None;
        changed = true;
    }
    if status.error.is_none() {
        status.error = Some("stale: process not running".to_string());
        changed = true;
    }
    if changed {
        status.last_seen_at = Some(now_rfc3339());
    }

    Ok(changed)
}

pub(super) fn should_show_default(status: &StatusState) -> bool {
    !matches!(status, StatusState::Exited | StatusState::Stale)
}

pub(super) fn stream_base_name(stdout: bool) -> &'static str {
    if stdout { "stdout.log" } else { "stderr.log" }
}

pub(super) fn rotate_if_needed(
    paths: &Paths,
    id: &str,
    stdout: bool,
    logging: &LoggingConfig,
) -> Result<()> {
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

pub(super) fn append_bytes(
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

pub(super) async fn pump_stream<R: AsyncRead + Unpin>(
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

pub(super) fn parse_signal(name: &str) -> Result<Signal> {
    match name.to_ascii_uppercase().as_str() {
        "TERM" | "SIGTERM" => Ok(Signal::SIGTERM),
        "INT" | "SIGINT" => Ok(Signal::SIGINT),
        "KILL" | "SIGKILL" => Ok(Signal::SIGKILL),
        other => bail!("unsupported signal: {other}"),
    }
}

pub(super) fn to_signal_name(sig: Option<i32>) -> Option<String> {
    sig.and_then(|n| Signal::try_from(n).ok())
        .map(|s| format!("{s:?}").trim_start_matches("SIG").to_string())
}

pub(super) fn kill_pgroup(pid: i32, signal: Signal) -> Result<()> {
    let pgid = Pid::from_raw(pid);
    match killpg(pgid, signal) {
        Ok(()) => Ok(()),
        Err(Errno::ESRCH) => Ok(()),
        Err(e) => Err(anyhow!(e)),
    }
}
