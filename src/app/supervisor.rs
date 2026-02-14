use super::cli::gc_impl;
use super::ops::*;
use super::*;

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
            IpcRequest::Run { id } => {
                if let Some(job) = self.jobs.get(&id)
                    && job.runtime.is_none()
                    && matches!(job.status.state, StatusState::Exited)
                    && matches!(job.meta.restart.policy, RestartPolicy::No)
                    && job.status.last_exit.is_some()
                {
                    return IpcResponse::ok(
                        json!({"id":id,"name":job.meta.name,"pid":job.status.pid}),
                    );
                }

                match self.start_if_needed(&id).await {
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
                        if let Some(job) = self.jobs.get(&id)
                            && job.runtime.is_none()
                            && matches!(job.status.state, StatusState::Exited)
                            && matches!(job.meta.restart.policy, RestartPolicy::No)
                            && job.status.last_exit.is_some()
                        {
                            return IpcResponse::ok(
                                json!({"id":id,"name":job.meta.name,"pid":job.status.pid}),
                            );
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
                }
            }
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

pub(super) async fn read_frame_async(stream: &mut TokioUnixStream) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await?;
    Ok(body)
}

pub(super) async fn write_frame_async(stream: &mut TokioUnixStream, body: &[u8]) -> Result<()> {
    let len = body.len() as u32;
    stream.write_all(&len.to_le_bytes()).await?;
    stream.write_all(body).await?;
    Ok(())
}

pub(super) fn read_frame(stream: &mut UnixStream) -> Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf)?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body)?;
    Ok(body)
}

pub(super) fn write_frame(stream: &mut UnixStream, body: &[u8]) -> Result<()> {
    let len = body.len() as u32;
    stream.write_all(&len.to_le_bytes())?;
    stream.write_all(body)?;
    Ok(())
}

pub(super) async fn supervisor_main(paths: Paths) -> Result<()> {
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

pub(super) fn start_supervisor_detached(paths: &Paths) -> Result<()> {
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

pub(super) fn connect_ipc(paths: &Paths) -> Result<UnixStream> {
    let sock = paths.socket_path();
    let stream = UnixStream::connect(sock)?;
    Ok(stream)
}

pub(super) fn send_ipc(paths: &Paths, req: &IpcRequest) -> Result<IpcResponse> {
    let mut stream = connect_ipc(paths)?;
    let body = serde_json::to_vec(req)?;
    write_frame(&mut stream, &body)?;
    let resp = read_frame(&mut stream)?;
    Ok(serde_json::from_slice(&resp)?)
}

pub(super) fn ensure_supervisor(paths: &Paths) -> Result<()> {
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

pub(super) struct NewJobSpec {
    pub(super) name: Option<String>,
    pub(super) cwd: PathBuf,
    pub(super) env_map: HashMap<String, String>,
    pub(super) inherit_env: bool,
    pub(super) cmd: Vec<String>,
    pub(super) logging: LoggingConfig,
    pub(super) restart: RestartConfig,
}
