# budom

`budom` is a per-user background daemon manager for Linux/Unix user processes.

Example terminal log:

```text
$ budom run --name demo -- /bin/sh -lc 'while true; do echo hello; sleep 1; done'
3mZ7aK2pQ9xT

$ budom ps
ID            NAME  DESIRED  STATE    PID     RESTARTS
3mZ7aK2pQ9xT  demo  Running  Running  482311  0

$ budom logs demo --tail 3
hello
hello
hello

$ budom stop demo

$ budom ps --all
ID            NAME  DESIRED  STATE   PID  RESTARTS
3mZ7aK2pQ9xT  demo  Stopped  Exited  -    0
```

It provides:
- run/list/inspect/stop/remove lifecycle management
- a per-user supervisor process (no systemd dependency)
- restart policies with exponential backoff
- raw stdout/stderr log capture and size-based rotation
- stable job names and short/partial ID references
- tag-based targeting for bulk stop/remove operations

## Build

```bash
cargo build --release
```

Install locally:

```bash
install -m 755 target/release/budom ~/.local/bin/budom
```

## Quick Start

Run a long-lived process:

```bash
budom run --name demo -- /bin/sh -lc 'while true; do echo hello; sleep 1; done'
```

List running jobs:

```bash
budom ps
```

Inspect details:

```bash
budom inspect demo --json
```

Read logs:

```bash
budom logs demo --tail 50
```

Interleave logs from all daemons matching tags (colorized per daemon):

```bash
budom logs --tag api --tag blue --tail 100 -f
```

Stop one or many jobs:

```bash
budom stop demo other-job 01abcde
```

Remove job state:

```bash
budom rm demo --force
```

Run with tags:

```bash
budom run --name api-a --tag api --tag blue -- /bin/sh -lc 'sleep 9999'
```

Filter by tags:

```bash
budom ps --tag api --tag blue
```

Stop/remove by tags:

```bash
budom stop --tag api --tag blue
budom rm tag:api --force
```

Replace an existing named daemon:

```bash
# Graceful replace: TERM, wait (default 10s), then KILL if needed
budom run --name api-a --replace --replace-timeout 15s -- /path/to/new-binary

# Force replace: immediate forced removal of old daemon
budom run --name api-a --replace --force -- /path/to/new-binary
```

## ID and Ref Resolution

Commands that take a `<ref>` accept:
1. exact name
2. exact full ID (case-sensitive)
3. unique ID prefix (including 1 character if unique)
4. `tag:<tag>` selectors (for commands supporting multi-target refs, like `stop`/`rm`)

If a prefix is ambiguous, the command fails with an ambiguity error.

## Development

Checks used for CI/public quality:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

## License

MIT (see `LICENSE`).
