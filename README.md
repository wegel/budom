# budom

`budom` is a per-user background daemon manager for Linux/Unix user processes.

It provides:
- run/list/inspect/stop/remove lifecycle management
- a per-user supervisor process (no systemd dependency)
- restart policies with exponential backoff
- raw stdout/stderr log capture and size-based rotation
- stable job names and short/partial ID references

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

Stop one or many jobs:

```bash
budom stop demo other-job 01abcde
```

Remove job state:

```bash
budom rm demo --force
```

## ID and Ref Resolution

Commands that take a `<ref>` accept:
1. exact name
2. exact full ID (case-insensitive)
3. unique ID prefix (including 1 character if unique)

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
