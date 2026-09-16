# ADR 0004 — The project path contains a space, and no config may reference it

- **Status:** Accepted
- **Date:** 2026-09-16
- **Decision:** The absolute project path never appears in any file in this repository.

## Context

The project lives at `C:\Users\longt\data engineer\wikiflow`. The space in
`data engineer` is a deliberate choice by the repository owner.

Spaces in paths break tooling in ways that are quiet and hard to diagnose:

- GNU Make handles spaces in targets and prerequisites badly, and failures manifest as
  a target silently not running rather than an error.
- Compose derives its project name from the directory when `COMPOSE_PROJECT_NAME` is
  unset, and sanitises it unpredictably.
- An unquoted `$VAR` in a shell script splits into two arguments.

## Decision

Rather than quote defensively everywhere and hope, remove the exposure:

| Surface | Rule |
|---|---|
| `docker-compose.yml` | Relative volume mounts only (`./dags:/opt/airflow/dags`), resolved against the compose file's own directory |
| `.env` | `COMPOSE_PROJECT_NAME=wikiflow` pinned; no paths stored |
| `Makefile` | Relative paths; never a path as a target or prerequisite |
| Shell scripts | Every expansion quoted, enforced by shellcheck in pre-commit |
| Python | `pathlib` only, enforced by ruff's `PTH` rules; subprocess takes argument lists |
| Airflow | Runs at container paths (`/opt/airflow/...`); the host path is invisible to it |

## Consequences

- The stack is unaffected by the space. Verified: `docker compose up`, a full DAG run
  and both test suites all pass from the real directory.
- `uv` handles it natively, encoding the venv path as `data%20engineer`.
- The repo is *more* portable as a side effect, since it has no absolute paths to
  rewrite when cloned elsewhere.
- One caveat outside the repo's control: Git Bash on Windows rewrites POSIX arguments
  like `/opt/airflow/tests` into Windows paths. `MSYS_NO_PATHCONV=1` is needed when
  invoking container paths from Git Bash. The Makefile and `make.ps1` are unaffected.
