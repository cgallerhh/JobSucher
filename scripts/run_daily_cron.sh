#!/bin/zsh

set -euo pipefail

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"

SCRIPT_DIR="${0:A:h}"
DEFAULT_SOURCE_REPO="${SCRIPT_DIR:h}"
SOURCE_REPO="${JOBSUCHER_SOURCE_REPO:-$DEFAULT_SOURCE_REPO}"
RUNTIME_ROOT="${JOBSUCHER_RUNTIME_ROOT:-/Users/cgaller/Library/Application Support/JobSucher}"
ENV_FILE="${JOBSUCHER_ENV_FILE:-$SOURCE_REPO/.env}"
STATE_FILE="${JOBSUCHER_SEEN_FILE:-$RUNTIME_ROOT/seen_jobs.json}"
LOG_DIR="$RUNTIME_ROOT/logs"
LOG_FILE="$LOG_DIR/cron.log"
LOCK_DIR="$RUNTIME_ROOT/run.lock"
PYTHON_BIN="$SOURCE_REPO/.venv/bin/python"

umask 077
mkdir -p "$RUNTIME_ROOT" "$LOG_DIR"

# Keep one previous log and prevent unbounded growth.
if [[ -f "$LOG_FILE" ]] && (( $(stat -f %z "$LOG_FILE") > 5242880 )); then
  mv "$LOG_FILE" "$LOG_FILE.1"
fi
exec >>"$LOG_FILE" 2>&1

log() {
  print -r -- "$(date '+%Y-%m-%d %H:%M:%S %Z')  $*"
}

if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  log "SKIP: Ein JobSucher-Lauf ist bereits aktiv."
  exit 0
fi

cleanup() {
  rmdir "$LOCK_DIR" 2>/dev/null || true
}
trap cleanup EXIT INT TERM

log "START: echter Cronlauf"

if [[ ! -f "$ENV_FILE" ]]; then
  log "FEHLER: Konfiguration fehlt: $ENV_FILE"
  exit 1
fi

if [[ ! -f "$STATE_FILE" ]]; then
  cp "$SOURCE_REPO/data/seen_jobs.json" "$STATE_FILE"
  log "Seen-State initialisiert: $STATE_FILE"
fi

if [[ ! -x "$PYTHON_BIN" ]]; then
  log "Virtuelle Python-Umgebung wird angelegt."
  /usr/bin/python3 -m venv "$SOURCE_REPO/.venv"
  "$SOURCE_REPO/.venv/bin/pip" install -r "$SOURCE_REPO/requirements.txt"
fi

set -a
source "$ENV_FILE"
set +a
export JOBSUCHER_SEEN_FILE="$STATE_FILE"

cd "$SOURCE_REPO"
"$PYTHON_BIN" -m job_search.main

log "ENDE: Cronlauf erfolgreich"
