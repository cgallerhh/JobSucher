#!/bin/zsh

set -euo pipefail

SCRIPT_DIR="${0:A:h}"
SOURCE_REPO="${SCRIPT_DIR:h}"
RUNNER="$SCRIPT_DIR/run_daily_cron.sh"
MARKER="# JobSucher real cron - managed by scripts/install_cron.sh"
ENTRY="0 7 * * * JOBSUCHER_SOURCE_REPO=$SOURCE_REPO /bin/zsh $RUNNER"
TEMP_FILE="$(mktemp -t jobsucher-crontab)"

cleanup() {
  rm -f "$TEMP_FILE"
}
trap cleanup EXIT INT TERM

(crontab -l 2>/dev/null || true) \
  | awk '!/JobSucher real cron/ && !/scripts\/run_daily_cron\.sh/' \
  > "$TEMP_FILE"

if [[ "${1:-}" != "--remove" ]]; then
  printf '%s\n%s\n' "$MARKER" "$ENTRY" >> "$TEMP_FILE"
fi

crontab "$TEMP_FILE"

if [[ "${1:-}" == "--remove" ]]; then
  print "JobSucher-Cron entfernt."
else
  print "JobSucher-Cron installiert: täglich 07:00 Europe/Berlin"
  print "$ENTRY"
fi
