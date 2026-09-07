#!/usr/bin/env bash
# Picks how the container runs. RUN_MODE:
#   cron (default) - supercronic fires `python -m watcher --once` on CRON_SCHEDULE.
#                    The container idles between ticks; nothing is resident but cron.
#   once           - one scan, then exit. For an external scheduler
#                    (systemd timer, Windows Task Scheduler, Kubernetes CronJob).
#   poll           - stay resident and scan every VRFW_POLL_INTERVAL_SECONDS.
#   <anything>     - exec'd verbatim, so `docker compose run ... --status` works.
set -euo pipefail

# ── IAM Roles Anywhere: patch host paths in the host-mounted .aws/config ──
# The host config references aws_signing_helper(.exe) and absolute paths into the
# host's .aws directory -- either Windows-style (C:\Users\...\.aws\) or a native
# Linux path (e.g. /home/user/.aws/, which is what a Linux host's own `aws configure`
# writes). Rewrite both forms so the IAmAnywhere profile works with the Linux binary
# shipped in this image and the files available under /root/.aws (the read-only
# bind-mount from the host, whatever the host-side directory happens to be called).
if [ -f /root/.aws/config ]; then
    sed \
        -e 's|[A-Za-z]:[/\\][Uu]sers[/\\][^/\\]*[/\\]\.aws[/\\]aws_signing_helper\.exe|/usr/local/bin/aws_signing_helper|g' \
        -e 's|[A-Za-z]:[/\\][Uu]sers[/\\][^/\\]*[/\\]\.aws[/\\]\?|/root/.aws/|g' \
        -e 's|/[^[:space:]]*/\.aws/aws_signing_helper|/usr/local/bin/aws_signing_helper|g' \
        -e 's|/[^[:space:]]*/\.aws/|/root/.aws/|g' \
        /root/.aws/config > /tmp/aws-config-linux
    export AWS_CONFIG_FILE=/tmp/aws-config-linux
    echo "[entrypoint] ~/.aws/config patched for Linux (Windows paths → /root/.aws/, signing helper → Linux binary)"
fi

RUN_MODE="${RUN_MODE:-cron}"
CRON_SCHEDULE="${CRON_SCHEDULE:-*/10 * * * *}"

# Any argument given to `docker run`/`docker compose run` wins over RUN_MODE, so
# `docker compose run --rm packaging-watcher --status` does what it looks like.
if [ "$#" -gt 0 ]; then
  exec python -m watcher "$@"
fi

case "$RUN_MODE" in
  cron)
    echo "[entrypoint] cron mode: '${CRON_SCHEDULE}' -> python -m watcher --once"
    # A single-line crontab, generated at start-up so the schedule stays an env var.
    printf '%s cd /app && python -m watcher --once\n' "${CRON_SCHEDULE}" > /app/crontab
    if [ "${RUN_AT_STARTUP:-true}" = "true" ]; then
      echo "[entrypoint] priming: one scan now (set RUN_AT_STARTUP=false to skip)"
      python -m watcher --once || echo "[entrypoint] priming scan failed; cron will retry"
    fi
    exec /usr/local/bin/supercronic -passthrough-logs /app/crontab
    ;;
  once)
    exec python -m watcher --once
    ;;
  poll)
    exec python -m watcher
    ;;
  *)
    echo "[entrypoint] unknown RUN_MODE='${RUN_MODE}' (expected cron, once or poll)" >&2
    exit 64
    ;;
esac
