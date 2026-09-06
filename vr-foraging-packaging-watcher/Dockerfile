# syntax=docker/dockerfile:1
ARG PYTHON_VERSION=3.13
FROM ghcr.io/astral-sh/uv:python${PYTHON_VERSION}-bookworm-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy

# libgl1 & co: opencv (pulled in via the packaging CLI) needs the X/GL runtime
# even for headless video decoding.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates curl unzip git tzdata \
      libgl1 libglib2.0-0 libxcb1 libsm6 libxext6 libxrender1 \
 && rm -rf /var/lib/apt/lists/*

# --- AWS CLI v2 ---------------------------------------------------------------
RUN set -eux; \
    case "$(dpkg --print-architecture)" in \
      amd64) arch=x86_64 ;; \
      arm64) arch=aarch64 ;; \
      *) echo "unsupported architecture: $(dpkg --print-architecture)" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-${arch}.zip" -o /tmp/awscliv2.zip; \
    unzip -q /tmp/awscliv2.zip -d /tmp; \
    /tmp/aws/install; \
    rm -rf /tmp/aws /tmp/awscliv2.zip; \
    aws --version

# --- IAM Roles Anywhere signing helper -------------------------------------------
# The host profile (IAmAnywhere) uses aws_signing_helper.exe (Windows).  We ship
# the Linux equivalent here; the entrypoint rewrites the mounted config at startup
# to point at this binary instead of the Windows exe.
ARG SIGNING_HELPER_VERSION=1.4.0
RUN set -eux; \
    case "$(dpkg --print-architecture)" in \
      amd64) arch=X86_64 ;; \
      arm64) arch=aarch64 ;; \
      *) echo "unsupported architecture" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://rolesanywhere.amazonaws.com/releases/${SIGNING_HELPER_VERSION}/${arch}/Linux/aws_signing_helper" \
        -o /usr/local/bin/aws_signing_helper; \
    chmod +x /usr/local/bin/aws_signing_helper

# --- supercronic: cron for containers (logs to stdout, no PID-1 quirks) -------
ARG SUPERCRONIC_VERSION=v0.2.33
RUN set -eux; \
    case "$(dpkg --print-architecture)" in \
      amd64) arch=amd64 ;; \
      arm64) arch=arm64 ;; \
      *) echo "unsupported architecture" >&2; exit 1 ;; \
    esac; \
    curl -fsSL "https://github.com/aptible/supercronic/releases/download/${SUPERCRONIC_VERSION}/supercronic-linux-${arch}" \
        -o /usr/local/bin/supercronic; \
    chmod +x /usr/local/bin/supercronic; \
    supercronic -version

# --- The watcher + the packaging CLI (pinned as the `packaging-cli` extra in
# pyproject.toml), in one venv --------------------------------------------------
WORKDIR /app
ENV VIRTUAL_ENV=/app/.venv \
    PATH="/app/.venv/bin:${PATH}"
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --locked --no-dev --extra packaging-cli --no-install-project \
 && vr-foraging-packaging --help > /dev/null
COPY watcher ./watcher
COPY docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
# Strip CRLF in case this was checked out on Windows -- the kernel reads it as
# part of the shebang and refuses to execute otherwise.
RUN tr -d '\r' < /usr/local/bin/docker-entrypoint.sh > /tmp/entrypoint \
 && mv /tmp/entrypoint /usr/local/bin/docker-entrypoint.sh \
 && chmod +x /usr/local/bin/docker-entrypoint.sh

ENV VRFW_INPUT_DIR=/data/raw \
    VRFW_STATE_DIR=/data/state \
    VRFW_SCRATCH_DIR=/data/scratch \
    RUN_MODE=cron \
    CRON_SCHEDULE="*/10 * * * *"
VOLUME ["/data/state", "/data/scratch"]

# Liveness = "a scan finished recently" (the watcher writes a heartbeat file).
HEALTHCHECK --interval=120s --timeout=15s --start-period=120s --retries=3 \
  CMD test -f /data/state/heartbeat \
   && [ $(( $(date +%s) - $(stat -c %Y /data/state/heartbeat) )) -lt "${HEARTBEAT_MAX_AGE_SECONDS:-5400}" ]

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
