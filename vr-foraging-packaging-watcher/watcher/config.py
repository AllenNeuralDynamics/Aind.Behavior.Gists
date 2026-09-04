"""Configuration for the packaging watcher.

Every field is settable from the environment with the ``VRFW_`` prefix, e.g.
``VRFW_POLL_INTERVAL_SECONDS=30``. Lists are passed as JSON, e.g.
``VRFW_PACKAGING_EXTRA_ARGS='["--write-nwb"]'``.
"""

from __future__ import annotations

import re
from datetime import UTC, date, datetime
from pathlib import Path

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

DEFAULT_SESSION_REGEX = r"^(?P<subject>.+)_(?P<date>\d{4}-\d{2}-\d{2})T\d{6}Z$"


class Config(BaseSettings):
    """Everything the service needs to know, resolved once at start-up."""

    model_config = SettingsConfigDict(env_prefix="VRFW_", extra="forbid")

    # --- Paths (container-side; see docker-compose.yml for the host mappings) ---
    input_dir: Path = Path("/data/raw")
    """Root that is scanned for session folders. Mounted read-only."""
    state_dir: Path = Path("/data/state")
    """Holds the processed-session ledger and the heartbeat file. Must be writable."""
    scratch_dir: Path = Path("/data/scratch")
    """Parent of the per-session temporary export directory. Must be writable."""

    # --- Which folders count as sessions ---
    session_glob: str = "*"
    """Glob, relative to ``input_dir``, that enumerates candidate session folders.
    Use ``*/*`` if sessions are nested one level down (e.g. ``<subject>/<session>``)."""
    session_regex: str = DEFAULT_SESSION_REGEX
    """Only folders whose *name* matches this are treated as sessions. Empty = accept all."""
    subjects: str = ""
    """Subject allowlist as one string, comma- or whitespace-separated
    (e.g. ``"789917, 754573 760332"``). Empty = every subject."""
    min_session_date: str = "today"
    """Ignore sessions dated before this. ``today`` (resolved once, at start-up),
    an ISO date such as ``2026-09-04``, or empty for no lower bound. A session's
    date comes from its folder name; if the name carries no date, the folder's
    mtime is used instead."""
    require_files: list[str] = Field(default_factory=list)
    """Paths, relative to the session folder, that must all exist before it is
    considered complete (e.g. ``["Behavior/Logs/Launcher"]``). Empty = rely on quiescence alone."""

    # --- Timing ---
    poll_interval_seconds: float = 60.0
    """How long to sleep between scans. Only used when running as a resident loop."""
    settle_seconds: float = 300.0
    """A session must be unchanged for this long before it is processed. Guards
    against packaging a session that is still being acquired or copied."""

    # --- Retries ---
    max_attempts: int = 3
    """Attempts per session before it is parked as permanently failed."""
    retry_backoff_seconds: float = 900.0
    """Base delay before retrying a failed session; doubles per attempt."""

    # --- Packaging step ---
    packaging_command: list[str] = Field(default_factory=lambda: ["vr-foraging-packaging"])
    """Argv prefix for the packaging CLI. The image installs it on PATH; override with
    e.g. ``["uvx","--from","git+https://.../Aind.Behavior.VrForaging.Packaging.git","vr-foraging-packaging"]``."""
    packaging_subcommand: str = "session"
    """``session`` (one raw session per invocation) is what this service drives."""
    packaging_extra_args: list[str] = Field(default_factory=list)
    """Appended verbatim, after ``--input-dir``/``--output-dir``/``--log-file``."""
    packaging_timeout_seconds: float = 3600.0

    # --- Upload step ---
    aws_command: list[str] = Field(default_factory=lambda: ["aws"])
    s3_bucket: str = "aind-scratch-data"
    """Bucket name. The AIND ``bucket@region`` form is accepted and split automatically."""
    s3_prefix: str = "vr-foraging/replenishment-temp-sharing"
    """Key prefix; each session is synced to ``<prefix>/<session_name>/``."""
    aws_region: str = "us-west-2"
    s3_sync_extra_args: list[str] = Field(default_factory=list)
    """Appended to ``aws s3 sync``, e.g. ``["--storage-class","INTELLIGENT_TIERING"]``."""
    upload_timeout_seconds: float = 7200.0
    check_remote_before_upload: bool = True
    """Before packaging, run ``aws s3 ls`` on the session's destination prefix. If
    anything is already there, skip packaging and uploading entirely and just mark
    the session done. This is what makes the S3 bucket — not the local ledger —
    the real source of truth for "already handled": a wiped state volume does not
    cause every in-scope session to be re-packaged and re-uploaded."""
    remote_check_timeout_seconds: float = 60.0

    # --- Behaviour ---
    process_existing: bool = True
    """True: any session that passes the subject and date filters is processed, whether
    it was already on the share at start-up or appeared afterwards — ``min_session_date``
    is what keeps the archive out. False: sessions present at first start are recorded
    as pre-existing and never uploaded, however new they look."""
    dry_run: bool = False
    """Log the two commands for each due session instead of running them."""
    log_level: str = "INFO"

    @field_validator("session_regex")
    @classmethod
    def _check_regex(cls, value: str) -> str:
        re.compile(value)
        return value

    @model_validator(mode="after")
    def _normalise(self) -> Config:
        # "aind-scratch-data@us-west-2" is how AIND writes these paths; the AWS CLI
        # does not understand it, so peel the region off and pass it via --region.
        if "@" in self.s3_bucket:
            bucket, _, region = self.s3_bucket.partition("@")
            self.s3_bucket = bucket
            self.aws_region = region
        self.s3_prefix = self.s3_prefix.strip("/")

        # Pin "today" to a concrete date now, so a container that runs for a week
        # keeps the floor it started with instead of silently dropping yesterday's
        # sessions at every midnight.
        if self.min_session_date.strip().lower() == "today":
            self.min_session_date = datetime.now(UTC).date().isoformat()
        elif self.min_session_date.strip():
            date.fromisoformat(self.min_session_date.strip())  # fail fast on a typo
            self.min_session_date = self.min_session_date.strip()
        return self

    @property
    def subject_ids(self) -> frozenset[str]:
        """The subject allowlist, parsed. Empty means "no filter"."""
        return frozenset(token for token in re.split(r"[,;\s]+", self.subjects) if token)

    @property
    def min_date(self) -> date | None:
        return date.fromisoformat(self.min_session_date) if self.min_session_date else None

    def s3_destination(self, session_name: str) -> str:
        """The ``s3://`` URI a given session's export is synced to."""
        prefix = f"{self.s3_prefix}/" if self.s3_prefix else ""
        return f"s3://{self.s3_bucket}/{prefix}{session_name}/"

    @property
    def heartbeat_path(self) -> Path:
        return self.state_dir / "heartbeat"

    @property
    def ledger_path(self) -> Path:
        return self.state_dir / "sessions.json"

    @property
    def lock_path(self) -> Path:
        return self.state_dir / "watcher.lock"
