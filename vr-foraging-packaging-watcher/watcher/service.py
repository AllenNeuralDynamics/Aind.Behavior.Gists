"""The scan-and-process pass that ties discovery, the ledger and the pipeline together.

The same pass backs all three run modes: one cron tick, one ``--once`` invocation,
or one iteration of the resident loop. Nothing about it assumes the process will
still be alive a minute later, which is what makes cron mode safe.
"""

from __future__ import annotations

import logging
import signal
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

from .config import Config
from .discovery import is_ready, scan
from .pipeline import StepFailed, process_session
from .state import Ledger, SessionRecord, Status

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(UTC).isoformat()


class Service:
    def __init__(self, config: Config) -> None:
        self._config = config
        self._ledger = Ledger(config.ledger_path)
        self._stop = threading.Event()

    # -- lifecycle ----------------------------------------------------------
    def install_signal_handlers(self) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._on_signal)

    def _on_signal(self, signum: int, _frame) -> None:
        logger.warning("Received signal %s — finishing the current session, then exiting.", signum)
        self._stop.set()

    def log_filters(self) -> None:
        """State the filters up front. Silently processing the wrong subjects, or
        silently processing nothing at all, are the two failure modes worth one
        line of log per run."""
        subjects = self._config.subject_ids
        logger.info("Subjects: %s", ", ".join(sorted(subjects)) if subjects else "ALL (no allowlist set)")
        logger.info("Sessions dated on or after: %s", self._config.min_date or "no lower bound")

    def ledger_counts(self) -> dict[str, int]:
        return self._ledger.counts()

    def run_once(self) -> None:
        """One scan-and-process pass, then return. This is what cron fires."""
        self._config.state_dir.mkdir(parents=True, exist_ok=True)
        self.log_filters()
        self._pass()
        self._heartbeat()

    def run(self) -> None:
        """Stay resident, passing over the share every ``poll_interval_seconds``."""
        config = self._config
        config.state_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            "Watching %s (glob %r) every %.0fs", config.input_dir, config.session_glob, config.poll_interval_seconds
        )
        self.log_filters()
        logger.info("Destination: %s", config.s3_destination("<session>"))
        if config.dry_run:
            logger.warning("DRY RUN — no packaging or upload will actually happen.")

        while not self._stop.is_set():
            try:
                self._pass()
            except Exception:
                # A bad pass (share offline, permissions) must not kill the service.
                logger.exception("Scan failed; retrying at the next poll.")
            self._heartbeat()
            self._stop.wait(config.poll_interval_seconds)

        logger.info("Stopped. Ledger: %s", self._ledger.counts())

    # -- one pass -----------------------------------------------------------
    def _pass(self) -> None:
        """List the share once, then work through whatever is in scope."""
        sessions = scan(self._config)
        if len(self._ledger) == 0:
            self._baseline(sessions)

        for path in sessions:
            if self._stop.is_set():
                return
            record = self._ledger.ensure(path.name)
            if not self._is_due(record):
                continue
            if not is_ready(self._config, path, record, self._ledger):
                continue
            self._process(record, path)

    def _baseline(self, sessions: list[Path]) -> None:
        """First ever run: decide what to do with sessions that already exist.

        With ``process_existing`` on (the default) this only logs — the sessions
        are picked up by the pass that follows, since the date floor is what keeps
        the archive out. With it off, they are parked so that only folders
        appearing from now on are ever uploaded.
        """
        if not sessions:
            logger.info("No sessions in scope yet.")
            return
        if self._config.process_existing:
            logger.info("First run: %d session(s) already in scope will be processed.", len(sessions))
            return
        logger.warning(
            "First run: recording %d in-scope session(s) as %s, because process_existing is off.",
            len(sessions),
            Status.PRE_EXISTING,
        )
        for path in sessions:
            self._ledger.ensure(path.name, Status.PRE_EXISTING)

    def _is_due(self, record: SessionRecord) -> bool:
        if record.status in (Status.DONE, Status.PRE_EXISTING):
            return False
        if record.status is Status.FAILED:
            return False
        return not (record.retry_after is not None and time.time() < record.retry_after)

    def _process(self, record: SessionRecord, path: Path) -> None:
        attempt = record.attempts + 1
        logger.info("Processing %s (attempt %d/%d)", record.name, attempt, self._config.max_attempts)
        self._ledger.update(record, attempts=attempt, last_attempt=_now())
        try:
            uri = process_session(self._config, path)
        except StepFailed as exc:
            self._on_failure(record, str(exc), attempt)
        except Exception as exc:  # unexpected, but one session must not stop the run
            logger.exception("Unexpected failure processing %s", record.name)
            self._on_failure(record, f"{type(exc).__name__}: {exc}", attempt)
        else:
            self._ledger.update(
                record,
                status=Status.DONE,
                completed_at=_now(),
                retry_after=None,
                last_error=None,
                s3_uri=uri,
            )
            logger.info("Done: %s -> %s", record.name, uri)

    def _on_failure(self, record: SessionRecord, message: str, attempt: int) -> None:
        if attempt >= self._config.max_attempts:
            logger.error(
                "Session %s failed %d times, giving up: %s. Remove it from %s to retry.",
                record.name,
                attempt,
                message,
                self._config.ledger_path,
            )
            self._ledger.update(record, status=Status.FAILED, last_error=message, retry_after=None)
            return
        delay = self._config.retry_backoff_seconds * (2 ** (attempt - 1))
        logger.warning("Session %s failed: %s — retrying in %.0fs", record.name, message, delay)
        self._ledger.update(record, status=Status.PENDING, last_error=message, retry_after=time.time() + delay)

    def _heartbeat(self) -> None:
        """Liveness marker for the container healthcheck: "a pass finished"."""
        try:
            self._config.heartbeat_path.write_text(_now(), encoding="utf-8")
        except OSError as exc:
            logger.warning("Could not write heartbeat: %s", exc)
