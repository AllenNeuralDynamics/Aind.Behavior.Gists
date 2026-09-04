"""The processed-session ledger.

A single JSON file, rewritten atomically after every change. It is the only thing
that makes the service idempotent across restarts: a session recorded as ``done``
is never packaged or uploaded again.
"""

from __future__ import annotations

import dataclasses
import json
import logging
import os
import tempfile
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path

logger = logging.getLogger(__name__)


def _now() -> str:
    return datetime.now(UTC).isoformat()


class Status(StrEnum):
    PENDING = "pending"
    """Seen, but not yet successfully processed."""
    DONE = "done"
    """Packaged and uploaded."""
    FAILED = "failed"
    """Out of attempts; needs a human. Clear the record to retry."""
    PRE_EXISTING = "pre_existing"
    """Present at first start-up while ``process_existing`` was false."""


@dataclasses.dataclass
class SessionRecord:
    name: str
    status: Status = Status.PENDING
    attempts: int = 0
    first_seen: str = dataclasses.field(default_factory=_now)
    last_attempt: str | None = None
    completed_at: str | None = None
    retry_after: float | None = None
    """Monotonic-independent wall-clock epoch before which no retry is made."""
    last_error: str | None = None
    s3_uri: str | None = None
    signature: dict | None = None
    """Last observed directory fingerprint; see :func:`watcher.discovery.is_ready`."""
    signature_seen_at: float | None = None

    @classmethod
    def from_dict(cls, raw: dict) -> SessionRecord:
        known = {f.name for f in dataclasses.fields(cls)}
        return cls(**{k: v for k, v in raw.items() if k in known})


class Ledger:
    """Dict-like store of :class:`SessionRecord` persisted to one JSON file."""

    def __init__(self, path: Path) -> None:
        self._path = path
        self._records: dict[str, SessionRecord] = {}
        self._load()

    def _load(self) -> None:
        if not self._path.exists():
            logger.info("No ledger at %s — starting a new one.", self._path)
            return
        raw = json.loads(self._path.read_text(encoding="utf-8"))
        self._records = {name: SessionRecord.from_dict(rec) for name, rec in raw.get("sessions", {}).items()}
        logger.info("Loaded %d session records from %s", len(self._records), self._path)

    def save(self) -> None:
        payload = {
            "updated_at": _now(),
            "sessions": {name: dataclasses.asdict(rec) for name, rec in sorted(self._records.items())},
        }
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # Write-then-rename so a crash mid-write cannot truncate the ledger.
        fd, tmp = tempfile.mkstemp(dir=self._path.parent, prefix=".sessions-", suffix=".json")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2, sort_keys=True)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, self._path)
        except BaseException:
            Path(tmp).unlink(missing_ok=True)
            raise

    def __contains__(self, name: str) -> bool:
        return name in self._records

    def __len__(self) -> int:
        return len(self._records)

    def get(self, name: str) -> SessionRecord | None:
        return self._records.get(name)

    def ensure(self, name: str, status: Status = Status.PENDING) -> SessionRecord:
        """Return the record for *name*, creating it with *status* if it is new."""
        record = self._records.get(name)
        if record is None:
            record = SessionRecord(name=name, status=status)
            self._records[name] = record
            self.save()
            logger.info("New session registered: %s (%s)", name, status)
        return record

    def update(self, record: SessionRecord, **changes) -> SessionRecord:
        for key, value in changes.items():
            setattr(record, key, value)
        self._records[record.name] = record
        self.save()
        return record

    def counts(self) -> dict[str, int]:
        tally: dict[str, int] = {}
        for record in self._records.values():
            key = str(record.status)
            tally[key] = tally.get(key, 0) + 1
        return tally
