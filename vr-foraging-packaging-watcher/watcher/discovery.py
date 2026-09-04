"""Finding session folders, and deciding when one has stopped changing.

Network shares do not deliver usable filesystem events, so this is a poller.

Two things happen here. First the candidate folders are filtered down — by name,
by subject, and by date — so that a share holding the whole archive only ever
yields the handful of sessions we care about. Then each survivor is checked for
completeness, because packaging a session that is still being written produces
garbage. Completeness needs two independent criteria to agree, and both survive a
process restart, so this works identically in the resident loop and in a one-shot
cron run:

1. *Quiet* — the newest mtime anywhere in the tree is at least ``settle_seconds``
   old. Catches an acquisition or a copy that is still in flight.
2. *Stable* — the tree's signature (file count, total size, newest mtime) matches
   the one recorded in the ledger the last time we looked. Catches a copy that
   preserves source mtimes (robocopy /COPY:T, rsync -t), which would otherwise
   look quiet while still growing.

A session seen for the very first time has no recorded signature, so criterion 2
is vacuously satisfied and a long-idle session is picked up on the first pass.
"""

from __future__ import annotations

import logging
import re
import time
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from .config import Config
from .state import Ledger, SessionRecord

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Signature:
    """Cheap fingerprint of a directory tree; equality means "looks unchanged"."""

    files: int
    total_bytes: int
    newest_mtime: float


def subject_of(path: Path, match: re.Match | None) -> str:
    """The subject a session folder belongs to.

    Read from the regex's ``subject`` group when there is one; otherwise the part
    of the folder name before the first underscore.
    """
    if match is not None and match.groupdict().get("subject"):
        return match.group("subject")
    return path.name.split("_", 1)[0]


def date_of(path: Path, match: re.Match | None) -> date:
    """The date a session belongs to.

    Preferred source is the date embedded in the folder name, which is what the
    rig wrote and is immune to copying. Only if the name carries no date does this
    fall back to the folder's mtime — note that a true creation timestamp is not
    available over CIFS, so a copied folder's mtime may not be its acquisition date.
    """
    if match is not None and match.groupdict().get("date"):
        try:
            return date.fromisoformat(match.group("date"))
        except ValueError:
            logger.debug("Session %s has an unparseable date group; falling back to mtime.", path.name)
    return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).date()


def scan(config: Config) -> list[Path]:
    """Session folders under ``input_dir`` that pass every filter.

    Logs one summary line per scan rather than one line per rejected folder — the
    share can hold thousands of sessions and the interesting number is how many
    got through.
    """
    if not config.input_dir.is_dir():
        raise FileNotFoundError(f"input_dir does not exist or is not a directory: {config.input_dir}")

    pattern = re.compile(config.session_regex) if config.session_regex else None
    allowed_subjects = config.subject_ids
    floor = config.min_date

    found: list[Path] = []
    rejected = {"name": 0, "subject": 0, "date": 0}
    for path in sorted(config.input_dir.glob(config.session_glob)):
        if not path.is_dir():
            continue

        match = pattern.match(path.name) if pattern is not None else None
        if pattern is not None and match is None:
            rejected["name"] += 1
            continue

        if allowed_subjects and subject_of(path, match) not in allowed_subjects:
            rejected["subject"] += 1
            continue

        if floor is not None and date_of(path, match) < floor:
            rejected["date"] += 1
            continue

        found.append(path)

    skipped = sum(rejected.values())
    logger.info(
        "Scan of %s: %d session(s) in scope, %d skipped (%d name, %d subject, %d before %s).",
        config.input_dir,
        len(found),
        skipped,
        rejected["name"],
        rejected["subject"],
        rejected["date"],
        floor or "-",
    )
    return found


def signature(path: Path) -> Signature:
    """Walk *path* and summarise it.

    Unreadable entries are skipped rather than fatal: a file being written on an
    SMB share can briefly fail to stat, and that is exactly the state we expect
    to observe here.
    """
    files = 0
    total = 0
    newest = 0.0
    for entry in path.rglob("*"):
        try:
            stat = entry.stat()
        except OSError as exc:
            logger.debug("Could not stat %s: %s", entry, exc)
            continue
        newest = max(newest, stat.st_mtime)
        if entry.is_file():
            files += 1
            total += stat.st_size
    return Signature(files=files, total_bytes=total, newest_mtime=newest)


def is_ready(config: Config, path: Path, record: SessionRecord, ledger: Ledger) -> bool:
    """Whether *path* is complete enough to package.

    Records the observed signature on *record* as a side effect, so the next run
    — in this process or a later one — can tell whether the tree moved.
    """
    if missing := [f for f in config.require_files if not (path / f).exists()]:
        logger.info("Session %s not ready: missing %s", path.name, ", ".join(missing))
        return False

    current = asdict(signature(path))
    previous = record.signature
    if previous is not None and previous != current:
        logger.info(
            "Session %s changed since the last look (%d -> %d files, %.1f -> %.1f MiB) — waiting.",
            path.name,
            previous["files"],
            current["files"],
            previous["total_bytes"] / 1024**2,
            current["total_bytes"] / 1024**2,
        )
        ledger.update(record, signature=current, signature_seen_at=time.time())
        return False

    if previous is None:
        ledger.update(record, signature=current, signature_seen_at=time.time())

    quiet_for = time.time() - current["newest_mtime"]
    if quiet_for < config.settle_seconds:
        logger.info(
            "Session %s was written %.0fs ago, needs %.0fs of quiet — waiting.",
            path.name,
            quiet_for,
            config.settle_seconds,
        )
        return False

    logger.info(
        "Session %s looks complete: %d files, %.1f MiB, quiet for %.0fs.",
        path.name,
        current["files"],
        current["total_bytes"] / 1024**2,
        quiet_for,
    )
    return True
