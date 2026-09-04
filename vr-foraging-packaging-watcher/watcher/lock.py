"""A cross-process lock so two runs never package the same session at once.

In cron mode a tick can fire while the previous one is still uploading. Rather
than queueing, the newcomer exits immediately: the work is idempotent and the
next tick will pick up whatever is still outstanding.
"""

from __future__ import annotations

import contextlib
import logging
import os
from collections.abc import Iterator
from pathlib import Path

logger = logging.getLogger(__name__)


class AlreadyRunning(RuntimeError):
    """Another process holds the lock."""


@contextlib.contextmanager
def single_instance(path: Path) -> Iterator[None]:
    """Hold an exclusive advisory lock on *path* for the duration of the block."""
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = open(path, "a+", encoding="utf-8")  # noqa: SIM115 - released in the finally below
    try:
        # "a+" leaves the position at EOF; msvcrt.locking() locks whatever byte
        # is there, which then doesn't match the byte truncate()+write() below
        # operate on. Pin it to 0 first so the same byte is locked every time.
        handle.seek(0)
        _acquire(handle, path)
        handle.truncate()
        handle.write(f"pid={os.getpid()}\n")
        handle.flush()
        yield
    finally:
        handle.close()


def _acquire(handle, path: Path) -> None:
    try:
        import fcntl
    except ImportError:  # Windows, i.e. local development only
        import msvcrt

        try:
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        except OSError as exc:
            raise AlreadyRunning(f"another run holds {path}") from exc
        return

    try:
        fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError as exc:
        raise AlreadyRunning(f"another run holds {path}") from exc
