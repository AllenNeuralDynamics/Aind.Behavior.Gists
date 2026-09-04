"""The per-session job: package into a temporary directory, then sync it to S3.

Both steps are external processes. Their output is streamed line-by-line into
this service's logger so ``docker logs`` shows one coherent story, and a non-zero
exit code raises :class:`StepFailed`.
"""

from __future__ import annotations

import logging
import shutil
import subprocess
import tempfile
from pathlib import Path

from .config import Config

logger = logging.getLogger(__name__)


class StepFailed(RuntimeError):
    """An external command exited non-zero, timed out, or could not be started."""


def _run(argv: list[str], *, timeout: float, label: str) -> None:
    logger.info("[%s] $ %s", label, " ".join(argv))
    try:
        process = subprocess.Popen(
            argv,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
        )
    except OSError as exc:
        raise StepFailed(f"{label}: could not start {argv[0]!r}: {exc}") from exc

    assert process.stdout is not None
    try:
        for line in process.stdout:
            logger.info("[%s] %s", label, line.rstrip())
        returncode = process.wait(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        process.kill()
        process.wait()
        raise StepFailed(f"{label}: timed out after {timeout:.0f}s") from exc
    except BaseException:
        process.kill()
        process.wait()
        raise

    if returncode != 0:
        raise StepFailed(f"{label}: exited with code {returncode}")


def packaging_argv(config: Config, session_dir: Path, output_dir: Path) -> list[str]:
    return [
        *config.packaging_command,
        config.packaging_subcommand,
        "--input-dir",
        str(session_dir),
        "--output-dir",
        str(output_dir),
        "--log-file",
        str(output_dir / "packaging.log"),
        *config.packaging_extra_args,
    ]


def sync_argv(config: Config, output_dir: Path, destination: str) -> list[str]:
    return [
        *config.aws_command,
        "s3",
        "sync",
        str(output_dir),
        destination,
        "--region",
        config.aws_region,
        "--only-show-errors",
        *config.s3_sync_extra_args,
    ]


def ls_argv(config: Config, destination: str) -> list[str]:
    return [*config.aws_command, "s3", "ls", destination, "--region", config.aws_region]


def remote_session_exists(config: Config, session_dir: Path) -> bool:
    """Whether *session_dir* already has anything uploaded at its destination.

    Run even during a dry run, since ``aws s3 ls`` is read-only — that way a dry
    run also previews which sessions would be skipped as already-done. A failure
    to check (network hiccup, bad credentials) is treated as "not found" rather
    than raised, so a transient S3 error degrades to re-uploading, not to the
    watcher grinding to a halt.
    """
    destination = config.s3_destination(session_dir.name)
    argv = ls_argv(config, destination)
    logger.info("[check %s] $ %s", session_dir.name, " ".join(argv))
    try:
        result = subprocess.run(
            argv,
            capture_output=True,
            text=True,
            timeout=config.remote_check_timeout_seconds,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired) as exc:
        logger.warning("Could not check %s for existing data: %s. Proceeding as if it is empty.", destination, exc)
        return False

    if result.returncode != 0:
        logger.warning(
            "aws s3 ls exited %d checking %s: %s. Proceeding as if it is empty.",
            result.returncode,
            destination,
            result.stderr.strip(),
        )
        return False

    return bool(result.stdout.strip())


def process_session(config: Config, session_dir: Path) -> str:
    """Package *session_dir* and upload the result. Returns the destination URI.

    The export lives in a temporary directory under ``scratch_dir`` and is removed
    whether or not the upload succeeded — a retry re-packages from scratch rather
    than trusting a half-written export.

    If the destination already has content — from an earlier run whose ledger
    entry was since lost, e.g. — packaging and uploading are both skipped. The S3
    bucket, not the local ledger, is the real record of what's already done.
    """
    destination = config.s3_destination(session_dir.name)

    if config.check_remote_before_upload and remote_session_exists(config, session_dir):
        logger.info("Session %s already has data at %s — skipping packaging and upload.", session_dir.name, destination)
        return destination

    if config.dry_run:
        logger.warning("[dry-run] would package %s and sync it to %s", session_dir, destination)
        logger.warning("[dry-run] %s", " ".join(packaging_argv(config, session_dir, Path("<tmp>"))))
        logger.warning("[dry-run] %s", " ".join(sync_argv(config, Path("<tmp>"), destination)))
        return destination

    config.scratch_dir.mkdir(parents=True, exist_ok=True)
    workdir = Path(tempfile.mkdtemp(dir=config.scratch_dir, prefix=f"{session_dir.name}-"))
    try:
        export_dir = workdir / "export"
        export_dir.mkdir()

        _run(
            packaging_argv(config, session_dir, export_dir),
            timeout=config.packaging_timeout_seconds,
            label=f"package {session_dir.name}",
        )

        produced = sorted(p.name for p in export_dir.iterdir())
        if not produced:
            raise StepFailed(f"packaging produced no files in {export_dir}")
        logger.info("Packaged %s -> %d entries: %s", session_dir.name, len(produced), ", ".join(produced))

        _run(
            sync_argv(config, export_dir, destination),
            timeout=config.upload_timeout_seconds,
            label=f"upload {session_dir.name}",
        )
    finally:
        shutil.rmtree(workdir, ignore_errors=True)

    logger.info("Session %s uploaded to %s", session_dir.name, destination)
    return destination
