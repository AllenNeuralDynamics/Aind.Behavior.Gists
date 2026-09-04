"""Entry point: ``python -m watcher``.

Three ways to run, all sharing the same ledger and the same lock:

* ``--once``   one scan, then exit. This is what the container's cron fires, and
  what you would call from an external scheduler (systemd timer, Task Scheduler).
* (default)    stay resident and scan every ``poll_interval_seconds``.
* ``--status`` / ``--show-config``  inspect without touching the share.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys

from .config import Config
from .lock import AlreadyRunning, single_instance
from .service import Service


def _configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        stream=sys.stdout,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="watcher", description=__doc__)
    parser.add_argument("--once", action="store_true", help="Run a single scan and exit (cron mode).")
    parser.add_argument("--status", action="store_true", help="Print the session ledger and exit.")
    parser.add_argument("--show-config", action="store_true", help="Print the resolved configuration and exit.")
    args = parser.parse_args(argv)

    config = Config()
    _configure_logging(config.log_level)
    logger = logging.getLogger("watcher")

    if args.show_config:
        print(config.model_dump_json(indent=2))
        return 0

    if args.status:
        service = Service(config)
        print(json.dumps(service.ledger_counts(), indent=2))
        print(config.ledger_path.read_text(encoding="utf-8") if config.ledger_path.exists() else "{}")
        return 0

    config.state_dir.mkdir(parents=True, exist_ok=True)
    try:
        with single_instance(config.lock_path):
            service = Service(config)
            if args.once:
                service.run_once()
            else:
                service.install_signal_handlers()
                service.run()
    except AlreadyRunning as exc:
        # Not an error: the previous tick is still working and will finish the job.
        logger.warning("Skipping this run — %s", exc)
        return 0
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
