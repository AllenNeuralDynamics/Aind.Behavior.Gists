"""Batch-override VR Foraging curriculum suggestions in Dataverse — A/B reversal variant.

Identical to main.py, but builds the TrainerState from curriculum_reversal.py (two odors,
rewarded/non-rewarded flipped every block, contrast flipping with it) instead of
curriculum.py (7-odor pairs).

Usage:
    uv run main_reversal.py --subjects 828426 841306 828415
    uv run main_reversal.py --subjects-file subjects.txt
    uv run main_reversal.py --subjects 828426 --dry-run      # build + validate, push nothing
    uv run main_reversal.py --subjects 828426 --yes          # skip the confirmation prompt

Credentials: env vars DATAVERSE_* or ../secrets/dataverse (see dataverse_push.py).
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from aind_behavior_curriculum import TrainerState
from rich.console import Console
from rich.table import Table

from curriculum_reversal import build_trainer_state
from dataverse_push import make_client, push_override

console = Console()


def _load_subjects(args: argparse.Namespace) -> list[str]:
    subjects: list[str] = list(args.subjects or [])
    if args.subjects_file:
        lines = Path(args.subjects_file).read_text(encoding="utf-8").splitlines()
        subjects += [ln.strip() for ln in lines if ln.strip() and not ln.strip().startswith("#")]
    # de-dup, preserve order
    seen: set[str] = set()
    return [s for s in subjects if not (s in seen or seen.add(s))]


def _summarize(ts: TrainerState) -> str:
    env = ts.stage.task.task_parameters.environment
    n_blocks = len(env.blocks)
    n_patches = len(env.blocks[0].environment.patches)
    return f"stage='{ts.stage.name}' task='{ts.stage.task.name}' blocks={n_blocks} patches/block={n_patches}"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subjects", nargs="*", help="Subject ids (aibs_mouse_id).")
    parser.add_argument("--subjects-file", help="File with one subject id per line ('#' comments allowed).")
    parser.add_argument("--dry-run", action="store_true", help="Build + validate; do not push.")
    parser.add_argument("--yes", action="store_true", help="Skip the confirmation prompt.")
    args = parser.parse_args()

    subjects = _load_subjects(args)
    if not subjects:
        console.print("[red]No subjects provided.[/red] Use --subjects or --subjects-file.")
        return 2

    # Build + validate every trainer state up front (fail fast, before touching the remote).
    console.print(f"Building A/B reversal trainer states for [bold]{len(subjects)}[/bold] subject(s)...")
    states: dict[str, TrainerState] = {}
    for subject in subjects:
        ts = build_trainer_state()  # descriptive name auto-derived from the curriculum knobs
        TrainerState.model_validate_json(ts.model_dump_json())  # round-trip guard
        states[subject] = ts

    table = Table(title="Override plan (A/B reversal)")
    table.add_column("subject")
    table.add_column("trainer_state")
    for subject, ts in states.items():
        table.add_row(subject, _summarize(ts))
    console.print(table)

    if args.dry_run:
        console.print("[yellow]Dry run: nothing pushed.[/yellow]")
        return 0

    if not args.yes:
        console.print(
            f"\n[bold red]This appends new suggestions to Dataverse for {len(subjects)} subject(s).[/bold red]"
        )
        if input("Type 'yes' to proceed: ").strip().lower() != "yes":
            console.print("Aborted.")
            return 1

    client = make_client()
    ok, failed = 0, []
    for subject, ts in states.items():
        try:
            push_override(client, subject, ts)
            console.print(f"[green]pushed[/green] {subject}")
            ok += 1
        except Exception as e:  # keep going; report at the end
            console.print(f"[red]FAILED[/red] {subject}: {e}")
            failed.append(subject)

    console.print(f"\nDone. [green]{ok} ok[/green]" + (f", [red]{len(failed)} failed[/red]: {failed}" if failed else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
