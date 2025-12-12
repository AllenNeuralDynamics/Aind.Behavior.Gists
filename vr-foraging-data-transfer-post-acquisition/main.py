from pathlib import Path
import dataclasses
import re
from aind_behavior_vr_foraging.cli import DataMapperCli
from aind_behavior_vr_foraging.data_contract import dataset
from datetime import datetime
import logging
from datetime import timezone

logger = logging.getLogger(__name__)

target_folder = r"\\allen\aind\stage\vr-foraging\quarantined"
FORCE_METADATA_REGEN = False


@dataclasses.dataclass
class SessionInfo:
    session_id: str
    date: datetime
    subject: int
    session_directory: Path

    @classmethod
    def from_path(cls, path: Path) -> "SessionInfo":
        # Example string: "789917_2025-12-02T205233Z"
        match = re.match(r"([^_]+)_([^_]+)", path.name)
        if not match:
            raise ValueError(f"Invalid session string format: {path.name}")
        subject = match.group(1)
        date_str = match.group(2)
        date = datetime.fromisoformat(date_str)
        session_id = path.name
        return cls(
            session_id=session_id,
            date=date,
            subject=int(subject),
            session_directory=path.resolve(),
        )


def get_last_log_timestamp(path: Path) -> datetime | None:
    """
    Returns the last log timestamp as a timezone-aware datetime object (UTC).
    """
    timestamp_re = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{6}Z)")

    with open(path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    for line in reversed(lines):
        m = timestamp_re.match(line)
        if m:
            dt = datetime.strptime(m.group(1), "%Y-%m-%dT%H%M%SZ")
            return dt.replace(tzinfo=timezone.utc)

    return None


def main():
    for session_dir in Path(target_folder).iterdir():
        if not session_dir.is_dir():
            continue

        try:
            session_info = SessionInfo.from_path(session_dir)
        except ValueError as e:
            print(f"Skipping invalid session directory {session_dir.name}: {e}")
            continue

        if (
            Path(session_dir, "acquisition.json").exists()
            and Path(session_dir, "instrument.json").exists()
            and not FORCE_METADATA_REGEN
        ):
            logger.info(
                f"Session {session_info.session_id} already has acquisition.json and instrument.json, skipping."
            )

        else:
            logging.info(
                f"Processing and creating metadata for session {session_info.session_id}..."
            )
            this_dataset = dataset(session_info.session_directory)
            try:
                launcher = this_dataset["Behavior"]["Logs"][
                    "Launcher"
                ].reader_params.path
                end_of_session_time = get_last_log_timestamp(launcher)
            except Exception as e:
                logger.error(
                    f"Failed to load EndSession log for {session_info.session_id}: {e}"
                )
                end_of_session_time = None
            DataMapperCli(
                data_path=session_info.session_directory,
                repo_path=Path("./Aind.Behavior.VrForaging"),
                session_end_time=end_of_session_time,
            ).cli_cmd()


if __name__ == "__main__":
    main()
