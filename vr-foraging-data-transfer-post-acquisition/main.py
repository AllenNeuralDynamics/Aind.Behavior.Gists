from pathlib import Path
import dataclasses
import re
from aind_behavior_vr_foraging.cli import DataMapperCli
from aind_behavior_vr_foraging.data_contract import dataset
from aind_behavior_vr_foraging import __semver__
from aind_data_transfer_service.configs.platforms_v1 import Platform

from datetime import datetime
import logging
from datetime import timezone
from clabe.data_transfer import aind_watchdog
import requests

import aind_data_transfer_service.models.core
from pathlib import PurePosixPath
from aind_data_schema.core import acquisition

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

target_folder = r"\\allen\aind\stage\vr-foraging\quarantined"
FORCE_METADATA_REGEN = False

PROJECT_NAME = "Cognitive flexibility in patch foraging"
JOB_TYPE = "vr_foraging"
TRANSFER_ENDPOINT = "http://aind-data-transfer-service/api/v2/submit_jobs"


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
            version = (
                this_dataset["Behavior"]["InputSchemas"]["TaskLogic"]
                .load()
                .data.version
            )
            repo_version = __semver__

            if version != repo_version:
                raise ValueError(
                    "Version mismatch for session %s: dataset version %s != submodule repository version %s. Consider updating the submodule to match the dataset version."
                    % (session_info.session_id, version, repo_version)
                )

            DataMapperCli(
                data_path=session_info.session_directory,
                repo_path=Path("./Aind.Behavior.VrForaging"),
                session_end_time=end_of_session_time,
            ).cli_cmd()

        available_modalities = (
            aind_watchdog.WatchdogDataTransferService._find_modality_candidates(
                session_info.session_directory
            )
        )
        acquisition_json = acquisition.Acquisition.model_validate_json(
            Path(session_info.session_directory, "acquisition.json").read_text(
                encoding="utf-8"
            )
        )
        tasks = {}
        tasks["modality_transformation_settings"] = {
            modality: aind_data_transfer_service.models.core.Task(
                job_settings={
                    "input_source": str(
                        PurePosixPath(session_info.session_directory / modality)
                    )
                }
            )
            for modality in available_modalities.keys()
        }

        tasks["gather_preliminary_metadata"] = (
            aind_data_transfer_service.models.core.Task(
                job_settings={
                    "metadata_dir": str(PurePosixPath(session_info.session_directory))
                }
            )
        )

        upload_job_configs_v2 = (
            aind_data_transfer_service.models.core.UploadJobConfigsV2(
                job_type=JOB_TYPE,
                project_name=PROJECT_NAME,
                platform=Platform.BEHAVIOR,
                modalities=[
                    aind_data_transfer_service.models.core.Modality.from_abbreviation(m)
                    for m in available_modalities.keys()
                ],
                subject_id=str(session_info.subject),
                acq_datetime=acquisition_json.acquisition_start_time,
                tasks=tasks,
            )
        )

        submit_request_v2 = aind_data_transfer_service.models.core.SubmitJobRequestV2(
            upload_jobs=[upload_job_configs_v2],
            user_email=f"{acquisition_json.experimenters[0]}@alleninstitute.org",
        )

        logger.info(
            f"Submitting data transfer job for session {session_info.session_id}..."
        )
        logger.debug(
            f"Submit request: {submit_request_v2.model_dump(mode='json', exclude_none=True)}"
        )

        submit_job_response = requests.post(
            url=TRANSFER_ENDPOINT,
            json=submit_request_v2.model_dump(mode="json", exclude_none=True),
        )
        submit_job_response.raise_for_status()
        logger.info(submit_job_response.json())


if __name__ == "__main__":
    main()
