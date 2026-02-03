import logging
from pathlib import Path
from typing import Any, cast

from aind_behavior_services.calibration.aind_manipulator import ManipulatorPosition
from aind_behavior_services.session import AindBehaviorSessionModel
from aind_behavior_services.utils import utcnow
from clabe.apps import (
    CurriculumApp,
    CurriculumSettings,
    CurriculumSuggestion,
)
import glob

from clabe.data_transfer.aind_watchdog import (
    WatchdogDataTransferService,
    WatchdogSettings,
)
from clabe.launcher import Launcher, LauncherCliArgs
from clabe.pickers import ByAnimalModifier, DefaultBehaviorPickerSettings
from clabe.pickers.dataverse import DataversePicker
from contraqctor.contract.json import SoftwareEvents
from pydantic_settings import CliApp

from aind_behavior_vr_foraging import data_contract
from aind_behavior_vr_foraging.data_mappers import DataMapperCli
from aind_behavior_vr_foraging.rig import AindVrForagingRig
from aind_behavior_vr_foraging.task_logic import AindVrForagingTaskLogic

logger = logging.getLogger(__name__)

sessions = [
    "828426_2026-02-03T001916Z",
    "841306_2026-02-03T002112Z",
    "828415_2026-02-03T002024Z",
]
root = "c:/"


async def experiment(launcher: Launcher) -> None:
    # Start experiment setup
    picker = DataversePicker(
        launcher=launcher, settings=DefaultBehaviorPickerSettings()
    )

    # Pick and register session
    with open(
        root + sessions[0] + r"behavior\Logs\session_input.json", "r", encoding="utf-8"
    ) as f:
        deserialized_session = AindBehaviorSessionModel.model_validate_json(f.read())
        deserialized_session.root_path = Path(root)
    session = deserialized_session
    picker._session = deserialized_session

    launcher.register_session(picker.session)

    # Fetch the task settings
    with open(
        root + sessions[0] + r"behavior\Logs\tasklogic_input.json",
        "r",
        encoding="utf-8",
    ) as f:
        deserialized_task_logic = AindVrForagingTaskLogic.model_validate_json(f.read())

    trainer_state_files = glob.glob(
        root + sessions[0] + r"behavior\Logs\TrainerState_*.json"
    )
    if trainer_state_files:
        with open(trainer_state_files[0], "r", encoding="utf-8") as f:
            deserialized_trainer_state = CurriculumSuggestion.model_validate_json(
                f.read()
            )
    else:
        raise FileNotFoundError("Trainer state file not found.")

    trainer_state, task_logic = (deserialized_trainer_state, deserialized_task_logic)
    input_trainer_state_path = Path(trainer_state_files[0])

    # Fetch rig settings
    with open(
        root + sessions[0] + r"behavior\Logs\rig_input.json", "r", encoding="utf-8"
    ) as f:
        deserialized_rig = AindVrForagingRig.model_validate_json(f.read())
    rig = deserialized_rig

    # Curriculum
    suggestion: CurriculumSuggestion | None = None
    suggestion_path: Path | None = None
    if not (
        (picker.trainer_state is None)
        or (picker.trainer_state.is_on_curriculum is False)
        or (picker.trainer_state.stage is None)
    ):
        trainer = CurriculumApp(
            settings=CurriculumSettings(
                input_trainer_state=input_trainer_state_path.resolve(),
                data_directory=launcher.session_directory,
            )
        )
        # Run the curriculum
        await trainer.run_async()
        suggestion = trainer.process_suggestion()
        # Dump suggestion for debugging (for now, but we will prob remove this later)
        suggestion_path = _dump_suggestion(suggestion, launcher.session_directory)
        # Push updated trainer state back to the database
        picker.push_new_suggestion(suggestion.trainer_state)

    # Mappers
    assert launcher.repository.working_tree_dir is not None

    DataMapperCli(
        data_path=launcher.session_directory,
        repo_path=launcher.repository.working_tree_dir,  # type: ignore[arg-type]
        curriculum_suggestion=suggestion_path,
        session_end_time=utcnow(),
    ).cli_cmd()

    # Watchdog
    is_transfer = picker.ui_helper.prompt_yes_no_question(
        "Would you like to transfer data?"
    )
    if not is_transfer:
        logger.info("Data transfer skipped by user.")
        return

    launcher.copy_logs()
    watchdog_settings = WatchdogSettings()
    watchdog_settings.destination = (
        Path(watchdog_settings.destination) / launcher.session.subject
    )
    WatchdogDataTransferService(
        source=launcher.session_directory,
        settings=watchdog_settings,
        session=session,
    ).transfer()

    return


def _dump_suggestion(suggestion: CurriculumSuggestion, session_directory: Path) -> Path:
    logger.info(
        f"Dumping curriculum suggestion to: {session_directory / 'Behavior' / 'Logs' / 'suggestion.json'}"
    )
    suggestion_path = session_directory / "Behavior" / "Logs" / "suggestion.json"
    with open(suggestion_path, "w", encoding="utf-8") as f:
        f.write(suggestion.model_dump_json(indent=2))
    return suggestion_path


class ByAnimalManipulatorModifier(ByAnimalModifier[AindVrForagingRig]):
    """Modifier to set and update manipulator initial position based on animal-specific data."""

    def __init__(
        self,
        subject_db_path: Path,
        model_path: str,
        model_name: str,
        *,
        launcher: Launcher,
        **kwargs,
    ) -> None:
        super().__init__(subject_db_path, model_path, model_name, **kwargs)
        self._launcher = launcher

    def _process_before_dump(self) -> ManipulatorPosition:
        _dataset = data_contract.dataset(self._launcher.session_directory)
        manipulator_parking_position: SoftwareEvents = cast(
            SoftwareEvents,
            _dataset["Behavior"]["SoftwareEvents"]["SpoutParkingPositions"].load(),
        )
        data: dict[str, Any] = manipulator_parking_position.data.iloc[0]["data"][
            "ResetPosition"
        ]
        position = ManipulatorPosition.model_validate(data)
        return position


class ClabeCli(LauncherCliArgs):
    def cli_cmd(self):
        launcher = Launcher(settings=self)
        launcher.run_experiment(experiment)
        return None


def main() -> None:
    CliApp().run(ClabeCli)


if __name__ == "__main__":
    main()
