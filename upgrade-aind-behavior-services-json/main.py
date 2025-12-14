from aind_behavior_vr_foraging.rig import AindVrForagingRig
from pathlib import Path
import pydantic
import logging
import json
from datetime import datetime, timezone
import pytz

logger = logging.getLogger(__name__)
logging.disable(logging.CRITICAL)

## This example will be for rigs only

CONFIGURATION_DIRECTORY = Path(r"\\allen\aind\scratch\AindBehavior.db\AindVrForaging")
RIG_DIRECTORY = CONFIGURATION_DIRECTORY / "Rig"
DRY_RUN = True
DELETE_UNPARSABLE = False

available_rigs = list(RIG_DIRECTORY.iterdir())
print(f"Available rigs: {[d.name for d in available_rigs]}")

for pc in available_rigs:
    jsons = pc.glob("*.json")
    for file in jsons:
        try:
            rig = AindVrForagingRig.model_validate_json(
                file.read_text(encoding="utf-8")
            )
            print(f"Rewriting file: {file}")
            if not DRY_RUN:
                file.write_text(rig.model_dump_json(), encoding="utf-8")
        except pydantic.ValidationError as e:
            logger.error("Validation error for %s: %s", file, e)
            _raw_json = json.loads(file.read_text(encoding="utf-8"))
            print(
                f"Validation error in file: {file}. Date = {_raw_json.get('date', 'N/A')}. Version = {_raw_json.get('version', 'N/A')}"
            )
            file_date_modified_utc = datetime.fromtimestamp(
                file.stat().st_mtime, tz=timezone.utc
            )
            seattle_tz = pytz.timezone("America/Los_Angeles")
            file_date_modified = file_date_modified_utc.astimezone(seattle_tz)
            print(f"File last modified timestamp: {file_date_modified}")
            if DELETE_UNPARSABLE and not DRY_RUN:
                print(f"Deleting unparsable file: {file}")
                file.unlink()
        else:
            logger.info("Successfully loaded rig configuration from %s", file)
