"""Override plumbing: authenticate to Dataverse and append a suggestion for a subject.

Because the Dataverse suggestions table is append-only and the rig picker reads back only the
newest row, appending a fresh row IS the override. We use clabe's low-level
`_append_suggestion` directly so we don't need a Launcher/session per subject.

⚠️ `_append_suggestion` / `_DataverseRestClient` are clabe-internal (underscore) APIs and may
change across versions — keep `clabe` pinned in pyproject.toml.
"""

from __future__ import annotations

from pathlib import Path

from aind_behavior_curriculum import TrainerState
from clabe.pickers.dataverse import (  # type: ignore[attr-defined]
    _append_suggestion,
    _DataverseRestClient,
    _DataverseRestClientSettings,
)
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

# Where explicit credentials live. Keep out of git (the repo `secrets/` folder is gitignored).
_SECRETS_FILE = Path(__file__).resolve().parent.parent / "secrets" / "dataverse"


class DataverseSecrets(BaseSettings):
    """Explicit Dataverse credentials, loaded from env vars or `../secrets/dataverse`.

    Precedence: constructor args > `DATAVERSE_*` env vars > the secrets dotenv file.
    Expected keys (env var name in parentheses):
        tenant_id   (DATAVERSE_TENANT_ID)
        client_id   (DATAVERSE_CLIENT_ID)
        org         (DATAVERSE_ORG)
        username    (DATAVERSE_USERNAME)
        password    (DATAVERSE_PASSWORD)
        domain      (DATAVERSE_DOMAIN, optional)
    """

    model_config = SettingsConfigDict(
        env_prefix="DATAVERSE_",
        env_file=str(_SECRETS_FILE),
        env_file_encoding="utf-8",
        extra="ignore",
    )

    tenant_id: str
    client_id: str
    org: str
    username: str
    password: SecretStr
    domain: str = "alleninstitute.org"


def make_client(secrets: DataverseSecrets | None = None) -> _DataverseRestClient:
    """Build a Dataverse REST client from explicit credentials (not KeePass)."""
    secrets = secrets or DataverseSecrets()  # type: ignore[call-arg]
    settings = _DataverseRestClientSettings(
        tenant_id=secrets.tenant_id,
        client_id=secrets.client_id,
        org=secrets.org,
        username=secrets.username,
        password=secrets.password,
        domain=secrets.domain,
    )
    return _DataverseRestClient(settings)


def push_override(client: _DataverseRestClient, subject_id: str, trainer_state: TrainerState) -> None:
    """Append `trainer_state` as the newest suggestion for `subject_id`."""
    _append_suggestion(client, subject_id, trainer_state)
