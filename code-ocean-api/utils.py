import os
from pathlib import Path

from codeocean import CodeOcean

CODEOCEAN_DOMAIN = "https://codeocean.allenneuraldynamics.org"
_SECRETS_FILE = Path("../secrets/codeocean")


def get_codeocean_client() -> CodeOcean:
    """Initialize Code Ocean client.

    Resolves the API token in order:
    1. ``CODEOCEAN_TOKEN`` environment variable
    2. ``../secrets/codeocean`` file (fallback)
    """
    token = os.environ.get("CODEOCEAN_TOKEN")
    if token is None:
        token = _SECRETS_FILE.read_text().strip()
    return CodeOcean(domain=CODEOCEAN_DOMAIN, token=token)
