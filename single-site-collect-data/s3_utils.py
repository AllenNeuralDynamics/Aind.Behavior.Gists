import logging
import subprocess
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlparse


def check_aws_cli_exists() -> None:
    """Check if AWS CLI is installed and available in PATH."""
    try:
        subprocess.run(
            ["aws", "--version"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except (subprocess.CalledProcessError, FileNotFoundError):
        raise RuntimeError(
            "AWS CLI is not installed or not found in PATH. Please install it to proceed."
        )


check_aws_cli_exists()


def extract_s3_locations(record: Dict[str, Any]) -> List[str]:
    """Best-effort extraction of S3 locations from a record's `location` field."""

    locations: List[str] = []
    raw_location = record.get("location")

    def handle_one(item: Any) -> None:
        if isinstance(item, str) and item.startswith("s3://"):
            locations.append(item)
            return

        if isinstance(item, dict):
            uri = None
            if "s3_uri" in item and isinstance(item["s3_uri"], str):
                uri = item["s3_uri"]
            elif "bucket" in item and "key" in item:
                bucket = item["bucket"]
                key = item["key"]
                if isinstance(bucket, str) and isinstance(key, str):
                    uri = f"s3://{bucket}/{key}"

            if uri and isinstance(uri, str) and uri.startswith("s3://"):
                locations.append(uri)

    if isinstance(raw_location, list):
        for sub in raw_location:
            if isinstance(sub, list):
                for subsub in sub:
                    handle_one(subsub)
            else:
                handle_one(sub)
    else:
        handle_one(raw_location)

    return locations


def download_s3_asset(s3_uri: str, output_root: Path) -> None:
    """Download a folder-like S3 asset using aws s3 sync."""

    parsed = urlparse(s3_uri)
    if parsed.scheme != "s3":
        logging.warning("Skipping non-s3 URI: %s", s3_uri)
        return

    prefix_name = Path(parsed.path).name or parsed.netloc
    dest_dir = output_root / prefix_name
    dest_dir.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "aws",
        "s3",
        "sync",
        s3_uri,
        str(dest_dir),
        "--exclude",
        "Behavior-Videos/*",
        "--no-progress",
        "--only-show-errors",
    ]

    logging.info("Starting download: %s -> %s", s3_uri, dest_dir)
    subprocess.run(
        cmd,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
