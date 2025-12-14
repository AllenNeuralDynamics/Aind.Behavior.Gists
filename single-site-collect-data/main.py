import json
import logging
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from aind_data_access_api.document_db import MetadataDbClient
from rich.console import Console, Group
from rich.live import Live
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TextColumn
from rich.prompt import Confirm
from rich.table import Table
from s3_utils import download_s3_asset, extract_s3_locations
from datetime import datetime

API_GATEWAY_HOST = "api.allenneuraldynamics.org"

DOWNLOAD_ROOT = Path("assets")
MAX_CONCURRENT_DOWNLOADS = 4


def main() -> None:
    docdb_api_client = MetadataDbClient(
        host=API_GATEWAY_HOST,
    )

    query = {
        "subject.subject_id": {"$in": ["789917", "808619", "808728"]},
        "acquisition.acquisition_start_time": {
            "$gt": str(datetime.fromisoformat("2023-10-01T00:00:00"))
        },
    }
    projection = {
        "name": 1,
        "created": 1,
        "location": 1,
        "subject.subject_id": 1,
        "subject.date_of_birth": 1,
    }
    records = docdb_api_client.retrieve_docdb_records(
        filter_query=query,
        projection=projection,
    )

    logging.info("Retrieved %d records", len(records))
    logging.debug(
        "Records: %s", json.dumps(records, indent=4, sort_keys=True, default=str)
    )

    DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)

    # Collect all S3 URIs first so we can build progress UI and prompt.
    uris: list[str] = []
    for record in records:
        s3_uris = extract_s3_locations(record)
        if not s3_uris:
            continue
        uris.extend(s3_uris)

    if not uris:
        logging.info("No S3 assets found to download.")
        return

    console = Console()

    message = f"About to download {len(uris)} sessions. Proceed?"

    if not Confirm.ask(message, default=False):
        logging.info("Download cancelled by user.")
        return

    statuses: dict[str, str] = {uri: "Not started" for uri in uris}

    progress = Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
    )
    overall_task = progress.add_task("Overall", total=len(uris))

    def render_status_table() -> Table:
        table = Table(show_header=True, header_style="bold")
        table.add_column("Asset")
        table.add_column("Status")

        for uri in uris:
            status = statuses.get(uri, "Not started")
            if status == "Not started":
                style = "dim"
            elif status == "Downloading":
                style = "cyan"
            elif status == "Done":
                style = "green"
            elif status == "Error":
                style = "red"
            else:
                style = "white"

            table.add_row(uri, f"[{style}]{status}[/{style}]")

        return table

    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADS) as executor:
        futures: dict[object, str] = {}

        for uri in uris:
            statuses[uri] = "Downloading"
            future = executor.submit(download_s3_asset, uri, DOWNLOAD_ROOT)
            futures[future] = uri

        with Live(
            Group(progress, render_status_table()),
            console=console,
            refresh_per_second=4,
        ) as live:
            for future in as_completed(futures):
                uri = futures[future]
                try:
                    future.result()
                    statuses[uri] = "Done"
                except subprocess.CalledProcessError as exc:
                    statuses[uri] = "Error"
                    logging.error("Failed to download %s: %s", uri, exc)
                except Exception as exc:
                    statuses[uri] = "Error"
                    logging.exception("Unexpected error downloading %s: %s", uri, exc)

                progress.update(overall_task, advance=1)
                live.update(Group(progress, render_status_table()))


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    main()
