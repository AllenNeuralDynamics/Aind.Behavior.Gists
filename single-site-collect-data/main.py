import json
import logging
import subprocess
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

from aind_data_access_api.document_db import MetadataDbClient
from s3_utils import download_s3_asset, extract_s3_locations

API_GATEWAY_HOST = "api.allenneuraldynamics.org"

DOWNLOAD_ROOT = Path("assets")
MAX_CONCURRENT_DOWNLOADS = 4


def main() -> None:
    docdb_api_client = MetadataDbClient(
        host=API_GATEWAY_HOST,
    )

    query = {"subject.subject_id": {"$in": ["789917", "808619", "808728"]}}
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

    download_futures = []
    with ThreadPoolExecutor(max_workers=MAX_CONCURRENT_DOWNLOADS) as executor:
        for record in records:
            s3_uris = extract_s3_locations(record)
            if not s3_uris:
                continue

            for uri in s3_uris:
                future = executor.submit(download_s3_asset, uri, DOWNLOAD_ROOT)
                download_futures.append((uri, future))

        for uri, future in download_futures:
            try:
                future.result()
                logging.info("Completed download: %s", uri)
            except subprocess.CalledProcessError as exc:
                logging.error("Failed to download %s: %s", uri, exc)
            except Exception as exc:
                logging.exception("Unexpected error downloading %s: %s", uri, exc)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    main()
