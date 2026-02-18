import argparse
import logging
from pathlib import Path
from typing import Optional

import requests
from codeocean import CodeOcean
from rich.console import Console
from rich.progress import (
    BarColumn,
    DownloadColumn,
    Progress,
    TextColumn,
    TimeRemainingColumn,
    TransferSpeedColumn,
)
from rich.prompt import Confirm
from rich.table import Table

# Hard-coded root directory for downloads
DOWNLOAD_ROOT = Path(r"C:\data\codeocean_downloads")
DEFAULT_MAX_FILE_SIZE_MB = 50


def get_codeocean_client() -> CodeOcean:
    """Initialize Code Ocean client with API key from secrets."""
    co_api_key = Path("../secrets/codeocean").read_text().strip()
    co_client = CodeOcean(
        domain="https://codeocean.allenneuraldynamics.org", token=co_api_key
    )
    return co_client


def get_file_size_mb(url: str) -> Optional[float]:
    """Get the size of a file from URL headers in MB."""
    try:
        response = requests.head(url, allow_redirects=True, timeout=10)
        if "Content-Length" in response.headers:
            size_bytes = int(response.headers["Content-Length"])
            return size_bytes / (1024 * 1024)  # Convert to MB
        return None
    except Exception as e:
        logging.warning("Could not determine size for %s: %s", url, e)
        return None


def list_all_files(
    co_client: CodeOcean, computation_id: str, path: str = ""
) -> list[dict]:
    """
    Recursively list all files in a computation result.
    Returns a list of dicts with 'path' and 'size' keys.
    """
    files = []

    try:
        results = co_client.computations.list_computation_results(
            computation_id=computation_id, path=path
        )

        for item in results.items:
            item_path = item.path
            item_size = getattr(item, "size", 0)

            # Check if item is a directory (no size or has children)
            if item_size == 0 or not hasattr(item, "size"):
                # Recursively explore directories
                sub_files = list_all_files(co_client, computation_id, item_path)
                files.extend(sub_files)
            else:
                # It's a file
                files.append({"path": item_path, "size": item_size})

    except Exception as e:
        logging.debug("Error listing path %s: %s", path, e)

    return files


def download_file(
    url: str, dest_path: Path, progress: Progress, task_id: Optional[int] = None
) -> None:
    """Download a file from URL to destination path with progress tracking."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(url, stream=True, timeout=30)
    response.raise_for_status()

    total_size = int(response.headers.get("Content-Length", 0))

    if task_id is not None and total_size > 0:
        progress.update(task_id, total=total_size)

    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                if task_id is not None:
                    progress.update(task_id, advance=len(chunk))


def main(
    job_id: str, max_file_size_mb: Optional[float] = DEFAULT_MAX_FILE_SIZE_MB
) -> None:
    """
    Download all files for a given Code Ocean job_id, preserving folder structure.

    Args:
        job_id: The Code Ocean computation ID to download results from
        max_file_size_mb: Maximum file size in MB to download (None for no limit)
    """
    logging.info("Initializing Code Ocean client...")
    co_client = get_codeocean_client()

    logging.info("Retrieving computation information for job_id: %s", job_id)

    try:
        computation = co_client.computations.get_computation(job_id)
        logging.info("Computation found: %s", computation)
    except Exception as e:
        logging.error("Failed to retrieve computation %s: %s", job_id, e)
        return

    # List all files in the computation results
    logging.info("Scanning computation results...")
    all_files = list_all_files(co_client, job_id)

    if not all_files:
        logging.warning("No files found in computation results for job_id: %s", job_id)
        return

    # Filter files by size if specified
    files_to_download = []
    skipped_files = []

    for file_info in all_files:
        file_path = file_info["path"]
        file_size = file_info["size"]
        file_size_mb = file_size / (1024 * 1024) if file_size else 0

        if max_file_size_mb is not None and file_size_mb > max_file_size_mb:
            skipped_files.append((file_path, file_size_mb))
            logging.info(
                "Skipping %s: size %.2f MB exceeds limit of %.2f MB",
                file_path,
                file_size_mb,
                max_file_size_mb,
            )
        else:
            files_to_download.append(file_info)

    console = Console()

    # Show summary
    console.print("\n[bold cyan]Download Summary[/bold cyan]")
    console.print(f"Job ID: {job_id}")
    console.print(f"Total files found: {len(all_files)}")
    console.print(f"Files to download: {len(files_to_download)}")
    console.print(f"Files skipped (size): {len(skipped_files)}")

    if skipped_files:
        console.print("\n[yellow]Skipped files:[/yellow]")
        table = Table(show_header=True, header_style="bold yellow")
        table.add_column("File Path", style="dim")
        table.add_column("Size (MB)", justify="right")

        for file_path, size_mb in skipped_files[:10]:  # Show first 10
            table.add_row(file_path, f"{size_mb:.2f}")

        if len(skipped_files) > 10:
            table.add_row("...", f"... and {len(skipped_files) - 10} more")

        console.print(table)

    if not files_to_download:
        logging.info("No files to download after filtering.")
        return

    # Calculate total download size
    total_size_mb = sum(f["size"] / (1024 * 1024) for f in files_to_download)

    size_filter_msg = (
        f" (excluding files > {max_file_size_mb} MB)"
        if max_file_size_mb is not None
        else ""
    )
    message = (
        f"\nAbout to download {len(files_to_download)} file(s) "
        f"({total_size_mb:.2f} MB){size_filter_msg}\n"
        f"Download root: {DOWNLOAD_ROOT}\n"
        f"Proceed?"
    )

    if not Confirm.ask(message, default=False):
        logging.info("Download cancelled by user.")
        return

    # Create download directory
    job_download_dir = DOWNLOAD_ROOT / job_id
    job_download_dir.mkdir(parents=True, exist_ok=True)

    # Download files with progress tracking
    console.print("\n[bold green]Downloading files...[/bold green]")

    progress = Progress(
        TextColumn("[bold blue]{task.description}", justify="right"),
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.1f}%",
        "•",
        DownloadColumn(),
        "•",
        TransferSpeedColumn(),
        "•",
        TimeRemainingColumn(),
    )

    with progress:
        overall_task = progress.add_task("[cyan]Overall", total=len(files_to_download))

        for file_info in files_to_download:
            file_path = file_info["path"]
            file_size = file_info["size"]

            # Get download URL
            try:
                url_response = co_client.computations.get_result_file_urls(
                    computation_id=job_id, path=file_path
                )
                download_url = url_response.download_url
            except Exception as e:
                logging.error("Failed to get URL for %s: %s", file_path, e)
                progress.update(overall_task, advance=1)
                continue

            # Determine local path (preserve structure from Code Ocean)
            # Remove leading slash and create path relative to job directory
            relative_path = file_path.lstrip("/")
            dest_path = job_download_dir / relative_path

            # Create file task
            file_task = progress.add_task(
                f"[green]{relative_path}", total=file_size, visible=True
            )

            try:
                download_file(download_url, dest_path, progress, file_task)
                logging.info("Downloaded: %s", relative_path)
            except Exception as e:
                logging.error("Failed to download %s: %s", relative_path, e)
            finally:
                progress.remove_task(file_task)
                progress.update(overall_task, advance=1)

    console.print(
        f"\n[bold green]✓[/bold green] Download complete. Files saved to: {job_download_dir}"
    )


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Download all files for a Code Ocean job_id"
    )
    parser.add_argument(
        "job_id",
        type=str,
        help="The Code Ocean computation ID to download results from",
    )
    parser.add_argument(
        "--max-size-mb",
        type=float,
        default=DEFAULT_MAX_FILE_SIZE_MB,
        help=f"Maximum file size in MB to download (default: {DEFAULT_MAX_FILE_SIZE_MB} MB, use 0 for no limit)",
    )

    args = parser.parse_args()

    max_size = None if args.max_size_mb <= 0 else args.max_size_mb

    main(args.job_id, max_size)
