import argparse
import json
import logging
from pathlib import Path

import requests
from codeocean import CodeOcean
from codeocean.models.computation import Computation, ComputationState
from codeocean.models.folder import FileURLs, Folder, FolderItem
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

from utils import get_codeocean_client

# Hard-coded root directory for downloads
DOWNLOAD_ROOT = Path(r"C:\data\codeocean_downloads")
DEFAULT_MAX_FILE_SIZE_MB = 50
DEFAULT_FORCE_DOWNLOAD = False


def load_jobs_from_json(jobs_file: Path) -> dict[str, str]:
    """
    Load jobs from a jobs.json file and return a dict of job_key -> computation_id.

    Args:
        jobs_file: Path to the jobs.json file

    Returns:
        Dictionary mapping job key to computation ID
    """
    with open(jobs_file, "r") as f:
        data = json.load(f)

    jobs_dict: dict[str, str] = {}

    if "jobs" in data:
        for job_key, job_info in data["jobs"].items():
            computation_id = job_info.get("computation_id")
            if computation_id:
                jobs_dict[job_key] = computation_id

    return jobs_dict


def check_computation_status(
    co_client: CodeOcean, computation_id: str
) -> tuple[ComputationState | None, bool]:
    """
    Check the status of a computation.

    Args:
        co_client: Code Ocean client
        computation_id: The computation ID to check

    Returns:
        Tuple of (state, has_results)
    """
    try:
        computation: Computation = co_client.computations.get_computation(
            computation_id
        )
        state: ComputationState | None = computation.state
        has_results: bool = computation.has_results or False
        return state, has_results
    except Exception as e:
        logging.error("Failed to get status for computation %s: %s", computation_id, e)
        return None, False


def list_all_files(
    co_client: CodeOcean, computation_id: str, path: str = ""
) -> list[FolderItem]:
    """
    Recursively list all files in a computation result.
    Returns a list of FolderItem objects representing files (not directories).
    """
    files: list[FolderItem] = []

    try:
        results: Folder = co_client.computations.list_computation_results(
            computation_id=computation_id, path=path
        )

        for item in results.items:
            item_path: str = item.path
            item_size: int | None = item.size

            # Check if item is a directory (size is None or 0)
            if item_size is None or item_size == 0:
                sub_files = list_all_files(co_client, computation_id, item_path)
                files.extend(sub_files)
            else:
                files.append(item)

    except Exception as e:
        logging.debug("Error listing path %s: %s", path, e)

    return files


def download_file(
    url: str, dest_path: Path, progress: Progress, task_id: int | None = None
) -> None:
    """Download a file from URL to destination path with progress tracking."""
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    response = requests.get(url, stream=True, timeout=30)
    response.raise_for_status()

    total_size = int(response.headers.get("Content-Length", 0))

    if task_id is not None and total_size > 0:
        progress.update(task_id, total=total_size)  # type: ignore[arg-type]

    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)
                if task_id is not None:
                    progress.update(task_id, advance=len(chunk))  # type: ignore[arg-type]


def download_job(
    co_client: CodeOcean,
    job_id: str,
    max_file_size_mb: float | None = DEFAULT_MAX_FILE_SIZE_MB,
    force_download: bool = DEFAULT_FORCE_DOWNLOAD,
) -> bool:
    """
    Download all files for a given Code Ocean job_id, preserving folder structure.

    Args:
        co_client: Code Ocean client
        job_id: The Code Ocean computation ID to download results from
        max_file_size_mb: Maximum file size in MB to download (None for no limit)
        force_download: If True, download files even if they already exist locally

    Returns:
        True if download was successful, False otherwise
    """
    logging.info("Retrieving computation information for job_id: %s", job_id)

    try:
        computation: Computation = co_client.computations.get_computation(job_id)
        state: ComputationState | None = computation.state
        has_results: bool = computation.has_results or False

        logging.info(
            "Computation found: %s (state: %s, has_results: %s)",
            job_id,
            state,
            has_results,
        )

        # Check if computation is completed
        if state and state.value not in ["completed", "failed", "stopped"]:
            logging.warning(
                "Computation %s is not in a terminal state (current state: %s). Skipping.",
                job_id,
                state.value,
            )
            return False

        if not has_results:
            logging.warning(
                "Computation %s has no results to download. Skipping.", job_id
            )
            return False

    except Exception as e:
        logging.error("Failed to retrieve computation %s: %s", job_id, e)
        return False

    # List all files in the computation results
    logging.info("Scanning computation results...")
    all_files: list[FolderItem] = list_all_files(co_client, job_id)

    if not all_files:
        logging.warning("No files found in computation results for job_id: %s", job_id)
        return False

    # Create download directory
    job_download_dir = DOWNLOAD_ROOT / job_id
    job_download_dir.mkdir(parents=True, exist_ok=True)

    # Filter files by size and existence if specified
    files_to_download: list[FolderItem] = []
    skipped_files: list[tuple[str, float]] = []
    existing_files: list[tuple[str, float]] = []

    for file_item in all_files:
        path: str = file_item.path
        size: int | None = file_item.size

        # Skip if size is None (shouldn't happen for files, but be safe)
        if size is None:
            logging.warning("File %s has no size information, skipping", path)
            continue

        file_size_mb: float = size / (1024 * 1024)

        if max_file_size_mb is not None and file_size_mb > max_file_size_mb:
            skipped_files.append((path, file_size_mb))
            logging.info(
                "Skipping %s: size %.2f MB exceeds limit of %.2f MB",
                path,
                file_size_mb,
                max_file_size_mb,
            )
            continue

        # Check if file already exists
        rel_path: str = path.lstrip("/")
        dest_file_path: Path = job_download_dir / rel_path
        if dest_file_path.exists() and not force_download:
            existing_files.append((path, file_size_mb))
            logging.info("File already exists, skipping: %s", path)
        else:
            files_to_download.append(file_item)

    console = Console()

    # Show summary
    console.print("\n[bold cyan]Download Summary[/bold cyan]")
    console.print(f"Job ID: {job_id}")
    console.print(f"Total files found: {len(all_files)}")
    console.print(f"Files to download: {len(files_to_download)}")
    console.print(f"Files skipped (size): {len(skipped_files)}")
    console.print(f"Files already exist: {len(existing_files)}")

    if files_to_download:
        console.print("\n[green]Files to download:[/green]")
        download_table = Table(show_header=True, header_style="bold green")
        download_table.add_column("File Path", style="cyan", no_wrap=False)
        download_table.add_column("Size (MB)", justify="right", style="green")

        for file_item in files_to_download[:20]:  # Show first 20
            file_path = file_item.path
            file_size_mb = (file_item.size or 0) / (1024 * 1024)
            download_table.add_row(file_path, f"{file_size_mb:.2f}")

        if len(files_to_download) > 20:
            download_table.add_row("...", f"... and {len(files_to_download) - 20} more")

        console.print(download_table)

    if skipped_files:
        console.print("\n[yellow]Skipped files:[/yellow]")
        skipped_table = Table(show_header=True, header_style="bold yellow")
        skipped_table.add_column("File Path", style="dim", no_wrap=False)
        skipped_table.add_column("Size (MB)", justify="right", style="yellow")

        for file_path, size_mb in skipped_files[:10]:  # Show first 10
            skipped_table.add_row(file_path, f"{size_mb:.2f}")

        if len(skipped_files) > 10:
            skipped_table.add_row("...", f"... and {len(skipped_files) - 10} more")

        console.print(skipped_table)

    if existing_files:
        console.print("\n[blue]Files already exist (skipped):[/blue]")
        existing_table = Table(show_header=True, header_style="bold blue")
        existing_table.add_column("File Path", style="dim", no_wrap=False)
        existing_table.add_column("Size (MB)", justify="right", style="blue")

        for file_path, size_mb in existing_files[:10]:  # Show first 10
            existing_table.add_row(file_path, f"{size_mb:.2f}")

        if len(existing_files) > 10:
            existing_table.add_row("...", f"... and {len(existing_files) - 10} more")

        console.print(existing_table)

    if not files_to_download:
        logging.info("No files to download after filtering.")
        return True

    total_size_mb: float = sum(
        (item.size or 0) / (1024 * 1024) for item in files_to_download
    )

    size_filter_msg = (
        f" (excluding files > {max_file_size_mb} MB)"
        if max_file_size_mb is not None
        else ""
    )
    force_msg = " (force download enabled)" if force_download else ""
    message = (
        f"\nAbout to download {len(files_to_download)} file(s) "
        f"({total_size_mb:.2f} MB){size_filter_msg}{force_msg}\n"
        f"Download root: {DOWNLOAD_ROOT}\n"
        f"Proceed?"
    )

    if not Confirm.ask(message, default=False):
        logging.info("Download cancelled by user.")
        return False

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

        for file_item in files_to_download:
            file_path: str = file_item.path
            file_size: int = file_item.size or 0

            # Get download URL
            try:
                url_response: FileURLs = co_client.computations.get_result_file_urls(
                    computation_id=job_id, path=file_path
                )
                download_url: str = url_response.download_url
            except Exception as e:
                logging.error("Failed to get URL for %s: %s", file_path, e)
                progress.update(overall_task, advance=1)
                continue

            # Determine local path (preserve structure from Code Ocean)
            # Remove leading slash and create path relative to job directory
            relative_path_str: str = file_path.lstrip("/")
            dest_path: Path = job_download_dir / relative_path_str

            # Create file task
            file_task = progress.add_task(
                f"[green]{relative_path_str}", total=file_size, visible=True
            )

            try:
                download_file(download_url, dest_path, progress, file_task)
                logging.info("Downloaded: %s", relative_path_str)
            except Exception as e:
                logging.error("Failed to download %s: %s", relative_path_str, e)
            finally:
                progress.remove_task(file_task)
                progress.update(overall_task, advance=1)

    console.print(
        f"\n[bold green]✓[/bold green] Download complete. Files saved to: {job_download_dir}"
    )

    return True


def main(
    job_id: str | None = None,
    jobs_file: Path | None = None,
    max_file_size_mb: float | None = DEFAULT_MAX_FILE_SIZE_MB,
    force_download: bool = DEFAULT_FORCE_DOWNLOAD,
) -> None:
    """
    Download files from Code Ocean computations.

    Args:
        job_id: Single job ID to download (mutually exclusive with jobs_file)
        jobs_file: Path to jobs.json file containing multiple jobs (mutually exclusive with job_id)
        max_file_size_mb: Maximum file size in MB to download (None for no limit)
        force_download: If True, download files even if they already exist locally
    """
    logging.info("Initializing Code Ocean client...")
    co_client = get_codeocean_client()

    # Determine which mode we're in
    if jobs_file is not None:
        # Batch mode: process jobs from file
        logging.info("Loading jobs from file: %s", jobs_file)

        if not jobs_file.exists():
            logging.error("Jobs file not found: %s", jobs_file)
            return

        jobs_dict = load_jobs_from_json(jobs_file)

        if not jobs_dict:
            logging.error("No jobs found in %s", jobs_file)
            return

        logging.info("Found %d job(s) in %s", len(jobs_dict), jobs_file)

        # Check status of all jobs first
        console = Console()
        console.print("\n[bold cyan]Checking job status...[/bold cyan]")

        status_table = Table(show_header=True, header_style="bold cyan")
        status_table.add_column("Job Key", style="cyan")
        status_table.add_column("Computation ID", style="dim")
        status_table.add_column("State", style="white")
        status_table.add_column("Has Results", justify="center")
        status_table.add_column("Action", style="white")

        jobs_to_download: list[tuple[str, str]] = []

        for job_key, computation_id in jobs_dict.items():
            state, has_results = check_computation_status(co_client, computation_id)

            if state is None:
                status_table.add_row(
                    job_key, computation_id, "[red]Error[/red]", "?", "[red]Skip[/red]"
                )
            elif state.value in ["completed", "failed", "stopped"] and has_results:
                status_table.add_row(
                    job_key,
                    computation_id,
                    f"[green]{state.value}[/green]",
                    "[green]✓[/green]",
                    "[green]Download[/green]",
                )
                jobs_to_download.append((job_key, computation_id))
            else:
                action = (
                    "[yellow]Skip (no results)[/yellow]"
                    if not has_results
                    else f"[yellow]Skip ({state.value})[/yellow]"
                )
                status_table.add_row(
                    job_key,
                    computation_id,
                    f"[yellow]{state.value if state else 'unknown'}[/yellow]",
                    "[yellow]✗[/yellow]" if not has_results else "[yellow]?[/yellow]",
                    action,
                )

        console.print(status_table)
        console.print(
            f"\n[bold]Jobs ready to download: {len(jobs_to_download)} / {len(jobs_dict)}[/bold]"
        )

        if not jobs_to_download:
            logging.info("No jobs are ready to download.")
            return

        # Download each completed job
        success_count = 0
        for idx, (job_key, computation_id) in enumerate(jobs_to_download, 1):
            console.print(f"\n[bold cyan]{'=' * 80}[/bold cyan]")
            console.print(
                f"[bold cyan]Processing job {idx}/{len(jobs_to_download)}: {job_key}[/bold cyan]"
            )
            console.print(f"[bold cyan]{'=' * 80}[/bold cyan]\n")

            success = download_job(
                co_client, computation_id, max_file_size_mb, force_download
            )

            if success:
                success_count += 1

        console.print(f"\n[bold green]{'=' * 80}[/bold green]")
        console.print(
            f"[bold green]Batch download complete: {success_count}/{len(jobs_to_download)} jobs downloaded successfully[/bold green]"
        )
        console.print(f"[bold green]{'=' * 80}[/bold green]")

    elif job_id is not None:
        # Single job mode
        download_job(co_client, job_id, max_file_size_mb, force_download)
    else:
        logging.error("Must provide either --job-id or --jobs-file")
        return


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )

    parser = argparse.ArgumentParser(
        description="Download all files for Code Ocean computation(s)"
    )

    # Mutually exclusive group for job ID or jobs file
    job_group = parser.add_mutually_exclusive_group(required=True)
    job_group.add_argument(
        "--job-id",
        type=str,
        help="The Code Ocean computation ID to download results from",
    )
    job_group.add_argument(
        "--jobs-file",
        type=Path,
        help="Path to jobs.json file containing multiple jobs to download",
    )
    parser.add_argument(
        "--max-size-mb",
        type=float,
        default=DEFAULT_MAX_FILE_SIZE_MB,
        help=f"Maximum file size in MB to download (default: {DEFAULT_MAX_FILE_SIZE_MB} MB, use 0 for no limit)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force download even if files already exist locally",
    )

    args = parser.parse_args()

    max_size: float | None = None if args.max_size_mb <= 0 else args.max_size_mb

    main(args.job_id, args.jobs_file, max_size, args.force)
