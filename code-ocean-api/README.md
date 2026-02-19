# Code Ocean API Tools

Tools for interacting with Code Ocean capsules and downloading computation results.

## Setup

1. Install dependencies:
```bash
uv sync
```

2. Add your Code Ocean API key to `../secrets/codeocean`

## Usage

### Submit Batch Jobs (`main.py`)

Submit multiple parametric runs to a Code Ocean capsule and monitor their status.

```bash
uv run main.py
```

Job information is saved to `jobs.json`.

### Download Results (`get_url.py`)

Download a single job:
```bash
uv run get_url.py --job-id <computation_id>
```

Download all completed jobs from `jobs.json`:
```bash
uv run get_url.py --jobs-file jobs.json
```

Options:
- `--max-size-mb N` - Skip files larger than N MB (default: 50)
- `--force` - Re-download files that already exist
- `--max-size-mb 0` - No size limit

The script automatically skips jobs that are still running or don't have results yet.

Files are downloaded to `C:\data\codeocean_downloads\<job_id>\` with the original folder structure preserved.

## Notes

- `get_url.py` checks job status before downloading - only completed jobs with results are processed
- Existing files are skipped by default unless `--force` is used
- Large files (videos, models) can be excluded with `--max-size-mb`
