import json
import time
from datetime import datetime, timezone
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict

from codeocean import CodeOcean
from codeocean.computation import NamedRunParam, RunParams

co_api_key = Path("../secrets/codeocean").read_text()
co_client = CodeOcean(
    domain="https://codeocean.allenneuraldynamics.org", token=co_api_key
)
del co_api_key
capsule_id = "2a66df60-f96d-401e-8384-2e4aedeee818"
respt = co_client.capsules.get_capsule(capsule_id)
print(respt)


parameters_to_vary = [
    {
        "learning_rate": 5e-5,
        "batch_size": 256,
    },
    {
        "learning_rate": 1e-4,
        "batch_size": 256,
    },
    {
        "learning_rate": 5e-5,
        "batch_size": 128,
    },
    {
        "learning_rate": 1e-4,
        "batch_size": 128,
    },
    {
        "learning_rate": 5e-4,
        "batch_size": 256,
    },
    {
        "learning_rate": 5e-4,
        "batch_size": 128,
    },
]

jobs: Dict[str, Dict[str, Any]] = {}
batch_submission_time = datetime.now(timezone.utc).isoformat()

print("=" * 80)
print("SUBMITTING JOBS")
print("=" * 80)

for run_idx, run_settings in enumerate(parameters_to_vary):
    param_str = "_".join([f"{k}={v}" for k, v in run_settings.items()])
    job_key = f"run_{run_idx}_{param_str}"
    print(f"Running pipeline with settings: {run_settings}")

    named_params = [
        NamedRunParam(param_name=param_name, value=str(param_value))
        for param_name, param_value in run_settings.items()
    ]
    named_params.append(
        NamedRunParam(param_name="base_output_dir", value=f"/results/npe/{job_key}")
    )

    run_params = RunParams(
        capsule_id=capsule_id,
        named_parameters=named_params,
    )

    try:
        response = co_client.computations.run_capsule(run_params=run_params)
        print(f"Started computation: {response}")

        computation_id = (
            response.get("id") if isinstance(response, dict) else response.id
        )

        jobs[job_key] = {
            "computation_id": computation_id,
            "run_settings": run_settings,
            "status": "submitted",
            "response": response,
        }

        print(f"  -> Job key: {job_key}, Computation ID: {computation_id}")
    except Exception as e:
        print(f"  -> ERROR submitting job {job_key}: {e}")
        jobs[job_key] = {
            "computation_id": None,
            "run_settings": run_settings,
            "status": "submission_failed",
            "error": str(e),
        }

print(f"\nTotal jobs submitted: {len(jobs)}")
print(f"Job keys: {list(jobs.keys())}")

jobs_file = Path("jobs.json")
print(f"\nSaving job information to {jobs_file}...")

jobs_to_save = {}
for job_key, job_info in jobs.items():
    job_data = job_info.copy()
    if "response" in job_data and not isinstance(
        job_data["response"], (dict, str, type(None))
    ):
        job_data["response"] = str(job_data["response"])
    jobs_to_save[job_key] = job_data

batch_data = {"batch_submission_time_utc": batch_submission_time, "jobs": jobs_to_save}

with open(jobs_file, "w") as f:
    json.dump(batch_data, f, indent=2)

print(f"Job information saved to {jobs_file}")

print("\n" + "=" * 80)
print("MONITORING JOB STATUS")
print("=" * 80)

terminal_states = ["completed", "failed", "stopped"]
poll_interval = 10

while True:
    all_done = True

    print(f"\n[{time.strftime('%Y-%m-%d %H:%M:%S')}] Status update:")
    print("-" * 80)

    for job_key, job_info in jobs.items():
        computation_id = job_info.get("computation_id")

        if computation_id is None:
            print(f"  {job_key:30} -> {job_info['status']}")
            continue

        try:
            computation = co_client.computations.get_computation(computation_id)
            state = (
                computation.get("state")
                if isinstance(computation, dict)
                else computation.state
            )

            old_status = job_info["status"]
            job_info["status"] = state

            status_change = "" if old_status == state else f" (was: {old_status})"
            print(f"  {job_key:30} -> {state}{status_change}")

            if state.lower() not in terminal_states:
                all_done = False

        except Exception as e:
            print(f"  {job_key:30} -> ERROR: {e}")
            job_info["status"] = "error_checking_status"
            job_info["error"] = str(e)

    if all_done:
        print("\n" + "=" * 80)
        print("ALL JOBS COMPLETED")
        print("=" * 80)
        break

    print(f"\nWaiting {poll_interval} seconds before next check...")
    time.sleep(poll_interval)

print("\nFINAL JOB SUMMARY")
print("=" * 80)

status_counts = {}
for job_key, job_info in jobs.items():
    status = job_info["status"]
    status_counts[status] = status_counts.get(status, 0) + 1

    settings_str = str(job_info.get("run_settings", {}))
    print(f"{job_key:50} | {settings_str:40} | {status}")

print("\n" + "-" * 80)
print("Status Summary:")
for status, count in sorted(status_counts.items()):
    print(f"  {status:20} : {count}")
print("=" * 80)
