import time
from pathlib import Path

from codeocean import CodeOcean
from codeocean.computation import NamedRunParam, RunParams

co_api_key = Path("../secrets/codeocean").read_text()
co_client = CodeOcean(
    domain="https://codeocean.allenneuraldynamics.org", token=co_api_key
)
del co_api_key

respt = co_client.capsules.get_capsule("ac433716-756b-478a-98fe-6a7c45b86726")
print(respt)

run_params = RunParams(
    capsule_id="ac433716-756b-478a-98fe-6a7c45b86726",
    named_parameters=[
        NamedRunParam(param_name="n_early_stopping_patience", value="501"),
    ],
)
response = co_client.computations.run_capsule(run_params=run_params)
print(f"Started computation: {response}")

# Extract computation ID from response
computation_id = response.get("id") if isinstance(response, dict) else response.id
print(f"Computation ID: {computation_id}")

# Poll for status updates
terminal_states = ["completed", "failed", "stopped"]
while True:
    try:
        computation = co_client.computations.get_computation(computation_id)
        state = (
            computation.get("state")
            if isinstance(computation, dict)
            else computation.state
        )
        print(f"Computation {computation_id} state: {state}")

        if state.lower() in terminal_states:
            print(f"Computation {computation_id} finished with state: {state}")
            break
    except Exception as e:
        print(f"Error checking computation status: {e}")
        break

    time.sleep(10)  # Wait 10 seconds before next check
