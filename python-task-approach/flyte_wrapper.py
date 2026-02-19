from flytekit import task, workflow
import anyscale
from anyscale.job.models import JobConfig
import os
import json
from pathlib import Path

cloud_name = "aws-public-us-west-2"
project_name = "flytetest"

@task
def run_anyscale_job(script_path: str):
    """
    Submits a job to Anyscale and waits for completion.
    The Flyte pod stays RUNNING until the Anyscale job finishes.
    """
    # Ensure ANYSCALE_CLI_TOKEN is set in your environment
    if "ANYSCALE_CLI_TOKEN" not in os.environ:
        credentials_path = Path.home() / ".anyscale" / "credentials.json"
        if credentials_path.exists():
            with open(credentials_path) as f:
                creds = json.load(f)
                if "cli_token" in creds:
                    os.environ["ANYSCALE_CLI_TOKEN"] = creds["cli_token"]
                    print("Found ANYSCALE_CLI_TOKEN in ~/.anyscale/credentials.json")

    # Ensure path is absolute for working_dir
    abs_script_path = os.path.abspath(script_path)
    working_dir = os.path.dirname(abs_script_path)
    entrypoint = f"python {os.path.basename(abs_script_path)}"

    # Create JobConfig
    config = JobConfig(
        name="flyte-proxy-job",
        entrypoint=entrypoint,
        working_dir=working_dir,
        image_uri="anyscale/ray:2.54.0-slim-py313-cu129",
        cloud=cloud_name,
        project=project_name,
    )

    # Submit the job
    job_id = anyscale.job.submit(config)
    
    print(f"Submitted Anyscale job: {job_id}")
    
    # Wait for completion (blocks the Flyte pod)
    anyscale.job.wait(id=job_id)
    
    # Get final status
    result = anyscale.job.status(id=job_id)
    print(f"Anyscale job {job_id} result: {result!r}")
    if result.state != "SUCCEEDED":
        raise RuntimeError(f"Anyscale job {job_id} failed with status: {result.state}")
    
    print(f"Anyscale job {job_id} completed successfully!")

@workflow
def anyscale_integration_wf():
    # In a real scenario, this path should be accessible by the Flyte task
    run_anyscale_job(script_path="ray_script.py")
