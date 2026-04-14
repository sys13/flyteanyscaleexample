from typing import Optional
from dataclasses import dataclass
from flytekit import workflow, FlyteContextManager, kwtypes
from flytekit.models.literals import LiteralMap
from flytekit.models.task import TaskTemplate
from flytekit.extend.backend.base_agent import AsyncAgentBase, AgentRegistry, Resource, ResourceMeta, AsyncAgentExecutorMixin
from flytekit.core.base_task import PythonTask
from flytekit.core.type_engine import TypeEngine
from flyteidl.core.execution_pb2 import TaskExecution
from flyteidl.core.execution_pb2 import TaskLog
import anyscale
from anyscale.job.models import JobConfig
import os
import json
from pathlib import Path

from compute_config import build_compute_config

cloud_name = "aws-public-us-west-2"
project_name = "flytetest"

@dataclass
class AnyscaleJobMetadata(ResourceMeta):
    """
    Metadata for tracking an Anyscale job.
    """
    job_id: str

class AnyscaleAgent(AsyncAgentBase):
    """
    Anyscale Flyte Agent that submits and tracks jobs on the Anyscale platform.
    """
    def __init__(self):
        super().__init__(task_type_name="anyscale_job", metadata_type=AnyscaleJobMetadata)

    def create(
        self,
        task_template: TaskTemplate,
        inputs: Optional[LiteralMap] = None,
        **kwargs,
    ) -> AnyscaleJobMetadata:
        # Resolve inputs using TypeEngine
        ctx = FlyteContextManager.current_context()
        python_inputs = TypeEngine.literal_map_to_kwargs(ctx, inputs, literal_types=task_template.interface.inputs)
        script_path = python_inputs.get("script_path")

        # Ensure ANYSCALE_CLI_TOKEN is set
        if "ANYSCALE_CLI_TOKEN" not in os.environ:
            credentials_path = Path.home() / ".anyscale" / "credentials.json"
            if credentials_path.exists():
                with open(credentials_path) as f:
                    creds = json.load(f)
                    if "cli_token" in creds:
                        os.environ["ANYSCALE_CLI_TOKEN"] = creds["cli_token"]

        # Prepare job config
        abs_script_path = os.path.abspath(script_path)
        working_dir = os.path.dirname(abs_script_path)
        entrypoint = f"python {os.path.basename(abs_script_path)}"

        # Translate the Flyte node spec to an Anyscale compute config (if present)
        compute_config = build_compute_config(task_template)

        config = JobConfig(
            name="flyte-agent-job",
            entrypoint=entrypoint,
            working_dir=working_dir,
            image_uri="anyscale/ray:2.54.0-slim-py313-cu129",
            cloud=cloud_name,
            project=project_name,
            **({"compute_config": compute_config} if compute_config else {}),
        )

        # Submit the job asynchronously
        job_id = anyscale.job.submit(config)
        print(f"Submitted Anyscale job via Agent: {job_id}")
        return AnyscaleJobMetadata(job_id=job_id)

    def get(self, resource_meta: AnyscaleJobMetadata, **kwargs) -> Resource:
        from anyscale.job.models import JobState

        status = anyscale.job.status(id=resource_meta.job_id)
        print(f"Anyscale job {resource_meta.job_id} status: {status.state}")

        phase_map = {
            JobState.STARTING: TaskExecution.INITIALIZING,
            JobState.RUNNING: TaskExecution.RUNNING,
            JobState.SUCCEEDED: TaskExecution.SUCCEEDED,
            JobState.FAILED: TaskExecution.FAILED,
            JobState.UNKNOWN: TaskExecution.UNDEFINED,
        }
        phase = phase_map.get(status.state, TaskExecution.UNDEFINED)

        log_links = [TaskLog(
            uri=f"https://console.anyscale.com/jobs/{resource_meta.job_id}",
            name="Anyscale Job Console",
        )]
        return Resource(phase=phase, outputs={"job_id": resource_meta.job_id}, log_links=log_links)

    def delete(self, resource_meta: AnyscaleJobMetadata, **kwargs):
        print(f"Cancelling Anyscale job: {resource_meta.job_id}")
        anyscale.job.terminate(id=resource_meta.job_id)

# Register the agent
AgentRegistry.register(AnyscaleAgent())

class AnyscaleJobTask(AsyncAgentExecutorMixin, PythonTask):
    """
    Task definition that uses the AnyscaleAgent for execution.
    By using AsyncAgentExecutorMixin, local execution will use the agent.
    """
    def __init__(self, name: str, **kwargs):
        from flytekit.core.interface import Interface
        super().__init__(
            task_type="anyscale_job",
            name=name,
            interface=Interface(inputs={"script_path": str}, outputs={"job_id": str}),
            task_config=None,
            **kwargs
        )

# Define the task instance
run_anyscale_job = AnyscaleJobTask(
    name="run_anyscale_job",
    inputs=kwtypes(script_path=str),
)

@workflow
def anyscale_integration_wf():
    # Submit the job via the Anyscale agent
    run_anyscale_job(script_path="ray_script.py")
