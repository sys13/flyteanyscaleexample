# Flyte Anyscale Agent Integration

This POC demonstrates how to implement a custom Flyte Agent to submit and track jobs on the Anyscale platform.

## Overview

The integration uses the Flyte Agent framework (`AsyncAgentBase`) to decouple the execution of Ray jobs from the Flyte propeller. This allows for:
- **Asynchronous Execution**: Flyte pods don't need to stay running while Anyscale jobs execute.
- **Local Testing**: Using `AsyncAgentExecutorMixin`, you can test the agent's logic locally without a full Flyte deployment.

## Prerequisites

1. **Anyscale Credentials**: Ensure you have a valid CLI token in `~/.anyscale/credentials.json` or set `ANYSCALE_CLI_TOKEN` in your environment.
2. **Environment**: This project uses `uv` for dependency management.
   ```bash
   uv sync
   ```

## How to Run the Local Test

You can run the Flyte workflow locally. The `AnyscaleJobTask` uses the `AsyncAgentExecutorMixin`, which tells `pyflyte` to use the `AnyscaleAgent` for execution.

### 1. Run the Workflow

Execute the following command in your terminal:

```bash
uv run pyflyte run flyte_wrapper.py anyscale_integration_wf
```

### 2. What Happens During Execution

1. **Agent Creation**: The `AnyscaleAgent.create()` method is called. It submits `ray_script.py` to Anyscale and returns a `job_id`.
2. **Polling**: `pyflyte` (mimicking FlytePropeller) will call `AnyscaleAgent.get()` periodically to check the job status.
3. **Completion**: Once the Anyscale job reaches a `SUCCEEDED` state, the local execution finishes.

### 3. Monitor the Job

The console output will display the Anyscale Job ID and the current status. You can also monitor the job directly in the [Anyscale Console](https://console.anyscale.com/jobs).

## Files

- `flyte_wrapper.py`: Contains the `AnyscaleAgent` implementation, the task definition, and the workflow.
- `ray_script.py`: The actual Ray code that will run on Anyscale.
- `pyproject.toml`: Project dependencies (`flytekit`, `anyscale`, `ray`).
