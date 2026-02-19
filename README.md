# Flyte Anyscale Integration Examples

This repository contains Proof-of-Concept (POC) examples of how to integrate [Flyte](https://flyte.org/) with [Anyscale](https://www.anyscale.com/) for distributed Ray jobs.

The project demonstrates two distinct approaches to submitting and tracking jobs on the Anyscale platform:

## 1. Agents Approach (`agents-approach/`)
This approach uses the Flyte Agent framework (`AsyncAgentBase`) to decouple the execution of Ray jobs from the Flyte propeller.
- **Key Benefit**: The Flyte pod finishes quickly while the job runs on Anyscale, saving Flyte cluster resources.
- **Implementation**: Uses `AnyscaleAgent` and `AnyscaleJobTask` with `AsyncAgentExecutorMixin` for asynchronous execution and local testing.

See the [Agents README](agents-approach/README.md) for more details.

## 2. Python Task Approach (`python-task-approach/`)
This is a simpler, blocking approach using a standard Flyte `@task`.
- **Mechanism**: The Flyte task submits the job to Anyscale and waits for its completion.
- **Trade-off**: The Flyte pod stays `RUNNING` for the entire duration of the Anyscale job, which may consume more cluster resources but is easier to implement.

## Prerequisites
1. **Anyscale Credentials**: Ensure you have a valid CLI token in `~/.anyscale/credentials.json` or set `ANYSCALE_CLI_TOKEN` in your environment.
2. **Environment**: These projects use `uv` for dependency management.
   ```bash
   uv sync
   ```

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

**THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND.**
