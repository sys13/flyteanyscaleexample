"""
Translate a Flyte task's k8s_pod spec into an Anyscale compute config dict.
"""

import json
import math
import re
from typing import Optional

from flytekit.models.task import TaskTemplate


AUTO_INJECTED_VOLUME_PREFIXES = ("kube-api-access",)
AUTO_INJECTED_MOUNT_PREFIXES = ("/var/run/secrets/kubernetes.io",)
GPU_RESOURCE_KEYS = ("nvidia.com/gpu", "amd.com/gpu", "gpu")

# Labels that are runtime-specific to KubeRay/Kueue/Flyte executions.
# These change every run and carry no meaning in a reusable compute config.
EPHEMERAL_LABEL_PREFIXES = (
    "kueue.x-k8s.io/",
    "ray.io/cluster",
    "execution-id",
    "shard-key",
)

SPEC_FIELDS_SKIP = {
    "nodeName", "hostname", "subdomain", "hostIP", "podIP", "podIPs",
    "phase", "startTime", "conditions", "initContainers",
}

CONTAINER_FIELDS_SKIP = {
    "name", "image", "command", "args", "containerID", "imageID",
    "state", "lastState", "ready", "restartCount", "started",
}


def _parse_cpu_ceil(value: str) -> int:
    value = str(value).strip()
    if value.endswith("m"):
        return math.ceil(int(value[:-1]) / 1000.0)
    return math.ceil(float(value))


def _parse_memory_gi(value: str) -> str:
    value = str(value).strip()
    if re.match(r"^\d+(\.\d+)?(Gi|Mi|Ti|G|M)$", value):
        return value
    return f"{math.ceil(float(value) / (1024 ** 3))}Gi"


def _parse_ray_start_args(args: list) -> dict:
    joined = " ".join(str(a) for a in args)
    result = {}

    m = re.search(r"--num-cpus=(\S+)", joined)
    if m:
        result["num_cpus"] = int(float(m.group(1)))

    m = re.search(r"--num-gpus=(\S+)", joined)
    if m:
        result["num_gpus"] = int(float(m.group(1)))

    m = re.search(r"--memory=(\d+)", joined)
    if m:
        result["memory_gi"] = _parse_memory_gi(m.group(1))

    m = re.search(r"--resources=(\{[^}]+\})", joined)
    if m:
        try:
            result["custom_resources"] = json.loads(m.group(1))
        except json.JSONDecodeError:
            pass

    return result


def _detect_gpu(container: dict) -> int:
    for key, val in container.get("resources", {}).get("limits", {}).items():
        if any(key.lower().startswith(g) for g in GPU_RESOURCE_KEYS):
            try:
                return int(val)
            except (ValueError, TypeError):
                pass
    return 0


def _strip_volumes(volumes: list) -> list:
    return [v for v in volumes
            if not any(v.get("name", "").startswith(p) for p in AUTO_INJECTED_VOLUME_PREFIXES)]


def _strip_mounts(mounts: list) -> list:
    return [m for m in mounts
            if not any(m.get("mountPath", "").startswith(p) for p in AUTO_INJECTED_MOUNT_PREFIXES)]


def _filter_labels(labels: dict) -> dict:
    return {k: v for k, v in labels.items()
            if not any(k.startswith(p) for p in EPHEMERAL_LABEL_PREFIXES)}


def _container_patch(container: dict) -> dict:
    patch: dict = {"name": "ray"}
    for key, value in container.items():
        if key in CONTAINER_FIELDS_SKIP:
            continue
        if key == "resources":
            limits = value.get("limits", {})
            if limits:
                patch["resources"] = {"limits": limits}
            continue
        if key == "volumeMounts":
            cleaned = _strip_mounts(value)
            if cleaned:
                patch["volumeMounts"] = cleaned
            continue
        patch[key] = value
    return patch


def build_compute_config(task_template: TaskTemplate) -> Optional[dict]:
    """
    Translate a Flyte task's k8s_pod spec into an Anyscale compute config dict
    suitable for passing inline to JobConfig(compute_config=...).

    Returns None if the task has no k8s_pod spec.
    """
    if task_template.k8s_pod is None:
        return None

    pod_labels = (task_template.k8s_pod.metadata.labels or {}) if task_template.k8s_pod.metadata else {}
    pod_annotations = (task_template.k8s_pod.metadata.annotations or {}) if task_template.k8s_pod.metadata else {}
    spec: dict = task_template.k8s_pod.pod_spec or {}

    group_name = pod_labels.get("ray.io/group", "workers")

    containers = spec.get("containers", [])
    container = next((c for c in containers if c.get("name") == "ray-worker"), None)
    if container is None:
        container = containers[0] if containers else {}

    k8s_requests = container.get("resources", {}).get("requests", {})
    ray_args = _parse_ray_start_args(container.get("args", []))

    cpu = ray_args.get("num_cpus") or _parse_cpu_ceil(k8s_requests.get("cpu", "0"))
    memory = ray_args.get("memory_gi") or _parse_memory_gi(k8s_requests.get("memory", "0"))
    gpu_count = ray_args.get("num_gpus") or _detect_gpu(container)

    required_resources: dict = {"CPU": cpu, "memory": memory}
    if gpu_count:
        required_resources["GPU"] = gpu_count
    required_resources.update(ray_args.get("custom_resources", {}))

    worker: dict = {
        "name": group_name,
        "required_resources": required_resources,
        "min_nodes": 0,
        "max_nodes": 1,
        "market_type": "ON_DEMAND",
    }

    accel_type = (
        pod_labels.get("ray.io/accelerator-type")
        or (spec.get("nodeSelector") or {}).get("ray.io/accelerator-type")
    )
    if gpu_count:
        worker["required_labels"] = {
            "ray.io/accelerator-type": accel_type or "<fill-in>"
        }

    aic_metadata: dict = {}
    aic_spec: dict = {}

    filtered_labels = _filter_labels(pod_labels)
    if filtered_labels:
        aic_metadata["labels"] = filtered_labels
    filtered_annotations = {k: v for k, v in pod_annotations.items()
                            if not k.startswith("kueue.x-k8s.io/")}
    if filtered_annotations:
        aic_metadata["annotations"] = filtered_annotations

    for key, value in spec.items():
        if key in SPEC_FIELDS_SKIP or key in ("volumes", "containers", "initContainers"):
            continue
        if value is not None:
            aic_spec[key] = value

    volumes = _strip_volumes(spec.get("volumes", []))
    if volumes:
        aic_spec["volumes"] = volumes

    patch = _container_patch(container)
    if len(patch) > 1:
        aic_spec["containers"] = [patch]

    advanced_instance_config: dict = {}
    if aic_metadata:
        advanced_instance_config["metadata"] = aic_metadata
    if aic_spec:
        advanced_instance_config["spec"] = aic_spec

    if advanced_instance_config:
        worker["advanced_instance_config"] = advanced_instance_config

    return {"worker_nodes": [worker]}
