from __future__ import annotations

from dataclasses import dataclass
from itertools import product
from pathlib import Path
from typing import Any
import hashlib
import json
import yaml


@dataclass(frozen=True)
class TaskSpec:
    key: str
    command: str
    cpus_per_task: int
    memory_mb: int
    walltime_sec: int
    env: dict[str, str]
    parameters: dict[str, Any]


@dataclass(frozen=True)
class RetryRule:
    attempts: int
    multiplier: float
    ceiling: int
    include_suspected: bool = False


@dataclass(frozen=True)
class ExperimentSpec:
    path: Path
    name: str
    config_hash: str
    max_in_flight: int
    max_attempts: int
    timeout_retry: RetryRule | None
    oom_retry: RetryRule | None
    slurm_extra_args: list[str]
    tasks: list[TaskSpec]


def _render(value: Any, params: dict[str, Any]) -> Any:
    if isinstance(value, str):
        return value.format(**params)
    return value


def _task_from_mapping(
    key: str,
    task: dict[str, Any],
    defaults: dict[str, Any],
    base_env: dict[str, str],
    params: dict[str, Any],
) -> TaskSpec:
    resources = dict(defaults)
    resources.update(task.get("resources", {}))
    command = _render(task["command"], params)
    env = {k: str(_render(v, params)) for k, v in base_env.items()}
    env.update({k: str(_render(v, params)) for k, v in task.get("env", {}).items()})
    return TaskSpec(
        key=key,
        command=str(command),
        cpus_per_task=int(_render(resources.get("cpus_per_task", 1), params)),
        memory_mb=int(_render(resources.get("memory_mb", 256), params)),
        walltime_sec=int(_render(resources.get("walltime_sec", 60), params)),
        env=env,
        parameters=params,
    )


def load_experiment(path: str | Path) -> ExperimentSpec:
    path = Path(path).resolve()
    raw_text = path.read_text()
    data = yaml.safe_load(raw_text)
    if not isinstance(data, dict):
        raise ValueError("experiment YAML must contain a mapping")

    name = str(data["name"])
    execution = data.get("execution", {})
    defaults = data.get("resources", {})
    base_env = data.get("env", {})

    tasks: list[TaskSpec] = []
    if "matrix" in data:
        workload = data.get("workload", {})
        if "command" not in workload:
            raise ValueError("matrix experiments require workload.command")
        matrix = data["matrix"]
        keys = list(matrix)
        values = [matrix[k] for k in keys]
        for combo in product(*values):
            params = dict(zip(keys, combo))
            key = ",".join(f"{k}={params[k]}" for k in keys)
            task_mapping = {"command": workload["command"], "env": workload.get("env", {})}
            tasks.append(_task_from_mapping(key, task_mapping, defaults, base_env, params))
    elif "tasks" in data:
        for i, item in enumerate(data["tasks"], start=1):
            params = dict(item.get("params", {}))
            key = str(item.get("id", f"task-{i:03d}"))
            tasks.append(_task_from_mapping(key, item, defaults, base_env, params))
    else:
        raise ValueError("experiment must define either matrix or tasks")

    retry = execution.get("retry", {})
    timeout_cfg = retry.get("timeout")
    oom_cfg = retry.get("out_of_memory")

    timeout_rule = None
    if timeout_cfg:
        timeout_rule = RetryRule(
            attempts=int(timeout_cfg.get("attempts", 1)),
            multiplier=float(timeout_cfg.get("multiplier", 1.5)),
            ceiling=int(timeout_cfg.get("max_seconds", 600)),
        )

    oom_rule = None
    if oom_cfg:
        oom_rule = RetryRule(
            attempts=int(oom_cfg.get("attempts", 1)),
            multiplier=float(oom_cfg.get("multiplier", 1.5)),
            ceiling=int(oom_cfg.get("max_memory_mb", 4096)),
            include_suspected=bool(oom_cfg.get("include_suspected", False)),
        )

    normalized = json.dumps(data, sort_keys=True, separators=(",", ":"))
    config_hash = hashlib.sha256(normalized.encode()).hexdigest()[:12]

    slurm_extra = data.get("slurm", {}).get("extra_args", [])
    return ExperimentSpec(
        path=path,
        name=name,
        config_hash=config_hash,
        max_in_flight=int(execution.get("max_in_flight", 4)),
        max_attempts=int(execution.get("max_attempts", 2)),
        timeout_retry=timeout_rule,
        oom_retry=oom_rule,
        slurm_extra_args=[str(x) for x in slurm_extra],
        tasks=tasks,
    )
