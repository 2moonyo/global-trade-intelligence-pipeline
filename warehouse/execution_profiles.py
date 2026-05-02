from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from warehouse.batch_plan import BatchDefinition, load_batch_plan, resolve_batch_plan_path

DEFAULT_EXECUTION_PROFILE_PATH = PROJECT_ROOT / "ops" / "execution_profiles.json"
SUPPORTED_RUNTIMES = {"vm", "cloud_run"}


@dataclass(frozen=True)
class ExecutionProfile:
    name: str
    description: str
    default_runtime: str
    datasets: dict[str, str]
    batches: dict[str, str]

    def runtime_for_dataset(self, dataset_name: str) -> str:
        return self.datasets.get(dataset_name, self.default_runtime)

    def owns_dataset(self, dataset_name: str, runtime: str) -> bool:
        return self.runtime_for_dataset(dataset_name) == runtime

    def runtime_for_batch(self, batch_id: str, dataset_name: str) -> str:
        return self.batches.get(batch_id, self.runtime_for_dataset(dataset_name))

    def owns_batch(self, batch_id: str, dataset_name: str, runtime: str) -> bool:
        return self.runtime_for_batch(batch_id, dataset_name) == runtime


def resolve_execution_profile_path(path: str | None = None) -> Path:
    configured = path or os.getenv("EXECUTION_PROFILE_PATH")
    return Path(configured).resolve() if configured else DEFAULT_EXECUTION_PROFILE_PATH


def _load_payload(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"Expected an object in {path}")
    return payload


def load_profiles(path: str | None = None) -> tuple[str, dict[str, ExecutionProfile]]:
    profile_path = resolve_execution_profile_path(path)
    payload = _load_payload(profile_path)
    default_profile = str(payload.get("default_profile") or "all_vm")
    raw_profiles = payload.get("profiles")
    if not isinstance(raw_profiles, dict):
        raise ValueError(f"Expected profiles object in {profile_path}")

    profiles: dict[str, ExecutionProfile] = {}
    for profile_name, raw_profile in raw_profiles.items():
        if not isinstance(raw_profile, dict):
            raise ValueError(f"Profile {profile_name!r} must be an object")
        default_runtime = str(raw_profile.get("default_runtime") or "vm")
        if default_runtime not in SUPPORTED_RUNTIMES:
            raise ValueError(
                f"Profile {profile_name!r} has unsupported default_runtime {default_runtime!r}; "
                f"supported: {sorted(SUPPORTED_RUNTIMES)}"
            )
        raw_datasets = raw_profile.get("datasets") or {}
        if not isinstance(raw_datasets, dict):
            raise ValueError(f"Profile {profile_name!r} datasets must be an object")
        datasets = {str(dataset): str(runtime) for dataset, runtime in raw_datasets.items()}
        unsupported = sorted({runtime for runtime in datasets.values() if runtime not in SUPPORTED_RUNTIMES})
        if unsupported:
            raise ValueError(
                f"Profile {profile_name!r} has unsupported dataset runtimes {unsupported}; "
                f"supported: {sorted(SUPPORTED_RUNTIMES)}"
            )
        raw_batches = raw_profile.get("batches") or {}
        if not isinstance(raw_batches, dict):
            raise ValueError(f"Profile {profile_name!r} batches must be an object")
        batches = {str(batch_id): str(runtime) for batch_id, runtime in raw_batches.items()}
        unsupported_batch_runtimes = sorted(
            {runtime for runtime in batches.values() if runtime not in SUPPORTED_RUNTIMES}
        )
        if unsupported_batch_runtimes:
            raise ValueError(
                f"Profile {profile_name!r} has unsupported batch runtimes {unsupported_batch_runtimes}; "
                f"supported: {sorted(SUPPORTED_RUNTIMES)}"
            )
        profiles[str(profile_name)] = ExecutionProfile(
            name=str(profile_name),
            description=str(raw_profile.get("description") or ""),
            default_runtime=default_runtime,
            datasets=datasets,
            batches=batches,
        )

    if default_profile not in profiles:
        raise ValueError(f"default_profile {default_profile!r} is not defined in {profile_path}")
    return default_profile, profiles


def current_profile_name(path: str | None = None) -> str:
    default_profile, profiles = load_profiles(path)
    requested = os.getenv("EXECUTION_PROFILE", default_profile).strip() or default_profile
    if requested not in profiles:
        raise ValueError(f"Unknown EXECUTION_PROFILE={requested!r}; available: {sorted(profiles)}")
    return requested


def current_runtime(default: str = "vm") -> str:
    runtime = os.getenv("EXECUTION_RUNTIME", default).strip().lower() or default
    if runtime not in SUPPORTED_RUNTIMES:
        raise ValueError(f"Unsupported EXECUTION_RUNTIME={runtime!r}; supported: {sorted(SUPPORTED_RUNTIMES)}")
    return runtime


def get_execution_profile(
    *,
    profile_name: str | None = None,
    path: str | None = None,
) -> ExecutionProfile:
    default_profile, profiles = load_profiles(path)
    resolved_name = profile_name or os.getenv("EXECUTION_PROFILE", default_profile).strip() or default_profile
    if resolved_name not in profiles:
        raise ValueError(f"Unknown execution profile {resolved_name!r}; available: {sorted(profiles)}")
    return profiles[resolved_name]


def runtime_for_dataset(
    dataset_name: str,
    *,
    profile_name: str | None = None,
    path: str | None = None,
) -> str:
    return get_execution_profile(profile_name=profile_name, path=path).runtime_for_dataset(dataset_name)


def runtime_for_batch(
    batch_id: str,
    dataset_name: str,
    *,
    profile_name: str | None = None,
    path: str | None = None,
) -> str:
    return get_execution_profile(profile_name=profile_name, path=path).runtime_for_batch(batch_id, dataset_name)


def batch_owned_by_runtime(
    batch: BatchDefinition,
    *,
    runtime: str | None = None,
    profile_name: str | None = None,
    path: str | None = None,
) -> bool:
    resolved_runtime = runtime or current_runtime()
    profile = get_execution_profile(profile_name=profile_name, path=path)
    return profile.owns_batch(batch.batch_id, batch.dataset_name, resolved_runtime)


def filter_batches_for_runtime(
    batches: Iterable[BatchDefinition],
    *,
    runtime: str | None = None,
    profile_name: str | None = None,
    path: str | None = None,
) -> list[BatchDefinition]:
    resolved_runtime = runtime or current_runtime()
    profile = get_execution_profile(profile_name=profile_name, path=path)
    return [batch for batch in batches if profile.owns_batch(batch.batch_id, batch.dataset_name, resolved_runtime)]


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Inspect Capstone dataset execution ownership profiles.")
    parser.add_argument("--profile", default=None, help="Profile to inspect. Defaults to EXECUTION_PROFILE or file default.")
    parser.add_argument("--runtime", default=None, help="Optional runtime filter: vm or cloud_run.")
    parser.add_argument("--profile-path", default=None, help="Optional execution profile JSON path.")
    parser.add_argument("--plan-path", default=None, help="Optional batch plan path used to print batch ownership.")
    parser.add_argument("--output", choices=("plain", "json"), default="plain")
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()
    profile = get_execution_profile(profile_name=args.profile, path=args.profile_path)
    runtime_filter = args.runtime.strip().lower() if args.runtime else None
    if runtime_filter and runtime_filter not in SUPPORTED_RUNTIMES:
        raise SystemExit(f"Unsupported --runtime {runtime_filter!r}; supported: {sorted(SUPPORTED_RUNTIMES)}")

    payload: dict[str, Any] = {
        "profile": profile.name,
        "description": profile.description,
        "default_runtime": profile.default_runtime,
        "datasets": profile.datasets,
        "batches_map": profile.batches,
        "profile_path": str(resolve_execution_profile_path(args.profile_path)),
    }

    if args.plan_path is not None:
        plan = load_batch_plan(args.plan_path)
        batches = sorted(plan.values(), key=lambda item: (item.schedule_lane, item.run_order, item.batch_id))
        rows = []
        for batch in batches:
            owner = profile.runtime_for_batch(batch.batch_id, batch.dataset_name)
            if runtime_filter and owner != runtime_filter:
                continue
            rows.append(
                {
                    "batch_id": batch.batch_id,
                    "dataset_name": batch.dataset_name,
                    "schedule_lane": batch.schedule_lane,
                    "runtime": owner,
                }
            )
        payload["batches"] = rows
        payload["plan_path"] = str(resolve_batch_plan_path(args.plan_path))

    if args.output == "json":
        print(json.dumps(payload, indent=2, ensure_ascii=False))
        return

    print(f"profile={payload['profile']} default_runtime={payload['default_runtime']}")
    print(f"profile_path={payload['profile_path']}")
    if payload.get("description"):
        print(payload["description"])
    for dataset, runtime in sorted(profile.datasets.items()):
        if runtime_filter and runtime != runtime_filter:
            continue
        print(f"dataset {dataset}: {runtime}")
    for batch_id, runtime in sorted(profile.batches.items()):
        if runtime_filter and runtime != runtime_filter:
            continue
        print(f"batch {batch_id}: {runtime}")
    for row in payload.get("batches", []):
        print(f"batch {row['batch_id']} ({row['dataset_name']}, lane={row['schedule_lane']}): {row['runtime']}")


if __name__ == "__main__":
    main()
