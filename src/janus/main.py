from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path

from janus.planner import Planner, PlannerError, PlanningRequest
from janus.registry import SourceNotFoundError
from janus.runtime import SourceExecutor
from janus.utils.environment import build_spark_session, load_environment_config, prepare_runtime


def default_project_root() -> Path:
    env_project_root = os.getenv("JANUS_PROJECT_ROOT")
    if env_project_root:
        return Path(env_project_root)

    cwd = Path.cwd()
    if (cwd / "conf" / "environments").exists():
        return cwd

    return Path(__file__).resolve().parents[2]


def parse_started_at(value: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "started-at must be a valid ISO-8601 timestamp"
        ) from exc

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise argparse.ArgumentTypeError("started-at must include a timezone offset")

    return parsed


def format_runtime_permission_error(exc: PermissionError) -> str:
    path = exc.filename or "<unknown>"
    message = (
        f"JANUS could not prepare the runtime path {path!r}. "
        "The active environment needs write access to the configured storage "
        "and Spark cache directories."
    )
    if str(path).startswith("/workspace/"):
        message += (
            " If you are running inside the local container, recreate it with "
            "`make down && make up` so the Docker/Podman user mapping is "
            "applied correctly."
        )
    return message


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate JANUS runtime configuration, plan one source, or execute one "
            "configured source through the framework runtime."
        )
    )
    parser.add_argument(
        "--environment",
        default="local",
        help="Environment profile name under conf/environments without the .yaml suffix.",
    )
    parser.add_argument(
        "--project-root",
        type=Path,
        default=default_project_root(),
        help="Project root used to resolve conf/ and data/ paths.",
    )
    parser.add_argument(
        "--with-spark",
        action="store_true",
        help="Create and stop a Spark session after validating the environment config.",
    )
    parser.add_argument(
        "--source-id",
        help="Configured source_id to plan from the source registry.",
    )
    parser.add_argument(
        "--run-id",
        help="Optional explicit run identifier for the planned source execution.",
    )
    parser.add_argument(
        "--started-at",
        type=parse_started_at,
        help="Optional timezone-aware ISO-8601 timestamp used to make planning deterministic.",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help=(
            "Execute the selected source through planning, extraction, Spark normalization, "
            "bronze writes, validation, and metadata persistence."
        ),
    )
    parser.add_argument(
        "--include-disabled",
        action="store_true",
        help="Allow planning or executing a source that is configured but disabled.",
    )
    args = parser.parse_args(argv)

    if args.execute and not args.source_id:
        parser.error("--execute requires --source-id")
    if args.include_disabled and not args.source_id:
        parser.error("--include-disabled requires --source-id")

    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = args.project_root.resolve()

    try:
        config = load_environment_config(args.environment, project_root)
        resolved_paths = prepare_runtime(config, project_root)
    except (FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 2
    except PermissionError as exc:
        print(format_runtime_permission_error(exc), file=sys.stderr)
        return 2

    summary = {
        "environment": config.get("name", args.environment),
        "project_root": str(project_root),
        "spark": {
            "app_name": config.get("spark", {}).get("app_name"),
            "master": config.get("spark", {}).get("master"),
            "config": config.get("spark", {}).get("config", {}),
        },
        "paths": {name: str(path) for name, path in resolved_paths.items()},
    }

    planned_run = None
    if args.source_id:
        try:
            planned_run = Planner().plan(
                PlanningRequest.create(
                    source_id=args.source_id,
                    environment=args.environment,
                    project_root=project_root,
                    run_id=args.run_id,
                    started_at=args.started_at,
                    include_disabled=args.include_disabled,
                    attributes={"trigger": "cli"},
                )
            )
        except (FileNotFoundError, PlannerError, SourceNotFoundError, ValueError) as exc:
            print(str(exc), file=sys.stderr)
            return 2
        summary["planned_run"] = planned_run.to_summary()

    if args.execute:
        assert planned_run is not None
        try:
            spark = build_spark_session(config, resolved_paths)
        except Exception as exc:  # pragma: no cover - defensive entrypoint guard
            print(str(exc), file=sys.stderr)
            return 1

        try:
            summary["spark_session"] = {
                "app_name": spark.sparkContext.appName,
                "master": spark.sparkContext.master,
            }
            executed_run = SourceExecutor().execute(planned_run, spark, config)
            summary["executed_run"] = executed_run.to_summary()
        finally:
            spark.stop()

        print(json.dumps(summary, indent=2, sort_keys=True))
        return 0 if executed_run.is_successful else 1

    print(json.dumps(summary, indent=2, sort_keys=True))

    if not args.with_spark:
        return 0

    spark = build_spark_session(config, resolved_paths)
    try:
        print(
            json.dumps(
                {
                    "spark_app_name": spark.sparkContext.appName,
                    "spark_master": spark.sparkContext.master,
                },
                indent=2,
                sort_keys=True,
            )
        )
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
