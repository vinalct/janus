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
from janus.utils.runtime import build_spark_session, load_environment_config, prepare_runtime


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


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Validate the JANUS runtime baseline and optionally build a deterministic "
            "source execution plan."
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
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    project_root = args.project_root.resolve()

    try:
        config = load_environment_config(args.environment, project_root)
        resolved_paths = prepare_runtime(config, project_root)
    except (FileNotFoundError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
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

    if args.source_id:
        try:
            planned_run = Planner().plan(
                PlanningRequest.create(
                    source_id=args.source_id,
                    environment=args.environment,
                    project_root=project_root,
                    run_id=args.run_id,
                    started_at=args.started_at,
                    attributes={"trigger": "cli"},
                )
            )
        except (FileNotFoundError, PlannerError, SourceNotFoundError, ValueError) as exc:
            print(str(exc), file=sys.stderr)
            return 2
        summary["planned_run"] = planned_run.to_summary()

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
