from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from janus.utils.runtime import build_spark_session, load_environment_config, prepare_runtime


def default_project_root() -> Path:
    env_project_root = os.getenv("JANUS_PROJECT_ROOT")
    if env_project_root:
        return Path(env_project_root)

    cwd = Path.cwd()
    if (cwd / "conf" / "environments").exists():
        return cwd

    return Path(__file__).resolve().parents[2]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate the JANUS runtime baseline and optionally create a Spark session."
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
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    project_root = args.project_root.resolve()
    config = load_environment_config(args.environment, project_root)
    resolved_paths = prepare_runtime(config, project_root)

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
