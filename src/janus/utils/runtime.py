from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

ENV_PATTERN = re.compile(r"\$\{(?P<name>[A-Z0-9_]+)(?::-(?P<default>[^}]*))?\}")
ICEBERG_SESSION_EXTENSIONS = (
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
)
ICEBERG_CATALOG_IMPL = "org.apache.iceberg.spark.SparkCatalog"


def expand_env_vars(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: expand_env_vars(item) for key, item in value.items()}
    if isinstance(value, list):
        return [expand_env_vars(item) for item in value]
    if isinstance(value, str):
        return ENV_PATTERN.sub(
            lambda match: os.getenv(match.group("name"), match.group("default") or ""),
            value,
        )
    return value


def load_environment_config(environment: str, project_root: Path) -> dict[str, Any]:
    config_path = project_root / "conf" / "environments" / f"{environment}.yaml"
    if not config_path.exists():
        raise FileNotFoundError(f"Environment config not found: {config_path}")

    with config_path.open("r", encoding="utf-8") as stream:
        data = yaml.safe_load(stream) or {}

    if not isinstance(data, dict):
        raise ValueError(f"Environment config must be a mapping: {config_path}")

    return expand_env_vars(data)


def resolve_project_path(project_root: Path, value: str) -> Path:
    path = Path(value)
    return path if path.is_absolute() else project_root / path


def materialize_runtime_paths(config: dict[str, Any], project_root: Path) -> dict[str, Path]:
    storage = config.get("storage", {})
    spark = config.get("spark", {})

    paths = {
        "root_dir": resolve_project_path(project_root, storage["root_dir"]),
        "raw_dir": resolve_project_path(project_root, storage["raw_dir"]),
        "bronze_dir": resolve_project_path(project_root, storage["bronze_dir"]),
        "metadata_dir": resolve_project_path(project_root, storage["metadata_dir"]),
        "warehouse_dir": resolve_project_path(project_root, spark["warehouse_dir"]),
    }

    if "ivy_dir" in spark:
        paths["ivy_dir"] = resolve_project_path(project_root, spark["ivy_dir"])

    iceberg = spark.get("iceberg", {})
    if isinstance(iceberg, dict) and "warehouse_dir" in iceberg:
        paths["iceberg_warehouse_dir"] = resolve_project_path(
            project_root, iceberg["warehouse_dir"]
        )

    return paths


def prepare_runtime(config: dict[str, Any], project_root: Path) -> dict[str, Path]:
    paths = materialize_runtime_paths(config, project_root)
    for path in paths.values():
        path.mkdir(parents=True, exist_ok=True)
    return paths


def merge_csv_values(existing: str | None, value: str) -> str:
    items = [item.strip() for item in (existing or "").split(",") if item.strip()]
    if value not in items:
        items.append(value)
    return ",".join(items)


def build_spark_options(
    config: dict[str, Any], resolved_paths: dict[str, Path]
) -> dict[str, str]:
    spark_config = config.get("spark", {})
    options: dict[str, str] = {
        "spark.sql.warehouse.dir": str(resolved_paths["warehouse_dir"]),
    }
    options.update({key: str(value) for key, value in spark_config.get("config", {}).items()})

    ivy_dir = resolved_paths.get("ivy_dir")
    if ivy_dir is not None:
        options.setdefault("spark.jars.ivy", str(ivy_dir))

    iceberg = spark_config.get("iceberg")
    if isinstance(iceberg, dict) and iceberg:
        catalog_name = iceberg["catalog_name"]
        runtime_package = iceberg["runtime_package"]

        options["spark.jars.packages"] = merge_csv_values(
            options.get("spark.jars.packages"), runtime_package
        )
        options["spark.sql.extensions"] = merge_csv_values(
            options.get("spark.sql.extensions"), ICEBERG_SESSION_EXTENSIONS
        )
        options.setdefault("spark.sql.defaultCatalog", catalog_name)
        options.setdefault(f"spark.sql.catalog.{catalog_name}", ICEBERG_CATALOG_IMPL)
        options.setdefault(f"spark.sql.catalog.{catalog_name}.type", "hadoop")

        iceberg_warehouse_dir = resolved_paths.get("iceberg_warehouse_dir")
        if iceberg_warehouse_dir is None:
            raise KeyError("Resolved Iceberg warehouse path is missing")

        options.setdefault(
            f"spark.sql.catalog.{catalog_name}.warehouse",
            str(iceberg_warehouse_dir),
        )

        default_namespace = iceberg.get("default_namespace")
        if default_namespace:
            options.setdefault(
                f"spark.sql.catalog.{catalog_name}.default-namespace",
                str(default_namespace),
            )

    return options


def build_spark_session(config: dict[str, Any], resolved_paths: dict[str, Path]):
    from pyspark.sql import SparkSession

    spark_config = config.get("spark", {})
    builder = SparkSession.builder.appName(spark_config["app_name"]).master(
        spark_config["master"]
    )

    for key, value in build_spark_options(config, resolved_paths).items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(config.get("runtime", {}).get("log_level", "WARN"))
    return spark
