from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any

from janus.models import ExecutionPlan, ExtractionResult
from janus.utils.storage import StorageLayout

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

SUPPORTED_SPARK_READ_FORMATS = frozenset(
    {"binary", "csv", "json", "jsonl", "parquet", "text"}
)


class SparkDatasetReader:
    """Generic Spark reader for raw artifacts and explicit JANUS output zones."""

    def read_paths(
        self,
        spark: SparkSession,
        paths: Sequence[str | Path],
        *,
        format_name: str,
        schema: Any | None = None,
        options: Mapping[str, Any] | None = None,
    ) -> DataFrame:
        normalized_paths = _normalize_paths(paths)
        reader = spark.read.format(_spark_read_format(format_name))
        if schema is not None:
            reader = reader.schema(schema)
        for key, value in _resolved_read_options(format_name, options).items():
            reader = reader.option(key, value)

        load_argument: str | list[str]
        if len(normalized_paths) == 1:
            load_argument = normalized_paths[0]
        else:
            load_argument = list(normalized_paths)
        return reader.load(load_argument)

    def read_extraction_result(
        self,
        spark: SparkSession,
        extraction_result: ExtractionResult,
        *,
        format_name: str | None = None,
        schema: Any | None = None,
        options: Mapping[str, Any] | None = None,
    ) -> DataFrame:
        if extraction_result.is_empty:
            raise ValueError("extraction_result must contain at least one artifact")

        resolved_format = format_name or _single_artifact_format(extraction_result)
        return self.read_paths(
            spark,
            extraction_result.artifact_paths,
            format_name=resolved_format,
            schema=schema,
            options=options,
        )

    def read_plan_output(
        self,
        spark: SparkSession,
        plan: ExecutionPlan,
        storage_layout: StorageLayout,
        zone: str,
        *,
        format_name: str | None = None,
        schema: Any | None = None,
        options: Mapping[str, Any] | None = None,
    ) -> DataFrame:
        resolved_target = storage_layout.resolve_output(plan, zone)
        return self.read_paths(
            spark,
            (resolved_target.resolved_path,),
            format_name=format_name or resolved_target.format,
            schema=schema,
            options=options,
        )


def _normalize_paths(paths: Sequence[str | Path]) -> tuple[str, ...]:
    normalized_paths = tuple(str(Path(path)) for path in paths)
    if not normalized_paths:
        raise ValueError("paths must not be empty")
    return normalized_paths


def _resolved_read_options(
    format_name: str,
    options: Mapping[str, Any] | None,
) -> dict[str, str]:
    resolved_options = _default_read_options(format_name)
    resolved_options.update(_normalize_options(options))
    return resolved_options


def _default_read_options(format_name: str) -> dict[str, str]:
    normalized = format_name.strip().lower()
    if normalized == "json":
        return {"multiLine": "true"}
    return {}


def _normalize_options(options: Mapping[str, Any] | None) -> dict[str, str]:
    if not options:
        return {}
    return {str(key): str(value) for key, value in options.items()}


def _single_artifact_format(extraction_result: ExtractionResult) -> str:
    formats = {artifact.format for artifact in extraction_result.artifacts}
    if len(formats) != 1:
        raise ValueError(
            "Artifacts contain multiple formats; pass format_name explicitly to read them"
        )
    return next(iter(formats))


def _spark_read_format(format_name: str) -> str:
    normalized = format_name.strip().lower()
    if normalized not in SUPPORTED_SPARK_READ_FORMATS:
        allowed = ", ".join(sorted(SUPPORTED_SPARK_READ_FORMATS))
        raise ValueError(f"format_name must be one of: {allowed}")
    if normalized == "binary":
        return "binaryFile"
    if normalized == "jsonl":
        return "json"
    return normalized
