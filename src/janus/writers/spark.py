from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Any

from janus.models import ExecutionPlan, WriteResult
from janus.utils.storage import StorageLayout

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

SUPPORTED_SPARK_WRITE_FORMATS = frozenset({"csv", "json", "jsonl", "parquet", "text"})


class SparkDatasetWriter:
    """Generic Spark writer for bronze-style datasets and other Spark-managed outputs."""

    def __init__(self, storage_layout: StorageLayout) -> None:
        self.storage_layout = storage_layout

    def write(
        self,
        dataframe: DataFrame,
        plan: ExecutionPlan,
        zone: str,
        *,
        path_suffix: str | None = None,
        format_name: str | None = None,
        mode: str | None = None,
        partition_by: tuple[str, ...] | None = None,
        options: Mapping[str, Any] | None = None,
        metadata: Mapping[str, str] | None = None,
        records_written: int | None = None,
        count_records: bool = False,
        apply_repartition: bool = True,
    ) -> WriteResult:
        resolved_target = self.storage_layout.resolve_output(plan, zone)
        path = (
            resolved_target.resolved_path
            if path_suffix is None
            else resolved_target.child(path_suffix)
        )
        resolved_format = format_name or resolved_target.format
        spark_format = _spark_write_format(resolved_format)
        write_mode = mode or plan.source_config.spark.write_mode
        partition_columns = partition_by or plan.source_config.spark.partition_by

        prepared_frame = dataframe
        if apply_repartition and zone == "bronze" and plan.source_config.spark.repartition:
            prepared_frame = prepared_frame.repartition(plan.source_config.spark.repartition)

        resolved_records_written = records_written
        if count_records and resolved_records_written is None:
            resolved_records_written = prepared_frame.count()

        writer = prepared_frame.write.mode(write_mode).format(spark_format)
        for key, value in _normalize_options(options).items():
            writer = writer.option(key, value)
        if partition_columns:
            writer = writer.partitionBy(*partition_columns)
        writer.save(str(path))

        return WriteResult.from_plan(
            plan,
            zone,
            path=str(path),
            format_name=resolved_format,
            mode=write_mode,
            records_written=resolved_records_written,
            partition_by=partition_columns,
            metadata=metadata,
        )


def _normalize_options(options: Mapping[str, Any] | None) -> dict[str, str]:
    if not options:
        return {}
    return {str(key): str(value) for key, value in options.items()}


def _spark_write_format(format_name: str) -> str:
    normalized = format_name.strip().lower()
    if normalized not in SUPPORTED_SPARK_WRITE_FORMATS:
        allowed = ", ".join(sorted(SUPPORTED_SPARK_WRITE_FORMATS))
        raise ValueError(f"format_name must be one of: {allowed}")
    if normalized == "jsonl":
        return "json"
    return normalized
