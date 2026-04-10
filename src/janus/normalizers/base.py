from __future__ import annotations

from datetime import UTC
from typing import TYPE_CHECKING

from janus.models import ExecutionPlan

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

NORMALIZATION_METADATA_COLUMNS = (
    "janus_run_id",
    "janus_source_id",
    "janus_source_name",
    "janus_environment",
    "janus_strategy_family",
    "janus_strategy_variant",
    "ingestion_timestamp",
    "ingestion_date",
)


class BaseNormalizer:
    """Shared normalization base that only adds run and ingestion metadata."""

    def normalize(self, dataframe: DataFrame, plan: ExecutionPlan) -> DataFrame:
        return reorder_columns(
            add_normalization_metadata(dataframe, plan),
            leading_columns=NORMALIZATION_METADATA_COLUMNS,
        )


def add_normalization_metadata(dataframe: DataFrame, plan: ExecutionPlan) -> DataFrame:
    from pyspark.sql.functions import col, lit, to_date, to_timestamp

    started_at = plan.run_context.started_at.astimezone(UTC).isoformat()
    normalized = dataframe
    normalized = normalized.withColumn("janus_run_id", lit(plan.run_context.run_id))
    normalized = normalized.withColumn("janus_source_id", lit(plan.source.source_id))
    normalized = normalized.withColumn("janus_source_name", lit(plan.source.name))
    normalized = normalized.withColumn("janus_environment", lit(plan.run_context.environment))
    normalized = normalized.withColumn("janus_strategy_family", lit(plan.source.strategy))
    normalized = normalized.withColumn(
        "janus_strategy_variant",
        lit(plan.source.strategy_variant),
    )
    normalized = normalized.withColumn("ingestion_timestamp", to_timestamp(lit(started_at)))
    normalized = normalized.withColumn("ingestion_date", to_date(col("ingestion_timestamp")))
    return normalized


def reorder_columns(
    dataframe: DataFrame,
    *,
    leading_columns: tuple[str, ...],
) -> DataFrame:
    ordered_leading_columns = tuple(
        column_name for column_name in leading_columns if column_name in dataframe.columns
    )
    trailing_columns = [
        column_name
        for column_name in dataframe.columns
        if column_name not in ordered_leading_columns
    ]
    return dataframe.select(*ordered_leading_columns, *trailing_columns)
