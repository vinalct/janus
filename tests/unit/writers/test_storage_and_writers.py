from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from janus.models import ExecutionPlan, RunContext
from janus.registry import load_registry
from janus.utils.storage import StorageLayout
from janus.writers import RawArtifactWriter, SparkDatasetWriter

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parents[3]

NORMALIZED_COLUMNS = {
    "janus_run_id",
    "janus_source_id",
    "janus_source_name",
    "janus_environment",
    "janus_strategy_family",
    "janus_strategy_variant",
    "ingestion_timestamp",
    "ingestion_date",
}


@pytest.fixture(scope="module")
def spark():
    pyspark_sql = pytest.importorskip("pyspark.sql")
    session = (
        pyspark_sql.SparkSession.builder.appName("janus-writer-tests")
        .master("local[1]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def test_storage_layout_resolves_zone_targets_against_runtime_storage_roots(tmp_path):
    plan = _build_plan(
        tmp_path,
        run_id="run-storage-001",
        started_at=datetime(2026, 4, 9, 12, 0, tzinfo=UTC),
    )
    storage_layout = _build_storage_layout(tmp_path)

    raw_target = storage_layout.resolve_output(plan, "raw")
    bronze_target = storage_layout.resolve_output(plan, "bronze")
    metadata_target = storage_layout.resolve_output(plan, "metadata")

    assert raw_target.resolved_path == (
        tmp_path / "runtime" / "raw" / "example" / "federal_open_data_example"
    )
    assert bronze_target.resolved_path == (
        tmp_path / "runtime" / "bronze" / "example" / "federal_open_data_example"
    )
    assert metadata_target.resolved_path == (
        tmp_path / "runtime" / "metadata" / "example" / "federal_open_data_example"
    )


def test_raw_artifact_writer_persists_json_payload_with_checksum(tmp_path):
    plan = _build_plan(
        tmp_path,
        run_id="run-writer-raw-001",
        started_at=datetime(2026, 4, 9, 12, 0, tzinfo=UTC),
    )
    storage_layout = _build_storage_layout(tmp_path)
    writer = RawArtifactWriter(storage_layout)

    persisted = writer.write_json(
        plan,
        "page-0001.json",
        {"page": 1, "records": [{"id": "1"}]},
        metadata={"source": "unit-test"},
    )

    persisted_path = Path(persisted.write_result.path)
    assert persisted_path == (
        tmp_path
        / "runtime"
        / "raw"
        / "example"
        / "federal_open_data_example"
        / "page-0001.json"
    )
    assert json.loads(persisted_path.read_text(encoding="utf-8")) == {
        "page": 1,
        "records": [{"id": "1"}],
    }
    assert persisted.artifact.path == str(persisted_path)
    assert persisted.artifact.checksum == persisted.write_result.metadata_as_dict()["checksum"]
    assert persisted.write_result.zone == "raw"
    assert persisted.write_result.records_written == 1
    assert persisted.write_result.metadata_as_dict()["source"] == "unit-test"


def test_spark_dataset_writer_persists_bronze_output_with_normalization_columns(
    spark: SparkSession,
    tmp_path,
):
    from janus.normalizers import BaseNormalizer

    plan = _build_plan(
        tmp_path,
        run_id="run-writer-bronze-001",
        started_at=datetime(2026, 4, 9, 12, 0, tzinfo=UTC),
    )
    storage_layout = _build_storage_layout(tmp_path)
    normalizer = BaseNormalizer()
    writer = SparkDatasetWriter(storage_layout)

    dataframe = spark.createDataFrame(
        [
            {"id": "1", "name": "alpha"},
            {"id": "2", "name": "beta"},
        ]
    )
    normalized = normalizer.normalize(dataframe, plan)

    result = writer.write(normalized, plan, "bronze", count_records=True)

    assert result.zone == "bronze"
    assert result.records_written == 2
    assert result.partition_by == ("ingestion_date",)
    assert Path(result.path) == (
        tmp_path / "runtime" / "bronze" / "example" / "federal_open_data_example"
    )

    persisted = spark.read.parquet(result.path)
    assert persisted.count() == 2
    assert NORMALIZED_COLUMNS.issubset(set(persisted.columns))

    first_row = persisted.select("janus_run_id", "janus_source_id", "ingestion_date").first()
    assert first_row.janus_run_id == "run-writer-bronze-001"
    assert first_row.janus_source_id == "federal_open_data_example"
    assert str(first_row.ingestion_date) == "2026-04-09"


def _build_plan(tmp_path: Path, *, run_id: str, started_at: datetime) -> ExecutionPlan:
    source_config = load_registry(PROJECT_ROOT).get_source("federal_open_data_example")
    run_context = RunContext.create(
        run_id=run_id,
        environment="local",
        project_root=tmp_path,
        started_at=started_at,
    )
    return ExecutionPlan.from_source_config(source_config, run_context)


def _build_storage_layout(tmp_path: Path) -> StorageLayout:
    return StorageLayout.from_environment_config(
        {
            "storage": {
                "root_dir": "runtime",
                "raw_dir": "runtime/raw",
                "bronze_dir": "runtime/bronze",
                "metadata_dir": "runtime/metadata",
            }
        },
        tmp_path,
    )
