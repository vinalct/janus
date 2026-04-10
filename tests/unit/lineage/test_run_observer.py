import json
from datetime import UTC, datetime
from pathlib import Path

from janus.lineage import RunObserver, compute_config_version
from janus.models import ExecutionPlan, ExtractedArtifact, ExtractionResult, RunContext, WriteResult
from janus.registry import load_registry

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_run_observer_persists_running_success_lineage_and_checkpoint_artifacts(tmp_path):
    plan = _build_plan(
        tmp_path,
        run_id="run-lineage-001",
        started_at=datetime(2026, 4, 8, 12, 0, tzinfo=UTC),
    )
    extraction_result = _build_extraction_result(plan)
    write_results = _build_write_results(plan)
    observer = RunObserver()

    started = observer.start_run(plan)
    assert started.run_metadata.status == "running"

    persisted = observer.record_success(
        plan,
        extraction_result,
        write_results,
        finished_at=datetime(2026, 4, 8, 12, 5, tzinfo=UTC),
    )

    run_payload = json.loads(started.run_metadata_path.read_text(encoding="utf-8"))
    assert run_payload["status"] == "succeeded"
    assert run_payload["run_id"] == "run-lineage-001"
    assert run_payload["duration_seconds"] == 300.0
    assert run_payload["records_extracted"] == 100
    assert run_payload["checkpoint_value"] == "2026-04-08T12:00:00Z"

    lineage_payload = json.loads(persisted.lineage_path.read_text(encoding="utf-8"))
    assert lineage_payload["status"] == "succeeded"
    assert lineage_payload["strategy_variant"] == "page_number_api"
    assert lineage_payload["config_version"] == compute_config_version(
        plan.source_config.config_path
    )
    assert lineage_payload["configured_outputs"][2]["path"] == plan.metadata_output.path
    assert lineage_payload["artifacts"][0]["path"].endswith("page-0001.json")

    assert persisted.checkpoint_result is not None
    assert persisted.checkpoint_result.decision == "advanced"
    checkpoint_payload = json.loads(
        persisted.checkpoint_result.current_path.read_text(encoding="utf-8")
    )
    assert checkpoint_payload["checkpoint_value"] == "2026-04-08T12:00:00Z"


def test_run_observer_persists_failure_reason_without_writing_checkpoint(tmp_path):
    plan = _build_plan(
        tmp_path,
        run_id="run-lineage-002",
        started_at=datetime(2026, 4, 8, 12, 0, tzinfo=UTC),
    )
    extraction_result = _build_extraction_result(plan)
    write_results = _build_write_results(plan)[:1]
    observer = RunObserver()

    persisted = observer.record_failure(
        plan,
        RuntimeError("upstream returned HTTP 500"),
        extraction_result,
        write_results,
        finished_at=datetime(2026, 4, 8, 12, 7, tzinfo=UTC),
    )

    run_payload = json.loads(persisted.run_metadata_path.read_text(encoding="utf-8"))
    assert run_payload["status"] == "failed"
    assert run_payload["error_type"] == "RuntimeError"
    assert run_payload["failure_reason"] == "upstream returned HTTP 500"

    lineage_payload = json.loads(persisted.lineage_path.read_text(encoding="utf-8"))
    assert lineage_payload["status"] == "failed"
    assert lineage_payload["failure_reason"] == "upstream returned HTTP 500"
    assert lineage_payload["materialized_outputs"][0]["zone"] == "raw"
    assert persisted.checkpoint_result is None


def _build_plan(tmp_path: Path, *, run_id: str, started_at: datetime) -> ExecutionPlan:
    source_config = load_registry(PROJECT_ROOT).get_source("federal_open_data_example")
    run_context = RunContext.create(
        run_id=run_id,
        environment="local",
        project_root=tmp_path,
        started_at=started_at,
    )
    return ExecutionPlan.from_source_config(source_config, run_context)


def _build_extraction_result(plan: ExecutionPlan) -> ExtractionResult:
    return ExtractionResult.from_plan(
        plan,
        artifacts=(
            ExtractedArtifact(
                path=f"{plan.raw_output.path}/page-0001.json",
                format="json",
            ),
        ),
        records_extracted=100,
        checkpoint_value="2026-04-08T12:00:00Z",
        metadata={"http_status": "200"},
    )


def _build_write_results(plan: ExecutionPlan) -> tuple[WriteResult, ...]:
    return (
        WriteResult.from_plan(
            plan,
            "raw",
            path=f"{plan.raw_output.path}/page-0001.json",
            format_name="json",
            mode="append",
            records_written=1,
        ),
        WriteResult.from_plan(
            plan,
            "bronze",
            path=f"{plan.bronze_output.path}/ingestion_date=2026-04-08",
            format_name="parquet",
            mode="append",
            records_written=100,
            partition_by=("ingestion_date",),
            metadata={"writer": "spark"},
        ),
    )
