import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

from janus.checkpoints import CheckpointStore
from janus.lineage import MetadataZonePaths
from janus.models import ExecutionPlan, RunContext
from janus.registry import load_registry

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_checkpoint_store_persists_and_loads_current_state(tmp_path):
    plan = _build_plan(
        tmp_path,
        run_id="run-checkpoint-001",
        started_at=datetime(2026, 4, 8, 10, 0, tzinfo=UTC),
    )
    store = CheckpointStore()

    result = store.save(
        plan,
        "2026-04-08T12:00:00Z",
        metadata={"records_extracted": "100"},
    )

    assert result.decision == "advanced"
    assert result.advanced is True
    assert result.state is not None
    assert result.state.checkpoint_value == "2026-04-08T12:00:00Z"
    assert result.current_path == MetadataZonePaths.from_plan(plan).checkpoint_state_path
    assert result.history_path == MetadataZonePaths.from_plan(plan).checkpoint_history_path(
        "run-checkpoint-001"
    )

    loaded = store.load(plan)
    assert loaded == result.state

    payload = json.loads(result.current_path.read_text(encoding="utf-8"))
    assert payload["checkpoint_field"] == "updated_at"
    assert payload["metadata"] == {"records_extracted": "100"}


def test_checkpoint_store_keeps_newer_value_during_reruns(tmp_path):
    initial_plan = _build_plan(
        tmp_path,
        run_id="run-checkpoint-002",
        started_at=datetime(2026, 4, 8, 10, 0, tzinfo=UTC),
    )
    store = CheckpointStore()
    store.save(initial_plan, "2026-04-08T12:00:00Z")

    rerun_plan = replace(
        initial_plan,
        run_context=RunContext.create(
            run_id="run-checkpoint-003",
            environment="local",
            project_root=tmp_path,
            started_at=datetime(2026, 4, 9, 10, 0, tzinfo=UTC),
        ),
    )
    retained = store.save(rerun_plan, "2026-04-07T12:00:00Z")

    assert retained.decision == "retained"
    assert retained.advanced is False
    assert retained.state is not None
    assert retained.state.run_id == "run-checkpoint-002"
    assert retained.state.checkpoint_value == "2026-04-08T12:00:00Z"

    current_payload = json.loads(
        MetadataZonePaths.from_plan(rerun_plan)
        .checkpoint_state_path.read_text(encoding="utf-8")
    )
    assert current_payload["run_id"] == "run-checkpoint-002"
    assert current_payload["checkpoint_value"] == "2026-04-08T12:00:00Z"

    history_payload = json.loads(retained.history_path.read_text(encoding="utf-8"))
    assert history_payload["candidate_value"] == "2026-04-07T12:00:00Z"
    assert history_payload["stored_value"] == "2026-04-08T12:00:00Z"
    assert history_payload["decision"] == "retained"


def test_checkpoint_store_skips_when_checkpointing_is_disabled(tmp_path):
    plan = _build_plan(
        tmp_path,
        run_id="run-checkpoint-004",
        started_at=datetime(2026, 4, 8, 10, 0, tzinfo=UTC),
    )
    disabled_plan = replace(plan, checkpoint_strategy="none", checkpoint_field=None)
    store = CheckpointStore()

    result = store.save(disabled_plan, "2026-04-08T12:00:00Z")

    assert result.decision == "skipped"
    assert result.state is None
    assert result.current_path is None
    assert result.history_path is None


def _build_plan(tmp_path: Path, *, run_id: str, started_at: datetime) -> ExecutionPlan:
    source_config = load_registry(PROJECT_ROOT).get_source("federal_open_data_example")
    run_context = RunContext.create(
        run_id=run_id,
        environment="local",
        project_root=tmp_path,
        started_at=started_at,
    )
    return ExecutionPlan.from_source_config(source_config, run_context)
