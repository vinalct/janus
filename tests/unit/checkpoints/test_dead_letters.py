import json
from datetime import UTC, datetime
from pathlib import Path

from janus.checkpoints import DeadLetterStore
from janus.models import ExecutionPlan, RunContext
from janus.registry import load_registry

PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_dead_letter_store_records_and_loads_entries(tmp_path):
    plan = _build_plan(
        tmp_path,
        run_id="run-dead-letter-001",
        started_at=datetime(2026, 4, 8, 10, 0, tzinfo=UTC),
    )
    store = DeadLetterStore()

    state = store.record(
        plan,
        item_key="entity_id=123",
        item_type="request_input",
        error=RuntimeError("missing entity"),
        metadata={"request_url": "https://example.invalid/entities/123"},
    )

    assert state.entry_count == 1
    assert state.item_keys == frozenset({"entity_id=123"})

    loaded = store.load(plan)
    assert loaded == state

    payload = json.loads(store.path(plan).read_text(encoding="utf-8"))
    assert payload["entries"][0]["error_type"] == "RuntimeError"
    assert payload["entries"][0]["metadata"]["request_url"] == "https://example.invalid/entities/123"


def test_dead_letter_store_deduplicates_and_clears_entries(tmp_path):
    plan = _build_plan(
        tmp_path,
        run_id="run-dead-letter-002",
        started_at=datetime(2026, 4, 8, 10, 0, tzinfo=UTC),
    )
    store = DeadLetterStore()

    store.record(
        plan,
        item_key="entity_id=123",
        item_type="request_input",
        error=RuntimeError("missing entity"),
        metadata={"request_url": "https://example.invalid/entities/123"},
    )
    state = store.record(
        plan,
        item_key="entity_id=123",
        item_type="request_input",
        error=RuntimeError("still missing"),
        metadata={"request_url": "https://example.invalid/entities/123"},
    )

    assert state.entry_count == 1

    store.clear(plan)

    assert store.load(plan) is None
    assert store.path(plan).exists() is False


def test_dead_letter_store_loads_existing_state_for_a_new_resume_run_id(tmp_path):
    initial_plan = _build_plan(
        tmp_path,
        run_id="run-dead-letter-003",
        started_at=datetime(2026, 4, 8, 10, 0, tzinfo=UTC),
    )
    store = DeadLetterStore()
    store.record(
        initial_plan,
        item_key="entity_id=123",
        item_type="request_input",
        error=RuntimeError("missing entity"),
        metadata={"request_url": "https://example.invalid/entities/123"},
    )

    resume_plan = _build_plan(
        tmp_path,
        run_id="run-dead-letter-004",
        started_at=datetime(2026, 4, 9, 10, 0, tzinfo=UTC),
    )

    loaded = store.load(resume_plan)

    assert loaded is not None
    assert loaded.item_keys == frozenset({"entity_id=123"})


def _build_plan(tmp_path: Path, *, run_id: str, started_at: datetime) -> ExecutionPlan:
    source_config = load_registry(PROJECT_ROOT).get_source("federal_open_data_example")
    run_context = RunContext.create(
        run_id=run_id,
        environment="local",
        project_root=tmp_path,
        started_at=started_at,
    )
    return ExecutionPlan.from_source_config(source_config, run_context)
