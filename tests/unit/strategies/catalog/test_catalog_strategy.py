from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from janus.checkpoints import CheckpointStore
from janus.models import ExecutionPlan, RunContext, SourceConfig
from janus.planner import StrategyCatalog
from janus.strategies.api import ApiResponse
from janus.strategies.catalog import CatalogStrategy
from janus.utils.logging import build_structured_logger
from janus.utils.storage import StorageLayout


@dataclass(frozen=True, slots=True)
class ResponseSpec:
    status_code: int
    payload: Any
    headers: dict[str, str] = field(default_factory=dict)


class FakeTransport:
    def __init__(self, responses: list[ResponseSpec | Exception]) -> None:
        self._responses = list(responses)
        self.requests = []
        self.opened = False
        self.closed = False

    def open(self) -> None:
        self.opened = True

    def close(self) -> None:
        self.closed = True

    def send(self, request):
        self.requests.append(request)
        if not self._responses:
            raise AssertionError("No fake responses remain for this transport")

        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response

        return ApiResponse(
            request=request,
            status_code=response.status_code,
            body=json.dumps(response.payload).encode("utf-8"),
            headers=tuple(sorted(response.headers.items())),
        )


def test_default_strategy_catalog_uses_catalog_strategy_for_catalog_variants(tmp_path):
    source_config = _build_source_config(tmp_path, source_id="catalog_source")

    binding = StrategyCatalog.with_defaults().resolve(source_config)

    assert isinstance(binding.strategy, CatalogStrategy)
    assert binding.dispatch_path == "catalog.metadata_catalog"


def test_catalog_strategy_logs_page_progress(tmp_path):
    stream = StringIO()
    logger = build_structured_logger("janus.tests.catalog.progress", stream=stream)
    plan = _build_plan(tmp_path, source_id="logged_catalog_source", page_size=2)
    strategy, _transport = _build_strategy(
        tmp_path,
        [
            ResponseSpec(
                200,
                {
                    "result": {
                        "results": [
                            {"id": "dataset-1", "title": "Dataset 1"},
                            {"id": "dataset-2", "title": "Dataset 2"},
                        ]
                    }
                },
            ),
            ResponseSpec(
                200,
                {
                    "result": {
                        "results": [
                            {"id": "dataset-3", "title": "Dataset 3"},
                        ]
                    }
                },
            ),
        ],
        logger=logger,
    )

    strategy.extract(plan)

    payloads = [json.loads(line) for line in stream.getvalue().splitlines()]
    events = [payload["event"] for payload in payloads]
    finished_pages = [
        payload for payload in payloads if payload["event"] == "catalog_request_finished"
    ]

    assert events[0] == "catalog_extraction_started"
    assert events[-1] == "catalog_extraction_finished"
    assert [page["fields"]["page_number"] for page in finished_pages] == [1, 2]
    assert finished_pages[0]["fields"]["records_extracted"] == 2
    assert finished_pages[0]["fields"]["entities_extracted"] == 2
    assert finished_pages[0]["fields"]["has_next_page"] is True
    assert finished_pages[0]["fields"]["next_page_number"] == 2
    assert finished_pages[1]["fields"]["records_extracted"] == 1
    assert finished_pages[1]["fields"]["entities_extracted"] == 1
    assert finished_pages[1]["fields"]["has_next_page"] is False
    assert payloads[-1]["fields"]["records_extracted"] == 3
    assert payloads[-1]["fields"]["request_count"] == 2


def test_catalog_strategy_page_number_extracts_raw_pages_and_normalized_entities(tmp_path):
    plan = _build_plan(
        tmp_path,
        source_id="catalog_metadata",
        checkpoint_field="metadata_modified",
        checkpoint_strategy="max_value",
        page_size=2,
    )
    strategy, transport = _build_strategy(
        tmp_path,
        [
            ResponseSpec(
                200,
                {
                    "result": {
                        "results": [
                            {
                                "id": "dataset-1",
                                "title": "Dataset 1",
                                "metadata_modified": "2026-04-10T01:00:00Z",
                                "organization": {
                                    "id": "org-1",
                                    "title": "Org 1",
                                },
                                "groups": [{"id": "grp-1", "title": "Group 1"}],
                                "resources": [
                                    {
                                        "id": "res-1",
                                        "format": "CSV",
                                        "url": "https://example.invalid/files/res-1.csv",
                                        "metadata_modified": "2026-04-10T01:30:00Z",
                                    }
                                ],
                            },
                            {
                                "id": "dataset-2",
                                "title": "Dataset 2",
                                "metadata_modified": "2026-04-10T03:00:00Z",
                                "organization": {
                                    "id": "org-1",
                                    "title": "Org 1",
                                },
                            },
                        ]
                    }
                },
            ),
            ResponseSpec(
                200,
                {
                    "result": {
                        "results": [
                            {
                                "id": "dataset-3",
                                "title": "Dataset 3",
                                "metadata_modified": "2026-04-10T05:00:00Z",
                                "organization": {
                                    "id": "org-1",
                                    "title": "Org 1",
                                },
                                "resources": [
                                    {
                                        "id": "res-2",
                                        "format": "JSON",
                                        "url": "https://example.invalid/files/res-2.json",
                                        "metadata_modified": "2026-04-10T05:30:00Z",
                                    }
                                ],
                            }
                        ]
                    }
                },
            ),
        ],
    )

    result = strategy.extract(plan)
    handoff = strategy.build_normalization_handoff(plan, result)
    metadata = result.metadata_as_dict()

    assert transport.opened is True
    assert transport.closed is True
    assert len(transport.requests) == 2

    first_query = parse_qs(urlsplit(transport.requests[0].full_url()).query)
    second_query = parse_qs(urlsplit(transport.requests[1].full_url()).query)
    assert first_query["page"] == ["1"]
    assert first_query["page_size"] == ["2"]
    assert second_query["page"] == ["2"]
    assert second_query["page_size"] == ["2"]

    assert result.records_extracted == 7
    assert result.checkpoint_value == "2026-04-10T05:30:00Z"
    assert metadata["raw_page_count"] == "2"
    assert metadata["organizations_extracted"] == "1"
    assert metadata["groups_extracted"] == "1"
    assert metadata["datasets_extracted"] == "3"
    assert metadata["resources_extracted"] == "2"
    assert metadata["normalized_artifact_count"] == "4"

    raw_page_one = Path(result.artifacts[0].path)
    raw_page_two = Path(result.artifacts[1].path)
    assert raw_page_one == (
        tmp_path / "runtime" / "raw" / "example" / "catalog_metadata" / "pages" / "page-0001.json"
    )
    assert raw_page_two == (
        tmp_path / "runtime" / "raw" / "example" / "catalog_metadata" / "pages" / "page-0002.json"
    )

    assert [Path(artifact.path).name for artifact in handoff.artifacts] == [
        "organizations.jsonl",
        "groups.jsonl",
        "datasets.jsonl",
        "resources.jsonl",
    ]

    organization_records = _read_jsonl(
        tmp_path
        / "runtime"
        / "raw"
        / "example"
        / "catalog_metadata"
        / "normalized"
        / "organizations.jsonl"
    )
    resource_records = _read_jsonl(
        tmp_path
        / "runtime"
        / "raw"
        / "example"
        / "catalog_metadata"
        / "normalized"
        / "resources.jsonl"
    )
    dataset_records = _read_jsonl(
        tmp_path
        / "runtime"
        / "raw"
        / "example"
        / "catalog_metadata"
        / "normalized"
        / "datasets.jsonl"
    )

    assert len(organization_records) == 1
    assert organization_records[0]["entity_id"] == "org-1"
    assert organization_records[0]["entity_type"] == "organization"

    assert len(resource_records) == 2
    assert resource_records[0]["parent_entity_type"] == "dataset"
    assert resource_records[0]["parent_entity_id"] == "dataset-1"
    assert resource_records[0]["catalog_raw_artifact_path"] == str(raw_page_one)
    assert resource_records[1]["parent_entity_id"] == "dataset-3"

    assert [record["entity_id"] for record in dataset_records] == [
        "dataset-1",
        "dataset-2",
        "dataset-3",
    ]

    emitted_metadata = strategy.emit_metadata(plan, result)
    assert emitted_metadata["artifact_count"] == 6
    assert emitted_metadata["strategy_variant"] == "metadata_catalog"
    assert emitted_metadata["input_format"] == "jsonl"


def test_catalog_strategy_incremental_uses_checkpoint_param_and_filters_old_entities(tmp_path):
    plan = _build_plan(
        tmp_path,
        source_id="catalog_incremental",
        extraction_mode="incremental",
        checkpoint_field="metadata_modified",
        checkpoint_strategy="max_value",
        lookback_days=1,
        page_size=50,
    )
    CheckpointStore().save(plan, "2026-04-10T06:00:00Z")
    strategy, transport = _build_strategy(
        tmp_path,
        [
            ResponseSpec(
                200,
                {
                    "result": {
                        "results": [
                            {
                                "id": "dataset-old",
                                "title": "Old Dataset",
                                "metadata_modified": "2026-04-10T05:00:00Z",
                            },
                            {
                                "id": "dataset-new",
                                "title": "New Dataset",
                                "metadata_modified": "2026-04-10T08:00:00Z",
                                "resources": [
                                    {
                                        "id": "resource-new",
                                        "format": "CSV",
                                        "url": "https://example.invalid/files/resource-new.csv",
                                        "metadata_modified": "2026-04-10T09:00:00Z",
                                    }
                                ],
                            },
                        ]
                    }
                },
            )
        ],
    )

    result = strategy.extract(plan)
    metadata = result.metadata_as_dict()

    query = parse_qs(urlsplit(transport.requests[0].full_url()).query)
    assert query["metadata_modified"] == ["2026-04-09T06:00:00Z"]
    assert result.records_extracted == 2
    assert result.checkpoint_value == "2026-04-10T09:00:00Z"
    assert metadata["checkpoint_loaded"] == "true"
    assert metadata["datasets_extracted"] == "1"
    assert metadata["resources_extracted"] == "1"

    dataset_records = _read_jsonl(
        tmp_path
        / "runtime"
        / "raw"
        / "example"
        / "catalog_incremental"
        / "normalized"
        / "datasets.jsonl"
    )
    assert [record["entity_id"] for record in dataset_records] == ["dataset-new"]


def test_catalog_strategy_resource_catalog_uses_resource_root_results_for_handoff(tmp_path):
    plan = _build_plan(
        tmp_path,
        source_id="catalog_resources",
        variant="resource_catalog",
        pagination_type="none",
    )
    strategy, _ = _build_strategy(
        tmp_path,
        [
            ResponseSpec(
                200,
                {
                    "data": {
                        "results": [
                            {
                                "id": "resource-1",
                                "format": "CSV",
                                "url": "https://example.invalid/files/resource-1.csv",
                            },
                            {
                                "id": "resource-2",
                                "format": "JSON",
                                "url": "https://example.invalid/files/resource-2.json",
                            },
                        ]
                    }
                },
            )
        ],
    )

    result = strategy.extract(plan)
    handoff = strategy.build_normalization_handoff(plan, result)

    assert result.records_extracted == 2
    assert [Path(artifact.path).name for artifact in handoff.artifacts] == ["resources.jsonl"]

    resource_records = _read_jsonl(
        tmp_path
        / "runtime"
        / "raw"
        / "example"
        / "catalog_resources"
        / "normalized"
        / "resources.jsonl"
    )
    assert [record["entity_id"] for record in resource_records] == [
        "resource-1",
        "resource-2",
    ]
    assert all(record["entity_type"] == "resource" for record in resource_records)


def _build_strategy(
    tmp_path: Path,
    responses: list[ResponseSpec | Exception],
    *,
    sleeper=None,
    logger=None,
):
    transport = FakeTransport(responses)
    strategy = CatalogStrategy(
        transport_factory=lambda: transport,
        storage_layout_factory=lambda plan: _storage_layout(tmp_path),
        sleeper=sleeper or (lambda seconds: None),
        clock=lambda: 0.0,
        logger=logger,
    )
    return strategy, transport


def _build_plan(
    tmp_path: Path,
    *,
    source_id: str,
    variant: str = "metadata_catalog",
    pagination_type: str = "page_number",
    extraction_mode: str = "full_refresh",
    checkpoint_field: str | None = None,
    checkpoint_strategy: str = "none",
    lookback_days: int | None = None,
    page_size: int = 100,
) -> ExecutionPlan:
    source_config = _build_source_config(
        tmp_path,
        source_id=source_id,
        variant=variant,
        pagination_type=pagination_type,
        extraction_mode=extraction_mode,
        checkpoint_field=checkpoint_field,
        checkpoint_strategy=checkpoint_strategy,
        lookback_days=lookback_days,
        page_size=page_size,
    )
    run_context = RunContext.create(
        run_id=f"run-{source_id}",
        environment="local",
        project_root=tmp_path,
        started_at=datetime(2026, 4, 10, 12, 0, tzinfo=UTC),
    )
    return ExecutionPlan.from_source_config(source_config, run_context)


def _build_source_config(
    tmp_path: Path,
    *,
    source_id: str,
    variant: str = "metadata_catalog",
    pagination_type: str = "page_number",
    extraction_mode: str = "full_refresh",
    checkpoint_field: str | None = None,
    checkpoint_strategy: str = "none",
    lookback_days: int | None = None,
    page_size: int = 100,
) -> SourceConfig:
    return SourceConfig.from_mapping(
        {
            "source_id": source_id,
            "name": source_id,
            "owner": "janus",
            "enabled": True,
            "source_type": "catalog",
            "strategy": "catalog",
            "strategy_variant": variant,
            "federation_level": "federal",
            "domain": "example",
            "public_access": True,
            "access": {
                "base_url": "https://example.invalid",
                "path": "/catalog",
                "method": "GET",
                "format": "json",
                "timeout_seconds": 30,
                "auth": {"type": "none"},
                "pagination": _pagination_block(pagination_type, page_size),
                "rate_limit": {
                    "requests_per_minute": 10,
                    "concurrency": 1,
                    "backoff_seconds": 5,
                },
            },
            "extraction": {
                "mode": extraction_mode,
                "checkpoint_field": checkpoint_field,
                "checkpoint_strategy": checkpoint_strategy,
                "lookback_days": lookback_days,
                "retry": {
                    "max_attempts": 3,
                    "backoff_strategy": "fixed",
                    "backoff_seconds": 1,
                },
            },
            "schema": {"mode": "infer"},
            "spark": {
                "input_format": "jsonl",
                "write_mode": "append",
            },
            "outputs": {
                "raw": {"path": f"data/raw/example/{source_id}", "format": "json"},
                "bronze": {"path": f"data/bronze/example/{source_id}", "format": "iceberg"},
                "metadata": {"path": f"data/metadata/example/{source_id}", "format": "json"},
            },
            "quality": {"allow_schema_evolution": True},
        },
        tmp_path / "conf" / "sources" / f"{source_id}.yaml",
    )


def _pagination_block(pagination_type: str, page_size: int) -> dict[str, Any]:
    if pagination_type == "page_number":
        return {
            "type": "page_number",
            "page_param": "page",
            "size_param": "page_size",
            "page_size": page_size,
        }
    if pagination_type == "offset":
        return {
            "type": "offset",
            "offset_param": "offset",
            "limit_param": "limit",
            "page_size": page_size,
        }
    if pagination_type == "cursor":
        return {
            "type": "cursor",
            "cursor_param": "cursor",
        }
    return {"type": "none"}


def _storage_layout(tmp_path: Path) -> StorageLayout:
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


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]
