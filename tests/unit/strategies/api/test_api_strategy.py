from __future__ import annotations

import json
import threading
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

import janus.strategies.api.http as http_module
from janus.checkpoints import CheckpointStore
from janus.models import ExecutionPlan, RunContext, SourceConfig
from janus.planner import StrategyCatalog
from janus.strategies.api import ApiHook, ApiResponse, ApiStrategy
from janus.utils.logging import build_structured_logger
from janus.utils.storage import StorageLayout


@dataclass(frozen=True, slots=True)
class ResponseSpec:
    status_code: int
    payload: Any
    headers: dict[str, str] = field(default_factory=dict)
    format_name: str = "json"


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

        if response.format_name == "json":
            body = json.dumps(response.payload).encode("utf-8")
        elif response.format_name == "jsonl":
            body = "\n".join(json.dumps(item) for item in response.payload).encode("utf-8")
        elif response.format_name == "text":
            body = str(response.payload).encode("utf-8")
        elif response.format_name == "binary":
            body = bytes(response.payload)
        else:
            raise AssertionError(f"Unsupported fake format: {response.format_name}")

        return ApiResponse(
            request=request,
            status_code=response.status_code,
            body=body,
            headers=tuple(sorted(response.headers.items())),
        )


class WindowHook(ApiHook):
    def checkpoint_params(self, plan: ExecutionPlan, checkpoint_value: str) -> dict[str, str]:
        del plan
        return {"since": checkpoint_value}

    def extract_records(self, plan, request, response, payload):
        del plan
        del request
        del response
        return payload["payload"]["rows"]


@dataclass(slots=True)
class ConcurrentPageState:
    payloads: dict[int, list[dict[str, str]]]
    requests: list = field(default_factory=list)
    start_times: dict[int, float] = field(default_factory=dict)
    active_requests: int = 0
    max_active_requests: int = 0
    first_page_started: threading.Event = field(default_factory=threading.Event)
    second_page_started: threading.Event = field(default_factory=threading.Event)
    lock: threading.Lock = field(default_factory=threading.Lock)


class ConcurrentPageTransport:
    def __init__(self, state: ConcurrentPageState) -> None:
        self.state = state

    def open(self) -> None:
        return None

    def close(self) -> None:
        return None

    def send(self, request):
        query = parse_qs(urlsplit(request.full_url()).query)
        page_number = int(query["page"][0])
        started_at = time.monotonic()
        with self.state.lock:
            self.state.requests.append(request)
            self.state.start_times[page_number] = started_at
            self.state.active_requests += 1
            self.state.max_active_requests = max(
                self.state.max_active_requests,
                self.state.active_requests,
            )

        try:
            if page_number == 1:
                self.state.first_page_started.set()
                assert self.state.second_page_started.wait(timeout=1)
                time.sleep(0.2)
            elif page_number == 2:
                self.state.second_page_started.set()
                time.sleep(0.15)
            else:
                time.sleep(0.02)

            payload = {"records": self.state.payloads.get(page_number, [])}
            return ApiResponse(
                request=request,
                status_code=200,
                body=json.dumps(payload).encode("utf-8"),
            )
        finally:
            with self.state.lock:
                self.state.active_requests -= 1


def test_urllib_transport_prefers_explicit_or_environment_ca_bundle(tmp_path, monkeypatch):
    explicit_bundle = tmp_path / "explicit-ca.pem"
    env_bundle = tmp_path / "env-ca.pem"
    explicit_bundle.write_text("explicit", encoding="utf-8")
    env_bundle.write_text("env", encoding="utf-8")

    monkeypatch.setenv("JANUS_CA_BUNDLE", str(env_bundle))
    monkeypatch.setenv("SSL_CERT_FILE", "/tmp/ignored-ssl-cert-file.pem")
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "/tmp/ignored-requests-ca-bundle.pem")

    assert http_module._resolve_ca_bundle(str(explicit_bundle)) == str(explicit_bundle)
    assert http_module._resolve_ca_bundle() == str(env_bundle)


def test_default_strategy_catalog_uses_api_strategy_for_api_variants(tmp_path):
    source_config = _build_source_config(tmp_path, source_id="catalog_api")

    binding = StrategyCatalog.with_defaults().resolve(source_config)

    assert isinstance(binding.strategy, ApiStrategy)
    assert binding.dispatch_path == "api.page_number_api"


def test_api_strategy_logs_page_progress(tmp_path):
    stream = StringIO()
    logger = build_structured_logger("janus.tests.api.progress", stream=stream)
    plan = _build_plan(tmp_path, source_id="logged_page_source", page_size=2)
    strategy, _transport = _build_strategy(
        tmp_path,
        [
            ResponseSpec(200, {"records": [{"id": "1"}, {"id": "2"}]}),
            ResponseSpec(200, {"records": [{"id": "3"}]}),
        ],
        logger=logger,
    )

    strategy.extract(plan)

    payloads = [json.loads(line) for line in stream.getvalue().splitlines()]
    events = [payload["event"] for payload in payloads]
    finished_pages = [
        payload for payload in payloads if payload["event"] == "api_request_finished"
    ]

    assert events[0] == "api_extraction_started"
    assert events[-1] == "api_extraction_finished"
    assert [page["fields"]["page_number"] for page in finished_pages] == [1, 2]
    assert finished_pages[0]["fields"]["records_extracted"] == 2
    assert finished_pages[0]["fields"]["has_next_page"] is True
    assert finished_pages[0]["fields"]["next_page_number"] == 2
    assert finished_pages[1]["fields"]["records_extracted"] == 1
    assert finished_pages[1]["fields"]["has_next_page"] is False


def test_api_strategy_page_number_extracts_raw_pages_and_tracks_checkpoint(tmp_path):
    plan = _build_plan(
        tmp_path,
        source_id="page_source",
        extraction_mode="incremental",
        checkpoint_field="updated_at",
        checkpoint_strategy="max_value",
        lookback_days=1,
        page_size=2,
    )
    CheckpointStore().save(plan, "2026-04-09T12:00:00Z")

    strategy, transport = _build_strategy(
        tmp_path,
        [
            ResponseSpec(
                200,
                {
                    "records": [
                        {"id": "1", "updated_at": "2026-04-10T01:00:00Z"},
                        {"id": "2", "updated_at": "2026-04-10T03:00:00Z"},
                    ]
                },
            ),
            ResponseSpec(
                200,
                {
                    "records": [
                        {"id": "3", "updated_at": "2026-04-10T05:00:00Z"},
                    ]
                },
            ),
        ],
    )

    result = strategy.extract(plan)
    metadata = result.metadata_as_dict()

    assert transport.opened is True
    assert transport.closed is True
    assert len(transport.requests) == 2

    first_query = parse_qs(urlsplit(transport.requests[0].full_url()).query)
    second_query = parse_qs(urlsplit(transport.requests[1].full_url()).query)
    assert first_query["page"] == ["1"]
    assert first_query["page_size"] == ["2"]
    assert first_query["updated_at"] == ["2026-04-08T12:00:00Z"]
    assert second_query["page"] == ["2"]
    assert second_query["page_size"] == ["2"]

    assert result.records_extracted == 3
    assert result.checkpoint_value == "2026-04-10T05:00:00Z"
    assert metadata["request_count"] == "2"
    assert metadata["retry_count"] == "0"
    assert metadata["checkpoint_loaded"] == "true"

    first_artifact = Path(result.artifacts[0].path)
    second_artifact = Path(result.artifacts[1].path)
    assert first_artifact == (
        tmp_path / "runtime" / "raw" / "example" / "page_source" / "pages" / "page-0001.json"
    )
    assert second_artifact == (
        tmp_path / "runtime" / "raw" / "example" / "page_source" / "pages" / "page-0002.json"
    )
    assert json.loads(first_artifact.read_text(encoding="utf-8"))["records"][0]["id"] == "1"

    emitted_metadata = strategy.emit_metadata(plan, result)
    assert emitted_metadata["artifact_count"] == 2
    assert emitted_metadata["pagination_type"] == "page_number"


def test_api_strategy_offset_pagination_reuses_offset_parameters_from_config(tmp_path):
    plan = _build_plan(
        tmp_path,
        source_id="offset_source",
        variant="offset_api",
        pagination_type="offset",
        page_size=2,
    )
    strategy, transport = _build_strategy(
        tmp_path,
        [
            ResponseSpec(200, {"records": [{"id": "1"}, {"id": "2"}]}),
            ResponseSpec(200, {"records": [{"id": "3"}]}),
        ],
    )

    result = strategy.extract(plan)

    first_query = parse_qs(urlsplit(transport.requests[0].full_url()).query)
    second_query = parse_qs(urlsplit(transport.requests[1].full_url()).query)
    assert first_query["offset"] == ["0"]
    assert first_query["limit"] == ["2"]
    assert second_query["offset"] == ["2"]
    assert second_query["limit"] == ["2"]

    assert result.records_extracted == 3
    assert Path(result.artifacts[0].path).name == "offset-00000000.json"
    assert Path(result.artifacts[1].path).name == "offset-00000002.json"


def test_api_strategy_page_number_supports_bounded_concurrency(tmp_path):
    plan = _build_plan(
        tmp_path,
        source_id="concurrent_page_source",
        page_size=2,
        requests_per_minute=600,
        concurrency=2,
    )
    state = ConcurrentPageState(
        payloads={
            1: [{"id": "1"}, {"id": "2"}],
            2: [{"id": "3"}, {"id": "4"}],
            3: [{"id": "5"}],
            4: [],
        }
    )
    strategy = ApiStrategy(
        transport_factory=lambda: ConcurrentPageTransport(state),
        storage_layout_factory=lambda plan: _storage_layout(tmp_path),
    )

    result = strategy.extract(plan)

    assert state.max_active_requests >= 2
    assert state.start_times[2] > state.start_times[1]
    assert state.start_times[2] - state.start_times[1] >= 0.08
    assert len(result.artifacts) == 3
    assert [Path(artifact.path).name for artifact in result.artifacts] == [
        "page-0001.json",
        "page-0002.json",
        "page-0003.json",
    ]
    assert result.records_extracted == 5


def test_api_strategy_retries_transient_failures_with_bounded_backoff(tmp_path):
    plan = _build_plan(
        tmp_path,
        source_id="retry_source",
        retry_max_attempts=3,
        retry_backoff_seconds=2,
        retry_backoff_strategy="exponential",
        rate_limit_backoff_seconds=3,
        requests_per_minute=None,
        page_size=2,
    )
    sleeps: list[float] = []
    strategy, transport = _build_strategy(
        tmp_path,
        [
            ResponseSpec(503, {"error": "temporarily unavailable"}),
            ResponseSpec(503, {"error": "temporarily unavailable"}),
            ResponseSpec(200, {"records": [{"id": "1"}]}),
        ],
        sleeper=sleeps.append,
    )

    result = strategy.extract(plan)
    metadata = result.metadata_as_dict()

    assert sleeps == [2.0, 3.0]
    assert len(transport.requests) == 3
    assert metadata["attempt_count"] == "3"
    assert metadata["retry_count"] == "2"
    assert metadata["request_count"] == "1"


def test_api_strategy_hook_can_override_checkpoint_params_and_record_extraction(tmp_path):
    plan = _build_plan(
        tmp_path,
        source_id="hooked_source",
        variant="date_window_api",
        pagination_type="none",
        extraction_mode="incremental",
        checkpoint_field="updated_at",
        checkpoint_strategy="max_value",
        lookback_days=2,
    )
    CheckpointStore().save(plan, "2026-04-10T06:00:00Z")
    strategy, transport = _build_strategy(
        tmp_path,
        [
            ResponseSpec(
                200,
                {
                    "payload": {
                        "rows": [
                            {"id": "99", "updated_at": "2026-04-11T00:00:00Z"},
                        ]
                    }
                },
            )
        ],
    )

    result = strategy.extract(plan, hook=WindowHook())

    query = parse_qs(urlsplit(transport.requests[0].full_url()).query)
    assert query["since"] == ["2026-04-08T06:00:00Z"]
    assert "updated_at" not in query
    assert result.records_extracted == 1
    assert result.checkpoint_value == "2026-04-11T00:00:00Z"


def _build_strategy(
    tmp_path: Path,
    responses: list[ResponseSpec | Exception],
    *,
    sleeper=None,
    logger=None,
):
    transport = FakeTransport(responses)
    strategy = ApiStrategy(
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
    variant: str = "page_number_api",
    pagination_type: str = "page_number",
    extraction_mode: str = "full_refresh",
    checkpoint_field: str | None = None,
    checkpoint_strategy: str = "none",
    lookback_days: int | None = None,
    page_size: int = 100,
    retry_max_attempts: int = 3,
    retry_backoff_seconds: int = 1,
    retry_backoff_strategy: str = "fixed",
    rate_limit_backoff_seconds: int | None = 5,
    requests_per_minute: int | None = 10,
    concurrency: int = 1,
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
        retry_max_attempts=retry_max_attempts,
        retry_backoff_seconds=retry_backoff_seconds,
        retry_backoff_strategy=retry_backoff_strategy,
        rate_limit_backoff_seconds=rate_limit_backoff_seconds,
        requests_per_minute=requests_per_minute,
        concurrency=concurrency,
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
    variant: str = "page_number_api",
    pagination_type: str = "page_number",
    extraction_mode: str = "full_refresh",
    checkpoint_field: str | None = None,
    checkpoint_strategy: str = "none",
    lookback_days: int | None = None,
    page_size: int = 100,
    retry_max_attempts: int = 3,
    retry_backoff_seconds: int = 1,
    retry_backoff_strategy: str = "fixed",
    rate_limit_backoff_seconds: int | None = 5,
    requests_per_minute: int | None = 10,
    concurrency: int = 1,
) -> SourceConfig:
    return SourceConfig.from_mapping(
        {
            "source_id": source_id,
            "name": source_id,
            "owner": "janus",
            "enabled": True,
            "source_type": "api",
            "strategy": "api",
            "strategy_variant": variant,
            "federation_level": "federal",
            "domain": "example",
            "public_access": True,
            "access": {
                "base_url": "https://example.invalid",
                "path": "/records",
                "method": "GET",
                "format": "json",
                "timeout_seconds": 30,
                "auth": {"type": "none"},
                "pagination": _pagination_block(pagination_type, page_size),
                "rate_limit": {
                    "requests_per_minute": requests_per_minute,
                    "concurrency": concurrency,
                    "backoff_seconds": rate_limit_backoff_seconds,
                },
            },
            "extraction": {
                "mode": extraction_mode,
                "checkpoint_field": checkpoint_field,
                "checkpoint_strategy": checkpoint_strategy,
                "lookback_days": lookback_days,
                "retry": {
                    "max_attempts": retry_max_attempts,
                    "backoff_strategy": retry_backoff_strategy,
                    "backoff_seconds": retry_backoff_seconds,
                },
            },
            "schema": {"mode": "infer"},
            "spark": {
                "input_format": "json",
                "write_mode": "append",
            },
            "outputs": {
                "raw": {"path": f"data/raw/example/{source_id}", "format": "json"},
                "bronze": {"path": f"data/bronze/example/{source_id}", "format": "parquet"},
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
