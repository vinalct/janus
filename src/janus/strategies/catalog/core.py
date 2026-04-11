from __future__ import annotations

import os
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

from janus.checkpoints import CheckpointState, CheckpointStore
from janus.models import (
    ExecutionPlan,
    ExtractedArtifact,
    ExtractionResult,
    SourceConfig,
    WriteResult,
)
from janus.strategies.api import (
    ApiClient,
    ApiRequest,
    ApiResponse,
    ApiTransport,
    ApiTransportError,
    AuthResolutionError,
    PaginationState,
    UrllibApiTransport,
    build_paginator,
    inject_auth,
)
from janus.strategies.base import BaseStrategy, SourceHook
from janus.utils.logging import StructuredLogger, redact_url
from janus.utils.runtime import load_environment_config
from janus.utils.storage import StorageLayout
from janus.writers import RawArtifactWriter

RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})
SUPPORTED_CATALOG_PAYLOAD_FORMATS = frozenset({"json"})
SUPPORTED_CATALOG_INPUT_FORMATS = frozenset({"jsonl"})
WRAPPER_CONTAINER_KEYS = ("result", "data", "payload", "response", "value")
GENERIC_COLLECTION_KEYS = ("results", "items")
ENTITY_FILE_NAMES = {
    "organization": "organizations",
    "group": "groups",
    "dataset": "datasets",
    "resource": "resources",
}
ENTITY_TYPE_ORDER = ("organization", "group", "dataset", "resource")
ROOT_ENTITY_PRIORITY = {
    "metadata_catalog": ("dataset", "organization", "group", "resource"),
    "resource_catalog": ("resource", "dataset", "organization", "group"),
}
COLLECTION_ALIASES = {
    "organization": ("organizations", "organization", "organization_list"),
    "group": ("groups", "group", "group_list"),
    "dataset": ("datasets", "dataset", "packages", "package"),
    "resource": ("resources", "resource"),
}
IDENTIFIER_KEYS = ("id", "identifier", "name", "slug", "guid", "uri", "url")
NAME_KEYS = ("name", "display_name", "label")
TITLE_KEYS = ("title", "display_name", "label", "name")
DESCRIPTION_KEYS = ("description", "notes", "summary", "excerpt")
URL_KEYS = ("url", "uri", "homepage", "download_url")
FORMAT_KEYS = ("format", "mimetype", "media_type", "type")
CREATED_AT_KEYS = ("created", "metadata_created", "issued", "publication_date")
UPDATED_AT_KEYS = (
    "updated",
    "metadata_modified",
    "modified",
    "revision_timestamp",
    "last_updated",
)
STATE_KEYS = ("state", "status", "visibility")
DATASET_HINT_KEYS = (
    "resources",
    "organization",
    "groups",
    "metadata_created",
    "metadata_modified",
    "owner_org",
    "notes",
    "private",
)
RESOURCE_HINT_KEYS = ("format", "mimetype", "download_url", "url_type", "resource_type")
ORGANIZATION_HINT_KEYS = ("packages", "dataset_count", "package_count", "image_url")
GROUP_HINT_KEYS = ("packages", "dataset_count", "package_count")


class CatalogStrategyError(RuntimeError):
    """Base failure for metadata/catalog strategy execution."""


class CatalogResponseError(CatalogStrategyError):
    """Raised when a catalog request finished with a non-success response."""

    def __init__(self, response: ApiResponse) -> None:
        self.response = response
        message = (
            f"Catalog request failed with status {response.status_code} for "
            f"{redact_url(response.request.full_url())}"
        )
        super().__init__(message)


class CatalogPayloadError(CatalogStrategyError):
    """Raised when a catalog payload cannot be decoded or traversed safely."""


class CatalogHook(SourceHook):
    """Catalog-specific hook points for wrapper quirks and source-local iteration details."""

    def prepare_request(
        self,
        plan: ExecutionPlan,
        request: ApiRequest,
        *,
        checkpoint_state: CheckpointState | None = None,
        pagination_state: PaginationState | None = None,
    ) -> ApiRequest:
        del plan
        del checkpoint_state
        del pagination_state
        return request

    def handle_response(
        self,
        plan: ExecutionPlan,
        request: ApiRequest,
        response: ApiResponse,
    ) -> ApiResponse:
        del plan
        del request
        return response

    def transform_payload(
        self,
        plan: ExecutionPlan,
        request: ApiRequest,
        response: ApiResponse,
        payload: Any,
    ) -> Any:
        del plan
        del request
        del response
        return payload

    def checkpoint_params(
        self,
        plan: ExecutionPlan,
        checkpoint_value: str,
    ) -> Mapping[str, str] | None:
        del plan
        del checkpoint_value
        return None

    def page_records(
        self,
        plan: ExecutionPlan,
        request: ApiRequest,
        response: ApiResponse,
        payload: Any,
    ) -> Sequence[Any] | None:
        del plan
        del request
        del response
        del payload
        return None


@dataclass(frozen=True, slots=True)
class CatalogEntityReference:
    """Minimal parent reference attached to nested catalog records."""

    entity_type: str
    entity_key: str
    entity_id: str | None = None
    entity_name: str | None = None


@dataclass(frozen=True, slots=True)
class CatalogBatch:
    """One entity collection discovered in a catalog payload."""

    entity_type: str
    records: tuple[dict[str, Any], ...]
    collection_path: str

    def __post_init__(self) -> None:
        if self.entity_type not in ENTITY_FILE_NAMES:
            allowed = ", ".join(sorted(ENTITY_FILE_NAMES))
            raise ValueError(f"entity_type must be one of: {allowed}")
        if not self.collection_path.strip():
            raise ValueError("collection_path must not be empty")


@dataclass(slots=True)
class CatalogRequestThrottle:
    """Single-threaded request throttle reused by metadata/catalog requests."""

    requests_per_minute: int | None
    clock: Callable[[], float]
    sleeper: Callable[[float], None]
    _next_allowed_at: float | None = None

    def wait_for_turn(self) -> None:
        if self.requests_per_minute is None:
            return

        interval_seconds = 60 / self.requests_per_minute
        now = self.clock()
        if self._next_allowed_at is not None and now < self._next_allowed_at:
            self.sleeper(self._next_allowed_at - now)
            now = self._next_allowed_at
        self._next_allowed_at = now + interval_seconds


@dataclass(slots=True)
class CatalogStrategy(BaseStrategy):
    """Reusable metadata-first strategy for public dataset catalogs."""

    transport_factory: Callable[[], ApiTransport] = UrllibApiTransport
    storage_layout_factory: Callable[[ExecutionPlan], StorageLayout] = field(
        default_factory=lambda: _default_storage_layout
    )
    raw_writer_factory: Callable[[StorageLayout], RawArtifactWriter] = RawArtifactWriter
    checkpoint_store: CheckpointStore = field(default_factory=CheckpointStore)
    env_reader: Callable[[str], str | None] = os.getenv
    sleeper: Callable[[float], None] = time.sleep
    clock: Callable[[], float] = time.monotonic
    logger: StructuredLogger | None = None

    @property
    def strategy_family(self) -> str:
        return "catalog"

    def plan(
        self,
        source_config: SourceConfig,
        run_context,
        hook: SourceHook | None = None,
    ) -> ExecutionPlan:
        self._validate_source_config(source_config)
        plan = ExecutionPlan.from_source_config(source_config, run_context)
        plan = plan.with_note("strategy_family:catalog")
        plan = plan.with_note(f"strategy_variant:{source_config.strategy_variant}")
        if hook is not None:
            return hook.on_plan(plan)
        return plan

    def extract(
        self,
        plan: ExecutionPlan,
        hook: SourceHook | None = None,
    ) -> ExtractionResult:
        catalog_hook = hook if isinstance(hook, CatalogHook) else None
        storage_layout = self.storage_layout_factory(plan)
        raw_writer = self.raw_writer_factory(storage_layout)
        checkpoint_state = self.checkpoint_store.load(plan)
        base_request = self._build_base_request(plan, checkpoint_state, catalog_hook)
        paginator = build_paginator(plan.source_config.access.pagination)
        pagination_state = paginator.initial_state(base_request)
        throttle = CatalogRequestThrottle(
            requests_per_minute=plan.source_config.access.rate_limit.requests_per_minute,
            clock=self.clock,
            sleeper=self.sleeper,
        )
        logger = self._bind_logger(plan)

        raw_artifacts: list[ExtractedArtifact] = []
        normalized_records: dict[str, list[dict[str, Any]]] = {
            entity_type: [] for entity_type in ENTITY_TYPE_ORDER
        }
        entity_indexes: dict[tuple[str, str], int] = {}
        checkpoint_value: str | None = None
        successful_requests = 0
        total_attempts = 0
        page_record_total = 0

        with ApiClient(self.transport_factory()) as client:
            while pagination_state is not None:
                request = paginator.apply(base_request, pagination_state)
                if catalog_hook is not None:
                    request = catalog_hook.prepare_request(
                        plan,
                        request,
                        checkpoint_state=checkpoint_state,
                        pagination_state=pagination_state,
                    )

                response, attempts_used = self._send_with_retries(
                    plan,
                    client,
                    request,
                    throttle,
                    logger,
                )
                total_attempts += attempts_used
                successful_requests += 1

                if catalog_hook is not None:
                    response = catalog_hook.handle_response(plan, request, response)

                payload = self._decode_payload(plan, response)
                if catalog_hook is not None:
                    payload = catalog_hook.transform_payload(plan, request, response, payload)

                persisted = self._persist_raw_payload(
                    plan,
                    raw_writer,
                    response=response,
                    payload=payload,
                    pagination_state=pagination_state,
                )
                raw_artifacts.append(persisted.artifact)

                primary_batch_size = self._page_record_count(
                    plan,
                    request,
                    response,
                    payload,
                    hook=catalog_hook,
                )
                page_record_total += primary_batch_size
                checkpoint_value = self._collect_catalog_entities(
                    plan,
                    payload=payload,
                    request=request,
                    response=response,
                    pagination_state=pagination_state,
                    raw_artifact=persisted.artifact,
                    checkpoint_state=checkpoint_state,
                    normalized_records=normalized_records,
                    entity_indexes=entity_indexes,
                    current_checkpoint_value=checkpoint_value,
                )
                pagination_state = paginator.next_state(
                    pagination_state,
                    records_extracted=primary_batch_size,
                    payload=payload,
                )

        normalized_artifacts = self._persist_normalized_records(
            plan,
            raw_writer,
            normalized_records,
        )
        records_extracted = sum(len(records) for records in normalized_records.values())

        extraction_result = ExtractionResult.from_plan(
            plan,
            tuple(raw_artifacts + normalized_artifacts),
            records_extracted=records_extracted,
            checkpoint_value=checkpoint_value,
            metadata={
                "request_count": str(successful_requests),
                "retry_count": str(max(total_attempts - successful_requests, 0)),
                "attempt_count": str(total_attempts),
                "pagination_type": plan.source_config.access.pagination.type,
                "checkpoint_loaded": str(checkpoint_state is not None).lower(),
                "raw_page_count": str(len(raw_artifacts)),
                "page_record_count": str(page_record_total),
                "normalized_artifact_count": str(len(normalized_artifacts)),
                "organizations_extracted": str(len(normalized_records["organization"])),
                "groups_extracted": str(len(normalized_records["group"])),
                "datasets_extracted": str(len(normalized_records["dataset"])),
                "resources_extracted": str(len(normalized_records["resource"])),
                "entity_types_emitted": ",".join(
                    entity_type
                    for entity_type in ENTITY_TYPE_ORDER
                    if normalized_records[entity_type]
                ),
            },
        )
        if hook is not None:
            return hook.on_extraction_result(plan, extraction_result)
        return extraction_result

    def build_normalization_handoff(
        self,
        plan: ExecutionPlan,
        extraction_result: ExtractionResult,
        hook: SourceHook | None = None,
    ) -> ExtractionResult:
        handoff_artifacts = tuple(
            artifact
            for artifact in extraction_result.artifacts
            if artifact.format == plan.source_config.spark.input_format
        )
        if not handoff_artifacts:
            raise CatalogStrategyError(
                "No normalized catalog artifacts match the configured "
                f"spark.input_format {plan.source_config.spark.input_format!r}"
            )

        handoff = replace(extraction_result, artifacts=handoff_artifacts).with_metadata(
            "normalization_artifact_count",
            str(len(handoff_artifacts)),
        )
        if hook is not None:
            return hook.on_normalization_handoff(plan, handoff)
        return handoff

    def emit_metadata(
        self,
        plan: ExecutionPlan,
        extraction_result: ExtractionResult,
        write_results: tuple[WriteResult, ...] = (),
        hook: SourceHook | None = None,
    ) -> Mapping[str, Any]:
        metadata: dict[str, Any] = {
            "strategy_family": self.strategy_family,
            "strategy_variant": plan.source.strategy_variant,
            "pagination_type": plan.source_config.access.pagination.type,
            "request_timeout_seconds": plan.source_config.access.timeout_seconds,
            "input_format": plan.source_config.spark.input_format,
            "artifact_count": len(extraction_result.artifacts),
            "records_extracted": extraction_result.records_extracted or 0,
            "checkpoint_value": extraction_result.checkpoint_value or "",
            "write_result_count": len(write_results),
        }
        metadata.update(extraction_result.metadata_as_dict())
        if hook is not None:
            metadata.update(hook.metadata_fields(plan, extraction_result, write_results))
        return metadata

    def _validate_source_config(self, source_config: SourceConfig) -> None:
        if source_config.access.format not in SUPPORTED_CATALOG_PAYLOAD_FORMATS:
            allowed = ", ".join(sorted(SUPPORTED_CATALOG_PAYLOAD_FORMATS))
            raise ValueError(f"Catalog access.format must be one of: {allowed}")
        if source_config.outputs.raw.format not in SUPPORTED_CATALOG_PAYLOAD_FORMATS:
            allowed = ", ".join(sorted(SUPPORTED_CATALOG_PAYLOAD_FORMATS))
            raise ValueError(f"Catalog outputs.raw.format must be one of: {allowed}")
        if source_config.spark.input_format not in SUPPORTED_CATALOG_INPUT_FORMATS:
            allowed = ", ".join(sorted(SUPPORTED_CATALOG_INPUT_FORMATS))
            raise ValueError(f"Catalog spark.input_format must be one of: {allowed}")

    def _build_base_request(
        self,
        plan: ExecutionPlan,
        checkpoint_state: CheckpointState | None,
        catalog_hook: CatalogHook | None,
    ) -> ApiRequest:
        source_access = plan.source_config.access
        request = ApiRequest(
            method=source_access.method,
            url=_resolve_url(plan.source_config),
            timeout_seconds=source_access.timeout_seconds,
            headers=_freeze_string_mapping(source_access.headers or {}),
            params=_freeze_string_mapping(source_access.params or {}),
        )
        request = inject_auth(request, source_access.auth, env_reader=self._resolve_env_var)

        checkpoint_value = _checkpoint_request_value(plan, checkpoint_state)
        checkpoint_params = _default_checkpoint_params(plan, checkpoint_value)
        if catalog_hook is not None and checkpoint_value is not None:
            hook_checkpoint_params = catalog_hook.checkpoint_params(plan, checkpoint_value)
            if hook_checkpoint_params is not None:
                checkpoint_params = _stringify_mapping(hook_checkpoint_params)

        if checkpoint_params:
            request = request.with_params(checkpoint_params)
        return request

    def _send_with_retries(
        self,
        plan: ExecutionPlan,
        client: ApiClient,
        request: ApiRequest,
        throttle: CatalogRequestThrottle,
        logger: StructuredLogger | None,
    ) -> tuple[ApiResponse, int]:
        retry_config = plan.source_config.extraction.retry
        last_transport_error: Exception | None = None

        for attempt in range(1, retry_config.max_attempts + 1):
            throttle.wait_for_turn()
            try:
                response = client.send(request)
            except (ApiTransportError, AuthResolutionError) as exc:
                last_transport_error = exc
                if attempt == retry_config.max_attempts:
                    raise CatalogStrategyError(str(exc)) from exc
                self._sleep_for_retry(plan, attempt, response=None, logger=logger)
                continue

            if 200 <= response.status_code < 300:
                return response, attempt

            if (
                response.status_code not in RETRYABLE_STATUS_CODES
                or attempt == retry_config.max_attempts
            ):
                raise CatalogResponseError(response)

            self._sleep_for_retry(plan, attempt, response=response, logger=logger)

        if last_transport_error is not None:
            raise CatalogStrategyError(str(last_transport_error)) from last_transport_error
        raise AssertionError("Retry loop exited without a response or error")

    def _sleep_for_retry(
        self,
        plan: ExecutionPlan,
        attempt: int,
        *,
        response: ApiResponse | None,
        logger: StructuredLogger | None,
    ) -> None:
        delay = _retry_delay_seconds(plan, attempt, response)
        if logger is not None:
            logger.warning(
                "catalog_retry_scheduled",
                attempt=attempt,
                delay_seconds=delay,
                status_code=response.status_code if response is not None else None,
            )
        self.sleeper(delay)

    def _decode_payload(self, plan: ExecutionPlan, response: ApiResponse) -> Any:
        if plan.source_config.access.format != "json":
            raise CatalogPayloadError(
                f"Unsupported catalog payload format: {plan.source_config.access.format}"
            )

        try:
            return response.json()
        except (UnicodeDecodeError, ValueError) as exc:
            raise CatalogPayloadError(
                f"Failed to decode catalog payload from {redact_url(response.request.full_url())}"
            ) from exc

    def _persist_raw_payload(
        self,
        plan: ExecutionPlan,
        raw_writer: RawArtifactWriter,
        *,
        response: ApiResponse,
        payload: Any,
        pagination_state: PaginationState,
    ):
        return raw_writer.write_json(
            plan,
            _raw_relative_path(pagination_state),
            payload,
            metadata={
                "request_url": response.request.full_url(),
                "status_code": str(response.status_code),
                "request_index": str(pagination_state.request_index),
            },
        )

    def _page_record_count(
        self,
        plan: ExecutionPlan,
        request: ApiRequest,
        response: ApiResponse,
        payload: Any,
        *,
        hook: CatalogHook | None,
    ) -> int:
        if hook is not None:
            hook_records = hook.page_records(plan, request, response, payload)
            if hook_records is not None:
                return len(tuple(hook_records))

        batches = _root_batches(payload, plan.source.strategy_variant, path="payload")
        if not batches:
            return 0
        return len(batches[0].records)

    def _collect_catalog_entities(
        self,
        plan: ExecutionPlan,
        *,
        payload: Any,
        request: ApiRequest,
        response: ApiResponse,
        pagination_state: PaginationState,
        raw_artifact: ExtractedArtifact,
        checkpoint_state: CheckpointState | None,
        normalized_records: dict[str, list[dict[str, Any]]],
        entity_indexes: dict[tuple[str, str], int],
        current_checkpoint_value: str | None,
    ) -> str | None:
        checkpoint_value = current_checkpoint_value
        for batch in _root_batches(payload, plan.source.strategy_variant, path="payload"):
            for index, record in enumerate(batch.records):
                checkpoint_value = self._collect_entity_tree(
                    plan,
                    entity_type=batch.entity_type,
                    record=record,
                    collection_path=batch.collection_path,
                    record_path=f"{batch.collection_path}[{index}]",
                    request=request,
                    response=response,
                    pagination_state=pagination_state,
                    raw_artifact=raw_artifact,
                    normalized_records=normalized_records,
                    entity_indexes=entity_indexes,
                    checkpoint_state=checkpoint_state,
                    current_checkpoint_value=checkpoint_value,
                )
        return checkpoint_value

    def _collect_entity_tree(
        self,
        plan: ExecutionPlan,
        *,
        entity_type: str,
        record: dict[str, Any],
        collection_path: str,
        record_path: str,
        request: ApiRequest,
        response: ApiResponse,
        pagination_state: PaginationState,
        raw_artifact: ExtractedArtifact,
        normalized_records: dict[str, list[dict[str, Any]]],
        entity_indexes: dict[tuple[str, str], int],
        checkpoint_state: CheckpointState | None,
        current_checkpoint_value: str | None,
        parent: CatalogEntityReference | None = None,
    ) -> str | None:
        checkpoint_value = current_checkpoint_value
        entity_reference = _build_entity_reference(entity_type, record, record_path)
        checkpoint_candidate = _checkpoint_candidate(plan, record)

        if not _should_skip_for_checkpoint(plan, checkpoint_state, checkpoint_candidate):
            normalized_record = _normalize_catalog_record(
                entity_type=entity_type,
                record=record,
                collection_path=collection_path,
                record_path=record_path,
                request=request,
                response=response,
                pagination_state=pagination_state,
                raw_artifact=raw_artifact,
                parent=parent,
            )
            _upsert_entity_record(
                plan,
                normalized_records,
                entity_indexes,
                entity_reference.entity_key,
                normalized_record,
            )
            if checkpoint_candidate is not None:
                checkpoint_value = _max_checkpoint_value(checkpoint_value, checkpoint_candidate)

        for child_batch in _nested_batches(
            record,
            variant=plan.source.strategy_variant,
            parent_type=entity_type,
            path=record_path,
        ):
            for index, child_record in enumerate(child_batch.records):
                checkpoint_value = self._collect_entity_tree(
                    plan,
                    entity_type=child_batch.entity_type,
                    record=child_record,
                    collection_path=child_batch.collection_path,
                    record_path=f"{child_batch.collection_path}[{index}]",
                    request=request,
                    response=response,
                    pagination_state=pagination_state,
                    raw_artifact=raw_artifact,
                    normalized_records=normalized_records,
                    entity_indexes=entity_indexes,
                    checkpoint_state=checkpoint_state,
                    current_checkpoint_value=checkpoint_value,
                    parent=entity_reference,
                )
        return checkpoint_value

    def _persist_normalized_records(
        self,
        plan: ExecutionPlan,
        raw_writer: RawArtifactWriter,
        normalized_records: Mapping[str, Sequence[dict[str, Any]]],
    ) -> list[ExtractedArtifact]:
        artifacts: list[ExtractedArtifact] = []
        for entity_type in ENTITY_TYPE_ORDER:
            records = normalized_records[entity_type]
            if not records:
                continue
            persisted = raw_writer.write_json_lines(
                plan,
                _normalized_relative_path(entity_type),
                records,
                metadata={
                    "entity_type": entity_type,
                    "record_count": str(len(records)),
                },
            )
            artifacts.append(persisted.artifact)
        return artifacts

    def _bind_logger(self, plan: ExecutionPlan) -> StructuredLogger | None:
        if self.logger is None:
            return None
        return self.logger.bind(
            run_id=plan.run_context.run_id,
            source_id=plan.source.source_id,
            strategy_family=self.strategy_family,
        )

    def _resolve_env_var(self, name: str) -> str | None:
        value = self.env_reader(name)
        if value is not None:
            return value
        return os.getenv(name)


def _default_storage_layout(plan: ExecutionPlan) -> StorageLayout:
    environment_config = load_environment_config(
        plan.run_context.environment,
        plan.run_context.project_root,
    )
    return StorageLayout.from_environment_config(
        environment_config,
        plan.run_context.project_root,
    )


def _resolve_url(source_config: SourceConfig) -> str:
    if source_config.access.url:
        return source_config.access.url

    base_url = (source_config.access.base_url or "").strip()
    path = (source_config.access.path or "").strip()
    if not base_url:
        raise ValueError("Catalog source requires access.base_url or access.url")
    if not path:
        return base_url
    return urljoin(base_url.rstrip("/") + "/", path.lstrip("/"))


def _default_checkpoint_params(
    plan: ExecutionPlan,
    checkpoint_value: str | None,
) -> dict[str, str]:
    if checkpoint_value is None or plan.checkpoint_field is None:
        return {}
    return {plan.checkpoint_field: checkpoint_value}


def _checkpoint_request_value(
    plan: ExecutionPlan,
    checkpoint_state: CheckpointState | None,
) -> str | None:
    if (
        plan.extraction_mode != "incremental"
        or plan.checkpoint_field is None
        or checkpoint_state is None
    ):
        return None

    value = checkpoint_state.checkpoint_value
    lookback_days = plan.source_config.extraction.lookback_days
    if not lookback_days:
        return value

    parsed_datetime = _parse_datetime(value)
    if parsed_datetime is None:
        return value
    return _format_datetime(parsed_datetime - timedelta(days=lookback_days))


def _retry_delay_seconds(
    plan: ExecutionPlan,
    attempt: int,
    response: ApiResponse | None,
) -> float:
    retry_config = plan.source_config.extraction.retry
    rate_limit_backoff = plan.source_config.access.rate_limit.backoff_seconds
    if retry_config.backoff_strategy == "exponential":
        delay = retry_config.backoff_seconds * (2 ** (attempt - 1))
    else:
        delay = retry_config.backoff_seconds

    maximum_delay = rate_limit_backoff or max(
        retry_config.backoff_seconds,
        retry_config.backoff_seconds * retry_config.max_attempts,
    )

    retry_after_header = None
    if response is not None:
        retry_after_header = response.headers_as_dict().get("Retry-After")
    if retry_after_header is not None:
        try:
            delay = max(delay, float(retry_after_header))
        except ValueError:
            pass
    return float(min(delay, maximum_delay))


def _raw_relative_path(pagination_state: PaginationState) -> Path:
    if pagination_state.page_number is not None:
        filename = f"page-{pagination_state.page_number:04d}.json"
    elif pagination_state.offset is not None:
        filename = f"offset-{pagination_state.offset:08d}.json"
    elif pagination_state.cursor is not None:
        filename = f"cursor-{pagination_state.request_index:04d}.json"
    else:
        filename = f"response-{pagination_state.request_index:04d}.json"
    return Path("pages") / filename


def _normalized_relative_path(entity_type: str) -> Path:
    return Path("normalized") / f"{ENTITY_FILE_NAMES[entity_type]}.jsonl"


def _root_batches(
    payload: Any,
    variant: str,
    *,
    path: str,
) -> tuple[CatalogBatch, ...]:
    if isinstance(payload, Mapping):
        direct_batches = _batches_from_mapping(
            payload, variant=variant, parent_type=None, path=path
        )
        if direct_batches:
            return direct_batches

        for key in WRAPPER_CONTAINER_KEYS:
            nested = payload.get(key)
            if isinstance(nested, Mapping):
                nested_batches = _root_batches(
                    nested,
                    variant,
                    path=f"{path}.{key}",
                )
                if nested_batches:
                    return nested_batches
        return ()

    records = _coerce_records(payload)
    if not records:
        return ()
    entity_type = _infer_entity_type(records, variant=variant, parent_type=None)
    return (CatalogBatch(entity_type=entity_type, records=records, collection_path=path),)


def _nested_batches(
    record: Mapping[str, Any],
    *,
    variant: str,
    parent_type: str,
    path: str,
) -> tuple[CatalogBatch, ...]:
    direct_batches = _batches_from_mapping(
        record, variant=variant, parent_type=parent_type, path=path
    )
    if direct_batches:
        return direct_batches

    nested_batches: list[CatalogBatch] = []
    for key in WRAPPER_CONTAINER_KEYS:
        nested = record.get(key)
        if not isinstance(nested, Mapping):
            continue
        nested_batches.extend(
            _batches_from_mapping(
                nested,
                variant=variant,
                parent_type=parent_type,
                path=f"{path}.{key}",
            )
        )
    return tuple(nested_batches)


def _batches_from_mapping(
    payload: Mapping[str, Any],
    *,
    variant: str,
    parent_type: str | None,
    path: str,
) -> tuple[CatalogBatch, ...]:
    batches: list[CatalogBatch] = []
    for entity_type in ROOT_ENTITY_PRIORITY.get(variant, ROOT_ENTITY_PRIORITY["metadata_catalog"]):
        for alias in COLLECTION_ALIASES[entity_type]:
            if alias not in payload:
                continue
            records = _coerce_records(payload[alias])
            if not records:
                continue
            batches.append(
                CatalogBatch(
                    entity_type=entity_type,
                    records=records,
                    collection_path=f"{path}.{alias}",
                )
            )
            break

    if batches:
        return tuple(batches)

    for alias in GENERIC_COLLECTION_KEYS:
        if alias not in payload:
            continue
        records = _coerce_records(payload[alias])
        if not records:
            continue
        entity_type = _infer_entity_type(records, variant=variant, parent_type=parent_type)
        return (
            CatalogBatch(
                entity_type=entity_type,
                records=records,
                collection_path=f"{path}.{alias}",
            ),
        )
    return ()


def _coerce_records(value: Any) -> tuple[dict[str, Any], ...]:
    if isinstance(value, Mapping):
        return (_normalize_mapping(value),)
    if not isinstance(value, Sequence) or isinstance(value, str | bytes | bytearray):
        return ()

    records: list[dict[str, Any]] = []
    for item in value:
        if isinstance(item, Mapping):
            records.append(_normalize_mapping(item))
            continue
        if item is None:
            continue
        records.append({"value": item, "name": str(item)})
    return tuple(records)


def _normalize_mapping(value: Mapping[str, Any]) -> dict[str, Any]:
    return {str(key): item for key, item in value.items()}


def _infer_entity_type(
    records: Sequence[Mapping[str, Any]],
    *,
    variant: str,
    parent_type: str | None,
) -> str:
    if parent_type == "dataset":
        return "resource"
    if parent_type in {"organization", "group"}:
        return "dataset"

    scores = {
        "organization": 0,
        "group": 0,
        "dataset": 0,
        "resource": 0,
    }
    for record in records[:5]:
        scores["dataset"] += _score_record(record, DATASET_HINT_KEYS)
        scores["resource"] += _score_record(record, RESOURCE_HINT_KEYS)
        scores["organization"] += _score_record(record, ORGANIZATION_HINT_KEYS)
        scores["group"] += _score_record(record, GROUP_HINT_KEYS)

    highest_score = max(scores.values())
    priority = ROOT_ENTITY_PRIORITY.get(variant, ROOT_ENTITY_PRIORITY["metadata_catalog"])
    if highest_score > 0:
        return max(
            priority,
            key=lambda entity_type: (scores[entity_type], -priority.index(entity_type)),
        )
    return priority[0]


def _score_record(record: Mapping[str, Any], keys: Sequence[str]) -> int:
    return sum(1 for key in keys if key in record and record.get(key) not in (None, "", (), [], {}))


def _build_entity_reference(
    entity_type: str,
    record: Mapping[str, Any],
    record_path: str,
) -> CatalogEntityReference:
    entity_id = _first_string(record, IDENTIFIER_KEYS)
    entity_name = _first_string(record, NAME_KEYS) or _first_string(record, TITLE_KEYS)
    entity_key = entity_id or entity_name or record_path
    return CatalogEntityReference(
        entity_type=entity_type,
        entity_key=entity_key,
        entity_id=entity_id,
        entity_name=entity_name,
    )


def _normalize_catalog_record(
    *,
    entity_type: str,
    record: Mapping[str, Any],
    collection_path: str,
    record_path: str,
    request: ApiRequest,
    response: ApiResponse,
    pagination_state: PaginationState,
    raw_artifact: ExtractedArtifact,
    parent: CatalogEntityReference | None,
) -> dict[str, Any]:
    entity_reference = _build_entity_reference(entity_type, record, record_path)
    return {
        "entity_type": entity_type,
        "entity_key": entity_reference.entity_key,
        "entity_id": entity_reference.entity_id,
        "entity_name": _first_string(record, NAME_KEYS),
        "entity_title": _first_string(record, TITLE_KEYS),
        "entity_description": _first_string(record, DESCRIPTION_KEYS),
        "entity_url": _first_string(record, URL_KEYS),
        "entity_format": _first_string(record, FORMAT_KEYS),
        "entity_created_at": _first_string(record, CREATED_AT_KEYS),
        "entity_updated_at": _first_string(record, UPDATED_AT_KEYS),
        "entity_state": _first_string(record, STATE_KEYS),
        "parent_entity_type": parent.entity_type if parent is not None else None,
        "parent_entity_key": parent.entity_key if parent is not None else None,
        "parent_entity_id": parent.entity_id if parent is not None else None,
        "parent_entity_name": parent.entity_name if parent is not None else None,
        "catalog_collection_path": collection_path,
        "catalog_record_path": record_path,
        "catalog_request_url": request.full_url(),
        "catalog_request_index": pagination_state.request_index,
        "catalog_page_number": pagination_state.page_number,
        "catalog_offset": pagination_state.offset,
        "catalog_cursor": pagination_state.cursor,
        "catalog_received_at": response.received_at.isoformat(),
        "catalog_raw_artifact_path": raw_artifact.path,
        "catalog_raw_artifact_checksum": raw_artifact.checksum,
        "catalog_payload": dict(record),
    }


def _first_string(record: Mapping[str, Any], keys: Sequence[str]) -> str | None:
    for key in keys:
        value = record.get(key)
        if value is None:
            continue
        normalized = str(value).strip()
        if normalized:
            return normalized
    return None


def _upsert_entity_record(
    plan: ExecutionPlan,
    normalized_records: dict[str, list[dict[str, Any]]],
    entity_indexes: dict[tuple[str, str], int],
    entity_key: str,
    candidate: dict[str, Any],
) -> None:
    entity_type = candidate["entity_type"]
    key = (entity_type, entity_key)
    existing_index = entity_indexes.get(key)
    if existing_index is None:
        entity_indexes[key] = len(normalized_records[entity_type])
        normalized_records[entity_type].append(candidate)
        return

    existing = normalized_records[entity_type][existing_index]
    if _prefer_candidate_record(plan, existing, candidate):
        normalized_records[entity_type][existing_index] = candidate


def _prefer_candidate_record(
    plan: ExecutionPlan,
    existing: Mapping[str, Any],
    candidate: Mapping[str, Any],
) -> bool:
    existing_value = _payload_checkpoint_value(existing, plan.checkpoint_field)
    candidate_value = _payload_checkpoint_value(candidate, plan.checkpoint_field)
    if candidate_value is None:
        return False
    if existing_value is None:
        return True
    return _compare_checkpoint_values(candidate_value, existing_value) > 0


def _payload_checkpoint_value(
    record: Mapping[str, Any],
    checkpoint_field: str | None,
) -> str | None:
    payload = record.get("catalog_payload")
    if not isinstance(payload, Mapping):
        return None
    return _string_value(_lookup_field(payload, checkpoint_field))


def _checkpoint_candidate(plan: ExecutionPlan, record: Mapping[str, Any]) -> str | None:
    if plan.checkpoint_field is None:
        return None
    return _string_value(_lookup_field(record, plan.checkpoint_field))


def _should_skip_for_checkpoint(
    plan: ExecutionPlan,
    checkpoint_state: CheckpointState | None,
    checkpoint_value: str | None,
) -> bool:
    if (
        checkpoint_state is None
        or checkpoint_value is None
        or plan.extraction_mode != "incremental"
    ):
        return False
    return _compare_checkpoint_values(checkpoint_value, checkpoint_state.checkpoint_value) <= 0


def _lookup_field(record: Mapping[str, Any], field_path: str | None) -> Any:
    if field_path is None:
        return None

    current: Any = record
    for segment in field_path.split("."):
        if not isinstance(current, Mapping):
            return None
        current = current.get(segment)
    return current


def _string_value(value: Any) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    if not normalized:
        return None
    return normalized


def _max_checkpoint_value(current_value: str | None, candidate_value: str) -> str:
    if current_value is None:
        return candidate_value
    if _compare_checkpoint_values(candidate_value, current_value) > 0:
        return candidate_value
    return current_value


def _freeze_string_mapping(values: Mapping[str, str] | None) -> tuple[tuple[str, str], ...]:
    if not values:
        return ()

    frozen_items: list[tuple[str, str]] = []
    for key, value in values.items():
        normalized_key = str(key).strip()
        normalized_value = str(value).strip()
        if not normalized_key:
            raise ValueError("mapping keys must be non-empty strings")
        if not normalized_value:
            raise ValueError("mapping values must be non-empty strings")
        frozen_items.append((normalized_key, normalized_value))
    return tuple(sorted(frozen_items))


def _stringify_mapping(values: Mapping[str, Any]) -> dict[str, str]:
    rendered: dict[str, str] = {}
    for key, value in values.items():
        normalized_key = str(key).strip()
        if not normalized_key:
            raise ValueError("mapping keys must be non-empty strings")
        if value is None:
            continue
        normalized_value = str(value).strip()
        if not normalized_value:
            continue
        rendered[normalized_key] = normalized_value
    return rendered


def _compare_checkpoint_values(left: str, right: str) -> int:
    normalized_left = _normalize_checkpoint_value(left)
    normalized_right = _normalize_checkpoint_value(right)
    if normalized_left[0] == normalized_right[0]:
        left_value = normalized_left[1]
        right_value = normalized_right[1]
        if left_value < right_value:
            return -1
        if left_value > right_value:
            return 1
        return 0

    if left < right:
        return -1
    if left > right:
        return 1
    return 0


def _normalize_checkpoint_value(value: str) -> tuple[str, Any]:
    parsed_datetime = _parse_datetime(value)
    if parsed_datetime is not None:
        return ("datetime", parsed_datetime.astimezone(UTC))

    try:
        return ("decimal", Decimal(value))
    except InvalidOperation:
        return ("text", value)


def _parse_datetime(value: str) -> datetime | None:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed


def _format_datetime(value: datetime) -> str:
    normalized = value.astimezone(UTC).isoformat()
    return normalized.replace("+00:00", "Z")
