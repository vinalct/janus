from __future__ import annotations

import json
import os
import time
from concurrent.futures import Future, ThreadPoolExecutor
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from threading import Lock
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
from janus.strategies.base import BaseStrategy, SourceHook
from janus.utils.environment import load_environment_config, prepare_runtime
from janus.utils.logging import StructuredLogger, redact_url
from janus.utils.storage import StorageLayout
from janus.writers import RawArtifactWriter

from .http import (
    ApiClient,
    ApiRequest,
    ApiResponse,
    ApiTransport,
    ApiTransportError,
    AuthResolutionError,
    UrllibApiTransport,
    inject_auth,
)
from .pagination import OffsetPaginator, PageNumberPaginator, PaginationState, build_paginator

SUPPORTED_API_PAYLOAD_FORMATS = frozenset({"binary", "json", "jsonl", "text"})
RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})
RAW_FILE_SUFFIXES = {
    "binary": ".bin",
    "json": ".json",
    "jsonl": ".jsonl",
    "text": ".txt",
}
DEFAULT_RECORD_KEYS = ("records", "items", "results", "data", "value")


class ApiStrategyError(RuntimeError):
    """Base failure for API strategy execution."""


class ApiResponseError(ApiStrategyError):
    """Raised when an API call finished with a non-success response."""

    def __init__(self, response: ApiResponse) -> None:
        self.response = response
        message = (
            f"API request failed with status {response.status_code} for "
            f"{redact_url(response.request.full_url())}"
        )
        super().__init__(message)


class ApiPayloadError(ApiStrategyError):
    """Raised when the configured payload format cannot be decoded."""


class ApiHook(SourceHook):
    """API-specific hook points layered on top of the generic source-hook contract."""

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

    def extract_records(
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

    def resolve_next_cursor(
        self,
        plan: ExecutionPlan,
        request: ApiRequest,
        response: ApiResponse,
        payload: Any,
    ) -> str | None:
        del plan
        del request
        del response
        del payload
        return None

    def checkpoint_params(
        self,
        plan: ExecutionPlan,
        checkpoint_value: str,
    ) -> Mapping[str, str] | None:
        del plan
        del checkpoint_value
        return None


@dataclass(slots=True)
class ApiRequestThrottle:
    """Thread-safe request pacing aligned with JANUS safe defaults."""

    requests_per_minute: int | None
    clock: Callable[[], float]
    sleeper: Callable[[float], None]
    _next_allowed_at: float | None = None
    _lock: Lock = field(default_factory=Lock, init=False, repr=False)

    def wait_for_turn(self) -> None:
        if self.requests_per_minute is None:
            return

        interval_seconds = 60 / self.requests_per_minute
        with self._lock:
            now = self.clock()
            scheduled_at = now
            if self._next_allowed_at is not None and now < self._next_allowed_at:
                scheduled_at = self._next_allowed_at
            self._next_allowed_at = scheduled_at + interval_seconds

        delay = scheduled_at - now
        if delay > 0:
            self.sleeper(delay)


@dataclass(frozen=True, slots=True)
class SubmittedApiRequest:
    pagination_state: PaginationState
    request: ApiRequest
    future: Future[tuple[ApiResponse, int]]


@dataclass(frozen=True, slots=True)
class ProcessedApiRequest:
    artifact: ExtractedArtifact
    records_extracted: int
    checkpoint_value: str | None
    next_pagination_state: PaginationState | None


@dataclass(slots=True)
class ApiStrategy(BaseStrategy):
    """Reusable HTTP strategy for public federal API integrations."""

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
        return "api"

    def plan(
        self,
        source_config: SourceConfig,
        run_context,
        hook: SourceHook | None = None,
    ) -> ExecutionPlan:
        self._validate_source_config(source_config)
        plan = ExecutionPlan.from_source_config(source_config, run_context)
        plan = plan.with_note("strategy_family:api")
        plan = plan.with_note(f"strategy_variant:{source_config.strategy_variant}")
        if hook is not None:
            return hook.on_plan(plan)
        return plan

    def extract(
        self,
        plan: ExecutionPlan,
        hook: SourceHook | None = None,
    ) -> ExtractionResult:
        api_hook = hook if isinstance(hook, ApiHook) else None
        storage_layout = self.storage_layout_factory(plan)
        raw_writer = self.raw_writer_factory(storage_layout)
        checkpoint_state = self.checkpoint_store.load(plan)
        base_request = self._build_base_request(plan, checkpoint_state, api_hook)
        paginator = build_paginator(plan.source_config.access.pagination)
        pagination_state = paginator.initial_state(base_request)
        throttle = ApiRequestThrottle(
            requests_per_minute=plan.source_config.access.rate_limit.requests_per_minute,
            clock=self.clock,
            sleeper=self.sleeper,
        )
        logger = self._bind_logger(plan)
        if logger is not None:
            logger.info(
                "api_extraction_started",
                request_url=base_request.full_url(),
                method=base_request.method,
                pagination_type=plan.source_config.access.pagination.type,
                page_size=plan.source_config.access.pagination.page_size,
                checkpoint_loaded=checkpoint_state is not None,
                timeout_seconds=base_request.timeout_seconds,
            )

        if _supports_concurrent_pagination(
            paginator,
            plan.source_config.access.rate_limit.concurrency,
        ):
            (
                artifacts,
                total_records,
                successful_requests,
                total_attempts,
                checkpoint_value,
            ) = self._extract_concurrent_pages(
                plan,
                api_hook=api_hook,
                base_request=base_request,
                paginator=paginator,
                pagination_state=pagination_state,
                checkpoint_state=checkpoint_state,
                raw_writer=raw_writer,
                throttle=throttle,
                logger=logger,
            )
        else:
            (
                artifacts,
                total_records,
                successful_requests,
                total_attempts,
                checkpoint_value,
            ) = self._extract_sequential_pages(
                plan,
                api_hook=api_hook,
                base_request=base_request,
                paginator=paginator,
                pagination_state=pagination_state,
                checkpoint_state=checkpoint_state,
                raw_writer=raw_writer,
                throttle=throttle,
                logger=logger,
            )

        if logger is not None:
            logger.info(
                "api_extraction_finished",
                request_count=successful_requests,
                retry_count=max(total_attempts - successful_requests, 0),
                attempt_count=total_attempts,
                records_extracted=total_records,
                artifact_count=len(artifacts),
                checkpoint_value=checkpoint_value,
            )

        extraction_result = ExtractionResult.from_plan(
            plan,
            tuple(artifacts),
            records_extracted=total_records,
            checkpoint_value=checkpoint_value,
            metadata={
                "request_count": str(successful_requests),
                "retry_count": str(max(total_attempts - successful_requests, 0)),
                "attempt_count": str(total_attempts),
                "pagination_type": plan.source_config.access.pagination.type,
                "auth_type": plan.source_config.access.auth.type,
                "checkpoint_loaded": str(checkpoint_state is not None).lower(),
                "records_extracted": str(total_records),
            },
        )
        if hook is not None:
            return hook.on_extraction_result(plan, extraction_result)
        return extraction_result

    def _extract_sequential_pages(
        self,
        plan: ExecutionPlan,
        *,
        api_hook: ApiHook | None,
        base_request: ApiRequest,
        paginator,
        pagination_state: PaginationState | None,
        checkpoint_state: CheckpointState | None,
        raw_writer: RawArtifactWriter,
        throttle: ApiRequestThrottle,
        logger: StructuredLogger | None,
    ) -> tuple[list[ExtractedArtifact], int, int, int, str | None]:
        artifacts: list[ExtractedArtifact] = []
        total_records = 0
        successful_requests = 0
        total_attempts = 0
        checkpoint_value: str | None = None

        with ApiClient(self.transport_factory()) as client:
            while pagination_state is not None:
                request = self._prepare_request(
                    plan,
                    base_request,
                    pagination_state,
                    paginator,
                    api_hook=api_hook,
                    checkpoint_state=checkpoint_state,
                    logger=logger,
                )
                response, attempts_used = self._send_with_retries(
                    plan,
                    client,
                    request,
                    throttle,
                    logger,
                )
                successful_requests += 1
                total_attempts += attempts_used
                processed = self._process_response(
                    plan,
                    raw_writer,
                    paginator,
                    pagination_state,
                    request,
                    response,
                    attempts_used,
                    checkpoint_value,
                    api_hook=api_hook,
                    logger=logger,
                    total_records=total_records,
                )
                artifacts.append(processed.artifact)
                total_records += processed.records_extracted
                checkpoint_value = processed.checkpoint_value
                pagination_state = processed.next_pagination_state

        return (
            artifacts,
            total_records,
            successful_requests,
            total_attempts,
            checkpoint_value,
        )

    def _extract_concurrent_pages(
        self,
        plan: ExecutionPlan,
        *,
        api_hook: ApiHook | None,
        base_request: ApiRequest,
        paginator: PageNumberPaginator | OffsetPaginator,
        pagination_state: PaginationState | None,
        checkpoint_state: CheckpointState | None,
        raw_writer: RawArtifactWriter,
        throttle: ApiRequestThrottle,
        logger: StructuredLogger | None,
    ) -> tuple[list[ExtractedArtifact], int, int, int, str | None]:
        concurrency = plan.source_config.access.rate_limit.concurrency
        artifacts: list[ExtractedArtifact] = []
        total_records = 0
        successful_requests = 0
        total_attempts = 0
        checkpoint_value: str | None = None
        next_request_index = pagination_state.request_index if pagination_state is not None else 1
        predicted_state = pagination_state
        pending: dict[int, SubmittedApiRequest] = {}
        stop_submitting = False

        executor = ThreadPoolExecutor(
            max_workers=concurrency,
            thread_name_prefix="janus-api",
        )
        try:
            while predicted_state is not None or pending:
                while (
                    predicted_state is not None
                    and not stop_submitting
                    and len(pending) < concurrency
                ):
                    request = self._prepare_request(
                        plan,
                        base_request,
                        predicted_state,
                        paginator,
                        api_hook=api_hook,
                        checkpoint_state=checkpoint_state,
                        logger=logger,
                    )
                    future = executor.submit(
                        self._fetch_with_dedicated_client,
                        plan,
                        request,
                        throttle,
                        logger,
                    )
                    pending[predicted_state.request_index] = SubmittedApiRequest(
                        pagination_state=predicted_state,
                        request=request,
                        future=future,
                    )
                    predicted_state = _predicted_next_pagination_state(
                        paginator,
                        predicted_state,
                    )

                if next_request_index not in pending:
                    break

                submitted = pending.pop(next_request_index)
                response, attempts_used = submitted.future.result()
                successful_requests += 1
                total_attempts += attempts_used
                processed = self._process_response(
                    plan,
                    raw_writer,
                    paginator,
                    submitted.pagination_state,
                    submitted.request,
                    response,
                    attempts_used,
                    checkpoint_value,
                    api_hook=api_hook,
                    logger=logger,
                    total_records=total_records,
                )
                artifacts.append(processed.artifact)
                total_records += processed.records_extracted
                checkpoint_value = processed.checkpoint_value
                next_request_index += 1
                if processed.next_pagination_state is None:
                    stop_submitting = True
                    for pending_request in pending.values():
                        pending_request.future.cancel()
                    break
        finally:
            executor.shutdown(wait=True, cancel_futures=True)

        return (
            artifacts,
            total_records,
            successful_requests,
            total_attempts,
            checkpoint_value,
        )

    def build_normalization_handoff(
        self,
        plan: ExecutionPlan,
        extraction_result: ExtractionResult,
        hook: SourceHook | None = None,
    ) -> ExtractionResult:
        if hook is not None:
            return hook.on_normalization_handoff(plan, extraction_result)
        return extraction_result

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
            "auth_type": plan.source_config.access.auth.type,
            "request_timeout_seconds": plan.source_config.access.timeout_seconds,
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
        access_format = source_config.access.format
        raw_format = source_config.outputs.raw.format
        if access_format not in SUPPORTED_API_PAYLOAD_FORMATS:
            allowed_formats = ", ".join(sorted(SUPPORTED_API_PAYLOAD_FORMATS))
            raise ValueError(f"API access.format must be one of: {allowed_formats}")
        if raw_format not in SUPPORTED_API_PAYLOAD_FORMATS:
            allowed_formats = ", ".join(sorted(SUPPORTED_API_PAYLOAD_FORMATS))
            raise ValueError(f"API outputs.raw.format must be one of: {allowed_formats}")

        variant = source_config.strategy_variant
        pagination_type = source_config.access.pagination.type
        if variant == "page_number_api" and pagination_type != "page_number":
            raise ValueError("page_number_api requires access.pagination.type='page_number'")
        if variant == "offset_api" and pagination_type != "offset":
            raise ValueError("offset_api requires access.pagination.type='offset'")
        if variant == "cursor_api" and pagination_type != "cursor":
            raise ValueError("cursor_api requires access.pagination.type='cursor'")
        if variant == "date_window_api" and source_config.extraction.mode != "incremental":
            raise ValueError("date_window_api requires extraction.mode='incremental'")

    def _build_base_request(
        self,
        plan: ExecutionPlan,
        checkpoint_state: CheckpointState | None,
        api_hook: ApiHook | None,
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
        if api_hook is not None and checkpoint_value is not None:
            hook_checkpoint_params = api_hook.checkpoint_params(plan, checkpoint_value)
            if hook_checkpoint_params is not None:
                checkpoint_params = _stringify_mapping(hook_checkpoint_params)

        if checkpoint_params:
            request = request.with_params(checkpoint_params)
        return request

    def _prepare_request(
        self,
        plan: ExecutionPlan,
        base_request: ApiRequest,
        pagination_state: PaginationState,
        paginator,
        *,
        api_hook: ApiHook | None,
        checkpoint_state: CheckpointState | None,
        logger: StructuredLogger | None,
    ) -> ApiRequest:
        request = paginator.apply(base_request, pagination_state)
        if api_hook is not None:
            request = api_hook.prepare_request(
                plan,
                request,
                checkpoint_state=checkpoint_state,
                pagination_state=pagination_state,
            )

        if logger is not None:
            logger.info(
                "api_request_started",
                request_index=pagination_state.request_index,
                page_number=pagination_state.page_number,
                offset=pagination_state.offset,
                cursor=pagination_state.cursor,
                request_url=request.full_url(),
            )
        return request

    def _fetch_with_dedicated_client(
        self,
        plan: ExecutionPlan,
        request: ApiRequest,
        throttle: ApiRequestThrottle,
        logger: StructuredLogger | None,
    ) -> tuple[ApiResponse, int]:
        with ApiClient(self.transport_factory()) as client:
            return self._send_with_retries(
                plan,
                client,
                request,
                throttle,
                logger,
            )

    def _send_with_retries(
        self,
        plan: ExecutionPlan,
        client: ApiClient,
        request: ApiRequest,
        throttle: ApiRequestThrottle,
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
                    raise ApiStrategyError(str(exc)) from exc
                self._sleep_for_retry(plan, attempt, response=None, logger=logger)
                continue

            if 200 <= response.status_code < 300:
                return response, attempt

            if (
                response.status_code not in RETRYABLE_STATUS_CODES
                or attempt == retry_config.max_attempts
            ):
                raise ApiResponseError(response)

            self._sleep_for_retry(plan, attempt, response=response, logger=logger)

        if last_transport_error is not None:
            raise ApiStrategyError(str(last_transport_error)) from last_transport_error
        raise AssertionError("Retry loop exited without a response or error")

    def _process_response(
        self,
        plan: ExecutionPlan,
        raw_writer: RawArtifactWriter,
        paginator,
        pagination_state: PaginationState,
        request: ApiRequest,
        response: ApiResponse,
        attempts_used: int,
        checkpoint_value: str | None,
        *,
        api_hook: ApiHook | None,
        logger: StructuredLogger | None,
        total_records: int,
    ) -> ProcessedApiRequest:
        if api_hook is not None:
            response = api_hook.handle_response(plan, request, response)

        payload = self._decode_payload(plan, response)
        if api_hook is not None:
            payload = api_hook.transform_payload(plan, request, response, payload)

        records = self._extract_records(plan, request, response, payload, api_hook)
        resolved_checkpoint_value = self._resolve_checkpoint_value(
            checkpoint_value,
            records,
            checkpoint_field=plan.checkpoint_field,
        )

        persisted = self._persist_raw_payload(
            plan,
            raw_writer,
            response=response,
            payload=payload,
            pagination_state=pagination_state,
        )

        next_cursor = None
        if api_hook is not None:
            next_cursor = api_hook.resolve_next_cursor(plan, request, response, payload)
        next_pagination_state = paginator.next_state(
            pagination_state,
            records_extracted=len(records),
            payload=payload,
            next_cursor=next_cursor,
        )
        if logger is not None:
            logger.info(
                "api_request_finished",
                request_index=pagination_state.request_index,
                page_number=pagination_state.page_number,
                offset=pagination_state.offset,
                cursor=pagination_state.cursor,
                status_code=response.status_code,
                attempts_used=attempts_used,
                records_extracted=len(records),
                total_records=total_records + len(records),
                artifact_path=persisted.artifact.path,
                has_next_page=next_pagination_state is not None,
                next_page_number=(
                    next_pagination_state.page_number
                    if next_pagination_state is not None
                    else None
                ),
                next_offset=(
                    next_pagination_state.offset
                    if next_pagination_state is not None
                    else None
                ),
            )

        return ProcessedApiRequest(
            artifact=persisted.artifact,
            records_extracted=len(records),
            checkpoint_value=resolved_checkpoint_value,
            next_pagination_state=next_pagination_state,
        )

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
                "api_retry_scheduled",
                attempt=attempt,
                delay_seconds=delay,
                status_code=response.status_code if response is not None else None,
            )
        self.sleeper(delay)

    def _decode_payload(self, plan: ExecutionPlan, response: ApiResponse) -> Any:
        format_name = plan.source_config.access.format
        try:
            if format_name == "binary":
                return response.body
            if format_name == "text":
                return response.text()
            if format_name == "json":
                return response.json()
            if format_name == "jsonl":
                text = response.text()
                if not text.strip():
                    return []
                return [json.loads(line) for line in text.splitlines() if line.strip()]
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApiPayloadError(
                "Failed to decode "
                f"{format_name} payload from {redact_url(response.request.full_url())}"
            ) from exc

        raise ApiPayloadError(f"Unsupported API payload format: {format_name}")

    def _extract_records(
        self,
        plan: ExecutionPlan,
        request: ApiRequest,
        response: ApiResponse,
        payload: Any,
        api_hook: ApiHook | None,
    ) -> tuple[Any, ...]:
        if api_hook is not None:
            hook_records = api_hook.extract_records(plan, request, response, payload)
            if hook_records is not None:
                return tuple(hook_records)
        return tuple(_default_records_from_payload(payload))

    def _persist_raw_payload(
        self,
        plan: ExecutionPlan,
        raw_writer: RawArtifactWriter,
        *,
        response: ApiResponse,
        payload: Any,
        pagination_state: PaginationState,
    ):
        relative_path = _raw_relative_path(
            plan.source_config.outputs.raw.format,
            pagination_state,
        )
        metadata = {
            "request_url": response.request.full_url(),
            "status_code": str(response.status_code),
            "request_index": str(pagination_state.request_index),
        }
        if pagination_state.page_number is not None:
            metadata["page_number"] = str(pagination_state.page_number)
        if pagination_state.offset is not None:
            metadata["offset"] = str(pagination_state.offset)
        if pagination_state.cursor is not None:
            metadata["cursor"] = pagination_state.cursor

        raw_format = plan.source_config.outputs.raw.format
        if raw_format == "json":
            return raw_writer.write_json(plan, relative_path, payload, metadata=metadata)
        if raw_format == "jsonl":
            return raw_writer.write_json_lines(
                plan,
                relative_path,
                _default_records_from_payload(payload),
                metadata=metadata,
            )
        if raw_format == "text":
            return raw_writer.write_text(plan, relative_path, response.text(), metadata=metadata)
        if raw_format == "binary":
            return raw_writer.write_bytes(plan, relative_path, response.body, metadata=metadata)
        raise ValueError(f"Unsupported raw output format for API strategy: {raw_format}")

    def _resolve_checkpoint_value(
        self,
        current_value: str | None,
        records: Sequence[Any],
        *,
        checkpoint_field: str | None,
    ) -> str | None:
        if checkpoint_field is None:
            return current_value

        resolved_value = current_value
        for record in records:
            if not isinstance(record, Mapping):
                continue
            candidate = _string_value(_lookup_field(record, checkpoint_field))
            if candidate is None:
                continue
            if resolved_value is None or _compare_checkpoint_values(candidate, resolved_value) > 0:
                resolved_value = candidate
        return resolved_value

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
    prepare_runtime(environment_config, plan.run_context.project_root)
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
        raise ValueError("API source requires access.base_url or access.url")
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


def _supports_concurrent_pagination(
    paginator: Any,
    concurrency: int,
) -> bool:
    return concurrency > 1 and isinstance(paginator, PageNumberPaginator | OffsetPaginator)


def _predicted_next_pagination_state(
    paginator: PageNumberPaginator | OffsetPaginator,
    pagination_state: PaginationState,
) -> PaginationState | None:
    return paginator.next_state(
        pagination_state,
        records_extracted=paginator.page_size,
        payload=None,
    )


def _raw_relative_path(raw_format: str, pagination_state: PaginationState) -> Path:
    suffix = RAW_FILE_SUFFIXES[raw_format]
    if pagination_state.page_number is not None:
        filename = f"page-{pagination_state.page_number:04d}{suffix}"
    elif pagination_state.offset is not None:
        filename = f"offset-{pagination_state.offset:08d}{suffix}"
    elif pagination_state.cursor is not None:
        filename = f"cursor-{pagination_state.request_index:04d}{suffix}"
    else:
        filename = f"response-{pagination_state.request_index:04d}{suffix}"
    return Path("pages") / filename


def _default_records_from_payload(payload: Any) -> Sequence[Any]:
    if payload is None:
        return ()
    if isinstance(payload, list):
        return payload
    if isinstance(payload, tuple):
        return payload
    if isinstance(payload, Mapping):
        for key in DEFAULT_RECORD_KEYS:
            nested = payload.get(key)
            if isinstance(nested, list):
                return nested
        return (payload,)
    return ()


def _lookup_field(record: Mapping[str, Any], field_path: str) -> Any:
    current: Any = record
    for segment in field_path.split("."):
        if not isinstance(current, Mapping):
            return None
        current = current.get(segment)
    return current


def _string_value(value: Any) -> str | None:
    if value is None:
        return None
    rendered = str(value).strip()
    if not rendered:
        return None
    return rendered


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
