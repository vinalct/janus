from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from janus.models import PaginationConfig

from .http import ApiRequest

_CURSOR_HINT_KEYS = ("next_cursor", "nextCursor", "cursor")
_CURSOR_CONTAINER_KEYS = ("meta", "metadata", "pagination")


@dataclass(frozen=True, slots=True)
class PaginationState:
    """Mutable-looking pagination cursor carried immutably between requests."""

    request_index: int
    page_number: int | None = None
    offset: int | None = None
    cursor: str | None = None


class ApiPaginator:
    """Base paginator contract for reusable API request iteration."""

    pagination_type: str = "none"

    def initial_state(self, request: ApiRequest) -> PaginationState:
        del request
        return PaginationState(request_index=1)

    def apply(self, request: ApiRequest, state: PaginationState) -> ApiRequest:
        del state
        return request

    def next_state(
        self,
        state: PaginationState,
        *,
        records_extracted: int,
        payload: Any,
        next_cursor: str | None = None,
    ) -> PaginationState | None:
        del records_extracted
        del payload
        del next_cursor
        return None


@dataclass(frozen=True, slots=True)
class NoPaginationPaginator(ApiPaginator):
    pagination_type: str = "none"


@dataclass(frozen=True, slots=True)
class PageNumberPaginator(ApiPaginator):
    page_param: str
    size_param: str
    page_size: int
    pagination_type: str = "page_number"

    def initial_state(self, request: ApiRequest) -> PaginationState:
        start_page = int(request.params_as_dict().get(self.page_param, "1"))
        return PaginationState(request_index=1, page_number=start_page)

    def apply(self, request: ApiRequest, state: PaginationState) -> ApiRequest:
        assert state.page_number is not None
        return request.with_params(
            {
                self.page_param: str(state.page_number),
                self.size_param: str(self.page_size),
            }
        )

    def next_state(
        self,
        state: PaginationState,
        *,
        records_extracted: int,
        payload: Any,
        next_cursor: str | None = None,
    ) -> PaginationState | None:
        del payload
        del next_cursor
        if records_extracted == 0 or records_extracted < self.page_size:
            return None
        assert state.page_number is not None
        return PaginationState(
            request_index=state.request_index + 1,
            page_number=state.page_number + 1,
        )


@dataclass(frozen=True, slots=True)
class OffsetPaginator(ApiPaginator):
    offset_param: str
    limit_param: str
    page_size: int
    pagination_type: str = "offset"

    def initial_state(self, request: ApiRequest) -> PaginationState:
        start_offset = int(request.params_as_dict().get(self.offset_param, "0"))
        return PaginationState(request_index=1, offset=start_offset)

    def apply(self, request: ApiRequest, state: PaginationState) -> ApiRequest:
        assert state.offset is not None
        return request.with_params(
            {
                self.offset_param: str(state.offset),
                self.limit_param: str(self.page_size),
            }
        )

    def next_state(
        self,
        state: PaginationState,
        *,
        records_extracted: int,
        payload: Any,
        next_cursor: str | None = None,
    ) -> PaginationState | None:
        del payload
        del next_cursor
        if records_extracted == 0 or records_extracted < self.page_size:
            return None
        assert state.offset is not None
        return PaginationState(
            request_index=state.request_index + 1,
            offset=state.offset + self.page_size,
        )


@dataclass(frozen=True, slots=True)
class CursorPaginator(ApiPaginator):
    cursor_param: str
    pagination_type: str = "cursor"

    def initial_state(self, request: ApiRequest) -> PaginationState:
        return PaginationState(
            request_index=1,
            cursor=request.params_as_dict().get(self.cursor_param),
        )

    def apply(self, request: ApiRequest, state: PaginationState) -> ApiRequest:
        if state.cursor is None:
            return request
        return request.with_params({self.cursor_param: state.cursor})

    def next_state(
        self,
        state: PaginationState,
        *,
        records_extracted: int,
        payload: Any,
        next_cursor: str | None = None,
    ) -> PaginationState | None:
        del records_extracted
        resolved_cursor = next_cursor
        if resolved_cursor is None:
            resolved_cursor = default_cursor_from_payload(payload)
        if not resolved_cursor:
            return None
        return PaginationState(
            request_index=state.request_index + 1,
            cursor=resolved_cursor,
        )


def build_paginator(config: PaginationConfig) -> ApiPaginator:
    if config.type == "none":
        return NoPaginationPaginator()
    if config.type == "page_number":
        return PageNumberPaginator(
            page_param=config.page_param or "page",
            size_param=config.size_param or "page_size",
            page_size=config.page_size or 100,
        )
    if config.type == "offset":
        return OffsetPaginator(
            offset_param=config.offset_param or "offset",
            limit_param=config.limit_param or "limit",
            page_size=config.page_size or 100,
        )
    if config.type == "cursor":
        return CursorPaginator(cursor_param=config.cursor_param or "cursor")
    raise ValueError(f"Unsupported pagination type: {config.type}")


def default_cursor_from_payload(payload: Any) -> str | None:
    if isinstance(payload, dict):
        for key in _CURSOR_HINT_KEYS:
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()

        for container_key in _CURSOR_CONTAINER_KEYS:
            nested = payload.get(container_key)
            if isinstance(nested, dict):
                nested_cursor = default_cursor_from_payload(nested)
                if nested_cursor:
                    return nested_cursor
    return None
