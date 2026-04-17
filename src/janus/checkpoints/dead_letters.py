from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from janus.lineage.persistence import MetadataZonePaths, read_json_mapping, write_json_atomic
from janus.models import ExecutionPlan


@dataclass(frozen=True, slots=True)
class DeadLetterEntry:
    """One execution item skipped after exhausting request-level retries."""

    item_key: str
    item_type: str
    error_type: str
    error_message: str
    recorded_at: datetime
    metadata: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not self.item_key.strip():
            raise ValueError("item_key must not be empty")
        if not self.item_type.strip():
            raise ValueError("item_type must not be empty")
        if not self.error_type.strip():
            raise ValueError("error_type must not be empty")
        if not self.error_message.strip():
            raise ValueError("error_message must not be empty")
        if self.recorded_at.tzinfo is None or self.recorded_at.utcoffset() is None:
            raise ValueError("recorded_at must be timezone-aware")

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> DeadLetterEntry:
        metadata = payload.get("metadata") or {}
        if not isinstance(metadata, Mapping):
            raise ValueError("dead letter metadata must be a mapping")
        return cls(
            item_key=_require_string(payload, "item_key"),
            item_type=_require_string(payload, "item_type"),
            error_type=_require_string(payload, "error_type"),
            error_message=_require_string(payload, "error_message"),
            recorded_at=_parse_datetime(_require_string(payload, "recorded_at"), "recorded_at"),
            metadata=_freeze_string_mapping(
                {str(key): str(value) for key, value in metadata.items()}
            ),
        )

    def metadata_as_dict(self) -> dict[str, str]:
        return dict(self.metadata)

    def to_dict(self) -> dict[str, Any]:
        return {
            "item_key": self.item_key,
            "item_type": self.item_type,
            "error_type": self.error_type,
            "error_message": self.error_message,
            "recorded_at": self.recorded_at.isoformat(),
            "metadata": self.metadata_as_dict(),
        }


@dataclass(frozen=True, slots=True)
class DeadLetterState:
    """Source-scoped dead-letter state persisted in the metadata zone."""

    run_id: str
    source_id: str
    strategy_family: str
    strategy_variant: str
    updated_at: datetime
    entries: tuple[DeadLetterEntry, ...] = ()

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        if not self.source_id.strip():
            raise ValueError("source_id must not be empty")
        if not self.strategy_family.strip():
            raise ValueError("strategy_family must not be empty")
        if not self.strategy_variant.strip():
            raise ValueError("strategy_variant must not be empty")
        if self.updated_at.tzinfo is None or self.updated_at.utcoffset() is None:
            raise ValueError("updated_at must be timezone-aware")

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> DeadLetterState:
        raw_entries = payload.get("entries") or []
        if not isinstance(raw_entries, list):
            raise ValueError("dead letter entries must be a list")
        return cls(
            run_id=_require_string(payload, "run_id"),
            source_id=_require_string(payload, "source_id"),
            strategy_family=_require_string(payload, "strategy_family"),
            strategy_variant=_require_string(payload, "strategy_variant"),
            updated_at=_parse_datetime(_require_string(payload, "updated_at"), "updated_at"),
            entries=tuple(DeadLetterEntry.from_dict(entry) for entry in raw_entries),
        )

    @property
    def entry_count(self) -> int:
        return len(self.entries)

    @property
    def item_keys(self) -> frozenset[str]:
        return frozenset(entry.item_key for entry in self.entries)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "source_id": self.source_id,
            "strategy_family": self.strategy_family,
            "strategy_variant": self.strategy_variant,
            "updated_at": self.updated_at.isoformat(),
            "entries": [entry.to_dict() for entry in self.entries],
        }


@dataclass(slots=True)
class DeadLetterStore:
    """Persist dead-lettered execution items so resume can skip them safely."""

    def load(self, plan: ExecutionPlan) -> DeadLetterState | None:
        payload = read_json_mapping(self.path(plan))
        if payload is None:
            return None

        state = DeadLetterState.from_dict(payload)
        if state.source_id != plan.source.source_id:
            return None
        return state

    def record(
        self,
        plan: ExecutionPlan,
        *,
        item_key: str,
        item_type: str,
        error: Exception,
        metadata: Mapping[str, str] | None = None,
        recorded_at: datetime | None = None,
    ) -> DeadLetterState:
        normalized_item_key = item_key.strip()
        if not normalized_item_key:
            raise ValueError("item_key must not be empty")

        existing_state = self.load(plan)
        if existing_state is not None and normalized_item_key in existing_state.item_keys:
            return existing_state

        resolved_recorded_at = recorded_at or datetime.now(tz=UTC)
        if resolved_recorded_at.tzinfo is None or resolved_recorded_at.utcoffset() is None:
            raise ValueError("recorded_at must be timezone-aware")

        entry = DeadLetterEntry(
            item_key=normalized_item_key,
            item_type=item_type,
            error_type=type(error).__name__,
            error_message=(str(error).strip() or type(error).__name__),
            recorded_at=resolved_recorded_at,
            metadata=_freeze_string_mapping(metadata),
        )
        entries = (existing_state.entries if existing_state is not None else ()) + (entry,)
        state = DeadLetterState(
            run_id=plan.run_context.run_id,
            source_id=plan.source.source_id,
            strategy_family=plan.source.strategy,
            strategy_variant=plan.source.strategy_variant,
            updated_at=resolved_recorded_at,
            entries=entries,
        )
        write_json_atomic(self.path(plan), state.to_dict())
        return state

    def clear(self, plan: ExecutionPlan) -> None:
        path = self.path(plan)
        if path.exists():
            path.unlink()

    def path(self, plan: ExecutionPlan) -> Path:
        return MetadataZonePaths.from_plan(plan).dead_letter_state_path


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


def _parse_datetime(value: str, field_name: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"Invalid datetime for {field_name}: {value!r}") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return parsed


def _require_string(payload: Mapping[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if value is None:
        raise ValueError(f"Missing field: {field_name}")
    normalized = str(value).strip()
    if not normalized:
        raise ValueError(f"Field {field_name} must not be empty")
    return normalized


def can_continue_after_dead_letter(
    *,
    total_item_count: int,
    dead_letter_count: int,
    dead_letter_max_items: int,
) -> bool:
    return total_item_count > 1 and dead_letter_count <= dead_letter_max_items
