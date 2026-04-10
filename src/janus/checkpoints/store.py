from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any

from janus.lineage.persistence import MetadataZonePaths, read_json_mapping, write_json_atomic
from janus.models import ExecutionPlan

SUPPORTED_CHECKPOINT_DECISIONS = frozenset({"advanced", "retained", "reused", "skipped"})


@dataclass(frozen=True, slots=True)
class CheckpointState:
    """Current checkpoint state for one source in the metadata zone."""

    run_id: str
    source_id: str
    strategy_family: str
    strategy_variant: str
    checkpoint_field: str
    checkpoint_strategy: str
    checkpoint_value: str
    updated_at: datetime
    metadata: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        if not self.source_id.strip():
            raise ValueError("source_id must not be empty")
        if not self.strategy_family.strip():
            raise ValueError("strategy_family must not be empty")
        if not self.strategy_variant.strip():
            raise ValueError("strategy_variant must not be empty")
        if not self.checkpoint_field.strip():
            raise ValueError("checkpoint_field must not be empty")
        if not self.checkpoint_strategy.strip():
            raise ValueError("checkpoint_strategy must not be empty")
        if not self.checkpoint_value.strip():
            raise ValueError("checkpoint_value must not be empty")
        if self.updated_at.tzinfo is None or self.updated_at.utcoffset() is None:
            raise ValueError("updated_at must be timezone-aware")

    @classmethod
    def from_dict(cls, payload: Mapping[str, Any]) -> CheckpointState:
        metadata = payload.get("metadata") or {}
        if not isinstance(metadata, Mapping):
            raise ValueError("checkpoint metadata must be a mapping")
        metadata_mapping = {str(key): str(value) for key, value in metadata.items()}
        return cls(
            run_id=_require_string(payload, "run_id"),
            source_id=_require_string(payload, "source_id"),
            strategy_family=_require_string(payload, "strategy_family"),
            strategy_variant=_require_string(payload, "strategy_variant"),
            checkpoint_field=_require_string(payload, "checkpoint_field"),
            checkpoint_strategy=_require_string(payload, "checkpoint_strategy"),
            checkpoint_value=_require_string(payload, "checkpoint_value"),
            updated_at=_parse_datetime(_require_string(payload, "updated_at"), "updated_at"),
            metadata=_freeze_string_mapping(metadata_mapping),
        )

    def metadata_as_dict(self) -> dict[str, str]:
        return dict(self.metadata)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "source_id": self.source_id,
            "strategy_family": self.strategy_family,
            "strategy_variant": self.strategy_variant,
            "checkpoint_field": self.checkpoint_field,
            "checkpoint_strategy": self.checkpoint_strategy,
            "checkpoint_value": self.checkpoint_value,
            "updated_at": self.updated_at.isoformat(),
            "metadata": self.metadata_as_dict(),
        }


@dataclass(frozen=True, slots=True)
class CheckpointHistoryEntry:
    """One checkpoint write decision captured for later diagnosis."""

    run_id: str
    source_id: str
    strategy_family: str
    strategy_variant: str
    checkpoint_field: str
    checkpoint_strategy: str
    candidate_value: str
    stored_value: str
    previous_value: str | None
    recorded_at: datetime
    decision: str
    advanced: bool
    metadata: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if self.decision not in SUPPORTED_CHECKPOINT_DECISIONS:
            allowed = ", ".join(sorted(SUPPORTED_CHECKPOINT_DECISIONS))
            raise ValueError(f"decision must be one of: {allowed}")
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        if not self.source_id.strip():
            raise ValueError("source_id must not be empty")
        if not self.strategy_family.strip():
            raise ValueError("strategy_family must not be empty")
        if not self.strategy_variant.strip():
            raise ValueError("strategy_variant must not be empty")
        if not self.checkpoint_field.strip():
            raise ValueError("checkpoint_field must not be empty")
        if not self.checkpoint_strategy.strip():
            raise ValueError("checkpoint_strategy must not be empty")
        if not self.candidate_value.strip():
            raise ValueError("candidate_value must not be empty")
        if not self.stored_value.strip():
            raise ValueError("stored_value must not be empty")
        if self.recorded_at.tzinfo is None or self.recorded_at.utcoffset() is None:
            raise ValueError("recorded_at must be timezone-aware")

    def metadata_as_dict(self) -> dict[str, str]:
        return dict(self.metadata)

    def to_dict(self) -> dict[str, Any]:
        payload = {
            "run_id": self.run_id,
            "source_id": self.source_id,
            "strategy_family": self.strategy_family,
            "strategy_variant": self.strategy_variant,
            "checkpoint_field": self.checkpoint_field,
            "checkpoint_strategy": self.checkpoint_strategy,
            "candidate_value": self.candidate_value,
            "stored_value": self.stored_value,
            "recorded_at": self.recorded_at.isoformat(),
            "decision": self.decision,
            "advanced": self.advanced,
            "metadata": self.metadata_as_dict(),
        }
        if self.previous_value is not None:
            payload["previous_value"] = self.previous_value
        return payload


@dataclass(frozen=True, slots=True)
class CheckpointWriteResult:
    """Result of one checkpoint persistence attempt."""

    state: CheckpointState | None
    decision: str
    advanced: bool = False
    current_path: Path | None = None
    history_path: Path | None = None

    def __post_init__(self) -> None:
        if self.decision not in SUPPORTED_CHECKPOINT_DECISIONS:
            allowed = ", ".join(sorted(SUPPORTED_CHECKPOINT_DECISIONS))
            raise ValueError(f"decision must be one of: {allowed}")


@dataclass(slots=True)
class CheckpointStore:
    """Read and persist monotonic checkpoints in the metadata zone."""

    def load(self, plan: ExecutionPlan) -> CheckpointState | None:
        if plan.checkpoint_strategy == "none" or plan.checkpoint_field is None:
            return None

        checkpoint_path = MetadataZonePaths.from_plan(plan).checkpoint_state_path
        payload = read_json_mapping(checkpoint_path)
        if payload is None:
            return None

        state = CheckpointState.from_dict(payload)
        self._validate_loaded_state(plan, state)
        return state

    def save(
        self,
        plan: ExecutionPlan,
        checkpoint_value: str | None,
        *,
        run_id: str | None = None,
        updated_at: datetime | None = None,
        metadata: Mapping[str, str] | None = None,
    ) -> CheckpointWriteResult:
        if plan.checkpoint_strategy == "none" or plan.checkpoint_field is None:
            return CheckpointWriteResult(state=None, decision="skipped")

        normalized_value = (checkpoint_value or "").strip()
        if not normalized_value:
            return CheckpointWriteResult(state=self.load(plan), decision="skipped")

        resolved_run_id = (run_id or plan.run_context.run_id).strip()
        if not resolved_run_id:
            raise ValueError("run_id must not be empty")

        resolved_updated_at = updated_at or datetime.now(tz=UTC)
        if resolved_updated_at.tzinfo is None or resolved_updated_at.utcoffset() is None:
            raise ValueError("updated_at must be timezone-aware")

        metadata_paths = MetadataZonePaths.from_plan(plan)
        existing_state = self.load(plan)
        candidate_state = CheckpointState(
            run_id=resolved_run_id,
            source_id=plan.source.source_id,
            strategy_family=plan.source.strategy,
            strategy_variant=plan.source.strategy_variant,
            checkpoint_field=plan.checkpoint_field,
            checkpoint_strategy=plan.checkpoint_strategy,
            checkpoint_value=normalized_value,
            updated_at=resolved_updated_at,
            metadata=_freeze_string_mapping(metadata),
        )

        decision = "advanced"
        advanced = existing_state is None
        stored_state = candidate_state
        current_path: Path | None = None

        if existing_state is not None:
            comparison = _compare_checkpoint_values(
                candidate_state.checkpoint_value,
                existing_state.checkpoint_value,
            )
            if comparison < 0:
                decision = "retained"
                advanced = False
                stored_state = existing_state
            elif comparison == 0:
                decision = "reused"
                advanced = False

        if decision != "retained":
            current_path = write_json_atomic(
                metadata_paths.checkpoint_state_path,
                candidate_state.to_dict(),
            )
            stored_state = candidate_state
        elif metadata_paths.checkpoint_state_path.exists():
            current_path = metadata_paths.checkpoint_state_path

        history_entry = CheckpointHistoryEntry(
            run_id=resolved_run_id,
            source_id=plan.source.source_id,
            strategy_family=plan.source.strategy,
            strategy_variant=plan.source.strategy_variant,
            checkpoint_field=plan.checkpoint_field,
            checkpoint_strategy=plan.checkpoint_strategy,
            candidate_value=candidate_state.checkpoint_value,
            stored_value=stored_state.checkpoint_value,
            previous_value=(
                existing_state.checkpoint_value if existing_state is not None else None
            ),
            recorded_at=resolved_updated_at,
            decision=decision,
            advanced=advanced,
            metadata=_freeze_string_mapping(metadata),
        )
        history_path = write_json_atomic(
            metadata_paths.checkpoint_history_path(resolved_run_id),
            history_entry.to_dict(),
        )
        return CheckpointWriteResult(
            state=stored_state,
            decision=decision,
            advanced=advanced,
            current_path=current_path,
            history_path=history_path,
        )

    def _validate_loaded_state(self, plan: ExecutionPlan, state: CheckpointState) -> None:
        expected = {
            "source_id": plan.source.source_id,
            "strategy_family": plan.source.strategy,
            "strategy_variant": plan.source.strategy_variant,
            "checkpoint_field": plan.checkpoint_field,
            "checkpoint_strategy": plan.checkpoint_strategy,
        }
        actual = {
            "source_id": state.source_id,
            "strategy_family": state.strategy_family,
            "strategy_variant": state.strategy_variant,
            "checkpoint_field": state.checkpoint_field,
            "checkpoint_strategy": state.checkpoint_strategy,
        }
        if actual != expected:
            raise ValueError("Stored checkpoint state does not match the current execution plan")


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
    normalized = value.strip()
    try:
        return ("datetime", _parse_datetime(normalized, "checkpoint_value").astimezone(UTC))
    except ValueError:
        pass

    try:
        return ("decimal", Decimal(normalized))
    except InvalidOperation:
        return ("text", normalized)


def _freeze_string_mapping(values: Mapping[str, str] | None) -> tuple[tuple[str, str], ...]:
    if not values:
        return ()

    frozen_items: list[tuple[str, str]] = []
    for key, value in values.items():
        if not isinstance(key, str) or not key.strip():
            raise ValueError("mapping keys must be non-empty strings")
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"mapping value for {key!r} must be a non-empty string")
        frozen_items.append((key.strip(), value.strip()))
    return tuple(sorted(frozen_items))


def _require_string(payload: Mapping[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be a non-empty string")
    return value.strip()


def _parse_datetime(value: str, field_name: str) -> datetime:
    normalized = value.strip()
    if normalized.endswith("Z"):
        normalized = f"{normalized[:-1]}+00:00"

    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid ISO-8601 timestamp") from exc

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return parsed
