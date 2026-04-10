from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from janus.models import ExecutionPlan

SUPPORTED_VALIDATION_OUTCOMES = frozenset({"failed", "passed", "skipped"})
SUPPORTED_VALIDATION_PHASES = frozenset({"config", "data", "output"})


def _freeze_string_mapping(values: Mapping[str, Any] | None = None) -> tuple[tuple[str, str], ...]:
    if not values:
        return ()
    return tuple((str(key), str(value)) for key, value in sorted(values.items()))


@dataclass(frozen=True, slots=True)
class ValidationCheck:
    """One reusable validation result emitted by the quality layer."""

    phase: str
    name: str
    outcome: str
    message: str
    details: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if self.phase not in SUPPORTED_VALIDATION_PHASES:
            allowed = ", ".join(sorted(SUPPORTED_VALIDATION_PHASES))
            raise ValueError(f"phase must be one of: {allowed}")
        if self.outcome not in SUPPORTED_VALIDATION_OUTCOMES:
            allowed = ", ".join(sorted(SUPPORTED_VALIDATION_OUTCOMES))
            raise ValueError(f"outcome must be one of: {allowed}")
        if not self.name.strip():
            raise ValueError("name must not be empty")
        if not self.message.strip():
            raise ValueError("message must not be empty")

    @classmethod
    def passed(
        cls,
        phase: str,
        name: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> ValidationCheck:
        return cls(
            phase=phase,
            name=name,
            outcome="passed",
            message=message,
            details=_freeze_string_mapping(details),
        )

    @classmethod
    def failed(
        cls,
        phase: str,
        name: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> ValidationCheck:
        return cls(
            phase=phase,
            name=name,
            outcome="failed",
            message=message,
            details=_freeze_string_mapping(details),
        )

    @classmethod
    def skipped(
        cls,
        phase: str,
        name: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> ValidationCheck:
        return cls(
            phase=phase,
            name=name,
            outcome="skipped",
            message=message,
            details=_freeze_string_mapping(details),
        )

    @property
    def is_successful(self) -> bool:
        return self.outcome != "failed"

    def details_as_dict(self) -> dict[str, str]:
        return dict(self.details)

    def to_dict(self) -> dict[str, Any]:
        return {
            "phase": self.phase,
            "name": self.name,
            "outcome": self.outcome,
            "message": self.message,
            "details": self.details_as_dict(),
        }


@dataclass(frozen=True, slots=True)
class ValidationReport:
    """Validation report persisted for one run in the metadata zone."""

    run_id: str
    source_id: str
    source_name: str
    environment: str
    strategy_family: str
    strategy_variant: str
    emitted_at: datetime
    checks: tuple[ValidationCheck, ...]
    metadata: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        if not self.run_id.strip():
            raise ValueError("run_id must not be empty")
        if not self.source_id.strip():
            raise ValueError("source_id must not be empty")
        if not self.source_name.strip():
            raise ValueError("source_name must not be empty")
        if not self.environment.strip():
            raise ValueError("environment must not be empty")
        if not self.strategy_family.strip():
            raise ValueError("strategy_family must not be empty")
        if not self.strategy_variant.strip():
            raise ValueError("strategy_variant must not be empty")
        if self.emitted_at.tzinfo is None or self.emitted_at.utcoffset() is None:
            raise ValueError("emitted_at must be timezone-aware")

    @classmethod
    def from_plan(
        cls,
        plan: ExecutionPlan,
        checks: Sequence[ValidationCheck],
        *,
        emitted_at: datetime | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> ValidationReport:
        return cls(
            run_id=plan.run_context.run_id,
            source_id=plan.source.source_id,
            source_name=plan.source.name,
            environment=plan.run_context.environment,
            strategy_family=plan.source.strategy,
            strategy_variant=plan.source.strategy_variant,
            emitted_at=emitted_at or datetime.now(tz=UTC),
            checks=tuple(checks),
            metadata=_freeze_string_mapping(metadata),
        )

    @property
    def failed_checks(self) -> tuple[ValidationCheck, ...]:
        return tuple(check for check in self.checks if check.outcome == "failed")

    @property
    def is_successful(self) -> bool:
        return not self.failed_checks

    def summary(self) -> dict[str, int]:
        return {
            "passed": sum(1 for check in self.checks if check.outcome == "passed"),
            "failed": sum(1 for check in self.checks if check.outcome == "failed"),
            "skipped": sum(1 for check in self.checks if check.outcome == "skipped"),
        }

    def metadata_as_dict(self) -> dict[str, str]:
        return dict(self.metadata)

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "source_id": self.source_id,
            "source_name": self.source_name,
            "environment": self.environment,
            "strategy_family": self.strategy_family,
            "strategy_variant": self.strategy_variant,
            "emitted_at": self.emitted_at.isoformat(),
            "summary": self.summary(),
            "checks": [check.to_dict() for check in self.checks],
            "metadata": self.metadata_as_dict(),
        }


class QualityValidationError(RuntimeError):
    """Raised when one or more reusable JANUS quality checks fail."""

    def __init__(self, report: ValidationReport) -> None:
        self.report = report
        lines = [
            "Validation failed for source "
            f"{report.source_id!r} run {report.run_id!r}:"
        ]
        for check in report.failed_checks:
            lines.append(f"- [{check.phase}.{check.name}] {check.message}")
        super().__init__("\n".join(lines))
