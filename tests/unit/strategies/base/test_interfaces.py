from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from janus.models import (
    ExecutionPlan,
    ExtractedArtifact,
    ExtractionResult,
    RunContext,
    WriteResult,
)
from janus.models.source_config import SourceConfig
from janus.registry import load_registry
from janus.strategies.base import BaseStrategy, SourceHook


PROJECT_ROOT = Path(__file__).resolve().parents[4]


class RecordingHook(SourceHook):
    def __init__(self) -> None:
        self.events: list[str] = []

    def on_plan(self, plan: ExecutionPlan) -> ExecutionPlan:
        self.events.append("plan")
        return plan.with_note("hook-planned")

    def on_extraction_result(
        self, plan: ExecutionPlan, extraction_result: ExtractionResult
    ) -> ExtractionResult:
        del plan
        self.events.append("extract")
        return extraction_result.with_metadata("hook_extract", "true")

    def on_normalization_handoff(
        self, plan: ExecutionPlan, extraction_result: ExtractionResult
    ) -> ExtractionResult:
        del plan
        self.events.append("handoff")
        return extraction_result.with_metadata("hook_handoff", "true")

    def metadata_fields(
        self,
        plan: ExecutionPlan,
        extraction_result: ExtractionResult,
        write_results: tuple[WriteResult, ...] = (),
    ) -> dict[str, object]:
        del plan
        del extraction_result
        self.events.append("metadata")
        return {
            "hook_metadata": True,
            "write_zones": [result.zone for result in write_results],
        }


class FakeStrategy(BaseStrategy):
    def __init__(self, family: str) -> None:
        self._family = family

    @property
    def strategy_family(self) -> str:
        return self._family

    def plan(
        self,
        source_config: SourceConfig,
        run_context: RunContext,
        hook: SourceHook | None = None,
    ) -> ExecutionPlan:
        plan = ExecutionPlan.from_source_config(source_config, run_context).with_note(
            f"strategy:{self.strategy_family}"
        )
        if hook is not None:
            return hook.on_plan(plan)
        return plan

    def extract(
        self,
        plan: ExecutionPlan,
        hook: SourceHook | None = None,
    ) -> ExtractionResult:
        extraction_result = ExtractionResult.from_plan(
            plan,
            artifacts=(
                ExtractedArtifact(
                    path=f"{plan.raw_output.path}/batch-0001.{plan.raw_output.format}",
                    format=plan.raw_output.format,
                ),
            ),
            records_extracted=1,
            metadata={"strategy_family": self.strategy_family},
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
        if hook is not None:
            return hook.on_normalization_handoff(plan, extraction_result)
        return extraction_result

    def emit_metadata(
        self,
        plan: ExecutionPlan,
        extraction_result: ExtractionResult,
        write_results: tuple[WriteResult, ...] = (),
        hook: SourceHook | None = None,
    ) -> dict[str, object]:
        metadata = {
            "run_id": plan.run_context.run_id,
            "source_id": plan.source.source_id,
            "strategy_family": self.strategy_family,
            "records_extracted": extraction_result.records_extracted,
        }
        if hook is not None:
            metadata.update(hook.metadata_fields(plan, extraction_result, write_results))
        return metadata


def test_source_hook_defaults_to_no_op_behavior():
    source_config = load_registry(PROJECT_ROOT).get_source("federal_open_data_example")
    run_context = RunContext.create(
        run_id="run-20260408-010",
        environment="local",
        project_root=PROJECT_ROOT,
    )
    plan = ExecutionPlan.from_source_config(source_config, run_context)
    extraction_result = ExtractionResult.from_plan(plan, artifacts=())
    write_result = WriteResult.from_plan(plan, "metadata", records_written=1)
    hook = SourceHook()

    assert hook.on_plan(plan) is plan
    assert hook.on_extraction_result(plan, extraction_result) is extraction_result
    assert hook.on_normalization_handoff(plan, extraction_result) is extraction_result
    assert hook.metadata_fields(plan, extraction_result, (write_result,)) == {}


@pytest.mark.parametrize(
    ("family", "variant"),
    [
        ("api", "page_number_api"),
        ("file", "static_file"),
        ("catalog", "metadata_catalog"),
    ],
)
def test_api_file_and_catalog_strategies_can_share_the_same_base_contract(
    family: str, variant: str
):
    base_source = load_registry(PROJECT_ROOT).get_source("federal_open_data_example")
    source_config = replace(
        base_source,
        source_id=f"{family}_source",
        name=f"{family.title()} Source",
        source_type=family,
        strategy=family,
        strategy_variant=variant,
    )
    run_context = RunContext.create(
        run_id=f"run-20260408-{family}",
        environment="local",
        project_root=PROJECT_ROOT,
        started_at=datetime(2026, 4, 8, 12, 0, tzinfo=UTC),
    )
    hook = RecordingHook()
    strategy = FakeStrategy(family)

    plan = strategy.plan(source_config, run_context, hook)
    extraction_result = strategy.extract(plan, hook)
    handoff = strategy.build_normalization_handoff(plan, extraction_result, hook)
    write_result = WriteResult.from_plan(plan, "bronze", records_written=1)
    metadata = strategy.emit_metadata(plan, handoff, (write_result,), hook)

    assert strategy.strategy_family == family
    assert plan.source.strategy == family
    assert plan.notes == (f"strategy:{family}", "hook-planned")
    assert extraction_result.metadata_as_dict() == {
        "hook_extract": "true",
        "strategy_family": family,
    }
    assert handoff.metadata_as_dict() == {
        "hook_extract": "true",
        "hook_handoff": "true",
        "strategy_family": family,
    }
    assert metadata == {
        "run_id": f"run-20260408-{family}",
        "source_id": f"{family}_source",
        "strategy_family": family,
        "records_extracted": 1,
        "hook_metadata": True,
        "write_zones": ["bronze"],
    }
    assert hook.events == ["plan", "extract", "handoff", "metadata"]


def test_base_strategy_remains_abstract_until_all_lifecycle_methods_are_implemented():
    with pytest.raises(TypeError):
        BaseStrategy()
