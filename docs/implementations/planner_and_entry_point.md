# Planner, Executor, and Entry Point

The short version is: JANUS now has a real framework execution path.

The registry no longer hands validated source objects straight into ad hoc code, and the CLI no longer stops at a planning summary. There is now one explicit flow that turns “this source is configured” into “this source was planned and, when requested, executed through the framework runtime with explicit outputs and metadata.”

That matters because it keeps JANUS aligned with the foundation and PRD principles: metadata-driven where possible, strategy-based by family, extension by pattern instead of script copy, and reproducible by design.

## What was added

- `src/janus/planner/core.py` defines the planning flow and the small planner-facing objects.
- `src/janus/runtime/executor.py` defines the runtime orchestration layer through `SourceExecutor` and the execution result contract through `ExecutedRun`.
- `src/janus/runtime/__init__.py` exposes the runtime API for CLI and tests.
- `src/janus/main.py` now supports both deterministic planning and full framework execution from the command line.
- `tests/unit/planner/test_planner.py` covers planning, disabled-source handling, and CLI wiring.
- `tests/unit/runtime/test_executor.py` covers the runtime orchestration path, quality failures, and execution summaries.

## What the planner is responsible for

The planner takes a small set of explicit inputs:

- a `source_id`;
- an environment name;
- a project root;
- an optional explicit run id;
- an optional explicit start timestamp;
- an optional `include_disabled` flag for protected operational cases.

From there it does five things, in order:

1. loads the source registry;
2. fetches the validated `SourceConfig` for the requested source;
3. resolves the strategy family and strategy variant through metadata;
4. resolves an optional source hook if one is configured;
5. builds a runtime `ExecutionPlan` plus pre-run metadata.

That means the planner owns deterministic dispatch, but it does not own extraction mechanics.

## What the runtime executor is responsible for

`SourceExecutor` is the framework-native orchestration layer that takes a `PlannedRun` and executes it through the existing JANUS building blocks.

For one selected source it performs the following steps:

1. starts run observation;
2. calls the resolved strategy extraction flow;
3. projects raw extracted artifacts into runtime-visible `WriteResult` objects;
4. builds the normalization handoff from the strategy;
5. reads the handoff into Spark;
6. runs the base normalizer;
7. writes the bronze dataset through the configured storage layout;
8. emits strategy-level metadata;
9. validates and persists quality results;
10. records success or failure, including lineage and checkpoint metadata.

This keeps execution inside the framework boundary instead of pushing contributors toward one-off scripts for each source.

## The planner-facing objects

A few small objects keep this layer explicit without getting noisy.

### `PlanningRequest`

This is the narrow input object for planning.

It prevents the planning API from turning into a loose pile of parameters and validates what really belongs here, such as a non-empty source id and a timezone-aware explicit start timestamp. It also carries `include_disabled`, which lets operators intentionally run a configured but protected source without weakening the default registry behavior.

### `StrategyBinding` and `StrategyCatalog`

These objects are the dispatch registry for the planner.

A binding says, effectively, “for this family and this variant, use this strategy implementation.” The catalog resolves the right binding from `SourceConfig.strategy` and `SourceConfig.strategy_variant`.

That resolution is metadata-driven on purpose. There is no branching by source name.

### `HookCatalog`

This is the planner-side lookup for optional source hooks.

If a source declares a hook id and no matching hook is registered, the planner fails with a direct error instead of continuing in a half-configured state.

### `PlannedRun`

This is the planner output.

It carries the `ExecutionPlan`, the resolved strategy instance, the resolved hook when present, and pre-run metadata that can later feed lineage, logging, or run tracking.

### `ExecutedRun`

This is the runtime execution result.

It carries execution status, extracted artifacts, materialized outputs, persisted validation metadata, lineage/checkpoint paths, strategy metadata, and failure details when a run does not succeed. The CLI prints it as `executed_run` so operators get a stable machine-readable summary of what happened.

## The command-line flow

`janus` now supports three useful modes.

### 1. Runtime validation only

```bash
janus --environment local
```

This validates the environment profile, prepares runtime paths, and prints the resolved runtime summary.

### 2. Deterministic planning for one source

```bash
janus       --environment local       --source-id federal_open_data_example       --run-id run-20260409-001       --started-at 2026-04-09T12:00:00+00:00
```

That command loads the source from the registry, resolves the dispatch path, and prints a `planned_run` summary.

### 3. Full framework execution for one source

```bash
janus       --environment local       --source-id federal_open_data_example       --run-id run-20260409-001       --started-at 2026-04-09T12:00:00+00:00       --execute
```

That command performs planning plus extraction, Spark normalization, bronze writing, validation persistence, and lineage/checkpoint recording. The resulting JSON contains both `planned_run` and `executed_run`.

If a source is intentionally disabled in the registry, the operator must opt in explicitly:

```bash
janus       --environment local       --source-id transparencia_servidores_por_orgao       --include-disabled       --execute
```

That keeps the default behavior safe while still supporting controlled operational runs.

## What the tests lock down

The planner and runtime tests protect the orchestration boundary rather than pretending to be source-specific business tests.

They cover:

- building a valid plan from the checked-in example source;
- selecting a strategy implementation from metadata rather than source name;
- surfacing clear errors when a variant is not registered;
- surfacing clear errors when a configured hook is missing;
- allowing disabled sources only when explicitly requested;
- printing a stable CLI planning summary;
- wiring the CLI execution path to `SourceExecutor`;
- returning failed execution status when quality validation fails.

## Why this matters architecturally

This step is the point where JANUS starts behaving like a framework instead of a collection of helper modules.

The foundation and PRD both require the project to be modular by strategy, reproducible, safe by default, and extensible by pattern rather than by duplicated scripts. A real CLI execution path is part of that promise. It gives contributors one shared control plane for running sources through the framework contracts that already exist, instead of inventing a new runner for each dataset.

That does not mean every future source will be zero-code. It means any source-specific differences must now enter through the expected seams: configuration, strategy variants, and isolated hooks.
