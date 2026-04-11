# Planner and Entry Point

The short version is: JANUS now has a real planning layer.

The registry no longer hands its validated source objects straight into whatever comes next. There is now one explicit place that turns “this source is configured” into “this is the run we are about to execute, through this dispatch path, with these output targets, under this run id.”

That sounds small, but it matters. This is the step where the project stops being a set of contracts and starts behaving like a framework with an execution boundary.

## What was added

- `src/janus/planner/core.py` now defines the planner flow and the small objects that support it.
- `src/janus/planner/__init__.py` exposes the planner API for downstream imports.
- `src/janus/main.py` can now build a deterministic plan for one configured source from the command line.
- `tests/unit/planner/test_planner.py` covers the main planning path, strategy resolution, hook resolution, planner failures, and the CLI flow.

## What the planner is responsible for

The planner takes a few inputs:

- a `source_id`;
- an environment name;
- a project root;
- an optional explicit run id;
- an optional explicit start timestamp.

From there it does five things, in order:

1. loads the source registry;
2. fetches the validated `SourceConfig` for the requested source;
3. resolves the strategy family and strategy variant through metadata;
4. resolves an optional source hook if one is configured;
5. builds a runtime `ExecutionPlan` plus a pre-run metadata payload.

That means the planner now owns orchestration, but it still does not own extraction logic.

## The planner-facing objects

A few small objects were introduced so this layer can stay explicit without getting noisy.

### `PlanningRequest`

This is the narrow input object for planning.

It keeps the planner API from growing into a loose pile of function arguments, and it validates the things that really should be validated here, such as a non-empty source id and a timezone-aware explicit start timestamp.

### `StrategyBinding` and `StrategyCatalog`

These objects are the dispatch registry for the planner.

A binding says, in effect, “for this family and this variant, use this strategy implementation.” The catalog is then responsible for resolving the right binding from `SourceConfig.strategy` and `SourceConfig.strategy_variant`.

That resolution is metadata-driven on purpose. There is no branching by source name.

### `HookCatalog`

This is the planner-side lookup for optional source hooks.

If a source declares a hook id and no matching hook is registered, the planner fails with a direct error instead of quietly continuing in a half-configured state.

### `PlannedRun`

This is the full output of the planner.

It carries:

- the `ExecutionPlan`;
- the resolved strategy instance;
- the resolved hook, when present;
- a pre-run metadata payload that can later feed logging, lineage, or run tracking.

It also exposes a small summary shape so the CLI can print the planned run in a stable JSON form.

## One design choice worth calling out

The default strategy catalog currently wires every supported family and variant to a planning-only strategy implementation.

That is deliberate.

This task needed to prove that JANUS can resolve the right dispatch path and produce a clean runtime plan before the concrete API, file, and catalog strategy modules are implemented in later steps.

So the planner can already answer:

- which source is being run;
- which family and variant were selected;
- which hook was selected;
- which outputs and checkpoint settings apply;
- which run id and start time identify the run.

What it does not do yet is perform extraction. Later strategy tasks will replace those planning-only defaults with real implementations behind the same contract.

## The command-line flow

`janus` still supports the runtime-baseline behavior, but it can now also plan one source deterministically.

Example:

```bash
janus \
  --environment local \
  --source-id federal_open_data_example \
  --run-id run-20260408-001 \
  --started-at 2026-04-08T12:00:00+00:00
```

That command now:

- loads the selected environment profile;
- prepares the runtime paths;
- loads the requested source from the registry;
- resolves the dispatch path;
- prints a JSON summary of the planned run.

If the environment config is missing, the source does not exist, the source is disabled, the hook is missing, or the strategy variant is not registered, the command exits with a direct error instead of falling through into vague behavior.

## What the tests lock down

The planner tests are not pretending to be end-to-end ingestion tests.

They are there to protect the orchestration boundary.

They cover:

- building a valid plan from the checked-in example source;
- selecting a strategy implementation from metadata rather than source name;
- surfacing clear errors when a variant is not registered;
- surfacing clear errors when a configured hook is missing;
- printing a stable CLI planning summary for one configured source.

The broader unit suite still passes with these changes, which matters because `main.py` is shared with the earlier runtime-baseline work.

## What this step does not do yet

This step still does not:

- call an external API;
- download a file;
- build a Spark DataFrame;
- persist checkpoints;
- emit lineage records;
- run normalization;
- implement real source hooks;
- implement the concrete API, file, or catalog strategy cores.

Those pieces belong to the next layers.

The important part here is that they now have a clean handoff point. The control flow described in the foundation is no longer only aspirational. JANUS can now move from:

`validated source config -> planner -> execution plan and dispatch path`

without inventing orchestration on the fly in each later strategy module.
