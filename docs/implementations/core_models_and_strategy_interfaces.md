# Core Models and Strategy Interfaces

The short version is: JANUS now has the runtime contract layer that sits between the source registry work and the later planner and strategy tasks.

This step was not about downloading anything yet. It was about stopping the project from drifting into raw dictionaries, ad hoc handoffs, and strategy code that quietly invents its own object shapes.

The result is a small set of typed runtime models, one shared strategy contract, one minimal hook contract, and tests that make those boundaries harder to erode later.

## What was added

- `src/janus/models/contracts.py` now defines the runtime-facing models used after config validation.
- `src/janus/models/__init__.py` now re-exports the new contract objects alongside the existing source-config models.
- `src/janus/strategies/base/interfaces.py` now defines the base strategy lifecycle and the source-hook interface.
- `src/janus/strategies/base/__init__.py` exposes the base interfaces cleanly for downstream imports.
- `tests/unit/models/test_contracts.py` covers the core model behavior.
- `tests/unit/strategies/base/test_interfaces.py` covers the shared strategy and hook expectations.

## The runtime models

The new models are intentionally small.

### `SourceReference`

This is the lighter runtime identity for a source. It keeps the fields that later layers actually need for planning, logging, and metadata emission without dragging the full source config everywhere.

It is created from `SourceConfig`, so the registry remains the point where contract validation happens.

### `RunContext`

This object carries run-scoped information such as run id, environment, project root, start time, and simple string attributes.

The main reason it exists is to keep runtime tracing explicit from the beginning instead of letting run identifiers get threaded through the code as loose function arguments.

### `ExecutionPlan`

This is the planner handoff object.

It takes a validated `SourceConfig` plus a `RunContext` and turns that into the smaller runtime shape that strategies are expected to consume. It keeps the output targets and checkpoint mode visible on purpose. Yes, some of that information already exists inside `SourceConfig`, but later layers should receive a plan, not re-parse config details every time.

### `ExtractedArtifact` and `ExtractionResult`

Extraction needs to hand something concrete to the normalization layer. For now that handoff is deliberately simple: extracted raw artifacts, a record count when known, an optional checkpoint value, and small string metadata.

That is enough for later tasks to build readers, raw persistence, and normalization without inventing another near-duplicate result object too early.

### `WriteResult`

This model records what one write operation produced for one JANUS zone.

It keeps the zone explicit (`raw`, `bronze`, or `metadata`) and carries the path, format, mode, row count, partitioning, and small string metadata. That should make later lineage and run-metadata work much easier to bolt on cleanly.

## The base strategy contract

The new base strategy interface defines four responsibilities and nothing more:

- build a plan from a validated source config and run context;
- extract raw data for that plan;
- hand off the extraction result to normalization;
- emit metadata for the run.

Just as important is what the interface does not do. It does not know about HTTP, files, catalogs, Spark, checkpoints on disk, or source-specific parsing rules. Those belong in later tasks and in the appropriate strategy families.

## The hook contract

The hook interface is intentionally boring.

A hook can:

- adjust a plan;
- adjust an extraction result;
- adjust the normalization handoff;
- add source-local metadata fields.

That is enough to make source-specific behavior explicit without teaching the base layer about one awkward source.

If future work starts pushing API pagination details or file-layout quirks into this contract, that is a sign the logic belongs in a strategy module instead.

## A couple of design choices worth calling out

One choice here may look repetitive at first glance: `ExecutionPlan` keeps output targets and checkpoint settings even though those values already came from the source config.

That duplication is deliberate. The planner needs to hand later layers a runtime object that says, “this is the run we are doing,” not a full config object that every downstream caller has to interpret for itself.

Another choice is using `ExtractionResult` as the normalization handoff shape for now. That keeps the contract simple enough for the next tasks while still being explicit about what extraction produced.

## What the tests lock down

The tests were written to protect the contract shape, not to fake a whole ingestion system.

They cover:

- projection from `SourceConfig` into the smaller runtime models;
- guardrails such as valid output zones and matching source ids;
- the default no-op behavior of `SourceHook`;
- the expectation that API, file, and catalog strategies can all implement the same base contract.

That last point matters. The whole purpose of this step was to make the shared lifecycle real before the first strategy family starts adding its own habits.

## What this step does not do yet

This step does not:

- choose a concrete strategy implementation;
- call external APIs;
- download files;
- build Spark DataFrames;
- persist lineage or run metadata;
- implement real source hooks.

Those pieces belong to the next tasks. The point here was to make sure those later layers inherit a stable contract instead of inventing one on the fly.
