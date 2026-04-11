# Observability, Checkpoints, and Lineage

The short version is: JANUS now has a metadata trail for runs.

This step was about giving reruns, failures, and downstream strategy work somewhere concrete to land. Before it, the planner could describe a run, but the project still had no shared way to persist what happened, remember checkpoint progress, or record enough context to diagnose a failure without digging through ad hoc logs.

The result is a small observability layer built around the existing runtime contracts: run metadata records, lineage records, monotonic checkpoints, and structured logging that redacts secrets by default.

## What was added

- `src/janus/checkpoints/store.py` now defines the checkpoint state, checkpoint history, and the persistence rules for rerun-safe writes.
- `src/janus/checkpoints/__init__.py` now exposes the checkpoint API for downstream imports.
- `src/janus/lineage/models.py` now defines the run metadata and lineage models used to describe one run from start to finish.
- `src/janus/lineage/persistence.py` now resolves metadata-zone paths and writes JSON artifacts atomically.
- `src/janus/lineage/store.py` now provides the persistence helpers and the shared `RunObserver` service.
- `src/janus/lineage/__init__.py` now exposes the lineage API without forcing callers into internal module paths.
- `src/janus/utils/logging.py` now provides structured JSON logging with secret redaction.
- `tests/unit/checkpoints/test_store.py` covers checkpoint persistence and rerun behavior.
- `tests/unit/lineage/test_run_observer.py` covers run metadata, lineage persistence, and failure capture.
- `tests/unit/lineage/test_logging.py` covers redaction and structured log output.

## The metadata artifacts

There are now two main metadata records for a run.

### `RunMetadata`

This is the operational record.

It captures the run id, source identity, strategy family and variant, execution mode, checkpoint settings, configured outputs, materialized outputs, timing, row counts when known, and failure details when a run ends badly.

The intent is straightforward: after a run finishes, there should be one place to answer basic questions such as:

- which source ran;
- when it started and ended;
- which strategy variant handled it;
- which outputs were written;
- whether it succeeded or failed;
- what the failure reason was.

### `LineageRecord`

This is the provenance-facing record.

It carries the fields that matter when we want to connect one run to the exact config and artifacts that produced its outputs. That includes:

- a hash of the source config file used for the run;
- the configured output targets;
- the materialized outputs actually written;
- the raw extraction artifacts;
- extraction metadata;
- checkpoint and failure details when relevant.

The config hash is important here. It gives later work a stable way to say, “this output came from this version of the source definition,” without relying on a vague assumption about what the YAML looked like at the time.

## The metadata-zone layout

This step also establishes a clearer layout under the metadata zone for one source.

Given the configured metadata output path, the code now writes:

- `runs/<run_id>.json` for the run record;
- `lineage/<run_id>.json` for the lineage record;
- `checkpoints/current.json` for the latest stored checkpoint state;
- `checkpoints/history/<run_id>.json` for the write decision taken during that run.

Those files are written atomically so a rerun does not leave half-written metadata behind if the process is interrupted at the wrong moment.

## The checkpoint rules

The checkpoint layer was written with reruns in mind rather than as a thin “dump a value to disk” helper.

A checkpoint write now records both the current state and the decision taken during the write. The store supports four outcomes:

- `advanced` when a new value moves the checkpoint forward;
- `reused` when the new value matches the current one;
- `retained` when a rerun tries to write an older value and the store keeps the newer checkpoint;
- `skipped` when checkpointing is disabled or there is no usable value to persist.

That means a rerun cannot quietly move the checkpoint backwards and make later incremental runs behave as if newer data had never been seen.

The comparison logic tries to do the sensible thing for the common cases first. ISO-8601 timestamps are compared as datetimes, numeric values are compared numerically, and only then does it fall back to plain string comparison.

## The `RunObserver`

The new `RunObserver` is the shared service future strategy modules are expected to call.

It gives the runtime three clear points of interaction:

- `start_run(plan)` to persist the initial running state;
- `record_success(...)` to persist the final run record, lineage record, and checkpoint state;
- `record_failure(...)` to persist the failed run record and failure-oriented lineage details.

That keeps later strategy code from inventing its own checkpoint files or run logs in parallel.

## Structured logging and redaction

This step also adds a small structured logging module.

The logger emits JSON lines and sanitizes nested payloads before they are written. The redaction rules cover the things that are most likely to leak in ingestion code:

- authorization headers;
- API tokens and token-like header names;
- cookies and session identifiers;
- password and secret fields;
- sensitive query parameters such as `token`, `api_key`, and `access_token`.

The point is not to hide all detail. The point is to keep the logs useful while removing the values that should never end up in routine run output.

## What the tests lock down

The new unit tests were written to protect the operational behavior of the layer rather than just its object shapes.

They cover:

- writing and loading the current checkpoint state;
- preventing an older rerun from moving the checkpoint backward;
- skipping checkpoint writes cleanly when checkpointing is disabled;
- persisting the running and successful run records through the observer;
- persisting failure reasons in both run metadata and lineage;
- redacting secret-bearing log fields while preserving the rest of the event payload.

The focused verification for this step passed with:

- `python -m py_compile ...`
- `python -m pytest tests/unit/checkpoints/test_store.py tests/unit/lineage/test_run_observer.py tests/unit/lineage/test_logging.py`
- `python -m ruff check src/janus/checkpoints src/janus/lineage src/janus/utils/logging.py tests/unit/checkpoints tests/unit/lineage`
