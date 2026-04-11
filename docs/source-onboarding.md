# JANUS Source Onboarding Guide

This guide is for adding a new source without turning JANUS into a pile of exceptions.

The short version is:

1. prove the source belongs in JANUS;
2. classify it into an existing strategy family and variant;
3. express as much as possible in source config;
4. reuse the family runtime;
5. add a source hook only when there is a clear, narrow mismatch;
6. add tests and documentation for anything reusable.

## Before you add a source

Only onboard a source if it is:

- federal;
- clearly public and legally reusable;
- documented enough to access responsibly;
- useful for evaluating JANUS as a framework;
- representative of a real ingestion pattern.

If the source is private, protected, anti-bot guarded, or outside the federal scope, it does not belong in phase 1.

## Step 1: classify the source first

Choose the source family before you write code.

### Use `api` when:

- the source exposes records through HTTP requests;
- pagination, rate limits, headers, or checkpoint query parameters matter;
- the main unit of extraction is an API response payload.

Supported variants today:

- `page_number_api`
- `offset_api`
- `cursor_api`
- `date_window_api`

### Use `file` when:

- the main unit of extraction is a file or package;
- discovery, version selection, checksums, or archive extraction matter;
- the important handoff is a persisted file artifact rather than an API record page.

Supported variants today:

- `static_file`
- `versioned_file`
- `archive_package`

### Use `catalog` when:

- the source is a metadata registry;
- the value is in datasets, organizations, groups, and resources;
- JANUS should ingest metadata about resources, not the resource payloads themselves.

Supported variants today:

- `metadata_catalog`
- `resource_catalog`

If you cannot classify the source cleanly, stop there. Do not start coding until the classification is clear.

## Step 2: start from the existing contract

Use `conf/sources/example_source.yaml` as the template for new source definitions.

Every source should define:

- identity fields such as `source_id`, `name`, `owner`, `domain`, and `enabled`;
- family metadata such as `source_type`, `strategy`, and `strategy_variant`;
- the `access` block for endpoints, paths, auth, pagination, rate limits, and formats;
- the `extraction` block for refresh mode, retry policy, and checkpoint semantics;
- the `schema` block;
- the `spark` block;
- the `outputs` block for `raw`, `bronze`, and `metadata`;
- the `quality` block.

Keep `source_type` and `strategy` aligned. In the current JANUS design, family and strategy are the same concept.

## Step 3: prefer config reuse over code

Before touching Python, ask:

- Can this source fit an existing family and variant?
- Can the odd behavior be expressed through config already?
- Are output paths, schema mode, and checkpoint rules declarative?
- Is the only missing piece a runtime secret value that should come from the environment?

If the answer is yes, stay in YAML.

Examples of behavior that should stay in config:

- base URL or direct file URL;
- auth mode and env var names;
- page size or offset parameter names;
- checkpoint field and checkpoint strategy;
- output formats and paths;
- quality rules such as required fields and uniqueness keys.

## Step 4: choose the lightest extension point

When config is not enough, choose the smallest honest extension.

### Use an existing strategy as-is when:

- the source follows the family pattern already;
- only source values differ;
- there is no real runtime mismatch.

### Add a source hook when:

- the source mostly fits the family;
- one or two edges differ in a source-specific way;
- the change can stay isolated to that source.

Good hook use cases:

- a custom next-cursor field for one API;
- a checkpoint query parameter that does not match the checkpoint field name;
- a version token hidden in one file naming convention;
- archive-member selection rules for one package layout;
- a non-standard wrapper around one catalog payload.

### Improve the family strategy when:

- the behavior is reusable across sources;
- a second source would benefit from the same logic;
- the change belongs to the pattern, not the source identity.

If the only implementation plan is "copy the nearest source and adjust it," you are in the wrong extension point.

## Step 5: keep hooks on a short leash

Hooks are allowed, but they are not a second home for generic logic.

A good hook is:

- tied to one source;
- small enough to explain in a paragraph;
- named after the source, not after a vague generic behavior;
- covered by focused tests.

A bad hook is:

- branching on several source ids;
- reimplementing strategy loops;
- doing work the registry should have validated already;
- hiding business rules that really belong in a reusable family helper.

When in doubt, make the hook smaller or redesign the family boundary.

## Step 6: define outputs and checkpoints deliberately

Do not treat outputs as an afterthought.

Each source must define:

- a raw path for preserved upstream artifacts;
- a bronze path for minimally standardized structured data;
- a metadata path for run records, checkpoints, lineage, and validations.

Checkpoint choices should match the family:

- API sources usually checkpoint by record timestamp or field value;
- file sources usually checkpoint by publication/version token;
- catalog sources usually checkpoint by metadata freshness, not underlying business facts.

If a source cannot explain its checkpoint semantics clearly, incremental mode is probably premature.

## Step 7: test what you changed

At minimum, add or update tests for the behavior you introduced.

Typical coverage includes:

- source-config validation or registry loading;
- planner resolution for the chosen family and variant;
- strategy behavior for the new source shape;
- hook behavior when a hook is necessary;
- metadata, checkpoint, or validation side effects when they changed.

Do not rely on a manual run as the only proof that a new pattern belongs in JANUS.

## Step 8: update the docs when the pattern changed

Update the contributor docs when you introduce:

- a new reusable strategy variant;
- a new hook boundary contributors are expected to use;
- a new reproducibility requirement;
- a new anti-pattern the project should reject explicitly.

If the code learns a new reusable pattern and the docs stay silent, the next contributor will reverse-engineer it from implementation details, which is exactly what JANUS is trying to avoid.

## Onboarding checklist

Use this checklist before considering a source "added":

- The source is federal, public, and in scope.
- The family and variant are explicit.
- The YAML contract is complete and validated.
- Secrets are referenced by env var name only.
- Output paths land in raw, bronze, and metadata zones.
- Checkpoint semantics are intentional, or incremental mode is disabled.
- Existing strategy behavior was reused wherever possible.
- Any hook is narrow, source-local, and tested.
- Any new reusable pattern is reflected in the docs.

## Anti-patterns

Do not onboard a source by:

- copying an old implementation and renaming identifiers;
- adding `if source_id == ...` in generic strategy code;
- teaching the registry about one source's business rules;
- hardcoding tokens, cookies, usernames, or passwords in YAML;
- writing outside the configured storage layout;
- treating raw persistence as optional because the payload looks simple;
- skipping validation and metadata because a first run "looked fine".

JANUS grows by reusable patterns and explicit contracts. If a source does not fit those rules yet, the right move is to improve the framework boundary, not to sneak around it.
