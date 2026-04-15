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

Use `conf/sources/example/example_source.yaml` as the template for new source definitions.

Registry discovery is recursive, so files like `conf/sources/ibge/sidra.yaml` are valid. When one provider file needs several endpoint-backed source contracts, declare them under a top-level `sources:` list.

There are two valid top-level shapes:

1. One source per file.

```yaml
source_id: transparencia_servidores_por_orgao
name: Portal da Transparencia - Servidores agregados por orgao
...
```

2. Several source entries in one provider file.

```yaml
sources:
  - source_id: ibge_pib_brasil
    name: IBGE - PIB nacional a precos correntes
    ...
  - source_id: ibge_agro_abacaxi_pronaf
    name: IBGE - Quantidade produzida de abacaxi
    ...
```

Important detail: `sources:` must contain a YAML list. Each entry starts with `-`. If you repeat keys like `source_id`, `access`, or `outputs` under one mapping instead of using a list item, the file is invalid YAML.

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

### What The Main Blocks Currently Support

The registry contract is intentionally small. The most important current options are:

### `access`

- API and catalog sources require `access.base_url` or `access.url`.
- File sources require `access.url`, `access.path`, or `access.discovery_pattern`.
- `access.format` must match the payload you expect JANUS to fetch, such as `json`, `jsonl`, `csv`, `parquet`, `text`, or `binary`.
- `access.method` must be a supported HTTP method such as `GET` or `POST`.
- `access.auth.type` controls auth shape. Supported values today are `none`, `header_token`, `bearer_token`, `query_token`, and `basic`.
- `access.pagination.type` must match the chosen family variant when pagination is used: `page_number`, `offset`, `cursor`, or `none`.
- `access.rate_limit` carries `requests_per_minute`, optional `backoff_seconds`, and optional `concurrency`.
- `access.params` is the home for static literal request parameters.
- `access.request_inputs` is an optional API-only block for bounded runtime request contexts before pagination starts.
- `access.parameter_bindings` is an optional API-only block for request parameters resolved from the current request input or checkpoint state.

### API Request Shaping

For API sources, keep the request contract split by responsibility:

- `access.params` holds fixed literals that should be sent on every request.
- `access.request_inputs` defines the bounded outer request contexts JANUS should resolve before pagination starts.
- `access.parameter_bindings` maps request parameter names to runtime values from the current request input or the current checkpoint.
- hooks remain the escape hatch when the request still needs source-specific preparation after those three layers are exhausted.

If `access.request_inputs` is omitted, JANUS keeps the existing one-stream behavior with `type: none`.

Supported request-input types:

- `none`
- `date_window` with `start`, `end`, and `step`
- `iceberg_rows` with `namespace`, `table_name`, `columns`, and optional `distinct`
- `combined` with an `inputs:` list of two or more `date_window` or `iceberg_rows` entries

Supported binding sources:

- `checkpoint_value`
- `request_input.window_start`
- `request_input.window_end`
- `request_input.<field>` for fields from `iceberg_rows` columns, or from any sub-input when `type: combined`

Keep the boundaries sharp:

- use `access.params` for literals such as `situacao: TODAS`;
- use `access.parameter_bindings` for values such as `mesAno`, `dataIdaDe`, `dataIdaAte`, or a detail identifier that changes per request input;
- use a hook only when the request shape is still irregular, such as custom signing, request-body generation, or a one-off cursor rule.

Request inputs do not replace pagination. They sit outside it. Choose the strategy variant and `access.pagination` that match the API request loop, then add `access.request_inputs` only when the endpoint also needs a bounded outer context.

### Example: Monthly Parameter Binding

This pattern is a good fit when the endpoint still paginates normally, but also expects one bounded date context per request stream.

```yaml
strategy: api
strategy_variant: page_number_api

access:
  params:
    situacao: TODAS
  request_inputs:
    type: date_window
    start: 2025-01-01
    end: 2025-03-31
    step: month
  parameter_bindings:
    mesAno:
      from: request_input.window_end
      format: "%Y%m"
    dataIdaDe:
      from: request_input.window_start
      format: "%Y-%m-%d"
    dataIdaAte:
      from: request_input.window_end
      format: "%Y-%m-%d"
  pagination:
    type: page_number
    page_param: pagina
    size_param: tamanhoPagina
    page_size: 50
```

JANUS resolves one monthly window at a time, binds the three runtime parameters for that window, and then runs the normal page loop inside each request stream.

### Example: Iceberg-Driven Identifier Iteration

This pattern is a good fit for detail endpoints that depend on identifiers JANUS already wrote to Bronze Iceberg.

```yaml
strategy: api
strategy_variant: page_number_api

access:
  params:
    situacao: ATIVO
  request_inputs:
    type: iceberg_rows
    namespace: bronze_transparencia
    table_name: orgaos__siape
    columns:
      orgao_codigo: codOrgaoExercicioSiape
    distinct: true
  parameter_bindings:
    codigoOrgao:
      from: request_input.orgao_codigo
  pagination:
    type: page_number
    page_param: pagina
    size_param: tamanhoPagina
    page_size: 50
```

JANUS loads one projected row per distinct `codOrgaoExercicioSiape` value, binds `codigoOrgao` from that request input, and keeps the shared pagination, retry, raw-persistence, and metadata flow unchanged inside each request stream.

### Example: Combined Identifier and Date Window

This pattern is a good fit when an endpoint requires both an upstream entity identifier and a bounded date context per request stream — neither input alone is sufficient.

```yaml
strategy: api
strategy_variant: page_number_api

access:
  request_inputs:
    type: combined
    inputs:
      - type: iceberg_rows
        namespace: bronze_transparencia
        table_name: orgaos
        columns:
          orgao_codigo: codigo
        distinct: true
      - type: date_window
        start: 2025-01-01
        end: 2025-12-31
        step: month
  parameter_bindings:
    codigoOrgao:
      from: request_input.orgao_codigo
    dataInicio:
      from: request_input.window_start
      format: "%Y-%m-%d"
    dataFinal:
      from: request_input.window_end
      format: "%Y-%m-%d"
  pagination:
    type: page_number
    page_param: pagina
    size_param: tamanhoPagina
    page_size: 50
```

JANUS computes the Cartesian product of the two input streams and runs one request stream per combination. With 5 org codes and 12 monthly windows, that produces 60 request streams, each bound with its own `codigoOrgao`, `dataInicio`, and `dataFinal`.

### `extraction`

- `extraction.mode` is one of `full_refresh`, `snapshot`, or `incremental`.
- `extraction.retry` currently supports `max_attempts`, `backoff_strategy`, and `backoff_seconds`.
- `extraction.checkpoint_field` and `extraction.checkpoint_strategy` matter only when the source is incremental.
- `extraction.checkpoint_strategy` is one of `none`, `max_value`, or `date_window`.
- `extraction.lookback_days` is optional and only matters when incremental checkpoint values are time-based.

Practical rule: if `extraction.mode` is `incremental`, define a real `checkpoint_field` and use a checkpoint strategy other than `none`.

### `schema`

The `schema` block is narrower than it may look at first glance. Today it supports only:

- `mode`
- `path`

Supported values are:

- `mode: infer`
- `mode: explicit`

Examples:

```yaml
schema:
  mode: infer
```

```yaml
schema:
  mode: explicit
  path: conf/schemas/transparencia/servidores_por_orgao_schema.json
```

If `mode` is `explicit`, `path` is required. JANUS does not currently support inline field definitions, inline types, or other schema metadata in the source YAML.

### `spark`

- `spark.input_format` tells JANUS how the normalization handoff should be read.
- `spark.write_mode` is one of `append`, `overwrite`, or `ignore`.
- `spark.repartition` is optional.
- `spark.partition_by` is an optional list of partition columns.
- `spark.read_options` is an optional string-to-string mapping for reader options such as CSV header, separator, or encoding.

### `outputs`

- Each source defines `outputs.raw`, `outputs.bronze`, and `outputs.metadata`.
- Each output target must define `path` and `format`.
- Only `outputs.bronze` may define `namespace` and `table_name`.
- `namespace` and `table_name` are only valid when `outputs.bronze.format` is `iceberg`.

### `quality`

- `quality.required_fields` is an optional list of fields that must be present.
- `quality.unique_fields` is an optional list of uniqueness hints.
- `quality.allow_schema_evolution` is a boolean flag.

If you are unsure whether a field belongs in config, check the example source and the typed contract before inventing a new key.

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
- output formats and paths, plus optional `outputs.bronze.namespace` and `outputs.bronze.table_name` for Iceberg bronze targets;
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

If `outputs.bronze.format` is `iceberg`, the source may also define `outputs.bronze.namespace` and `outputs.bronze.table_name` when the default path-derived Iceberg identifier is not the desired bronze table name.

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
- If bronze uses Iceberg, any explicit `namespace` and `table_name` are intentional and documented.
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
