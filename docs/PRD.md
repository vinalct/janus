# PRD: JANUS

## 1. Document Status

- **Project:** JANUS
- **Version:** 1.0
- **Date:** 2026-04-08
- **Type:** Product Requirements Document
- **Product class:** Internal data engineering framework / academic project foundation

---

## 2. Executive Summary

JANUS is a metadata-driven, strategy-based, reproducible, safe, and extensible Spark framework for ingesting Brazilian federal open data. Its first version is intentionally limited in scope: it will support 3 to 5 clearly public federal sources and prove that heterogeneous public data can be ingested through a clean and maintainable architecture instead of a growing collection of ad hoc scripts.

The product goal is not to centralize all public Brazilian data in the first phase. The goal is to build a strong ingestion foundation that separates reusable framework concerns from strategy-specific behavior and isolates truly source-specific logic when needed.

JANUS is also the practical implementation core of a post-graduation project and the architectural basis for future academic evolution.

---

## 3. Problem Statement

Brazilian federal public data is available through multiple exposure patterns, including APIs, downloadable files, and metadata catalogs. These sources differ in protocol, authentication, pagination, schema stability, update semantics, file publication patterns, rate limits, and payload structure.

Without a framework, ingestion usually devolves into duplicated scripts, hardcoded endpoints, inconsistent retry logic, weak reproducibility, and fragile maintenance. This makes onboarding new sources slow and causes architectural decay as scope grows.

JANUS addresses this by providing a generic core for common ingestion concerns, strategy modules for source families, and small source-specific adapters only when needed.

---

## 4. Product Vision

JANUS will provide a practical and academically credible data engineering framework that:

- ingests heterogeneous federal Brazilian public data sources safely and reproducibly;
- uses metadata and configuration wherever behavior is declarative;
- organizes execution by explicit strategy families instead of one-off scripts;
- remains portable across local, containerized, and cluster Spark environments;
- stays simple enough for a post-graduation delivery while leaving room for future growth.

---

## 5. Product Goals

### 5.1 Primary Goals

1. Build a working Spark-based ingestion framework for federal Brazilian open data.
2. Demonstrate that heterogeneous sources can be supported without script sprawl.
3. Enable source onboarding through YAML-driven definitions plus reusable strategies.
4. Guarantee reproducible execution across local, containerized, and cluster-like environments.
5. Make safety, observability, checkpointing, and rerun behavior first-class concerns.

### 5.2 Secondary Goals

1. Produce an implementation suitable for post-graduation final work evaluation.
2. Establish an architecture that can evolve toward broader federation support.
3. Create a foundation for later research in adaptive ingestion and resilient pipelines.

### 5.3 Non-Goals for Phase 1

- Cover all federal public datasets.
- Support state or municipal sources.
- Ingest private, restricted, or protected data.
- Build a full analytical warehouse across all domains.
- Perform deep semantic harmonization across unrelated public datasets.
- Make real-time streaming the primary ingestion mode.

---

## 6. Target Users and Stakeholders

### 6.1 Primary Users

- Data engineering students or researchers implementing and extending the framework.
- Technical contributors onboarding new public data sources.
- Operators running ingestion jobs in local or Spark environments.

### 6.2 Secondary Stakeholders

- Post-graduation advisors and academic evaluators.
- Future maintainers extending JANUS to new public-source families.
- Public-data practitioners who need a clean ingestion baseline for open government data.

---

## 7. User Needs

### 7.1 Data Engineer / Contributor

Needs to add a new source without copying an old script and editing random lines. Expects a clear contract for where configuration ends, strategy logic begins, and source-specific hooks are allowed.

### 7.2 Operator / Researcher

Needs consistent job execution with deterministic outputs, structured logs, checkpoints, and safe reruns across environments.

### 7.3 Academic Evaluator

Needs evidence that JANUS is not just functional, but architecturally coherent, measurable, reproducible, and defensible as a data engineering framework.

---

## 8. Scope

### 8.1 In Scope

- Federal Brazilian public/open data only.
- Clearly public and legally reusable sources.
- Initial support for 3 to 5 sources.
- Batch-oriented ingestion.
- Spark-based normalization and writing.
- Raw, bronze, and metadata output zones.
- YAML-driven source registry.
- Strategy-based execution model.
- Checkpointing, logging, validation, and run metadata.
- Reproducible execution in local, containerized, and cluster-compatible modes.

### 8.2 Out of Scope

- State and municipal public data.
- Non-public or protected systems.
- Circumvention of access controls, captchas, or anti-bot protections.
- Manual scraping of restricted portals.
- Full downstream semantic standardization across all datasets.
- Real-time streaming as a core requirement.

---

## 9. Initial Source Strategy

The first release must represent multiple ingestion patterns rather than multiple sources of the same kind.

### 9.1 Candidate Sources

- Portal da Transparencia
- dados.gov.br catalog API
- IBGE data services
- INEP microdata
- Ministry of Health / DATASUS open data

### 9.2 MVP Source Coverage Requirement

The MVP must include at least 3 sources and cover at least these 3 pattern types:

- one API-oriented source;
- one bulk file or package-based source;
- one catalog or metadata-oriented source.

### 9.3 Source Acceptance Criteria

A source may be onboarded only if it is:

- publicly accessible;
- legally reusable;
- documented enough for responsible access;
- relevant to framework evaluation;
- representative of a distinct ingestion pattern.

---

## 10. Core Product Principles

1. **Metadata-driven first, not metadata-only**  
   Configuration should describe declarative behavior, but custom logic belongs in explicit code modules when required.

2. **Generic core, modular execution**  
   Shared framework concerns belong in common abstractions; source-family behavior belongs in strategy modules.

3. **Extension by pattern, not by copy**  
   New capabilities should emerge through strategies, variants, and isolated hooks instead of cloned scripts.

4. **Idempotent and rerunnable behavior**  
   Repeated runs must behave predictably and support checkpoint recovery.

5. **Safety by default**  
   The framework must respect rate limits, public-access constraints, and secure handling of tokens and logs.

6. **Reproducibility as a product requirement**  
   Local and cluster-like execution should use pinned versions and environment-driven configuration.

7. **Simplicity over speculative complexity**  
   The architecture should remain clean and teachable while still being extensible.

---

## 11. Functional Requirements

### 11.1 Source Registry

The framework must provide a YAML-based source registry that defines, at minimum:

- source identity and ownership metadata;
- source type and strategy family;
- strategy variant;
- access details such as URL, method, file path, or discovery pattern;
- authentication mode when applicable;
- extraction mode and checkpoint semantics;
- output destinations, including optional `outputs.bronze.namespace` and `outputs.bronze.table_name` when bronze is written as Iceberg;
- quality and schema settings;
- operational settings such as rate limits and retries.

### 11.2 Planning Layer

The framework must include a planner that:

- reads source metadata;
- validates the configuration;
- selects the appropriate strategy module and variant;
- prepares an execution plan with predictable behavior.

### 11.3 Strategy Modules

The MVP must support at least these strategy families:

- API strategy;
- file/bulk download strategy;
- catalog/metadata strategy.

Each strategy should own the reusable behavior for its source family and expose a stable contract to the planner.

### 11.4 Strategy Variants

The system should support reusable sub-patterns such as:

- page-based pagination;
- offset-based pagination;
- cursor pagination when needed later;
- date-window extraction;
- static file download;
- versioned file download;
- archive extraction;
- metadata traversal patterns.

### 11.5 Source-Specific Hooks

The framework must allow small source-specific hooks for behavior that cannot be described through configuration and an existing strategy. Hooks must be:

- isolated by source;
- well named;
- minimal in scope;
- covered by tests when feasible.

### 11.6 Data Zones

JANUS must write outputs into explicit zones:

- `raw` for exact downloaded or extracted payloads;
- `bronze` for minimally standardized Spark-readable datasets;
- `metadata` for run logs, checkpoints, validation results, lineage, and source metadata.

### 11.7 Spark Processing Layer

The framework must use Spark for normalization and writing, with support for:

- schema-aware reads when possible;
- controlled partitioning;
- deterministic write behavior;
- append and overwrite semantics as configured;
- environment-driven Spark session setup.

### 11.8 Checkpointing and Reruns

The framework must support:

- explicit checkpoint definitions per source;
- rerun-safe behavior;
- recoverability after failures;
- deterministic output path conventions.

### 11.9 Quality and Validation

The framework must provide configurable validation support, including:

- required field checks;
- config validation before execution;
- validation result persistence in metadata outputs;
- explicit failure reporting.

### 11.10 Logging, Metrics, and Metadata

The framework must capture:

- structured logs;
- run identifiers;
- source and strategy metadata;
- execution timestamps;
- validation outcomes;
- ingestion metrics and failure reasons.

### 11.11 Execution Modes

The framework must run in:

- local mode using Spark local;
- containerized mode using Docker Compose or equivalent;
- cluster-compatible mode using a generic Spark cluster contract.

### 11.12 Documentation and Onboarding

The framework must include documentation for:

- architecture overview;
- source onboarding workflow;
- strategy patterns;
- reproducibility and setup steps.

---

## 12. Non-Functional Requirements

### 12.1 Reproducibility

- Python, Spark, and dependency versions must be pinned.
- Container definitions must be provided.
- Environment-specific assumptions must be externalized through configuration.

### 12.2 Maintainability

- The repository structure must remain modular and navigable.
- Source configs must remain outside core logic.
- Interfaces between planner, strategies, hooks, and writers must be explicit.

### 12.3 Scalability

- The design must support growth from a handful of sources to dozens without collapsing into condition-heavy code.

### 12.4 Portability

- Storage and execution behavior must not depend on one cloud vendor.
- Local filesystem and production-like storage contracts must be configurable.

### 12.5 Observability

- Failure reasons must be clear.
- Run metadata must be queryable from the metadata zone.

### 12.6 Testability

- Unit tests must cover configuration validation, reusable components, and core adapters.
- Integration tests must demonstrate at least one reproducible local execution path.

### 12.7 Safety

- Tokens and secrets must never be hardcoded.
- Logs must avoid leaking secrets, cookies, or risky identifiers.
- Access patterns must respect documented source constraints.

---

## 13. Safety and Responsible-Use Requirements

1. The framework must ingest only clearly public and legally reusable datasets.
2. The framework must not bypass authentication barriers or anti-bot protections.
3. Token-based public APIs must use secure secret handling outside source control.
4. Default behavior must favor bounded concurrency and retry with backoff.
5. Public microdata must be handled responsibly, without re-identification or harmful recombination goals.

---

## 14. High-Level Solution Design

The product architecture should follow this control flow:

`YAML -> planner -> strategy module -> optional source hook -> Spark normalization/write -> raw/bronze/metadata outputs`

### 14.1 Core Components

- Source registry
- Planner
- Strategy layer
- Strategy variants / reusable components
- Source-specific hooks
- Spark processing layer
- Checkpoint and lineage store
- Output zone writers

### 14.2 Expected Repository Shape

The implementation should preserve separation of concerns across:

- configuration;
- strategy modules;
- hooks;
- readers and writers;
- quality checks;
- checkpoints and lineage;
- tests and fixtures;
- documentation.

---

## 15. MVP Definition

The JANUS MVP is complete when it delivers:

1. A working framework skeleton with planner, registry, and strategy interfaces.
2. At least 3 integrated federal public sources.
3. At least 3 reusable strategy modules or clearly reusable strategy families in practice.
4. YAML-driven onboarding for each integrated source.
5. Raw, bronze, and metadata outputs.
6. Checkpointing and rerun-safe execution behavior.
7. Structured logging and run metadata capture.
8. Reproducible execution instructions for local and containerized modes, plus a cluster-compatible contract.
9. Basic validation checks and automated tests for core components.
10. Documentation explaining architecture, onboarding, and reproducibility.

---

## 16. Success Metrics

### 16.1 Delivery Metrics

- 3 to 5 federal public sources onboarded.
- At least 3 distinct ingestion patterns demonstrated.
- Local execution path documented and reproducible.
- Containerized execution path documented and reproducible.

### 16.2 Engineering Quality Metrics

- A new source can be added primarily through configuration plus an existing strategy when the pattern already exists.
- Rerunning the same ingestion does not create uncontrolled duplication.
- Failures produce traceable metadata and actionable logs.
- Source-specific logic remains isolated instead of spreading across the framework.

### 16.3 Academic Evaluation Dimensions

- reproducibility;
- extensibility;
- throughput;
- rerun safety;
- recoverability after failure;
- onboarding effort for a new source;
- architectural cleanliness;
- strategy reuse across sources.

---

## 17. Milestones

### Milestone 1: Foundation

- Define repository structure.
- Establish pinned runtime and dependency versions.
- Implement base configuration and source registry schema.
- Set up local and containerized development environments.

### Milestone 2: Core Framework

- Implement planner.
- Implement strategy interfaces and base components.
- Implement writers for raw, bronze, and metadata zones.
- Add structured logging, config validation, and checkpoint foundation.

### Milestone 3: Initial Strategy Modules

- Deliver API strategy.
- Deliver file/bulk strategy.
- Deliver catalog strategy.
- Add shared components such as pagination, retries, archive handling, and normalization hooks.

### Milestone 4: Source Integrations

- Integrate 3 to 5 approved federal public sources.
- Add minimal source-specific hooks only where required.
- Add schemas, validation rules, and example runs.

### Milestone 5: Evaluation and Documentation

- Run reproducibility demonstrations.
- Measure rerun behavior and failure recovery.
- Document architecture and onboarding.
- Prepare academic evaluation narrative and results.

---

## 18. Risks and Mitigations

### Risk 1: Strategy Over-Generalization

Trying to force all sources into one generic ingestion model may create fragile abstractions.

**Mitigation:** keep the core generic, but organize execution by strategy families and allow isolated hooks.

### Risk 2: Framework Sprawl

New sources may encourage copied scripts and ad hoc behavior.

**Mitigation:** require onboarding through config, standard strategy contracts, tests, and documented extension points.

### Risk 3: Environmental Drift

Differences between local and cluster environments may undermine reproducibility.

**Mitigation:** pin versions, provide container definitions, and externalize environment settings.

### Risk 4: Source Instability

Public endpoints may change structure, rate limits, or availability.

**Mitigation:** start with clearly documented sources, keep adapters small, and capture explicit failure metadata.

### Risk 5: Scope Creep

The project may drift toward solving every public-data problem at once.

**Mitigation:** enforce federal-only scope, batch-first delivery, and a 3 to 5 source MVP target.

---

## 19. Assumptions

1. Spark is the right execution engine for the intended data volumes and reproducibility goals.
2. The selected initial sources remain publicly accessible during the implementation period.
3. At least one source from each target pattern family will remain stable enough for MVP integration.
4. Local filesystem support is sufficient for development, while storage abstraction can support future distributed backends.
5. The first phase prioritizes architectural clarity over broad source count.

---

## 20. Acceptance Criteria

JANUS will be considered successful for Phase 1 if:

1. It ingests at least 3 approved federal public sources representing at least 3 distinct access patterns.
2. Each onboarded source is defined through YAML and mapped to a strategy family.
3. Strategy modules encapsulate reusable family logic, while custom logic remains isolated in source hooks.
4. Each run produces raw, bronze, and metadata outputs as applicable.
5. The framework supports checkpoint-aware reruns without uncontrolled duplication.
6. The project runs reproducibly in local mode and containerized mode, with a documented cluster-compatible setup contract.
7. Logging, validation, and run metadata make failures diagnosable.
8. Documentation explains architecture, onboarding, and reproducibility clearly enough for a new contributor to follow.

---

## 21. Deliverables

Phase 1 should produce:

- a working JANUS framework;
- source definitions for 3 to 5 initial federal sources;
- reusable strategy modules;
- reproducible setup and execution instructions;
- architecture and onboarding documentation;
- basic automated tests;
- an evaluation section or report describing framework behavior and results.

---

## 22. Research Alignment

The central research question for the post-graduation phase is:

**How can a metadata-driven and strategy-based Spark framework ingest heterogeneous federal Brazilian public data sources in a reproducible, safe, and scalable way?**

This PRD supports that question by defining product requirements that are both implementable and measurable in an academic setting.

---

## 23. Future Evolution

After the first phase, JANUS may evolve toward:

- state and municipal public-data support;
- broader source-type coverage;
- improved schema evolution handling;
- stronger lineage and provenance tracking;
- orchestration integration;
- adaptive strategy selection;
- benchmark and resiliency suites for heterogeneous ingestion.

---

## 24. Final Product Statement

JANUS is not intended to be a one-off data collection script set. It is a deliberate framework for safe, reproducible, and maintainable public-data ingestion, designed to prove that a metadata-driven and strategy-based architecture can scale more cleanly than source-specific script sprawl.

The first version succeeds if it stays focused, integrates a small but heterogeneous set of federal sources well, and demonstrates sound data engineering architecture that can support future academic and technical expansion.
