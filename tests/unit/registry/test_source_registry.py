from pathlib import Path

import pytest

from janus.models.source_config import SourceConfig, SourceConfigValidationError
from janus.registry import SourceNotFoundError, load_registry


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def test_checked_in_registry_lists_enabled_sources():
    registry = load_registry(PROJECT_ROOT)

    enabled_source_ids = [source.source_id for source in registry.list_sources()]

    assert enabled_source_ids == ["federal_open_data_example"]


def test_checked_in_registry_returns_typed_source_config():
    registry = load_registry(PROJECT_ROOT)

    source = registry.get_source("federal_open_data_example")

    assert isinstance(source, SourceConfig)
    assert source.strategy == "api"
    assert source.strategy_variant == "page_number_api"
    assert source.access.auth.type == "header_token"
    assert source.outputs.bronze.path == "data/bronze/example/federal_open_data_example"
    assert source.quality.required_fields == ("id", "updated_at")


def test_registry_excludes_disabled_sources_by_default(tmp_path):
    project_root = _create_project(
        tmp_path,
        {
            "enabled.yaml": _valid_source_yaml("enabled_source", enabled=True),
            "disabled.yaml": _valid_source_yaml("disabled_source", enabled=False),
        },
    )

    registry = load_registry(project_root)

    assert [source.source_id for source in registry.list_sources()] == ["enabled_source"]
    assert [source.source_id for source in registry.list_sources(enabled_only=False)] == [
        "disabled_source",
        "enabled_source",
    ]
    with pytest.raises(SourceNotFoundError, match="disabled"):
        registry.get_source("disabled_source")


def test_invalid_source_yaml_has_actionable_validation_errors(tmp_path):
    project_root = _create_project(
        tmp_path,
        {
            "broken.yaml": """
source_id: broken_source
name: Broken Source
owner: janus
enabled: true
source_type: api
strategy: api
strategy_variant: not_real
federation_level: federal
domain: example
public_access: true

access:
  base_url: https://example.invalid
  method: GET
  format: json
  auth:
    type: header_token
  pagination:
    type: page_number
  rate_limit:
    requests_per_minute: 10

extraction:
  mode: incremental
  retry:
    max_attempts: 3
    backoff_strategy: fixed
    backoff_seconds: 1

schema:
  mode: explicit

spark:
  input_format: json
  write_mode: append

outputs:
  raw:
    format: json
  bronze:
    path: data/bronze/example/broken
    format: parquet
  metadata:
    path: data/metadata/example/broken
    format: json

quality:
  allow_schema_evolution: true
""",
        },
    )

    with pytest.raises(SourceConfigValidationError) as exc_info:
        load_registry(project_root)

    message = str(exc_info.value)
    assert "broken.yaml" in message
    assert "strategy_variant: must be one of" in message
    assert "access.auth.env_var: is required for token-based auth" in message
    assert (
        "access.auth.header_name: is required when access.auth.type is "
        "'header_token'" in message
    )
    assert (
        "access.pagination.page_param: is required when access.pagination.type "
        "is 'page_number'" in message
    )
    assert (
        "extraction.checkpoint_field: is required when extraction.mode is "
        "'incremental'" in message
    )
    assert "schema.path: is required when schema.mode is 'explicit'" in message
    assert "outputs.raw.path: is required" in message


def test_duplicate_source_ids_fail_fast(tmp_path):
    project_root = _create_project(
        tmp_path,
        {
            "first.yaml": _valid_source_yaml("duplicate_source", enabled=True),
            "second.yaml": _valid_source_yaml("duplicate_source", enabled=True),
        },
    )

    with pytest.raises(ValueError, match="Duplicate source_id 'duplicate_source'"):
        load_registry(project_root)


def _create_project(tmp_path: Path, sources: dict[str, str]) -> Path:
    conf_dir = tmp_path / "conf"
    sources_dir = conf_dir / "sources"
    sources_dir.mkdir(parents=True)
    (conf_dir / "app.yaml").write_text(
        "registry:\n  sources_dir: conf/sources\n  file_pattern: \"*.yaml\"\n",
        encoding="utf-8",
    )
    for file_name, content in sources.items():
        (sources_dir / file_name).write_text(content.lstrip(), encoding="utf-8")
    return tmp_path


def _valid_source_yaml(source_id: str, *, enabled: bool) -> str:
    enabled_value = "true" if enabled else "false"
    return f"""
source_id: {source_id}
name: {source_id}
owner: janus
enabled: {enabled_value}
source_type: api
strategy: api
strategy_variant: page_number_api
federation_level: federal
domain: example
public_access: true

access:
  base_url: https://example.invalid
  path: /records
  method: GET
  format: json
  timeout_seconds: 30
  auth:
    type: none
  pagination:
    type: page_number
    page_param: page
    size_param: page_size
    page_size: 100
  rate_limit:
    requests_per_minute: 10
    concurrency: 1

extraction:
  mode: full_refresh
  retry:
    max_attempts: 3
    backoff_strategy: fixed
    backoff_seconds: 1

schema:
  mode: infer

spark:
  input_format: json
  write_mode: append

outputs:
  raw:
    path: data/raw/example/{source_id}
    format: json
  bronze:
    path: data/bronze/example/{source_id}
    format: parquet
  metadata:
    path: data/metadata/example/{source_id}
    format: json

quality:
  allow_schema_evolution: true
"""
