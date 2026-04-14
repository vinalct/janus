from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from janus.models import IcebergRowsRequestInputsConfig, ParameterBinding

REQUEST_INPUT_BINDING_PREFIX = "request_input."
_SUPPORTED_STRFTIME_DIRECTIVES = frozenset(
    {
        "%",
        "A",
        "B",
        "C",
        "D",
        "F",
        "G",
        "H",
        "I",
        "M",
        "P",
        "R",
        "S",
        "T",
        "U",
        "V",
        "W",
        "X",
        "Y",
        "Z",
        "a",
        "b",
        "c",
        "d",
        "f",
        "g",
        "h",
        "j",
        "m",
        "p",
        "r",
        "u",
        "w",
        "x",
        "y",
        "z",
    }
)
_SUPPORTED_STRFTIME_MODIFIERS = frozenset({"#", "-", "0", "^", "_"})


class ApiRequestInputError(RuntimeError):
    """Base error for request-input loading and parameter binding resolution."""


class ApiRequestInputLoadError(ApiRequestInputError):
    """Raised when a configured upstream request-input source cannot be loaded safely."""


class ApiParameterBindingError(ApiRequestInputError):
    """Raised when declarative request-parameter bindings cannot be resolved safely."""


def validate_iceberg_request_input_source(
    request_inputs: IcebergRowsRequestInputsConfig,
    *,
    available_columns: Sequence[str] | None,
) -> None:
    """Validate that a configured Iceberg request-input source exists and exposes columns."""
    table_identifier = f"{request_inputs.namespace}.{request_inputs.table_name}"
    if available_columns is None:
        raise ApiRequestInputLoadError(
            f"access.request_inputs: configured Iceberg table {table_identifier!r} does not exist"
        )

    normalized_columns = {
        normalized_column
        for column in available_columns
        if (normalized_column := str(column).strip())
    }
    for binding_name, source_column in sorted(request_inputs.columns.items()):
        if source_column not in normalized_columns:
            message = (
                f"access.request_inputs.columns.{binding_name}: source column "
                f"{source_column!r} was not found in Iceberg table {table_identifier!r}"
            )
            raise ApiRequestInputLoadError(message)


def resolve_parameter_bindings(
    parameter_bindings: Mapping[str, ParameterBinding] | None,
    *,
    request_input: Mapping[str, Any] | None = None,
    checkpoint_value: str | None = None,
) -> dict[str, str]:
    """Resolve runtime request parameters from the current request-input context."""
    if not parameter_bindings:
        return {}

    resolved_bindings: dict[str, str] = {}
    for parameter_name, binding in parameter_bindings.items():
        binding_path = f"access.parameter_bindings.{parameter_name}"
        value = _resolve_binding_value(
            binding.from_,
            request_input=request_input,
            checkpoint_value=checkpoint_value,
            binding_path=binding_path,
        )
        resolved_bindings[parameter_name] = _render_bound_value(
            value,
            output_format=binding.format,
            binding_path=binding_path,
        )
    return resolved_bindings


def merge_request_params(
    static_params: Mapping[str, str] | None,
    bound_params: Mapping[str, str] | None,
) -> dict[str, str]:
    """Merge static and runtime-bound params while rejecting ambiguous overlaps."""
    static_mapping = dict(static_params or {})
    bound_mapping = dict(bound_params or {})
    duplicate_keys = sorted(set(static_mapping).intersection(bound_mapping))
    if duplicate_keys:
        duplicates = ", ".join(duplicate_keys)
        message = (
            "access.params and access.parameter_bindings must not define the same "
            f"request parameter(s): {duplicates}"
        )
        raise ApiParameterBindingError(message)

    merged = dict(static_mapping)
    merged.update(bound_mapping)
    return merged


def _resolve_binding_value(
    binding_source: str,
    *,
    request_input: Mapping[str, Any] | None,
    checkpoint_value: str | None,
    binding_path: str,
) -> Any:
    if binding_source == "checkpoint_value":
        if checkpoint_value is None or not checkpoint_value.strip():
            raise ApiParameterBindingError(
                f"{binding_path}.from: checkpoint_value is not available for this run"
            )
        return checkpoint_value

    field_name = binding_source.removeprefix(REQUEST_INPUT_BINDING_PREFIX)
    if request_input is None:
        raise ApiParameterBindingError(
            f"{binding_path}.from: request_input context is not available for this binding"
        )
    if field_name not in request_input:
        raise ApiParameterBindingError(
            f"{binding_path}.from: {binding_source} is not available in the current request input"
        )
    return request_input[field_name]


def _render_bound_value(
    value: Any,
    *,
    output_format: str | None,
    binding_path: str,
) -> str:
    if output_format is not None:
        _validate_strftime_format(output_format, field_path=f"{binding_path}.format")
        if not isinstance(value, date | datetime):
            message = (
                f"{binding_path}.format: requires a date or datetime value, got "
                f"{type(value).__name__}"
            )
            raise ApiParameterBindingError(message)
        rendered_value = value.strftime(output_format)
        if not rendered_value:
            raise ApiParameterBindingError(
                f"{binding_path}.format: resolved to an empty string"
            )
        return rendered_value

    if value is None:
        raise ApiParameterBindingError(f"{binding_path}.from: resolved to null")
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bool):
        return str(value).lower()
    if isinstance(value, Decimal | float | int):
        return str(value)
    if isinstance(value, str):
        normalized_value = value.strip()
        if not normalized_value:
            raise ApiParameterBindingError(
                f"{binding_path}.from: resolved to an empty string"
            )
        return normalized_value

    message = (
        f"{binding_path}.from: resolved to unsupported type "
        f"{type(value).__name__}; expected a scalar, date, or datetime"
    )
    raise ApiParameterBindingError(message)


def _validate_strftime_format(format_string: str, *, field_path: str) -> None:
    index = 0
    while index < len(format_string):
        if format_string[index] != "%":
            index += 1
            continue

        index += 1
        if index >= len(format_string):
            raise ApiParameterBindingError(
                f"{field_path}: contains an incomplete strftime directive"
            )

        while index < len(format_string) and format_string[index] in _SUPPORTED_STRFTIME_MODIFIERS:
            index += 1
        while index < len(format_string) and format_string[index].isdigit():
            index += 1

        if index >= len(format_string):
            raise ApiParameterBindingError(
                f"{field_path}: contains an incomplete strftime directive"
            )

        directive = format_string[index]
        if directive not in _SUPPORTED_STRFTIME_DIRECTIVES:
            raise ApiParameterBindingError(
                f"{field_path}: contains unsupported strftime directive '%{directive}'"
            )
        index += 1
