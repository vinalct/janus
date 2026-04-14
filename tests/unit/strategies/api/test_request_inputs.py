from datetime import date

import pytest

from janus.models import (
    DateWindowRequestInputsConfig,
    IcebergRowsRequestInputsConfig,
    ParameterBinding,
    RequestInputsConfig,
)
from janus.strategies.api.request_inputs import (
    ApiParameterBindingError,
    ApiRequestInputLoadError,
    load_request_inputs,
    merge_request_params,
    resolve_parameter_bindings,
    validate_iceberg_request_input_source,
)


def test_load_request_inputs_preserves_one_stream_behavior_for_none():
    assert load_request_inputs(RequestInputsConfig(type="none")) == (None,)


def test_load_request_inputs_generates_daily_windows():
    request_inputs = DateWindowRequestInputsConfig(
        type="date_window",
        start=date(2025, 1, 1),
        end=date(2025, 1, 3),
        step="day",
    )

    assert load_request_inputs(request_inputs) == (
        {
            "window_start": date(2025, 1, 1),
            "window_end": date(2025, 1, 1),
        },
        {
            "window_start": date(2025, 1, 2),
            "window_end": date(2025, 1, 2),
        },
        {
            "window_start": date(2025, 1, 3),
            "window_end": date(2025, 1, 3),
        },
    )


def test_load_request_inputs_generates_monthly_windows_bounded_by_interval():
    request_inputs = DateWindowRequestInputsConfig(
        type="date_window",
        start=date(2025, 1, 15),
        end=date(2025, 3, 10),
        step="month",
    )

    assert load_request_inputs(request_inputs) == (
        {
            "window_start": date(2025, 1, 15),
            "window_end": date(2025, 1, 31),
        },
        {
            "window_start": date(2025, 2, 1),
            "window_end": date(2025, 2, 28),
        },
        {
            "window_start": date(2025, 3, 1),
            "window_end": date(2025, 3, 10),
        },
    )


def test_resolve_parameter_bindings_formats_generated_date_window_values():
    bindings = {
        "dataIdaDe": ParameterBinding(
            from_="request_input.window_start",
            format="%Y-%m-%d",
        ),
        "mesAno": ParameterBinding(
            from_="request_input.window_end",
            format="%Y%m",
        ),
    }
    request_input = load_request_inputs(
        DateWindowRequestInputsConfig(
            type="date_window",
            start=date(2025, 1, 1),
            end=date(2025, 1, 31),
            step="month",
        )
    )[0]

    resolved = resolve_parameter_bindings(bindings, request_input=request_input)

    assert resolved == {
        "dataIdaDe": "2025-01-01",
        "mesAno": "202501",
    }


def test_resolve_parameter_bindings_rejects_missing_request_input_fields():
    with pytest.raises(ApiParameterBindingError) as exc_info:
        resolve_parameter_bindings(
            {"id": ParameterBinding(from_="request_input.emenda_id")},
            request_input={"other_id": "123"},
        )

    assert str(exc_info.value) == (
        "access.parameter_bindings.id.from: request_input.emenda_id is not available "
        "in the current request input"
    )


def test_resolve_parameter_bindings_rejects_invalid_date_formats():
    with pytest.raises(ApiParameterBindingError) as exc_info:
        resolve_parameter_bindings(
            {
                "mesAno": ParameterBinding(
                    from_="request_input.window_end",
                    format="%Q",
                )
            },
            request_input={"window_end": date(2025, 1, 31)},
        )

    assert str(exc_info.value) == (
        "access.parameter_bindings.mesAno.format: contains unsupported strftime "
        "directive '%Q'"
    )


def test_resolve_parameter_bindings_rejects_formatted_non_date_values():
    with pytest.raises(ApiParameterBindingError) as exc_info:
        resolve_parameter_bindings(
            {
                "mesAno": ParameterBinding(
                    from_="request_input.window_end",
                    format="%Y%m",
                )
            },
            request_input={"window_end": "2025-01-31"},
        )

    assert str(exc_info.value) == (
        "access.parameter_bindings.mesAno.format: requires a date or datetime value, "
        "got str"
    )


def test_resolve_parameter_bindings_rejects_non_scalar_values():
    with pytest.raises(ApiParameterBindingError) as exc_info:
        resolve_parameter_bindings(
            {"id": ParameterBinding(from_="request_input.emenda_id")},
            request_input={"emenda_id": {"id": "123"}},
        )

    assert str(exc_info.value) == (
        "access.parameter_bindings.id.from: resolved to unsupported type dict; "
        "expected a scalar, date, or datetime"
    )


def test_merge_request_params_rejects_duplicate_keys():
    with pytest.raises(ApiParameterBindingError) as exc_info:
        merge_request_params({"id": "fixed"}, {"id": "runtime"})

    assert str(exc_info.value) == (
        "access.params and access.parameter_bindings must not define the same "
        "request parameter(s): id"
    )


def test_validate_iceberg_request_input_source_rejects_missing_tables():
    request_inputs = IcebergRowsRequestInputsConfig(
        type="iceberg_rows",
        namespace="bronze_transparencia",
        table_name="emendas_parlamentares__emendas",
        columns={"emenda_id": "id"},
    )

    with pytest.raises(ApiRequestInputLoadError) as exc_info:
        validate_iceberg_request_input_source(request_inputs, available_columns=None)

    assert str(exc_info.value) == (
        "access.request_inputs: configured Iceberg table "
        "'bronze_transparencia.emendas_parlamentares__emendas' does not exist"
    )


def test_validate_iceberg_request_input_source_rejects_missing_columns():
    request_inputs = IcebergRowsRequestInputsConfig(
        type="iceberg_rows",
        namespace="bronze_transparencia",
        table_name="emendas_parlamentares__emendas",
        columns={"emenda_id": "id"},
    )

    with pytest.raises(ApiRequestInputLoadError) as exc_info:
        validate_iceberg_request_input_source(
            request_inputs,
            available_columns=("codigo",),
        )

    assert str(exc_info.value) == (
        "access.request_inputs.columns.emenda_id: source column 'id' was not found "
        "in Iceberg table 'bronze_transparencia.emendas_parlamentares__emendas'"
    )
