import json
from io import StringIO
from urllib.parse import parse_qs, urlsplit

from janus.utils.logging import (
    REDACTED_VALUE,
    build_structured_logger,
    redact_url,
    sanitize_log_payload,
)


def test_sanitize_log_payload_redacts_tokens_headers_and_cookies():
    payload = {
        "headers": {
            "Authorization": "Bearer super-secret-token",
            "Accept": "application/json",
            "X-API-Token": "abc123",
        },
        "cookie": "sessionid=secret",
        "url": "https://dados.gov.br/api?token=abc123&page=1",
        "nested": {"access_token": "secret", "items": [{"session_id": "xyz"}]},
    }

    sanitized = sanitize_log_payload(payload)

    assert sanitized["headers"]["Authorization"] == REDACTED_VALUE
    assert sanitized["headers"]["Accept"] == "application/json"
    assert sanitized["headers"]["X-API-Token"] == REDACTED_VALUE
    assert sanitized["cookie"] == REDACTED_VALUE
    assert sanitized["nested"]["access_token"] == REDACTED_VALUE
    assert sanitized["nested"]["items"][0]["session_id"] == REDACTED_VALUE

    query = parse_qs(urlsplit(sanitized["url"]).query)
    assert query["token"] == [REDACTED_VALUE]
    assert query["page"] == ["1"]


def test_structured_logger_emits_redacted_json_lines():
    stream = StringIO()
    logger = build_structured_logger("janus.tests.logging", stream=stream)

    logger.bind(run_id="run-logging-001", source_id="source-a").info(
        "request_sent",
        headers={"Authorization": "Bearer abc123"},
        url="https://api.example.gov/data?api_key=secret&page=2",
    )

    payload = json.loads(stream.getvalue().strip())

    assert payload["event"] == "request_sent"
    assert payload["fields"]["run_id"] == "run-logging-001"
    assert payload["fields"]["source_id"] == "source-a"
    assert payload["fields"]["headers"]["Authorization"] == REDACTED_VALUE
    query = parse_qs(urlsplit(payload["fields"]["url"]).query)
    assert query["api_key"] == [REDACTED_VALUE]
    assert query["page"] == ["2"]


def test_redact_url_masks_nextcloud_share_tokens_in_paths():
    share_url = "https://example.gov.br/index.php/s/MYTOKEN99?download=1"
    dav_url = "https://example.gov.br/public.php/dav/files/MYTOKEN99/data.csv"

    assert "MYTOKEN99" not in redact_url(share_url)
    assert "MYTOKEN99" not in redact_url(dav_url)
    assert REDACTED_VALUE in redact_url(share_url)
    assert REDACTED_VALUE in redact_url(dav_url)
