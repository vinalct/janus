from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote, urlsplit

ARCHIVE_FILE_SUFFIXES = frozenset({".zip"})

DIRECT_FILE_EXTENSIONS = frozenset({
    ".csv", ".json", ".jsonl", ".ndjson", ".parquet",
    ".zip", ".tar.gz", ".tgz", ".txt", ".tsv", ".xlsx", ".xls", ".pdf", ".xml",
})

SUPPORTED_FILE_INPUT_FORMATS = frozenset({"binary", "csv", "json", "jsonl", "parquet", "text"})

SANITIZE_SEGMENT_PATTERN = re.compile(r"[^A-Za-z0-9._-]+")

_CONTENT_DISPOSITION_FILENAME_RE = re.compile(
    r"""filename\*?=(?:UTF-8''|")?(?P<filename>[^";]+)"""
)


def _is_tarball_filename(filename: str) -> bool:
    lower = filename.lower()
    return lower.endswith(".tar.gz") or lower.endswith(".tgz")


def _filename_from_url(url: str) -> str:
    path = urlsplit(url).path
    candidate = Path(unquote(path)).name.strip()
    return candidate or "download.bin"


def _filename_from_content_disposition(header: str) -> str | None:
    match = _CONTENT_DISPOSITION_FILENAME_RE.search(header)
    if not match:
        return None
    name = unquote(match.group("filename").strip())
    return Path(name).name.strip() or None


def _infer_format_name(value: str, *, fallback: str) -> str:
    if _is_tarball_filename(value):
        return "binary"
    suffix = Path(value).suffix.lower()
    if suffix == ".csv":
        return "csv"
    if suffix == ".json":
        return "json"
    if suffix in {".jsonl", ".ndjson"}:
        return "jsonl"
    if suffix == ".parquet":
        return "parquet"
    if suffix in {".txt", ".tsv"}:
        return "text"
    if suffix in {".zip", ".xlsx", ".xls"}:
        return "binary"
    normalized_fallback = fallback.strip().lower()
    if normalized_fallback in SUPPORTED_FILE_INPUT_FORMATS:
        return normalized_fallback
    return "binary"


def _safe_path_segment(value: str) -> str:
    normalized = SANITIZE_SEGMENT_PATTERN.sub("-", value.strip()).strip("-.")
    return normalized or "current"
