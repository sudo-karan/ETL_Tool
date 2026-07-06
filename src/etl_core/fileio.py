"""File formats + a filesystem-access policy for file_source / file_sink.

Format contract:

* ``json`` / ``jsonl`` preserve the nested-JSON record contract exactly
  (records may contain nested objects and arrays); read/written with the
  standard library.
* ``csv`` / ``parquet`` are tabular (polars / pyarrow). Scalar values keep
  their column types on round-trip; nested values (objects/arrays) are
  JSON-encoded to a string cell on write. Use json/jsonl when you need to
  round-trip nesting.

Security. On a multi-user server, file nodes are a local-file read/write
primitive, so a :class:`FileAccessPolicy` can confine every path to a set of
allowed base directories (the server sets this per deployment/user). The
headless/dev default is unrestricted, mirroring how the SSRF guard is opt-in
per host -- except files default open because a local CLI legitimately reads
arbitrary local paths. Set ``allowed_dirs`` to lock it down.
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, Field

Records = list[dict[str, Any]]

FileFormat = Literal["csv", "json", "jsonl", "parquet"]
_EXTENSIONS: dict[str, FileFormat] = {
    ".csv": "csv",
    ".json": "json",
    ".jsonl": "jsonl",
    ".ndjson": "jsonl",
    ".parquet": "parquet",
    ".pq": "parquet",
}


class FileAccessError(Exception):
    """A path was denied by policy, or could not be read/written."""


class FileFormatError(Exception):
    """A file could not be parsed as its declared format."""


class FileAccessPolicy(BaseModel):
    """Confines file paths to ``allowed_dirs`` (empty = unrestricted).

    Entries are directories; a target path is allowed when its real,
    symlink-resolved location is inside one of them. The parent directory is
    resolved for not-yet-existing sink files so a new file in an allowed dir
    is permitted.
    """

    allowed_dirs: list[str] = Field(default_factory=list)

    def resolve(self, path: str, *, for_write: bool = False) -> Path:
        target = Path(path).expanduser()
        # Resolve symlinks/.. as far as the path exists; for a new write
        # target, resolve the nearest existing ancestor (usually the parent).
        resolved = self._realpath(target)
        if not self.allowed_dirs:
            return resolved
        for base in self.allowed_dirs:
            base_resolved = Path(base).expanduser().resolve()
            if resolved == base_resolved or resolved.is_relative_to(base_resolved):
                return resolved
        raise FileAccessError(
            f"path {path!r} is outside the allowed directories "
            f"({', '.join(self.allowed_dirs)}); adjust the file access policy if intentional"
        )

    @staticmethod
    def _realpath(target: Path) -> Path:
        existing = target
        while not existing.exists() and existing != existing.parent:
            existing = existing.parent
        # Real-resolve the existing ancestor, then re-append the missing tail.
        anchor = existing.resolve()
        try:
            tail = target.relative_to(existing)
        except ValueError:
            return anchor
        return (anchor / tail).resolve() if str(tail) != "." else anchor


def infer_format(path: str) -> FileFormat:
    suffix = Path(path).suffix.lower()
    fmt = _EXTENSIONS.get(suffix)
    if fmt is None:
        raise FileFormatError(
            f"cannot infer format from path {path!r}; set 'format' explicitly "
            f"(one of csv, json, jsonl, parquet)"
        )
    return fmt


# --------------------------------------------------------------------------
# Reading
# --------------------------------------------------------------------------
def read_records(
    path: Path,
    fmt: FileFormat,
    *,
    limit: int | None = None,
    has_header: bool = True,
    delimiter: str = ",",
    infer_schema: bool = True,
    records_path: str | None = None,
) -> Records:
    if not path.exists():
        raise FileAccessError(f"file not found: {path}")
    if fmt == "json":
        return _read_json(path, records_path, limit)
    if fmt == "jsonl":
        return _read_jsonl(path, limit)
    if fmt == "csv":
        import polars as pl

        frame = pl.read_csv(
            path,
            has_header=has_header,
            separator=delimiter,
            infer_schema_length=None if infer_schema else 0,
            n_rows=limit,
        )
        return frame.to_dicts()
    # parquet
    import polars as pl

    frame = pl.read_parquet(path, n_rows=limit)
    return frame.to_dicts()


def _coerce_item(item: Any) -> dict[str, Any]:
    return item if isinstance(item, dict) else {"value": item}


def _read_json(path: Path, records_path: str | None, limit: int | None) -> Records:
    from .errors import PathNotFoundError
    from .paths import get_path

    try:
        data = json.loads(path.read_text())
    except ValueError as exc:
        raise FileFormatError(f"{path} is not valid JSON: {exc}") from exc
    if records_path:
        try:
            data = get_path(data, records_path)
        except PathNotFoundError as exc:
            raise FileFormatError(f"records_path {records_path!r} not found in {path}: {exc}") from exc
    items = data if isinstance(data, list) else [data]
    records = [_coerce_item(item) for item in items]
    return records[:limit] if limit is not None else records


def _read_jsonl(path: Path, limit: int | None) -> Records:
    records: Records = []
    with path.open() as handle:
        for line_no, line in enumerate(handle, 1):
            line = line.strip()
            if not line:
                continue
            try:
                records.append(_coerce_item(json.loads(line)))
            except ValueError as exc:
                raise FileFormatError(f"{path} line {line_no} is not valid JSON: {exc}") from exc
            if limit is not None and len(records) >= limit:
                break
    return records


# --------------------------------------------------------------------------
# Writing
# --------------------------------------------------------------------------
def write_records(
    records: Records,
    path: Path,
    fmt: FileFormat,
    *,
    mode: Literal["overwrite", "append", "error"] = "overwrite",
    make_parents: bool = True,
    has_header: bool = True,
    delimiter: str = ",",
    json_indent: int | None = 2,
) -> int:
    if mode == "error" and path.exists():
        raise FileAccessError(f"refusing to overwrite existing file: {path}")
    if make_parents:
        path.parent.mkdir(parents=True, exist_ok=True)

    combined = records
    if mode == "append" and path.exists():
        existing = read_records(path, fmt)
        combined = existing + records

    if fmt == "jsonl":
        _write_jsonl(combined, path)
    elif fmt == "json":
        path.write_text(json.dumps(combined, indent=json_indent, default=str))
    elif fmt == "csv":
        import polars as pl

        _frame(pl, combined).write_csv(path, separator=delimiter, include_header=has_header)
    else:  # parquet
        import polars as pl

        _frame(pl, combined).write_parquet(path)
    return len(records)


def _write_jsonl(records: Records, path: Path) -> None:
    with path.open("w") as handle:
        for record in records:
            handle.write(json.dumps(record, default=str))
            handle.write("\n")


def _frame(pl: Any, records: Records) -> Any:
    """Build a polars DataFrame for csv/parquet: union of keys, nested values
    JSON-encoded, mixed-type columns coerced instead of raising."""
    if not records:
        return pl.DataFrame([])
    columns: list[str] = []
    seen: set[str] = set()
    for record in records:
        for key in record:
            if key not in seen:
                seen.add(key)
                columns.append(key)
    rows = []
    for record in records:
        row = {}
        for key in columns:
            value = record.get(key, None)
            if isinstance(value, (dict, list)):
                value = json.dumps(value, default=str)
            row[key] = value
        rows.append(row)
    return pl.DataFrame(rows, strict=False)
