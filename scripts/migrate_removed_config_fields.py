"""Safely migrate retired, no-op production configuration fields.

The config loader intentionally rejects these names so stale settings cannot
look active.  Deployment invokes this tool before service restart: it removes
only fields rejected by the loader, writes a timestamped sibling backup, and
then re-runs the normal loader validation.
"""

from __future__ import annotations

import argparse
import os
import re
import shutil
import sys
from datetime import UTC, datetime
from pathlib import Path

from lightfee.config.loader import _REMOVED_PRODUCTION_FIELDS, load_config
from lightfee.core.errors import ConfigError


_SECTION_RE = re.compile(r"^\s*\[([^]]+)]\s*(?:#.*)?$")
_KEY_RE = re.compile(r"^\s*([A-Za-z0-9_-]+)\s*=")


def retired_fields_in_config(path: Path) -> list[tuple[str, str]]:
    """Return sorted retired ``(section, key)`` pairs present in *path*."""
    import tomllib

    with path.open("rb") as handle:
        raw = tomllib.load(handle)
    return [
        (section, field)
        for section, fields in _REMOVED_PRODUCTION_FIELDS.items()
        if isinstance(raw.get(section), dict)
        for field in sorted(fields.intersection(raw[section]))
    ]


def remove_retired_field_lines(text: str, fields: set[tuple[str, str]]) -> str:
    """Remove exact scalar assignments for known retired fields only."""
    current_section = ""
    kept: list[str] = []
    for line in text.splitlines(keepends=True):
        section_match = _SECTION_RE.match(line)
        if section_match:
            current_section = section_match.group(1)
            kept.append(line)
            continue
        key_match = _KEY_RE.match(line)
        if key_match and (current_section, key_match.group(1)) in fields:
            continue
        kept.append(line)
    return "".join(kept)


def backup_and_apply(path: Path, fields: list[tuple[str, str]]) -> Path:
    """Atomically remove *fields* after making a same-directory backup."""
    original = path.read_text(encoding="utf-8")
    migrated = remove_retired_field_lines(original, set(fields))
    if migrated == original:
        raise ValueError("retired config fields were parsed but no assignment lines were found")

    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    backup = path.with_name(f"{path.name}.removed-fields-{stamp}.bak")
    suffix = 1
    while backup.exists():
        backup = path.with_name(f"{path.name}.removed-fields-{stamp}-{suffix}.bak")
        suffix += 1
    shutil.copy2(path, backup)

    mode = path.stat().st_mode
    temporary = path.with_name(f".{path.name}.removed-fields.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(migrated)
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return backup


def restore_backup(path: Path, backup: Path) -> None:
    """Atomically restore *path* from a retained migration backup."""
    mode = path.stat().st_mode
    temporary = path.with_name(f".{path.name}.removed-fields.restore.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            handle.write(backup.read_text(encoding="utf-8"))
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="back up and remove retired fields before validating the config",
    )
    args = parser.parse_args(argv)
    config_path = args.config.resolve()

    try:
        fields = retired_fields_in_config(config_path)
    except (OSError, ValueError) as error:
        print(f"config migration read failed: {error}", file=sys.stderr)
        return 1

    if fields and not args.apply:
        names = ", ".join(f"{section}.{field}" for section, field in fields)
        print(f"config migration required: {names}", file=sys.stderr)
        return 1

    if fields:
        try:
            backup = backup_and_apply(config_path, fields)
        except (OSError, ValueError) as error:
            print(f"config migration apply failed: {error}", file=sys.stderr)
            return 1
        names = ", ".join(f"{section}.{field}" for section, field in fields)
        print(f"migrated retired config fields: {names}")
        print(f"config backup: {backup}")
    else:
        print("no retired config fields found")

    try:
        load_config(config_path)
    except ConfigError as error:
        if fields:
            try:
                restore_backup(config_path, backup)
            except OSError as restore_error:
                print(
                    f"config migration rollback failed; restore manually from {backup}: "
                    f"{restore_error}",
                    file=sys.stderr,
                )
            else:
                print(f"config migration rolled back from: {backup}", file=sys.stderr)
        print(f"config validation failed after migration: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
