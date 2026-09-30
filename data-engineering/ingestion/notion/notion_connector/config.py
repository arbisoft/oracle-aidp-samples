# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
"""Load and validate the connector configuration.

The configuration never holds the Notion token. It names where the notebook
should read the token from (an AIDP Credential Store entry, or an environment
variable for local runs).
"""

from __future__ import annotations

import datetime
import re
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Tuple

# Run order: users and data sources first, blocks last (blocks depend on pages).
SUPPORTED_OBJECTS: Tuple[str, ...] = ("users", "data_sources", "pages", "blocks")
MODES: Tuple[str, ...] = ("cdc", "full")
DEFAULT_API_VERSION = "2026-03-11"

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class ConfigError(ValueError):
    """The configuration is missing something or holds an invalid value."""


@dataclass(frozen=True)
class NotionSettings:
    credential_name: Optional[str] = None
    credential_key: str = "secret"
    token_env: str = "NOTION_TOKEN"
    api_version: str = DEFAULT_API_VERSION
    requests_per_second: float = 3.0


@dataclass(frozen=True)
class TargetSettings:
    catalog: str
    schema: str
    table_prefix: str = ""

    @property
    def qualified_schema(self) -> str:
        return f"{self.catalog}.{self.schema}"

    def table(self, name: str) -> str:
        return f"{self.catalog}.{self.schema}.{self.table_prefix}{name}"


@dataclass(frozen=True)
class SyncSettings:
    objects: Tuple[str, ...]
    mode: str = "cdc"
    overlap_seconds: int = 120


@dataclass(frozen=True)
class Config:
    notion: NotionSettings
    target: TargetSettings
    sync: SyncSettings


def normalize_mode(value: Any) -> str:
    mode = str(value).strip().lower()
    if mode not in MODES:
        raise ConfigError(f"mode must be one of {', '.join(MODES)}; got {value!r}")
    return mode


def load_config(path: str) -> Config:
    import yaml

    with open(path, "r", encoding="utf-8") as handle:
        return parse_config(yaml.safe_load(handle))


def parse_config(data: Any) -> Config:
    if not isinstance(data, Mapping):
        raise ConfigError("configuration must be a mapping with 'target' and 'sync' sections")
    unknown = sorted(set(data) - {"notion", "target", "sync"})
    if unknown:
        raise ConfigError(f"unknown section(s): {', '.join(map(str, unknown))}")
    notion = _section(
        data,
        "notion",
        ("credential_name", "credential_key", "token_env", "api_version", "requests_per_second"),
        required=False,
    )
    target = _section(data, "target", ("catalog", "schema", "table_prefix"))
    sync = _section(data, "sync", ("mode", "objects", "overlap_seconds"))
    return Config(notion=_notion(notion), target=_target(target), sync=_sync(sync))


def _section(data: Mapping, name: str, allowed: Tuple[str, ...], required: bool = True) -> Mapping:
    value = data.get(name)
    if value is None:
        if required:
            raise ConfigError(f"missing section '{name}'")
        return {}
    if not isinstance(value, Mapping):
        raise ConfigError(f"'{name}' must be a mapping")
    unknown = sorted(set(value) - set(allowed))
    if unknown:
        raise ConfigError(f"unknown key(s) in '{name}': {', '.join(map(str, unknown))}")
    return value


def _notion(section: Mapping) -> NotionSettings:
    api_version = section.get("api_version") or DEFAULT_API_VERSION
    if isinstance(api_version, (datetime.date, datetime.datetime)):
        # An unquoted 2026-03-11 in YAML arrives as a date.
        api_version = api_version.isoformat()[:10]
    rate = section.get("requests_per_second", 3.0)
    if isinstance(rate, bool) or not isinstance(rate, (int, float)) or rate <= 0:
        raise ConfigError("notion.requests_per_second must be a number greater than 0")
    credential_name = section.get("credential_name")
    return NotionSettings(
        credential_name=str(credential_name) if credential_name else None,
        credential_key=str(section.get("credential_key") or "secret"),
        token_env=str(section.get("token_env") or "NOTION_TOKEN"),
        api_version=str(api_version),
        requests_per_second=float(rate),
    )


def _identifier(value: Any, name: str, allow_empty: bool = False) -> str:
    if value is None or value == "":
        if allow_empty:
            return ""
        raise ConfigError(f"{name} is required")
    if not isinstance(value, str) or not _IDENTIFIER.match(value):
        raise ConfigError(
            f"{name} must contain only letters, digits and underscores and not start with a digit; got {value!r}"
        )
    return value


def _target(section: Mapping) -> TargetSettings:
    return TargetSettings(
        catalog=_identifier(section.get("catalog"), "target.catalog"),
        schema=_identifier(section.get("schema"), "target.schema"),
        table_prefix=_identifier(section.get("table_prefix"), "target.table_prefix", allow_empty=True),
    )


def _sync(section: Mapping) -> SyncSettings:
    raw_mode = section.get("mode")
    mode = normalize_mode(raw_mode) if raw_mode is not None else "cdc"

    objects = section.get("objects")
    if not isinstance(objects, (list, tuple)) or not objects:
        raise ConfigError(
            f"sync.objects must be a non-empty list drawn from: {', '.join(SUPPORTED_OBJECTS)}"
        )
    for name in objects:
        if name not in SUPPORTED_OBJECTS:
            raise ConfigError(
                f"sync.objects: unknown object {name!r}; supported: {', '.join(SUPPORTED_OBJECTS)}"
            )
    if len(set(objects)) != len(objects):
        raise ConfigError("sync.objects contains a duplicate entry")

    overlap = section.get("overlap_seconds", 120)
    if isinstance(overlap, bool) or not isinstance(overlap, int) or overlap < 0:
        raise ConfigError("sync.overlap_seconds must be an integer of 0 or more")

    return SyncSettings(objects=tuple(objects), mode=mode, overlap_seconds=overlap)
