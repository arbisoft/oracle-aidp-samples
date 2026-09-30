# Copyright (c) 2026, Oracle and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl/
"""Notion -> Oracle AI Data Platform ingestion sample."""

from .config import Config, ConfigError, load_config, parse_config

__all__ = ["Config", "ConfigError", "load_config", "parse_config"]
