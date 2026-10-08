"""
Project settings: config.yaml, overridden by config.local.yaml and environment variables.

    from pipeline.common.config import settings, project_path
    settings["cbs"]["start_year"]               # 2020
    project_path(settings["paths"]["lakehouse"])  # absolute Path

Order (later wins): config.yaml (or the file in LAKEHOUSE_CONFIG) < config.local.yaml
next to it < environment variables LAKEHOUSE__<SECTION>__<KEY>. Overrides may only set
keys that exist in config.yaml, so a typo is an error instead of being ignored.
"""
from __future__ import annotations

import atexit
import os
import time
from collections.abc import Mapping
from pathlib import Path

import yaml

PROJECT_DIR = Path(__file__).resolve().parents[2]      # pipeline/common/config.py -> project
ENV_PREFIX = "LAKEHOUSE__"

# Workaround for a crash in the deltalake library (seen with 1.4 - 1.6.6): right after a
# Delta table is read, a background thread of the library is still busy for a moment.
# If Python exits in that moment, the process aborts ("terminate called without an
# active exception", exit code 134) although all work is done. Measured 2026-10-08: a
# script that reads one table and exits crashed in 9-14 of 20 runs; with a short pause
# at exit in 0 of 30 (already at 0.02 s). Every script imports this module, so one
# pause here covers them all. Remove it once deltalake fixes the race.
EXIT_PAUSE_SECONDS = 0.1
atexit.register(time.sleep, EXIT_PAUSE_SECONDS)


class ConfigError(ValueError):
    pass


def _merge(base: dict, override: Mapping, source: str, trail: str = "") -> dict:
    """Copy of base with override applied; override may only use base's keys."""
    out = dict(base)
    for key, value in override.items():
        where = f"{trail}{key}"
        if key not in base:
            raise ConfigError(f"{source}: unknown setting '{where}' (known: {sorted(base)})")
        if isinstance(base[key], dict):
            if not isinstance(value, Mapping):
                raise ConfigError(f"{source}: '{where}' must be a mapping")
            out[key] = _merge(base[key], value, source, f"{where}.")
        else:
            out[key] = value
    return out


def _env_overrides(environ: Mapping[str, str]) -> dict:
    """LAKEHOUSE__CBS__START_YEAR=2022 -> {"cbs": {"start_year": 2022}}"""
    nested: dict = {}
    for name, raw in environ.items():
        if not name.startswith(ENV_PREFIX):
            continue
        keys = [k.lower() for k in name[len(ENV_PREFIX):].split("__")]
        node = nested
        for k in keys[:-1]:
            node = node.setdefault(k, {})
        node[keys[-1]] = yaml.safe_load(raw) if raw.strip() else None
    return nested


def load(config_file: Path | None = None, environ: Mapping[str, str] | None = None) -> dict:
    """All settings: config.yaml (or LAKEHOUSE_CONFIG), then config.local.yaml next to
    it, then LAKEHOUSE__* environment variables; later sources win. `environ` can be
    passed in so the tests don't depend on the real environment."""
    environ = os.environ if environ is None else environ
    config_file = Path(config_file or environ.get("LAKEHOUSE_CONFIG") or PROJECT_DIR / "config.yaml")
    with config_file.open(encoding="utf-8") as f:
        settings = yaml.safe_load(f) or {}

    local = config_file.with_name("config.local.yaml")
    if local.exists():
        with local.open(encoding="utf-8") as f:
            settings = _merge(settings, yaml.safe_load(f) or {}, local.name)
    return _merge(settings, _env_overrides(environ), "environment")


def project_path(value: str | Path) -> Path:
    """Relative paths in the config are relative to the project folder."""
    p = Path(value).expanduser()
    return p if p.is_absolute() else PROJECT_DIR / p


settings = load()
