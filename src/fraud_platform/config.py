"""Configuration loading.

Settings live in ``config/settings.yaml`` and fraud rules in ``config/rules.yaml``.
Any setting can be overridden by an environment variable following the pattern
``FRAUD__<SECTION>__<KEY>`` (double underscore separates nesting levels), e.g.::

    FRAUD__SOURCE__TYPE=kafka
    FRAUD__WAREHOUSE__TYPE=postgres
"""
from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
from typing import Any

import yaml

ENV_PREFIX = "FRAUD__"


def _default_config_dir() -> Path:
    env_dir = os.environ.get("FRAUD_CONFIG_DIR")
    if env_dir:
        return Path(env_dir)
    # <repo>/src/fraud_platform/config.py -> <repo>/config
    return Path(__file__).resolve().parents[2] / "config"


def _coerce(value: str) -> Any:
    """Parse env-var strings into bool/int/float/list when possible (YAML semantics)."""
    try:
        return yaml.safe_load(value)
    except yaml.YAMLError:
        return value


def apply_env_overrides(cfg: dict, environ: dict[str, str] | None = None) -> dict:
    environ = os.environ if environ is None else environ
    out = copy.deepcopy(cfg)
    for name, raw in environ.items():
        if not name.startswith(ENV_PREFIX):
            continue
        path = [p.lower() for p in name[len(ENV_PREFIX):].split("__") if p]
        if not path:
            continue
        node = out
        for key in path[:-1]:
            node = node.setdefault(key, {})
            if not isinstance(node, dict):
                break
        else:
            node[path[-1]] = _coerce(raw)
    return out


def load_yaml(path: str | Path) -> dict:
    with open(path, encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def load_settings(path: str | Path | None = None) -> dict:
    path = Path(path) if path else _default_config_dir() / "settings.yaml"
    return apply_env_overrides(load_yaml(path))


def load_rules(path: str | Path | None = None) -> dict:
    path = Path(path) if path else _default_config_dir() / "rules.yaml"
    rules = load_yaml(path)
    rules["config_hash"] = config_fingerprint(rules)
    return rules


def config_fingerprint(cfg: dict) -> str:
    """Stable short hash of a config dict - stored with every score for auditability."""
    payload = json.dumps({k: v for k, v in cfg.items() if k != "config_hash"}, sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()[:12]
