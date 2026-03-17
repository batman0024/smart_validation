from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict
import yaml

@dataclass
class Config:
    raw: Dict[str, Any]

    @classmethod
    def from_file(cls, path: str = 'config.yaml') -> 'Config':
        p = Path(path)
        if not p.exists():
            raise FileNotFoundError(f'Config not found: {p}')
        with p.open('r', encoding='utf-8') as f:
            raw = yaml.safe_load(f) or {}
        return cls(raw=raw)

    def get(self, *keys, default=None):
        cur = self.raw
        for k in keys:
            if not isinstance(cur, dict):
                return default
            cur = cur.get(k, default)
        return cur
