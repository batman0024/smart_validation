from __future__ import annotations
from pathlib import Path
from typing import List
import re
import pandas as pd

def is_hidden(path: Path) -> bool:
    return any(part.startswith('.') or part.startswith('~$') for part in path.as_posix().split('/'))

def normalize_stem(name: str) -> str:
    return name[2:] if name.lower().startswith('v-') else name

def detect_domain(stem: str) -> str:
    up = stem.upper()
    if re.search(r'^(l-|t-|g-)', stem, flags=re.IGNORECASE) and 'MERGE' not in up:
        return 'TFL'
    if re.search(r'ad', stem, flags=re.IGNORECASE):
        return 'ADAM'
    return 'SDTM'

def scan_dir(dir_path: Path, include_exts: List[str], ignore_hidden: bool=True, label: str='') -> pd.DataFrame:
    rows = []
    if not dir_path.exists():
        return pd.DataFrame(columns=['file_path','filename','ext','filetype','program_name','domain','mtime','size','label'])
    for ext in include_exts:
        for p in sorted(set(dir_path.rglob(f'*{ext}'))):
            if ignore_hidden and is_hidden(p):
                continue
            suffix = p.suffix.lower()
            if suffix == '.sas': ftype = 'SAS'
            elif suffix == '.log': ftype = 'LOG'
            elif suffix == '.lst': ftype = 'LST'
            elif suffix == '.out': ftype = 'OUT'
            else: ftype = 'OTHER'
            stem = normalize_stem(p.stem)
            rows.append(dict(
                file_path=str(p.resolve()), filename=p.name, ext=suffix, filetype=ftype,
                program_name=stem, domain=detect_domain(stem), mtime=p.stat().st_mtime,
                size=p.stat().st_size, label=label
            ))
    return pd.DataFrame(rows)
