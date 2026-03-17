
from __future__ import annotations
from pathlib import Path
import re
import pandas as pd

# Matches comment lines like: * Program Author: (userid) *
AUTHOR_RE = re.compile(
    r"^\s*\*?\s*Program\s+Author:\s*\(([^)]+)\)\s*\*?\s*$",
    re.IGNORECASE
)

def _safe_head(path: Path, n: int = 80):
    """Yield up to n lines from the start of a file, safely."""
    try:
        with path.open('r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f):
                if i >= n:
                    break
                yield line.rstrip('\n')
    except Exception:
        return

def extract_programmers(df_sas: pd.DataFrame) -> pd.DataFrame:
    """
    Build a table of discovered programmers/users from SAS programs.
    Expected columns in df_sas: ['program_name','file_path','mtime','domain','label', ...]
    Returns: columns ['program_name','userid','file_path','mtime','domain','is_tool_generated','label']
    """
    columns = ['program_name','userid','file_path','mtime','domain','is_tool_generated','label']
    if df_sas is None or df_sas.empty:
        return pd.DataFrame(columns=columns)

    rows = []
    for _, r in df_sas.iterrows():
        p = Path(r['file_path'])
        userid = ''
        for L in _safe_head(p):
            m = AUTHOR_RE.match(L.strip())
            if m:
                userid = m.group(1).strip()
                break

        # Heuristic: TFL programs with "-s" are considered tool-generated
        is_tool = (r['domain'] == 'TFL' and ('-s' in r['program_name']))

        rows.append(dict(
            program_name=r['program_name'],
            userid=userid or ('TOOL' if r.get('label') == 'tools' else ''),
            file_path=str(p),
            mtime=r['mtime'],
            domain=r['domain'],
            is_tool_generated=is_tool,
            label=r.get('label', '')
        ))

    out = pd.DataFrame(rows)
    if out.empty:
        return out

    # Keep most recent per (domain, program_name, label)
    return (out.sort_values('mtime', ascending=False)
              .drop_duplicates(['domain','program_name','label']))
