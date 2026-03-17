
from __future__ import annotations
from pathlib import Path
import re
import pandas as pd

MISMATCH_RE = re.compile(r"(Mismatch|mismatch|DIFF|Diff|Difference|difference)")
UNMERGED_RE = re.compile(r"unmerged.*observations", re.IGNORECASE)

def _read_lines(path: Path, nmax: int = 20000):
    try:
        with path.open('r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f):
                if i >= nmax:
                    break
                yield line.rstrip('\n')
    except Exception:
        return

def analyze_lsts(df_lst: pd.DataFrame) -> pd.DataFrame:
    """
    Scan validation .lst outputs for mismatch/error patterns and emit issues.
    Ensures a 'domain' field is present so downstream code can group/filter.
    Returns columns at least:
    ['program_name','issue_type','issue_text','file_path','source','domain']
    """
    rows = []
    if df_lst is None or df_lst.empty:
        return pd.DataFrame(rows)

    for _, r in df_lst.iterrows():
        # Only consider LSTs from the validation area
        if r.get('label') != 'valid':
            continue

        p   = Path(r['file_path'])
        prog = r['program_name']
        dom  = r.get('domain')  # <- ensure domain is carried forward

        found = False
        for L in _read_lines(p):
            if MISMATCH_RE.search(L) or UNMERGED_RE.search(L):
                rows.append(dict(program_name=prog, issue_type='MISMATCH',
                                 issue_text=L[:500], file_path=str(p),
                                 source='LST', domain=dom))
                found = True
            if 'invalid numeric data' in L or 'Invalid numeric data' in L:
                rows.append(dict(program_name=prog, issue_type='ERROR',
                                 issue_text='Invalid numeric data found',
                                 file_path=str(p), source='LST', domain=dom))
                found = True
            if 'uninitialized' in L or 'Uninitialized' in L:
                rows.append(dict(program_name=prog, issue_type='WARNING',
                                 issue_text='Uninitialized variable found',
                                 file_path=str(p), source='LST', domain=dom))
                found = True
            if 'division by zero' in L or 'Division by zero' in L:
                rows.append(dict(program_name=prog, issue_type='ERROR',
                                 issue_text='Division by zero detected',
                                 file_path=str(p), source='LST', domain=dom))
                found = True
            if 'merge statement has more than one data set' in L:
                rows.append(dict(program_name=prog, issue_type='ERROR',
                                 issue_text='Merge statement issue',
                                 file_path=str(p), source='LST', domain=dom))
                found = True

        if not found:
            rows.append(dict(program_name=prog, issue_type='NOTE',
                             issue_text='No issues found in LST file',
                             file_path=str(p), source='LST', domain=dom))

    return pd.DataFrame(rows)
