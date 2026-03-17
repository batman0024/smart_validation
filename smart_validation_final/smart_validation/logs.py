
from __future__ import annotations
from pathlib import Path
import re
import pandas as pd

# Ignore common benign NOTE: lines
NOTE_IGNORE_RE = re.compile(
    r"^(?:NOTE:\s*(?:DATA statement|PROCEDURE|There were|The SAS System|Copyright))",
    re.IGNORECASE
)
ERR_RE  = re.compile(r"\bERROR:\b", re.IGNORECASE)
WARN_RE = re.compile(r"\bWARNING:\b", re.IGNORECASE)
NOTE_RE = re.compile(r"\bNOTE:\b", re.IGNORECASE)

def _read_lines(path: Path, nmax: int = 20000):
    try:
        with path.open('r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f):
                if i >= nmax:
                    break
                yield line.rstrip('\n')
    except Exception:
        return

def analyze_logs(df_logs: pd.DataFrame):
    """
    Parse SAS .log files for ERROR/WARNING/NOTE (minus common ignorable notes),
    and compute latest run timestamps.

    Returns:
        issues_df (columns: ['program_name','issue_type','issue_text','file_path','domain'])
        run_df    (columns: ['program_name','run_datetime','domain'])
    """
    issue_rows, time_rows = [], []
    if df_logs is None or df_logs.empty:
        return pd.DataFrame(issue_rows), pd.DataFrame(time_rows)

    for _, r in df_logs.iterrows():
        p = Path(r['file_path'])
        prog = r['program_name']
        dom  = r['domain']
        run_dt = pd.to_datetime(r['mtime'], unit='s', utc=True)

        for L in _read_lines(p):
            if ERR_RE.search(L):
                issue_rows.append(dict(program_name=prog, issue_type='ERROR',
                                       issue_text=L[:500], file_path=str(p), domain=dom))
            elif WARN_RE.search(L):
                issue_rows.append(dict(program_name=prog, issue_type='WARNING',
                                       issue_text=L[:500], file_path=str(p), domain=dom))
            elif NOTE_RE.search(L) and not NOTE_IGNORE_RE.search(L):
                issue_rows.append(dict(program_name=prog, issue_type='NOTE',
                                       issue_text=L[:500], file_path=str(p), domain=dom))

        time_rows.append(dict(program_name=prog, run_datetime=run_dt, domain=dom))

    return pd.DataFrame(issue_rows), pd.DataFrame(time_rows)
