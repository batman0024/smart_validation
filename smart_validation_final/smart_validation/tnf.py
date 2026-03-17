from __future__ import annotations
from pathlib import Path
import pandas as pd

def tnf_check(study, cfg):
    excel_name = cfg.get('tnf','excel_name', default='tnf.xlsx')
    prog_name  = cfg.get('tnf','program_name', default='tnfconvert.sas')
    log_name   = cfg.get('tnf','log_name', default='tnfconvert.log')
    rows = []
    tnf_excel = study.docs / excel_name
    tnf_prog  = study.tools / prog_name
    tnf_log   = study.tools / log_name
    if not tnf_excel.exists():
        rows.append(dict(program_name=prog_name, status='MISSING', details='TNF Excel file not found'))
        return pd.DataFrame(rows)
    excel_m = pd.to_datetime(tnf_excel.stat().st_mtime, unit='s', utc=True)
    if not tnf_prog.exists():
        rows.append(dict(program_name=prog_name, status='MISSING', details='TNF conversion program not found in tools'))
        return pd.DataFrame(rows)
    prog_m = pd.to_datetime(tnf_prog.stat().st_mtime, unit='s', utc=True)
    if not tnf_log.exists():
        rows.append(dict(program_name=prog_name, status='NOT RUN', details='TNF conversion program has not been run'))
        return pd.DataFrame(rows)
    log_m = pd.to_datetime(tnf_log.stat().st_mtime, unit='s', utc=True)
    if excel_m > log_m:
        rows.append(dict(program_name=prog_name, status='OUTDATED', details='TNF Excel updated but conversion not re-run'))
    elif prog_m > log_m:
        rows.append(dict(program_name=prog_name, status='OUTDATED', details='TNF program modified but not re-run'))
    else:
        rows.append(dict(program_name=prog_name, status='CURRENT', details='TNF conversion up-to-date'))
    return pd.DataFrame(rows)
