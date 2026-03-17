
from __future__ import annotations

import re
from pathlib import Path as _P
import pandas as pd

from smart_validation.scanner import scan_dir
from smart_validation.author import extract_programmers
from smart_validation.logs import analyze_logs
from smart_validation.lst import analyze_lsts
from smart_validation.tfl_map import tfl_pairs
from smart_validation.tnf import tnf_check


# ---------------------------
# Helpers for robust merging
# ---------------------------

def _first_present(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first column name from candidates that exists in df, else None."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _ensure_output_stem(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure an 'output_stem' column exists.
    If absent, derive from the first present of common filename columns by stripping extension.
    """
    if 'output_stem' in df.columns:
        return df

    candidates = [
        'output', 'outfile', 'output_file', 'output_filename',
        'out', 'out_name', 'filename', 'file'
    ]
    src = _first_present(df, candidates)
    if src is not None:
        dfx = df.copy()
        # Strip the last extension if any, e.g., abc.lst -> abc
        dfx['output_stem'] = dfx[src].astype(str).str.replace(r'\.[^.\\/:]+$', '', regex=True)
        return dfx

    # If we really have nothing to derive from, create a nullable column to avoid KeyErrors downstream.
    dfx = df.copy()
    dfx['output_stem'] = pd.NA
    return dfx


def _safe_merge_pairs(
    side_df: pd.DataFrame,
    pairs_df: pd.DataFrame,
    side: str  # 'prod' or 'valid'
) -> pd.DataFrame:
    """
    Merge TFL program table (side_df) with tfl_pairs_df robustly.
    Tries multiple right_on candidates; falls back to joining on 'program_name' if practical.
    """
    assert side in ('prod', 'valid')

    # Candidate right-side join keys in preference order
    if side == 'prod':
        candidates = ['prod_program', 'program_name', 'program', 'prog', 'sas_program']
    else:
        candidates = ['valid_program', 'program_name', 'program', 'prog', 'sas_program']

    pairs_df = _ensure_output_stem(pairs_df)

    # Try preferred right_on keys
    col = _first_present(pairs_df, candidates)
    if col is not None:
        merged = side_df.merge(pairs_df, left_on='program_name', right_on=col, how='left')
        return merged

    # If the pairs frame has a plain 'program_name', merge on it
    if 'program_name' in pairs_df.columns:
        merged = side_df.merge(pairs_df, on='program_name', how='left')
        return merged

    # As a last resort, perform a left join with a dummy key so we keep the left rows and avoid KeyErrors.
    merged = side_df.copy()
    return merged


def collect(study, cfg):
    include_exts = cfg.get('scan', 'include_extensions', default=['.sas', '.log', '.lst', '.out'])
    ignore_hidden = bool(cfg.get('scan', 'ignore_hidden', default=True))

    return {
        'prod_sdtm': scan_dir(study.prod_sdtm, include_exts, ignore_hidden, label='prod_sdtm'),
        'prod_adam': scan_dir(study.prod_adam, include_exts, ignore_hidden, label='prod_adam'),
        'prod_tfl':  scan_dir(study.prod_tfl,  include_exts, ignore_hidden, label='prod_tfl'),
        'valid':     scan_dir(study.valid,     include_exts, ignore_hidden, label='valid'),
        'tools':     scan_dir(study.tools,     include_exts, ignore_hidden, label='tools'),
    }


def assemble(parts, study, cfg):
    # Partition by filetype
    prod_sdtm_sas = parts['prod_sdtm'].query("filetype=='SAS'")
    prod_adam_sas = parts['prod_adam'].query("filetype=='SAS'")
    prod_tfl_sas  = parts['prod_tfl'].query("filetype=='SAS'")
    valid_sas     = parts['valid'].query("filetype=='SAS'")
    tools_sas     = parts['tools'].query("filetype=='SAS'")

    # Programmers
    programs_prod = pd.concat(
        [
            extract_programmers(prod_sdtm_sas),
            extract_programmers(prod_adam_sas),
            extract_programmers(prod_tfl_sas),
        ],
        ignore_index=True
    )
    programs_valid = extract_programmers(valid_sas)
    programs_tools = extract_programmers(tools_sas)

    # Logs, lists
    prod_logs = pd.concat(
        [
            parts['prod_sdtm'].query("filetype=='LOG'"),
            parts['prod_adam'].query("filetype=='LOG'"),
            parts['prod_tfl'].query("filetype=='LOG'"),
        ],
        ignore_index=True
    )
    valid_logs = parts['valid'].query("filetype=='LOG'")

    log_issues_prod, run_times_prod = analyze_logs(prod_logs)
    log_issues_valid, run_times_valid = analyze_logs(valid_logs)
    lst_issues_valid = analyze_lsts(parts['valid'].query("filetype=='LST'"))

    all_issues = pd.concat(
        [
            log_issues_prod.assign(source='PROD_LOG'),
            log_issues_valid.assign(source='VALID_LOG'),
            lst_issues_valid.assign(source='VALID_LST'),
        ],
        ignore_index=True
    )

    # --- HARDEN all_issues to avoid KeyErrors if any upstream source is empty ---
    required_cols = ['domain', 'program_name', 'issue_type', 'issue_text', 'file_path', 'source']
    for c in required_cols:
        if c not in all_issues.columns:
            all_issues[c] = pd.NA

    # TFL pairs (prod ↔ valid ↔ outputs)
    tfl_pairs_df = tfl_pairs(
        prod_logs=prod_logs, val_logs=valid_logs,
        prod_out=parts['prod_tfl'].query("filetype=='OUT'"),
        val_out=parts['valid'].query("filetype=='OUT'"),
        cfg=cfg
    )

    # SDTM/ADaM merge on (domain, program_name)
    sd_ad_prod = programs_prod.query("domain!='TFL'")
    sd_ad_val  = programs_valid.query("domain!='TFL'")
    merged_sd_ad = sd_ad_prod.merge(
        sd_ad_val,
        on=['domain', 'program_name'],
        how='outer',
        suffixes=('_prod', '_valid')
    )

    # --- Robust TFL merge ---
    tfl_prod = programs_prod.query("domain=='TFL'")
    tfl_val  = programs_valid.query("domain=='TFL'")

    # Left: PROD ↔ pairs
    left = _safe_merge_pairs(tfl_prod, tfl_pairs_df, side='prod')
    # Right: VALID ↔ pairs
    right = _safe_merge_pairs(tfl_val, tfl_pairs_df, side='valid')

    # Ensure both sides have a join key; prefer output_stem, else fall back to program_name
    left_has = 'output_stem' in left.columns
    right_has = 'output_stem' in right.columns

    if left_has and right_has:
        tfl_join = left.merge(right, on='output_stem', how='outer', suffixes=('_prod', '_valid'))
    else:
        # Fall back on program_name-based join
        join_left_key = 'program_name'
        join_right_key = 'program_name'
        # If merge causes duplicate columns, suffixes handle it
        tfl_join = left.merge(right, left_on=join_left_key, right_on=join_right_key,
                              how='outer', suffixes=('_prod', '_valid'))

    def pick(a, b):
        return tfl_join[a].combine_first(tfl_join[b])

    tfl_merged = pd.DataFrame({
        'domain': 'TFL',
        'program_name': pick('program_name_prod', 'program_name_valid')
                          if 'program_name_prod' in tfl_join.columns and 'program_name_valid' in tfl_join.columns
                          else tfl_join.get('program_name', pd.Series([], dtype='object')),
        'userid_prod': tfl_join.get('userid_prod', ''),
        'userid_valid': tfl_join.get('userid_valid', ''),
        'file_path_prod': tfl_join.get('file_path_prod', ''),
        'file_path_valid': tfl_join.get('file_path_valid', ''),
        'is_tool_generated_prod': tfl_join.get('is_tool_generated_prod', False),
        'is_tool_generated_valid': tfl_join.get('is_tool_generated_valid', False),
    })

    sd_ad_merged = pd.DataFrame({
        'domain': merged_sd_ad['domain'],
        'program_name': merged_sd_ad['program_name'],
        'userid_prod': merged_sd_ad.get('userid_prod', ''),
        'userid_valid': merged_sd_ad.get('userid_valid', ''),
        'file_path_prod': merged_sd_ad.get('file_path_prod', ''),
        'file_path_valid': merged_sd_ad.get('file_path_valid', ''),
        'is_tool_generated_prod': merged_sd_ad.get('is_tool_generated_prod', False),
        'is_tool_generated_valid': merged_sd_ad.get('is_tool_generated_valid', False),
    })

    final_base = pd.concat([sd_ad_merged, tfl_merged], ignore_index=True)

    # Attach recent run times
    def _recent(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        return (df.sort_values('run_datetime', ascending=False)
                  .drop_duplicates(['domain', 'program_name']))

    rtp = _recent(run_times_prod)
    rtv = _recent(run_times_valid)

    final = final_base.merge(
        rtp.rename(columns={'run_datetime': 'prod_run_time'}),
        on=['domain', 'program_name'],
        how='left'
    )
    final = final.merge(
        rtv.rename(columns={'run_datetime': 'valid_run_time'}),
        on=['domain', 'program_name'],
        how='left'
    )

    # File mtime (guarded)
    def _mtime_map(col: str):
        def _f(x):
            try:
                if x and isinstance(x, str) and _P(x).exists():
                    return pd.to_datetime(_P(x).stat().st_mtime, unit='s', utc=True)
            except Exception:
                pass
            return pd.NaT
        return final[col].map(_f)

    final['prod_modify_time'] = _mtime_map('file_path_prod')
    final['valid_modify_time'] = _mtime_map('file_path_valid')

    # Timing logic
    def timing(row: pd.Series) -> str:
        if row.get('is_tool_generated_prod', False):
            return 'Tool-generated (No Validation)'
        if not row.get('file_path_prod'):
            return 'Missing Production Program'
        if not row.get('file_path_valid'):
            return 'Missing Validation Program'

        pm, pr = row.get('prod_modify_time'), row.get('prod_run_time')
        vm, vr = row.get('valid_modify_time'), row.get('valid_run_time')

        if pd.notna(pm) and pd.notna(pr) and pm > pr:
            return 'Production program modified after run'
        if pd.notna(vm) and pd.notna(vr) and vm > vr:
            return 'Validation program modified after run'
        if pd.notna(pr) and pd.notna(vr) and pr > vr:
            return 'Production run after validation'
        return 'Timing OK'

    final['timing_status'] = final.apply(timing, axis=1)

    # Issue counts
    def _sev(issue_df: pd.DataFrame, prog: str, dom: str, types: list[str]) -> int:
        if all_issues.empty:
            return 0
        m = (
            (issue_df['program_name'] == prog) &
            (issue_df['domain'] == dom) &
            (issue_df['issue_type'].isin(types))
        )
        return int(m.sum())

    errs, warns, notes, mism = [], [], [], []
    for _, row in final.iterrows():
        p, d = row['program_name'], row['domain']
        errs.append(_sev(all_issues, p, d, ['ERROR', 'MISMATCH']))
        warns.append(_sev(all_issues, p, d, ['WARNING']))
        notes.append(_sev(all_issues, p, d, ['NOTE']))
        mism.append(_sev(all_issues, p, d, ['MISMATCH']))

    final['error_count'] = errs
    final['warning_count'] = warns
    final['note_count'] = notes
    final['mismatch_count'] = mism

    # Overall status
    def overall(row: pd.Series) -> str:
        if row['timing_status'] in ['Missing Production Program', 'Missing Validation Program']:
            return row['timing_status']
        if row['error_count'] > 0:
            return 'Critical Issues Found'
        if row['timing_status'] in ['Production program modified after run', 'Validation program modified after run']:
            return 'Timing Issue'
        if row['timing_status'] == 'Production run after validation':
            return 'Run Order Issue'
        if row['warning_count'] > 0:
            return 'Warnings Found'
        if row.get('is_tool_generated_prod', False):
            return 'Tool-generated (No Validation)'
        return 'OK'

    final['overall_status'] = final.apply(overall, axis=1)

    # Paths summary
    def path_status(p: _P) -> str:
        if not p.exists():
            return 'MISSING'
        try:
            next(p.rglob('*.*'))
            return 'ACTIVE'
        except StopIteration:
            return 'EMPTY'

    paths_summary = pd.DataFrame([
        dict(path_type='Production SDTM', path_value=str(study.prod_sdtm), status=path_status(study.prod_sdtm)),
        dict(path_type='Production ADAM', path_value=str(study.prod_adam), status=path_status(study.prod_adam)),
        dict(path_type='Production TFL',  path_value=str(study.prod_tfl),  status=path_status(study.prod_tfl)),
        dict(path_type='Validation',      path_value=str(study.valid),     status=path_status(study.valid)),
        dict(path_type='Tools',           path_value=str(study.tools),     status=path_status(study.tools)),
        dict(path_type='TNF Excel',       path_value=str(study.docs / 'tnf.xlsx'),
             status=('ACTIVE' if (study.docs / 'tnf.xlsx').exists() else 'MISSING')),
    ])

    # Tools programs table (simple)
    if 'programs_tools' in locals() and isinstance(programs_tools, pd.DataFrame) and not programs_tools.empty:
        programs_tools_simple = programs_tools[['program_name', 'file_path', 'mtime']].rename(columns={'mtime': 'modify_time'})
    else:
        programs_tools_simple = pd.DataFrame(columns=['program_name', 'file_path', 'modify_time'])

    return {
        'programs_tools': programs_tools_simple,
        'tnf_check': tnf_check(study, cfg),
        'paths_summary': paths_summary,
        'all_issues': all_issues[['domain', 'program_name', 'issue_type', 'issue_text', 'file_path']]
                      if not all_issues.empty
                      else pd.DataFrame(columns=['domain', 'program_name', 'issue_type', 'issue_text', 'file_path']),
        'final_report': final.sort_values(['domain', 'overall_status', 'program_name']),
        'tfl_pairs': _ensure_output_stem(tfl_pairs_df),
    }
