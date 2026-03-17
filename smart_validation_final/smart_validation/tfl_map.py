
from __future__ import annotations
from pathlib import Path
import re
import pandas as pd


def _cfg_list(cfg, section: str, key: str, default: list[str]) -> list[str]:
    """Read a list setting from cfg with a default."""
    try:
        val = cfg.get(section, key, default=default)
        # cfg may return a python list already; if it returns a string, wrap it
        if isinstance(val, list):
            return val
        return list(val) if val is not None else list(default)
    except Exception:
        return list(default)


def _compile_patterns(cfg) -> list[re.Pattern]:
    patterns = _cfg_list(cfg, 'tfl', 'log_output_patterns', default=[
        r'(?i)\b(?:creating|writing|opening)\s+([^\s\'\"]+\.out)\b',
        r'(?i)\bNOTE:\s+(?:Writing|File)\s+[\'\"]?([^\'\"\s]+\.(?:out))[\'\"]?'
    ])
    compiled = []
    for p in patterns:
        try:
            compiled.append(re.compile(p))
        except re.error:
            # Skip invalid patterns rather than crashing
            continue
    return compiled


def _output_exts(cfg) -> list[str]:
    exts = _cfg_list(cfg, 'tfl', 'output_exts', default=['.out'])
    # Normalize to lowercase
    return [e.lower() for e in exts]


def _read_lines(path: Path, nmax: int = 50000):
    try:
        with path.open('r', encoding='utf-8', errors='ignore') as f:
            for i, line in enumerate(f):
                if i >= nmax:
                    break
                yield line.rstrip('\n')
    except Exception:
        return


def _pick_filename_from_match(m: re.Match, exts: list[str]) -> str | None:
    """
    Try to return a filename with one of the desired extensions from a regex match.
    Strategy:
      1) Check groups for something that looks like '*.ext'
      2) Fallback: scan the matched text for '*.ext'
    """
    text = m.group(0) or ""
    # Look through groups first
    for i in range(1, (m.lastindex or 0) + 1):
        g = m.group(i)
        if not g:
            continue
        for ext in exts:
            if re.search(rf'(?i)\b[A-Za-z0-9._-]+{re.escape(ext)}\b', g):
                return g

    # Fallback: scan the matched text
    for ext in exts:
        m2 = re.search(rf'(?i)\b([A-Za-z0-9._-]+{re.escape(ext)})\b', text)
        if m2:
            return m2.group(1)
    return None


def _stem(name: str) -> str:
    """Lowercase filename stem (without extension)."""
    if not name:
        return ""
    # Drop trailing extension only (keep dots in name)
    return re.sub(r'\.[^.\\/:]+$', '', name).lower()


def _build_side_map(df_logs: pd.DataFrame, side: str, cfg) -> pd.DataFrame:
    """
    For one side ('prod' or 'valid'), read logs and extract output filenames.
    Returns columns:
        [ '<side>_program', '<side>_output', 'output_stem' ]
    """
    required_cols = ['program_name', 'file_path']
    for col in required_cols:
        if col not in df_logs.columns:
            return pd.DataFrame(columns=[f'{side}_program', f'{side}_output', 'output_stem'])

    patterns = _compile_patterns(cfg)
    exts = _output_exts(cfg)

    rows = []
    for _, r in df_logs.iterrows():
        log_path = Path(r['file_path'])
        prog = r['program_name']
        # Parse the log for output references
        for line in _read_lines(log_path):
            for pat in patterns:
                m = pat.search(line)
                if not m:
                    continue
                fname = _pick_filename_from_match(m, exts)
                if not fname:
                    continue
                rows.append({
                    f'{side}_program': prog,
                    f'{side}_output': fname,
                    'output_stem': _stem(fname),
                })

    if not rows:
        return pd.DataFrame(columns=[f'{side}_program', f'{side}_output', 'output_stem'])

    df = pd.DataFrame(rows)

    # Deduplicate: keep first occurrence per (program, output_stem)
    df = (df.sort_values([f'{side}_program', 'output_stem'])
            .drop_duplicates([f'{side}_program', 'output_stem']))
    return df


def tfl_pairs(
    prod_logs: pd.DataFrame,
    val_logs: pd.DataFrame,
    prod_out: pd.DataFrame | None,
    val_out: pd.DataFrame | None,
    cfg
) -> pd.DataFrame:
    """
    Build a PROD↔VALID pairing of TFL programs via output filenames discovered in logs.
    Returns a DataFrame with at least:
        ['output_stem', 'prod_program', 'valid_program', 'prod_output', 'valid_output']
    This aligns with assemble.py's robust merging strategy.
    """

    prod_map = _build_side_map(prod_logs if prod_logs is not None else pd.DataFrame(), 'prod', cfg)
    valid_map = _build_side_map(val_logs if val_logs is not None else pd.DataFrame(), 'valid', cfg)

    # Full outer join on output_stem to pair up prod/valid programs that generate the same output
    if not prod_map.empty and not valid_map.empty:
        pairs = prod_map.merge(valid_map, on='output_stem', how='outer')
    elif not prod_map.empty:
        pairs = prod_map.copy()
        # Make sure valid columns exist
        for c in ['valid_program', 'valid_output']:
            if c not in pairs.columns:
                pairs[c] = pd.NA
    elif not valid_map.empty:
        pairs = valid_map.copy()
        # Make sure prod columns exist
        for c in ['prod_program', 'prod_output']:
            if c not in pairs.columns:
                pairs[c] = pd.NA
    else:
        # Nothing found in logs
        return pd.DataFrame(columns=['output_stem', 'prod_program', 'valid_program', 'prod_output', 'valid_output'])

    # Ensure canonical columns and order
    for rename_map in [
        { 'prod_program': 'prod_program', 'prod_output': 'prod_output' },
        { 'valid_program': 'valid_program', 'valid_output': 'valid_output' }
    ]:
        # These are no-ops; kept for clarity.

        # Ensure missing columns exist
        for k in rename_map.keys():
            if k not in pairs.columns:
                pairs[k] = pd.NA

    # Reorder and drop duplicates on (output_stem, prod_program, valid_program)
    pairs = pairs[['output_stem',
                   'prod_program', 'prod_output'
                   ] + ([c for c in ['valid_program','valid_output'] if c in pairs.columns])]
    pairs = (pairs.sort_values(['output_stem', 'prod_program', 'valid_program'], na_position='last')
                  .drop_duplicates(['output_stem', 'prod_program', 'valid_program']))

    return pairs
