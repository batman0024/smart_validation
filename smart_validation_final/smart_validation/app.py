from __future__ import annotations
import sys, os
pkg_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(pkg_dir, os.pardir))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

from smart_validation.config import Config
from smart_validation.paths import build_study_paths
from smart_validation.assemble import collect, assemble
from smart_validation.report import write_reports

from pathlib import Path
import streamlit as st
import pandas as pd

st.set_page_config(page_title='Smart Validation', layout='wide')

@st.cache_resource
def load_config(path: str):
    return Config.from_file(path)

@st.cache_data(show_spinner=False)
def list_projects(studies_root: Path):
    if not studies_root.exists(): return []
    return sorted([p.name for p in studies_root.iterdir() if p.is_dir() and p.name.lower().startswith('p')])

@st.cache_data(show_spinner=False)
def list_studies(studies_root: Path, project: str):
    base = studies_root / project
    if not base.exists(): return []
    return sorted([p.name for p in base.iterdir() if p.is_dir() and p.name.lower().startswith('s')])


def local_directory_picker(default: str='') -> str:
    try:
        import tkinter as tk
        from tkinter import filedialog
        root = tk.Tk(); root.withdraw()
        path = filedialog.askdirectory(initialdir=default or None)
        root.destroy()
        return path or ''
    except Exception:
        return ''


def style_status(df: pd.DataFrame, col: str):
    colors = {
        'MISSING': '#ffc0cb', 'EMPTY': '#fffacd', 'ACTIVE': '#90ee90', 'OUTDATED': '#ffdabe', 'NOT RUN': '#ffc0cb',
        'CURRENT': '#90ee90', 'Critical Issues Found': '#ffc0cb', 'Timing Issue': '#fffacd', 'Run Order Issue': '#fffacd',
        'Warnings Found': '#add8e6', 'OK': '#90ee90', 'Tool-generated (No Validation)': '#e0ffe0'
    }
    def _style(val): return f'background-color: {colors.get(str(val), "")}'
    return df.style.applymap(lambda v: _style(v), subset=pd.IndexSlice[:, [col]])


def main():
    st.title('Smart Validation Tool')

    with st.sidebar:
        config_path = st.text_input('Config path', value='config.yaml')
        cfg = load_config(config_path)
        studies_root = Path(cfg.get('studies_root'))
        mode = st.radio('Select study by', ['Project/Study dropdown', 'Browse folder'])
        study_root = None; project = study = ''
        if mode == 'Project/Study dropdown':
            projects = list_projects(studies_root)
            proj = st.selectbox('Project (pxxx)', options=projects)
            studies = list_studies(studies_root, proj) if proj else []
            stud = st.selectbox('Study (sxxxxxxx)', options=studies)
            if proj and stud:
                study_root = studies_root / proj / stud
                project, study = proj, stud
        else:
            default_dir = str(studies_root)
            if st.button('Browse...'):
                picked = local_directory_picker(default_dir)
                if picked: st.session_state['picked_dir'] = picked
            picked_dir = st.session_state.get('picked_dir', default_dir)
            study_root_input = st.text_input('Study root path', value=picked_dir)
            study_root = Path(study_root_input)
            parts = [p.lower() for p in study_root.parts]
            if 'projects' in parts:
                i = parts.index('projects')
                if i+1 < len(study_root.parts): project = study_root.parts[i+1]
                if i+2 < len(study_root.parts): study = study_root.parts[i+2]
        tolerant_missing = st.toggle('Do not fail on missing folders', value=True)
        live = st.toggle('Auto refresh (5s)', value=False)
        if live and hasattr(st, 'autorefresh'):
            st.autorefresh(interval=5000, key='__auto__')

    if not study_root or not Path(study_root).exists():
        st.info('Select a valid study path to start.'); return

    study_paths = build_study_paths(cfg, Path(study_root))

    st.caption(f"Project: **{study_paths.project_code}** | Study: **{study_paths.study_code}** | Root: {study_paths.study_root}")

    with st.spinner('Scanning & analyzing...'):
        parts = collect(study_paths, cfg)
        outputs = assemble(parts, study_paths, cfg)

    fin = outputs['final_report']
    c1,c2,c3,c4,c5,c6 = st.columns(6)
    c1.metric('Total', len(fin))
    c2.metric('SDTM', int((fin['domain']=='SDTM').sum()))
    c3.metric('ADaM', int((fin['domain']=='ADAM').sum()))
    c4.metric('TFL', int((fin['domain']=='TFL').sum()))
    c5.metric('Critical', int((fin['overall_status']=='Critical Issues Found').sum()))
    miss = int((fin['overall_status'].isin(['Missing Production Program','Missing Validation Program'])).sum())
    c6.metric('Missing (Prod/Val)', miss)

    st.subheader('Paths Summary'); st.write(style_status(outputs['paths_summary'], 'status'))
    st.subheader('Final Report'); st.write(style_status(fin, 'overall_status'))
    with st.expander('Detailed Issues'): st.dataframe(outputs['all_issues'], use_container_width=True)
    with st.expander('TFL Output Mapping (.out)'): st.dataframe(outputs['tfl_pairs'], use_container_width=True)
    with st.expander('Tools Programs'): st.dataframe(outputs['programs_tools'], use_container_width=True)
    if st.button('Export Excel/CSV'):
        path = write_reports(outputs, study_paths, cfg); st.success(f'Exported: {path}')

if __name__ == '__main__':
    main()
