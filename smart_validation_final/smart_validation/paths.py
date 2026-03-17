from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Tuple

@dataclass
class StudyPaths:
    study_root: Path
    prod_sdtm: Path
    prod_adam: Path
    prod_tfl: Path
    valid: Path
    tools: Path
    docs: Path
    project_code: str
    study_code: str


def infer_project_study_from_path(path: Path) -> Tuple[str,str]:
    parts = [p.lower() for p in path.parts]
    if 'projects' in parts:
        i = parts.index('projects')
        proj = path.parts[i+1] if i+1 < len(path.parts) else ''
        stud = path.parts[i+2] if i+2 < len(path.parts) else ''
        return proj, stud
    return ('', '')


def build_study_paths(cfg, study_root: Path) -> StudyPaths:
    L = cfg.get('layout', default={}) or {}
    prod_sdtm = study_root / L.get('prod_sdtm_dir', 'sdtmprog')
    prod_adam = study_root / L.get('prod_adam_dir', 'adamprog')
    prod_tfl  = study_root / L.get('prod_tfl_dir',  'prog')
    valid     = study_root / L.get('valid_dir',     'validation')
    tools     = study_root / L.get('tools_dir',     'tools')
    docs      = study_root / L.get('docs_dir',      'docs')
    proj, stud = infer_project_study_from_path(study_root)
    return StudyPaths(study_root, prod_sdtm, prod_adam, prod_tfl, valid, tools, docs, proj, stud)
