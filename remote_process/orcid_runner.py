"""
Wrapper de generic_runner pour l'enrichissement ORCID : fixe le chemin
du worker, et expose une API simple côté notebook.

Usage :

    from remote_process.orcid_runner import start, status, tail_log, stop

    start(
        df,
        first_name_col="first_name",
        last_name_col="last_name",
        orcid_col="orcid_id",
        # checkpoint_path est optionnel : par défaut, écrit hors du
        # projet dans EXTERNAL_DATA_DIR (voir plus bas)
        # label est un raccourci pratique pour distinguer plusieurs
        # jeux de données sans construire le chemin à la main :
        #   start(df, label="ERC") -> erc_orcid_checkpoint.csv
        # label identifie aussi le job auprès de generic_runner (son
        # propre .pid/.log), ce qui permet de lancer plusieurs labels
        # en parallèle et de les stopper/consulter indépendamment.
    )

    status()              # dernier job utilisé dans cette session
    status(label="ERC")   # job identifié par ce label
    tail_log(label="ERC")
    stop(label="ERC")
    is_running(label="ERC")

Note rétrocompatibilité : un job lancé par une ancienne version de ce
fichier (avant l'ajout de label au nom de job) a été enregistré sous le
nom de job fixe "orcid", quel que soit le label utilisé pour son
checkpoint. Pour stopper/consulter un tel job "historique", utiliser les
fonctions SANS argument label (elles retombent sur le nom de job
"orcid" par défaut). Les jobs lancés avec cette version, eux, sont
enregistrés sous "orcid_{label}".
"""

import sys
from pathlib import Path
from paths import PATH_HARVEST
import pandas as pd

# --------------------------------------------------------------------
# Emplacements, avec pathlib
# --------------------------------------------------------------------
# Ce fichier vit dans pcri/remote_process/, donc :
#   SCRIPT_DIR    = .../pcri/remote_process
#   PROJECT_ROOT  = .../pcri                (un cran au-dessus)
#   OUTSIDE_ROOT  = .../                    (le parent de pcri -> hors projet)
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
OUTSIDE_ROOT = PROJECT_ROOT.parent

# Dossier de résultats HORS du projet, à côté de pcri/ (pas versionné,
# pas mélangé avec le code). Change ce nom si tu préfères autre chose,
# ou remplace carrément la ligne par un chemin absolu fixe, par ex. :
#   EXTERNAL_DATA_DIR = Path("D:/data_pcri")   (Windows)
#   EXTERNAL_DATA_DIR = Path("/home/toi/data_pcri")  (Linux/Mac)
EXTERNAL_DATA_DIR = OUTSIDE_ROOT / f"{PATH_HARVEST}persons/"
EXTERNAL_DATA_DIR.mkdir(parents=True, exist_ok=True)  # créé si besoin, ne plante pas si existe déjà

CHECKPOINT_DIR = EXTERNAL_DATA_DIR / "checkpoint"
DEFAULT_CHECKPOINT_PATH = CHECKPOINT_DIR / "orcid_checkpoint.csv"

# generic_runner est dans le même dossier (remote_process/) : import direct
sys.path.insert(0, str(SCRIPT_DIR))
import generic_runner as gr  # noqa: E402

BASE_JOB_NAME = "orcid"
WORKER_PATH = SCRIPT_DIR / "orcid_worker.py"


def _safe_label(label):
    return str(label).strip().lower().replace(" ", "_") if label else None


def _resolve_job_name(label=None):
    """
    "orcid" si pas de label (comportement historique, un seul job à la
    fois), sinon "orcid_{label}" pour permettre plusieurs jobs en
    parallèle avec un suivi (.pid/.log) indépendant.
    """
    safe = _safe_label(label)
    return f"{BASE_JOB_NAME}_{safe}" if safe else BASE_JOB_NAME


def _resolve_checkpoint_path(checkpoint_path=None, label=None):
    """
    Priorité :
      1. checkpoint_path si fourni explicitement (chemin complet)
      2. label si fourni -> "{label}_orcid_checkpoint.csv" dans CHECKPOINT_DIR
      3. DEFAULT_CHECKPOINT_PATH
    """
    if checkpoint_path:
        return Path(checkpoint_path)
    safe = _safe_label(label)
    if safe:
        return CHECKPOINT_DIR / f"{safe}_orcid_checkpoint.csv"
    return DEFAULT_CHECKPOINT_PATH


def start(df, first_name_col="first_name", last_name_col="last_name",
          orcid_col="orcid_id",
          checkpoint_path=None, label=None, checkpoint_every=100,
          pause=0.2, mongo=None):
    """
    checkpoint_path : chemin complet du CSV de reprise/résultat, prioritaire
                       sur label si les deux sont fournis.
    label : raccourci pour nommer le checkpoint sans construire le chemin
            à la main, ex. label="ERC" -> "erc_orcid_checkpoint.csv".
            Sert aussi à identifier le job auprès de generic_runner
            (permet de lancer plusieurs labels en parallèle). Ignoré
            pour le nommage du checkpoint si checkpoint_path est fourni,
            mais reste utilisé pour identifier le job même dans ce cas.
    """
    job_name = _resolve_job_name(label)
    checkpoint_path = _resolve_checkpoint_path(checkpoint_path, label)
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    payload = {
        "df": df,
        "first_name_col": first_name_col,
        "last_name_col": last_name_col,
        "orcid_col": orcid_col,
        "checkpoint_path": str(checkpoint_path),
        "checkpoint_every": checkpoint_every,
        "pause": pause,
        "mongo": mongo,
    }
    gr.start(job_name, str(WORKER_PATH), payload=payload, script_dir=str(SCRIPT_DIR))

    global _last_checkpoint_path, _last_job_name
    _last_checkpoint_path = str(checkpoint_path)
    _last_job_name = job_name


def stop(label: str = None):
    """
    Sans argument : stoppe le dernier job connu dans cette session, ou
    le job historique "orcid" par défaut. Passer label pour cibler un
    job précis (ex: label="ERC" stoppe le job lancé avec ce label).
    """
    job_name = _resolve_job_name(label) if label else _last_job_name
    gr.stop(job_name, script_dir=str(SCRIPT_DIR))


def tail_log(n: int = 20, label: str = None):
    job_name = _resolve_job_name(label) if label else _last_job_name
    gr.tail_log(job_name, n=n, script_dir=str(SCRIPT_DIR))


def is_running(label: str = None) -> bool:
    job_name = _resolve_job_name(label) if label else _last_job_name
    return gr.is_running(job_name, script_dir=str(SCRIPT_DIR))


_last_checkpoint_path = str(DEFAULT_CHECKPOINT_PATH)
_last_job_name = BASE_JOB_NAME


def status(checkpoint_path: str = None, label: str = None):
    """
    Sans argument : affiche le statut du dernier job/checkpoint utilisé
    par start() dans cette session. Passer checkpoint_path ou label pour
    consulter un autre job explicitement.
    """
    if checkpoint_path or label:
        job_name = _resolve_job_name(label)
        resolved_checkpoint_path = str(_resolve_checkpoint_path(checkpoint_path, label))
    else:
        job_name = _last_job_name
        resolved_checkpoint_path = _last_checkpoint_path

    def _orcid_status(script_dir):
        if resolved_checkpoint_path and Path(resolved_checkpoint_path).exists():
            cdf = pd.read_csv(resolved_checkpoint_path)
            total = len(cdf)
            done = cdf["orcid_source"].notna().sum()
            print(f"Checkpoint : {resolved_checkpoint_path}")
            print(f"Lignes traitées : {done} / {total}")
            if done > 0:
                print(cdf["orcid_source"].value_counts(dropna=True).to_string())
        else:
            print(f"Pas encore de checkpoint trouvé ({resolved_checkpoint_path}).")

    gr.status(job_name, script_dir=str(SCRIPT_DIR), extra_status_fn=_orcid_status)


def list_jobs():
    """Liste tous les jobs orcid connus (tous labels confondus)."""
    gr.list_jobs(script_dir=str(SCRIPT_DIR))