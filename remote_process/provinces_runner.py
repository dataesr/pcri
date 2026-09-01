"""
Wrapper pour lancer fetch_postal_provinces.py via generic_runner.py,
avec une API simple côté notebook (pas besoin de manipuler job_name
ou le chemin du script à chaque appel).

fetch_postal_provinces.py est autonome (toute sa config est en dur
dans le fichier : COUNTRIES, PATH_HARVEST...), donc pas besoin de
payload ici, contrairement à des jobs qui reçoivent un DataFrame par
exemple.

Usage dans un notebook :

    from postal_provinces_runner import start, status, tail_log, stop

    start()
    status()
    tail_log()
    stop()
"""

import sys
from pathlib import Path

import generic_runner as gr

JOB_NAME = "postal_provinces"
SCRIPT_DIR = Path(__file__).resolve().parent
WORKER_PATH = SCRIPT_DIR / "fetch_postal_provinces.py"


def start():
    gr.start(JOB_NAME, str(WORKER_PATH), script_dir=str(SCRIPT_DIR))


def status():
    gr.status(JOB_NAME, script_dir=str(SCRIPT_DIR), extra_status_fn=_extra_status)


def tail_log(n: int = 20):
    gr.tail_log(JOB_NAME, n=n, script_dir=str(SCRIPT_DIR))


def stop():
    gr.stop(JOB_NAME, script_dir=str(SCRIPT_DIR))


def _extra_status(script_dir: Path):
    """Affiche en plus l'avancement métier : nb de pays traités / total."""
    sys.path.insert(0, str(script_dir))
    import fetch_postal_provinces as fpp  # import paresseux, ne relance rien

    output_dir = Path(fpp.OUTPUT_DIR)
    total = len(fpp.COUNTRIES)
    done = [p for p in output_dir.glob("*.csv") if p.stem != "all_countries"] if output_dir.exists() else []
    print(f"Pays traités : {len(done)} / {total}")