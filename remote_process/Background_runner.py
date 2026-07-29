"""
Lance fetch_postal_provinces.py comme process détaché, pour pouvoir
continuer à travailler dans le notebook pendant qu'il tourne.

Usage dans un notebook :

    from background_runner import start, status, tail_log, stop

    start()                 # lance le script en arrière-plan
    status()                # combien de pays traités / restants
    tail_log()               # dernières lignes du log
    stop()                   # arrête le process si besoin

Le process est détaché de la session Jupyter (start_new_session=True),
donc il continue même si tu redémarres le kernel. Il ne s'arrête que
si tu le stoppes explicitement ou si la machine redémarre.
"""

import os
import signal
import subprocess
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
SCRIPT_PATH = SCRIPT_DIR / "fetch_postal_provinces.py"
LOG_PATH = SCRIPT_DIR / "fetch_postal_provinces.log"
PID_PATH = SCRIPT_DIR / "fetch_postal_provinces.pid"


def start():
    """Lance le script en arrière-plan. Ne fait rien s'il tourne déjà."""
    if is_running():
        print(f"Déjà en cours d'exécution (PID {_read_pid()}). Utilise stop() d'abord si besoin.")
        return

    log_file = open(LOG_PATH, "w", encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, str(SCRIPT_PATH)],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        cwd=str(SCRIPT_DIR),
        start_new_session=True,   # détache du process Jupyter/notebook
    )
    PID_PATH.write_text(str(process.pid))
    print(f"Lancé en arrière-plan (PID {process.pid}). Log : {LOG_PATH}")


def is_running() -> bool:
    pid = _read_pid()
    if pid is None:
        return False
    try:
        os.kill(pid, 0)  # signal 0 = juste vérifier l'existence, ne tue rien
        return True
    except OSError:
        return False


def _read_pid():
    if not PID_PATH.exists():
        return None
    try:
        return int(PID_PATH.read_text().strip())
    except ValueError:
        return None


def stop():
    """Arrête le process en arrière-plan s'il tourne."""
    pid = _read_pid()
    if pid is None or not is_running():
        print("Aucun process en cours.")
        return
    os.kill(pid, signal.SIGTERM)
    PID_PATH.unlink(missing_ok=True)
    print(f"Process {pid} arrêté.")


def tail_log(n: int = 20):
    """Affiche les n dernières lignes du log."""
    if not LOG_PATH.exists():
        print("Pas encore de log.")
        return
    lines = LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
    print("\n".join(lines[-n:]))


def status(output_dir: str | Path = None, total_countries: int = None):
    """
    Affiche l'état d'avancement : running/stopped + nombre de pays traités.
    Si output_dir n'est pas fourni, tente de le déduire du script (OUTPUT_DIR).
    """
    running = is_running()
    print(f"En cours : {running}" + (f" (PID {_read_pid()})" if running else ""))

    if output_dir is None:
        # import paresseux du script pour récupérer sa config sans le relancer
        sys.path.insert(0, str(SCRIPT_DIR))
        import fetch_postal_provinces as fpp
        output_dir = fpp.OUTPUT_DIR
        total_countries = total_countries or len(fpp.COUNTRIES)

    output_dir = Path(output_dir)
    done = [p for p in output_dir.glob("*.csv") if p.stem != "all_countries"] if output_dir.exists() else []
    print(f"Pays traités : {len(done)}" + (f" / {total_countries}" if total_countries else ""))
    tail_log(5)


