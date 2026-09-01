"""
Rafraîchit une collection MongoDB en arrière-plan (delete + insert),
pour continuer à travailler dans le notebook pendant que ça tourne.

Usage dans un notebook :

    from remote_process.mongo_runner import refresh_mongo_collection, status, tail_log, stop

    future = refresh_mongo_collection('horizon', df, cols_select_xls, 'projects-entities')

    status()        # état du process
    tail_log()      # dernières lignes du log
    stop()          # arrête si besoin
    future.result() # attend la fin et lève une exception si erreur
"""

import os
import signal
import subprocess
import sys
import pickle
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

from remote_process.mongo import mongo_delete_all, mongo_bulk_insert_df

SCRIPT_DIR   = Path(__file__).resolve().parent
LOG_PATH     = SCRIPT_DIR / "mongo_insert.log"
PID_PATH     = SCRIPT_DIR / "mongo_insert.pid"
PAYLOAD_PATH = SCRIPT_DIR / "mongo_insert.pkl"


def refresh_mongo_collection(FP, df, cols_select_xls, tab_mongo):
    """Delete synchrone + insert en arrière-plan."""
    from functions_shared import cols_select_mongo

    tmp = df[cols_select_mongo(FP, cols_select_xls)] if cols_select_xls is not None else df

    cm = f"european-projects_{tab_mongo}"
    mongo_delete_all(cm)  # ← synchrone, on attend que ce soit fini

    # Insert en background
    executor = ThreadPoolExecutor(max_workers=1)
    future = executor.submit(mongo_bulk_insert_df, tmp, cm)
    executor.shutdown(wait=False)

    return future


def mongo_start(FP: str, df, cols_select_xls, tab_mongo: str):
    """Lance refresh_mongo_collection comme process détaché du notebook."""
    if mongo_is_running():
        print(f"Déjà en cours (PID {_read_pid()}). Utilise stop() d'abord si besoin.")
        return None

    with open(PAYLOAD_PATH, "wb") as f:
        pickle.dump({"FP": FP, "df": df, "cols_select_xls": cols_select_xls, "tab_mongo": tab_mongo}, f)

    log_file = open(LOG_PATH, "w", encoding="utf-8")
    process = subprocess.Popen(
        [sys.executable, str(SCRIPT_DIR / "mongo_worker.py")],
        stdout=log_file,
        stderr=subprocess.STDOUT,
        cwd=str(SCRIPT_DIR),
        start_new_session=True,
    )
    PID_PATH.write_text(str(process.pid))
    print(f"Insertion lancée en arrière-plan (PID {process.pid}). Log : {LOG_PATH}")


def mongo_is_running() -> bool:
    pid = _read_pid()
    if pid is None:
        return False
    try:
        os.kill(pid, 0)
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


def mongo_stop():
    pid = _read_pid()
    if pid is None or not mongo_is_running():
        print("Aucun process en cours.")
        return
    os.kill(pid, signal.SIGTERM)
    PID_PATH.unlink(missing_ok=True)
    print(f"Process {pid} arrêté.")


def mongo_tail_log(n: int = 20):
    if not LOG_PATH.exists():
        print("Pas encore de log.")
        return
    lines = LOG_PATH.read_text(encoding="utf-8", errors="replace").splitlines()
    print("\n".join(lines[-n:]))


def mongo_status():
    running = mongo_is_running()
    print(f"En cours : {running}" + (f" (PID {_read_pid()})" if running else " (terminé)"))
    mongo_tail_log(5)