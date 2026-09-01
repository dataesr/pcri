"""
Runner générique pour lancer un script Python en arrière-plan (process
détaché du notebook/kernel), avec suivi via PID + log, et payload
optionnel transmis par pickle.

Chaque "job" est identifié par un nom (job_name). Les fichiers de suivi
sont nommés d'après ce nom, ce qui permet de faire tourner plusieurs
jobs en parallèle sans collision :

    {script_dir}/{job_name}.pid
    {script_dir}/{job_name}.log
    {script_dir}/{job_name}_payload.pkl   (si payload fourni)

Usage direct :

    import generic_runner as gr

    gr.start("orcid", "/path/orcid_worker.py", payload={"df": df, ...})
    gr.status("orcid")
    gr.tail_log("orcid")
    gr.stop("orcid")
    gr.list_jobs()

Usage recommandé : faire un petit wrapper par job (voir orcid_runner.py)
qui fixe worker_path et le nom, pour garder une API "start(df, ...)"
simple côté notebook plutôt que de manipuler des noms de jobs à la main.

Note Windows : la vérification "le process existe-t-il encore" et
l'arrêt du process passent par psutil plutôt que par os.kill/signal.
os.kill(pid, 0) est une astuce Unix (signal 0 = no-op, sert juste à
tester l'existence du process) qui n'est pas fiable sous Windows : le
module os y route les signaux vers TerminateProcess et peut échouer
silencieusement ou lever une exception selon le contexte. psutil gère
ça correctement sur les deux OS.
"""

import pickle
import subprocess
import sys
from pathlib import Path

import psutil


def _paths(job_name: str, script_dir: Path):
    return {
        "pid": script_dir / f"{job_name}.pid",
        "log": script_dir / f"{job_name}.log",
        "payload": script_dir / f"{job_name}_payload.pkl",
    }


def start(job_name: str, worker_path: str, payload: dict = None,
          script_dir: str = None, args: list = None):
    """
    Lance worker_path en arrière-plan sous l'identité job_name.

    - payload : dict optionnel, sérialisé en pickle et déposé à côté du
      worker ; charge-le toi-même dans le worker via
      pickle.load(open(f"{job_name}_payload.pkl", "rb")).
    - args : arguments supplémentaires passés en ligne de commande au
      worker (ex: le nom du job, pour qu'il retrouve son payload).
    - script_dir : dossier où écrire pid/log/payload et cwd du process.
      Par défaut, le dossier du worker_path.
    """
    worker_path = Path(worker_path).resolve()
    script_dir = Path(script_dir).resolve() if script_dir else worker_path.parent
    p = _paths(job_name, script_dir)

    if is_running(job_name, script_dir):
        print(f"[{job_name}] Déjà en cours d'exécution (PID {_read_pid(p)}). "
              f"Utilise stop('{job_name}') d'abord si besoin.")
        return

    if payload is not None:
        with open(p["payload"], "wb") as f:
            pickle.dump(payload, f)

    cmd = [sys.executable, str(worker_path), "--job-name", job_name]
    if args:
        cmd += args

    log_file = open(p["log"], "w", encoding="utf-8")

    popen_kwargs = dict(
        stdout=log_file,
        stderr=subprocess.STDOUT,
        cwd=str(script_dir),
    )
    if sys.platform == "win32":
        # Détache le process du notebook/console sous Windows : sans ça,
        # fermer/arrêter le kernel Jupyter peut tuer le sous-process, et
        # start_new_session (POSIX) n'existe pas sous Windows.
        popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
    else:
        popen_kwargs["start_new_session"] = True  # détache du process Jupyter/notebook (POSIX)

    process = subprocess.Popen(cmd, **popen_kwargs)
    p["pid"].write_text(str(process.pid))
    print(f"[{job_name}] Lancé en arrière-plan (PID {process.pid}). Log : {p['log']}")


def is_running(job_name: str, script_dir: str = None) -> bool:
    script_dir = Path(script_dir).resolve() if script_dir else Path(".").resolve()
    p = _paths(job_name, script_dir)
    pid = _read_pid(p)
    if pid is None:
        return False
    return psutil.pid_exists(pid)


def _read_pid(p):
    if not p["pid"].exists():
        return None
    try:
        return int(p["pid"].read_text().strip())
    except ValueError:
        return None


def stop(job_name: str, script_dir: str = None):
    """Arrête le job s'il tourne."""
    script_dir = Path(script_dir).resolve() if script_dir else Path(".").resolve()
    p = _paths(job_name, script_dir)
    pid = _read_pid(p)
    if pid is None or not is_running(job_name, script_dir):
        print(f"[{job_name}] Aucun process en cours.")
        return
    try:
        proc = psutil.Process(pid)
        proc.terminate()  # équivalent SIGTERM sous Unix, TerminateProcess sous Windows
        try:
            proc.wait(timeout=5)
        except psutil.TimeoutExpired:
            proc.kill()  # forcer l'arrêt si le process ne répond pas
    except psutil.NoSuchProcess:
        pass
    p["pid"].unlink(missing_ok=True)
    print(f"[{job_name}] Process {pid} arrêté.")


def tail_log(job_name: str, n: int = 20, script_dir: str = None):
    """Affiche les n dernières lignes du log."""
    script_dir = Path(script_dir).resolve() if script_dir else Path(".").resolve()
    p = _paths(job_name, script_dir)
    if not p["log"].exists():
        print(f"[{job_name}] Pas encore de log.")
        return
    lines = p["log"].read_text(encoding="utf-8", errors="replace").splitlines()
    print("\n".join(lines[-n:]))


def status(job_name: str, script_dir: str = None, extra_status_fn=None):
    """
    Affiche running/stopped + queue le log.
    extra_status_fn(script_dir) : callback optionnel pour afficher un
    état métier (ex: nb de lignes traitées dans un checkpoint CSV).
    """
    script_dir = Path(script_dir).resolve() if script_dir else Path(".").resolve()
    running = is_running(job_name, script_dir)
    p = _paths(job_name, script_dir)
    print(f"[{job_name}] En cours : {running}" +
          (f" (PID {_read_pid(p)})" if running else ""))

    if extra_status_fn:
        extra_status_fn(script_dir)

    tail_log(job_name, 5, script_dir)


def list_jobs(script_dir: str = None):
    """Liste les jobs connus (ayant un .pid ou .log) dans script_dir."""
    script_dir = Path(script_dir).resolve() if script_dir else Path(".").resolve()
    names = sorted({
        f.stem for f in script_dir.glob("*.log")
    } | {
        f.stem for f in script_dir.glob("*.pid")
    })
    if not names:
        print("Aucun job trouvé dans", script_dir)
        return
    for name in names:
        p = _paths(name, script_dir)
        running = is_running(name, script_dir)
        print(f"- {name}: {'en cours (PID ' + str(_read_pid(p)) + ')' if running else 'arrêté'}")