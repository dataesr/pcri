# paths.py
#
# Point d'entree UNIQUE pour tous les chemins du projet.
# Tous les autres scripts doivent faire : from paths import PATH_REF, PATH_SOURCE, ...
# (au lieu de redefinir PATH_DATA / PATH_REF / etc. eux-memes)

from pathlib import Path

try:
    from config_local import PATH
except ImportError as exc:
    raise RuntimeError(
        "\n\n[ERREUR CONFIGURATION]\n"
        "Le fichier 'config_local.py' est introuvable.\n\n"
        "Marche a suivre :\n"
        "  1. Copie 'config_template.py' en 'config_local.py'\n"
        "     (a la racine du projet, au meme niveau que ce fichier)\n"
        "  2. Ouvre config_local.py et renseigne ton propre chemin PATH\n"
        "  3. Relance le script\n\n"
        "Astuce : tu peux aussi lancer 'python setup.py' qui fait cette\n"
        "etape pour toi de maniere interactive.\n"
    ) from exc

if PATH == "CHANGE_ME":
    raise RuntimeError(
        "Le fichier config_local.py existe mais PATH n'a pas ete renseigne. "
        "Ouvre config_local.py et remplace 'CHANGE_ME' par ton chemin reel."
    )

# Normalisation : on s'assure que PATH se termine par un separateur
PATH = str(Path(PATH)) + "/"

PATH_DATA = f"{PATH}eCorda_data/"
PATH_REF = f"{PATH_DATA}data_reference/"
PATH_SOURCE = f"{PATH_DATA}data_load/"
PATH_WP = f"{PATH_DATA}data_wp/"
PATH_WORK = f"{PATH_DATA}data_work/"
PATH_CLEAN = f"{PATH_DATA}data_clean/"
PATH_CONNECT = f"{PATH_DATA}connection_to_tableau/"
PATH_ODS = f"{PATH_DATA}data_ods/"
PATH_ORG = f"{PATH_DATA}data_orga/"
PATH_HARVEST = f"{PATH_DATA}data_harvest/"
PATH_MATCH = f"{PATH_DATA}data_for_matching/"
PATH_UNIT = f"{PATH_DATA}data_unit/"

_ALL_PATHS = [
    PATH_DATA, PATH_REF, PATH_SOURCE, PATH_WP, PATH_WORK,
    PATH_CLEAN, PATH_CONNECT, PATH_ODS, PATH_ORG,
    PATH_HARVEST, PATH_MATCH, PATH_UNIT,
]


def ensure_folders_exist():
    """Cree les dossiers manquants (utile a la premiere installation)."""
    for p in _ALL_PATHS:
        Path(p).mkdir(parents=True, exist_ok=True)


def check_data_present():
    """Avertit si un dossier attendu est vide ou absent (donnees pas encore copiees)."""
    missing = [p for p in _ALL_PATHS if not Path(p).exists() or not any(Path(p).iterdir())]
    if missing:
        print("[ATTENTION] Les dossiers suivants sont vides ou absents :")
        for m in missing:
            print(f"   - {m}")
        print("Verifie que les donnees ont bien ete copiees/synchronisees a cet emplacement.")