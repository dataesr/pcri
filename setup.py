# setup.py
#
# A lancer une seule fois, juste apres avoir clone le projet :
#     python setup.py
#
# Ce script cree config_local.py de maniere interactive et verifie
# que la structure de dossiers de donnees existe.

import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parent
TEMPLATE = ROOT / "config_template.py"
LOCAL = ROOT / "config_local.py"


def main():
    print("=== Installation du projet ===\n")

    if LOCAL.exists():
        print(f"'{LOCAL.name}' existe deja, rien a faire.")
    else:
        if not TEMPLATE.exists():
            print(f"[ERREUR] '{TEMPLATE.name}' introuvable. Verifie que tu es "
                  f"bien a la racine du projet.")
            return

        print("Ou se trouve le dossier racine des donnees (PCRI) sur ta machine ?")
        print(r"Exemple Windows : C:/Users/toto/OneDrive/PCRI/")
        print(r"Exemple Mac/Linux : /Users/toto/OneDrive/PCRI/")
        user_path = input("> Chemin : ").strip()

        if not user_path:
            print("Chemin vide, installation annulee.")
            return

        if not user_path.endswith("/"):
            user_path += "/"

        shutil.copy(TEMPLATE, LOCAL)
        content = LOCAL.read_text(encoding="utf-8")
        content = content.replace('PATH = "CHANGE_ME"', f'PATH = "{user_path}"')
        LOCAL.write_text(content, encoding="utf-8")

        print(f"\n'{LOCAL.name}' cree avec PATH = {user_path}")

    # Import differe pour que config_local existe deja au moment de l'import
    import sys
    sys.path.insert(0, str(ROOT))
    from paths import ensure_folders_exist, check_data_present

    print("\nVerification / creation des dossiers...")
    ensure_folders_exist()
    check_data_present()

    print("\n=== Installation terminee ===")
    print("Pense a synchroniser/copier les donnees dans le dossier "
          "'eCorda_data' si ce n'est pas deja fait (OneDrive, disque partage, etc.)")


if __name__ == "__main__":
    main()