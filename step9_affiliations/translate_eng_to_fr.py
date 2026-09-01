import pandas as pd
import argostranslate.package
import argostranslate.translate
from langdetect import detect, DetectorFactory
from tqdm import tqdm

DetectorFactory.seed = 0
tqdm.pandas()  # active .progress_apply() pour pandas

# --- 1. Télécharger et installer le modèle anglais -> français (une seule fois) ---
def installer_modele(from_code="en", to_code="fr"):
    # Vérifie si le modèle est déjà installé pour éviter de le re-télécharger
    langues_installees = argostranslate.translate.get_installed_languages()
    deja_installe = any(
        l.code == from_code and any(t.to_lang.code == to_code for t in l.translations_from)
        for l in langues_installees
    )
    if deja_installe:
        print(f"Modèle {from_code} -> {to_code} déjà installé.")
        return

    print(f"Installation du modèle {from_code} -> {to_code}...")
    argostranslate.package.update_package_index()
    packages_disponibles = argostranslate.package.get_available_packages()
    package = next(
        (p for p in packages_disponibles if p.from_code == from_code and p.to_code == to_code),
        None
    )
    if package is None:
        raise Exception(f"Aucun modèle trouvé pour {from_code} -> {to_code}")
    chemin = package.download()
    argostranslate.package.install_from_path(chemin)
    print("Installation terminée.")

installer_modele("en", "fr")

# --- 2. Fonctions de détection + traduction ---
def detecter_langue(texte):
    try:
        return detect(str(texte))
    except Exception:
        return "inconnu"

def translate_if_english(texte):
    if pd.isna(texte) or str(texte).strip() == "":
        return texte
    try:
        if detecter_langue(texte) == "en":
            return argostranslate.translate.translate(str(texte), "en", "fr")
        else:
            return texte
    except Exception as e:
        print(f"Erreur sur : {texte} -> {e}")
        return texte