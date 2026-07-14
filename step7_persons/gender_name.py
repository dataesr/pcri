"""
Estimation du genre (homme/femme) pour une liste mondiale de prénoms.

Approche hybride :
1. Source principale : 'gender-guesser' -> base de données onomastique
   couvrant des prénoms de nombreux pays / langues.
2. Filet de secours (heuristique) : pour les prénoms non reconnus,
   estimation approximative basée sur des terminaisons statistiquement
   associées à un genre dans plusieurs langues (ex: "-a", "-ia" -> féminin
   dans beaucoup de langues latines/slaves ; certaines consonnes finales
   -> masculin plus fréquent). Cette partie est indicative, pas fiable
   à 100%, et signalée comme telle dans le résultat.

Installation requise :
    pip install gender-guesser
"""
import gender_guesser.detector as gender

# ---------------------------------------------------------------------
# 1) Source principale : gender-guesser
# ---------------------------------------------------------------------

# Résultat brut -> proba
TRADUCTION = {
    "male":          ("male",   0.99, "high"),
    "female":        ("female",   0.99, "high"),
    "mostly_male":   ("male",   0.75, "mid"),
    "mostly_female": ("female",   0.75, "mid"),
    "andy":          ("andy",   0.50, "unisex"),
    "unknown":       (None, None, None),  # déclenche le fallback heuristique
}

# ---------------------------------------------------------------------
# 2) Filet de secours heuristique (terminaisons courantes, plusieurs langues)
#    Utilisé UNIQUEMENT si gender-guesser ne connaît pas le prénom.
# ---------------------------------------------------------------------

TERMINAISONS_FEMININES = (
    "a", "ia", "ah", "ina", "ella", "etta", "een", "ine", "elle",
    "ana", "yn", "lyn", "issa", "iyah", "unn",
)
TERMINAISONS_MASCULINES = (
    "o", "os", "us", "an", "on", "el", "ar", "in", "im", "ov",
    "iy", "ir", "old", "olf", "ald", "ert",
)


def heuristique_terminaison(name):
    p = name.lower()
    for suf in sorted(TERMINAISONS_FEMININES, key=len, reverse=True):
        if p.endswith(suf):
            return ("female", 0.55, "mid (heuristique)")
    for suf in sorted(TERMINAISONS_MASCULINES, key=len, reverse=True):
        if p.endswith(suf):
            return ("male", 0.55, "mid (heuristique)")
    return ("unknown", None, None)


def gender_by_first_name(names):
    d = gender.Detector(case_sensitive=False)
    resultats = []
    for name_original in names:
        name = name_original.strip()
        if not name:
            continue

        genre_brut = d.get_gender(name.capitalize())
        label, proba, fiability = TRADUCTION.get(genre_brut, (None, None, None))

        if label is None:
            # fallback heuristique
            label, proba, fiability = heuristique_terminaison(name)

        resultats.append({
            "first_name": name_original,
            "gender": label,
            "probability": proba,
            "fiability": fiability
        })
    return resultats


def afficher_resultats(resultats):
    largeur = max(len(r["first_name"]) for r in resultats) + 2
    print(f"{'first_name'.ljust(largeur)} | gender | probability | fiability")
    print("-" * (largeur + 65))
    for r in resultats:
        proba_str = f"{int(r['probability']*100)}%" if r["probability"] is not None else "N/A"
        print(f"{r['first_name'].ljust(largeur)} | {r['gender'].ljust(8)} | {proba_str.ljust(11)} | {r['fiability'].ljust(24)} ")


# def exporter_csv(resultats, chemin="resultats_genre.csv"):
#     with open(chemin, "w", newline="", encoding="utf-8") as f:
#         writer = csv.DictWriter(f, fieldnames=["prenom", "genre", "probabilite", "fiabilite", "source"])
#         writer.writeheader()
#         writer.writerows(resultats)
#     print(f"\nRésultats exportés dans : {chemin}")


# if __name__ == "__main__":
#     # === Option 1 : liste codée en dur ci-dessous ===
#     liste_prenoms = [
#         "Camille", "Antoine", "Léa", "Dominique", "Nathan", "Marie", "Claude",
#         "Yuki", "Mohammed", "Fatima", "Olga", "Ravi", "Priya", "Kwame",
#         "Xiomara", "Sven", "Ingrid", "Chen", "Amara", "Diego",
#     ]

#     # === Option 2 : lire depuis un fichier texte (un prénom par ligne) ===
#     # Décommente ces lignes et adapte le chemin si tu as un fichier :
#     # with open("mes_prenoms.txt", encoding="utf-8") as f:
#     #     liste_prenoms = [ligne.strip() for ligne in f if ligne.strip()]

#     resultats = predire_genre(liste_prenoms)
#     afficher_resultats(resultats)
#     exporter_csv(resultats)
