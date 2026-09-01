import pandas as pd, ast, json, glob, os
from paths import PATH_HARVEST
PATH_PERSONS = f"{PATH_HARVEST}persons/"


def load_checkpoint():

    dos = f"{PATH_PERSONS}checkpoint"

    # Recherche de tous les fichiers se terminant par "checkpoint.csv"
    fichiers = glob.glob(os.path.join(dos, "*checkpoint.csv"))

    print(f"{len(fichiers)} fichier(s) trouvé(s) :")
    for f in fichiers:
        print(" -", f)

    # Chargement et fusion
    dfs = []
    for f in fichiers:
        df = pd.read_csv(f)
        df["source_fichier"] = os.path.basename(f)  # optionnel : traçabilité
        dfs.append(df)

    df_final = pd.concat(dfs, ignore_index=True)

    print(f"\nDataFrame fusionné : {df_final.shape[0]} lignes, {df_final.shape[1]} colonnes")

    return df_final

def parse_safe(x):
    if not isinstance(x, str) or x.strip() == '':
        return None
    try:
        return json.loads(x)
    except json.JSONDecodeError:
        return None


def convertir_dates(df, colonnes):
    for col in colonnes:
        s = df[col].astype("string").str.strip()

        # On initialise la colonne avec NaT
        df[col] = pd.NaT

        # YYYY-MM-DD → on conserve année + mois,
        # donc même une date invalide comme 2016-09-31 devient 2016-09-01
        mask_ymd = s.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
        df.loc[mask_ymd, col] = s[mask_ymd].str[:7] + "-01"

        # YYYY-MM → premier jour du mois
        mask_ym = s.str.match(r"^\d{4}-\d{2}$", na=False)
        df.loc[mask_ym, col] = s[mask_ym] + "-01"

        # YYYY → premier jour de l'année
        mask_y = s.str.match(r"^\d{4}$", na=False)
        df.loc[mask_y, col] = s[mask_y] + "-01-01"

        # Conversion en datetime
        df[col] = pd.to_datetime(df[col], errors="coerce")

    return df


orc_raw = load_checkpoint()
print("Shape brute :", orc_raw.shape)


# 2. Parser employments_detail en JSON
orc_raw['employments_detail'] = orc_raw['employments_detail'].apply(parse_safe)

# 3. Exploser
orc_exploded = orc_raw.explode('employments_detail').reset_index(drop=True)
orc_exploded['employments_detail'] = orc_exploded['employments_detail'].apply(
    lambda x: x if isinstance(x, dict) else {}
)

# 4. Déplier
detail_df = pd.json_normalize(orc_exploded['employments_detail']).add_prefix('emp_')

orc_final = pd.concat(
    [orc_exploded.drop(columns='employments_detail').reset_index(drop=True), detail_df.reset_index(drop=True)],
    axis=1
).rename(columns={
    'emp_organisation': 'employer_name',
    'emp_role': 'employer_role',
    'emp_department': 'employer_department',
    'emp_start_date': 'employer_start_date',
    'emp_end_date': 'employer_end_date',
    'emp_org_city': 'employer_city',
    'emp_org_region': 'employer_region',
    'emp_org_country': 'employer_country',
    'emp_org_id': 'employer_org_id',
    'emp_org_id_source': 'employer_org_id_source',
}).drop(columns=['country_code', 'source_fichier']
).drop_duplicates()


# 5. Vérifier AVANT toute conversion de date
print(orc_final.loc[orc_final['last_name'] == 'faranda', ['employer_role', 'employer_start_date', 'employer_end_date']])
print(orc_final['employer_end_date'].dtype)

print("Avant explosion :", len(orc_raw))
print("Shape final :", orc_final.shape)
print("employer_name non-null :", orc_final['employer_name'].notna().sum())

# 1. Convertir les dates en vrai format date
colonnes_dates = [
    "employer_end_date",
    "employer_start_date",
]

orc_final = convertir_dates(orc_final, colonnes_dates)

# 7. Doublons + suppression
orc_final['is_duplicate'] = orc_final.duplicated(subset='orcid_id_final', keep=False) & orc_final['orcid_id_final'].notna()
print("Lignes en doublon :", orc_final['is_duplicate'].sum())

a_supprimer = orc_final['is_duplicate'] & (orc_final['employer_end_date'] < '2021-01-01')
print("Lignes à supprimer :", a_supprimer.sum())

orc_final = orc_final[~a_supprimer].drop(columns='is_duplicate').reset_index(drop=True)

print("Shape après :", orc_final.shape)

# vérif sur Faranda
print(orc_final.loc[orc_final['orcid_id_final'].isin(['0000-0001-6672-2944', '0000-0001-5001-5698']), 
                    [ 'orcid_id_final', 'employer_role', 'employer_start_date', 'employer_end_date']])


orc_final = orc_final[orc_final['orcid_id_final'].notna()]
orc_final = orc_final[~(orc_final['employer_name'].isnull() & (orc_final['orcid_source'] == 'fourni'))]
orc_final = orc_final[~((orc_final['orcid_source'] == 'ambigu') & orc_final['employers'].isna())]


orc_final['is_duplicate'] = orc_final.duplicated(subset='orcid_id_final', keep=False) & orc_final['orcid_id_final'].notna()
print("Lignes en doublon :", orc_final['is_duplicate'].sum())

# Priorité de fiabilité des sources : "fourni" (orcid_id donné en entrée)
# est plus fiable que "trouve" (retrouvé par nom), lui-même plus fiable
# qu'un statut "ambigu". En cas de doublon de contenu, on garde la ligne
# venant de la source la plus fiable.
source_priority = {
    "fourni": 0,
    "trouve_desambiguise": 1,
    "trouve_desambiguise_permute": 1,
    "trouve": 2,
    "trouve_permute": 2,
    "ambigu": 3,
    "ambigu_permute": 3,
    "non_trouve": 4,
}
orc_final["_source_priority"] = orc_final["orcid_source"].map(source_priority).fillna(9)

# Clé d'identité d'un EMPLOI (pas juste de la personne) : deux lignes
# avec le même orcid_id_final mais un poste/dates/employeur différents
# sont deux vrais emplois distincts, à garder toutes les deux.
employment_key = [
    "orcid_id_final", "employer_name", "employer_role", "employer_department",
    "employer_start_date", "employer_end_date", "employer_city",
    "employer_region", "employer_country", "employer_org_id", "employer_org_id_source",
]

orc_final = (
    orc_final
    .sort_values("_source_priority")
    .drop_duplicates(subset=employment_key, keep="first")
    .drop(columns="_source_priority")
    .reset_index(drop=True)
)

print("Shape après dédup provenance :", orc_final.shape)


orc_final['is_duplicate'] = orc_final.duplicated(subset='orcid_id_final', keep=False) & orc_final['orcid_id_final'].notna()
print("Lignes en doublon :", orc_final['is_duplicate'].sum())