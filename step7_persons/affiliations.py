import pandas as pd, json, glob, os
from paths import PATH_CLEAN, PATH_HARVEST
from functions_shared import country_iso_shift
PATH_PERSONS=f"{PATH_HARVEST}persons/"


def persons_choose(action_choose=None):

    perso = pd.read_pickle(f"{PATH_CLEAN}persons_all.pkl")

    # Si une action est spécifiée, on filtre dessus
    action_filter = (
        perso.action_code.isin(action_choose)
        if action_choose is not None
        else True
    )

    perso = perso.loc[
        (perso.reason != "main_contact_non_justifie")
        & ~(perso.last_name.isna() & perso.orcid_id.isna())
        & action_filter
        & perso.numero_national_de_structure.isna()
        & (
            # ERC / MSCA
            (
                perso.action_code.isin(["ERC", "MSCA"])
                & (
                    (perso.country_code == "FRA")
                    | (perso.nationality_country_code == "FRA")
                )
            )
            |
            # Autres actions
            (
                ~perso.action_code.isin(["ERC", "MSCA"])
                & (perso.country_code == "FRA")
                & perso.operateur_num.notna()
            )
        )
    ]


    print(
        f"size tab: {len(perso)}, "
        f"info sur tab without orcid: {perso.orcid_id.isna().sum()}"
    )

    return perso


def load_checkpoint():
    """
    Charge uniquement les checkpoints ORCID (nom de fichier contenant
    "orcid" et se terminant par "checkpoint.csv"), pour éviter de charger
    par erreur les checkpoints IdRef (ex: "all_idref_checkpoint.csv")
    qui vivent désormais dans le même dossier.
    """
    dos = f"{PATH_PERSONS}checkpoint"

    # glob est sensible à la casse sous Linux -> on prend tous les
    # "*checkpoint.csv" puis on filtre nous-mêmes sur "orcid" en
    # minuscule, insensible à la casse.
    tous_fichiers = glob.glob(os.path.join(dos, "*checkpoint.csv"))
    fichiers = [f for f in tous_fichiers if "orcid" in os.path.basename(f).lower()]

    ignores = set(tous_fichiers) - set(fichiers)
    if ignores:
        print(f"{len(ignores)} fichier(s) checkpoint ignoré(s) (pas de 'orcid' dans le nom) :")
        for f in sorted(ignores):
            print("  -", f)

    print(f"{len(fichiers)} fichier(s) checkpoint ORCID trouvé(s) :")
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

def orcid_back():

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
    # print(orc_final.loc[orc_final['last_name'] == 'faranda', ['employer_role', 'employer_start_date', 'employer_end_date']])
    # print(orc_final['employer_end_date'].dtype)

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

    # # vérif sur Faranda
    # print(orc_final.loc[orc_final['orcid_id_final'].isin(['0000-0001-6672-2944', '0000-0001-5001-5698']), 
    #                     [ 'orcid_id_final', 'employer_role', 'employer_start_date', 'employer_end_date']])


    orc_final = orc_final[orc_final['orcid_id_final'].notna()]
    orc_final = orc_final[~(orc_final['employer_name'].isnull() & (orc_final['orcid_source'] == 'fourni'))]
    orc_final = orc_final[~((orc_final['orcid_source'].str.startswith('ambigu')) & orc_final['employers'].isna())]


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

    print("Shape après dédup provenance :", orc_final.shape,  "orcid_source :", orc_final.orcid_source.value_counts(dropna=False))


    orc_final['is_duplicate'] = orc_final.duplicated(subset='orcid_id_final', keep=False) & orc_final['orcid_id_final'].notna()
    print("Lignes en doublon :", orc_final['is_duplicate'].sum())

    return orc_final


def _clean_name(df, last_name_col="last_name", first_name_col="first_name"):
    def c(s):
        return s.astype(str).str.strip().str.lower()
    return c(df[last_name_col]) + "||" + c(df[first_name_col])



def affiliation_orcid():
 
    orc_final = orcid_back()
 
    perso = persons_choose()
 
    perso["_name_key"] = _clean_name(perso)
    orc_final["_name_key"] = _clean_name(orc_final)
 
    # ------------------------------------------------------------------
    # Garde-fou homonymes : noms associés à plusieurs orcid_id DIFFÉRENTS
    # dans perso (là où orcid_id était déjà connu en entrée). Pour ces
    # noms-là, impossible de deviner sans risque via le seul nom -> on ne
    # tentera pas de repli par nom pour eux plus bas.
    # ------------------------------------------------------------------
    homonymes_a_risque = (
        perso[perso["orcid_id"].notna()]
        .groupby("_name_key")["orcid_id"]
        .nunique()
        .reset_index(name="nb_orcid_distincts")
        .query("nb_orcid_distincts > 1")
    )
    print(f"{len(homonymes_a_risque)} nom(s) associés à plusieurs orcid_id distincts (homonymes réels) :")
    print(homonymes_a_risque)
    noms_ambigus_perso = set(homonymes_a_risque["_name_key"])
 
    # ------------------------------------------------------------------
    # 1. Table employeurs indexée par orcid_id_final : identifiant fiable,
    #    aucun risque d'homonyme ici. Regroupe tous les employeurs connus
    #    pour cet orcid_id_final, peu importe quelle ligne source
    #    (fourni / trouve / ambigu...) les a produits -> plus besoin de
    #    conserver de ligne en double pour que le lien fonctionne.
    # ------------------------------------------------------------------
    employers_history = (
        orc_final[[
            "orcid_id_final", "employer_name", "employer_role",
            "employer_department", "employer_start_date", "employer_end_date",
            "employer_city", "employer_region", "employer_country",
            "employer_org_id", "employer_org_id_source",
        ]]
        .dropna(subset=["orcid_id_final"])
        .drop_duplicates()
    )
 
    # ------------------------------------------------------------------
    # 2. Table nom -> orcid_id_final, uniquement pour les noms non
    #    ambigus dans orc_final (un seul orcid_id_final associé à ce nom
    #    dans tout le fichier). Sert de repli quand perso['orcid_id'] est
    #    vide pour cette ligne mais que la personne a été identifiée
    #    ailleurs (autre projet) dans orc_final -> règle le cas Faranda.
    # ------------------------------------------------------------------
    nb_orcid_par_nom_orc = (
        orc_final.dropna(subset=["orcid_id_final"])
        .groupby("_name_key")["orcid_id_final"].nunique()
    )
    noms_non_ambigus_orc = set(nb_orcid_par_nom_orc[nb_orcid_par_nom_orc == 1].index)
 
    source_priority = {
        "fourni": 0,
        "trouve_desambiguise": 1, "trouve_desambiguise_permute": 1,
        "trouve": 2, "trouve_permute": 2,
        "ambigu": 3, "ambigu_permute": 3,
        "non_trouve": 4,
    }
    orc_final["_source_priority"] = orc_final["orcid_source"].map(source_priority).fillna(9)
 
    canonical_by_name = (
        orc_final[orc_final["_name_key"].isin(noms_non_ambigus_orc)]
        .dropna(subset=["orcid_id_final"])
        .sort_values("_source_priority")
        .drop_duplicates(subset="_name_key", keep="first")
        [["_name_key", "orcid_id_final"]]
        .rename(columns={"orcid_id_final": "orcid_id_par_nom"})
    )
 
    # ------------------------------------------------------------------
    # 3. Résolution : orcid_id d'origine EN PRIORITÉ.
    #    Pour les lignes sans orcid_id :
    #      a. si le nom n'est pas ambigu -> repli par nom seul
    #         (canonical_by_name, cf. étape 2)
    #      b. si le nom EST ambigu (homonymes réels), deux replis
    #         possibles, testés du plus fiable au moins fiable :
    #         b1. (nom + email complet) : certaines sources désambiguïsent
    #             déjà les homonymes au niveau de l'email lui-même (ex.
    #             "martine.laporte" vs "martine.laporte2") -> signal fort,
    #             on l'utilise en priorité.
    #         b2. (nom + domaine d'email) : plus faible, ne fonctionne
    #             QUE si ce couple (nom, domaine) est lui-même associé à
    #             un orcid_id unique -> si les deux homonymes partagent
    #             le même domaine (même institution/région), ce couple
    #             reste ambigu et le repli est automatiquement écarté :
    #             le flag orcid_a_verifier reste bien à True pour eux.
    # ------------------------------------------------------------------
    perso["_email_key"] = (
        perso["_name_key"] + "||" +
        perso["email"].astype(str).str.strip().str.lower().replace({"nan": ""})
    )
    nb_orcid_par_email = (
        perso[perso["orcid_id"].notna() & (perso["email"].astype(str).str.strip() != "")]
        .groupby("_email_key")["orcid_id"].nunique()
    )
    emails_non_ambigus = set(nb_orcid_par_email[nb_orcid_par_email == 1].index)
    email_to_orcid = (
        perso[perso["orcid_id"].notna() & perso["_email_key"].isin(emails_non_ambigus)]
        .drop_duplicates(subset="_email_key")
        [["_email_key", "orcid_id"]]
        .rename(columns={"orcid_id": "orcid_id_par_email"})
    )
    perso = perso.merge(email_to_orcid, on="_email_key", how="left")
 
    perso["_domain_key"] = (
        perso["_name_key"] + "||" +
        perso["domaine_email"].astype(str).str.strip().str.lower().replace({"nan": ""})
    )
    nb_orcid_par_domaine = (
        perso[perso["orcid_id"].notna() & (perso["domaine_email"].astype(str).str.strip() != "")]
        .groupby("_domain_key")["orcid_id"].nunique()
    )
    domaines_non_ambigus = set(nb_orcid_par_domaine[nb_orcid_par_domaine == 1].index)
    domain_to_orcid = (
        perso[perso["orcid_id"].notna() & perso["_domain_key"].isin(domaines_non_ambigus)]
        .drop_duplicates(subset="_domain_key")
        [["_domain_key", "orcid_id"]]
        .rename(columns={"orcid_id": "orcid_id_par_domaine"})
    )
    perso = perso.merge(domain_to_orcid, on="_domain_key", how="left")
 
    perso = perso.merge(canonical_by_name, on="_name_key", how="left")
 
    perso["orcid_id_final"] = perso["orcid_id"]
 
    # a. nom non ambigu -> repli par nom
    fallback_nom = perso["orcid_id"].isna() & ~perso["_name_key"].isin(noms_ambigus_perso)
    perso.loc[fallback_nom, "orcid_id_final"] = perso.loc[fallback_nom, "orcid_id_par_nom"]
 
    # b1. nom ambigu MAIS (nom + email complet) résolu -> repli le plus fiable
    fallback_email = (
        perso["orcid_id"].isna()
        & perso["_name_key"].isin(noms_ambigus_perso)
        & perso["orcid_id_par_email"].notna()
    )
    perso.loc[fallback_email, "orcid_id_final"] = perso.loc[fallback_email, "orcid_id_par_email"]
 
    # b2. toujours ambigu après b1, mais (nom + domaine) résolu -> repli plus faible
    fallback_domaine = (
        perso["orcid_id"].isna()
        & perso["_name_key"].isin(noms_ambigus_perso)
        & perso["orcid_id_final"].isna()
        & perso["orcid_id_par_domaine"].notna()
    )
    perso.loc[fallback_domaine, "orcid_id_final"] = perso.loc[fallback_domaine, "orcid_id_par_domaine"]
 
    perso["orcid_a_verifier"] = perso["orcid_id_final"].isna() & perso["_name_key"].isin(noms_ambigus_perso)
    print(f"{fallback_email.sum()} ligne(s) résolues via nom + email complet")
    print(f"{fallback_domaine.sum()} ligne(s) résolues via nom + domaine d'email")
    print(f"{perso['orcid_a_verifier'].sum()} ligne(s) laissées sans orcid_id_final "
          f"(nom homonyme, aucun repli ne permet de trancher sans risque)")
 
    # ------------------------------------------------------------------
    # 4. Jointure finale des employeurs, sur l'identifiant fiable
    #    orcid_id_final (jamais sur le nom). On garde l'index d'origine
    #    de perso comme identité de ligne stable : project_id + nom ne
    #    suffit pas, une même personne peut avoir plusieurs lignes
    #    légitimes pour un même projet (rôles différents : fellow vs
    #    main_contact, entités différentes) -> il ne faut PAS les fondre
    #    en une seule via drop_duplicates(project_id, nom).
    # ------------------------------------------------------------------
    perso = perso.reset_index(drop=True)
    perso["_perso_row_id"] = perso.index
 
    merged = perso.merge(employers_history, on="orcid_id_final", how="left")
 
    # Correspondance de pays : on privilégie host_country_code (pays
    # d'ACCUEIL du projet pour les ERC/MSCA, converti d'ISO3 vers ISO2
    # via country_iso_shift pour être comparable à employer_country
    # renvoyé par ORCID), plus précis que iso2/country_code pour ce
    # contexte -> repli sur iso2 si host_country_code est absent.
    perso_host_iso2 = perso[["_perso_row_id", "host_country_code"]].rename(
        columns={"host_country_code": "_host_country_code"}
    )
    perso_host_iso2 = country_iso_shift(perso_host_iso2, "_host_country_code", iso2_to3=False)
    merged = merged.merge(perso_host_iso2, on="_perso_row_id", how="left")
    merged["_host_iso2"] = merged["_host_country_code"].fillna(merged["iso2"])
 
    merged["country_match"] = (
        merged["_host_iso2"].astype(str).str.upper().str.strip()
        == merged["employer_country"].astype(str).str.upper().str.strip()
    )
 
    # Correspondance temporelle : la date de référence du projet est
    # l'année de DÉPÔT du dossier (call_year, comparée à l'année civile
    # complète) -> c'est la situation du candidat au moment de la
    # candidature qui compte, pas la date de démarrage effectif du
    # financement (start_date), qui peut intervenir 1-2 ans plus tard
    # (négociation de convention, notamment pour les ERC) sans que le
    # chercheur ait changé de labo entre-temps. start_date sert de repli
    # uniquement si call_year est absent.
    call_year_date = pd.to_datetime(merged["call_year"], format="%Y", errors="coerce")
    start_date = pd.to_datetime(merged["start_date"], errors="coerce")
    merged["_project_start"] = call_year_date.fillna(start_date)
 
    # Fenêtre de comparaison : l'année civile de call_year (1er janvier
    # au 31 décembre) si dispo, sinon la durée du projet à partir de
    # start_date comme avant.
    duration_months = pd.to_numeric(merged["duration"], errors="coerce")
    project_end_from_duration = merged["_project_start"] + pd.to_timedelta(
        (duration_months.fillna(0) * 30.44), unit="D"
    )
    # si la référence est call_year, on élargit la fenêtre a minima à
    # l'année civile complète (31 décembre de cette année), même sans
    # durée connue -> une candidature déposée en janvier reste valide
    # jusqu'à fin décembre de la même année.
    end_of_call_year = call_year_date + pd.offsets.YearEnd(0)
    merged["_project_end"] = project_end_from_duration.where(
        project_end_from_duration.notna() & (duration_months.fillna(0) > 0),
        end_of_call_year.fillna(merged["_project_start"]),
    )
 
    emp_start = pd.to_datetime(merged["employer_start_date"], errors="coerce")
    emp_end = pd.to_datetime(merged["employer_end_date"], errors="coerce").fillna(pd.Timestamp.max)
 
    # chevauchement de deux intervalles [a,b] et [c,d] : a <= d et c <= b
    merged["temporal_match"] = (
        merged["_project_start"].notna()
        & emp_start.notna()
        & (emp_start <= merged["_project_end"])
        & (merged["_project_start"] <= emp_end)
    )
 
    # Correspondance précise sur le DÉMARRAGE du projet (plus stricte,
    # utile pour départager plusieurs employeurs qui chevauchent tous la
    # durée du projet) : priorisée en cas d'égalité avec temporal_match.
    merged["temporal_match_debut"] = (
        merged["_project_start"].notna()
        & emp_start.notna()
        & (emp_start <= merged["_project_start"])
        & (merged["_project_start"] <= emp_end)
    )
 
    # Unité de recherche la plus précise disponible pour cette ligne
    # employeur : le département ORCID s'il est renseigné, sinon le nom
    # de l'employeur.
    merged["orcid_unite_recherche"] = merged["employer_department"].fillna(merged["employer_name"])
 
    # Tri de priorité pour choisir l'employeur "principal" à afficher en
    # colonnes employer_* :
    #   1. pays ET période du projet correspondent tous les deux (signal
    #      le plus fiable : ex. poste CNRS/France qui chevauche la durée
    #      du projet, même s'il n'a pas démarré pile au jour J)
    #   2. correspondance précise au démarrage du projet (utile si aucun
    #      employeur ne matche le pays)
    #   3. chevauchement plus large avec la durée du projet
    #   4. pays seul
    #   5. poste le plus récent/en cours
    merged["_tier_pays_periode"] = merged["country_match"] & merged["temporal_match"]
    merged["_end_sort"] = emp_end
    merged_sorted = merged.sort_values(
        by=["_perso_row_id", "_tier_pays_periode", "temporal_match_debut",
            "temporal_match", "country_match", "_end_sort"],
        ascending=[True, False, False, False, False, False],
    )
 
    # Étiquette lisible pour chaque entrée de la liste agrégée
    def _label(row):
        val = row["orcid_unite_recherche"]
        if pd.isna(val):
            return None
        tags = [t for t, cond in (("période de démarrage", row["temporal_match_debut"]),
                                   ("période du projet", row["temporal_match"]),
                                   ("pays correspondant", row["country_match"])) if cond]
        return f"{val} [{', '.join(tags)}]" if tags else str(val)
 
    merged_sorted["_label"] = merged_sorted.apply(_label, axis=1)
 
    unites_agg = (
        merged_sorted.groupby("_perso_row_id")["_label"]
        .apply(lambda s: " | ".join(dict.fromkeys(x for x in s if x)))  # dédoublonne, garde l'ordre
        .rename("orcid_unites_recherche")
    )
 
    # Une seule ligne par ligne perso d'origine : le meilleur candidat
    # (selon le tri ci-dessus) sert de "principal", la liste complète
    # reste disponible dans orcid_unites_recherche.
    persons_final = (
        merged_sorted
        .drop_duplicates(subset="_perso_row_id", keep="first")
        .merge(unites_agg, on="_perso_row_id", how="left")
        .drop(columns=["_perso_row_id", "_end_sort", "_tier_pays_periode", "_host_iso2", "_host_country_code",
                        "_domain_key", "orcid_id_par_domaine", "_email_key", "orcid_id_par_email", "orcid_id_par_nom",
                        "_project_start", "_project_end",
                        "_label", "orcid_unite_recherche"])
        .reset_index(drop=True)
    )
 
    return persons_final