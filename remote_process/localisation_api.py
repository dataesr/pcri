def geonames_api(df,cc):
    from config_path import PATH_HARVEST
    import requests, pandas as pd
    import time
    from datetime import datetime

    HOURLY_LIMIT = 1000
    DAILY_LIMIT = 10000
    DELAY = 3600 / HOURLY_LIMIT
    MAX_RETRIES = 3
    SAVE_EVERY = 50  # save every 50 requests

    hourly_count = 0
    daily_count = 0
    hour_start = datetime.now()
    today = datetime.now().date()
    file_out = f"{PATH_HARVEST}geoloc/by_countries/geo_foreign_{cc}.pkl"


    for i, row in df.iterrows():
        # Skip already processed rows (allows resuming)
        if pd.notna(df.at[i, 'location']):
            continue

        # Skip null postal codes
        cp = row['postalCode']
        # cc = row['countryCode']
        if pd.isna(cp) or pd.isna(cc) or str(cp).lower() == 'none':
            print(f"Row {i} -> skipped (null postalCode or countryCode)")
            continue

        if daily_count >= DAILY_LIMIT:
            print("Daily limit reached, stopping.")
            break
        
        now = datetime.now()
        elapsed = (now - hour_start).total_seconds()
        if elapsed >= 3600:
            hourly_count = 0
            hour_start = now

        if hourly_count >= HOURLY_LIMIT:
            wait = 3600 - elapsed
            print(f"Hourly limit reached. Waiting {wait:.0f}s...")
            time.sleep(wait)
            hourly_count = 0
            hour_start = datetime.now()

        # Retry logic on timeout
        url = f"http://api.geonames.org/postalCodeSearchJSON?postalcode={cp}&country={cc}&maxRows=500&username=zoefri"
        res = None
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                r = requests.get(url, timeout=10)
                if r.status_code == 200:
                    res = r.json()
                break  # success, exit retry loop
            except requests.exceptions.Timeout:
                print(f"Row {i} -> timeout (attempt {attempt}/{MAX_RETRIES})")
                time.sleep(5 * attempt)  # backoff: 5s, 10s, 15s
            except requests.exceptions.ConnectionError as e:
                print(f"Row {i} -> connection error (attempt {attempt}/{MAX_RETRIES}): {e}")
                time.sleep(5 * attempt)

        if res is None:
            print(f"Row {i} -> failed after {MAX_RETRIES} attempts, skipping.")
            continue

        if 'status' in res:
            print(f"API Error: {res['status']['message']} (code {res['status']['value']})")
            if res['status']['value'] in (18, 19):
                print("Limit exceeded, stopping.")
                df.to_pickle(file_out)  # save before stopping
                break
        else:
            print(f"{cp}, {cc} -> ok")
            df.at[i, 'location'] = res

        hourly_count += 1
        daily_count += 1

        # Incremental save every N rows
        if daily_count % SAVE_EVERY == 0:
            df.to_pickle(file_out)
            print(f"Progress saved ({daily_count} requests done)")

        time.sleep(DELAY)

    # Final save
    df.to_pickle(file_out)
    print(f"Done. Total requests: {daily_count}")




"""
FRENCH LOCALISATION

Géocodage des cp_ville (et adresses en fallback) vers le code commune INSEE
via l'API Géoplateforme (data.geopf.fr), pour import dans Grist.
 
Entrée attendue : un DataFrame `df` (déjà filtré sur la France) avec au minimum les colonnes :
    - cp_ville      (ex: "20600 bastia")
    - postalCode    (ex: "20600")
    - city          (ex: "bastia")
    - street        (adresse complète, utilisée en fallback étape 2)
    - drop          (bool, lignes à ignorer si True — colonne créée si absente)
 
Sortie : un DataFrame `final_df` avec les colonnes :
    - cp_ville
    - com_code
    - drop
    - score
    - match_step   ("cp_ville", "street" ou "dep_ville" selon l'étape ayant matché,
                     vide si aucune étape n'a abouti)
"""

import time
import difflib
import requests
import pandas as pd
 
session = requests.Session()
BASE_URL = "https://data.geopf.fr/geocodage/search/"
GEO_COMMUNES_URL = "https://geo.api.gouv.fr/communes"
 
 
def geocode_query(params, retries=3, pause=0.02):
    """Appelle l'API et retourne la liste des features (avec retry léger)."""
    for attempt in range(retries):
        try:
            r = session.get(BASE_URL, params=params, timeout=5)
            if r.status_code == 200:
                time.sleep(pause)  # respecte la limite de 50 req/s/IP
                return r.json().get("features", [])
            elif r.status_code == 429:
                time.sleep(1)
            else:
                return []
        except requests.exceptions.RequestException:
            time.sleep(0.5)
    return []
 
 
def best_feature(features, type_filter=None):
    """Retourne la feature avec le meilleur score, filtrée par type si précisé."""
    candidates = features
    if type_filter:
        candidates = [f for f in features if f["properties"].get("type") == type_filter]
    if not candidates:
        return None
    return max(candidates, key=lambda f: f["properties"].get("score", 0))
 
 
def department_from_postcode(postcode):
    """Extrait le code département à partir d'un code postal (gère les DOM-TOM)."""
    postcode = str(postcode).strip().zfill(5)
    if postcode.startswith(("97", "98")):
        return postcode[:3]
    return postcode[:2]
 
 
def name_similarity(a, b):
    """Score de similarité textuelle simple entre deux noms de commune (0 à 1)."""
    return difflib.SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()
 
 
def geocode_by_city_department(city, postcode, retries=3):
    """
    Recherche une commune par nom + département via l'API Découpage administratif.
    Retourne (com_code, score) ou (None, None) si rien de probant.
    """
    dept = department_from_postcode(postcode)
    params = {
        "nom": city,
        "codeDepartement": dept,
        "boost": "population",
        "fields": "nom,code",
    }
    for attempt in range(retries):
        try:
            r = session.get(GEO_COMMUNES_URL, params=params, timeout=5)
            if r.status_code == 200:
                results = r.json()
                if results:
                    best = results[0]  # déjà trié par boost=population
                    score = name_similarity(city, best.get("nom", ""))
                    return best.get("code"), score
                return None, None
            elif r.status_code == 429:
                time.sleep(1)
            else:
                return None, None
        except requests.exceptions.RequestException:
            time.sleep(0.5)
    return None, None
 
 
def fr_geocode(df, score_threshold_step2=0.0, score_threshold_step3=0.6):
    """
    Enchaîne étape 1 (cp_ville) puis étape 2 (adresse) sur les lignes non matchées.
    Retourne un DataFrame final avec cp_ville / com_code / drop / score.
    """
    df = df.copy()
    if "drop_loc" not in df.columns:
        df["drop_loc"] = False
 
    mask_fra = (df["drop_loc"] != True)
    work = df.loc[mask_fra, ["cp_ville", "postalCode", "city"]].drop_duplicates().reset_index(drop=True)
 
    print(f"- Étape 1 : recherche de {len(work)} cp_ville distincts par CP + ville")
 
    work["com_code"] = pd.NA
    work["score"] = pd.NA
    work["match_step"] = pd.NA
 
    for idx, row in work.iterrows():
        features = geocode_query({
            "q": row["city"],
            "postcode": row["postalCode"],
            "type": "municipality",
        })
        best = best_feature(features)
        if best:
            props = best["properties"]
            work.at[idx, "com_code"] = props.get("citycode")
            work.at[idx, "score"] = props.get("score")
            work.at[idx, "match_step"] = "cp_ville"
 
    work["drop_loc"] = work["com_code"].isnull()
 
    n_unmatched = int(work["drop_loc"].sum())
    print(f"  -> {len(work) - n_unmatched} matchés, {n_unmatched} non matchés")
 
    # -------------------------------------------------------------
    # Étape 2 : fallback sur l'adresse complète pour les non-matchés
    # -------------------------------------------------------------
    if n_unmatched > 0:
        unmatched_cpville = work.loc[work["drop_loc"], "cp_ville"].unique()
        step2_src = df.loc[
            df["cp_ville"].isin(unmatched_cpville),
            ["cp_ville", "postalCode", "street"],
        ].drop_duplicates(subset="cp_ville").reset_index(drop=True)
 
        print(f"- Étape 2 : recherche de {len(step2_src)} adresses en fallback")
 
        for idx, row in step2_src.iterrows():
            if pd.isna(row.get("street")) or not str(row["street"]).strip():
                continue
            features = geocode_query({
                "q": row["street"],
                "postcode": row["postalCode"],
            })
            best = best_feature(features)  # meilleur score, tous types
            if best:
                props = best["properties"]
                score = props.get("score", 0)
                if score >= score_threshold_step2:
                    step2_src.at[idx, "com_code"] = props.get("citycode")
                    step2_src.at[idx, "score"] = score
                    step2_src.at[idx, "match_step"] = "street"
 
        step2_src = step2_src[["cp_ville", "com_code", "score", "match_step"]]
 
        # on comble les trous dans `work` avec les résultats de l'étape 2
        work = work.merge(step2_src, on="cp_ville", how="left", suffixes=("", "_s2"))
        work["com_code"] = work["com_code"].fillna(work["com_code_s2"])
        work["score"] = work["score"].fillna(work["score_s2"])
        work["match_step"] = work["match_step"].fillna(work["match_step_s2"])
        work.drop(columns=["com_code_s2", "score_s2", "match_step_s2"], inplace=True)
        work["drop_loc"] = work["com_code"].isnull()
 
    # -------------------------------------------------------------
    # Étape 3 : fallback ville + département (2 premiers chiffres du CP)
    # utile pour les petites communes non reconnues en étape 1/2
    # -------------------------------------------------------------
    n_still_unmatched = int(work["drop_loc"].sum())
    if n_still_unmatched > 0:
        still_unmatched = work.loc[work["drop_loc"], "cp_ville"].unique()
        step3_src = df.loc[
            df["cp_ville"].isin(still_unmatched),
            ["cp_ville", "postalCode", "city"],
        ].drop_duplicates(subset="cp_ville").reset_index(drop=True)
 
        print(f"- Étape 3 : rapprochement ville + département pour {len(step3_src)} communes")
 
        for idx, row in step3_src.iterrows():
            com_code, score = geocode_by_city_department(row["city"], row["postalCode"])
            if com_code and score >= score_threshold_step3:
                step3_src.at[idx, "com_code"] = com_code
                step3_src.at[idx, "score"] = score
                step3_src.at[idx, "match_step"] = "dep_ville"
 
        step3_src = step3_src[["cp_ville", "com_code", "score", "match_step"]]
 
        work = work.merge(step3_src, on="cp_ville", how="left", suffixes=("", "_s3"))
        work["com_code"] = work["com_code"].fillna(work["com_code_s3"])
        work["score"] = work["score"].fillna(work["score_s3"])
        work["match_step"] = work["match_step"].fillna(work["match_step_s3"])
        work.drop(columns=["com_code_s3", "score_s3", "match_step_s3"], inplace=True)
        work["drop_loc"] = work["com_code"].isnull()
 
        n_recovered = n_still_unmatched - int(work["drop_loc"].sum())
        print(f"  -> {n_recovered} communes récupérées, {int(work['drop_loc'].sum())} toujours non matchées")
 
    final_df = work[["cp_ville", "com_code", "drop_loc", "score", "match_step"]].drop_duplicates(subset="cp_ville").reset_index(drop=True)
    return final_df
 
