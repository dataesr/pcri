"""
Enrichissement d'un DataFrame (contact2, orcid_id) via l'API publique ORCID.

Pour chaque ligne :
  - si orcid_id est renseigné -> on récupère directement les employments
  - si orcid_id est NaN       -> on cherche par nom (expanded-search, qui
                                  renvoie aussi given-names/family-names),
                                  on score chaque candidat par similarité
                                  de nom avec la ligne d'origine, et on
                                  retient le meilleur si le score est net.
                                  Sinon on note l'ambiguïté et on garde le
                                  détail des candidats pour vérif manuelle.
  - puis on récupère les employments du candidat retenu

Résultat : DataFrame enrichi avec les colonnes
  orcid_id_final, orcid_source (fourni / trouve / trouve_desambiguise /
  ambigu / non_trouve), nb_candidats, candidats_detail (noms des
  candidats, pour vérif manuelle en cas d'ambiguïté), employers (liste
  des organisations séparées par ' | '), employments_detail (liste de
  dicts, pour usage ultérieur)
"""

import time, requests, os, json, unicodedata, re
import pandas as pd

CLIENT_ID = os.environ.get('ORCID_CLIENT')
CLIENT_SECRET = os.environ.get('ORCID_SECRET')

TOKEN_URL = "https://orcid.org/oauth/token"
BASE_URL = "https://pub.orcid.org/v3.0"

SESSION = requests.Session()


# ---------------------------------------------------------------------
# Requête HTTP avec retry, y compris sur coupure réseau
# ---------------------------------------------------------------------
def _request_with_retry(method, url, retries=3, **kwargs):
    """
    Wrapper autour de SESSION.request qui retente en cas de :
      - code HTTP 429 / 503 (rate limit / service indisponible)
      - erreur de connexion (Wi-Fi coupé, MaxRetryError, timeout...) qui
        lève une exception AVANT même d'obtenir une réponse HTTP -> sans
        ce filet, ces erreurs échappaient à tout retry et faisaient
        perdre la ligne immédiatement (elle n'était retentée qu'au
        prochain lancement complet du script).
    Lève la dernière exception rencontrée si tous les essais échouent.
    """
    last_exc = None
    for attempt in range(retries):
        try:
            resp = SESSION.request(method, url, timeout=30, **kwargs)
        except requests.exceptions.RequestException as e:
            last_exc = e
            time.sleep(2 ** attempt)
            continue
        if resp.status_code == 429 or resp.status_code == 503:
            time.sleep(2 ** attempt)
            continue
        return resp
    if last_exc is not None:
        raise last_exc
    return resp  # dernière réponse HTTP (429/503) après épuisement des essais


# ---------------------------------------------------------------------
# Authentification
# ---------------------------------------------------------------------
def get_access_token():
    resp = _request_with_retry(
        "POST", TOKEN_URL,
        headers={"Accept": "application/json"},
        data={
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "grant_type": "client_credentials",
            "scope": "/read-public",
        },
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


# ---------------------------------------------------------------------
# Normalisation / scoring de noms pour la désambiguïsation
# ---------------------------------------------------------------------
def _normalize(s):
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    return s.strip().lower()


def _name_score(candidate, given, family):
    """
    Score de correspondance entre un candidat ORCID (dict avec
    given_names / family_names / credit_name) et le nom/prénom d'origine.
    Plus le score est haut, meilleure la correspondance.
    """
    target_given = _normalize(given)
    target_family = _normalize(family)

    cand_given = _normalize(candidate.get("given_names"))
    cand_family = _normalize(candidate.get("family_names"))
    cand_credit = _normalize(candidate.get("credit_name"))

    score = 0

    if cand_family and target_family:
        if cand_family == target_family:
            score += 3
        elif target_family in cand_family or cand_family in target_family:
            score += 1

    if cand_given and target_given:
        if cand_given == target_given:
            score += 3
        elif cand_given.startswith(target_given) or target_given.startswith(cand_given):
            score += 1

    # Filet de sécurité : parfois le nom est renseigné en "credit-name"
    # (nom d'usage) plutôt que given/family séparés.
    if cand_credit and target_family and target_given:
        if target_family in cand_credit and target_given in cand_credit:
            score += 2

    return score


def _lucene_quote(value: str) -> str:
    """
    Encadre une valeur de guillemets pour Lucene, afin qu'elle soit
    traitee comme UNE SEULE phrase sur le champ cible plutot que comme
    plusieurs termes separes des lors qu'elle contient un espace.
 
    Bug corrige : pour un nom de famille avec particule comme
    "de Reynies", la requete NON guillemetee family-name:de Reynies
    est interpretee par Lucene comme deux termes distincts -- "de"
    contraint sur le champ family-name, et "Reynies" comme terme LIBRE
    non rattache a aucun champ -> la recherche s'elargit enormement et
    peut remonter des candidats sans rapport (ex: "de Boisanger" au
    lieu de "de Reynies", simplement parce que "de" matche).
    Avec guillemets, family-name:"de Reynies" est traite comme une
    phrase exacte sur le champ family-name.
    """
    escaped = str(value).replace('"', '\\"')
    return f'"{escaped}"'
 
 
def search_orcid_by_name(token, given, family, retries=3):
    """
    Retourne une liste de dicts :
      {"orcid_id": ..., "given_names": ..., "family_names": ..., "credit_name": ...}
    via l'endpoint /expanded-search/, qui contrairement a /search/ renvoie
    directement les noms des candidats (utile pour desambiguiser sans
    requete supplementaire par candidat).
 
    PATCH : les valeurs sont desormais entourees de guillemets Lucene
    (cf. _lucene_quote) pour eviter qu'un nom de famille contenant un
    espace (particule "de"/"van"/"von"...) ne soit coupe en deux termes
    dont l'un devient un terme libre non filtre.
    """
    given = str(given).strip() if pd.notna(given) else ""
    family = str(family).strip() if pd.notna(family) else ""
 
    if not given and not family:
        return []
 
    if given and family:
        query = f'given-names:{_lucene_quote(given)} AND family-name:{_lucene_quote(family)}'
    elif family:
        query = f'family-name:{_lucene_quote(family)}'
    else:
        query = f'given-names:{_lucene_quote(given)}'
 
    resp = _request_with_retry(
        "GET", f"{BASE_URL}/expanded-search/",
        retries=retries,
        params={"q": query},
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    resp.raise_for_status()
    data = resp.json()
    candidates = []
    for item in data.get("expanded-result", []) or []:
        candidates.append({
            "orcid_id": item.get("orcid-id"),
            "given_names": item.get("given-names"),
            "family_names": item.get("family-names"),
            "credit_name": item.get("credit-name"),
        })
    return candidates


# ---------------------------------------------------------------------
# Employments
# ---------------------------------------------------------------------
def get_employments(token, orcid_id, retries=3):
    resp = _request_with_retry(
        "GET", f"{BASE_URL}/{orcid_id}/employments",
        retries=retries,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        },
    )
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    data = resp.json()

    employments = []
    for group in data.get("employment-summary", []) or []:
        employments.append(_parse_employment_summary(group))

    # Selon la version de la réponse, les données peuvent être groupées
    # sous 'affiliation-group' -> 'summaries' -> 'employment-summary'
    for group in data.get("affiliation-group", []) or []:
        for s in group.get("summaries", []):
            summary = s.get("employment-summary", {})
            employments.append(_parse_employment_summary(summary))

    return employments


def _parse_employment_summary(summary):
    org = summary.get("organization") or {}
    address = org.get("address") or {}
    disambiguated = org.get("disambiguated-organization") or {}

    return {
        "organisation": org.get("name"),
        "role": summary.get("role-title"),
        "department": summary.get("department-name"),
        "start_date": _format_date(summary.get("start-date")),
        "end_date": _format_date(summary.get("end-date")),
        "org_city": address.get("city"),
        "org_region": address.get("region"),
        "org_country": address.get("country"),
        # Identifiant désambiguïsé de l'organisation : depuis août 2023
        # ORCID privilégie le ROR, mais on peut aussi trouver du
        # RINGGOLD / GRID / FUNDREF / LEI sur des entrées plus anciennes.
        "org_id": disambiguated.get("disambiguated-organization-identifier"),
        "org_id_source": disambiguated.get("disambiguation-source"),
    }


def _format_date(date_dict):
    if not date_dict:
        return None
    year = (date_dict.get("year") or {}).get("value")
    month = (date_dict.get("month") or {}).get("value")
    day = (date_dict.get("day") or {}).get("value")
    parts = [p for p in [year, month, day] if p]
    return "-".join(parts) if parts else None


# ---------------------------------------------------------------------
# Traitement du DataFrame
# ---------------------------------------------------------------------

# Seuil au-delà duquel on considère qu'un candidat se distingue assez
# nettement des autres pour être retenu automatiquement malgré
# l'homonymie. À ajuster si besoin selon la qualité observée.
_DISAMBIGUATION_MIN_SCORE = 4
_DISAMBIGUATION_MIN_GAP = 2



 
def _hyphen_to_apostrophe_variant(last_name: str):
    """
    Ne remplace le tiret par une apostrophe QUE dans le cas precis d'une
    elision francaise a une seule lettre en debut de nom (d'/l'), PAS
    pour n'importe quel tiret :
      "d-aleo"       -> "d'aleo"        (elision "d'", a corriger)
      "l-hermitte"   -> "l'hermitte"    (elision "l'", a corriger)
      "de-la-fontaine" -> INCHANGE      (nom compose legitime, "de" fait
                                          2 lettres -> pas une elision)
    Ne remplace que le PREMIER tiret rencontre, meme si le nom en
    contient plusieurs par ailleurs.
    Retourne None si le nom ne correspond pas au motif d'elision.
    """
    match = re.match(r"^([dlDL])-(.+)$", str(last_name).strip())
    if not match:
        return None
    return f"{match.group(1)}'{match.group(2)}"
 
 
def _any_exact_family_match(candidates, family) -> bool:
    return any(_normalize(c.get("family_names")) == _normalize(family) for c in candidates)
 
 
def _search_with_fallback(token, first_name, last_name):
    """
    Cherche les candidats ORCID pour (first_name, last_name). Essaie,
    dans l'ordre, et ne s'arrete a une etape que si elle produit une
    correspondance EXACTE de nom de famille (sinon on continue les
    tentatives suivantes, meme si l'etape precedente a deja renvoye un
    candidat -- un candidat errone ne doit pas empecher d'essayer une
    meilleure requete) :
      1. (prenom, nom) tel quel
      2. (nom, prenom) permutes -> certaines lignes sources ont
         first_name/last_name inverses
      3. nom avec tiret remplace par apostrophe -> motif frequent pour
         les noms composes francais mal saisis a la source
         (ex: "d-aleo" au lieu de "D'Aleo", "d-artagnan" au lieu de
         "D'Artagnan") -> le tiret ne matche pas bien l'apostrophe cote
         Solr, d'ou un candidat errone sans ce filet de securite.
 
    Si AUCUNE tentative ne produit de correspondance exacte, retombe
    sur le premier resultat non vide trouve (comportement d'origine)
    pour laisser _process_row gerer via le score de similarite
    (desambiguation multi-candidats) ou le flag "ambigu" -- on ne
    perd jamais un vrai cas d'homonymie a departager manuellement.
 
    Retourne (candidates, given_effectif, family_effectif, permute_utilise).
    """
    candidates = search_orcid_by_name(token, first_name, last_name)
    if candidates and _any_exact_family_match(candidates, last_name):
        return candidates, first_name, last_name, False
 
    swapped = search_orcid_by_name(token, last_name, first_name)
    if swapped and _any_exact_family_match(swapped, first_name):
        return swapped, last_name, first_name, True
 
    via_apostrophe = []
    last_name_apostrophe = last_name
    apostrophe_variant = _hyphen_to_apostrophe_variant(last_name)
    if apostrophe_variant is not None:
        last_name_apostrophe = apostrophe_variant
        via_apostrophe = search_orcid_by_name(token, first_name, last_name_apostrophe)
        if via_apostrophe and _any_exact_family_match(via_apostrophe, last_name_apostrophe):
            return via_apostrophe, first_name, last_name_apostrophe, False
 
    # Aucune correspondance exacte nulle part -> on retombe sur le
    # premier resultat non vide (ordre de priorite : normal > permute
    # > apostrophe), pour laisser _process_row decider (desambiguation
    # ou statut ambigu), comme avant ce patch.
    if candidates:
        return candidates, first_name, last_name, False
    if swapped:
        return swapped, last_name, first_name, True
    if via_apostrophe:
        return via_apostrophe, first_name, last_name_apostrophe, False
    return [], first_name, last_name, False



# Seuil minimal de similarite pour les cas ou plusieurs candidats sont
# a departager (inchange, cf. _DISAMBIGUATION_MIN_SCORE plus haut dans
# le fichier).
 
def _family_name_matches_exactly(candidate, family) -> bool:
    """
    Pour le cas 'un seul candidat', on exige une correspondance EXACTE
    du nom de famille (normalise : accents/casse/espaces ignores) --
    PAS le score composite _name_score(), qui peut etre gonfle a tort
    par le seul prenom (ex: 'D' matche partiellement "d'Aleo" via un
    test de sous-chaine ('d' in "d'aleo"), ce qui laissait passer un
    candidat completement different des lors que le prenom concordait).
    """
    return _normalize(candidate.get("family_names")) == _normalize(family)
 
 
def _process_row(token, first_name, last_name, orcid):
    """Traite une ligne : determine l'ORCID final et recupere les employments."""
    candidats_detail = ""
 
    if pd.notna(orcid) and str(orcid).strip():
        final_orcid = str(orcid).strip()
        source = "fourni"
        nb_candidats = 1
    else:
        candidates, eff_given, eff_family, permute_utilise = _search_with_fallback(
            token, first_name, last_name
        )
        nb_candidats = len(candidates)
 
        if nb_candidats == 0:
            final_orcid = None
            source = "non_trouve"
        elif nb_candidats == 1:
            # ---------------------------------------------------------
            # PATCH : meme avec un seul candidat, on verifie qu'il
            # ressemble reellement au nom cherche avant de l'accepter
            # aveuglement. Un candidat unique issu d'une requete cassee
            # (accents, apostrophes, tirets...) peut etre une personne
            # totalement differente -> on exige une correspondance
            # EXACTE du nom de famille (normalise), le prenom seul
            # n'etant pas un signal suffisant (trop de personnes
            # partagent un prenom courant).
            # ---------------------------------------------------------
            score = _name_score(candidates[0], eff_given, eff_family)
            candidats_detail = (
                f"{candidates[0].get('given_names') or ''} {candidates[0].get('family_names') or ''} "
                f"({candidates[0].get('orcid_id')}, score={score})"
            )
            if _family_name_matches_exactly(candidates[0], eff_family):
                final_orcid = candidates[0]["orcid_id"]
                source = "trouve_permute" if permute_utilise else "trouve"
            else:
                # Nom de famille different -> candidat suspect (souvent
                # du a une requete cassee par un caractere special dans
                # le nom cherche), a verifier manuellement plutot que
                # d'accepter automatiquement.
                final_orcid = candidates[0]["orcid_id"]
                source = "ambigu_nom_different_permute" if permute_utilise else "ambigu_nom_different"
        else:
            # Plusieurs homonymes : on score chaque candidat par
            # similarite de nom avec le nom effectivement utilise pour
            # la recherche (l'ordre permute si c'est celui qui a donne
            # des resultats).
            scored = sorted(
                (( _name_score(c, eff_given, eff_family), c) for c in candidates),
                key=lambda x: x[0], reverse=True,
            )
            top_score, top_candidate = scored[0]
            second_score = scored[1][0] if len(scored) > 1 else -1
 
            note_permute = " [recherche avec prenom/nom permutes]" if permute_utilise else ""
            candidats_detail = note_permute + " | ".join(
                f"{c.get('given_names') or ''} {c.get('family_names') or ''} "
                f"({c.get('orcid_id')}, score={s})"
                for s, c in scored
            )
 
            if top_score >= _DISAMBIGUATION_MIN_SCORE and (top_score - second_score) >= _DISAMBIGUATION_MIN_GAP:
                final_orcid = top_candidate["orcid_id"]
                source = "trouve_desambiguise_permute" if permute_utilise else "trouve_desambiguise"
            else:
                final_orcid = top_candidate["orcid_id"]
                source = "ambigu_permute" if permute_utilise else "ambigu"
 
    employments = []
    if final_orcid:
        employments = get_employments(token, final_orcid)
 
    employers_str = " | ".join(
        e["organisation"] for e in employments if e.get("organisation")
    )
    org_ids_str = " | ".join(
        f"{e['org_id_source']}:{e['org_id']}"
        for e in employments if e.get("org_id")
    )
 
    return {
        "orcid_id_final": final_orcid,
        "orcid_source": source,
        "nb_candidats": nb_candidats,
        "candidats_detail": candidats_detail,
        "employers": employers_str,
        "org_ids": org_ids_str,
        "employments_detail": json.dumps(employments, ensure_ascii=False),
    }


def _make_identity_key(df, first_name_col, last_name_col, orcid_col):
    """
    Clé d'identité stable par personne (nom + prénom + orcid_id,
    normalisés), utilisée pour faire correspondre un checkpoint existant
    au df courant. On ne peut PAS se fier à la position de la ligne
    (index 0, 1, 2...) car le df régénéré à chaque run peut avoir un
    contenu, un tri ou un nombre de lignes différent d'un run à l'autre
    (données sources mises à jour, filtre différent, etc.) : matcher par
    position associerait silencieusement de mauvais résultats à de
    mauvaises personnes, ou ferait planter la boucle si le df actuel est
    plus petit que le checkpoint existant.
    """
    def _clean(s):
        return s.astype(str).str.strip().str.lower().replace({"nan": "", "none": ""})

    return (
        _clean(df[last_name_col]) + "||" +
        _clean(df[first_name_col]) + "||" +
        _clean(df[orcid_col])
    )


def enrich_dataframe(df, first_name_col="first_name", last_name_col="last_name",
                      orcid_col="orcid_id",
                      pause=0.2, verbose=True,
                      checkpoint_path="orcid_checkpoint.csv",
                      checkpoint_every=100):
    """
    Enrichit df avec les infos ORCID / employments, avec sauvegarde
    périodique dans checkpoint_path (CSV) et reprise automatique si le
    fichier existe déjà (les lignes déjà traitées ne sont pas refaites).

    La reprise se fait par IDENTITÉ (nom + prénom + orcid_id), pas par
    position de ligne : robuste si le df a changé (ordre, filtre,
    nombre de lignes) entre deux runs sur le même checkpoint_path.
    """
    token = get_access_token()

    n = len(df)
    df = df.reset_index(drop=True)

    result_cols = ["orcid_id_final", "orcid_source", "nb_candidats",
                    "candidats_detail", "employers", "org_ids", "employments_detail"]

    # Si le df d'entrée contient déjà d'anciennes colonnes de résultat
    # (ex: il vient lui-même d'un export enrichi précédemment), on les
    # retire avant de fusionner avec le checkpoint pour éviter les
    # doublons de colonnes (suffixes _x/_y créés par pd.merge).
    df = df.drop(columns=[c for c in result_cols if c in df.columns], errors="ignore")

    df["_merge_key"] = _make_identity_key(df, first_name_col, last_name_col, orcid_col)

    # Reprise depuis un checkpoint existant : on aligne par identité, pas
    # par position, puis on retombe sur un df de la même forme/ordre que
    # le df actuel (une ligne par ligne de df, dans le même ordre).
    if os.path.exists(checkpoint_path):
        old_checkpoint = pd.read_csv(checkpoint_path)
        if "_merge_key" not in old_checkpoint.columns:
            old_checkpoint["_merge_key"] = _make_identity_key(
                old_checkpoint, first_name_col, last_name_col, orcid_col
            )
        # au cas où une même identité apparaîtrait plusieurs fois dans le
        # vieux checkpoint, on garde la dernière occurrence
        old_checkpoint = old_checkpoint.drop_duplicates(subset="_merge_key", keep="last")

        for c in result_cols:
            if c not in old_checkpoint.columns:
                old_checkpoint[c] = pd.NA

        checkpoint_df = df.merge(
            old_checkpoint[["_merge_key"] + result_cols],
            on="_merge_key", how="left",
        )

        if verbose:
            done = checkpoint_df["orcid_source"].notna().sum()
            ignored = len(old_checkpoint) - checkpoint_df["orcid_source"].notna().sum()
            print(f"Reprise depuis checkpoint : {checkpoint_path} "
                  f"({done} / {n} lignes du df actuel déjà traitées reconnues)")
            if len(old_checkpoint) != done:
                print(f"  (le checkpoint contenait {len(old_checkpoint)} lignes au total ; "
                      f"les entrées ne correspondant à aucune ligne du df actuel sont ignorées)")
    else:
        checkpoint_df = df.copy()
        for c in result_cols:
            checkpoint_df[c] = pd.NA

    # index des lignes pas encore traitées (orcid_source encore vide)
    todo_mask = checkpoint_df["orcid_source"].isna()
    todo_idx = checkpoint_df.index[todo_mask].tolist()

    if verbose:
        print(f"{len(todo_idx)} / {n} lignes restant à traiter")

    since_last_save = 0
    consecutive_errors = 0  # circuit-breaker : ralentit en cas de coupure réseau prolongée
    for count, idx in enumerate(todo_idx, start=1):
        first_name = df.at[idx, first_name_col]
        last_name = df.at[idx, last_name_col]
        orcid = df.at[idx, orcid_col]
        display_name = f"{first_name} {last_name}"

        try:
            row_result = _process_row(token, first_name, last_name, orcid)
        except requests.exceptions.RequestException as e:
            # En cas d'erreur réseau/API persistante (au-delà des retries
            # internes) : on note l'échec et on continue plutôt que de
            # tout arrêter. Ces lignes pourront être retentées lors d'une
            # prochaine exécution (orcid_source reste vide dans le
            # checkpoint -> elles seront reprises).
            consecutive_errors += 1
            if verbose:
                print(f"[{idx}] ERREUR pour '{display_name}': {e} -> ligne reportée "
                      f"(échecs consécutifs : {consecutive_errors})")
            # Coupure réseau prolongée (Wi-Fi tombé, etc.) : plutôt que
            # d'enchaîner les échecs à vide et brûler todo_idx pour rien,
            # on marque une pause croissante pour laisser le temps à la
            # connexion de revenir.
            if consecutive_errors >= 5:
                backoff = min(60, 5 * consecutive_errors)
                if verbose:
                    print(f"  -- {consecutive_errors} échecs consécutifs, "
                          f"pause de {backoff}s avant de continuer --")
                time.sleep(backoff)
            since_last_save += 1
            continue

        consecutive_errors = 0

        for c, v in row_result.items():
            checkpoint_df.at[idx, c] = v

        if verbose and count % 50 == 0:
            print(f"[{count}/{len(todo_idx)}] {display_name} -> "
                  f"{row_result['orcid_id_final']} ({row_result['orcid_source']})")

        since_last_save += 1
        if since_last_save >= checkpoint_every:
            checkpoint_df.drop(columns="_merge_key").to_csv(checkpoint_path, index=False)
            since_last_save = 0
            if verbose:
                print(f"  -- checkpoint sauvegardé ({checkpoint_path}) --")

        time.sleep(pause)  # ménage l'API, évite le 429

    # Sauvegarde finale (sans la colonne technique _merge_key, recalculée
    # à chaque lecture du checkpoint)
    checkpoint_df.drop(columns="_merge_key").to_csv(checkpoint_path, index=False)
    if verbose:
        print(f"Terminé. Résultat complet sauvegardé dans {checkpoint_path}")

    return checkpoint_df.drop(columns="_merge_key")