"""
Recherche d'identifiants (idref, orcid) et d'affiliations dans le dump
local scanR (persons_denormalized.jsonl.gz, ~2.5 Go), en UN SEUL passage
streaming sur le fichier -> beaucoup plus rapide qu'une recherche par
personne via une API, puisque tout est deja en local.

Principe :
  1. On construit l'ensemble des cles nom+prenom qu'on cherche (a partir
     de ton df de personnes), y compris la variante inversee.
  2. On lit le fichier ligne par ligne (jamais tout en memoire d'un
     coup), et on ne garde que les lignes dont fullName/lastName+
     firstName correspond a une des cles cherchees.
  3. Un meme nom peut matcher plusieurs personnes scanR (homonymes) ->
     toutes les lignes correspondantes sont gardees, a departager comme
     pour idref_res (via check_merge_integrity plus tard).

Usage :
    from scanr_runner import scanr_lookup
    scanr_res = scanr_lookup(df, dump_path="persons_denormalized.jsonl.gz")
"""

import gzip
import json
import time
import pandas as pd


def _normalize(s) -> str:
    return str(s).strip().lower() if pd.notna(s) else ""


def _make_key(last_name, first_name) -> str:
    return f"{_normalize(last_name)}||{_normalize(first_name)}"


def _build_target_keys(df, last_name_col="last_name", first_name_col="first_name"):
    """
    Construit le dict {cle_normale: (last_name_orig, first_name_orig)}
    pour la recherche normale ET la recherche inversee (prenom/nom
    permutes), comme pour idref_runner/orcid_enrichment.
    """
    targets = {}
    for _, row in df[[last_name_col, first_name_col]].drop_duplicates().iterrows():
        ln, fn = row[last_name_col], row[first_name_col]
        targets[_make_key(ln, fn)] = (ln, fn, "normal")
        targets[_make_key(fn, ln)] = (ln, fn, "inverse")  # variante permutee
    return targets


def _extract_external_id(external_ids, id_type: str):
    """external_ids = [{'type': 'idref', 'id': '032309163', 'url': ...}, ...]"""
    for item in external_ids or []:
        if str(item.get("type", "")).lower() == id_type.lower():
            return item.get("id")
    return None


def _get_default_label(value):
    """Certains champs scanR sont soit une chaine, soit un dict {'default': ...}."""
    if isinstance(value, dict):
        return value.get("default")
    return value


def _get_structure_id(structure):
    """
    'structure' peut etre soit directement le PID (string), soit un
    objet contenant le PID sous 'id' (ou parfois 'structure') plus des
    champs denormalises (label, mainAddress...) imbriques dedans.
    Retourne toujours une valeur hashable (string ou None), jamais
    un dict, pour pouvoir l'utiliser comme cle/dans un set.
    """
    if isinstance(structure, dict):
        return structure.get("id") or structure.get("structure") or structure.get("pid")
    return structure


def _get_structure_field(structure, field):
    """Lit un champ denormalise (label, mainAddress...) SI structure est un objet."""
    if isinstance(structure, dict):
        return structure.get(field)
    return None


def _parse_scanr_affiliation(aff: dict) -> dict:
    """
    Mappe une affiliation scanR vers le meme schema employer_* que cote
    ORCID (employer_name, employer_start_date, ...), pour pouvoir
    concatener les deux sources facilement plus tard.

    Champs scanR (doc) :
      structure (PID, ou objet {id, label, mainAddress, ...} si mappe
      dans l'index organisation), startDate, endDate, sources,
      recentAffiliations.
    """
    structure = aff.get("structure")
    structure_id = _get_structure_id(structure)
    label = _get_structure_field(structure, "label")
    main_address = _get_structure_field(structure, "mainAddress")
    if not isinstance(main_address, dict):
        main_address = {}

    return {
        "employer_name": _get_default_label(label) or structure_id,
        "employer_role": None,        # non fourni par scanR
        "employer_department": None,  # non fourni par scanR
        "employer_start_date": aff.get("startDate"),
        "employer_end_date": aff.get("endDate"),
        "employer_city": main_address.get("city"),
        "employer_region": main_address.get("region"),
        "employer_country": main_address.get("country"),
        "employer_org_id": structure_id,
        "employer_org_id_source": "scanR",
    }


def _pick_main_scanr_affiliation(affiliations: list) -> dict:
    """
    Choisit l'affiliation "principale" a plat parmi la liste :
    priorite aux affiliations encore actives (recentAffiliations,
    <=3 ans), sinon la plus recente par endDate (une endDate absente
    = poste toujours en cours -> priorite maximale).
    """
    empty = _parse_scanr_affiliation({})
    if not affiliations:
        return empty

    recent_ids = set()
    # recentAffiliations peut etre une liste de PID (str) ou d'objets
    # -> on gere les deux cas defensivement, le schema exact n'etant
    # pas garanti ("array of undefined" dans la doc scanR)
    for aff in affiliations:
        for r in aff.get("recentAffiliations", []) or []:
            candidate = r.get("structure") if isinstance(r, dict) else r
            recent_ids.add(_get_structure_id(candidate))

    def _sort_key(aff):
        sid = _get_structure_id(aff.get("structure"))
        is_recent = sid in recent_ids
        end_date = aff.get("endDate") or "9999"  # pas de fin = en cours = priorite max
        return (is_recent, end_date)

    best = sorted(affiliations, key=_sort_key, reverse=True)[0]
    return _parse_scanr_affiliation(best)


def _parse_person(obj: dict, name_order_used: str) -> dict:
    external_ids = obj.get("externalIds", [])

    idref_ppn = obj.get("idref") or _extract_external_id(external_ids, "idref")
    orcid = _extract_external_id(external_ids, "orcid")

    affiliations = obj.get("affiliations", [])
    main_affiliation = _pick_main_scanr_affiliation(affiliations)

    row = {
        "last_name": obj.get("lastName"),
        "first_name": obj.get("firstName"),
        "scanr_id": obj.get("id"),
        "scanr_idref": f"idref{idref_ppn}" if idref_ppn else None,
        "scanr_orcid": orcid,
        "scanr_gender": obj.get("gender"),
        "scanr_publications_count": obj.get("publicationsCount"),
        "scanr_affiliations": json.dumps(affiliations, ensure_ascii=False) if affiliations else None,
        "scanr_domains": json.dumps(
            [d.get("label", {}).get("default") for d in obj.get("topDomains", [])],
            ensure_ascii=False,
        ) if obj.get("topDomains") else None,
        "scanr_name_order_used": name_order_used,
    }
    row.update(main_affiliation)  # employer_name, employer_start_date, ...
    return row


def scanr_lookup(df: pd.DataFrame, dump_path: str,
                  last_name_col="last_name", first_name_col="first_name",
                  progress_every: int = 200_000) -> pd.DataFrame:
    """
    Parcourt le dump scanR en streaming et retourne toutes les lignes
    correspondant aux personnes de df (recherche normale + inversee).

    Un nom peut matcher 0, 1 ou plusieurs personnes scanR -> le retour
    peut avoir plusieurs lignes pour un meme (last_name, first_name)
    d'origine (homonymes a departager plus tard, comme pour idref_res).
    """
    targets = _build_target_keys(df, last_name_col, first_name_col)
    print(f"Recherche de {len(df[[last_name_col, first_name_col]].drop_duplicates())} "
          f"personne(s) dans le dump scanR ({len(targets)} cle(s), normal+inverse)...")

    results = []
    matched_original_keys = set()
    t0 = time.time()

    with gzip.open(dump_path, "rt", encoding="utf-8") as f:
        for i, line in enumerate(f, start=1):
            if progress_every and i % progress_every == 0:
                elapsed = time.time() - t0
                print(f"  ... {i:,} ligne(s) lues ({elapsed:.0f}s, "
                      f"{len(results)} match(es) trouve(s) jusqu'ici)")

            # On parse chaque ligne (json.loads) et compare son nom aux
            # cles cherchees. Avec ~800k lignes, ce parsing complet reste
            # de l'ordre de quelques minutes -> acceptable pour un usage
            # ponctuel, pas besoin d'optimisation plus poussee ici.
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            key_normal = _make_key(obj.get("lastName"), obj.get("firstName"))
            if key_normal in targets:
                orig_last, orig_first, order = targets[key_normal]
                row = _parse_person(obj, order)
                row["last_name"], row["first_name"] = orig_last, orig_first
                results.append(row)
                matched_original_keys.add(_make_key(orig_last, orig_first))

    elapsed = time.time() - t0
    print(f"Termine en {elapsed:.0f}s : {len(results)} correspondance(s) trouvee(s) "
          f"pour {len(matched_original_keys)} personne(s) distincte(s).")

    n_asked = len(df[[last_name_col, first_name_col]].drop_duplicates())
    n_found = len(matched_original_keys)
    print(f"{n_found} / {n_asked} personne(s) trouvee(s) dans scanR "
          f"({n_asked - n_found} absente(s) du dump)")

    return pd.DataFrame(results)


def split_scanr_resolved(df: pd.DataFrame, scanr_res: pd.DataFrame,
                          last_name_col="last_name", first_name_col="first_name") -> tuple:
    """
    Separe les personnes de df en deux groupes selon le resultat scanR :
      - resolved   : trouvees dans scanR SANS ambiguite (une seule
        correspondance pour ce nom) ET avec au moins idref OU orcid.
        -> pas besoin de repasser par ORCID/IdRef pour elles.
      - remaining  : absentes de scanR, OU trouvees plusieurs fois avec
        des identifiants DIFFERENTS (homonyme scanR, risque a ne pas
        trancher automatiquement) -> a traiter via orcid_runner /
        idref_runner comme avant.

    Retourne (resolved_df, remaining_df). resolved_df a les colonnes
    orcid_id_final / idref_id deja renseignees (alignees sur le schema
    utilise par merge_orcid_idref), pour une fusion finale directe.
    """
    def _key(d):
        return (d[last_name_col].astype(str).str.strip().str.lower() + "||" +
                d[first_name_col].astype(str).str.strip().str.lower())

    df = df.copy()
    df["_key"] = _key(df)

    if len(scanr_res) == 0:
        print("Aucun resultat scanR -> tout le monde part en traitement ORCID/IdRef.")
        return df.iloc[0:0].drop(columns="_key"), df.drop(columns="_key")

    scanr_res = scanr_res.copy()
    scanr_res["_key"] = _key(scanr_res)

    # nb d'identifiants distincts (idref+orcid combines) par nom -> >1
    # signale un homonyme scanR non tranchable automatiquement
    scanr_res["_ident_combo"] = (
        scanr_res["scanr_idref"].fillna("") + "|" + scanr_res["scanr_orcid"].fillna("")
    )
    nb_combos = scanr_res.groupby("_key")["_ident_combo"].nunique()
    noms_ambigus_scanr = set(nb_combos[nb_combos > 1].index)

    has_ident = scanr_res["scanr_idref"].notna() | scanr_res["scanr_orcid"].notna()
    resolvable = scanr_res[has_ident & ~scanr_res["_key"].isin(noms_ambigus_scanr)]
    resolved_map = resolvable.drop_duplicates(subset="_key", keep="first").set_index("_key")

    is_resolved = df["_key"].isin(resolved_map.index)
    resolved = df[is_resolved].copy()
    remaining = df[~is_resolved].copy()

    resolved["orcid_id_final"] = resolved["_key"].map(resolved_map["scanr_orcid"])
    resolved["idref_id"] = resolved["_key"].map(resolved_map["scanr_idref"])
    resolved["idref_source"] = "scanr"
    resolved["idref_gender"] = resolved["_key"].map(resolved_map["scanr_gender"])
    resolved["idref_description"] = resolved["_key"].map(resolved_map["scanr_domains"])

    employer_cols = ["employer_name", "employer_role", "employer_department",
                      "employer_start_date", "employer_end_date", "employer_city",
                      "employer_region", "employer_country", "employer_org_id",
                      "employer_org_id_source"]
    for c in employer_cols:
        resolved[c] = resolved["_key"].map(resolved_map[c])

    n_ambigus = len(set(scanr_res[scanr_res["_key"].isin(noms_ambigus_scanr)]["_key"]))
    print(f"scanR : {len(resolved)} personne(s) resolue(s) sans ambiguite, "
          f"{n_ambigus} nom(s) ambigu(s) (identifiants differents) -> renvoyes en traitement, "
          f"{len(remaining) - n_ambigus} absente(s) de scanR -> renvoyees en traitement")
    print(f"Total a traiter via ORCID/IdRef : {len(remaining)} personne(s)")

    return resolved.drop(columns="_key"), remaining.drop(columns="_key")