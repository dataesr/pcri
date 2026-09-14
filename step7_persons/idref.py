"""
Enrichissement IdRef, en complément du pipeline ORCID existant
(orcid_runner / affiliation_orcid).

Logique :
  1. Si un orcid_id_final est déjà connu (trouvé par orcid_runner ou fourni
     en entrée) -> résolution DIRECTE idref via SPARQL (data.idref.fr,
     owl:sameAs). Rapide, fiable, aucune ambiguïté de nom possible.
  2. Sinon, ou si l'étape 1 échoue -> recherche par nom via pydref
     (Solr IdRef), en testant "prenom nom" PUIS "nom prenom" inversé
     (certaines sources arrivent avec les champs permutés).
  3. Si une notice idref est trouvée ET qu'elle contient un orcid ET
     qu'un orcid_id_final était déjà connu -> comparaison et flag
     idref_orcid_match (True/False/None).
  4. Mapping des infos idref (job/description) vers le même schéma de
     colonnes que employers_history (orc_final), pour pouvoir les
     concaténer facilement plus tard.

Usage :
    from idref_runner import idref_runner
    res = idref_runner(persons_final, label="ERC")
"""

import os
import re
import time
import json
import warnings
import pandas as pd
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from SPARQLWrapper import SPARQLWrapper, JSON as SPARQL_JSON

from paths import PATH_HARVEST

# Silence l'avertissement bs4 déclenché à l'intérieur de pydref.py lui-même
# (BeautifulSoup(notice, 'lxml') sans préciser features="xml") -> pas
# modifiable sans toucher au fichier source de la librairie.
warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# ---------------------------------------------------------------
# pydref : chargement par chemin de fichier (pas de package pip)
# ---------------------------------------------------------------
import importlib.util

PYDREF_PATH = r"C:/Users/zfriant/Onedrive/GitHub/pydref/pydref.py"  # <- ajuste si besoin
spec = importlib.util.spec_from_file_location("pydref_module", PYDREF_PATH)
pydref_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(pydref_module)
Pydref = pydref_module.Pydref
pydref = Pydref()


# -----------------------------------------------------------------------
# Correctif runtime d'un bug de pydref.get_identifiers_from_idref_notice.
# Le champ MARC 035 a deux sous-champs distincts :
#   $2 = TYPE réel de l'identifiant (ex: ORCID, SCOPUSID, SUDOC)
#   $C = SOURCE via laquelle il a été rattaché à la notice (peut valoir
#        "ORCID" même pour un identifiant Scopus retrouvé via ORCID !)
# Le code d'origine de pydref regarde n'importe quel sous-champ (donc $C
# inclus) pour décider si un 035 est un ORCID -> un ID Scopus rattaché via
# ORCID ($C=ORCID, $2=SCOPUSID) se retrouve étiqueté "orcid" à tort.
# On patche la méthode pour se baser uniquement sur $2 (le vrai type).
# -----------------------------------------------------------------------
import types

_ID_TYPE_MAP = {
    "ORCID": "orcid",
    "SUDOC": "sudoc",
    "SCOPUSID": "scopus",
    "ISNI": "isni",
}


def _get_identifiers_from_idref_notice_fixed(self, soup):
    identifiers = []
    for datafield in soup.findAll("datafield"):
        tag = datafield.attrs.get("tag")

        if tag == "024":
            for subfield in datafield.findAll("subfield"):
                if subfield.attrs.get("code") == "a":
                    identifiers.append({"isni": subfield.text.strip()})
                    break

        elif tag == "033":
            for subfield in datafield.findAll("subfield"):
                if subfield.attrs.get("code") == "a":
                    identifiers.append({"ark": subfield.text.strip()})
                    break

        elif tag == "035":
            value, id_type = None, None
            for subfield in datafield.findAll("subfield"):
                code = subfield.attrs.get("code")
                if code == "a":
                    value = subfield.text.strip()
                elif code == "2":  # $2 = type réel, PAS $C (source)
                    id_type = subfield.text.strip().upper()
            if value and id_type:
                key = _ID_TYPE_MAP.get(id_type, id_type.lower())
                identifiers.append({key: value})

    return identifiers


pydref.get_identifiers_from_idref_notice = types.MethodType(
    _get_identifiers_from_idref_notice_fixed, pydref
)

PATH_PERSONS = f"{PATH_HARVEST}persons/"

# Même schéma de colonnes que employers_history (orc_final), pour pouvoir
# concaténer les deux sources plus tard sans renommage.
EMPLOYER_SCHEMA = [
    "employer_name", "employer_role", "employer_department",
    "employer_start_date", "employer_end_date", "employer_city",
    "employer_region", "employer_country", "employer_org_id",
    "employer_org_id_source",
]


# ---------------------------------------------------------------
# 1. Résolution directe ORCID -> idref via SPARQL
# ---------------------------------------------------------------
def idref_ppn_from_orcid(orcid_id: str, timeout: int = 5):
    """Retourne le PPN idref (ex: '069739208') à partir d'un ORCID, ou None."""
    if not orcid_id or pd.isna(orcid_id):
        return None

    sparql = SPARQLWrapper("https://data.idref.fr/sparql")
    sparql.setTimeout(timeout)
    query = f"""
    PREFIX owl: <http://www.w3.org/2002/07/owl#>
    SELECT ?idref WHERE {{
        ?idref owl:sameAs <https://orcid.org/{orcid_id}> .
    }}
    """
    sparql.setQuery(query)
    sparql.setReturnFormat(SPARQL_JSON)
    try:
        results = sparql.query().convert()
        bindings = results["results"]["bindings"]
        if bindings:
            uri = bindings[0]["idref"]["value"]
            # Les URIs data.idref.fr se terminent souvent par "/id"
            # (convention Linked Data) : http://www.idref.fr/069739208/id
            # -> on veut le PPN (069739208), pas le suffixe "id".
            segments = uri.rstrip("/").split("/")
            ppn = segments[-1]
            if ppn.lower() == "id" and len(segments) >= 2:
                ppn = segments[-2]
            if not ppn.isdigit():
                print(f"[sparql] URI idref inattendue, ppn non numérique ignoré: {uri}")
                return None
            return ppn
    except Exception as e:
        print(f"[sparql] erreur orcid={orcid_id}: {e}")
    return None


ORCID_PATTERN = re.compile(r"^\d{4}-\d{4}-\d{4}-\d{3}[\dX]$")


def _is_valid_orcid_format(value: str) -> bool:
    return bool(value) and bool(ORCID_PATTERN.match(str(value).strip()))


def _extract_orcid(identifiers):
    """
    identifiers = [{'idref': ...}, {'orcid': ...}, {'isni': ...}, ...]
    Certaines notices idref contiennent PLUSIEURS entrées taguées "orcid",
    dont une correspondant en réalité à un autre identifiant mal étiqueté
    dans la notice (ex: un ID Scopus à 11 chiffres). On ne retient que
    celles au format ORCID valide (xxxx-xxxx-xxxx-xxxx, dernier caractère
    pouvant être X). En cas de plusieurs candidats valides, on garde le
    premier (cas rare, mais possible en cas de doublon réel dans la notice).
    """
    candidates = [d["orcid"] for d in (identifiers or []) if "orcid" in d]
    valid = [c for c in candidates if _is_valid_orcid_format(c)]
    if valid:
        return valid[0]
    return None


def _notice_to_dict(ppn, source_tag):
    """Télécharge + parse une notice idref à partir de son PPN."""
    notice = pydref.get_idref_notice(ppn)
    if not notice:
        return None
    soup = BeautifulSoup(notice, "lxml-xml")
    name_info = pydref.get_name_from_idref_notice(soup)
    identifiers = pydref.get_identifiers_from_idref_notice(soup)
    return {
        "idref_id": f"idref{ppn}",
        "idref_source": source_tag,
        "idref_last_name": name_info.get("last_name"),
        "idref_first_name": name_info.get("first_name"),
        "idref_job": name_info.get("job"),
        "idref_gender": pydref.get_gender(soup),
        "idref_description": " | ".join(pydref.get_description_from_idref_notice(soup)),
        "idref_identifiers": json.dumps(identifiers, ensure_ascii=False),
        "idref_orcid_found": _extract_orcid(identifiers),
        "idref_nb_candidats": 1,
    }


# ---------------------------------------------------------------
# 2. Recherche par nom (Solr IdRef), avec inversion nom/prénom
# ---------------------------------------------------------------
def _search_by_name(last_name, first_name, **identify_kwargs):
    """
    Essaie "prenom nom" puis, si pas trouvé, "nom prenom" inversé.
    Retourne (idres, name_order_used) où name_order_used est
    'normal', 'inverse' ou None si rien de concluant.
    """
    last_name = (last_name or "").strip()
    first_name = (first_name or "").strip()

    attempts = [("normal", f"{first_name} {last_name}".strip())]
    if first_name and last_name:
        attempts.append(("inverse", f"{last_name} {first_name}".strip()))

    best = None
    best_order = None
    for order, full_name in attempts:
        if not full_name:
            continue
        idres = pydref.identify(full_name, **identify_kwargs)
        status = idres.get("status")
        if status == "found":
            return idres, order
        # on garde en mémoire le meilleur résultat "ambigu" au cas où
        # aucun des deux essais ne donne "found"
        if best is None or (status == "not_found_ambiguous" and best.get("status") == "not_found"):
            best = idres
            best_order = order

    return best, best_order


# ---------------------------------------------------------------
# 3. Résolution d'une personne (orcid direct puis repli nom)
# ---------------------------------------------------------------
def search_idref_person(last_name, first_name, orcid_id=None,
                         min_birth_year=1920, min_death_year=2005):

    result = {
        "last_name": last_name,
        "first_name": first_name,
        "orcid_id_final": orcid_id,
        "idref_id": None,
        "idref_source": "non_trouve",       # orcid_reverse | nom_trouve | nom_trouve_inverse | ambigu | non_trouve
        "idref_last_name": None,
        "idref_first_name": None,
        "idref_job": None,
        "idref_gender": None,
        "idref_description": None,
        "idref_identifiers": None,
        "idref_orcid_found": None,
        "idref_orcid_match": None,          # True / False / None (pas de comparaison possible)
        "idref_nb_candidats": 0,
        "idref_name_order_used": None,
    }

    has_orcid = orcid_id is not None and not pd.isna(orcid_id) and str(orcid_id).strip() != ""

    # --- 1. Résolution directe via ORCID ---
    if has_orcid:
        ppn = idref_ppn_from_orcid(orcid_id)
        if ppn:
            info = _notice_to_dict(ppn, "orcid_reverse")
            if info:
                result.update(info)
                # trouvé PAR l'orcid lui-même : cohérence garantie par construction
                result["idref_orcid_match"] = True
                return result

    # --- 2. Repli : recherche par nom (avec inversion) ---
    idres, order = _search_by_name(
        last_name, first_name,
        min_birth_year=min_birth_year,
        min_death_year=min_death_year,
        is_scientific=True,
        is_exact_fullname=False,
    )

    if idres is None:
        return result

    result["idref_nb_candidats"] = idres.get("nb_homonyms", 0)
    status = idres.get("status")

    if status == "found":
        found_orcid = _extract_orcid(idres.get("identifiers"))
        match = None
        if has_orcid and found_orcid:
            match = (str(found_orcid).strip() == str(orcid_id).strip())

        result.update({
            "idref_id": idres.get("idref"),
            "idref_source": "nom_trouve" if order == "normal" else "nom_trouve_inverse",
            "idref_last_name": idres.get("last_name"),
            "idref_first_name": idres.get("first_name"),
            "idref_job": idres.get("job"),
            "idref_gender": idres.get("gender"),
            "idref_description": " | ".join(idres.get("description", [])),
            "idref_identifiers": json.dumps(idres.get("identifiers", []), ensure_ascii=False),
            "idref_orcid_found": found_orcid,
            "idref_orcid_match": match,
            "idref_name_order_used": order,
        })
    elif status == "not_found_ambiguous":
        result["idref_source"] = "ambigu"
        result["idref_identifiers"] = json.dumps(idres.get("potential_matches", []), ensure_ascii=False)
        result["idref_name_order_used"] = order

    return result


# ---------------------------------------------------------------
# 4. Mapping vers le schéma employers_history (best effort)
# ---------------------------------------------------------------
def to_employer_row(idref_row: dict) -> dict:
    """
    IdRef n'a pas de champs structurés employer_city/country/dates comme
    ORCID -> on ne remplit que ce qui est disponible (job en guise de
    role/nom d'employeur, idref_id en org_id).
    """
    row = {c: None for c in EMPLOYER_SCHEMA}
    row["employer_role"] = idref_row.get("idref_job")
    row["employer_org_id"] = idref_row.get("idref_id")
    row["employer_org_id_source"] = "idref" if idref_row.get("idref_id") else None
    return row


# ---------------------------------------------------------------
# 5. Runner avec checkpoint (reprise possible)
# ---------------------------------------------------------------
def idref_runner(df: pd.DataFrame, label: str,
                  last_name_col="last_name", first_name_col="first_name",
                  orcid_col="orcid_id_final",
                  batch_size=50, sleep_s=0.3):
    """
    df doit contenir au moins last_name_col, first_name_col, et de
    préférence orcid_col (peut contenir des NaN pour les personnes sans
    orcid connu).
    """
    checkpoint_dir = f"{PATH_PERSONS}checkpoint"
    os.makedirs(checkpoint_dir, exist_ok=True)
    checkpoint_file = f"{checkpoint_dir}/{label.lower()}_idref_checkpoint.csv"

    if os.path.exists(checkpoint_file):
        done = pd.read_csv(checkpoint_file)
        done_keys = set(zip(
            done.last_name.astype(str).str.lower(),
            done.first_name.astype(str).str.lower(),
        ))
        print(f"[{label}] {len(done)} ligne(s) déjà traitée(s), reprise...")
    else:
        done = pd.DataFrame()
        done_keys = set()

    rows = []
    total = len(df)
    for i, (_, row) in enumerate(df.iterrows(), start=1):
        last_name = row.get(last_name_col)
        first_name = row.get(first_name_col)
        key = (str(last_name).lower(), str(first_name).lower())
        if key in done_keys:
            continue

        orcid_id = row.get(orcid_col) if orcid_col in df.columns else None
        res = search_idref_person(last_name, first_name, orcid_id)
        rows.append(res)

        if len(rows) % batch_size == 0:
            partial = pd.concat([done, pd.DataFrame(rows)], ignore_index=True)
            partial.to_csv(checkpoint_file, index=False)
            print(f"[{label}] {len(partial)}/{total} ligne(s) sauvegardée(s)...")

        time.sleep(sleep_s)  # ménager l'API IdRef / le triple store

    final = pd.concat([done, pd.DataFrame(rows)], ignore_index=True)
    final.to_csv(checkpoint_file, index=False)

    print(f"[{label}] Terminé : {len(final)} ligne(s) -> {checkpoint_file}")
    print(final["idref_source"].value_counts(dropna=False).to_string())
    if "idref_orcid_match" in final.columns:
        mismatches = final[final["idref_orcid_match"] == False]  # noqa: E712
        if len(mismatches):
            print(f"\n⚠️  {len(mismatches)} mismatch(es) ORCID <-> IdRef à vérifier :")
            print(mismatches[["last_name", "first_name", "orcid_id_final", "idref_orcid_found"]].to_string(index=False))

    return final