import pandas as pd, requests, numpy as np, math
from config_api import grist_headers
from config_url import grist_url

"""
list docs in workspaces
list tables in docs
"""

GRIST_TYPE_CAST = {
    "Text":          str,
    "Numeric":       float,
    "Int":           int,
    "Bool":          bool,
    "Date":          str,
    "DateTime":      str,
    "Choice":        str,
    "ChoiceList":    str,
    "Reference":     int,
    "ReferenceList": str,
}


def cast_value(value, grist_type: str):
    """
    Force le cast d'une valeur selon le type Grist de la colonne.
    """
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None

    cast_fn = GRIST_TYPE_CAST.get(grist_type)
    if cast_fn is None:
        return value  # type inconnu → on laisse tel quel

    try:
        if cast_fn == bool:
            if isinstance(value, str):
                return value.strip().lower() in ("true", "1", "yes", "oui")
            return bool(value)
        return cast_fn(value)
    except (ValueError, TypeError):
        print(f"⚠️  Impossible de caster '{value}' en {grist_type}, valeur laissée telle quelle.")
        return value

def grist_fetch_docs(org_id, grist_space: list):
    """
    fetch doc identifier for each doc 
    args:
        org_id : 'dataesr'
        grist_space : workspace name ['pcri', 'nomenclatures']
    return:
        dict for each doc name its ID
    """
    url=f"{grist_url}orgs/{org_id}/workspaces"
    r=requests.get(url, headers=grist_headers)
    r=r.json()
    return {doc['name']: doc['id'] for item in r for doc in item['docs'] if item['name'] in (grist_space)}

def grist_list_tables(doc_id) -> list:
    """
    fetch tables per doc
    """
    url = f"{grist_url}docs/{doc_id}/tables"
    r=requests.get(url, headers=grist_headers)
    return [i['id'] for i in r.json()['tables']]

#############################################################
# ── Helper: 
def clean_value(x):
    """
    clean invalid data
    """
    if isinstance(x, float) and (math.isnan(x) or math.isinf(x)):
        return None
    return x
    
def find_bad_values(df):
    """
    if import failed check bad value in df
    """
    for col in df.select_dtypes(include="float").columns:
        bad = df[col].apply(lambda x: isinstance(x, float) and (math.isnan(x) or math.isinf(x)))
        if bad.any():
            print(f"⚠️  Colonne '{col}' : {bad.sum()} valeurs invalides")

##########################################################################
# ── Step 1 – Create the table with columns ───────────────────────────────────
def create_table(df: pd.DataFrame, grist_url, doc_id, table_name) -> bool:

    """
    create table structure in grist
    args:
        df : data to load
        grist_url : url base
        doc_id : doc containing table
        table_name
    
    """

    grist_headers["Content-Type"] = "application/json"
    grist_headers["accept"] = "application/json"

    DTYPE_MAP = {
    "int64": "Int",
    "float64": "Numeric",
    "bool": "Bool",
    "object": "Text",
    "datetime64[ns]": "DateTime"
    }


    #Create a Grist table whose columns match the DataFrame
    columns = [
        {"id": col, 
         "fields": {
             "label": col, # ← id explicite
             "type": DTYPE_MAP.get(str(df[col].dtype), "Text")
             }
        }  
        for col in df.columns
    ]
    payload = {"tables": [
                {   "id": table_name, 
                    "columns": columns}]}
 
    url = f"{grist_url}docs/{doc_id}/tables"
    response = requests.post(url, headers=grist_headers, json=payload)
 
    if response.status_code in (200, 201):
        print(f"✅ Table '{table_name}' created successfully.")
        return True
    elif response.status_code == 409:
        print(f"ℹ️  Table '{table_name}' already exists – skipping creation.")
        return True
    else:
        print(f"❌ Failed to create table: {response.status_code} – {response.text}")
        return False
 
 
def delete_table(grist_url, doc_id, table_name) -> bool:
    """
    delete Grist table via l'API useractions.
    """
    url = f"{grist_url}docs/{doc_id}/apply"
    payload = [["RemoveTable", table_name.capitalize()]]
    
    response = requests.post(url, headers=grist_headers, json=payload)
    
    if response.status_code == 200:
        print(f"✅ Table '{table_name}' supprimée.")
        return True
    else:
        print(f"❌ Échec suppression : {response.status_code} – {response.text}")
        return False


# ── Step 2 – Insert rows ─────────────────────────────────────────────────────
def insert_rows(df: pd.DataFrame, grist_url, doc_id, table_name) -> bool:
    """
    Insert all DataFrame rows into the Grist table.
    args:
        df : data to load
        grist_url : url base
        doc_id : doc containing table
        table_name : table id to be capitalize
    """
    grist_headers["Content-Type"] = "application/json"
    grist_headers["accept"] = "application/json"

    records = [
        {"fields": {k: clean_value(v) for k, v in row.to_dict().items()}}
        for _, row in df.iterrows()
    ]

    # Découpage en chunks
    CHUNK_SIZE = 100  # à ajuster selon la taille de tes données

    def chunked(lst, size):
        for i in range(0, len(lst), size):
            yield lst[i:i + size]

    success, errors = 0, []

    for chunk in chunked(records, CHUNK_SIZE):
        payload = {"records": chunk}
        response = requests.post(
            f"{grist_url}docs/{doc_id}/tables/{table_name.capitalize()}/records",
            headers=grist_headers,
            json=payload,
    )
        
        if response.status_code == 200:
            success += len(chunk)
        else:
            errors.append(response.json())

    print(f"✅ Insertion : {success} lignes")
    if errors:
        print(f"❌ Erreurs : {errors}")
 


# ── or – add rows ─────────────────────────────────────────────────────

def add_rows(df: pd.DataFrame, grist_url, workspace, doc_name, table_name) -> bool:
    """
    add rows into the existing Grist table.
    args:
        df : data to load
        grist_url : url base
        doc_id : doc containing table
        table_name : table id to be capitalize
    """
    grist_headers["Content-Type"] = "application/json"
    grist_headers["accept"] = "application/json"

    doc_dict = grist_fetch_docs('dataesr', [workspace])
    doc_id = doc_dict[doc_name]

    records = [
        {"fields": {k: clean_value(v) for k, v in row.to_dict().items()}}
        for _, row in df.iterrows()
    ]

    payload = {"records": records}
    response = requests.post(
        f"{grist_url}docs/{doc_id}/tables/{table_name.capitalize()}/records",
        headers=grist_headers,
        json=payload,
    )


    if response.status_code == 200:
        print(f"✅ Insertion : {len(records)} lignes") 
    else:
        print(f"❌ Erreurs : {response.json()}")
     

# ── Pipeline principal ────────────────────────────────────────────────────────
def load_to_grist(df: pd.DataFrame, grist_url, workspace, doc_name, table_name) -> None:
    """
    load data to grist one by one
    args:
        df : data to load
        grist_url : url base
        workspace : specify which space to load it in
        doc_id : specify which doc to load it in
        table_name

    
    """
    print(f"📊 DataFrame : {df.shape[0]} lignes × {df.shape[1]} colonnes\n")
    doc_dict = grist_fetch_docs('dataesr', [workspace])
    doc_id = doc_dict[doc_name]
    tabs = grist_list_tables(doc_id)

    if table_name.lower() in  [tab.lower() for tab in tabs]:
        delete_table(grist_url, doc_id, table_name)

    create_table(df, grist_url, doc_id, table_name)
    insert_rows(df, grist_url, doc_id, table_name)



# ── Step 3 (optional) – Upsert / patch existing rows ────────────────────────
def get_grist_table_columns(grist_url, doc_id, table_name) -> list[str]:
    """
    Fetch list of table columns + types from Grist.
    Returns a dict {col_id: grist_type}
    """
    grist_headers["accept"] = "application/json"
    response = requests.get(
        f"{grist_url}docs/{doc_id}/tables/{table_name.capitalize()}/columns",
                    headers=grist_headers)
    
    response.raise_for_status()
    columns = response.json().get("columns", [])
    # On exclut la colonne système 'manualSort' automatiquement gérée par Grist
    return {
        col["id"]: col["fields"]["type"]
        for col in columns
        if col["id"] != "manualSort"
    }


def add_records_to_grist(df: pd.DataFrame, grist_url, workspace, doc_name, table_name) -> dict:
    """
    Ajoute les records d'un DataFrame dans une table Grist.
    Les colonnes sont récupérées automatiquement depuis l'API.
    Les colonnes Grist absentes du DataFrame sont mises à None.
    Les colonnes du DataFrame absentes de Grist sont ignorées.

    Args:
        df         : DataFrame contenant les données à insérer
        table_name : Nom de la table Grist cible
        doc_id     : ID du document Grist
        api_key    : Clé API Grist
        grist_url  : URL de base de l'instance Grist

    Returns:
        La réponse JSON de l'API Grist
    """
    grist_headers["Content-Type"] = "application/json"
    grist_headers["accept"] = "application/json"
    
    doc_dict = grist_fetch_docs('dataesr', [workspace])
    doc_id = doc_dict[doc_name]

    # 1. Récupération automatique des colonnes de la table
    grist_columns = get_grist_table_columns(grist_url, doc_id, table_name)
    print(f"📋 Colonnes détectées dans '{table_name}':")
    for col, t in grist_columns.items():
        print(f"   - {col}: {t}")

    # 2. Construction des records
    records = []
    for _, row in df.iterrows():
        fields = {}
        for col, grist_type in grist_columns.items():
            raw_value = row[col] if col in df.columns else None
            fields[col] = cast_value(raw_value, grist_type)
        records.append({"fields": fields})

    # 3. Envoi à l'API
    payload = {"records": records}
    response = requests.post(
        f"{grist_url}docs/{doc_id}/tables/{table_name.capitalize()}/records",
        headers=grist_headers,
        json=payload
    )
    response.raise_for_status()
    result = response.json()
    print(f"✅ {len(records)} record(s) ajouté(s) dans '{table_name}'.")


# def grist_doc_name(doc_id):
#     url=f"{grist_url}docs/{doc_id}"
#     r=requests.get(url, headers=grist_headers)
#     r=r.json()['name']
#     return r


def fetch_one_table_grist(doc_id, table_id):
    url=f"{grist_url}docs/{doc_id}/tables/{table_id}/records"
    r=requests.get(url, headers=grist_headers)
    r=r.json()
    r=r.get("records", [])

    return r

def load_grist_tables(doc_id) -> dict:
    """
    load all tables in a doc
    create dict to call
    """

    tables = grist_list_tables(doc_id)

    referentiel = {}
    
    for table in tables:
        records = fetch_one_table_grist(doc_id, table)
        
        if records:
            # Aplatir id + fields en une seule ligne par enregistrement
            rows = [r["fields"] for r in records]
            referentiel[table] = pd.DataFrame(rows)
        else:
            referentiel[table] = pd.DataFrame()
        
        print(f"✓ Table '{table}' chargée : {len(referentiel[table])} lignes")
    
    return referentiel

geoG = {}
categoriesG = {}
communesG = {}
countryG = {}
idsG = {}

_initialized = False

def init_data_grist():
    global geoG, categoriesG, communesG, countryG, idsG, _initialized

    if _initialized:
        return

    docs_dict = grist_fetch_docs('dataesr', ['pcri', 'nomenclatures'])

    geoG.update(load_grist_tables(docs_dict['geo']))
    categoriesG.update(load_grist_tables(docs_dict['categories']))
    communesG.update(load_grist_tables(docs_dict['communes']))
    countryG.update(load_grist_tables(docs_dict['pays']))
    idsG.update(load_grist_tables(docs_dict['identification']))

    _initialized = True

init_data_grist()

def update_doc_grist(dict, doc):
    """
    if modify tables reload data grist
    """
    docs_dict = grist_fetch_docs('dataesr', ['pcri', 'nomenclatures'])
    dict.update(load_grist_tables(docs_dict[doc]))


