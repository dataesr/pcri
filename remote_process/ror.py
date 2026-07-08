import requests, json, pandas as pd, ast, os
import shutil
from tempfile import mkdtemp
from zipfile import ZipFile
from config_path import PATH_REF, PATH
from functions_shared import work_csv
from dotenv import load_dotenv
load_dotenv()
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning) 

def strip_ror(x):
    """Remove https://ror.org/ and return bare ROR."""
    if isinstance(x, str):
        return x.replace("https://ror.org/", "").strip()
    return x
########################################

def get_last_ror_dump_url():
    ROR_URL = "https://zenodo.org/api/communities/ror-data/records?q=&sort=newest"
    response = requests.get(url=ROR_URL).json()
    ror_dump_url = response['hits']['hits'][0]['files'][-1]['links']['self']
    print(f'Last ROR dump url found: {ror_dump_url}')
    return ror_dump_url

def ror_load():
    ror_downloaded_file=f'{PATH}referentiel/ror_data_dump.zip'
    ror_unzipped_folder = mkdtemp()
    with ZipFile(file=ror_downloaded_file, mode='r') as file:
        file.extractall(ror_unzipped_folder)

    for data_file in os.listdir(ror_unzipped_folder):
        if data_file.endswith('.json'):
            with open(f'{ror_unzipped_folder}/{data_file}', 'r') as file:
                data = json.load(file)
    shutil.rmtree(path=ror_unzipped_folder)
    return data   

def ror_load_url():
    ror_downloaded_file=f'{PATH}referentiel/ror_data_dump.zip'
    url_site = get_last_ror_dump_url()
    response = requests.get(url_site, stream=True)

    CHUNK_SIZE=128
    with open(file=ror_downloaded_file, mode='wb') as file:
        for chunk in response.iter_content(chunk_size=CHUNK_SIZE):
            file.write(chunk)
    return ror_load()

#######################

def ror_names(entry: dict):
    results = []
    names = entry.get('names', [])
    locs = entry.get('locations', [])

    # Extraire le code pays de la première localisation si disponible
    country_code = None
    if locs:
        first_loc = locs[0]
        country_code = first_loc.get('geonames_details', {}).get('country_code', '').lower()

    # Initialiser les valeurs pour chaque type de nom
    name_usual = None
    name_en = None
    name_local = None

    # Trier les noms pour donner la priorité au français
    sorted_names = sorted(names, key=lambda x: (x.get('lang') != 'fr', x.get('lang') != 'en'))

    # Parcourir les noms pour remplir les colonnes selon la hiérarchie
    for name in sorted_names:
        value = name.get('value')
        types = name.get('types', [])
        lang = name.get('lang')

        # 1. Priorité absolue : label et lang=='fr' → name_usual
        if lang == 'fr' and 'label' in types and name_usual is None:
            name_usual = value
        # 2. Ensuite : ror_display et lang=='fr' → name_usual
        elif lang == 'fr' and 'ror_display' in types and name_usual is None:
            name_usual = value
        # 3. Ensuite : ror_display et lang=='en' → name_en et si name_usual is None → name_usual
        elif lang == 'en' and 'ror_display' in types and name_en is None:
            name_en = value
            if name_usual is None:
                name_usual = value
        # 4. Ensuite : label et lang=='en' → name_en et si name_usual is None → name_usual
        elif lang == 'en' and 'label' in types and name_en is None:
            name_en = value
            if name_usual is None:
                name_usual = value
        # 5. Ensuite : ror_display ou label et ni fr ni en → name_usual si toujours None
        elif ('ror_display' in types or 'label' in types) and lang not in ['fr', 'en'] and name_usual is None:
            name_usual = value

        # name_local : si lang == country_code et ror_display ou label
        if country_code and lang == country_code and ('ror_display' in types or 'label' in types):
            name_local = value

    # Extraire les acronymes et alias
    acronyms = [name.get("value") for name in names if "acronym" in name.get("types", [])]
    alias = [name.get("value") for name in names if "alias" in name.get("types", [])]

    # Ajouter les résultats pour chaque type de nom
    if name_usual is not None:
        results.append({'value': name_usual, 'type': 'name_usual'})
    if name_en is not None:
        results.append({'value': name_en, 'type': 'name_en'})
    if name_local is not None:
        results.append({'value': name_local, 'type': 'name_local', 'lang': country_code})
    if acronyms:
        results.append({'value': acronyms, 'type': 'acronym'})
    if alias:
        results.append({'value': alias, 'type': 'alias'})
    
    return results

def category_ror(entry, key):
    mapping = json.load(open("data_files/ror_types_to_paysage.json"))

    if key=='cj':
        types = [entry['types'][0]]

    else:
        types = entry['types']

    ids = []
    for t in types:
        # Convertir le type en première lettre majuscule pour correspondre aux clés de mapping
        type_capitalized = t.capitalize()
        if type_capitalized in mapping[key]:
            ids.append(mapping[key][type_capitalized])
    return ids

def ror_info(result: list):
    
    delete = ['locations', 'names', 'established', 'admin', 'external_ids', 'links', 'domains']

    to_keep = []
    for p in result:
        if p:
            elem = {k: v for k, v in p.items() if (v and v != "NaT")}  

            names_info = ror_names(elem)

            # Ajouter les résultats de ror_names à elem
            for name_info in names_info:
                name_type = name_info['type']
                elem[f"{name_type}"] = name_info['value']
                if name_type == 'name_local':
                    elem[f"{name_type}_lang"] = name_info['lang']

            # elem['relation_type'] = []
            # elem['relation_id'] = []
            # if elem.get('relationships'):
            #     for rel in elem['relationships']:
            #         if rel.get('type') in ['parent', 'successor']:
            #             elem['relation_type'].append(rel.get('type', None))
            #             elem['relation_id'].append(rel.get('id', None).split('/')[-1])

            if elem.get('locations'):
                for loc in elem.get('locations', []):
                        
                    elem['iso2'] = loc.get('geonames_details', {}).get('country_code')
                    elem['city'] = loc.get('geonames_details', {}).get('name')
                    elem['latitude'] = loc.get('geonames_details', {}).get('lat')
                    elem['longitude'] = loc.get('geonames_details', {}).get('lng')

                    elem['geo_admin1_code'] = loc.get('geonames_details', {}).get('country_subdivision_code')
                    elem['geo_admin1_name'] = loc.get('geonames_details', {}).get('country_subdivision_name')

            if elem.get('external_ids'):  
                for ext_id in elem.get('external_ids', []):
                    ext_id_type = ext_id.get('type')
                    ext_id_all = ext_id.get('all', [])
                    elem[f'{ext_id_type}'] = ';'.join(ext_id_all)
                    if ext_id.get('preferred'):
                        elem[f'{ext_id_type}_preferred'] = ext_id.get('preferred')
                    
            if elem.get('links'):        # Extracting links information
                for link in elem.get('links', []):
                    link_type = link.get('type')
                    link_value = link.get('value')
                # Joining list elements into semicolon-separated string
                elem[f'link_{link_type}'] = link_value

            elem['link_ror'] = elem.get('id')
            elem['id'] = elem.get('id').split('/')[-1]
            elem['year'] = str(elem.get('established'))

            elem['cj'] = category_ror(elem, 'cj')
            elem['paysageCat'] = category_ror(elem, 'cat')

            l = ['types', 'cj', 'paysageCat', 'alias', 'acronym']
            # l = ['types', 'relation_type', 'relation_id', 'cj', 'paysageCat', 'alias', 'acronym']
            for e in l:
                if elem.get(e):
                    elem[e] = ';'.join([code for code in elem.get(e) if code is not None])

            for field in delete:
                if elem.get(field):
                    elem.pop(field)

            elem = {k: v for k, v in elem.items() if (v and v != "NaT")}
            to_keep.append(elem)
        
    return to_keep
#########################

def ror_parent(r, ri_rid):

    # ---------------------------------------------------------
    # 1. REMOVE PREFIX FUNCTION
    # ---------------------------------------------------------

    df = pd.DataFrame(r)
    print(len(df))
    df['id'] = df['id'].map(strip_ror)
    df = df.loc[df['id'].isin(list(ri_rid))]
    print(len(df))

    child_map = {}

    for _, row in df.iterrows():
        rid = row["id"].strip()


        for rel in row["relationships"]:
            if rel.get("type") == "child":
                target = rel.get("id")

                if isinstance(target, str):
                    target = target.strip()
                if isinstance(target, str) and target.startswith("https://ror.org/"):
                    child_map.setdefault(rid, []).append(target)


    # ---------------------------------------------------------
    # 4. BUILD RESULTS DATAFRAME (WITH LABEL)
    # ---------------------------------------------------------

    rows = []

    for rid, childs in child_map.items():
        rows.append({
            "id": strip_ror(rid),
            "child_count": len(childs),
            "childs": [strip_ror(c) for c in childs]
        })

    results_df = pd.DataFrame(rows).sort_values("child_count", ascending=False)

    work_csv(results_df.sort_values("child_count", ascending=False), 'ror_child_count.csv')



def ror_relation(ri, ri_rid):

    df = pd.DataFrame(ri)
    print(len(df))

    def parse_rels(x):
        # Already parsed
        if isinstance(x, list):
            return x

        # Missing
        if x is None or (isinstance(x, float) and pd.isna(x)):
            return []

        # Parse string
        if isinstance(x, str):
            s = x.strip()
            if s == "" or s == "[]":
                return []
            try:
                return ast.literal_eval(s)
            except (ValueError, SyntaxError):
                # fallback: sometimes it's JSON not python literal
                try:
                    return json.loads(s)
                except Exception:
                    return []

        return []

    df["relationships"] = df["relationships"].apply(parse_rels)

    def clean_rel_ids(rels):
        if not isinstance(rels, list):
            return []
        for r in rels:
            if isinstance(r, dict) and isinstance(r.get("id"), str):
                r["id"] = strip_ror(r["id"])
        return rels

    df["relationships"] = df["relationships"].apply(clean_rel_ids)


    # ---------------------------------------------------------
    # 2. BUILD SUCCESSOR + PARENT MAPS
    # ---------------------------------------------------------

    successor_map = {}
    parent_map = {}

    for _, row in df.iterrows():
        rid = row["id"]
        for rel in row["relationships"]:
            rtype = rel.get("type")
            target = rel.get("id")

            if not isinstance(target, str):
                continue

            if rtype == "successor":
                successor_map.setdefault(rid, []).append(target)

            if rtype == "parent":
                parent_map.setdefault(rid, []).append(target)

    # ---------------------------------------------------------
    # 3. HELPERS
    # ---------------------------------------------------------

    def last_in_chain(start, graph):
        visited = set()
        curr = start
        while curr in graph and graph[curr]:
            if curr in visited:
                break
            visited.add(curr)
            curr = graph[curr][0]
        return curr

    def highest_parent_not_deleted(start, delete_dict):
        visited = set()
        curr = start
        best = None

        while curr in parent_map and parent_map[curr]:
            if curr in visited:
                break
            visited.add(curr)
            curr = parent_map[curr][0]
            if curr not in delete_dict:
                best = curr

        return best

    # ---------------------------------------------------------
    # 4. DELETE DICTIONARY
    # ---------------------------------------------------------
    delete_dict = json.load(open("data_files/ror_parent_delete.json"))

    # ---------------------------------------------------------
    # 5. BUILD FINAL results_df
    # ---------------------------------------------------------

    rows = []

    for root_id, iso_root in ri_rid.items():
        last_succ = last_in_chain(root_id, successor_map)

        canonical_id = last_succ if last_succ else root_id

        top_parent = highest_parent_not_deleted(canonical_id, delete_dict)

        id_clean = top_parent or canonical_id or root_id

        rows.append({
            "id_source": root_id,
            "iso_root": iso_root,
            "canonical_id": canonical_id,
            "top_parent_not_deleted": top_parent,
            "id_clean": id_clean
        })

    res = pd.merge(pd.DataFrame(rows), df[['id', 'iso2']], how='left', left_on='id_clean', right_on='id').drop(columns='id')
    res.loc[res['iso_root']!=res['iso2'], 'id_clean'] = res.loc[res['iso_root']!=res['iso2'], 'canonical_id']
    res.loc[res['id_clean'].isnull(), 'id_clean'] = res.loc[res['id_clean'].isnull(), 'id_source']
    
    return res

    
############################
def get_ror(id_source, id_var, countries, load_url=True):
    
    ror_list = list(id_source.loc[id_source['source_id'].isin(['ror']), id_var].unique())
    print(f"nombre d'identifiants ror à extraire: {len(ror_list)}")

    if load_url:
        r = ror_load_url()
    else:
        r = ror_load()
    
    ri = ror_info(r)
    ri_rid = {
        i['id']: i['iso2']
        for i in ri
        if i.get('iso2') != 'FR' and i.get('id') in ror_list
    }
    # ror_parent(r, ri_rid)

    relation = ror_relation(ri, ri_rid)
    print(len(relation))

    if not relation.empty:
        tmp = (pd.merge(pd.DataFrame(ri)[['id']],
                       relation[['id_source', 'id_clean']], 
                        how='left', 
                        left_on='id', 
                        right_on='id_source')
                    .drop(columns='id_source')
                    )

        tmp = tmp.loc[tmp['id'].isin(ror_list)].rename(columns={'id':'id_source'})
        tmp.loc[tmp['id_clean'].isnull(), 'id_clean'] = tmp.loc[tmp['id_clean'].isnull(), 'id_source']
        print(len(tmp))

        tmp = (pd.merge(tmp,
                        pd.DataFrame(ri).drop(columns='relationships'),
                        how='left', 
                        left_on='id_clean', 
                        right_on='id')
                .drop(columns='id')
        )
        print(len(tmp))
    
    work_csv(tmp.loc[tmp['id_source']!=tmp['id_clean'], ['id_source', 'id_clean', 'name_usual']], 'ror_relation.csv')

    print(tmp.columns)
    tmp = pd.merge(tmp, countries[['countryCode', 'country_code']].drop_duplicates(), how='left', left_on='iso2', right_on='countryCode')
    tmp = pd.merge(tmp, countries[['country_name_fr', 'countryCode_iso3']].drop_duplicates(), how='left', left_on='country_code', right_on='countryCode_iso3')
    tmp = tmp[[col for col in tmp.columns if not col.startswith('iso')]]
    tmp = tmp.assign(source_id='ror')


    #######
    # clean name_usual for entities CZECH REPUBLIC
    mask_cze = tmp['country_code'] == "CZE"
    tmp.loc[mask_cze, 'name_usual'] = (
        tmp.loc[mask_cze, 'name_usual']
          .str.replace(r"czech academy of sciences", "", case=False, regex=True)
          .str.replace(r"[^\w\s]", "", regex=True)  # remove punctuation
          .str.strip()
    )

    ###############################################

    file_name = f"{PATH_REF}ror.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(tmp, file)

    return tmp