from config_path import PATH_REF, PATH_HARVEST
from config_url import sirene_url
from config_api import sirene_headers
from step3_entities.categories import legal_category
from functions_shared import length_code_geo
import time, requests, pandas as pd, json, numpy as np
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
from ratelimit import limits, sleep_and_retry
from dotenv import load_dotenv
load_dotenv()


####################################

def url_valid(id_type, id_value):
    URL=f"{sirene_url}siret?q={id_type}:{str(id_value)}"
    if id_type == 'siret':
        return URL
    else:
        return f"{URL} AND etablissementSiege:true"

@sleep_and_retry
@limits(calls=29, period=60)
def make_limited_request(url, headers):
    return requests.get(url, headers=headers)


def get_last_info_siret(x):
    if not x:
        return {}

    # Trouver la première période pour la date de début
    first_period = min(x, key=lambda k: k['dateDebut'])
    date_debut = first_period['dateDebut']

    # Trouver la dernière période
    last_period = max(x, key=lambda k: k['dateDebut'])

    # Si la dernière période est fermée ('F'), la date de début et de fin sont celles de cette période
    if last_period.get('etatAdministratifEtablissement') == 'F':
        date_fin = last_period['dateDebut']
    else:
        date_fin = None

    return {
        "etat_et": last_period.get('etatAdministratifEtablissement'),
        "ens1": last_period.get('enseigne1Etablissement'),
        "ens2": last_period.get('enseigne2Etablissement'),
        "ens3": last_period.get('enseigne3Etablissement'),
        "denom_us": last_period.get('denominationUsuelleEtablissement'),
        "naf_et": last_period.get("activitePrincipaleEtablissement"),
        "date_debut": date_debut,
        "date_fin": date_fin
    }


def replace_nd_in_list(cell):
    if isinstance(cell, list):
        return [np.nan if x == '[ND]' else x for x in cell]
    return cell

# Fonction pour remplacer '[ND]' dans les chaînes de caractères
def replace_nd_in_string(cell):
    if isinstance(cell, str):
        return cell.replace('[ND]', 'None')  # ou np.nan si tu préfères
    return cell

def clean_anomaly(df):
    for col in df.columns:
        # Remplacer '[ND]' dans les listes
        if df[col].apply(lambda x: isinstance(x, list)).any():
            df[col] = df[col].apply(replace_nd_in_list)
        # Remplacer '[ND]' dans les chaînes de caractères
        else:
            df[col] = df[col].apply(replace_nd_in_string)
    return df


def concat_value_unique(row, var_list: list):
    var_values = []
    for col in var_list:
        value = row[col]
        if pd.notna(value) and value not in var_values:
            var_values.append(value)
    return ', '.join(var_values) if var_values else None

def names_sirene(df):
    # Application de la fonction pour créer une nouvelle colonne
    df['ens'] = df.apply(lambda x: concat_value_unique(x, ['denom_us','ens1', 'ens2', 'ens3']), axis=1)
    df['nom_perso'] = df.apply(lambda x: concat_value_unique(x, ['nom_pp', 'prenom']), axis=1)
    df['nom'] = df['nom_ul']
    df.loc[df['nom'].isnull(), 'nom'] = df['ens']
    df.loc[df['nom'].isnull(), 'nom'] = df['nom_perso']
    df['nom'] = df['nom'].str.capitalize()
    return df.drop(columns=['ens1', 'ens2', 'ens3', 'denom_us', 'nom_pp', 'prenom', 'nom_ul'])

def adress_sirene(df):
    '''CP et COM_CODE gestion des 0 manquants'''        

    # clean com_code and cp = '0None'
    def code_geo_clean(var):
        if var is None:
            return None
        if 'None' in var:
            return None
        else:
            return str(var)

    for v in ['cp', 'com_code']:
        df[v] = df[v].map(code_geo_clean) 
        df[v] = df[v].map(length_code_geo)

    for v in ['voie_num', 'voie_type', 'voie_lib', 'ville_etr', 'ville']:
        df[v] = df[v].fillna('').astype(str).str.strip()

    df['voie_num'] = df['voie_num'].fillna('').astype(str)
    df['voie_type'] = df['voie_type'].str.lower()
    df['voie_lib'] = df['voie_lib'].str.title()
    df['address'] = df.apply(
        lambda x: f"{x.get('voie_num', '')} {x.get('voie_type', '')} {x.get('voie_lib', '')}",
        axis=1
    )
    df['address'] = df['address'].str.strip()
    df.loc[df['address'].isnull(), 'address'] = ''

    df['ville_etr'] = df['ville_etr'].fillna('').astype(str).str.strip()
    df.loc[(df['ville'] == '')&(~df['ville_etr'].str.contains("[0-9]+", regex=True)), 'ville'] = df.loc[(df['ville'] == '')&(~df['ville_etr'].str.contains("[0-9]+", regex=True)), 'ville_etr']
    df['address'] = df['address'].str.strip(', ').replace('', None)
    df['ville'] = df['ville'].str.title()
    return df

def country_sirene(df):
    p=[('99109', 'DEU'), ('99134', 'ESP'), ('99140', 'CHE'), ('99132', 'GBR'), ('99101', 'DNK'), ('99127', 'ITA'), 
       ('99131', 'BEL'),('99216', 'CHN'), ('99404', 'USA'), ('99135', 'NLD'), ('99401', 'CAN')]

    for code, iso3 in p:
        df.loc[df['pays_code']==code, 'iso3'] = iso3
    
    df.loc[(df['iso3'].isna()), 'iso3'] = 'FRA'
    return df


def category_sirene(df):
    cj = legal_category()
    df = pd.merge(df, cj[['inseeCode', 'id']], how='left', left_on='cj', right_on='inseeCode')
    if any(df['inseeCode'].isna()):
        print(f"- Warning ! unexpected inseeCode values: {df.loc[df['inseeCode'].isna(), 'cj'].unique()}")

    cj_lib=json.load(open("data_files/cj_code_to_paysage.json"))
    for inseeCode, paysageCat in cj_lib.items():
        df.loc[df['cj']==inseeCode, 'paysageCat'] = paysageCat
    df.loc[df['paysageCat'].isna(), 'paysageCat'] = '7w3QE'
    return df
        

def sirene_id_to_harvest(id_source, id_var: str, s_old):

    sirene_dict = (id_source
                    .loc[(id_source['in_paysage']==False)&(~id_source[id_var].isna())&(id_source['source_id'].isin(['siren', 'siret', 'rna']))]
                    .drop_duplicates()
                    .assign(source_id=lambda x: x['source_id'].replace({'rna':'identifiantAssociationUniteLegale'}))
                    .sort_values(by=['source_id'], ascending=False)
                    )
    sirene_dict = sirene_dict.loc[(~sirene_dict['from_id_to_ref'].isin(s['siren']))&(~sirene_dict['from_id_to_ref'].isin(s['siret']))&(~sirene_dict['from_id_to_ref'].isin(s['rna']))]

    print(f"- nombre d'identifiants de entities avec sirene {len(sirene_dict)}")
    return sirene_dict.to_dict('records')


def get_sirene(id_source, id_var: str):
    """
    extract sirene data via API
    - load old sirene
    - update siren/siret
    - save 
    
    """



    print("### SIRENE")
    print(time.strftime("%H:%M:%S"))
    s_old = pd.read_pickle(f"{PATH_REF}sirene.pkl")
    mask = (s_old['diffus'].isin(['P', 'N']))|(s_old['etat_ul'].isin(['C']))
    s_old = s_old[mask]
    sirene_dict = sirene_id_to_harvest(id_source, id_var, s_old)

    result = []
    n=0
    for i in sirene_dict:
        IDENTIFIANT=i[id_var]
        SOURCE=i['source_id']
        n=n+1
        if n % 100 == 0: 
            print(f"{n}", end=',')
        
        url = url_valid(SOURCE, IDENTIFIANT)
        rinit = make_limited_request(url, sirene_headers)
        STATUS = rinit.status_code

        while STATUS == 429:
            # Si 429, attendre avant de continuer
            time.sleep(0.8)   
            print(f"\n- erreur 429 Rate limit exceeded. Retrying request for {IDENTIFIANT}...")
            rinit = make_limited_request(url, sirene_headers)
            STATUS = rinit.status_code

        try:
            if STATUS == 200:
                r2 = rinit.json()['etablissements'][0]
                ru = r2.get('uniteLegale')
                ra = r2.get('adresseEtablissement')
        #         print(r2)
                response = {   
                    "siren": str(r2.get("siren")),
                    "siret": str(r2.get("siret")),
                    'diffus': ru.get("statutDiffusionUniteLegale"),
                    "siege": bool(r2.get("etablissementSiege")),    
                    "etat_ul": ru.get("etatAdministratifUniteLegale"),
                    "debut_ul": ru.get("dateCreationUniteLegale"),
                    "sigle": ru.get("sigleUniteLegale"),
                    "nom_ul": ru.get("denominationUniteLegale"),
                    "nom_pp": ru.get("nomUniteLegale"),
                    "prenom":ru.get("prenom1UniteLegale"),
                    "cat": ru.get("categorieEntreprise"),
                    "cat_an": ru.get("anneeCategorieEntreprise"),
                    "cj": str(ru.get("categorieJuridiqueUniteLegale")),
                    "naf_ul": ru.get("activitePrincipaleUniteLegale"),                         
                    "rna": ru.get("identifiantAssociationUniteLegale"),

                    "com_code": str(ra.get("codeCommuneEtablissement")),
                    "voie_num": ra.get("numeroVoieEtablissement"),
                    "voie_type": ra.get("typeVoieEtablissement"),
                    "voie_lib": ra.get("libelleVoieEtablissement"),
                    "voie_comp": ra.get("complementAdresseEtablissement"),
                    "cp": str(ra.get("codePostalEtablissement")),                    
                    "ville": ra.get("libelleCommuneEtablissement"),       
                    "pays_code": ra.get("codePaysEtrangerEtablissement"),   
                    "pays_lib": ra.get("libellePaysEtrangerEtablissement"),                 
                    "ville_etr": ra.get("libelleCommuneEtrangerEtablissement"),
                    "lat": str(ra.get("coordonneeLambertAbscisseEtablissement")),
                    "long": str(ra.get("coordonneeLambertOrdonneeEtablissement"))
                    }


                rj = r2.get('periodesEtablissement')
                response_siret = get_last_info_siret(rj)
                response.update(response_siret)
                if response['etat_ul']=="C":
                    URL=f"{sirene_url}siren/{str(IDENTIFIANT[:9])}"
                    rinit = make_limited_request(URL, sirene_headers)
                    response['fin_ul'] = rinit.json()['uniteLegale']['periodesUniteLegale'][0]['dateDebut']

        #         print(response_siret)
                result.append(response)

        except requests.exceptions.HTTPError as http_err:
            print(f"\n{IDENTIFIANT}, {STATUS} -> HTTP error occurred: {http_err}")
            sirene_dict.append(i)
        except requests.exceptions.RequestException as err:
            print(f"\n{IDENTIFIANT}, {STATUS} -> Error occurred: {err}")
            sirene_dict.append(i)
        except Exception as e:
            print(f"\n{IDENTIFIANT}, {STATUS} -> An unexpected error occurred: {e}")

    sirene=pd.DataFrame(result)

    for s in ['siren', 'siret']:
        x=set([i['id'] for i in sirene_dict if i['source_id']==s])
        y=set(sirene.loc[sirene[s].isin(x), s])
        if len(x)!=len(y):
            print(f"- ATTENTION, missing {len(x-y)} {s}\n{x-y}")


    sirene = clean_anomaly(sirene)
    sirene = names_sirene(sirene)
    sirene = country_sirene(sirene)
    sirene = adress_sirene(sirene)
    sirene = category_sirene(sirene)

    sirene = sirene.drop_duplicates()
    
    print(f"- sirene final size: {len(sirene)}")
    print(time.strftime("%H:%M:%S"))

    file_name = f"{PATH_HARVEST}sirene.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(sirene, file)


    s = pd.concat([s_old, sirene], ignore_index=True)
    file_name = f"{PATH_REF}sirene.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(s, file)

    print(f"- sirene saved to {file_name}")

    return sirene

def get_cat_entreprise(id_list):
    result = []
    n=0
    for i in id_list:
        n=n+1
        if n % 100 == 0: 
            print(f"{n}", end=',')
        
        url = f"https://api.insee.fr/api-sirene/3.11/siren/{i}"
        rinit = make_limited_request(url, sirene_headers)
        STATUS = rinit.status_code

        while STATUS == 429:
            # Si 429, attendre avant de continuer
            time.sleep(0.9)   
            print(f"\n- erreur 429 Rate limit exceeded. Retrying request for {i}...")
            rinit = make_limited_request(url, sirene_headers)
            STATUS = rinit.status_code

        try:
            if STATUS == 200:
                r2 = rinit.json()['uniteLegale']
                response={
                    "siren": i,
                    "cat": r2.get("categorieEntreprise"),
                    "cat_an": r2.get("anneeCategorieEntreprise")
                }
                result.append(response)  
            else:
                result.append({"siren": i, "cat": None, "cat_an": None})

        except requests.exceptions.HTTPError as http_err:
            print(f"\n{i}, {STATUS} -> HTTP error occurred: {http_err}")
            id_list.append(i)
        except requests.exceptions.RequestException as err:
            print(f"\n{i}, {STATUS} -> Error occurred: {err}")
            id_list.append(i)
        except Exception as e:
            print(f"\n{i}, {STATUS} -> An unexpected error occurred: {e}")

    return result