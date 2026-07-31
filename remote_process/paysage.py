import time, requests, pandas as pd, copy, numpy as np, re
from paths import PATH_HARVEST
from remote_process.grist import categoriesG
from functions_shared import trace_chain
from retry import retry
from dotenv import load_dotenv
load_dotenv()
from config_api import paysage_headers

@retry(delay=100, tries=3)

def get_paysageObj(collection):
    from config_api import paysage_headers
    base_url = f'https://api.paysage.dataesr.ovh/{collection}'
    rinit = requests.get(base_url, headers=paysage_headers, verify=False)
    tot = rinit.json()['totalCount']

    params = {"limit": tot}
    rinit = requests.get(base_url, params=params, headers=paysage_headers, verify=False)
    return rinit.json()['data']

def get_paysageODS(dataset):
    from config_api import ods_headers
    url=f"https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/{dataset}/exports/json"

    response = requests.get(url, headers=ods_headers)
    result=response.json()
    return pd.DataFrame(result)

def get_paysage_struct_fromODS():
    from config_api import ods_headers
    base_url = "https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/structures-de-paysage-v2/exports/json"

    params = {
            "select": "id,usualname,shortname,acronymfr,nameen,acronymen,acronymlocal,legalcategory_inseecode,"
            "legalcategory_longnamefr,legalcategory_sector,category_id,category_usualnamefr,cityid"

        }

    response = requests.get(base_url, params=params, headers=ods_headers)

    if response.status_code == 200:
        result = response.json()
        r = pd.DataFrame(result)
        # r = r.loc[r['id'].isin(id_list)]
    else:
        print(f"Erreur {response.status_code} : {response.text}")

    return r


def get_ID_by_cat(id):
    from config_api import paysage_headers
    base_url = f"https://api.paysage.dataesr.ovh/relations?filters[relationTag]=structure-categorie&filters[relatedObjectId]={id}"
    rinit = requests.get(base_url, headers=paysage_headers)
    tot = rinit.json()['totalCount']
    print(f"- number of struct for {id}: {tot}")

    if tot == 0:
        return []


    PAGE_SIZE = 5000  # lot raisonnable pour éviter le timeout
    all_data = []
    offset = 0

    while offset < tot:
        try:
            params = {
            'limit': PAGE_SIZE,
            'skip': offset,
            'sort': '-resourceId'
        }
           
            r = requests.get(base_url, params=params, headers=paysage_headers)
            r.raise_for_status()
            
            batch = r.json().get('data', [])
            if not batch:
                break
            
            all_data.extend(batch)
            offset += len(batch)
            
            if tot > 1000:  # log la progression pour les gros ids
                print(f"  {id}: {offset}/{tot} récupérés...")
            

        except requests.exceptions.ConnectionError as e:
            print(f"\n{id} -> Connexion coupée à offset {offset}: {e}")
            time.sleep(5)  # attendre avant retry (géré aussi par @retry)
            raise  # laisser @retry reprendre depuis le début si besoin
        except requests.exceptions.Timeout:
            print(f"\n{id} -> Timeout à offset {offset}")
            raise
        except requests.exceptions.HTTPError as e:
            print(f"\n{id} -> HTTP error: {e}")
            raise
        except Exception as e:
            print(f"\n{id} -> Erreur inattendue: {e}")
            raise

    return all_data

#############################################################################################
def ID_to_IDpaysage(lid_source, siren_siret=[]):
    from remote_process.paysage import get_paysageODS

    print("## harvest IDpaysage from ID")
    paysage_liste = list(set([i['api_id'] for i in lid_source if not i['source_id'] in ['ror', 'siren', 'paysage']]))
    print(f"- start paysage liste: {len(paysage_liste)}")
    if 'siren_siret' in globals() or 'siren_siret' in locals():
        paysage_liste = list(set(paysage_liste+siren_siret))
    print(f"- new paysage liste with siren_siret: {len(paysage_liste)}")
    x=pd.DataFrame([i['api_id'] for i in lid_source if i['source_id'] in ['paysage']], columns=["id_source"])

    if paysage_liste:
        dataset="fr-esr-paysage_structures_identifiants"
        paysage_id=get_paysageODS(dataset)
        # paysage_id = get_IDpaysage(paysage_liste)
        
        paysage_id=(paysage_id
                    .loc[paysage_id.id_value.isin(paysage_liste), 
                        ['id_value','id_paysage','active','id_enddate']]
                    .rename(columns={'id_value':'id_source', 'active':'status', 'id_enddate':'end'}))
        x['id_paysage'] = x.id_source
        paysage_id = pd.concat([paysage_id, x], ignore_index=True)
        
    else:
        paysage_id = x
        paysage_id['id_paysage'] = paysage_id.id_source

    file_name = f"{PATH_HARVEST}paysage_id.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(paysage_id, file)

    paysage_id = pd.DataFrame(paysage_id)
    paysage_id = paysage_id[~paysage_id.id_paysage.isnull()]
    print(f"- end size paysage id: {len(paysage_id)}")
    return paysage_id

    ###############################

def IDpaysage_status(paysage_id):

    print("## control IDpaysage status")
    # paysage_id = pd.DataFrame(paysage_id)
    # paysage_id = paysage_id[~paysage_id.id_paysage.isnull()]
    # x=pd.DataFrame([i['api_id'] for i in lid_source if i['source_id'] in ['paysage']], columns=["id_source"])

    # try:
        # paysage_id = pd.concat([paysage_id, x], ignore_index=True)
    print(f"- {len(paysage_id)} entities paysage to check")
    paysage_id.loc[paysage_id.id_paysage.isnull(), 'id_paysage'] = paysage_id.id_source
    paysage_id['nb'] = paysage_id.groupby('id_source')['id_paysage'].transform('count')
    if 'nb' and 'status' in paysage_id.columns:
        paysage_id = (paysage_id.loc[~((paysage_id.nb>1)&(paysage_id.status==False))]
                .drop(columns=['status', 'nb'])
                .drop_duplicates())
    doublon=list(paysage_id.loc[(paysage_id.groupby('id_source')['id_paysage'].transform('count')>1)].id_paysage)
    if doublon:
        for i in doublon:
            url1=f'https://api.paysage.dataesr.ovh/structures/{str(i)}'
            rinit = requests.get(url1, headers=paysage_headers, verify=False)
            r = rinit.json()
            print({i, r.get('structureStatus')})
            if r.get('structureStatus')=='inactive':
                paysage_id=paysage_id[paysage_id.id_paysage!=i]
            elif r.get('structureStatus') is None:
                print(f"1- vérifier et ajouter un statut dans paysage pour: {i}")
    # except:
        # paysage_id = x
        # paysage_id['id_paysage'] = paysage_id.id_source

    # #provisoire essayer de régler ce problème à la source
    paysage_id=paysage_id[paysage_id.id_paysage!='im9o8']
    print(f"- entities paysagés {len(paysage_id[~paysage_id.id_paysage.isnull()])}")
    if doublon:
        return paysage_id, doublon
    else:
        return paysage_id, pd.DataFrame()
###############################

# def IDpaysage_successor(df):
def IDpaysage_successor():
    # traitement des successeurs
    print("## IDpaysage successors")
    # #successor      

    url_base=f'https://api.paysage.dataesr.ovh/relations?filters[relationTag]=structure-predecesseur'
    rinit = requests.get(url_base, headers=paysage_headers, verify=False)
    nb_tot=rinit.json()['totalCount']
    rinit = requests.get(url_base, params={'limit':nb_tot}, headers=paysage_headers, verify=False)
    r=rinit.json()['data']
    paysage_successor=[]
    for i in r:
        paysage_successor.append({'id':i.get('relatedObjectId'),
                                'id_s0': i.get('resourceId'),
                                'active':i.get('resource').get('structureStatus'),
                                'start_date':i.get('resource').get('startDate'),
                                'end_date':i.get('resource').get('endDate')})


    file_name = f"{PATH_HARVEST}paysage_successor.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(paysage_successor, file)

    if paysage_successor:
        print(f"\n- size de resultat paysage successor {len(paysage_successor)}")
        paysage_successor = pd.DataFrame.from_records(paysage_successor).drop_duplicates()
        # paysage_successor['nb'] = paysage_successor.groupby('id_paysage')['id_s0'].transform('count')
        # if any(paysage_successor['nb']>1):
        #     print(f"\n- ++successeurs pour id_paysage:\n{paysage_successor[paysage_successor['nb']>1]}")            
        #     paysage_successor['nb_date'] = paysage_successor.groupby('id_paysage')['start_date'].transform('nunique')
        #     paysage_successor = paysage_successor.loc[~((paysage_successor.nb>1)&(~paysage_successor.end_date.isnull()))]
        #     paysage_successor = paysage_successor.loc[~((paysage_successor.nb>1)&(paysage_successor.nb_date==1))].drop(columns='nb_date')

        # paysage_successor = paysage_successor.groupby('id_paysage').first().reset_index().drop(columns=['end_date','start_date','active','nb'])

        successor = dict(zip(paysage_successor.id, paysage_successor.id_s0))
        
        paysage_successor['id_succ'] = paysage_successor['id'].apply(lambda x: trace_chain(x, successor))
            
        paysage_successor = paysage_successor[['id', 'id_succ']].drop_duplicates()
        paysage_successor['nb'] = paysage_successor.groupby('id')['id_succ'].transform('count')
        if any(paysage_successor.nb>1):
            print(f"\n- ⚠️, several successors for one id check\n{paysage_successor[paysage_successor.nb>1]}")   
    # if len(paysage_successor)>0:    
    #     paysage = df.loc[df['source_id']=='paysage'].merge(paysage_successor[['id_paysage', 'id_clean']].drop_duplicates(), how='left', left_on='id_extend', right_on='id_paysage')
    #     paysage.loc[paysage['id_clean'].isnull(), 'id_clean'] = paysage['id_extend']
    #     # paysage=paysage.rename(columns={'id_paysage':'id_paysage_1'})
    # else:
    #     paysage = df.loc[df['source_id']=='paysage'].assign(id_clean=df['id_extend']).drop(columns='source_id')
        
    # if any(paysage.groupby('id_extend')['id_clean'].transform('count')>1):
    #         print(f"\ndoublons:\n{paysage[paysage.groupby('id_extend')['id_clean'].transform('count')>1][['id_extend','id_clean']]}")
    return paysage_successor.drop(columns='nb')
###############################

def IDpaysage_parent():
    print("## IDpaysage parent")
    # ## Parent

    url_base = 'https://api.paysage.dataesr.ovh/relations?filters[relationTag]=structure-interne'

    # 1. Première requête pour obtenir le nombre total d'éléments
    rinit = requests.get(url_base, headers=paysage_headers, verify=False)
    nb_tot = rinit.json()['totalCount']

    # 2. Configuration de la pagination
    limit_par_page = 100  # Taille de paquet raisonnable pour le serveur
    all_data = []
    skip = 0

    # 3. Boucle de récupération par paquets
    while skip < nb_tot:
        params = {
            'limit': limit_par_page,
            'skip': skip
        }
        
        response = requests.get(url_base, params=params, headers=paysage_headers, verify=False)
        
        # Sécurité : on arrête si la requête échoue
        if response.status_code != 200:
            print(f"Erreur d'API au skip {skip} : {response.status_code}")
            break
            
        data_page = response.json().get('data', [])
        
        # Si la page est vide (sécurité), on stoppe la boucle
        if not data_page:
            break
            
        all_data.extend(data_page)
        
        # On avance le curseur pour la page suivante
        skip += limit_par_page

    print(f"Total récupéré : {len(all_data)} / {nb_tot}")

    if all_data:
        paysage_relation = []
        for i in all_data:
            res = i.get('relatedObject', {}).get('identifiers', [])
            rnsr = next((j.get('type') for j in res if j.get('type') == 'rnsr'), None)
            paysage_relation.append({'id':i.get('relatedObjectId'),
                                    'id_source_status':i.get('relatedObject').get('structureStatus'),
                                    'rnsr': rnsr,
                                    'id_p0': i.get('resourceId'),
                                    'id_p_status':i.get('resource').get('structureStatus'),
                                    'end_date':i.get('endDate')})    
                                    


    file_name = f"{PATH_HARVEST}paysage_parent.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(paysage_relation, file)

    if paysage_relation:
        paysage_relation = pd.DataFrame(paysage_relation)
        # paysage_relation=paysage_relation[paysage_relation['end_date'].isnull()]
        print(f"\n- size de resultat paysage relation {len(paysage_relation)}")
        paysage_relation = paysage_relation[paysage_relation['rnsr'].isnull()].drop(columns='rnsr')

        # À insérer juste après la création du DataFrame :
        paysage_relation['end_date'] = pd.to_datetime(paysage_relation['end_date'], errors='coerce')

        # Remplacement de la ligne problématique :
        year_now = time.localtime().tm_year
        tb = paysage_relation[(paysage_relation.id_source_status == 'active') & 
                            (paysage_relation.end_date.dt.year == year_now)]
        
        if any(tb):
            print(f"-⚠️ check parent link closed in {year_now}:\n{tb} ")

        paysage_relation = paysage_relation[~((paysage_relation.id_source_status=='active')&(paysage_relation.end_date.notna()))]

        parent = dict(zip(paysage_relation.id, paysage_relation.id_p0))

        paysage_relation['id_parent'] = paysage_relation['id'].apply(lambda x: trace_chain(x, parent))

        paysage_relation = paysage_relation[['id', 'id_parent']].drop_duplicates()

        liste_no_parent = ['Py0K5', 'dUyiC', 'H1TgQ', 'S0Jbc']
        paysage_relation = paysage_relation.loc[~(paysage_relation['id_parent'].isin(liste_no_parent))]

        paysage_relation['nb'] = paysage_relation.groupby('id')['id_parent'].transform('count')
        if any(paysage_relation.nb>1):
            print(f"- ⚠️, several parents for one id check\n{paysage_relation[paysage_relation.nb>1]}")
        

    # if len(paysage_relation)>0:
    #     paysage = paysage.merge(paysage_relation[['id_source', 'id_p']].drop_duplicates(), how='left', left_on='id_clean', right_on='id_source')
    #     paysage.loc[~paysage.id_p.isnull(), 'id_clean'] = paysage.id_p
    #     paysage = paysage[['id_extend', 'id_clean']].drop_duplicates()

    #     if any(paysage.groupby('id_extend')['id_clean'].transform('count')>1):
    #         print(f"\ndoublons:\n{paysage[paysage.groupby('id_extend')['id_clean'].transform('count')>1][['id_extend','id_clean']]}")
    return paysage_relation.drop(columns='nb')
###############################################

def IDpaysage_info():
    # print("## IDpaysage name")
    # paysage_liste=paysage['id_clean'].dropna().astype(str).unique().tolist()
    # print(f"- size paysage id à importer:{len(paysage_liste)}")

    # paysage_infos = get_paysage_struct_fromODS(paysage_liste)
    paysage_infos = get_paysage_struct_fromODS()
    paysage_infos = paysage_infos.rename(columns={
                        'usualname':'name',
                        'shortname':'shortName',
                        'acronymfr':'acronymFr',
                        'acronymen':'acronymEn',
                        'acronymlocal':'acronymLocal',
                        'legalcategory_inseecode':'cj_code',
                        'legalcategory_longnamefr':'cj_name',
                        'legalcategory_sector':'sector',
                        'category_id':'category_id',
                        'category_usualnamefr':'category_name'}
                        )

    print(f"\n- resultat paysage infos:{len(paysage_infos)}")

    # print(f"\nliste des exceptions d'extraction\n{[i for i in paysage_infos if i.get('status')]}")
    # verif2 = [i.get('id_parent') for i in paysage_infos if i.get('status')]
    # print(f"\nErreurs lévées automatiquement, vérifier la liste\n{[i for i in paysage_infos if i.get('id_parent') in verif2]}")

    file_name = f"{PATH_HARVEST}paysage_infos.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(paysage_infos, file)
        
    # if 'status' in paysage_infos.columns:
    #     paysage_infos = paysage_infos.loc[paysage_infos.status.isnull()].drop(columns='status')

    paysage_infos['acronym'] = np.where(~paysage_infos.shortName.isnull(), paysage_infos.shortName, paysage_infos.acronymFr)
    for i in ['acronymEn', 'acronymLocal']:
        paysage_infos['acronym'] = np.where(paysage_infos.acronym.isnull(), paysage_infos[i], paysage_infos.acronym)
    # paysage_infos['acro_tmp'] = paysage_infos['otherNames'].apply(lambda x: min(x, key=len) if x is not None and len(x)!=0 else '')

    paysage_infos = (paysage_infos.drop(columns=['shortName', 'acronymFr'])
                     .rename(columns={'name':'name_clean','acronym':'acronym_clean'})
                     .drop_duplicates()
    )

    if any(paysage_infos.name_clean.isnull()):
        print(paysage_infos[paysage_infos.name_clean.isnull()])
        
    # if any(paysage.groupby('id_extend')['id_clean'].transform('count')>1):
    #     print(f"\n- doublon {paysage[paysage.groupby('id_extend')['id_clean'].transform('count')>1]}") 
    return paysage_infos
################################################

def IDpaysage_siret():
    # specifiquement pour les entreprises :
    # - recuperer la categorie entreprise dans sirene pour els structures paysage (ETI, PME...)
    # - lister les siren pour les groupes ATTENTION aux siren qui changent de groupes
    try:
        url_base = "https://api.paysage.dataesr.ovh/identifiers?filters[type]=siret"
        rinit = requests.get(url_base, headers=paysage_headers)
        nb_tot = rinit.json()['totalCount']
        print(nb_tot)
        rinit = requests.get(url_base, params={'limit':nb_tot}, headers=paysage_headers)
        r = rinit.json()['data']
        paysage_siret=[]
        for i in r:
            paysage_siret.append({'id_clean':i.get('resourceId'),'siret':i.get('value'), 'siren_end_date':i.get('endDate'), 'active':i.get('active')}
        )
    except requests.exceptions.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as err:
        print(f"Error occurred: {err}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

    if len(paysage_siret)>0:
        paysage_siret = pd.DataFrame(paysage_siret)
        paysage_siret['siren'] = paysage_siret.siret.str[:9]
        paysage_siret = paysage_siret.sort_values(['id_clean', 'siren','active'], ascending=[True, False, True])


        paysage_siret['siren_main'] = None
        # Appliquer les règles
        for id_clean, group in paysage_siret.groupby('id_clean'):
            # Cas 1 : Au moins un active=True
            if any(group['active']):
                first_active_row = group[group['active']].iloc[0]
                paysage_siret.loc[group.index, 'siren_main'] = first_active_row['siren']
            # Cas 2 : Uniquement un active=False
            elif len(group) == 1:
                paysage_siret.loc[group.index, 'siren_main'] = group['siren'].iloc[0]
            # Cas 3 : Plusieurs active=False
            else:
                paysage_siret.loc[group.index, 'siren_main'] = group['siren'].iloc[0]

        file_name = f"{PATH_HARVEST}paysage_siret.pkl"
        with open(file_name, 'wb') as file:
            pd.to_pickle(paysage_siret, file)

        # paysage=pd.merge(paysage, paysage_siret[['id_clean', 'siren', 'active', 'siren_main']].drop_duplicates(), how='left', on='id_clean')
        # print(f"\n- size paysage : {len(paysage)}")

        # paysage_siret['nb'] = paysage_siret.groupby('id_extend')['id_clean'].transform('count')
        # if len(paysage[paysage.nb>1])>0:
        #     print(f"\ndoublons dans paysage à régler à la source -> {paysage[paysage.nb>1][['id_extend', 'id_clean', 'name_clean']]}")
    # return paysage_siret[['id_clean', 'siren', 'siren_main']].drop_duplicates()

################################################

def check_var_null(paysage):
    for i in [ 'cj_code', 'cj_name', 'sector', 'name_clean', 'acronym_clean']:
        if i in ['name_clean']:
            print(f" {i}-> {paysage.loc[paysage[i].isnull()].id_clean.unique()}")
        else:
            print(f" {i} -> {paysage.loc[paysage[i].isnull(), 'id_clean'].nunique()}")

################################################
def paysage_getRefInfo():
    """
    load information for each paysage IDS
    - successor
    - parents
    save result into data_harvest paysage_df
    
    """
    print("### PAYSAGE HARVEST")
    
    paysage_successor = IDpaysage_successor()
    paysage_relation = IDpaysage_parent()
    # paysage=IDpaysage_cj(paysage)
    paysage_infos = IDpaysage_info()
    IDpaysage_siret()

    paysage = pd.merge(paysage_infos[['id']], paysage_successor, how='left', on='id')
    paysage.loc[paysage['id_succ'].isnull(), 'id_succ'] = paysage['id']
    paysage = (pd.merge(paysage, 
                        paysage_relation.rename(columns={'id':'id_succ'}), 
                        how='left', on='id_succ')
    )
    paysage.loc[paysage['id_parent'].isnull(), 'id_parent'] = paysage['id_succ']

    paysage = paysage[['id', 'id_parent']].drop_duplicates().rename(columns={'id':'id_source'})
    
    paysage = pd.merge(paysage, paysage_successor, how='left', left_on='id_parent', right_on='id')
    paysage.loc[paysage['id_succ'].isnull(), 'id_succ'] = paysage.loc[paysage['id_succ'].isnull(), 'id_parent']

    paysage = (pd.merge(paysage[['id_source', 'id_succ']].rename(columns={'id_succ':'id_clean'}).drop_duplicates(), 
                        paysage_infos, how='left', left_on='id_clean', right_on='id')
               .drop(columns=['id'])
               .drop_duplicates()
               )
    print(f"- size paysage: {len(paysage)}")


    # print(f"- size paysage after siret: {len(paysage)}")
    check_var_null(paysage)
    file_name = f"{PATH_HARVEST}paysage_df.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(paysage, file)

######################################################

def IDpaysage_category():
    """
    load categories for each iDS from paysage
    """
    print("## IDpaysage category")
        # pc = pd.read_csv("data_files/cat_paysage.csv", sep=';')
    pc = pd.DataFrame(categoriesG['Cat_paysage'])
    cat_liste = list(pc.loc[pc['keep']==True, 'category_id'])
    print(f"- size CAT id à importer:{len(cat_liste)}")

    paysage_category=[]
    
    for cl in cat_liste:   
        try:
            response = get_ID_by_cat(cl)
            result = [{'pid':i.get('resourceId'), 'category_id':i.get('relatedObjectId')} for i in response]
            paysage_category.extend(result)

        except requests.exceptions.HTTPError as http_err:
            print(f"\n{cl} -> HTTP error occurred: {http_err}")
            cat_liste.append(str(cl))
        except requests.exceptions.RequestException as err:
            print(f"\n{cl} -> Error occurred: {err}")
            cat_liste.append(str(cl))
        except Exception as e:
            print(f"{cl} -> An unexpected error occurred: {e}")
        
    file_name = f"{PATH_HARVEST}paysage_category.pkl"
    with open(file_name, 'wb') as file:
            pd.to_pickle(paysage_category, file)
    return paysage_category

################################################

def get_mires():
    import requests, pandas as pd
    from config_api import paysage_headers
    from paths import PATH_REF
    from dotenv import load_dotenv
    load_dotenv()

    ## liste opérateurs de la MIRES
    paysage_mires = pd.DataFrame()

    rinit = requests.get('https://api.paysage.dataesr.ovh/relations?filters[relationTag]=categorie-parent&filters[relatedObjectId]=41ZMP&limit=2000&sort=resource.priority', headers=paysage_headers)
    r = rinit.json()['data']

    paysage_mires = (pd.json_normalize(r)[['resourceId','resource.displayName']]
                        .rename(columns={'resourceId':'category_id', 'resource.displayName':'operateur_name'}))

    paysage_mires = (paysage_mires
               .assign(operateur_num=paysage_mires.operateur_name.replace('([^0-9]*)','', regex=True),
                       operateur_lib=paysage_mires.operateur_name.str.split('-').str[1].str.strip()
                  ))      
    paysage_mires.operateur_lib = paysage_mires.operateur_lib+" ("+paysage_mires.operateur_num+")"

    file_name = f"{PATH_HARVEST}operateurs_mires.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(paysage_mires, file)
    return paysage_mires