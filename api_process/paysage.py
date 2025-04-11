import time, requests, pandas as pd, copy, numpy as np
from config_api import paysage_headers
from config_path import PATH_API, PATH_REF, PATH_WORK
from retry import retry
from dotenv import load_dotenv
load_dotenv()
@retry(delay=100, tries=3)


def get_IDpaysage(paysage_liste):
    import time, requests
    from config_api import paysage_headers
    from dotenv import load_dotenv
    load_dotenv()

    print(time.strftime("%H:%M:%S"))
    paysage_id = []
    n=0
    for i in paysage_liste:
        n=n+1
        if n % 100 == 0: 
            print(f"{n}", end=',')

        time.sleep(0.2)
        try:
            url1 = f'https://api.paysage.dataesr.ovh/identifiers?filters[value]={str(i)}'
            rinit = requests.get(url1, headers=paysage_headers, verify=False)
            r = rinit.json()['data']
            if r:
                for item in r:
                    response={'id_source':i, 'id_paysage':item.get('resourceId'), 'status':item.get('active'), 'end':item.get('endDate')}
                    paysage_id.append(response)
            else:
                response={'id_source':i, 'status':'non'}
                paysage_id.append(response)

        except requests.exceptions.HTTPError as http_err:
            print(f"\n{i} -> HTTP error occurred: {http_err}")
            paysage_liste.append(str(i))
        except requests.exceptions.RequestException as err:
            print(f"\n{i} -> Error occurred: {err}")
            paysage_liste.append(str(i))
        except Exception as e:
            print(f"\n{i} -> An unexpected error occurred: {e}")
                        
    print(f"1 - resultat id entities paysagés {len(paysage_id)}")
    print(time.strftime("%H:%M:%S"))
    return paysage_id


def get_paysageODS(dataset):
    from config_api import ods_headers
    url=f"https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/{dataset}/exports/json"

    response = requests.get(url, headers=ods_headers)
    result=response.json()
    return pd.DataFrame(result)


def ID_to_IDpaysage(lid_source, siren_siret=[]):
    import pandas as pd
    from config_path import PATH_SOURCE
    from api_process.paysage import get_IDpaysage

    print("## harvest IDpaysage from ID")
    paysage_liste = list(set([i['api_id'] for i in lid_source if not i['source_id'] in ['ror', 'siren', 'paysage']]))
    print(f"- start paysage liste: {len(paysage_liste)}")
    if 'siren_siret' in globals() or 'siren_siret' in locals():
        paysage_liste = list(set(paysage_liste+siren_siret))
    print(f"- new paysage liste with siren_siret: {len(paysage_liste)}")

    dataset="fr-esr-paysage_structures_identifiants"
    paysage_id=get_paysageODS(dataset)
    # paysage_id = get_IDpaysage(paysage_liste)
    
    paysage_id=(paysage_id
                .loc[paysage_id.id_value.isin(paysage_liste), 
                     ['id_value','id_paysage','active','id_enddate']]
                .rename(columns={'id_value':'id_source', 'active':'status', 'id_enddate':'end'}))

    file_name = f"{PATH_API}paysage_id.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(paysage_id, file)

    paysage_id = pd.DataFrame(paysage_id)
    paysage_id = paysage_id[~paysage_id.id_paysage.isnull()]
    return paysage_id

    ###############################

def IDpaysage_status(lid_source, paysage_id):

    import requests, pandas as pd
    from config_api import paysage_headers
    from dotenv import load_dotenv
    load_dotenv()


    print("## control IDpaysage status")
    paysage_id = pd.DataFrame(paysage_id)
    paysage_id = paysage_id[~paysage_id.id_paysage.isnull()]
    x=pd.DataFrame([i['api_id'] for i in lid_source if i['source_id'] in ['paysage']], columns=["id_source"])

    try:
        paysage_id = pd.concat([paysage_id, x], ignore_index=True)
        print(f"- {len(paysage_id)} entities paysage to check")
        paysage_id.loc[paysage_id.id_paysage.isnull(), 'id_paysage'] = paysage_id.id_source
        paysage_id['nb'] = paysage_id.groupby('id_source')['id_paysage'].transform('count')
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
    except:
        paysage_id = x
        paysage_id['id_paysage'] = paysage_id.id_source

    # #provisoire essayer de régler ce problème à la source
    paysage_id=paysage_id[paysage_id.id_paysage!='im9o8']
    print(f"- entities paysagés {len(paysage_id[~paysage_id.id_paysage.isnull()])}")
    if doublon:
        return paysage_id, doublon
    else:
        return paysage_id, pd.DataFrame()
###############################

def IDpaysage_successor(paysage_id):
    import time, requests, pandas as pd, copy
    from config_api import paysage_headers
    from dotenv import load_dotenv
    load_dotenv()


    # traitement des successeurs
    print("## IDpaysage successors")
    paysage_relat = paysage_id['id_paysage'].dropna().unique().astype(str).tolist()
    print(f"2 - size de paysage relat à vérifier {len(paysage_relat)}")

    # #successor
    paysage_successor=[]
    n=0
    for i in paysage_relat:
        time.sleep(0.2)
        n=n+1
        if n % 100 == 0: 
            print(f"{n}", end=',')
            
        try:
            url2=f'https://api.paysage.dataesr.ovh/relations?filters[relationTag]=structure-predecesseur&filters[relatedObjectId]={str(i)}&limit=500'
            rinit = requests.get(url2, headers=paysage_headers, verify=False)
            pages = rinit.json().get('totalCount')
            r = rinit.json()['data']
            if r:
                for page in range(0,pages):
                    response={'id_paysage':r[page].get('relatedObject').get('id'), 'id_s0':r[page].get('resourceId'), 'end_date':r[page].get('endDate'), 'start_date':r[page].get('startDate'), 'active':r[page].get('active')}
                    paysage_successor.append(response)
                    if r[page].get('resourceId') not in paysage_relat:
                        paysage_relat.append(r[page].get('resourceId'))  

        except requests.exceptions.HTTPError as http_err:
            print(f"\n{i} -> HTTP error occurred: {http_err}")
            paysage_relat.append(str(i))
        except requests.exceptions.RequestException as err:
            print(f"\n{i} -> Error occurred: {err}")
            paysage_relat.append(str(i))
        except Exception as e:
            print(f"\n{i} -> An unexpected error occurred: {e}")
            paysage_relat.append(str(i))

    if paysage_successor:
        print(f"\n- size de resultat paysage successor {len(paysage_successor)}")
        paysage_successor = pd.DataFrame.from_records(paysage_successor).drop_duplicates()
        paysage_successor['nb'] = paysage_successor.groupby('id_paysage')['id_s0'].transform('count')
        if any(paysage_successor['nb']>1):
            print(f"\n- ++successeurs pour id_paysage:\n{paysage_successor[paysage_successor['nb']>1]}")            
            paysage_successor['nb_date'] = paysage_successor.groupby('id_paysage')['start_date'].transform('nunique')
            paysage_successor = paysage_successor.loc[~((paysage_successor.nb>1)&(~paysage_successor.end_date.isnull()))]
            paysage_successor = paysage_successor.loc[~((paysage_successor.nb>1)&(paysage_successor.nb_date==1))]

        paysage_successor = paysage_successor.groupby('id_paysage').first().reset_index().drop(columns=['end_date','start_date','active','nb','nb_date'])

        i=0
        paysage_tmp = copy.deepcopy(paysage_successor.rename(columns={'id_s0':'id_s', 'id_paysage':'id_tmp'})) 
        if f'id_s{i}' in paysage_successor.columns: 
            paysage_successor=(paysage_successor.merge(paysage_tmp, how='left', left_on=f'id_s{i}', right_on='id_tmp')
            .rename(columns={'id_s':f"id_s{str(i+1)}"})
            .drop(columns='id_tmp'))
            i+=1  

        n=i-1
        while any(paysage_successor.loc[paysage_successor[f'id_s{i}'].isnull()]) and n>=0:
            paysage_successor.loc[paysage_successor[f'id_s{i}'].isnull(), f'id_s{i}'] = paysage_successor[f"id_s{n}"]
            n=n-1
            
        paysage_successor.rename(columns={f'id_s{i}':'id_clean'}, inplace=True)

    if len(paysage_successor)>0:    
        paysage = paysage_id.loc[~paysage_id.id_paysage.isnull()].merge(paysage_successor[['id_paysage', 'id_clean']].drop_duplicates(), how='left', on='id_paysage')
        paysage.loc[paysage.id_clean.isnull(), 'id_clean'] = paysage['id_paysage']
        paysage=paysage.rename(columns={'id_paysage':'id_paysage_1'})
    else:
        paysage = paysage_id.assign(id_clean=paysage_id.id_paysage)
        
    if any(paysage.groupby('id_source')['id_clean'].transform('count')>1):
            print(f"\ndoublons:\n{paysage[paysage.groupby('id_source')['id_clean'].transform('count')>1][['id_source','id_clean']]}")
    return paysage
###############################

def IDpaysage_parent(paysage):
    print("## IDpaysage parent")
    paysage_relat=paysage['id_clean'].dropna().astype(str).unique().tolist()
    print(f"- size de paysage relat à vérifier {len(paysage_relat)}")

    # ## Parent
    paysage_relation=[]
    n=0
    for i in paysage_relat:
        time.sleep(0.2)
        n=n+1
        if n % 100 == 0: 
            print(f"{n}", end=',')
            
        try:
            url2=f'https://api.paysage.dataesr.ovh/relations?filters[relationTag]=structure-interne&filters[relatedObjectId]={str(i)}&limit=200&sort=-startDate'
            rinit = requests.get(url2, headers=paysage_headers, verify=False)
            r = rinit.json()['data']
            if r:
                response={'id_paysage':r[0].get('relatedObject').get('id'), 'id_p0':r[0].get('resourceId'), 'end':r[0].get('endDate')}
                paysage_relation.append(response)
                if r[0].get('resourceId') not in paysage_relat:
                    paysage_relat.append(r[0].get('resourceId'))      
                    
        except requests.exceptions.HTTPError as http_err:
            print(f"\n{i} -> HTTP error occurred: {http_err}")
            paysage_relat.append(str(i))
        except requests.exceptions.RequestException as err:
            print(f"\n{i} -> Error occurred: {err}")
            paysage_relat.append(str(i))
        except Exception as e:
            print(f"\n{i} -> An unexpected error occurred: {e}")
            paysage_relat.append(str(i))

    file_name = f"{PATH_API}paysage_parent.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(paysage_relation, file)

    if paysage_relation:
        paysage_relation = pd.DataFrame(paysage_relation)
        paysage_relation=paysage_relation[paysage_relation.end.isnull()]
        print(f"\n- size de resultat paysage relation {len(paysage_relation)}")
        paysage_relation = pd.DataFrame(paysage_relation).drop_duplicates()
        paysage_relation['nb'] = paysage_relation.groupby('id_paysage')['id_p0'].transform('count')
        if any(paysage_relation['nb']>1):
            print(paysage_relation[paysage_relation['nb']>1])            

        liste_no_parent = ['Py0K5', 'dUyiC', 'H1TgQ', 'S0Jbc']
        paysage_relation = paysage_relation.loc[~(paysage_relation.id_p0.isin(liste_no_parent))]
        i=0
        paysage_tmp = copy.deepcopy(pd.DataFrame(paysage_relation).rename(columns={'id_p0':'id_p', 'id_paysage':'id_tmp'}).drop(columns='nb'))
        if f'id_p{i}'in paysage_relation.columns:   
            paysage_relation=(paysage_relation.merge(paysage_tmp, how='left', left_on=f'id_p{i}', right_on='id_tmp')
            .rename(columns={'id_p':f"id_p{str(i+1)}"})
            .drop(columns='id_tmp'))
            i+=1   

        n=i-1
        while any(paysage_relation.loc[paysage_relation[f'id_p{i}'].isnull()]) and n>=0:
            paysage_relation.loc[paysage_relation[f'id_p{i}'].isnull(), f'id_p{i}'] = paysage_relation[f"id_p{n}"]
            n=n-1

        paysage_relation.rename(columns={f'id_p{i}':'id_p'}, inplace=True)

        if len(paysage_relation.loc[(paysage_relation.id_p.isin(liste_no_parent))])>0:
            print(f"revoir un peu le code pour traiter ce prob ->\n{paysage_relation.loc[(paysage_relation.id_p.isin(liste_no_parent))]}")

    if len(paysage_relation)>0:
        paysage = paysage.merge(paysage_relation[['id_paysage', 'id_p']].drop_duplicates(), how='left', left_on='id_clean', right_on='id_paysage')
        paysage.loc[~paysage.id_p.isnull(), 'id_clean'] = paysage.id_p
        paysage = paysage[['id_source', 'id_clean']].drop_duplicates()

        if any(paysage.groupby('id_source')['id_clean'].transform('count')>1):
            print(f"\ndoublons:\n{paysage[paysage.groupby('id_source')['id_clean'].transform('count')>1][['id_source','id_clean']]}")
    return paysage
###############################################

def IDpaysage_cj(paysage):
    print("## IDpaysage CJ")
    paysage_liste=paysage['id_clean'].dropna().astype(str).unique().tolist()
    paysage_cj=pd.DataFrame()
    n=0
    len(paysage_liste)

    print(f"- size paysage id à CJ:{len(paysage_liste)}")

    for i in paysage_liste:
        time.sleep(0.2)
        n=n+1
        if n % 100 == 0: 
            print(f"{n}", end=',')    
        
        try:
            url2=f'https://api.paysage.dataesr.ovh/relations?filters[resourceId]={str(i)}&filters[relationTag]=structure-categorie-juridique&limit=500'
            rinit = requests.get(url2, headers=paysage_headers, verify=False)
            r = rinit.json()['data']
            if r:
                temp = pd.json_normalize(r)
                temp = (temp[['resourceId','relatedObject.inseeCode','relatedObject.displayName', 'relatedObject.sector']]
                        .rename(columns={'resourceId':'id_clean','relatedObject.inseeCode':'cj_code','relatedObject.displayName':'cj_name', 'relatedObject.sector':'sector'}))
                paysage_cj=pd.concat([paysage_cj, temp], ignore_index=True)

        except requests.exceptions.HTTPError as http_err:
            print(f"{i} -> HTTP error occurred: {http_err}")
            response = pd.json_normalize({'id_clean': i, 'status': 'http_error'})
            paysage_cj=pd.concat([paysage_cj, response], ignore_index=True)
            paysage_liste.append(str(i))
        except requests.exceptions.RequestException as err:
            print(f"{i} -> Error occurred: {err}")
            response = pd.json_normalize({'id_clean': i, 'status': 'request_error'})
            paysage_cj=pd.concat([paysage_cj, response], ignore_index=True)
            paysage_liste.append(str(i))
        except Exception as e:
            print(f"{i} -> An unexpected error occurred: {e}")
            response = pd.json_normalize({'id_clean': i, 'status': 'unexpected_error'})
            paysage_cj=pd.concat([paysage_cj, response], ignore_index=True)

    if len(paysage_cj)>0:   
        if 'status' in paysage_cj:
            err=paysage_cj.loc[~paysage_cj.status.isnull()].id_clean.unique()
            print(err)
            print(f"{paysage_cj.loc[paysage_cj.id_clean.isin(err), ['id_clean', 'cj_code']]}")

        print(f"\n- size resultat paysage_cj:{len(paysage_cj)}") 

    if len(paysage_cj)>0: 
        if 'status' in paysage_cj:
            paysage_cj = paysage_cj.loc[paysage_cj.status.isnull()]
        paysage_cj = paysage_cj.sort_values(['id_clean','cj_code']).drop_duplicates().groupby('id_clean', as_index=False).agg(lambda x: ';'.join(x.dropna().astype(str))).drop_duplicates()
        paysage = paysage.merge(paysage_cj, how='left',on='id_clean').drop_duplicates()  
    if len(paysage.loc[paysage.cj_code.isnull()])>0:
        print(f"\nSANS CJ -> à compléter dans paysage\n{paysage.loc[paysage.cj_code.isnull()].id_clean.unique()}")
    return paysage
################################################

def IDpaysage_name(paysage):
    print("## IDpaysage name")
    paysage_liste=paysage['id_clean'].dropna().astype(str).unique().tolist()
    print(f"- size paysage id à nommer:{len(paysage_liste)}")
    paysage_name=[]
    n=0

    for i in paysage_liste:
        time.sleep(0.2)
        n=n+1
        if n % 100 == 0: 
            print(f"{n}", end=',')      
        
        try:
            url='https://api.paysage.dataesr.ovh/structures/' 
            url2=f"{url}{str(i)}"
            rinit=requests.get(url2, headers=paysage_headers, verify=False)
            r=rinit.json()
            response={'id_parent': r.get('id'),
                'name':r.get('currentName').get('usualName'),
                'shortName':r.get('currentName').get('shortName'),
                'acronymFr':r.get('currentName').get('acronymFr'),
                'otherNames':r.get('currentName').get('otherNames')}
            paysage_name.append(response)

        except requests.exceptions.HTTPError as http_err:
            print(f"{i} -> HTTP error occurred: {http_err}")
            response = {'id_parent': i, 'status': 'http_error'}
            paysage_name.append(response)
            paysage_liste.append(str(i))
        except requests.exceptions.RequestException as err:
            print(f"{i} -> Error occurred: {err}")
            response = {'id_parent': i, 'status': 'request_error'}
            paysage_name.append(response)
            paysage_liste.append(str(i))
        except Exception as e:
            print(f"{i} -> An unexpected error occurred: {e}")
            response = {'id_parent': i, 'status': 'unexpected_error'}
            paysage_name.append(response)     
            
    print(f"\n- resultat paysage name:{len(paysage_name)}")

    print(f"\nliste des exceptions d'extraction\n{[i for i in paysage_name if i.get('status')]}")
    verif2 = [i.get('id_parent') for i in paysage_name if i.get('status')]
    print(f"\nErreurs lévées automatiquement, vérifier la liste\n{[i for i in paysage_name if i.get('id_parent') in verif2]}")

    file_name = f"{PATH_API}paysage_name.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(paysage_name, file)
        
    paysage_name = pd.DataFrame(paysage_name)

    if 'status' in paysage_name.columns:
        paysage_name = paysage_name.loc[paysage_name.status.isnull()].drop(columns='status')

    paysage_name['acronym'] = np.where(~paysage_name.shortName.isnull(), paysage_name.shortName, paysage_name.acronymFr)
    paysage_name['acro_tmp'] = paysage_name['otherNames'].apply(lambda x: min(x, key=len) if x is not None and len(x)!=0 else '')

    paysage_name = paysage_name.drop(columns=['otherNames', 'shortName', 'acronymFr']).drop_duplicates()

    paysage_name['nb'] = paysage_name.groupby('id_parent')['name'].transform('count')
    if any(paysage_name['nb']>1):
        print(f"\n- ATTENTION doublon de id_clean\n{paysage_name[paysage_name['nb']>1]}")

    paysage = (paysage.merge(paysage_name, how='left',left_on='id_clean',right_on='id_parent')
            .drop(['id_parent', 'nb'], axis=1)
            .rename(columns={'id_source':'id','name':'name_clean','acronym':'acronym_clean'})
            .drop_duplicates())

    if any(paysage.name_clean.isnull()):
        print(paysage[paysage.name_clean.isnull()])
        
    if any(paysage.groupby('id')['id_clean'].transform('count')>1):
        print(f"\n- doublon {paysage[paysage.groupby('id')['id_clean'].transform('count')>1]}") 
    return paysage
################################################

def IDpaysage_siret(paysage):
    paysage_liste=list(filter(None, paysage['id_clean'].dropna().astype(str).unique().tolist()))
    print(f"- size paysage id à siretiser:{len(paysage_liste)}")
    siren_to_remove=["183830017"]

    paysage_siret = pd.DataFrame()
    n=0
    for el in paysage_liste:
        n=n+1
        if n % 100 == 0: 
            print(f"{n}", end=',')  
        
        try:
            rinit = requests.get(f"https://api.paysage.dataesr.ovh/structures/{str(el)}/identifiers?filters[type]=siret", headers=paysage_headers)
            r = rinit.json()['data']
            for i in r:
                response=pd.json_normalize(r)
                paysage_siret=pd.concat([paysage_siret, response], ignore_index=True)

        except requests.exceptions.HTTPError as http_err:
            print(f"HTTP error occurred: {http_err}")
            response = pd.json_normalize({'id_source': i, 'status': 'http_error'})
            paysage_siret=pd.concat([paysage_siret, response], ignore_index=True)
            paysage_liste.append(str(i))
        except requests.exceptions.RequestException as err:
            print(f"Error occurred: {err}")
            response = pd.json_normalize({'id_source': i, 'status': 'request_error'})
            paysage_siret=pd.concat([paysage_siret, response], ignore_index=True)
            paysage_liste.append(str(i))
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            response = pd.json_normalize({'id_source': i, 'status': 'unexpected_error'})
            paysage_siret=pd.concat([paysage_siret, response], ignore_index=True)
            
    if len(paysage_siret)>0:
        paysage_siret=paysage_siret[['resourceId','value', 'endDate', 'active']].rename(columns={'resourceId':'id_clean','value':'siret', 'endDate':'siren_end_date'})
        paysage_siret['nb'] = paysage_siret.groupby('id_clean', dropna=False)['siret'].transform('count')
        paysage_siret=paysage_siret.loc[~((paysage_siret.nb>1)&((paysage_siret.active==False)|(~paysage_siret.siren_end_date.isnull())))].drop_duplicates()
        paysage_siret['siren']=paysage_siret.siret.str[:9]
        paysage_siret['nb'] = paysage_siret.groupby('id_clean', dropna=False).siren.transform('count')
        if any(paysage_siret['nb']>1):
            print(f"\n- doublons siren:\n{paysage_siret[paysage_siret['nb']>1]}")
            paysage_siret=paysage_siret[~((paysage_siret.nb>1)&(paysage_siret.siren.isin(siren_to_remove)))]
            paysage_siret['nb'] = paysage_siret.groupby('id_clean', dropna=False).siren.transform('count')
            if any(paysage_siret['nb']>1):
                print(f"- encore des doublons après suppresion de certains:\n{paysage_siret[paysage_siret['nb']>1]}")
            else:
                print(f"\n- doublons réglés")
        
        paysage_siret=paysage_siret.drop(columns=['siret', 'active', 'nb']).drop_duplicates()
        paysage_siret['nb'] = paysage_siret.groupby('id_clean', dropna=False)['siren'].transform('count')
        if any(paysage_siret['nb']>1):
            print(f"\n- s'il reste encore des doublons de siren : {paysage_siret[paysage_siret['nb']>1]}")
            paysage_siret=paysage_siret.groupby('id_clean', as_index=False).agg(lambda x: ';'.join(x.dropna().astype(str))).drop_duplicates()

        paysage=paysage.merge(paysage_siret.drop(columns='nb'), how='left', on='id_clean')
        print(f"\n- size paysage : {len(paysage)}")

        paysage['nb']=paysage.groupby('id')['id_clean'].transform('count')
        if len(paysage[paysage.nb>1])>0:
            print(f"\ndoublons dans paysage à régler à la source -> {paysage[paysage.nb>1][['id', 'id_clean', 'name_clean']]}")
        return paysage
################################################

def check_var_null(paysage):
    for i in ['cj_code', 'cj_name', 'sector', 'name_clean', 'acronym_clean']:
        print(f" {i}-> {paysage.loc[paysage[i].isnull()].id_clean.unique()}")

################################################

def IDpaysage_category(paysage):
    print("## IDpaysage category")
    paysage_liste=paysage['id_clean'].dropna().astype(str).unique().tolist()
    paysage_category=pd.DataFrame()
    n=0
    print(f"- size paysage id à catégoriser:{len(paysage_liste)}")

    for i in paysage_liste:
        time.sleep(0.2)
        n=n+1
        if n % 100 == 0: 
            print(f"{n}", end=',')  
        
        try:
            url2=f'https://api.paysage.dataesr.ovh/relations?filters[relationTag]=structure-categorie&filters[resourceId]={str(i)}&limit=100&sort=-endDate'
            rinit = requests.get(url2, headers=paysage_headers, verify=False)
            r = rinit.json()['data']
            if r:
                temp = pd.json_normalize(r)
                temp = (temp[['resourceId','relatedObject.id','relatedObject.displayName', 'relatedObject.priority', 'active', 'endDate']]
                        .rename(columns={'resourceId':'id_clean','relatedObject.id':'category_id',
                                        'relatedObject.displayName':'category_name', 'endDate':'category_end',
                                        'relatedObject.priority':'category_priority'})
                    .drop_duplicates())
                paysage_category=pd.concat([paysage_category, temp], ignore_index=True)

        except requests.exceptions.HTTPError as http_err:
            print(f"\n{i} -> HTTP error occurred: {http_err}")
            response = pd.json_normalize({'id_clean': i, 'status': 'http_error'})
            paysage_category=pd.concat([paysage_category, response], ignore_index=True)
            paysage_liste.append(str(i))
        except requests.exceptions.RequestException as err:
            print(f"\n{i} -> Error occurred: {err}")
            response = pd.json_normalize({'id_clean': i, 'status': 'request_error'})
            paysage_category=pd.concat([paysage_category, response], ignore_index=True)
            paysage_liste.append(str(i))
        except Exception as e:
            print(f"{i} -> An unexpected error occurred: {e}")
            response = pd.json_normalize({'id_clean': i, 'status': 'unexpected_error'})
            paysage_category=pd.concat([paysage_category, response], ignore_index=True)

    if len(paysage_category)>0:
        if 'status' in paysage_category.columns:   
            err=paysage_category.loc[~paysage_category.status.isnull()].id_clean.unique()
            print(err)
            print(f"{paysage_category.loc[paysage_category.id_clean.isin(err), ['id_clean', 'category_id']]}")
        
            paysage_category=paysage_category.loc[paysage_category.status.isnull()]
        print(f"\n- size resultat paysage_category:{len(paysage_category)}")

        paysage_category=paysage_category.sort_values(['id_clean', 'category_id', 'category_priority', 'category_end'], ascending=False)
        paysage_category['category_end'] = pd.to_datetime(paysage_category['category_end'], format='mixed', errors='ignore')

        paysage_category = paysage_category.loc[~(paysage_category.category_end<pd.Timestamp("today"))]
        paysage_category = paysage_category.groupby(['id_clean', 'category_id', 'category_priority']).first().reset_index()

        paysage_category = (paysage_category[['id_clean', 'category_id', 'category_name', 'category_priority']]
                            .sort_values(['id_clean', 'category_priority', 'category_id'])
                            .drop_duplicates())
        
        file_name = f"{PATH_API}paysage_category.pkl"
        with open(file_name, 'wb') as file:
            pd.to_pickle(paysage_category, file)
        return paysage_category

################################################

def get_mires():
    import requests, pandas as pd
    from config_api import paysage_headers
    from config_path import PATH_REF
    from dotenv import load_dotenv
    load_dotenv()

    ## liste opérateurs de la MIRES
    paysage_mires = pd.DataFrame()

    rinit = requests.get('https://api.paysage.dataesr.ovh/relations?filters[relationTag]=categorie-parent&filters[relatedObjectId]=41ZMP&limit=2000&sort=resource.priority', headers=paysage_headers)
    r = rinit.json()['data']

    search_id=pd.json_normalize(r).resourceId.unique()

    for i in search_id:   
        url_struct=f"https://api.paysage.dataesr.ovh/relations?filters[relationTag]=structure-categorie&filters[relatedObjectId]={i}&limit=2000&sort=-startDate"
        rinit2 = requests.get(url_struct, headers=paysage_headers)
        r2 = rinit2.json()['data']    
        if r2:
            result=(pd.json_normalize(r2)[['relatedObjectId', 'relatedObject.displayName', 'resourceId', 'resource.displayName']]
                .drop_duplicates()
                .rename(columns={'relatedObjectId':'operateur_code', 'relatedObject.displayName':'operateur_name', 'resourceId':'entities_id', 'resource.displayName':'struct_name'}))
            paysage_mires=pd.concat([paysage_mires, result], ignore_index=True)
            
    paysage_mires = (paysage_mires
               .assign(operateur_num=paysage_mires.operateur_name.replace('([^0-9]*)','', regex=True),
                       operateur_lib=paysage_mires.operateur_name.str.split('-').str[1].str.strip()
                  ))      
    paysage_mires.operateur_lib = paysage_mires.operateur_lib+" ("+paysage_mires.operateur_num+")"

    file_name = f"{PATH_API}operateurs_mires.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(paysage_mires, file)
    return paysage_mires
