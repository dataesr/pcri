import requests, time, pandas as pd, copy
# from config_api import sirene_headers, scanr_headers, paysage_headers
from config_path import PATH_SOURCE, PATH_WORK, PATH_REF
from Api_requests.ror import *
from dotenv import load_dotenv
load_dotenv()
requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning) 


def ror_info(result: list):
    
    delete = ['addresses', 'country', 'lat', 'lng', 'wikipedia_url',  'labels', 'established', 'relationships']

    to_keep = []
    for p in result:
        if p:
            elem = {k: v for k, v in p.items() if (v and v != "NaT")}  

            elem['link_ror'] = elem.get('id')
            elem['id'] = 'R' + elem.get('id').split('/')[-1]

            elem['country_code'] = elem.get('country').get('country_code')
            elem['date_start'] = str(elem.get('established'))



            elem['labels_name'] = []
            elem['labels_language'] = []
            if elem.get('labels'):
                for lab in elem['labels']:
                    elem['labels_name'].append(lab.get('label', None))
                    elem['labels_language'].append(lab.get('iso639', None)) 


            elem['relation_name'] = []
            elem['relation_type'] = []
            elem['relation_id'] = []
            if elem.get('relationships'):
                for rel in elem['relationships']:
                    elem['relation_name'].append(rel.get('label', None))
                    elem['relation_type'].append(rel.get('type', None))
                    elem['relation_id'].append('R' + rel.get('id', None).split('/')[-1])


            if elem.get('addresses'):
                for ad in elem['addresses']:
                    elem['city'] = ad.get('city', None)
                    elem['latitude'] = ad["lat"]
                    elem['longitude'] = ad["lng"]

                    geo = ad.get('geonames_city')
                    if geo.get('geonames_admin1'):
                        elem['geo_admin1_code'] = geo.get('geonames_admin1').get('code', None)
                        elem['geo_admin1_name'] = geo.get('geonames_admin1').get('name', None)
                        elem['geo_admin1_name_ascii'] = geo.get('geonames_admin1').get('ascii_name', None)

                    if geo.get('nuts_level1'):
                        elem['geo_nuts1_code'] = geo.get('nuts_level1').get('code', None)
                        elem['geo_nuts1_name'] = geo.get('nuts_level1').get('name', None)                


            l = ['aliases', 'links', 'types', 'acronyms', 'labels_name', 'labels_language', 'relation_name', 'relation_type', 'relation_id']
            for e in l:
                if elem.get(e):
                    elem[e] = ';'.join([code for code in elem.get(e) if code is not None])

            for field in delete:
                if elem.get(field):
                    elem.pop(field)

            elem = {k: v for k, v in elem.items() if (v and v != "NaT")}
            to_keep.append(elem)
        
    return to_keep


# def ror_query(id):
#     time.sleep(0.3)
#     try:
#         url = 'https://api.ror.org/organizations?query=' + id
#         rinit = requests.get(url, verify=False)

#         if rinit.status_code == 200:
#             r = rinit.json()
#             nb = r.get('number_of_results')
#             if nb != 0:
#                 for i in range(nb):
#                     return r.get('items')[i]
                
#     except requests.exceptions.HTTPError as http_err:
#         print(f"\n{i} -> HTTP error occurred: {http_err}")
#         ror_list.append(str(i))
#     except requests.exceptions.RequestException as err:
#         print(f"\n{i} -> Error occurred: {err}")
#         ror_list.append(str(i))
#     except Exception as e:
#         print(f"\n{i} -> An unexpected error occurred: {e}")


# def ror_relation(result):
    
#     def relation_list(result):
#         print(f"1 - size ror: {len(result)}")
#         id_relat=[]
#         for i in [i for i in result if i is not None]:
#             if i.get('relationships', []):
#                 for elem in i['relationships']:
#                     if elem['type'] in ['Parent', 'Successor']:
#                         id_relat.append(elem['id'].split('/')[-1])
#         return id_relat        
                    
#     id_relat=relation_list(result)               

#     id_result = []
#     for elem in result:
#         if elem['id']:
#             id_result.append(elem['id'].split('/')[-1])
#     id_relat = list(set(id_relat).difference(set(id_result)))
    
#     print(f'2 - traitement relations id_relat={len(id_relat)}')
    
#     n=0
#     while len(id_relat)>0:
#         for id in id_relat:
#             result.append(ror_query(id))
#         id_relat = relation_list(result)
#         id_result=[elem['id'].split('/')[-1] for elem in result if elem['id'] is not None]
#         id_relat = list(set(id_relat).difference(set(id_result)))                    
#         n+=1
#         print(n, end=',')
#     print(f"3- size new ror:{len(result)}")
#     return result

def get_ror(lid_source, ror_old=None):

    ####################
    def ror_query(id):
        time.sleep(0.3)
        try:
            url = 'https://api.ror.org/organizations?query=' + id
            rinit = requests.get(url, verify=False)

            if rinit.status_code == 200:
                r = rinit.json()
                nb = r.get('number_of_results')
                if nb != 0:
                    for i in range(nb):
                        return r.get('items')[i]
                    
        except requests.exceptions.HTTPError as http_err:
            print(f"\n{i} -> HTTP error occurred: {http_err}")
            ror_list.append(str(i))
        except requests.exceptions.RequestException as err:
            print(f"\n{i} -> Error occurred: {err}")                    
            ror_list.append(str(i))
        except Exception as e:
            print(f"\n{i} -> An unexpected error occurred: {e}")
    #######################

    def ror_relation(result):
        
        def relation_list(result):
            print(f"1 - size ror: {len(result)}")
            id_relat=[]
            for i in [i for i in result if i is not None]:
                if i.get('relationships', []):
                    for elem in i['relationships']:
                        if elem['type'] in ['Parent', 'Successor']:
                            id_relat.append(elem['id'].split('/')[-1])
            return id_relat        
                        
        id_relat=relation_list(result)               

        id_result = []
        for elem in result:
            if elem['id']:
                id_result.append(elem['id'].split('/')[-1])
        id_relat = list(set(id_relat).difference(set(id_result)))
        
        print(f'2 - traitement relations id_relat={len(id_relat)}')
        
        n=0
        while len(id_relat)>0:
            for id in id_relat:
                result.append(ror_query(id))
            id_relat = relation_list(result)
            id_result=[elem['id'].split('/')[-1] for elem in result if elem['id'] is not None]
            id_relat = list(set(id_relat).difference(set(id_result)))                    
            n+=1
            print(n, end=',')
        print(f"3- size new ror:{len(result)}")
        return result
    ###################


    print("### ROR")
    ### traitement identifiant ROR
    ror_list = [e['api_id'][1:] for e in lid_source if e['source_id']=='ror']
    print(f"nombre d'identifiants ror à extraire: {len(ror_list)}")
    # ror2=ror_info(ror_list)

    ror_result=[]
    n=0

    for id in ror_list:
        n=n+1
        print(f"{n}", end=',')
        ror_result.append(ror_query(id))  

    while None in ror_result:
        ror_result.remove(None)   
        
    if ror_result:    
        file_name = f"{PATH_WORK}ror_current.pkl"
        with open(file_name, 'wb') as file:
            pd.to_pickle(ror_result, file)
            
    ror_result=ror_relation(ror_result)

    ror_df=ror_info(ror_result)
    ror_df=pd.json_normalize(ror_df)

    if ror_df.empty:
        print("ror_df est vide")
    else:
        if 'ror_old' in globals() or 'ror_old' in locals():
            r = pd.concat([ror_old, ror_df], ignore_index=True)
        else:
            r = copy.deepcopy(ror_df)
            
        file_name = f"{PATH_SOURCE}ror_df.pkl"
        with open(file_name, 'wb') as file:
            pd.to_pickle(r, file)
    return r