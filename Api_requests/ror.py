import requests, time, pandas as pd, copy
from config_path import PATH_SOURCE, PATH_WORK
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


def ror_cleaning(r):
    ror_source=(pd.DataFrame(r)[['id','name','acronyms','types','country_code']]
                .rename(columns={'id':'id_clean','name':'name_clean','acronyms':'acronym_clean','types':'ror_category'})
                .drop_duplicates())
    ror_tmp = pd.DataFrame(r)
    ror_tmp = (ror_tmp[['id','name','types','country_code','relation_name','relation_id','relation_type']]
            .rename(columns={'id':'id_source'}))

    def explode_ror(df):
        df = df.assign(relation_name=df['relation_name'].str.split(';'), relation_type=df['relation_type'].str.split(';'), relation_id=df['relation_id'].str.split(';'))
        df = df.explode(['relation_name','relation_type','relation_id']).drop_duplicates()  
        return df

    '''ror no parent no successor'''
    ror = (ror_tmp.loc[(~ror_tmp.relation_type.str.contains("Parent", na=False)) & (~ror_tmp.relation_type.str.contains("Successor", na=False))]
                .filter(regex=r'^(?!relation_)', axis=1)
                .drop_duplicates()
                .assign(nb_parent=0))[['id_source', 'country_code', 'nb_parent']]
    print(f"1 - ror:{len(ror)}")


    '''ror successor no parent'''
    ror_successor = ror_tmp.loc[(~ror_tmp.relation_type.str.contains("Parent", na=False)) & (ror_tmp.relation_type.str.contains("Successor", na=False))]
    ror_successor = explode_ror(ror_successor).rename(columns={'id':'id_source'})
    ror_successor = ror_successor.loc[ror_successor.relation_type=='Successor']
    ror_successor['nb_succ'] = ror_successor.groupby(['id_source'], dropna=False)['relation_id'].transform('count')
    print(f"2 - ror_successor:{len(ror_successor)}")

    if ror_successor.empty:
        pass
    else:
        if len(ror_successor['nb_succ']>1)>0:
            ror_s = (ror_successor.loc[ror_successor.nb_succ>1]
                    .filter(regex=r'^(?!relation_)', axis=1)
                    .drop_duplicates()
                    .assign(nb_parent=0))[['id_source', 'country_code', 'nb_parent']]
            print(f"3 - ror_s:{len(ror_s)}")
        else:
            ror = pd.concat([ror, ror_s], ignore_index=True)
            print(f"4 - ror:{len(ror)}")


        if len(ror_successor['nb_succ']==1)>0:
            ror_s = (ror_successor.loc[ror_successor.nb_succ==1][['id_source', 'relation_id']]
                    .rename(columns={'relation_id':'id_sec'})
                    .merge(ror_tmp.rename(columns={'id_source':'id_sec'}), how='left', on='id_sec'))
            print(f"5 - ror_s:{len(ror_s)}")

            '''if successor no has new parent'''
            ror = pd.concat([ror, (ror_s.loc[(~ror_s.relation_type.str.contains("Parent", na=False))]
                    .filter(regex=r'^(?!relation_)', axis=1)
                    .drop_duplicates()
                    .assign(nb_parent=0)[['id_source', 'id_sec', 'country_code', 'nb_parent']])], ignore_index=True)
            print(f"6 - ror:{len(ror)}")

            '''if successor has new parent'''
            ror_s = ror_s.loc[(ror_s.relation_type.str.contains("Parent", na=False))]
            ror_s = explode_ror(ror_s)
            ror_s = ror_s.loc[ror_s.relation_type=='Parent'][['id_source', 'id_sec', 'country_code', 'relation_id']].rename(columns={'relation_id':'parent_id'})
            ror_s['nb_parent'] = ror_s.groupby(['id_sec'], dropna=False)['parent_id'].transform('count')
            print(f"7 - ror_s with parent:{len(ror_s)}")

        
    '''ror with Parent no successor'''
    ror_child = ror_tmp.loc[(ror_tmp.relation_type.str.contains("Parent", na=False)) & (~ror_tmp.relation_type.str.contains("Successor", na=False))]
    ror_child = explode_ror(ror_child).rename(columns={'id':'id_source'})
    ror_child = ror_child.loc[ror_child.relation_type=='Parent'][['id_source', 'country_code', 'relation_id']].rename(columns={'relation_id':'parent_id'})
    ror_child['nb_parent'] = ror_child.groupby(['id_source'], dropna=False)['parent_id'].transform('count')

    if 'ror_s' in globals() or 'ror_s' in locals():
        ror_child = pd.concat([ror_child, ror_s], ignore_index=True)[['id_source', 'id_sec', 'country_code','nb_parent','parent_id']]
        print(f"8 - ror_child:{len(ror_child)}")

    ror_child = ror_child.merge(ror_tmp.rename(columns={'id_source':'parent_id','name':'parent_name', 'types':'parent_types', 'country_code':'parent_country_code'}), how='left', on='parent_id')

    '''if country_code!=parent_country_code'''
    if len(ror_child['country_code']!=ror_child['parent_country_code'])>0:
        
        if 'id_sec' in ror_child.columns:
            cols_select=['id_source', 'id_sec', 'country_code', 'nb_parent']
        else:
            cols_select=['id_source', 'country_code', 'nb_parent']
            
        ror = pd.concat([ror, (ror_child.loc[ror_child['country_code']!=ror_child['parent_country_code']]
                .drop_duplicates()
                .assign(nb_parent=0)[cols_select])], ignore_index=True)
        print(f"9 - ror:{len(ror)}")


    ror_child = ror_child[~ror_child.id_source.isin(ror.id_source.unique())]

    '''if just one parent without other parent'''
    if 'id_sec' in ror_child.columns:
        cols_select=['id_source', 'id_sec', 'country_code', 'parent_id', 'nb_parent']
    else:
        cols_select=['id_source', 'country_code', 'parent_id', 'nb_parent']
    ror = pd.concat([ror, 
                (ror_child[(~ror_child.relation_type.str.contains('Parent', na=False)) & (ror_child.nb_parent==1)]
                .filter(regex=r'^(?!relation_)', axis=1)
                .drop_duplicates()[cols_select])], ignore_index=True)

    print(f"10 - ror:{len(ror)}")
    ror_child = ror_child[~ror_child.id_source.isin(ror.id_source.unique())]
    print(f"11 - ror_child:{len(ror_child)}")

    '''if ++ parent keep id_source'''
    if 'id_sec' in ror_child.columns:
        cols_select=['id_source', 'id_sec', 'country_code']
    else:
        cols_select=['id_source', 'country_code']
    ror = pd.concat([ror, 
                (ror_child[ror_child.nb_parent>1]
                .filter(regex=r'^(?!relation_)', axis=1)
                .drop_duplicates()
                .assign(nb_parent=0)[cols_select])], ignore_index=True)
    print(f"12 - ror:{len(ror)}")
    ror_child = ror_child[~ror_child.id_source.isin(ror.id_source.unique())]
    print(f"13 - ror_child:{len(ror_child)}")

    ror_child = explode_ror(ror_child)
    ror_child = (ror_child[(ror_child.relation_type=='Parent')]
    .rename(columns={'relation_id':'parent_id2', 'relation_name':'parent_id2_name'})
    .drop(columns='relation_type'))
    ror_child['nb_parent2'] = ror_child.groupby(['id_source','parent_id'], dropna=False)['parent_id2'].transform('count')

    if len(ror_child['nb_parent2']>1)>0:
        ror = pd.concat([ror, 
                        (ror_child[ror_child.nb_parent2>1][['id_source', 'id_sec', 'country_code', 'nb_parent', 'parent_id']]
                        .drop_duplicates())], ignore_index=True)
    print(f"14 - ror:{len(ror)}")
    ror_child = ror_child[~ror_child.id_source.isin(ror.id_source.unique())]
    print(f"15 - ror_child:{len(ror_child)}")

    print("16 - Attention ! il faut peut-être vérifier s'il y a des parents en sus à traiter cette fois-ci")
    if 'id_sec' in ror_child.columns:
        cols_select=['id_source', 'id_sec', 'country_code', 'nb_parent', 'parent_id', 'parent_id2']
    else:
        cols_select=['id_source', 'country_code', 'nb_parent', 'parent_id', 'parent_id2']
    ror = pd.concat([ror, 
                    (ror_child[cols_select]
                    .drop_duplicates())], ignore_index=True).drop_duplicates()
    print(f"17 - ror:{len(ror)}")


    ror['id_clean'] = ror.id_source
    if 'id_sec' in ror.columns:
        ror.loc[:,'id_clean'] = ror['id_clean'].mask(~ror.id_sec.isnull(), ror.id_sec).mask(~ror.parent_id.isnull(), ror.parent_id).mask(~ror.parent_id2.isnull(), ror.parent_id2)
    else:
        ror.loc[:,'id_clean'] = ror['id_clean'].mask(~ror.parent_id.isnull(), ror.parent_id).mask(~ror.parent_id2.isnull(), ror.parent_id2)

    ## verifier la prochaine fois si passer d'abord par id_sec et non id-source à la place de id_parent  -> trop problématique  
    # redressement helmholtz, gov italien, minsitere...
    del_parent = ['R008d1xp69', 'R0281dp749', 'R00xqkj280', 'R05a4cm665','R045vgz384']
    ror.loc[ror.id_clean.isin(del_parent), 'unused_parent'] = ror.id_clean
    ror.loc[ror.id_clean.isin(del_parent), 'id_clean'] = ror.id_source
        
    ror = ror[['id_source', 'id_clean', 'unused_parent']].drop_duplicates().merge(ror_source,how='left',on='id_clean').drop_duplicates()
    print(f"18 - id_source+id_clean doublon dans ror resultat ci-dessous :\n{ror[ror.groupby('id_source')['id_clean'].transform('count')>1][['id_source','id_clean']].drop_duplicates()}")

    reste = ror_source.loc[~ror_source.id_clean.isin(ror.id_source)]
    reste = reste.assign(id_source=reste.id_clean)

    ror=pd.concat([ror, reste], ignore_index=True)

    add_acro = {'R012kf4317':'Humboldt Foundation', 
                'R02e2c7k09':'TU Delft', 
                'R0281dp749':'Helmholtz Association', 
                'R01nffqt88':'POLIMI',
                'R008x57b05':'UAntwerp',
                'R03prydq77':'Univie',
                'R03yj89h83':'U. Oulu',
                'R00cv9y106':'UGent',
                'R012p63287':'RUG',
                'R01rvn4p91':'ETH Zurich',
                'R013meh722':'U. Cambridge',
                'R04cw6st05':'U. London'   
            }


    for k,v in add_acro.items():
        ror.loc[(ror.id_clean==k)&(ror.acronym_clean.isnull()), 'acronym_clean'] = v

    print(f"new size ror: {len(ror)}")
    return ror
########################################

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


    ror_list = [e['api_id'][1:] for e in lid_source if e['source_id']=='ror']
    print(f"nombre d'identifiants ror à extraire: {len(ror_list)}")

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

