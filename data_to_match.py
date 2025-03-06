import pandas as pd, pickle, numpy as np
pd.options.mode.copy_on_write = True
from IPython.display import HTML

# from api_requests.matcher import matcher
from step8_referentiels.referentiels import ref_externe_preparation
from step9_affiliations.prep_entities import entities_preparation
from functions_shared import work_csv
from step9_affiliations.organismes_cleaning import organismes_back


### one time
# organismes_back('2024')


# ref_externe_preparation()

# entities_preparation()
 ### si reprise du code en cours chargement des pickles -> entities_all
# keep = pd.read_pickle(f'{PATH}participants/data_for_matching/structure_fr.pkl')
# struct_et = pd.read_pickle(f'{PATH}participants/data_for_matching/struct_et.pkl')

def data_import():
    from config_path import PATH_MATCH,  PATH_CLEAN
    ref_all = pd.read_pickle(f"{PATH_MATCH}ref_all.pkl")
    print(f"size ref_all init: {len(ref_all)}")
    entities_all = pd.read_pickle(f'{PATH_MATCH}entities_all.pkl')
    print(f"size entities_all init: {len(entities_all)}")
    # pers = pd.read_pickle(f"{PATH_CLEAN}persons_current.pkl")
    # print(f"size persons: {len(pers)}")
    return ref_all, entities_all
ref_all, entities_all = data_import()

# perso = (perso[['project_id', 'generalPic', 'stage', 'tel_clean', 'email',
#        'domaine_email', 'contact', 'num_nat_struct']]
#        .mask(perso == ''))

# var_perso=['tel_clean', 'domaine_email', 'contact', 'num_nat_struct', 'email']
# perso=(pers
#     .mask(pers == '')
#     .groupby(['project_id', 'generalPic', 'stage'], as_index=False)[var_perso]
#     .agg(lambda x: ';'.join( x.dropna().unique()))
#     .drop_duplicates())
    
#         #    .agg(lambda x: x.split(';') if isinstance(x, str) else [])
# # perso=perso[~perso.astype(str).duplicated()]
# print(f"size perso for merging: {len(perso)}")

# print(f"size entities_all before perso: {len(entities_all)}")
# tmp=(entities_all.drop(columns='_merge')
#     .merge(perso, how='left', on=['project_id','generalPic', 'stage'], indicator=True))
# print(f"size entities_all after perso: {len(tmp)}")

# tmp1=tmp[tmp._merge=='both']

# var_perso.append('_merge')
# var_perso.remove('contact')
# tmp2=(tmp[tmp._merge=='left_only']
#     .drop(columns=var_perso)
#     .merge(perso.drop(columns=['stage'])
#     .drop_duplicates(), how='inner', on=['project_id','generalPic', 'contact']))

# if len(tmp2)>0:
#     # tmp=pd.concat([tmp[tmp._merge=='left_only'], tmp1, tmp2], ignore_index=True)
#     print(f"A verifier code si tmp2 n'est pas null: {len(tmp)}")
# else:
#     tmp=pd.concat([tmp[tmp._merge=='left_only'], tmp1], ignore_index=True)
#     print(f"size entities_all after perso clean: {len(tmp)}")

# #############
# #merge des nouveaux nns

# tmp=tmp.mask(tmp=='')
# tmp['num_nat_struct'] = tmp['num_nat_struct'].map(lambda x: x.split(';') if isinstance(x, str) else [])

# tmp.loc[tmp.rnsr_back.str.len()>0, 'method'] = 'orga'
# tmp.loc[(tmp.method.isnull())&(tmp.rnsr_merged.str.len()>0), 'method'] = 'corda'
# tmp.loc[(tmp.method.isnull())&(tmp.num_nat_struct.str.len()>0), 'method'] = 'openalex'
# tmp.loc[(tmp.method=='corda')&(tmp.num_nat_struct.str.len()>0), 'method'] = tmp.method+';openalex'
# tmp.loc[tmp.method.str.contains('openalex', na=False), 'resultat'] = 'a controler'


# tmp.loc[tmp.rnsr_merged.isnull(), 'rnsr_merged'] = tmp.loc[tmp.rnsr_merged.isnull(),'rnsr_merged'].apply(lambda x: [])
# tmp.loc[(tmp.method=='corda')|(tmp.method=='openalex'), 'rnsr_merged'] = tmp.apply(lambda x: list(set(x['rnsr_merged'] + x['num_nat_struct'])), axis=1)


### affiliations by mail
def get_id_by_var(df, var):
    temp=df.loc[(~df[var].isnull())&(df.rnsr_merged.str.len()==0)][var].str.split(';').explode()
    res=ref_all.loc[(~ref_all[var].isnull())&(ref_all[var].isin(temp))]

    ref=list(set(res.ref))
    var_keep=[var]

    def get_id_by_ref(var_keep, ref):
        src={'paysage':['numero_paysage'], 'ror':['numero_ror'], 'rnsr':['num_nat_struct'], 'sirene':['siren', 'siret']}
        for k,v in src.items():
            if k in ref:
                var_keep.extend(v)
        return var_keep

    res=res[get_id_by_ref(var_keep, ref)]

    df=df.assign(temp_tmp=df[var].str.split(';'), nns_temp=np.nan)
    for i, row in res.iterrows():
        df.loc[~df[var].isnull(), 'nns_temp'] = df.loc[~df[var].isnull()].apply(lambda x: row['num_nat_struct'] if row[var] in x['temp_tmp'] else x['nns_temp'], axis=1)

    df['nns_temp'] = df['nns_temp'].map(lambda x: x.split(';') if isinstance(x, str) else [])
    df.loc[(df.nns_temp.str.len()>0)&(df.method!='orga'), 'rnsr_merged'] = df.apply(lambda x: list(set(x['rnsr_merged'] + x['nns_temp'])), axis=1)
    df.loc[(df.nns_temp.str.len()>0)&(df.method.isnull()), 'method'] = var
    df.loc[(df.nns_temp.str.len()>0)&(~df.method.isin([var, 'orga'])), 'method'] = df.method+f';{var}'
    return df

tmp=get_id_by_var(entities_all, 'email')
tmp=get_id_by_var(tmp, 'tel_clean')



for i in ['rnsr_merged', 'org_merged', 'lab_merged']:
    tmp[i]=tmp[i].fillna('')
    tmp.loc[tmp[i].str.len()==0, i]=''

tmp[['rnsr_merged', 'org_merged', 'lab_merged']]=tmp[['rnsr_merged', 'org_merged', 'lab_merged']].map(lambda x: ' '.join(filter(None, x)))




tmp[['project_id', 
    'generalPic', 
    'orderNumber', 
    'country_code', 
    'country_name_fr', 
    'entities_full',
    'stage', 
    'role',  
    'org_merged', 
    'lab_merged', 
    'rnsr_merged', 
    'match', 
    'webPage', 
    'email',
    'resultat',
    'postalCode', 
    'code_postal', 
    'street_2', 
    'street_2_tag',
    'city',
    'city_tag',
    'domaine_email', 
    'contact',  
    'method']]



tmp.rename(columns={'webPage':'web',
                'street':'adresse',
                'entities_full':'nom_entier',
                'generalPic'
                }
            )



# from config_path import PATH_MATCH
# et=pd.read_pickle(f'{PATH_MATCH}struct_et.pkl')