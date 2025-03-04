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
    perso = pd.read_pickle(f"{PATH_CLEAN}persons_current.pkl")
    print(f"size persons: {len(perso)}")
    return ref_all, entities_all, perso
ref_all, entities_all, perso = data_import()

perso = (perso[['project_id', 'generalPic', 'stage', 'tel_clean', 'email',
       'domaine_email', 'contact', 'num_nat_struct']]
       .drop_duplicates()
       .mask(perso == ''))

print(f"size perso for merging: {len(perso)}")
# perso.mask(perso == '', inplace=True)
var_perso=['tel_clean', 'domaine_email', 'contact', 'num_nat_struct', 'email']
perso=(perso.groupby(['project_id', 'generalPic', 'stage'], as_index=False)[var_perso]
     .agg(lambda x: ';'.join( x.dropna().unique()))
     .drop_duplicates())

print(f"size entities_all before perso: {len(entities_all)}")
tmp=(entities_all.drop(columns='_merge')
    .merge(perso, how='left', on=['project_id','generalPic', 'stage'], indicator=True))
print(f"size entities_all after perso: {len(tmp)}")

tmp1=tmp[tmp._merge=='both']

var_perso.append('_merge')
var_perso.remove('contact')
tmp2=(tmp[tmp._merge=='left_only']
    .drop(columns=var_perso)
    .merge(perso.drop(columns=['stage'])
    .drop_duplicates(), how='inner', on=['project_id','generalPic', 'contact']))

if len(tmp2)>0:
    # tmp=pd.concat([tmp[tmp._merge=='left_only'], tmp1, tmp2], ignore_index=True)
    print(f"A verifier code si tmp2 n'est pas null: {len(tmp)}")
else:
    tmp=pd.concat([tmp[tmp._merge=='left_only'], tmp1], ignore_index=True)
    print(f"size entities_all after perso clean: {len(tmp)}")

tmp=tmp.mask(tmp=='')
tmp.loc[tmp.rnsr_back.str.len()>0, 'source_rnsr'] = 'orga'
tmp.loc[(tmp.source_rnsr.isnull())&(tmp.rnsr_merged.str.len()>0), 'source_rnsr'] = 'corda'
tmp.loc[(tmp.source_rnsr.isnull())&(~tmp.num_nat_struct.isnull()), 'source_rnsr'] = 'openalex'
tmp.loc[tmp.source_rnsr=='openalex', 'resultat'] = 'a controler'

tmp['num_nat_struct'] = tmp['num_nat_struct'].map(lambda x: x.split(';') if isinstance(x, str) else [])
tmp.loc[tmp.rnsr_merged.isnull(), 'rnsr_merged'] = tmp.loc[tmp.rnsr_merged.isnull(),'rnsr_merged'].apply(lambda x: [])
tmp.loc[(tmp.source_rnsr=='corda')|(tmp.source_rnsr=='openalex'), 'rnsr_merged'] = tmp.apply(lambda x: list(set(x['rnsr_merged'] + x['num_nat_struct'])), axis=1)


### affil perso to ref_all by phone

## affiliations by mail
mail=tmp.loc[(~tmp.email.isnull())&(tmp.rnsr_merged.str.len()==0)].email.str.split(';').explode()
res=ref_all.loc[(~ref_all.email.isnull())&(ref_all.email.isin(mail))]

ref=list(set(res.ref))
var_keep=['email']
src={'paysage':['numero_paysage'], 'ror':['numero_ror'], 'rnsr':['num_nat_struct'], 'sirene':['siren', 'siret']}
for k,v in src.items():
    if k in ref:
        var_keep.extend(v)

res=res[var_keep]

tmp=tmp.assign(email_tmp=tmp.email.str.split(';'), nns_mail=np.nan)
for i, row in res.iterrows():
    tmp.loc[~tmp.email.isnull(), 'nns_mail'] = tmp.loc[~tmp.email.isnull()].apply(lambda x: row['num_nat_struct'] if row['email'] in x['email_tmp'] else x['nns_mail'], axis=1)

tmp['nns_mail'] = tmp['nns_mail'].map(lambda x: x.split(';') if isinstance(x, str) else [])
tmp.loc[(tmp.nns_mail.str.len()>0)&(tmp.source_rnsr!='orga'), 'rnsr_merged'] = tmp.apply(lambda x: list(set(x['rnsr_merged'] + x['nns_mail'])), axis=1)
tmp.loc[(tmp.nns_mail.str.len()>0)&(tmp.source_rnsr.isnull()), 'source_rnsr'] = 'email'
tmp.loc[(tmp.nns_mail.str.len()>0)&(~tmp.source_rnsr.isin(['email', 'orga'])), 'source_rnsr'] = tmp.source_rnsr+';email'


# aff_by_tel = tmp.loc[~perso.tel_clean.isnull()].merge(ref_all.loc[~ref_all.tel_clean.isnull()], how='inner', on='tel_clean')
# print(f"size aff_by_tel: {len(aff_by_tel)}")




for i in ['rnsr_merged', 'org_merged', 'lab_merged']:
    tmp[i]=tmp[i].fillna('')
    tmp.loc[tmp[i].str.len()==0, i]=''

tmp[['rnsr_merged', 'org_merged', 'lab_merged']]=tmp[['rnsr_merged', 'org_merged', 'lab_merged']].map(lambda x: ' '.join(filter(None, x)))