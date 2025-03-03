import pandas as pd, pickle
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
    # perso = pd.read_pickle(f"{PATH_CLEAN}perso_app.pkl")
    # print(f"size perso init: {len(perso)}")
    ref_all = pd.read_pickle(f"{PATH_MATCH}ref_all.pkl")
    print(f"size ref_all init: {len(ref_all)}")
    entities_all = pd.read_pickle(f'{PATH_MATCH}entities_all.pkl')
    print(f"size entities_all init: {len(entities_all)}")
    perso = pd.read_pickle(f"{PATH_CLEAN}persons_current.pkl")
    print(f"size persons: {len(perso)}")
    return ref_all, entities_all, perso
ref_all, entities_all, perso = data_import()

perso = (perso[['project_id', 'generalPic', 'stage', 'tel_clean',
       'domaine_email', 'contact', 'num_nat_struct']]
       .drop_duplicates()
       .mask(perso == ''))

print(f"size perso for merging: {len(perso)}")
# perso.mask(perso == '', inplace=True)
var_perso=['tel_clean', 'domaine_email', 'contact', 'num_nat_struct']
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

### affil perso to struct -> search labo by openalex
entities_tmp = (
    entities_all.loc[((entities_all.country_code=='FRA')&(entities_all.rnsr_merged.str.len()==0)),
                    ['project_id','generalPic', 'pic', 'country_code', 'entities_id', 'entities_name']])
# entities_tmp=entities_all.loc[((entities_all.country_code=='FRA')&(entities_all.rnsr_merged.str.len()==0))|((entities_all.country_code!='FRA')&(entities_all.entities_id.str.contains('pic'))), ['project_id','generalPic', 'pic', 'country_code', 'entities_id', 'entities_name']]

# persons déjà traité juste faire merge avec refext et autres

### affil perso to ref_all by phone
# aff_by_tel = perso.loc[~perso.tel_clean.isnull()].merge(ref_all.loc[~ref_all.tel_clean.isnull()], how='inner', on='tel_clean')
# print(f"size aff_by_tel: {len(aff_by_tel)}")

