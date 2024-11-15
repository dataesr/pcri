import pandas as pd
from main_library import *

#################
# 1 - si nouvelle actualisation utiliser MAIN_FIRST_PROCESS
#################

# si traitement déjà effectués
### si besoin de charger les permiers traitements sns recommencer depuis le debut
        
projects = pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl")         
participation = pd.read_pickle(f"{PATH_CLEAN}participation_current.pkl") 
countries = pd.read_pickle(f"{PATH_CLEAN}country_current.pkl") 
entities_info = pd.read_pickle(f"{PATH_CLEAN}entities_info_current2.pkl") 
calls = pd.read_csv(f"{PATH_CONNECT}calls.csv", sep=";", parse_dates=['call_deadline'])

# step4
entities_part = ent(participation, entities_info, projects)
h20, FP7, FP6, h20_p, FP7_p, FP6_p = framework_load()
h20 = h20.reindex(sorted(h20.columns), axis=1)
entities_participation = entities_preparation(entities_part, h20)


# ### entities pour ODS
import math

tmp=(entities_participation[
    ['category_woven', 'cordis_is_sme', 'cordis_type_entity_acro', 'stage','acronym',
    'cordis_type_entity_code', 'cordis_type_entity_name_en', 'entities_name_source',
    'cordis_type_entity_name_fr', 'extra_joint_organization', 'is_ejo',
    'country_code', 'country_code_mapping',
    'country_group_association_code', 'country_group_association_name_en',
    'country_group_association_name_fr', 'country_name_en',
    'country_name_fr', 'country_name_mapping', 
    'participation_nuts', 'region_1_name', 'region_2_name', 'regional_unit_name',
    'entities_acronym', 'entities_id', 'entities_name', 'operateur_name',
    'insee_cat_code', 'insee_cat_name', 'participates_as', 'project_id',
    'role', 'ror_category', 'sector', 'paysage_category', 
    'coordination_number', 'calculated_fund', 'with_coord','abstract', 
    'number_involved', 'action_code', 'action_name', 'call_id', 'topic_code',
    'status_code', 'framework', 'call_year', 'ecorda_date', 'flag_entreprise',
    'pilier_name_en', 'pilier_name_fr', 'programme_code', 'programme_name_en', 
    'programme_name_fr', 'thema_code', 'thema_name_fr', 'thema_name_en', 'destination_code',
    'destination_lib', 'destination_name_en','action_code2', 'action_name2', 'free_keywords', 
    'operateur_num','operateur_lib', 'category_agregation', 'source_id', 'generalPic']]
    .rename(columns={ 
        'source_id':'entities_id_source',
        'action_code':'action_id', 
        'action_name':'action_name',
        'action_code2':'action_detail_id', 
        'action_name2':'action_detail_name',
        'calculated_fund':'fund_€',
        'number_involved':'numberofparticipants',
        'coordination_number':'coordination_number',
        'country_group_association_code':'country_association_code',
        'country_group_association_name_en':'country_association_name_en',
        'country_group_association_name_fr':'country_association_name_fr',
        'with_coord':'flag_coordination',
        'is_ejo':'flag_organization',
        'generalPic':'pic_number',
        'insee_cat_code':'entreprise_cat_code',
        'insee_cat_name':'entreprise_cat_name'
        }))

tmp.loc[tmp.entities_id_source=='ror', 'entities_id'] = tmp.loc[tmp.entities_id_source=='ror', 'entities_id'].str.replace("^R", "", regex=True)
tmp.loc[tmp.entities_id_source=='pic', 'entities_id_source'] = 'ecorda pic'
tmp.loc[tmp.entities_id_source=='identifiantAssociationUniteLegale', 'entities_id_source'] = 'rna'
tmp.loc[(tmp.entities_id_source.isnull())&(tmp.entities_id.str.contains('gent', na=False)), 'entities_id_source'] = 'paysage'

#     if i=='successful':
act_liste = ['RIA', 'MSCA', 'IA', 'CSA', 'ERC', 'EIC']
tmp = tmp.assign(action_group_code=tmp.action_id, action_group_name=tmp.action_name)
tmp.loc[~tmp.action_id.isin(act_liste), 'action_group_code'] = 'ACT-OTHER'
tmp.loc[~tmp.action_id.isin(act_liste), 'action_group_name'] = 'Others actions'

tmp.loc[tmp.thema_code.isin(['ERC','MSCA']), ['destination_code', 'destination_name_en']] = np.nan

for i in ['abstract', 'free_keywords']:
    tmp[i] = tmp[i].str.replace('\\n|\\t|\\r|\\s+|^\\"', ' ', regex=True).str.strip()

tmp['free_keywords'] = tmp['free_keywords'].str.lower()

tmp.loc[(tmp.stage=='successful')&(tmp.status_code=='UNDER_PREPARATION'), 'abstract'] = np.nan

# attention si changement de nom de vars -> la modifier aussi dans pcri_info_columns_order
tmp = order_columns(tmp, 'proj_entities')


for h in tmp.framework.unique():
    x = tmp[(tmp.stage=='successful')&(tmp.framework==h)].drop(columns=['stage'])
    chunk_size = int(math.ceil((x.shape[0] / 2)))
    i=0
    for start in range(0, x.shape[0], chunk_size):
        df_subset = x.iloc[start:start + chunk_size]
        i=i+1
        if h=='Horizon Europe':
            he='horizon'
        else:
            he='h2020'
        
        zipfile_ods(df_subset, f"fr-esr-{he}-projects-entities{i}")
        # df_subset.to_csv(f'{PATH_ODS}fr-esr-{he}-projects-entities{i}.csv', sep=';', encoding='utf-8', index=False, na_rep='', decimal=".")

tmp1 = (tmp.loc[(tmp.stage=='evaluated')]
    .rename(columns={ 'number_involved':'numberofapplicants'})
    .drop(columns=
        ['country_name_mapping', 'country_association_name_en', 'country_name_en', 
        'country_code_mapping', 'pilier_name_fr', 'programme_code', 'entities_name_source',
        'operateur_num','operateur_lib', 'ror_category', 'paysage_category', 'country_association_name_en',
        'country_association_name_fr', 'thema_name_fr', 'destination_lib',
        'programme_name_fr', 'action_group_code', 'action_group_name', 'stage',
        'cordis_type_entity_name_en', 'cordis_type_entity_acro','cordis_type_entity_name_fr']))


for h in tmp1.framework.unique():
    x = tmp1[(tmp1.framework==h)]
    chunk_size = int(math.ceil((x.shape[0] / 2)))
    i=0
    for start in range(0, x.shape[0], chunk_size):
        df_subset = x.iloc[start:start+chunk_size]
        i=i+1
        if h=='Horizon Europe':
            he='horizon'
        else:
            he='h2020'

        zipfile_ods(df_subset, f"fr-esr-{he}-projects-entities_evaluated{i}")