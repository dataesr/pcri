import pandas as pd, numpy as np
from config_path import PATH_CONNECT


print("### MSCA / ERC")
select_cols = ['action_code', 'action_name','calculated_fund', 'call_year', 'coordination_number',
    'cordis_type_entity_acro', 'cordis_type_entity_code', 'country_code', 'with_coord',
    'cordis_type_entity_name_en', 'cordis_type_entity_name_fr', 'extra_joint_organization',
    'country_group_association_code', 'country_group_association_name_en',
    'country_group_association_name_fr', 'country_name_en',  'free_keywords', 'abstract',
    'country_name_fr', 'destination_code', 'destination_detail_code', 
    'destination_detail_name_en', 'destination_name_en',  'number_involved',
    'panel_code', 'panel_name', 'project_id', 'role', 'erc_role', 'flag_entreprise',
    'stage', 'status_code', 'framework', 'thema_code', 'category_woven', 'category_agregation', 'source_id','ecorda_date']

me7= FP7.loc[FP7.thema_code.isin(["ERC","MSCA"]), select_cols]
me6= FP6.loc[FP6.thema_code.isin(["ERC","MSCA"]), list(set(select_cols).difference(['panel_code', 'panel_name','erc_role', 'extra_joint_organization', 'free_keywords', 'abstract','source_id', 'category_agregation', 'category_woven', 'flag_entreprise']))]
me20=h20.loc[h20.programme_code.isin(['ERC', 'MSCA']), select_cols]


projects_m = (projects.assign(framework='Horizon Europe')
.loc[(projects.programme_code.isin(['HORIZON.1.2', 'HORIZON.1.1'])), 
['project_id','topic_code', 'call_id', 'call_year', 'thema_code', 'action_code', 'action_name', 
'framework', 'panel_code', 'panel_name', 'panel_regroupement_code', 'panel_regroupement_name', 
'stage', 'status_code', 'free_keywords', 'abstract', 'acronym',
'destination_code', 'destination_name_en', 'destination_detail_code', 
'destination_detail_name_en', 'ecorda_date']])

for i in ['abstract', 'free_keywords']:
    projects_m[i] = projects_m[i].str.replace('\\n|\\t|\\r|\\s+', ' ', regex=True).str.strip()

projects_m['free_keywords'] = projects_m['free_keywords'].str.lower()

# MSCA/ERC PROJECTS HE
print("## MSCA_ERC data")
msca_erc = projects_m.merge(part, how='inner', on=['project_id', 'stage']).sort_values(['project_id','stage','country_code'])
msca_erc = msca_erc.assign(with_coord=np.where(msca_erc.destination_code.isin(['PF','ACCELERATOR','COST'])|(msca_erc.thema_code=='ERC'), False, True))
msca_erc.loc[msca_erc.destination_code=='SyG', 'with_coord'] = True 
print(f"size projects_MSCA_ERC: {len(msca_erc)}")

msca_erc.reset_index(drop=True, inplace=True)
me20.reset_index(drop=True, inplace=True)
me7.reset_index(drop=True, inplace=True)
me6.reset_index(drop=True, inplace=True)
msca_erc = pd.concat([msca_erc, me20, me7, me6], ignore_index=True, axis=0, join='outer')
msca_erc = msca_erc.assign(is_ejo=np.where(msca_erc.extra_joint_organization.isnull(), 'Sans', 'Avec'))

print(f"size projects_MSCA_ERC with others fp: {len(msca_erc)}")
(msca_erc.drop(columns=['ecorda_date', 'free_keywords', 'abstract', 'acronym'])
    .to_csv(PATH_CONNECT+"msca_projects_part.csv", index=False, encoding="UTF-8", sep=";", na_rep='', decimal="."))
