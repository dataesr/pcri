from functions_shared import zipfile_ods, order_columns
import numpy as np

def collab_signed_ods(collab):
    tmp=(collab.assign(stage_name=np.where(collab.stage=='evaluated', 'projets évalués', 'projets lauréats')))

    tmpP=(tmp
          .loc[(tmp.stage=='evaluated')&(~tmp.thema_code.isin(['ERC', 'MSCA'])), ['project_id', 'total_cost']]
          .rename(columns={'total_cost':'proposal_budget'})
          .drop_duplicates())

    tmp.loc[(tmp.stage=='successful')&(tmp.status_code=='UNDER_PREPARATION'), 'abstract'] = np.nan
    tmp=(tmp.loc[(tmp.stage=='successful')&(~tmp.thema_code.isin(['ERC', 'MSCA']))&(tmp.status_code!='REJECTED')]
            .merge(tmpP, how='left', on='project_id')
            [['project_id', 'country_name_fr', 'country_name_fr_collab', 'call_year',  'abstract', 'free_keywords',
            'participates_as', 'participates_as_collab',  'action_code', 'action_name',
            'thema_code', 'destination_code',  'destination_name_en', 'extra_joint_organization', 'extra_joint_organization_collab',
            'part_num', 'coord_num', 'fund', 'part_num_collab', 'fund_collab', 'total_cost', 'proposal_budget',
            'nuts_code','region_1_name',  'nuts_code_collab', 'region_1_name_collab', 
            'country_code', 'country_code_collab', 'country_code_mapping_collab', 
            'country_name_en', 'country_name_en_collab',  'pilier_name_en', 'programme_name_en', 'thema_name_en', 'with_coord', 'ecorda_date']]

       .rename(columns={'with_coord':'flag_coordination'}) )       

    for i in ['abstract', 'free_keywords']:
        tmp[i] = tmp[i].str.replace('\\n|\\t|\\r|\\s+', ' ', regex=True).str.strip()

    tmp['free_keywords'] = tmp['free_keywords'].str.lower()

    print(f"size collab {len(tmp)}")
        
    tmp = order_columns(tmp, 'proj_collab')

    zipfile_ods(tmp, 'fr-esr-all-signed-projects-collaborations')

# def collab_evaluated_ods(collab):
#     tmp=(collab.assign(stage_name=np.where(collab.stage=='evaluated', 'projets évalués', 'projets lauréats')))

#     # tmpP=(tmp.loc[(tmp.stage=='evaluated')&(~tmp.thema_code.isin(['ERC', 'MSCA'])), ['project_id', 'total_cost']].rename(columns={'total_cost':'proposal_budget'}).drop_duplicates())

#     # tmp.loc[(tmp.stage=='evaluated')&(tmp.status_code=='UNDER_PREPARATION'), 'abstract'] = np.nan
#     tmp=(tmp.loc[(tmp.stage=='evaluated')&(~tmp.thema_code.isin(['ERC', 'MSCA']))]
#             [['project_id', 'country_name_fr', 'country_name_fr_collab', 'call_year',  'abstract', 'free_keywords',
#             'participates_as', 'participates_as_collab',  'action_code', 'action_name',
#             'thema_code', 'destination_code',  'destination_name_en', 'extra_joint_organization', 
#             'extra_joint_organization_collab',
#             'part_num', 'coord_num', 'fund', 'part_num_collab', 'fund_collab', 'total_cost', 'proposal_budget',
#             'nuts_code','region_1_name',  'nuts_code_collab', 'region_1_name_collab', 
#             'country_code', 'country_code_collab', 'country_code_mapping_collab', 
#             'country_name_en', 'country_name_en_collab',  'pilier_name_en', 'programme_name_en', 'thema_name_en',
#             'ecorda_date']]

#         )       
#     print(f"size collab {len(tmp)}")
        
#     tmp = order_columns(tmp, 'proj_collab')

#     zipfile_ods(tmp, 'fr-esr-all-evaluated-projects-collaborations')