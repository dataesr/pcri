from functions_shared import zipfile_ods, cols_order
from config_path import PATH_CONNECT, PATH_CLEAN
import numpy as np, pandas as pd

def collab_signed_ods(collab):
    tmp=(collab
         .assign(stage_name=np.where(collab.stage=='evaluated', 'projets évalués', 'projets lauréats')))

    tmpP=(tmp
          .loc[(tmp.stage=='evaluated')&(~tmp.thema_code.isin(['ERC', 'MSCA'])), ['project_id', 'total_cost']]
          .rename(columns={'total_cost':'proposal_budget'})
          .drop_duplicates())

    tmp.loc[(tmp.stage=='successful')&(tmp.status_code=='UNDER_PREPARATION'), 'abstract'] = np.nan
    tmp=(tmp.loc[(tmp.stage=='successful')&(~tmp.thema_code.isin(['ERC', 'MSCA']))&(tmp.status_code!='REJECTED')]
            .merge(tmpP, how='left', on='project_id')
            [['project_id', 'country_name_fr', 'country_name_fr_collab', 'call_year',  
            'abstract', 'free_keywords', 'participates_as', 'participates_as_collab', 
            'action_code', 'action_name', 'thema_code', 'destination_code',  'destination_name_en', 
            'extra_joint_organization', 'extra_joint_organization_collab',
            'part_num', 'coord_num', 'fund', 'part_num_collab', 'fund_collab', 'total_cost', 
            'proposal_budget', 'participation_nuts','region_1_name', 'participation_nuts_collab', 
            'region_1_name_collab', 'country_code', 'country_code_collab', 'country_code_mapping_collab', 
            'country_name_en', 'country_name_en_collab',  'pilier_name_en', 'programme_name_en', 
            'thema_name_en', 'ecorda_date', 'with_coord']]
       .rename(columns={'with_coord':'flag_coordination'}) )    

    for i in ['abstract', 'free_keywords']:
        tmp[i] = tmp[i].str.replace('\\n|\\t|\\r|\\s+', ' ', regex=True).str.strip()

    tmp['free_keywords'] = tmp['free_keywords'].str.lower()

    print(f"size collab {len(tmp)}")
        
    tmp = cols_order(tmp, 'proj_collab')

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
        
#     tmp = cols_order(tmp, 'proj_collab')

#     zipfile_ods(tmp, 'fr-esr-all-evaluated-projects-collaborations')

def msca_collab_ods(collab):
    print("### COLLAB MSCA")
    tmp=(collab.assign(destination_code=np.where(~collab.destination_lib.isnull(), collab.destination_lib+" - "+collab.destination_code, collab.destination_code), stage_name=np.where(collab.stage=='evaluated', 'projets évalués', 'projets lauréats')))

    tmp=(tmp.loc[(collab.programme_code.isin(['HORIZON.1.2'])),
            ['project_id', 'country_name_fr', 'country_name_fr_collab', 'call_year', 'stage_name', 'with_coord',
            'participates_as', 'participates_as_collab', 'extra_joint_organization','extra_joint_organization_collab',
            'destination_code', 'destination_name_en', 'free_keywords', 'abstract',
            'part_num', 'coord_num', 'part_num_collab', 'total_cost', 
            'participation_nuts', 'region_1_name', 'participation_nuts_collab', 'region_1_name_collab', 
            'country_code', 'country_code_collab', 'country_code_mapping_collab', 
            'country_name_en', 'country_name_en_collab', 'country_group_association_code', 
            'country_group_association_code_collab', 'stage', 'status_code', 'ecorda_date']]
            .rename(columns={'with_coord':'flag_coordination'})
        )      

    tmp = cols_order(tmp, 'msca_collab')
    zipfile_ods(tmp, 'fr-esr-msca-projects-collaboration')


def msca_collab(collab):
    print("### MSCA tab")

    msca_collab = (collab.loc[collab.programme_code.isin(['HORIZON.1.2']),
        ['project_id', 'country_code', 'country_name_fr',
        'country_code_collab', 'participates_as', 'extra_joint_organization',
        'participates_as_collab', 'part_num', 'coord_num', 'with_coord', 'extra_joint_organization_collab',
        'part_num_collab', 'fund', 'fund_collab', 'stage', 'status_code',
        'country_code_mapping_collab', 'country_name_en', 'country_group_association_code',
        'participation_nuts', 'region_1_name', 'participation_nuts_collab', 'region_1_name_collab', 
        'country_name_en_collab', 'country_group_association_code_collab',
        'country_name_fr_collab', 'call_id', 'call_year', 'topic_code', 'destination_code', 
        'destination_name_en', 'destination_detail_code','destination_detail_name_en', 'total_cost']])

    pd.DataFrame(msca_collab).to_csv(f"{PATH_CONNECT}msca_collaboration_current.csv", index=False, encoding="UTF-8", sep=";", na_rep='')


# def collab_evolution(collab):
#     col_select=['framework', 'stage','call_year', 'project_id',  
#     'extra_joint_organization', 'country_code_collab', 'country_name_fr_collab', 
#     'participates_as', 'participates_as_collab', 'extra_joint_organization_collab',
#     'action_code', 'action_name',
#     'part_num', 'coord_num', 'part_num_collab', 'fund', 'fund_collab' ]


#     collab_fr = (collab.assign(framework='Horizon Europe').loc[(collab.with_coord==True)&(collab.country_code=='FRA'), col_select])
#     collab_fr =  (collab_fr.groupby(list(collab_fr.columns.difference(['part_num', 'coord_num', 'part_num_collab', 'fund', 'fund_collab'])), dropna=False, as_index=False)
#                 .sum()
#                 )

#     h20_collab = pd.read_pickle(f"{PATH_CLEAN}H2020_collab.pkl")

#     h20_collab = h20_collab.loc[h20_collab.with_coord==True, col_select]
#     h20_collab = (h20_collab.groupby(list(h20_collab.columns.difference(['part_num', 'coord_num', 'part_num_collab', 'fund', 'fund_collab'])), dropna=False, as_index=False)
#                 .sum()
#                 )
    
#     pd.concat([collab_fr, h20_collab], ignore_index=True).to_csv(f"{PATH_CONNECT}collab_evol.csv", index=False, encoding="UTF-8", sep=";", na_rep='')