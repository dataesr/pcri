import pandas as pd, numpy as np, json
from config_path import PATH_CONNECT
from functions_shared import entreprise_group_cleaning, FP_suivi


def msca_erc_projects(FP6, FP7, h20, projects, part):
    print("### MSCA / ERC")
    select_cols = ['calculated_fund', 'fund_ent_erc', 'call_year', 'coordination_number',
        'cordis_type_entity_acro', 'cordis_type_entity_code', 'country_code', 'with_coord',
        'cordis_type_entity_name_en', 'cordis_type_entity_name_fr', 'extra_joint_organization',
        'country_group_association_code', 'country_group_association_name_en',
        'country_group_association_name_fr', 'country_name_en',  'free_keywords', 'abstract',
        'country_name_fr',  'number_involved',  'destination_code', 'destination_detail_code',
        'destination_name_en', 'destination_detail_name_en',
        'panel_code', 'panel_name', 'panel_regroupement_code', 'panel_regroupement_name', 
        'project_id', 'role', 'erc_role', 'entreprise_flag',  'category_woven', 
        'stage', 'status_code', 'framework', 'action_code', 'thema_code',
        'groupe_id', 'groupe_name', 'participation_linked',
        'category_agregation', 'source_id','ecorda_date'
        ]

    me7 = FP_suivi(FP7)
    me7 = (me7.loc[me7.action_code.isin(["ERC","MSCA"]), list(set(select_cols)
            .difference(['participation_linked', 'fund_ent_erc', 'panel_regroupement_code', 'panel_regroupement_name', 'groupe_name', 'groupe_id']))])
    
    me6 = FP_suivi(FP6)
    me6 = (me6
            .loc[me6.action_code.isin(["MSCA"]), list(set(select_cols)
            .difference(['panel_code', 'panel_name',  'panel_regroupement_code', 'panel_regroupement_name',
                         'erc_role', 'fund_ent_erc',
                         'extra_joint_organization', 'free_keywords', 
                         'abstract','source_id', 'category_agregation', 
                         'category_woven', 'entreprise_flag',
                         'groupe_id', 'groupe_name', 'participation_linked']))])
    
    me20 = FP_suivi(h20)
    me20 = (me20.loc[me20.action_code.isin(['ERC', 'MSCA']), list(set(select_cols)
            .difference(['participation_linked']))])
    

    projects_m = projects.loc[(projects.programme_code.isin(['HORIZON.1.2', 'HORIZON.1.1']))&(projects.action_code.isin(['ERC','MSCA'])|(projects.destination_code=='CITIZENS'))]
    projects_m.loc[(projects.thema_code=='MSCA')&(projects.destination_code=='CITIZENS'), 'action_code'] = 'MSCA'
    projects_m = (projects_m.assign(framework='Horizon Europe')[
                    ['project_id','topic_code', 'call_id', 'call_year', 'action_code',
                    'framework', 'panel_code', 'panel_name', 'panel_regroupement_code', 'panel_regroupement_name', 
                    'stage', 'status_code', 'free_keywords', 'abstract', 'acronym',
                    'destination_code', 'destination_name_en', 'destination_detail_code', 
                    'destination_detail_name_en', 'ecorda_date']].drop_duplicates()
                    )

    for i in ['abstract', 'free_keywords']:
        projects_m[i] = projects_m[i].str.replace('\\n|\\t|\\r|\\s+', ' ', regex=True).str.strip()

    projects_m['free_keywords'] = projects_m['free_keywords'].str.lower()

    # MSCA/ERC PROJECTS HE
    print("## MSCA_ERC data")
    msca_erc = projects_m.merge(part, how='inner', on=['project_id', 'stage']).sort_values(['project_id','stage','country_code'])
    print(f"size projects_MSCA_ERC: {len(msca_erc)}")

    msca_erc.reset_index(drop=True, inplace=True)
    me20.reset_index(drop=True, inplace=True)
    me7.reset_index(drop=True, inplace=True)
    me6.reset_index(drop=True, inplace=True)
    msca_erc = pd.concat([msca_erc, me20, me7, me6], ignore_index=True, axis=0, join='outer')
    msca_erc = msca_erc.assign(is_ejo=np.where(msca_erc.extra_joint_organization.isnull(), 'Sans', 'Avec'))

    print(f"size projects_MSCA_ERC with others fp: {len(msca_erc)}")
    # msca_erc = entreprise_group_cleaning(msca_erc)
    (msca_erc.loc[~((msca_erc.framework=='FP7')&(msca_erc.thema_code=='ERC'))].drop(columns=['ecorda_date', 'free_keywords', 'abstract', 'acronym'])
     .to_csv(PATH_CONNECT+"msca_projects_part.csv", index=False, encoding="UTF-8", sep=";", na_rep='', decimal="."))

    return msca_erc


def msca_erc_resume(msca_erc):
    print("### MSCA / ERC evol preparation")
    me_resume = (msca_erc
            .groupby(list(msca_erc.columns.difference(['coordination_number', 'number_involved', 'calculated_fund', 'beneficiary_fund', 'fund_ent_erc'])), dropna=False)
            .agg({'number_involved':'sum', 'calculated_fund':'sum', 'coordination_number':'sum','fund_ent_erc':'sum'})
            .reset_index()
            .rename(columns={'calculated_fund':'funding_part', 'fund_ent_erc':'funding_entity'})
                )

    # msca_resume.columns
    tot = me_resume.loc[:,~me_resume.columns.str.contains('country|article', regex=True)] 
    tot = (tot.groupby(list(tot.columns.difference(['coordination_number', 'number_involved', 'funding_part','funding_entity'])), dropna=False)
    #       .groupby(['framework', 'stage', 'call_year', 'project_id', 'role','participates_as', 'thema_code', 'action_code', 'action_name',
    #        'panel_code', 'panel_name', 'destination_code', 'destination_detail_code', 'destination_name_en', 'destination_detail_name_en'], dropna=False)
        .agg({'funding_part':'sum', 'number_involved':'sum', 'coordination_number':'sum', 'funding_entity':'sum'})
        .reset_index()
        .assign(country_code='ALL'))

    me_resume = pd.concat([me_resume, tot], ignore_index=True, axis=0, join='outer')

    me_resume.drop(columns=['ecorda_date','abstract','free_keywords', 'acronym']).to_csv(PATH_CONNECT+"msca_resume.csv", index=False, encoding="UTF-8", sep=";", na_rep='', decimal=".")
    return me_resume

def msca_erc_ent(entities_participation):
    print("### MSCA /ERC entities")
    me_entities = entities_participation.loc[entities_participation.action_code.isin(['MSCA','ERC'])]

    # me_entities = entreprise_group_cleaning(me_entities)

    print(f"size msca_entities: {len(me_entities)}")
    me_entities.drop(columns=['ecorda_date', 'free_keywords', 'abstract']).to_csv(PATH_CONNECT+"msca_entities.csv", index=False, encoding="UTF-8", sep=";", na_rep='', decimal=".")
    return me_entities