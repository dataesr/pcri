import pandas as pd, numpy as np
from config_path import PATH_CONNECT
from functions_shared import entreprise_cat_cleaning


def msca_erc_projects(FP6, FP7, h20, projects, part):
    print("### MSCA / ERC")
    select_cols = ['action_code', 'action_name','calculated_fund', 'fund_ent_erc', 'call_year', 'coordination_number',
        'cordis_type_entity_acro', 'cordis_type_entity_code', 'country_code', 'with_coord',
        'cordis_type_entity_name_en', 'cordis_type_entity_name_fr', 'extra_joint_organization',
        'country_group_association_code', 'country_group_association_name_en',
        'country_group_association_name_fr', 'country_name_en',  'free_keywords', 'abstract',
        'country_name_fr', 'destination_code', 'destination_detail_code', 
        'destination_detail_name_en', 'destination_name_en',  'number_involved',
        'panel_code', 'panel_name', 'project_id', 'role', 'erc_role', 'flag_entreprise',
        'stage', 'status_code', 'framework', 'thema_code', 'category_woven', 
        'groupe_id', 'groupe_name', 'participation_linked',
        'category_agregation', 'source_id','ecorda_date']

    me7= (FP7.loc[FP7.thema_code.isin(["ERC","MSCA"]), list(set(select_cols)
            .difference(['participation_linked', 'fund_ent_erc']))])
    me6= (FP6
            .loc[FP6.thema_code.isin(["ERC","MSCA"]), list(set(select_cols)
            .difference(['panel_code', 'panel_name','erc_role', 'fund_ent_erc',
                         'extra_joint_organization', 'free_keywords', 
                         'abstract','source_id', 'category_agregation', 
                         'category_woven', 'flag_entreprise',
                         'groupe_id', 'groupe_name', 'participation_linked']))])
    me20=(h20.loc[h20.programme_code.isin(['ERC', 'MSCA']), list(set(select_cols)
            .difference(['participation_linked']))])


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
    print(f"size projects_MSCA_ERC: {len(msca_erc)}")

    msca_erc.reset_index(drop=True, inplace=True)
    me20.reset_index(drop=True, inplace=True)
    me7.reset_index(drop=True, inplace=True)
    me6.reset_index(drop=True, inplace=True)
    msca_erc = pd.concat([msca_erc, me20, me7, me6], ignore_index=True, axis=0, join='outer')
    msca_erc = msca_erc.assign(is_ejo=np.where(msca_erc.extra_joint_organization.isnull(), 'Sans', 'Avec'))

    print(f"size projects_MSCA_ERC with others fp: {len(msca_erc)}")
    # msca_erc = entreprise_cat_cleaning(msca_erc)
    (msca_erc.drop(columns=['ecorda_date', 'free_keywords', 'abstract', 'acronym'])
     .to_csv(PATH_CONNECT+"msca_projects_part.csv", index=False, encoding="UTF-8", sep=";", na_rep='', decimal="."))

    return msca_erc


def msca_erc_resume(msca_erc):
    print("### MSCA / ERC evol preparation")
    me_resume = (msca_erc
            .groupby(list(msca_erc.columns.difference(['coordination_number', 'number_involved', 'calculated_fund', 'beneficiary_subv', 'fund_ent_erc'])), dropna=False)
            .agg({'number_involved':'sum', 'calculated_fund':'sum', 'coordination_number':'sum'})
            .reset_index()
            .rename(columns={'calculated_fund':'funding_part'})
                )

    # msca_resume.columns
    tot = me_resume.loc[:,~me_resume.columns.str.contains('country|article', regex=True)] 
    tot = (tot.groupby(list(tot.columns.difference(['coordination_number', 'number_involved', 'funding_part'])), dropna=False)
    #       .groupby(['framework', 'stage', 'call_year', 'project_id', 'role','participates_as', 'thema_code', 'action_code', 'action_name',
    #        'panel_code', 'panel_name', 'destination_code', 'destination_detail_code', 'destination_name_en', 'destination_detail_name_en'], dropna=False)
        .agg({'funding_part':'sum', 'number_involved':'sum', 'coordination_number':'sum'})
        .reset_index()
        .assign(country_code='ALL'))

    me_resume = pd.concat([me_resume, tot], ignore_index=True, axis=0, join='outer')

    me_resume.drop(columns=['ecorda_date','abstract','free_keywords', 'acronym']).to_csv(PATH_CONNECT+"msca_resume.csv", index=False, encoding="UTF-8", sep=";", na_rep='', decimal=".")
    return me_resume

def msca_erc_ent(entities_participation):
    print("### MSCA /ERC entities")
    me_entities = (entities_participation
                    .drop(columns=['action_code2',
                                    'action_name2',
                                    'destination_lib',
                                    'pilier_code',
                                    'pilier_name_en',
                                    'pilier_name_fr',
                                    'programme_code',
                                    'programme_name_en',
                                    'programme_name_fr',
                                    'thema_name_en',
                                    'thema_name_fr',
                                    'topic_name'])
                    .loc[entities_participation.thema_code.isin(['MSCA','ERC'])])

    me_entities = entreprise_cat_cleaning(me_entities)

    print(f"size msca_entities: {len(me_entities)}")
    me_entities.drop(columns=['ecorda_date', 'free_keywords', 'abstract']).to_csv(PATH_CONNECT+"msca_entities.csv", index=False, encoding="UTF-8", sep=";", na_rep='', decimal=".")
    return me_entities