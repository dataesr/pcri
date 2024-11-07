import pandas as pd, numpy as np
from config_path import PATH_CONNECT
from functions_shared import order_columns, zipfile_ods


def synthese_preparation(participation, countries):
    print("\n### SYNTHESE preparation")
    print("\n## regroupement des participations")

    cc = countries.drop(columns=['countryCode', 'country_name_mapping','country_code_mapping']).drop_duplicates()

    part=(participation
        .drop(columns=['orderNumber', 'generalPic', 'pic', 'country_code_mapping'])
        .assign(number_involved=1)
        .groupby(['stage', 'project_id', 'role','participates_as', 'erc_role', 'cordis_is_sme', 'cordis_type_entity_acro',
        'cordis_type_entity_code', 'cordis_type_entity_name_en', 'extra_joint_organization',
        'cordis_type_entity_name_fr', 'country_code'],  dropna = False)
        .sum()
        .reset_index())

    part = part.merge(cc, how='left', on='country_code').drop(columns='countryCode_parent')

    print(f"nouvelle longueur pour les participations regroupées: {len(part)}")

    if '{:,.1f}'.format(participation['beneficiary_subv'].sum())=='{:,.1f}'.format(part['beneficiary_subv'].sum()):
        print("Etape participation/regroupement -> beneficiary_subv OK")
    else:
        print(f"ATTENTION ! Revoir le calcul de beneficiary_subv:{'{:,.1f}'.format(part['beneficiary_subv'].sum())}")

    if '{:,.1f}'.format(participation.loc[participation.stage=='successful', 'calculated_fund'].sum())=='{:,.1f}'.format(part.loc[part.stage=='successful', 'calculated_fund'].sum()):
        print("Etape participation/regroupement -> calculated_fund_successful OK")
    else:
        print(f"ATTENTION ! Revoir le calcul de calculated_fund_successful:{'{:,.1f}'.format(part.loc[part.stage=='successful', 'calculated_fund'].sum())}")
        
    if '{:,.1f}'.format(participation.loc[participation.stage=='evaluated', 'calculated_fund'].sum())=='{:,.1f}'.format(part.loc[part.stage=='evaluated', 'calculated_fund'].sum()):
        print("Etape participation/regroupement -> calculated_fund_eval OK")
    else:
        print(f"ATTENTION ! Revoir le calcul de calculated_fund_eval:{'{:,.1f}'.format(part.loc[part.stage=='evaluated', 'calculated_fund'].sum())}")
        
    if len(participation)==part['number_involved'].sum():
        print("Etape participation/regroupement -> participant_number OK")
    else:
        print(f"ATTENTION ! Revoir le calcul de calculated_participant_number:{'{:,.1f}'.format(part['number_involved'].sum())}") 
    return part


def projects_participations(projects, part):
    print("\n### PROJECTS + PARTICIPATION")
    # si besoin ne pas relancer tout le programme -> load json files : participation_current and projects_current

    projects_current=projects[['project_id', 'total_cost', 'start_date', 'end_date',  'call_deadline',
                'duration',  'call_id', 'call_year', 'stage', 'status_code', 'topic_code', 'topic_name',
                'pilier_name_en', 'pilier_name_fr', 'programme_code', 'programme_name_en', 'programme_name_fr', 
                'thema_code', 'thema_name_fr', 'thema_name_en', 
                'destination_code', 'destination_name_en', 'destination_lib',
                'destination_detail_code', 'destination_detail_name_en',
                'action_code', 'action_name', 'action_code2', 'action_name2', 'ecorda_date']]

    act_liste = ['RIA', 'MSCA', 'IA', 'CSA', 'ERC', 'EIC']
    projects_current = projects_current.assign(action_group_code=projects_current.action_code, action_group_name=projects_current.action_name)
    projects_current.loc[~projects_current.action_code.isin(act_liste), 'action_group_code'] = 'ACT-OTHERS'
    projects_current.loc[~projects_current.action_code.isin(act_liste), 'action_group_name'] = 'Others actions'

    projects_current = projects_current.merge(part, how='inner', on=['project_id', 'stage'])
    projects_current = projects_current.assign(is_ejo=np.where(projects_current.extra_joint_organization.isnull(), 'Sans', 'Avec')) 

    print(f"size: {len(projects_current)}, fund_signed: {'{:,.1f}'.format(projects_current.loc[projects_current.stage=='successful','calculated_fund'].sum())}, participant_signed: {'{:,.1f}'.format(projects_current.loc[projects_current.stage=='successful','number_involved'].sum())}")

    (pd.DataFrame(projects_current)
    .drop(columns=['topic_name','ecorda_date'])
    .to_csv(f"{PATH_CONNECT}projects_participations_current.csv", index=False, encoding="UTF-8", sep=";", na_rep=''))

    return projects_current

def synthese(projects_current):
    print("\n### SYNTHESE ODS")

    tmp=(projects_current.assign(stage_name=np.where(projects_current.stage=='evaluated', 'projets évalués', 'projets lauréats'))
        [['country_name_fr', 'stage_name', 'call_year', 'programme_name_fr', 'thema_code',  'thema_name_fr', 
        'destination_code', 'destination_name_en', 'action_code',  'action_name', 'action_code2', 'action_name2',  
        'action_group_code', 'action_group_name', 'extra_joint_organization',
        'cordis_type_entity_acro', 'cordis_type_entity_code', 'cordis_type_entity_name_en', 'cordis_type_entity_name_fr',
        'role', 'pilier_name_fr',  'calculated_fund', 'coordination_number', 'number_involved','project_id', 
        'country_group_association_name_fr','country_group_association_name_en', 
        'country_name_en', 'country_code', 'country_group_association_code', 'with_coord',
        'pilier_name_en','programme_name_en','thema_name_en','stage', 'status_code', 'ecorda_date'
        ]]
            .rename(columns={ 
            'action_code':'action_id', 
            'action_name':'action_name',
            'action_code2':'action_detail_id', 
            'action_name2':'action_detail_name',
            'calculated_fund':'fund_€',
            'country_group_association_code':'country_association_code',
            'country_group_association_name_en':'country_association_name_en',
            'country_group_association_name_fr':'country_association_name_fr',
            'with_coord':'flag_coordination'
            }))

    tmp.loc[tmp.thema_code.isin(['ERC','MSCA']), ['destination_code', 'destination_name_en']] = np.nan

    # attention si changement de nom de vars -> la modifier aussi dans pcri_info_columns_order
    tmp = order_columns(tmp, 'proj_synthese')

    print(f"{'{:,.1f}'.format(tmp.loc[tmp.stage=='successful','fund_€'].sum())}")
    zipfile_ods(tmp, 'fr-esr-all-projects-synthese')