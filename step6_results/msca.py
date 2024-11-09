import numpy as np, pandas as pd
from config_path import PATH_ODS
from functions_shared import order_columns
from step3_entities.ID_getSourceRef import *

def msca_ods(msca_erc):
    print("### MSCA ods")
    # MSCA -> ODS
    m = (msca_erc.assign(stage_name=np.where(msca_erc.stage=='evaluated', 'projets évalués', 'projets lauréats'))
        .loc[msca_erc.thema_code=='MSCA',
        ['action_code', 'action_name', 'calculated_fund', 'call_year', 'coordination_number', 'framework',
        'cordis_type_entity_acro', 'cordis_type_entity_code', 'cordis_type_entity_name_en', 
        'cordis_type_entity_name_fr',
        'country_group_association_code', 'country_group_association_name_en',
        'country_group_association_name_fr', 'country_name_en', 'with_coord',
        'country_name_fr', 'extra_joint_organization', 'is_ejo',
        'destination_code', 'destination_detail_code',
        'destination_detail_name_en', 'destination_name_en', 'number_involved',
        'panel_code', 'panel_name', 'participates_as', 'project_id', 'role', 
        'stage', 'stage_name', 'country_code', 'ecorda_date']]
        .rename(columns={
            'panel_code':'panel_id',
            'action_code':'action_id', 
            'role':'role_participant', 
            'calculated_fund':'fund_€',
            'country_group_association_code':'country_association_code',
            'country_group_association_name_en':'country_association_name_en',
            'country_group_association_name_fr':'country_association_name_fr',
            'with_coord':'flag_coordination',
            'is_ejo':'flag_organization'
            }))

    m = m.reindex(sorted(m.columns), axis=1)
    # attention si changement de nom de vars -> la modifier aussi dans pcri_info_columns_order
    m = order_columns(m, 'msca_synthese')
    m.to_csv(f"{PATH_ODS}fr-esr-msca-projects-synthese.csv", sep=';', encoding='UTF-8', index=False, na_rep='', decimal=".")

def msca_evol_ods(msca_resume):
    ## msca_resume for ODS
    print("### MSCA evol")
    tmp=(msca_resume
        .assign(status_name=np.where(msca_resume.stage=='evaluated', 'projets évalués', 'projets lauréats'))
        .loc[msca_resume.thema_code=='MSCA',
        ['framework', 'status_name','country_name_fr', 'call_year',  'action_code', 'action_name', 'with_coord',
        'destination_name_en', 'destination_detail_name_en', 'panel_name', 'role', 'participates_as',
        'extra_joint_organization', 'is_ejo',
        'funding_part', 'number_involved', 'coordination_number', 'project_id', 'country_code',
        'country_name_en', 'country_group_association_code','country_group_association_name_en',
        'country_group_association_name_fr', 'stage', 'panel_code', 'destination_code', 'destination_detail_code', 
        'ecorda_date']]
        
        .rename(columns={
            'panel_code':'panel_id',
            'action_code':'action_id', 
            'funding_part':'fund_€',
            'country_group_association_code':'country_association_code',
            'country_group_association_name_en':'country_association_name_en',
            'country_group_association_name_fr':'country_association_name_fr',
            'with_coord':'flag_coordination',
            'is_ejo':'flag_organization'
            }))

    tmp.loc[(tmp.framework=='FP7')&(tmp.stage=='evaluated'), 'fund_€']= 0
    tmp.loc[(tmp.country_code=='ALL'), 'country_name_en'] = 'All countries'
    tmp.loc[(tmp.country_code=='ALL'), 'country_name_fr'] = 'Tous pays'


    for i in ['coordination_number', 'number_involved', 'fund_€']:
        tmp.loc[(tmp.country_code=='ALL'), f'{i}_all'] = tmp[i]
        tmp.loc[(tmp.country_code=='ALL'), i] = np.nan

    tmp = tmp.reindex(sorted(tmp.columns), axis=1)
    # attention si changement de nom de vars -> la modifier aussi dans pcri_info_columns_order
    tmp = order_columns(tmp, 'msca_evol')
    tmp.sort_values(['fund_€_all'], ascending=False).to_csv(f"{PATH_ODS}fr-esr-msca-evolution-pcri.csv", sep=';', encoding='UTF-8', index=False, na_rep='', decimal=".")


def msca_entities(me_entities):
    print("### MSCA entities ODS")

    # l=list(set(me_entities.entities_id.unique()))   
    # l=sourcer_ID(l)  

    tmp = (me_entities.loc[(me_entities.stage=='successful')&(me_entities.thema_code=='MSCA'),

            ['framework','country_name_fr', 'call_year','destination_name_en', 'destination_detail_name_en', 'panel_name', 
            'role', 'participates_as', 'extra_joint_organization', 'is_ejo', 'with_coord',
            'cordis_type_entity_acro', 'cordis_type_entity_code',
            'cordis_type_entity_name_en', 'cordis_type_entity_name_fr', 
            'operateur_name', 'paysage_category', 'paysage_category_id', 'category_woven', 'insee_cat_code', 'insee_cat_name',
            'action_code', 'action_name', 'source_id',
            'entities_name', 'entities_acronym', 'calculated_fund', 'coordination_number', 'number_involved',
            'project_id', 'participation_nuts', 'region_1_name', 'region_2_name', 'regional_unit_name',
            'country_group_association_name_fr', 'country_name_mapping','country_name_en',
            'country_group_association_code', 'country_group_association_name_en', 'country_code_mapping', 'panel_code',
            'destination_code', 'destination_detail_code', 'entities_id', 'status_code', 'ecorda_date',
            'free_keywords', 'abstract', 'acronym', 'operateur_num','operateur_lib', 'paysage_category_priority'
            ]]
        .rename(columns={ 
            'source_id':'entities_id_source',
            'panel_code':'panel_id',
            'action_code':'action_id', 
            'calculated_fund':'fund_€', 
            'country_group_association_code':'country_association_code',
            'country_group_association_name_en':'country_association_name_en',
            'country_group_association_name_fr':'country_association_name_fr',
            'with_coord':'flag_coordination',
            'is_ejo':'flag_organization'
            }))

    tmp = tmp.loc[tmp.status_code!='REJECTED']
    tmp.loc[tmp.status_code=='UNDER_PREPARATION', 'abstract'] = np.nan

    tmp.loc[tmp.entities_id_source=='ror', 'entities_id'] = tmp.loc[tmp.entities_id_source=='ror', 'entities_id'].str.replace("^R", "", regex=True)
    tmp.loc[tmp.entities_id_source=='pic', 'entities_id_source'] = 'ecorda pic'
    tmp.loc[tmp.entities_id_source=='identifiantAssociationUniteLegale', 'entities_id_source'] = 'rna'
    tmp.loc[(tmp.entities_id_source.isnull())&(tmp.entities_id.str.contains('gent', na=False)), 'entities_id_source'] = 'paysage'

    tmp = order_columns(tmp, 'msca_entities')  

    tmp.to_csv(f"{PATH_ODS}fr-esr-msca-projects-entities.csv", sep=';', encoding='UTF-8', index=False, na_rep='', decimal=".")