import numpy as np, pandas as pd
from config_path import PATH_ODS
from functions_shared import order_columns
from step3_entities.ID_getSourceRef import *

def erc_ods(msca_erc):
    # ERC -> ODS
    print("### ERC ods")
        
    e = (msca_erc.assign(stage_name=np.where(msca_erc.stage=='evaluated', 'projets évalués', 'projets lauréats'))
        .loc[msca_erc.thema_code=='ERC',   
        ['action_code', 'action_name','calculated_fund', 'call_year', 'extra_joint_organization', 'is_ejo',
        'cordis_type_entity_acro', 'cordis_type_entity_code','cordis_type_entity_name_en', 'cordis_type_entity_name_fr',
        'country_group_association_code', 'country_group_association_name_en', 'with_coord',
        'country_group_association_name_fr', 'country_name_en',
        'country_name_fr', 'destination_code', 'destination_name_en', 'erc_role','framework', 'number_involved',
        'panel_code', 'panel_name', 'participates_as', 'project_id', 'role', 'ecorda_date',
        'stage', 'stage_name', 'country_code']]
        .rename(columns={
        'panel_code':'panel_id',
        'action_code':'action_id', 
        'role':'role_institution', 
        'erc_role':'role_chercheur',
        'calculated_fund':'fund_€',
        'country_group_association_code':'country_association_code',
        'country_group_association_name_en':'country_association_name_en',
        'country_group_association_name_fr':'country_association_name_fr',
        'with_coord':'flag_coordination',
        'is_ejo':'flag_organization'
        }))

    e = e.reindex(sorted(e.columns), axis=1)
    # attention si changement de nom de vars -> la modifier aussi dans pcri_info_columns_order
    e = order_columns(e, 'erc_synthese')

    e.to_csv(f"{PATH_ODS}fr-esr-erc-projects-synthese.csv", sep=';', encoding='UTF-8', index=False, na_rep='', decimal=".")

def erc_evol_ods(msca_resume):
    ## ERC_resume for ODS
    print("### ERC evol")
    tmp=(msca_resume
        .assign(status_name=np.where(msca_resume.stage=='evaluated', 'projets évalués', 'projets lauréats'),
                coordination_number=np.where(msca_resume.erc_role=='PI', 1, 0))
        .loc[msca_resume.thema_code=='ERC',
        ['framework', 'status_name','country_name_fr', 'call_year',  'action_code', 'action_name',
        'destination_name_en', 'panel_name', 'erc_role', 'participates_as', 'role', 
        'extra_joint_organization', 'is_ejo', 'with_coord',
        'funding_part', 'number_involved', 'coordination_number', 'project_id', 'country_code',
        'country_name_en', 'country_group_association_code','country_group_association_name_en',
        'country_group_association_name_fr', 'stage', 'panel_code', 'destination_code', 'ecorda_date']]
        
        .rename(columns={
            'panel_code':'panel_id',
            'action_code':'action_id', 
            'funding_part':'fund_€',
            'erc_role':'role_chercheur',
            'role':'role_institution',
            'country_group_association_code':'country_association_code',
            'country_group_association_name_en':'country_association_name_en',
            'country_group_association_name_fr':'country_association_name_fr',
            'with_coord':'flag_coordination',
            'is_ejo':'flag_organization'           
            }))

    tmp.loc[(tmp.country_code=='ALL'), 'country_name_en'] = 'All countries'
    tmp.loc[(tmp.country_code=='ALL'), 'country_name_fr'] = 'Tous pays'


    for i in ['coordination_number', 'number_involved', 'fund_€']:
        tmp.loc[(tmp.country_code=='ALL'), f'{i}_all'] = tmp[i]
        tmp.loc[(tmp.country_code=='ALL'), i] = np.nan

    tmp = tmp.reindex(sorted(tmp.columns), axis=1)
    # attention si changement de nom de vars -> la modifier aussi dans pcri_info_columns_order
    tmp = order_columns(tmp, 'erc_evol')    
        
    tmp.sort_values(['fund_€_all'], ascending=False).to_csv(f"{PATH_ODS}fr-esr-erc-evolution-pcri.csv", sep=';', encoding='UTF-8', index=False, na_rep='', decimal=".")

def erc_entities(me_entities):
    print("\n### ERC entities")
    ## ERC entities for ODS

    # l=list(set(me_entities.entities_id.unique()))   
    # l=sourcer_ID(l)  

    tmp = (me_entities.loc[(me_entities.stage=='successful')&(me_entities.thema_code=='ERC'),

            ['framework','country_name_fr', 'call_year','destination_name_en', 'panel_name', 
            'participates_as', 'erc_role', 'cordis_type_entity_acro', 'cordis_type_entity_code', 'action_code', 'action_name',
            'cordis_type_entity_name_en', 'cordis_type_entity_name_fr',  'role', 'extra_joint_organization', 'is_ejo',
            'operateur_name', 'paysage_category','paysage_category_id', 'category_woven', 'insee_cat_code', 'insee_cat_name',
            'entities_name', 'entities_acronym', 'calculated_fund', 'coordination_number', 'number_involved', 'with_coord',
            'project_id', 'participation_nuts', 'region_1_name', 'region_2_name', 'regional_unit_name',
            'country_group_association_name_fr', 'country_name_mapping','country_name_en',
            'country_group_association_code', 'country_group_association_name_en', 'country_code_mapping', 'panel_code',
            'destination_code', 'entities_id', 'status_code', 'ecorda_date',  'free_keywords', 'abstract', 'acronym',
            'operateur_num','operateur_lib', 'paysage_category_priority', 'source_id'
            ]]
        .rename(columns={ 
            'source_id':'entities_id_source',
            'panel_code':'panel_id',
            'action_code':'action_id', 
            'erc_role':'role_chercheur',
            'role':'role_institution',
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

    tmp = order_columns(tmp, 'erc_entities')  
    tmp.to_csv(f"{PATH_ODS}fr-esr-erc-projects-entities.csv", sep=';', encoding='UTF-8', index=False, na_rep='', decimal=".")