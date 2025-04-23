import pandas as pd, numpy as np
from functions_shared import order_columns, zipfile_ods, entreprise_cat_cleaning
from step3_entities.ID_getSourceRef import get_source_ID
from config_path import PATH_CONNECT

def entities_preparation(entities_part, h20):
    tmp= h20.rename(columns={ 'subv':'beneficiary_subv'})[
            ['action_code','action_name', 'action_code2', 'action_name2', 'article1', 'article2', 
            'call_deadline','abstract', 'source_id', 'generalPic', 'fund_ent_erc',
            'calculated_fund', 'call_id', 'call_year', 'category_woven',
            'coordination_number', 'cordis_is_sme', 'cordis_type_entity_acro', 
            'extra_joint_organization', 'with_coord', 'is_ejo', 'entreprise_flag',
            'cordis_type_entity_code', 'cordis_type_entity_name_en','cordis_type_entity_name_fr', 
            'participation_nuts', 'region_1_name', 'region_2_name', 'regional_unit_name',
            'country_association_code','country_association_name_en', 'country_code', 
            'country_code_mapping', 'country_group_association_code', 'country_group_association_name_en',
            'country_group_association_name_fr', 'country_name_en',
            'country_name_fr', 'country_name_mapping', 'destination_code', 'destination_name_en',
            'destination_detail_code', 'destination_detail_name_en', 
            'entities_acronym', 'entities_id', 'entities_name', 'framework', 
            'insee_cat_code', 'insee_cat_name', 'number_involved', 'participates_as', 
            'paysage_category', 'paysage_category_id', 'category_agregation',
            'pilier_name_en', 'pilier_name_fr', 'programme_code', 
            'panel_code', 'panel_name', 'panel_regroupement_code', 'panel_regroupement_name',
            'programme_name_en', 'project_id', 'role', 'erc_role', 'ror_category', 
            'stage', 'status_code', 'thema_code', 'thema_name_en', 'topic_code', 'free_keywords', 
            'operateur_name','operateur_num','operateur_lib', 
            'groupe_id', 'groupe_name', 'groupe_acronym', 'groupe_sector',
            'ecorda_date']]

    tmp = (tmp
        .groupby(list(tmp.columns.difference(['coordination_number', 'number_involved', 'calculated_fund', 'fund_ent_erc'])), dropna=False, as_index=False).sum()
        .drop_duplicates()
        )

    print(f"1 - tmp={'{:,.1f}'.format(tmp.loc[tmp.stage=='evaluated', 'calculated_fund'].sum())},h20={'{:,.1f}'.format(h20.loc[h20.stage=='evaluated', 'calculated_fund'].sum())}")
    print(f"2 - tmp={'{:,.1f}'.format(tmp.loc[tmp.stage=='successful', 'calculated_fund'].sum())},h20={'{:,.1f}'.format(h20.loc[h20.stage=='successful', 'calculated_fund'].sum())}")
    # print(f"3 - comparaison nb couple genpic + country (doit être égal) {len(part[['generalPic','country_code']].drop_duplicates())},{len(entities_info[['generalPic','country_code']].drop_duplicates())}")

    entities_part = pd.concat([entities_part, tmp], ignore_index=True)
    
    entities_part = entities_part.reindex(sorted(entities_part.columns), axis=1)
    return entities_part

def entities_ods(entities_participation):
    # ### entities pour ODS
    import math

    tmp=(entities_participation[
        ['category_woven', 'cordis_is_sme', 'cordis_type_entity_acro', 'stage','acronym',
        'cordis_type_entity_code', 'cordis_type_entity_name_en', 
        'cordis_type_entity_name_fr', 'extra_joint_organization', 'is_ejo',
        'country_code', 'country_code_mapping', 'participation_linked',
        'country_group_association_code', 'country_group_association_name_en',
        'country_group_association_name_fr', 'country_name_en',
        'country_name_fr', 'country_name_mapping', 
        'groupe_id', 'groupe_name', 'groupe_acronym',
        'participation_nuts', 'region_1_name', 'region_2_name', 'regional_unit_name',
        'entities_acronym', 'entities_id', 'entities_name', 'operateur_name',
        'insee_cat_code', 'insee_cat_name', 'participates_as', 'project_id',
        'role', 'ror_category', 'sector', 'paysage_category', 'fund_ent_erc',
        'coordination_number', 'calculated_fund', 'with_coord','abstract', 
        'number_involved', 'action_code', 'action_name', 'call_id', 'topic_code',
        'status_code', 'framework', 'call_year', 'ecorda_date', 'entreprise_flag',
        'pilier_name_en', 'pilier_name_fr', 'programme_code', 'programme_name_en', 
        'programme_name_fr', 'thema_code', 'thema_name_fr', 'thema_name_en', 'destination_code',
        'destination_lib', 'destination_name_en','action_code2', 'action_name2', 'free_keywords', 
        'operateur_num','operateur_lib', 'category_agregation', 'source_id', 'panel_regroupement_code', 'panel_code', 'erc_role']]
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
            'is_ejo':'flag_organization'
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

    # tmp.loc[tmp.thema_code.isin(['ERC','MSCA']), ['destination_code', 'destination_name_en']] = np.nan

    for i in ['abstract', 'free_keywords']:
        tmp[i] = tmp[i].str.replace('\\n|\\t|\\r|\\s+|^\\"', ' ', regex=True).str.strip()

    tmp['free_keywords'] = tmp['free_keywords'].str.lower()

    tmp.loc[(tmp.stage=='successful')&(tmp.status_code=='UNDER_PREPARATION'), 'abstract'] = np.nan

    tmp = order_columns(tmp, 'proj_entities')
    # ATTENTION si changement de nom de vars -> la modifier aussi dans pcri_info_columns_order
    
    for h in tmp.framework.unique():
        x = (tmp[(tmp.stage=='successful')&(tmp.framework==h)]
             .drop(columns=['panel_regroupement_code', 'panel_code', 'erc_role', 'fund_ent_erc']))
        x.loc[x.thema_code.isin(['ERC','MSCA']), ['destination_code', 'destination_name_en']] = np.nan
        x = entreprise_cat_cleaning(x)
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
 
    tmp1 = (tmp.loc[(tmp.stage=='evaluated')]
        .rename(columns={ 'number_involved':'numberofapplicants'})
        .drop(columns=
            ['country_name_mapping', 'country_association_name_en', 'country_name_en', 
            'country_code_mapping', 'pilier_name_fr', 'programme_code', 
            'operateur_num','operateur_lib', 'ror_category', 'paysage_category', 'country_association_name_en',
            'country_association_name_fr', 'thema_name_fr', 'destination_lib',
            'programme_name_fr', 'action_group_code', 'action_group_name', 
            'cordis_type_entity_name_en', 'cordis_type_entity_acro','cordis_type_entity_name_fr']))

    for h in tmp1.framework.unique():
        if h=='Horizon Europe':
            he='horizon'
            x = tmp1[(tmp1.framework==h)]
        else:
            he='h2020'
            x = tmp1[(tmp1.framework==h)&(tmp1.country_code=='FRA')]
        chunk_size = int(math.ceil((x.shape[0] / 2)))
        i=0
        for start in range(0, x.shape[0], chunk_size):
            df_subset = x.iloc[start:start+chunk_size]
            i=i+1
            zipfile_ods(df_subset, f"fr-esr-{he}-projects-entities-evaluated{i}")

def entities_collab(entities_participation):
# ## collab ENTITIES ##
    copy_signed = (entities_participation
        .loc[(entities_participation.stage=='successful'),
        ['project_id', 'ror_category','entities_id','entities_name','entities_acronym',
         'country_code','country_name_fr', 'calculated_fund']]
                    .add_suffix('_collab')
                    .rename(columns={'project_id_collab':'project_id'}))

    ent_signed = (entities_participation
        .loc[(entities_participation.country_code=='FRA')&(entities_participation.stage=='successful'),
        ['stage', 'framework', 'call_year','project_id', 'action_code', 'programme_name_fr', 
         'thema_code', 'thema_name_fr', 'destination_code', 'category_agregation','entities_id',
         'entities_name','entities_acronym', 'calculated_fund']])

    print(len(entities_participation.loc[(entities_participation.country_code=='FRA')&(entities_participation.stage=='successful')&(entities_participation.destination_code=='Destination 5 - SPACE'), ['project_id', 'entities_id']]))

    collab_ent = ent_signed.merge(copy_signed, on=['project_id'])
    collab_ent = collab_ent[~(collab_ent['entities_id']==collab_ent['entities_id_collab'])].drop_duplicates()

    (collab_ent
    .loc[(collab_ent.framework=='Horizon Europe')&(collab_ent.country_code_collab!='FRA')]
    .groupby(['programme_name_fr', 'thema_code', 'thema_name_fr', 'destination_code', 
              'action_code', 'entities_id_collab', 'entities_name_collab', 
              'entities_acronym_collab','country_name_fr_collab','country_code_collab' ])
    .agg({'project_id':'count', 'calculated_fund':'sum','calculated_fund_collab':'sum'})
    .reset_index()
    .sort_values('project_id', ascending=False)
    .to_csv(f"{PATH_CONNECT}entities_collaboration_current.csv", sep=";", index=False, encoding='UTF-8', na_rep=''))
   
    return collab_ent