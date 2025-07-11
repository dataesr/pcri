import pandas as pd, numpy as np
from functions_shared import zipfile_ods, entreprise_group_cleaning, select_cols_FP, rename_cols_FP, df_order_cols_FP, FP_suivi
from step3_entities.ID_getSourceRef import get_source_ID
from config_path import PATH_CONNECT


def entities_preparation(entities_part, h20):

    h20 = FP_suivi(h20)
    h20=(h20.rename(columns={ 'subv':'beneficiary_fund'})
    .drop(columns=['action_code3', 'action_name3', 'paysage_category_priority', 'programme_next_fp']))

    h20 = (h20
        .groupby(list(h20.columns.difference(['beneficiary_fund','coordination_number', 'number_involved', 'calculated_fund', 'fund_ent_erc'])), dropna=False, as_index=False).sum()
        .drop_duplicates()
        )
    
    entities_part = pd.concat([entities_part, h20], ignore_index=True)
    entities_part = entities_part.reindex(sorted(entities_part.columns), axis=1)

    print(f"1 - entities_part={'{:,.1f}'.format(entities_part.loc[entities_part.stage=='evaluated', 'calculated_fund'].sum())},h20={'{:,.1f}'.format(h20.loc[h20.stage=='evaluated', 'calculated_fund'].sum())}")
    print(f"2 - entities_part={'{:,.1f}'.format(entities_part.loc[entities_part.stage=='successful', 'calculated_fund'].sum())},h20={'{:,.1f}'.format(h20.loc[h20.stage=='successful', 'calculated_fund'].sum())}")
    return entities_part


def entities_ods(FP, entities_participation):
    # ### entities pour ODS
    import math
    if FP=='horizon':
        filter_FP='Horizon Europe'
    elif FP=='h20':
        filter_FP='Horizon 2020'

    tmp=entities_participation[select_cols_FP(FP, 'proj_entities')].loc[(entities_participation.framework==filter_FP)]
    tmp=tmp.rename(columns=rename_cols_FP(FP, 'proj_entities'))

    tmp.loc[tmp.entities_id_source=='ror', 'entities_id'] = tmp.loc[tmp.entities_id_source=='ror', 'entities_id'].str.replace("^R", "", regex=True)
    tmp.loc[tmp.entities_id_source=='pic', 'entities_id_source'] = 'ecorda pic'
    tmp.loc[tmp.entities_id_source=='identifiantAssociationUniteLegale', 'entities_id_source'] = 'rna'
    tmp.loc[(tmp.entities_id_source.isnull())&(tmp.entities_id.str.contains('gent', na=False)), 'entities_id_source'] = 'paysage'

    #     if i=='successful':
    act_liste = ['RIA', 'MSCA', 'IA', 'CSA', 'ERC', 'EIC']
    tmp = tmp.assign(action_group_code=tmp.action_id, action_group_name=tmp.action_name)
    tmp.loc[~tmp.action_id.isin(act_liste), 'action_group_code'] = 'ACT-OTHER'
    tmp.loc[~tmp.action_id.isin(act_liste), 'action_group_name'] = 'Others actions'

    # for i in ['abstract', 'free_keywords']:
    #     tmp[i] = tmp[i].str.replace('\\n|\\t|\\r|\\s+|^\\"', ' ', regex=True).str.strip()

    # tmp['free_keywords'] = tmp['free_keywords'].str.lower()

    tmp.loc[(tmp.stage=='successful')&(tmp.status_code=='UNDER_PREPARATION'), 'abstract'] = np.nan

    tmp = df_order_cols_FP(FP,  'proj_entities', tmp)
    # ATTENTION si changement de nom de vars -> la modifier aussi dans pcri_info_columns_order
    
    # for h in tmp.framework.unique():
    x = (tmp[(tmp.stage=='successful')]
            .drop(columns=['panel_regroupement_code', 'panel_code', 'erc_role', 'fund_ent_erc']))
    x.loc[x.thema_code.isin(['ERC','MSCA']), ['destination_code', 'destination_name_en']] = np.nan
    # x = entreprise_cat_cleaning(x)
    chunk_size = int(math.ceil((x.shape[0] / 2)))
    i=0
    for start in range(0, x.shape[0], chunk_size):
        df_subset = x.iloc[start:start + chunk_size]
        i=i+1
        if FP=='h20':
            FP='h2020'
        zipfile_ods(df_subset, f"fr-esr-{FP}-projects-entities{i}")
 
    tmp1 = tmp.loc[(tmp.stage=='evaluated')].rename(columns={ 'number_involved':'numberofapplicants'})

    l=['country_name_mapping', 'country_association_name_en', 'country_name_en', 
            'country_code_mapping', 'pilier_name_fr', 'programme_code', 
            'operateur_num','operateur_lib', 'ror_category', 'paysage_category', 'country_association_name_en',
            'country_association_name_fr', 'thema_name_fr', 'destination_lib',
            'programme_name_fr', 'action_group_code', 'action_group_name', 
            'cordis_type_entity_name_en', 'cordis_type_entity_acro','cordis_type_entity_name_fr']
    del_i=[i for i in l if i in tmp1.columns]
    tmp1.drop(columns=del_i, inplace=True)

    if FP=='h20':
        FP='h2020'
        x = tmp1[tmp1.country_code=='FRA']
    else:
        x=tmp1
    chunk_size = int(math.ceil((x.shape[0] / 2)))
    i=0
    for start in range(0, x.shape[0], chunk_size):
        df_subset = x.iloc[start:start+chunk_size]
        i=i+1
        zipfile_ods(df_subset, f"fr-esr-{FP}-projects-entities-evaluated{i}")




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