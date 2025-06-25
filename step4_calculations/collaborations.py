import pandas as pd, numpy as np

def collab_base(tab, stage_value:str):
    print("### COLLAB base")
    tmp = tab[(tab['stage']==stage_value)].rename(columns={'calculated_fund':'fund'}).drop(columns='country_code_mapping')

    print(f"subv:{'{:,.1f}'.format(tmp['fund'].sum())}")

    tmp['part_num'] = (tmp[["orderNumber", "generalPic", 'participates_as']]
                                .apply(lambda row:"-".join(row.values.astype(str)), axis=1))
    tmp['coord_num'] = (tmp.loc[tmp['role']=='coordinator', ["orderNumber", "generalPic"]]
                                .apply(lambda row:"-".join(row.values.astype(str)), axis=1))

    copy = (tab[(tab['stage']==stage_value)]
                    .drop(columns='role')
                    .add_suffix('_collab')
                    .rename(columns={'project_id_collab':'project_id', 'calculated_fund_collab':'fund_collab'})
                )

    print(f"subv copy:{'{:,.1f}'.format(copy['fund_collab'].sum())}, size: {len(copy)}")
    copy['part_num_collab'] = (copy[["orderNumber_collab", "generalPic_collab", 'participates_as_collab']]
                                .apply(lambda row:"-".join(row.values.astype(str)), axis=1))

    return tmp.merge(copy, on='project_id')

def collab_cross(i):
    return (i[~((i['orderNumber']==i['orderNumber_collab']) &
                (i['generalPic']==i['generalPic_collab']) &
                # (i['pic']==i['pic_collab'])&
                (i['participates_as']==i['participates_as_collab']))]
                .groupby(['stage','project_id','country_code', 'participation_nuts','region_1_name', 'extra_joint_organization','country_code_collab',
                        'participation_nuts_collab', 'region_1_name_collab','country_code_mapping_collab', 'participates_as', 'participates_as_collab', 
                        'extra_joint_organization_collab', 'is_ejo', 'with_coord'], dropna=False)
                .agg({'part_num':'nunique', 'coord_num':'nunique', 'part_num_collab':'nunique', 'fund':'sum', 
                    'fund_collab':'sum'})
                .reset_index())

def collab(participation, projects, countries):
    print("### COLLABORATIONS")

    p=(participation
       .drop(columns=['cordis_type_entity_code', 'cordis_is_sme', 'entreprise_flag', 'erc_role',
       'cordis_type_entity_name_fr', 'cordis_type_entity_name_en', 'cordis_type_entity_acro', 
       'coordination_number', 'beneficiary_fund', 'fund_ent_erc']))

    cc = (countries.drop(columns=['countryCode', 'country_code', 'countryCode_parent'])
          .drop_duplicates()
          .rename(columns={'countryCode_iso3':'country_code'}))

    collab_eval=collab_base(p, 'evaluated')
    collab_signed=collab_base(p, 'successful')

    '''COLLAB - general'''

    col_eval = collab_cross(collab_eval)
    col_signed = collab_cross(collab_signed)

    collab=pd.concat([col_eval, col_signed], ignore_index=True)

    # add countries infos
    collab = (collab.merge(cc, how='left', on='country_code')
                .drop(columns=['country_association_name_en', 'country_group_association_name_fr',
                'country_association_code', 'country_group_association_name_en']))

    countries_collab = cc.add_suffix('_collab')
    collab = (collab.merge(countries_collab, how='left', on='country_code_collab')
                .drop(columns=['country_association_name_en_collab', 'country_group_association_name_fr_collab',
                'country_association_code_collab', 'country_group_association_name_en_collab']))

    # add projects infos
    proj = projects[['project_id', 'stage', 'status_code', 'action_code', 'action_name','call_id', 'call_year','topic_code', 'topic_name',
                'pilier_code', 'pilier_name_en', 'pilier_name_fr', 'programme_code', 'programme_name_en','programme_name_fr',
                'thema_code', 'thema_name_fr', 'thema_name_en',  'ecorda_date', 
                'destination_code','destination_lib', 'destination_name_en', 'destination_detail_code', 
                'destination_detail_name_en', 'total_cost',  'abstract', 'free_keywords']].drop_duplicates()
    collab=(collab
            .merge(proj, how='inner', on=['project_id','stage'])
            .drop_duplicates())

    print(f"size collab {len(collab)}")
    return collab