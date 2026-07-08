import pandas as pd, numpy as np

def collab_cross(p):
    from itertools import combinations

    # --- STEP 1: group participants per (project_id, stage)
    grouped = p.groupby(["project_id", "stage"])

    collaboration_rows = []

    for (project_id, stage), g in grouped:
        # List of participants in that project/stage
        participants = g[["generalPic", 'orderNumber', 'extra_joint_organization', 'country_code']].drop_duplicates()

        # All unique unordered pairs
        for (p1, order1, ejo1, cc1), (p2, order2, ejo2, cc2) in combinations(participants.values, 2):
            collaboration_rows.append({
                "project_id": project_id,
                "stage": stage,
                "generalPic": p1,
                "orderNumber":order1,
                "extra_joint_organization":ejo1,
                "country_code":cc1,
                "generalPic_collab": p2,
                "ordreNumber_collab":order2,
                "extra_joint_organization_collab":ejo2,
                "country_code_collab":cc2,

            })
    print(f"-size collab_cross: {len(collaboration_rows)}")
    return pd.DataFrame(collaboration_rows)


# y=participation[['project_id', 'stage', 'generalPic', 'orderNumber', 'participates_as', 'role', 'calculated_fund']]

# def collab_base(tab, stage_value:str):
#     print("### COLLAB base")
#     tmp = tab[(tab['stage']==stage_value)].rename(columns={'calculated_fund':'fund'}).drop(columns='country_code_mapping')

#     print(f"subv:{'{:,.1f}'.format(tmp['fund'].sum())}")

#     tmp['part_num'] = (tmp[["orderNumber", "generalPic", 'participates_as']]
#                                 .apply(lambda row:"-".join(row.values.astype(str)), axis=1))
#     tmp['coord_num'] = (tmp.loc[tmp['role'].str.lower()=='coordinator', ["orderNumber", "generalPic"]]
#                                 .apply(lambda row:"-".join(row.values.astype(str)), axis=1))

#     copy = (tab[(tab['stage']==stage_value)]
#                     .drop(columns='role')
#                     .add_suffix('_collab')
#                     .rename(columns={'project_id_collab':'project_id', 'calculated_fund_collab':'fund_collab'})
#                 )

#     print(f"subv copy:{'{:,.1f}'.format(copy['fund_collab'].sum())}, size: {len(copy)}")
#     copy['part_num_collab'] = (copy[["orderNumber_collab", "generalPic_collab", 'participates_as_collab']]
#                                 .apply(lambda row:"-".join(row.values.astype(str)), axis=1))

#     return tmp.merge(copy, on='project_id')

# def collab_cross(i):
#     return (i[~((i['orderNumber']==i['orderNumber_collab']) &
#                 (i['generalPic']==i['generalPic_collab']) &
#                 # (i['pic']==i['pic_collab'])&
#                 (i['participates_as']==i['participates_as_collab']))]
#                 .groupby(['stage','project_id','country_code', 'participation_nuts','region_1_name', 'extra_joint_organization','country_code_collab',
#                         'participation_nuts_collab', 'region_1_name_collab','country_code_mapping_collab', 'participates_as', 'participates_as_collab', 
#                         'extra_joint_organization_collab', 'is_ejo', 'with_coord'], dropna=False)
#                 .agg({'part_num':'nunique', 'coord_num':'nunique', 'part_num_collab':'nunique', 'fund':'sum', 
#                     'fund_collab':'sum'})
#                 .reset_index())

def collab(participation, projects, countries):
    print("### COLLABORATIONS")

    pc = collab_cross(participation)
    pc = pc[['stage', 'project_id', 'extra_joint_organization', 'country_code', 'extra_joint_organization_collab', 'country_code_collab']].drop_duplicates()

    p = (participation[['stage', 'project_id', 'with_coord', 'country_code', 'extra_joint_organization', 'role', 'calculated_fund']]
         .rename(columns={'calculated_fund':'fund'})
         .assign(part_num=1, coord_num=np.where(participation['role']=='Coordinator', 1,0))
    )
    p = (p.groupby(['stage', 'project_id', 'with_coord', 'extra_joint_organization', 'country_code'], dropna=False)
            .agg({'fund':'sum', 'part_num':'sum', 'coord_num':'sum'})
            .reset_index()
    )
    p.loc[p['with_coord']==False, 'coord_num'] = 0


    pc1 = pd.merge(pc, p, how='left', on=['stage', 'project_id', 'extra_joint_organization', 'country_code'])

    rc = ['stage', 'project_id', 'with_coord']
    p = p.rename(columns={col: f"{col}_collab" for col in p.columns if col not in rc}).drop(columns='with_coord')

    pc1 = pd.merge(pc1, p, how='left', on=['stage', 'project_id', 'extra_joint_organization_collab', 'country_code_collab'])


    def link_cc(df, var, cc):
        for i in [var, f'{var}_collab']:
            if i==f'{var}_collab':
                cc = cc.rename(columns=lambda x: f"{x}_collab")
                df = pd.merge(df, cc, how='left', on=i)
            else:
                df = pd.merge(df, cc, how='left', on=i)
        return df

    cc = (countries[['countryCode_iso3', 'country_name_fr', 'country_name_en', 'country_group_association_code','country_group_association_name_fr','country_group_association_name_en']]
        .rename(columns={'countryCode_iso3':'country_code'})
        .drop_duplicates()
    )
    pc1 = link_cc(pc1, 'country_code', cc)

    # cc = (countries[['countryCode_iso3', 'country_name_en', 'country_name_fr', 'country_group_association_code','country_group_association_name_fr','country_group_association_name_en']]
    #     .rename(columns={'countryCode_iso3':'country_code'})
    #     .drop_duplicates()
    # )
    # pc1 = link_cc(pc1, 'country_code', cc)


    # for i in ['country_code', 'country_code_collab']:
    #     if i=='country_code_collab':
    #         cc = cc.rename(columns={col: f"{col}_collab" for col in rc})
    #         pc = pd.merge(pc, cc, how='left', on=i)
    #     else:
    #         pc = pd.merge(pc, cc, how='left', on=i)   

    # collab_eval=collab_base(p, 'evaluated')
    # collab_signed=collab_base(p, 'successful')

    # '''COLLAB - general'''

    # col_eval = collab_cross(collab_eval)
    # col_signed = collab_cross(collab_signed)

    # collab=pd.concat([col_eval, col_signed], ignore_index=True)

    # # add countries infos
    # collab = (collab.merge(cc, how='left', on='country_code')
    #             .drop(columns=['country_association_name_en', 'country_group_association_name_fr',
    #             'country_association_code', 'country_group_association_name_en']))

    # countries_collab = cc.add_suffix('_collab')
    # collab = (collab.merge(countries_collab, how='left', on='country_code_collab')
    #             .drop(columns=['country_association_name_en_collab', 'country_group_association_name_fr_collab',
    #             'country_association_code_collab', 'country_group_association_name_en_collab']))

    # add projects infos
    proj = projects[['project_id', 'stage', 'status_code', 'action_code', 'action_name','call_id', 'call_year','topic_code', 'topic_name',
                'pilier_code', 'pilier_name_en', 'pilier_name_fr', 'programme_code', 'programme_name_en','programme_name_fr',
                'thema_code', 'thema_name_fr', 'thema_name_en',  'ecorda_date', 'total_cost',
                'euro_ps_name', 'euro_partnerships_flag', 'euro_partnerships_type',
                'destination_code','destination_lib', 'destination_name_en', 'destination_detail_code', 
                'destination_detail_name_en', 'abstract', 'free_keywords']].drop_duplicates()
    
    collab=(pd.merge(pc1, proj, how='inner', on=['project_id','stage'])
            .drop_duplicates())

    print(f"size collab {len(collab)}")
    return collab