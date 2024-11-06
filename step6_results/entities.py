
def entities_preparation(entities_part, h20):
    tmp= h20.rename(columns={ 'subv':'beneficiary_subv'})[
            ['action_code','action_name', 'action_code2', 'action_name2', 'article1', 'article2', 
            'call_deadline',
            'calculated_fund', 'call_id', 'call_year', 'cj_code', 'category_woven',
            'coordination_number', 'cordis_is_sme', 'cordis_type_entity_acro', 
            'extra_joint_organization', 'with_coord',
            'cordis_type_entity_code', 'cordis_type_entity_name_en',
            'cordis_type_entity_name_fr', 'nuts_code', 'region_1_name', 'region_2_name', 
            'regional_unit_name',
            'country_association_code','country_association_name_en', 'country_code', 
            'country_code_mapping',
            'country_group_association_code', 'country_group_association_name_en',
            'country_group_association_name_fr', 'country_name_en',
            'country_name_fr', 'country_name_mapping', 'destination_code', 'destination_name_en',
            'destination_detail_code', 'destination_detail_name_en', 
            'entities_acronym', 'entities_id', 'entities_name', 'framework', 'groupe_sector',
            'insee_cat_code', 'insee_cat_name', 'number_involved', 'participates_as', 
            'paysage_category', 'paysage_category_id',
            'pilier_name_en', 'pilier_name_fr', 'programme_code', 
            'panel_code', 'panel_name', 'panel_regroupement_code', 'panel_regroupement_name',
            'programme_name_en', 'project_id', 'role', 'erc_role', 'ror_category', 'siren_cj',
            'stage', 'status_code', 'thema_code', 'thema_name_en', 'topic_code', 'free_keywords', 
            'abstract', 'ecorda_date']]


    tmp = tmp.merge(ope_cat[['entities_id','operateur_name','operateur_num','operateur_lib']], how='left', on='entities_id')

    tmp = (tmp
        .groupby(list(tmp.columns.difference(['coordination_number', 'number_involved', 'calculated_fund'])), dropna=False, as_index=False).sum()
        .drop_duplicates()
        )

    print(f"1 - tmp={'{:,.1f}'.format(tmp.loc[tmp.stage=='evaluated', 'calculated_fund'].sum())},h20={'{:,.1f}'.format(h20.loc[h20.stage=='evaluated', 'calculated_fund'].sum())}")
    print(f"2 - tmp={'{:,.1f}'.format(tmp.loc[tmp.stage=='successful', 'calculated_fund'].sum())},h20={'{:,.1f}'.format(h20.loc[h20.stage=='successful', 'calculated_fund'].sum())}")
    # print(f"3 - comparaison nb couple genpic + country (doit être égal) {len(part[['generalPic','country_code']].drop_duplicates())},{len(entities_info[['generalPic','country_code']].drop_duplicates())}")