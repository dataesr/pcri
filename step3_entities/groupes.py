

def merge_groupe(entities_tmp, groupe):
    print("\n### merge avec GROUPE")
    entities_tmp=entities_tmp.merge(groupe, how='left', on='siren')

    if any(entities_tmp.siren.str.contains(';', na=False)):
        print("ATTENTION faire code pour traiter deux siren différents -> ce qui serait bizarre qu'il y ait 2 siren")
    else:
        entities_tmp.loc[~entities_tmp.groupe_id.isnull(), 'entities_name_source']= entities_tmp.entities_name
        entities_tmp.loc[~entities_tmp.groupe_id.isnull(), 'entities_acronym_source']= entities_tmp.entities_acronym
        entities_tmp.loc[~entities_tmp.groupe_id.isnull(), 'entities_id']= entities_tmp.groupe_id
        entities_tmp.loc[~entities_tmp.groupe_id.isnull(), 'entities_acronym'] = entities_tmp.groupe_acronym
        entities_tmp.loc[~entities_tmp.groupe_id.isnull(), 'entities_name'] = entities_tmp.groupe_name

        entities_tmp.loc[entities_tmp.entities_id.str.contains('gent', na=False), 'siren_cj'] = 'GE_ENT'

        entities_tmp = entities_tmp.drop(['groupe_id','groupe_name','groupe_acronym'], axis=1).drop_duplicates()
    print(f"taille de entities_tmp après groupe {len(entities_tmp)}")
    return entities_tmp