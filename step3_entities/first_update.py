
def first_update(ref_source, entities_info, countries):
    from step3_entities.references import ref_source_1ere_select
    from step3_entities.entities_select import entities_tmp_create
    from step3_entities.IDlegal_cleaning import legal_id_clean, entities_link, list_to_check
    from step3_entities.ID_checkingRefExist import check_id
    import time
    
    print("### FIRST UPDATE entities and ref_source")
    ref = ref_source_1ere_select(ref_source)
    entities_tmp = entities_tmp_create(entities_info, countries, ref)
    print(f"size entities_tmp: {len(entities_tmp)}")
    identification = legal_id_clean(entities_tmp)
    multiple = entities_link(entities_tmp)
    identificaton = identification.merge(multiple, how='left', on="generalPic")
    identificaton['legalName'] = identificaton['legalName'].str.strip()
    print(f"Size tmp:{len(identification)}, size entities_tmp:{len(entities_tmp)}")
    check_id_liste = list_to_check(identificaton)
    #####################
    # SI BESOIN DE checker les ID de PIC
    # get_token('474333')
    liste=list(set(check_id_liste.loc[check_id_liste.check_id!='', 'check_id'].unique()))
    print(time.strftime("%H:%M:%S"))  
    result = check_id(liste)
    print(time.strftime("%H:%M:%S"))
    return result, check_id_liste, identification