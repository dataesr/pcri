
def entities_first_preparation(ref_source, entities_info):
    from step3_entities.references import ref_source_1ere_select
    from step3_entities.entities_select import entities_tmp_create
    
    print("### FIRST UPDATE entities and ref_source")
    ref = ref_source_1ere_select(ref_source)
    entities_tmp, rep = entities_tmp_create(entities_info, ref)
    print(f"size entities_tmp: {len(entities_tmp)}")
    return entities_tmp

def identification_update(source_json, entities_tmp):
    """
    create a list of IDs to chack
    VAT for France, legalEntitiesLinks for other countries
    List of IDs to check     
    """
    from step3_entities.IDlegal_cleaning import legal_id_clean, entities_link, list_to_check
    from remote_process.ID_getSourceRef import get_source_ID, fix_source_from_siren_to_ror

    identification = legal_id_clean(entities_tmp) # France, clean VAT and legalNumber declared -> return all records
    multiple = entities_link(source_json, entities_tmp) # for other countries, use the legalEntitiesLinks.json
    identification = identification.merge(multiple, how='left', on="generalPic")
    identification['legalName'] = identification['legalName'].str.strip()
    print(f"Size tmp:{len(identification)}, size entities_tmp:{len(entities_tmp)}")
    check_id_df = list_to_check(identification) # create a list of all ID to check in source API

    check_id_df = get_source_ID(check_id_df, 'check_id') # get source of ID to check in source API
    check_id_df = check_id_df.mask(check_id_df=='')
    check_id_df = fix_source_from_siren_to_ror(check_id_df, 'check_id') # fix ror ID
    return check_id_df, identification