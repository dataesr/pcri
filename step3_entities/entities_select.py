import pandas as pd, numpy as np
from config_path import PATH_WORK
from step3_entities.ID_getSourceRef import *


def entities_tmp_create(entities_info, countries, ref):
    print("### create ENTITIES TMP pour ref")
    tab = entities_info.merge(countries[['country_code_mapping', 'country_name_mapping', 'countryCode_parent']], how='left', on='country_code_mapping')
    tmp = tab.merge(ref, how='inner', on=['generalPic','country_code_mapping'])
    print(f"- size entities_info before:{len(entities_info)}, size entities_info+ref -> tmp:{len(tmp)}, Pic unique tmp:{len(tmp.generalPic.unique())}")
    # entities only into entities_info
    print("# missing entities into ref")
    tmp2 = tab.merge(tmp[['generalPic','country_code_mapping']], how='left', on=['generalPic','country_code_mapping'], indicator=True).query('_merge=="left_only"').drop(columns=['_merge'])
    print(f"- entities_info en + -> (tmp2): {len(tmp2)}")
    if not tmp2.empty:
        # test lien avec ref voire si un identifiant seulement sur le generalPic
        tmp2 = tmp2.merge(ref.drop(columns='country_code_mapping').drop_duplicates(), how='inner', on='generalPic')
        print(f"- size lien tmp2 with ref: {len(tmp2)}")
        ## add tmp2 to tmp
        tmp1 = pd.concat([tmp, tmp2], ignore_index=True)
        # entities_info without id
        tmp1 = tab.merge(tmp1[['generalPic','country_code_mapping']], how='left',on=['generalPic','country_code_mapping'], indicator=True).query('_merge=="left_only"').drop(columns=['_merge'])
        print(f"- size entities_info without id -> tmp1: {len(tmp1)}")
        tmp = pd.concat([tmp1, tmp], ignore_index=True)

    if (len(tmp))!=(len(entities_info)):
        print(f"1 - ATTENTION!!! size result {len(tmp)} diff size entities_info {len(entities_info)}")
    print(f"- End size entities_tmp {len(tmp)}")
    return tmp


def entities_for_merge(entities_tmp):
    entities_tmp = entities_tmp[['generalPic','legalName', 'businessName', 'id', 'id_secondaire', 'ZONAGE', 'country_code_mapping', 'countryCode_parent']]
    entities_tmp = entities_tmp.mask(entities_tmp=='')
    print(f"1 - After add ref to entities: {len(entities_tmp)}\n\n{entities_tmp.columns}")

    if any(entities_tmp.id.str.contains(';')):
        entities_tmp = entities_tmp.assign(id_extend=entities_tmp.id.str.split(';')).explode('id_extend').drop_duplicates()
        entities_size_to_keep = len(entities_tmp)
        print(f"2 - size entities si multi id -> entities_size_to_keep = {entities_size_to_keep}")
    return entities_tmp

def ID_entities_list(ref_source):
    ref = ref_source.loc[(ref_source.FP.str.contains('H20|HE|FP7'))&(~ref_source.id.isnull())].id.str.split(';').explode('id')
    lid=list(ref.drop_duplicates().sort_values())
    print(f"size lid:{len(lid)}")
    lid_source=sourcer_ID(lid)
    unknow_list = set(lid)-set([i['api_id'] for i in lid_source])
    print(f"id non sourcés :{len(unknow_list)}\n{unknow_list}")

    with open(f"{PATH_WORK}list_id_for_ref.pkl", 'wb') as fp:
        pd.to_pickle(lid, fp)
    return lid_source, unknow_list