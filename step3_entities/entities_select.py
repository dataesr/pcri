import pandas as pd, numpy as np
from config_path import PATH_WORK
from remote_process.ID_getSourceRef import *


def entities_tmp_create(df, ref):
    """
    link entities_info and filtered ef_source = ref
    
    """
    print("## create ENTITIES TMP pour ref")

    # merge with ref on country_code_source
    tmp = (df.merge(ref.drop(columns='country_code').drop_duplicates(), 
                     how='inner', on=['generalPic','country_code_source']))
    print(f"- size entities_info before:{len(df)}, size entities_info+ref -> tmp:{len(tmp)}, Pic unique tmp:{len(tmp.generalPic.unique())}")
    rep=[{'stage_process':'entities_merge_ref', 'entities_size':len(tmp)}]
 
    # keep only entities not merged ; missing generalPic+cc in ref_source
    print("# missing entities into ref")
    tmp1 = df.merge(tmp[['generalPic','country_code_source']], how='left', on=['generalPic','country_code_source'], indicator=True).query('_merge=="left_only"').drop(columns=['_merge'])
    print(f"- entities_info en + -> (tmp2): {len(tmp1)}")
    
    if not tmp1.empty:
        # merge the rest with ref on country_code
        tmp2 = (tmp1.merge(ref.drop(columns='country_code_source').drop_duplicates(), 
                           how='inner', on=['generalPic', 'country_code']))
        print(f"- size lien tmp2 with ref: {len(tmp2)}")
        ## add tmp2 to tmp
        tmp = pd.concat([tmp, tmp2], ignore_index=True)

        tmp1 = tmp1.merge(tmp2[['generalPic','country_code']], how='left', on=['generalPic','country_code'], indicator=True).query('_merge=="left_only"').drop(columns=['_merge'])
        print(f"- entities_info en + -> (tmp2): {len(tmp1)}")

        # merge just on generalPic ; remove generalPic duplicated
        tmp2 = tmp1.merge(ref.drop(columns=['country_code_source', 'country_code']).drop_duplicates(), how='inner', on='generalPic')
        if len(tmp2.groupby('generalPic')['country_code_source'].size().reset_index(name='nb').query('nb>1'))>0:
            remove=tmp2.groupby('generalPic')['country_code_source'].size().reset_index(name='nb').query('nb>1').generalPic.unique()
            tmp2 = tmp2.loc[tmp2.generalPic.isin(remove)]

        tmp = pd.concat([tmp, tmp2], ignore_index=True)
        
        # entities_info without id
        tmp1 = (df.merge(tmp[['generalPic','country_code_source']], 
                    how='left',on=['generalPic','country_code_source'], indicator=True)
                    .query('_merge=="left_only"')
                    .drop(columns=['_merge'])
                    .merge(tmp[['generalPic','country_code']], 
                    how='left',on=['generalPic','country_code'], indicator=True)
                    .query('_merge=="left_only"')
                    .drop(columns=['_merge']))
        print(f"- size entities_info without id -> tmp1: {len(tmp1)}")
        tmp = pd.concat([tmp1, tmp], ignore_index=True)

    # tmp.loc[tmp['source_id']=='paysage', 'id_paysage'] = tmp.loc[tmp['source_id']=='paysage', 'id']
    # tmp.loc[~tmp['resourceId'].isnull(), 'id_extend'] = tmp.loc[~tmp['resourceId'].isnull(), 'resourceId']

    if (len(tmp))!=(len(df)):
        print(f"1 - ATTENTION!!! size result {len(tmp)} diff size entities_info {len(df)}")
    print(f"- End size entities_tmp {len(tmp)}")
    rep.append({'stage_process':'entities_tmp', 'entities_size':len(tmp)})
    return tmp, rep

def entities_for_merge(entities_tmp):
    entities_tmp = entities_tmp.drop(columns='id_extend').rename(columns={'from_id_to_ref':'id_extend'})
    entities_tmp.loc[entities_tmp['resourceId'].notnull(), 'id_extend'] = entities_tmp.loc[entities_tmp['resourceId'].notnull(), 'resourceId']
    entities_tmp.loc[entities_tmp['in_paysage']==True, 'source_id'] = 'paysage'
    entities_tmp = entities_tmp[['generalPic','legalName', 'businessName', 'id_first', 'id_secondaire', 'ZONAGE', 'country_code_source', 'country_code', 'id_extend', 'source_id']]
    entities_tmp = entities_tmp.mask(entities_tmp=='')
    print(f"1 - After add ref to entities: {len(entities_tmp)}\n\n{entities_tmp.columns}")

    if any(entities_tmp['id_first'].str.contains(';')):
        # entities_tmp = entities_tmp.assign(id_extend=entities_tmp.id.str.split(';')).explode('id_extend').drop_duplicates()
        entities_size_to_keep = len(entities_tmp)
        print(f"2 - size entities si multi id -> entities_size_to_keep = {entities_size_to_keep}")
    return entities_tmp

def ID_entities_list(ref_source):
    ref = ref_source.loc[(ref_source.FP.str.contains('H20|HE|FP7'))&((~ref_source.id.isnull())|(ref_source.id!='0'))].id.str.split(';| ').explode('id')
    lid=list(ref.drop_duplicates().sort_values())
    print(f"size lid:{len(lid)}")
    lid_source=sourcer_ID(lid)
    unknow_list = set(lid)-set([i['api_id'] for i in lid_source])
    print(f"id non sourcés :{len(unknow_list)}\n{unknow_list}")

    with open(f"{PATH_WORK}list_id_for_ref.pkl", 'wb') as fp:
        pd.to_pickle(lid, fp)
    return lid_source, unknow_list