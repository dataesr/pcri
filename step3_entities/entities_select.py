import pandas as pd
def entities_tmp_create(entities_info, countries, ref):
    
    tab = entities_info.merge(countries[['countryCode', 'country_code_mapping', 'country_name_mapping', 'countryCode_parent']], how='left', on='countryCode')
    tmp1 = tab.merge(ref, how='inner', on=['generalPic','country_code_mapping'])
    print(f"longueur entities_info:{len(entities_info)}, longueur de tmp1:{len(tmp1)}, generalPic unique:{len(tmp1.generalPic.unique())}")

    tmp2 = tab.merge(tmp1[['generalPic','country_code_mapping']], how='left',on=['generalPic','country_code_mapping'], indicator=True).query('_merge=="left_only"').drop(columns=['_merge'])
    tmp2 = tmp2.merge(ref.drop(columns='country_code_mapping'), how='inner', on='generalPic')

    tmp1 = pd.concat([tmp1, tmp2], ignore_index=True)

    tmp = tab.merge(tmp1[['generalPic','country_code_mapping']], how='left',on=['generalPic','country_code_mapping'], indicator=True).query('_merge=="left_only"').drop(columns=['_merge'])

    return pd.concat([tmp1, tmp], ignore_index=True)