
def country_load(framework, liste_country):
    from constant_vars import FRAMEWORK, ZIPNAME
    from config_path import PATH_SOURCE, PATH_CONNECT, PATH_CLEAN
    from functions_shared import unzip_zip, my_country_code
    import json, pandas as pd, numpy as np

    print("\n### LOADING COUNTRY")
    data = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", "countries.json", 'utf8')
    data = pd.DataFrame(data)
    clist = list(set(data.countryCode.to_list()+liste_country))
    my_country=my_country_code()
    df = my_country.loc[my_country.iso2.isin(clist)]

    countryCode_error=list(set(clist)-set(df.iso2.to_list()))
    print(f"- ATTENTION countryCode missing in countryBase {countryCode_error}")

    df.rename(columns={'iso2':'countryCode',
                        'iso3':'country_code_mapping',
                        'country_name_en':'country_name_mapping',
                        'parent_iso2':'countryCode_parent',
                        'parent_iso3':'country_code'},inplace=True)

    df=(df.merge(my_country[['iso3', 'country_name_en']].drop_duplicates(), how='left', left_on='country_code', right_on='iso3')
          .drop(columns='iso3'))

    if any(df.country_name_en.isnull()):
        print(f'Attention ! country_name_en for country_code not exist {df[df.country_name_en.isnull()].country_code.unique()}')


    gr=(data.loc[(data.framework=='H2020')&(data.isoCountryCode.isin(df.country_code.unique())), 
                ['isoCountryCode', 'countryGroupAssociationCode', 'countryGroupAssociation']]
                .drop_duplicates()
                .rename(columns={'countryGroupAssociationCode':'country_association_code_2020',
                                'countryGroupAssociation':'country_association_name_2020_en'})
            .merge(data.loc[(data.framework=='HORIZON')&(data.isoCountryCode.isin(df.country_code.unique())), 
                    ['isoCountryCode', 'countryGroupAssociationCode', 'countryGroupAssociation']]
                    .drop_duplicates()
                    .rename(columns={'countryGroupAssociationCode':'country_association_code',
                                    'countryGroupAssociation':'country_association_name_en'}), 
            how='outer', on='isoCountryCode'))

    gr.loc[gr.country_association_code.str.startswith('THIRD'), 'country_group_association_code'] = "THIRD"
    gr.loc[gr.country_association_code_2020.str.startswith('THIRD'), 'country_group_association_code_2020'] = "THIRD"
    gr.loc[gr.country_association_code.isin(['ASSOCIATED', 'MEMBER-STATE']), 'country_group_association_code'] = "MEMBER-ASSOCIATED"
    gr.loc[gr.country_association_code_2020.isin(['ASSOCIATED', 'MEMBER-STATE']), 'country_group_association_code_2020'] = "MEMBER-ASSOCIATED"

    if any(gr.country_group_association_code_2020.isnull()):
        print(f"country_group h2020 non agregate: {gr[gr.country_group_association_code_2020.isnull()]}")

    gr['country_group_association_name_en'] = np.where(gr.country_group_association_code=='THIRD', "Other countries", "Member States or associated")
    gr['country_group_association_name_2020_en'] = np.where(gr.country_group_association_code_2020=='THIRD', "Other countries", "Member States or associated")
    gr['country_group_association_name_fr'] = np.where(gr.country_group_association_name_en=="Member States or associated", 'Pays membres ou associés', 'Pays tiers')
    gr['country_group_association_name_2020_fr'] = np.where(gr.country_group_association_name_2020_en=="Member States or associated", 'Pays membres ou associés', 'Pays tiers')

    statut_len = 5
    if len(gr['country_association_code'].drop_duplicates()) != statut_len:
        diff = len(gr['country_association_code'].unique())-statut_len
        if diff>0:
            print(f"1- check status countries ->\n{len(gr['country_association_code'].unique())-statut_len} new status")
        else:
            print(f"2 - info status countries ->\n{gr['country_association_code'].nunique()-statut_len} status in data {gr['country_association_code'].unique()}")

    countries = df.merge(gr, how='left', left_on='country_code', right_on='isoCountryCode').drop(columns='isoCountryCode')

    with open('data_files/countries_fr.json', 'r+', encoding='UTF-8') as fp:
                countries_fr = json.load(fp)
    countries_fr = pd.DataFrame(countries_fr).rename(columns={'country_name':'country_name_fr', 'country_code':'countryCode_parent'})
    countries = countries.merge(countries_fr, how='left', on='countryCode_parent')
    if any(countries.country_name_fr.isnull()):
        print(f"country_name FR missing: {countries[countries.country_name_fr.isnull()].country_name_en.unique()}")

    cc=['countryCode', 'countryCode_parent', 'country_code', 'country_association_code','country_group_association_code']
    cn_fr=['country_name_fr','country_group_association_name_fr']
    cn_en=['country_name_en', 'country_association_name_en','country_group_association_name_en']
    row=countries.shape[0]
    for code in ['ZOE','ZOI']:
        for i in cc:
            countries.loc[row,i]=code
        row=row+1

    for j in cn_fr:
        countries.loc[countries.countryCode=='ZOE', j]='Zone organisations européennes'
        countries.loc[countries.countryCode=='ZOI', j]='Zone organisations internationales'
    for j in cn_en:
        countries.loc[countries.countryCode=='ZOE', j]='European organisations area'
        countries.loc[countries.countryCode=='ZOI', j]='International organisations area'   

    with open('data_files/countries_genres.json', 'r+', encoding='UTF-8') as fp:
        article_fr = json.load(fp)
    article_fr = pd.DataFrame.from_dict(article_fr, orient='index').reset_index().rename(columns={'index':'country_code'})
    article_fr = article_fr.apply(lambda x: x.str.strip() if x.dtype.name == 'object' else x, axis=0)

    countries=countries.merge(article_fr, how='left', on='country_code').drop(columns='label')
   
    countries.to_csv(f"{PATH_CONNECT}country_current.csv", index=False, encoding="UTF-8", sep=";", na_rep='')
    file_name = f"{PATH_CLEAN}country_current.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(countries, file)

    # countries = countries.drop('countryCode_parent', axis=1).drop_duplicates()
    if len(countries.columns[countries.isnull().any()])>0:
        list_v = countries.columns[countries.isnull().any()]
        for v in list_v:
            print(f"- des valeurs nulles pour ces lignes:\n{pd.DataFrame(countries[countries[v].isnull()][['countryCode',v]])}")
            
    return countries, countryCode_error

def country_old(df):
    import json
    ccode=json.load(open("data_files/countryCode_match.json"))
    for k,v in ccode.items():
        df.loc[df.countryCode==k, 'countryCode'] = v
    return df

# def country_iso3(df, cc_code):
#     return df.merge(cc_code, how='left', left_on='countryCode', right_on='iso2').drop(columns='iso2').rename(columns={'iso3':'country_code_mapping'})
