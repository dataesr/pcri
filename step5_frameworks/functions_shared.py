def country_cleaning(df, FP):
    import pandas as pd, json
    from functions_shared import my_country_code
    my_country=my_country_code()
    old_country = pd.read_csv('data_files/FP_old_countries.csv', sep=';', keep_default_na=False)
    old_country = old_country.loc[old_country.FP==FP].drop_duplicates()
    
    x = df[['countryCode']].drop_duplicates().merge(my_country, how='left', left_on='countryCode', right_on='iso2')
    if any(x.iso2.isnull()):
        print(f"- countryCode not into my_country: {x[x.iso2.isnull()].countryCode.unique()}")
    if any(x.country_name_en.isnull()):
        print(f"- countryCode without name into my_country: {x[x.country_name_en.isnull()].countryCode.unique}")
    
    x = (x
        .rename(columns={"iso3":"country_code_mapping", 'country_name_en':'country_name_mapping'})
        .merge(my_country[['iso3', 'country_name_en']], how='left', left_on='parent_iso3', right_on='iso3')
        .drop(columns=['iso2', 'iso3'])
        .rename(columns={"parent_iso3":"country_code"})
        )

    if any(x.country_code.isnull()):
        print(f"- country_code parent null: {x[x.country_code.isnull()].country_code_mapping.unique()}")
    if any(x.country_name_mapping.isnull()):
        print(f"- country name mapping null: {x[x.country_name_mapping.isnull()].country_code_mapping.unique()}")

    countries_fr = json.load(open('data_files/countries_fr.json', 'r+', encoding='UTF-8'))
    countries_fr = pd.DataFrame(countries_fr).rename(columns={'country_name':'country_name_fr', 'country_code':'parent_iso2'})

    x = x.merge(countries_fr, how='left', on='parent_iso2')
    if any(x.country_name_fr.isnull()):
        print(f"- parent country_name_fr missing: {x[x.country_name_fr.isnull()][['country_code', 'parent_iso2', 'countryCode']]}")

    assoc=pd.read_json(open('data_files/countries_association.json', 'r+', encoding='UTF-8'))
    x = (x.merge(old_country[['country_code_mapping', 'status_new']].drop_duplicates()
            .rename(columns={'country_code_mapping':'country_code', 'status_new':'country_association_code'})
            .drop_duplicates(), 
                how='left', on='country_code')
        .merge(assoc, how='left', on='country_association_code')
        .drop_duplicates()
        )
    
    article_fr = json.load(open('data_files/countries_genres.json', 'r+', encoding='UTF-8'))
    article_fr = pd.DataFrame.from_dict(article_fr, orient='index').reset_index().rename(columns={'index':'country_code'})
    article_fr = article_fr.apply(lambda x: x.str.strip() if x.dtype.name == 'object' else x, axis=0)
    x = x.merge(article_fr, how='left', on='country_code').drop(columns='label')

    x = (x.merge(old_country[['countryCode', 'STATUS']].drop_duplicates(), how='left', on='countryCode')
        .rename(columns={'STATUS':'fp_specific_country_status'}))

    if any(x.country_association_code.isnull()):
        print(f"- country code association null: {x.loc[x.country_association_code.isnull(), ['country_code_mapping']]}")

    x = x.drop(columns=['parent_iso2']).drop_duplicates()
    print(f"size x {len(x)}")

    return x


def thema_msca_cleaning(df, FP):
    import pandas as pd
    msca_correspondence = pd.read_table('data_files/msca_correspondence.csv', sep=";")
    msca_correspondence = msca_correspondence.loc[msca_correspondence.framework==FP].drop(columns='framework')

    df['thema_code'] = 'MSCA'
    df.loc[(df.call_id.str.contains('NIGHT'))|(df.call_id=='FP6-2006-MOBILITY-13'), 'inst'] = 'NIGHT'
    df = (df.merge(msca_correspondence, how='left', left_on=['inst'], right_on=['old'])
          .rename(columns={'old':'destination_code', 'new':'destination_next_fp'}))
    
    for i in ['action', 'instrument']:
        if i in df.columns:
            df.loc[(df.destination_code.isnull())&(df[i].str.startswith('MC')), 'destination_code'] = df.loc[(df.destination_code.isnull())&(df[i].str.startswith('MC'))].inst 
    for i in ['destination_code', 'destination_next_fp']:
        df.loc[(df[i].isnull()), i] = 'MSCA-OTHER'
        
    for i in ['inst', 'action']:
        if i in df.columns:
            df.drop(columns=i, inplace=True)

    return df

def thema_euratom_cleaning(df, FP):
    import pandas as pd
    euratom = pd.read_csv('data_files/euratom_thema_all_FP.csv', sep=';', na_values='')
    euratom = euratom.loc[euratom.framework==FP, ['topic_area', 'thema_code']]
    df = df.merge(euratom, how='left', on='topic_area')

    df['programme_name_en'] = 'Nuclear fission and radiation protection'
    df['programme_code'] = 'NFRP'

    if FP=='FP7':
        df.loc[df.prog_abbr=='Fusion', 'programme_code'] = 'Fusion'
        df.loc[df.prog_abbr=='Fusion', 'programme_name_en'] = 'Fusion Energy'
        
    return df.drop(columns='topic_area')