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
    
    x = (x.merge(old_country[['countryCode', 'STATUS']].drop_duplicates(), how='left', on='countryCode')
        .rename(columns={'STATUS':'fp_specific_country_status'}))

    if any(x.country_association_code.isnull()):
        print(f"- country code association null: {x.loc[x.country_association_code.isnull(), ['country_code_mapping']]}")

    x = x.drop(columns=['parent_iso2']).drop_duplicates()
    print(f"size x {len(x)}")

    return x