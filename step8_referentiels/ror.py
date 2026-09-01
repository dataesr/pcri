
def ror_prep(countries):
    from functions_shared import unzip_zip, my_country_code
    from paths import PATH_REF
    import pandas as pd, numpy as np

    print('### ROR preparation')

    ror = pd.read_pickle(f"{PATH_REF}ror_all.pkl")
    ror = pd.json_normalize(ror)
    print(f"size ror: {len(ror)}")

    ror = (ror[
        ['id', 'name_usual', 'name_local', 'types', 'alias', 'acronym', 'status',
        'link_website', 'city', 'geo_admin1_name', 'iso2',]
        ]        
    )

    print(f"vars en list: {ror.columns[ror.map(lambda x: isinstance(x, list)).any()]}")

    mask_nan = ror['name_usual'].isna() | ror['name_local'].isna()
    mask_equal = ror['name_usual'].str.lower() == ror['name_local'].str.lower()

    ror['nom_long'] = np.select(
    [mask_nan, mask_equal],
    [ror['name_usual'].fillna(ror['name_local']), ror['name_usual']],
    default=ror['name_usual'] + " | " + ror['name_local']
    )

    ror.mask(ror=='', inplace=True)
    
    ror = (ror
            .merge(countries[
                ['iso2', 'iso3']
                ], how='left', on='iso2')
            .rename(columns={
                'id': 'numero_ror',  
                'acronym':'sigle',
                'city': 'ville',
                'geo_admin1_name': 'adresse',
                'link_wikipedia': 'wikipedia_url',
                'link_website': 'web',
                'iso3':'country_code_map'}
                )
            .assign(ref='ror')
    )

    if any(ror.country_code_map.isnull()):
        print(ror[ror.country_code_map.isnull()][['iso2']].drop_duplicates())

    ror.drop(columns=['alias', 'iso2', 'name_usual', 'name_local'], inplace=True)

    ror.mask(ror=='', inplace=True)
    print(f"ror end size: {len(ror)}")
    return ror
