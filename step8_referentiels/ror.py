######### IMPORT
def ror_import(DUMP_PATH):
    
    from config_path import PATH
    import requests
    site = requests.get('https://zenodo.org/api/records/8436953/files')
    r = site.json()
    ROR_ZIP = r['entries'][0]['key']

    response = requests.get(r['entries'][0]['links']['content'])
    with open(f"{DUMP_PATH}{ROR_ZIP}", 'wb') as f:
        f.write(response.content)
    return ROR_ZIP                

def ror_prep(DUMP_PATH, ROR_ZIP, countries):
    from functions_shared import unzip_zip, my_country_code
    import pandas as pd
    df=unzip_zip(ROR_ZIP, DUMP_PATH, f"{ROR_ZIP.rsplit('.', 1)[0]}.json", 'utf-8')

    ror=pd.json_normalize(df)
    ror['numero_ror'] = 'R'+ror.id.str.split('/').str[-1]
    ror = ror[['numero_ror', 'name', 'types', 'links', 'aliases', 'acronyms', 'status',
        'wikipedia_url', 'addresses', 'country.country_code',
        'country.country_name']]

    print(f"vars en list: {ror.columns[ror.map(lambda x: isinstance(x, list)).any()]}")

    ror['links'] = ror['links'].apply(lambda x: ';'.join(x))
    l=['aliases', 'acronyms']
    for i in l:
        ror[i] = ror[i].apply(lambda x: ' '.join(x))

    ror['ville']=ror['addresses'].apply(lambda x:  ';'.join(map(str, [item['city'] for item in x])))
                                        
    ror = (ror[['numero_ror', 'name', 'links', 'aliases', 'acronyms',  'ville', 
                'country.country_code', 'country.country_name']]
        .rename(columns={'name':'nom_long', 
                            'acronyms':'sigle',
                        'links':'web',
                        'country.country_code':'iso2'})
        .assign(ref='ror')
        )

    ror.mask(ror=='', inplace=True)
    
    ror = (ror.merge(countries[['iso2', 'iso3']], how='left', on='iso2')
       .rename(columns={'iso3':'country_code_map'})
      )

    if any(ror.country_code_map.isnull()):
        print(ror[ror.country_code_map.isnull()][['iso2']].drop_duplicates())

    ror.mask(ror=='', inplace=True)
    print(len(ror))
    return ror
