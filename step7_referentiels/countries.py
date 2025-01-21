
def ref_countries():
    from constant_vars import ZIPNAME, FRAMEWORK
    from config_path import PATH_SOURCE
    from functions_shared import unzip_zip
    from config_api import ODS_API
    import pandas as pd, requests, pycountry


    cc = unzip_zip(ZIPNAME, f'{PATH_SOURCE}{FRAMEWORK}/', "countries.json", 'utf8')
    cc = pd.DataFrame(cc)


    url='https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/curiexplore-pays/exports/json'
    response = requests.get(url, headers={"Authorization": f"apikey {ODS_API}"})
    result=response.json()
    mesr_iso=pd.json_normalize(result)[['iso3', 'iso2', 'name_en']]
    mesr_iso.loc[mesr_iso.iso3=='NAM', 'iso2'] = 'NA' 


    url = f"https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/countries-codes/exports/json"
    response = requests.get(url)
    result=response.json()
    ods_iso=(pd.json_normalize(result)[['iso3_code', 'iso2_code', 'label_en']]
            .rename(columns={'iso3_code':'iso3', 'iso2_code':'iso2', 'label_en':'country_name_en'}))



    tmp = mesr_iso.loc[~mesr_iso.iso2.isin(ods_iso.iso2.unique())]

    countries = pd.concat([ods_iso, tmp], ignore_index=True).sort_values('iso3')
    countries['iso3_dup'] = countries.groupby('iso3')['iso2'].transform(lambda x: 'Y' if x.count() > 1 else 'N')
    countries = countries.drop(columns='country_name_en').merge(ods_iso[['iso3', 'country_name_en']].drop_duplicates(), how='left', on='iso3')
    countries.loc[countries.country_name_en.isnull(), 'country_name_en'] = countries.name_en
    return countries