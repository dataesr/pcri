from functions_shared import get_gs, work_csv, unzip_zip
from remote_process.localisation_api import geonames_api
from reference_data import geoG
from paths import PATH_HARVEST, PATH_REF
import string, json, pandas as pd, requests, ast, unicodedata, re, numpy as np


def geo_subdivision():
    from iso3166_2 import Subdivisions
    iso = Subdivisions()
    rows = []
    for country_code, subdivisions in iso.all.items():
        for subdiv_code, details in subdivisions.items():
            row = {'countryCode': country_code, 'subdivCode': subdiv_code}
            row.update(details)
            rows.append(row)

    return pd.DataFrame(rows).drop(columns=['flag'])

def nuts_ref(source):
    # url = "https://gisco-services.ec.europa.eu/distribution/v2/nuts/download/ref-nuts-2021-03-31.csv"
    data = unzip_zip(source, 'eurostatNuts.json', 'utf8')
    print(f'1 - eurostatNuts -> {len(data)}')
    return pd.DataFrame(data)

def nuts_to_geoSub(source):
    sub_div = geo_subdivision()
    nuts = nuts_ref(source)

    nuts = (nuts[['nutsCode', 'nutsDescription', 'nutsLevel', 'lvl0Code']]
            .assign(temp=lambda x: x['nutsDescription'].str.lower())
            .rename(columns={'lvl0Code': 'countryCode'})
        )
    nuts.loc[nuts['countryCode']=='EL', 'countryCode'] = 'GR'
    nuts.loc[nuts['countryCode']=='UK', 'countryCode'] = 'GB'
    
    sub = (sub_div[['countryCode', 'subdivCode', 'name', 'type']]
                .assign(temp=lambda x: x['name'].str.lower())
                .rename(columns={'type': 'subLevel'})
        )
    
    x=pd.merge(nuts, sub, how='left', on=['countryCode', 'temp']).drop_duplicates()