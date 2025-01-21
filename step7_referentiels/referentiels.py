

import os, requests, json, pprint as pp, numpy as np, pandas as pd, zipfile, urllib, time, re, pycountry
from text_to_num import text2num, alpha2digit
from urllib.parse import urlparse
from IPython.display import HTML
from pathlib import Path

from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH, PATH_SOURCE
from functions_shared import unzip_zip
from config_api import *
DUMP_PATH=f'{PATH}referentiel/'

# ROR_ZIP='v1.34-2023-10-12-ror-data.zip'



cc = unzip_zip(ZIPNAME, f'{PATH_SOURCE}{FRAMEWORK}/', "countries.json", 'utf8')
cc = pd.DataFrame(cc)


url='https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/curiexplore-pays/exports/json'
response = requests.get(url, headers={"Authorization": "apikey "+ODS_API})
result=response.json()
mesr_iso=pd.json_normalize(result)[['iso3', 'iso2', 'name_en']]
mesr_iso.loc[mesr_iso.iso3=='NAM', 'iso2'] = 'NA' 


url = f"https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/countries-codes/exports/json"
response = requests.get(url)
result=response.json()
ods_iso=pd.json_normalize(result)[['iso3_code', 'iso2_code', 'label_en']].rename(columns={'iso3_code':'iso3', 'iso2_code':'iso2', 'label_en':'country_name_en'})



tmp = mesr_iso.loc[~mesr_iso.iso2.isin(ods_iso.iso2.unique())]

countries = pd.concat([ods_iso, tmp], ignore_index=True).sort_values('iso3')
countries['iso3_dup'] = countries.groupby('iso3')['iso2'].transform(lambda x: 'Y' if x.count() > 1 else 'N')
countries = countries.drop(columns='country_name_en').merge(ods_iso[['iso3', 'country_name_en']].drop_duplicates(), how='left', on='iso3')
countries.loc[countries.country_name_en.isnull(), 'country_name_en'] = countries.name_en