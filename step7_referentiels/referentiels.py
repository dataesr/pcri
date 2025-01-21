

import os, requests, json, pprint as pp, numpy as np, pandas as pd, zipfile, urllib, time, re, pycountry
from text_to_num import text2num, alpha2digit
from urllib.parse import urlparse
from IPython.display import HTML
from pathlib import Path

from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH, PATH_SOURCE
from functions_shared import unzip_zip
from config_api import *
# from step7_referentiels.countries import ref_countries
from step7_referentiels.ror import ror_import, ror_prep
from step7_referentiels.sirene import sirene_prep, sirene_refext
DUMP_PATH=f'{PATH}referentiel/'


pycountry.countries.add_entry(alpha_2="XK", alpha_3="XXK", name="Kosovo")
pycountry.countries.add_entry(alpha_2="UK", alpha_3="GBR", name="United Kingdom")
pycountry.countries.add_entry(alpha_2="EL", alpha_3="GRC", name="Greece")
tmp = [c.__dict__['_fields'] for c in list(pycountry.countries)]
countries = (pd.DataFrame(tmp)[['alpha_2', 'alpha_3', 'name']]
            .rename(columns={'alpha_2':'iso2', 'alpha_3':'iso3', 'name':'country_name_en'})
            .drop_duplicates()
)
print(len(countries))

ROR_ZIPNAME = ror_import(DUMP_PATH)
ror = ror_prep(DUMP_PATH, ROR_ZIPNAME, countries)

sirene = sirene_refext()
sirene = sirene_prep(DUMP_PATH, countries)