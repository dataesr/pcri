

import pandas as pd, pycountry
from text_to_num import text2num, alpha2digit
from urllib.parse import urlparse
from IPython.display import HTML
from pathlib import Path

from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH, PATH_SOURCE
from functions_shared import unzip_zip

# from step7_referentiels.countries import ref_countries
from step7_referentiels.ror import ror_import, ror_prep
from step7_referentiels.sirene import sirene_prep, sirene_refext
from step7_referentiels.rnsr import rnsr_import, rnsr_prep
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

if len(ror[ror.country_code_map.isnull()][['country.country_code']].drop_duplicates())>0:
    print(ror[ror.country_code_map.isnull()][['country.country_code']].drop_duplicates())
  
pays_fr = ["FR","BL","CP","GF","GP","MF","MQ","NC","PF","PM","RE","TF","WF","YT"]
ror.loc[ror['iso2'].isin(['MS', 'TC']), 'country_code'] ='GBR'
ror.loc[ror['iso2'].isin(['AX']), 'country_code'] ='FIN'



sirene_refext(DUMP_PATH) # -> sirene_ref_moulinette.pkl
sirene = sirene_prep(DUMP_PATH, countries)

### Extraction des données rnsr de dataESR

rnsr_import(DUMP_PATH)
rnsr = rnsr_prep(DUMP_PATH)

# work_csv(rnsr.loc[(rnsr.code_postal.isnull())|(rnsr.ville.isnull()), ['adresse_full', 'code_postal', 'ville']].drop_duplicates(), 'rnsr_adresse_a_completer')
# add_ad = pd.read_csv(f"{DUMP_PATH}rnsr_adresse_manquante.csv", encoding='utf-8', sep=';')
# add_ad