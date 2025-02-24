import pandas as pd, pickle, numpy as np, warnings, time, os
warnings.filterwarnings("ignore", "FutureWarning: Setting an item of incompatible dtype is deprecated and will raise an error in a future version of pandas")
pd.options.mode.copy_on_write = True
from config_path import PATH_CLEAN, PATH_API
from functions_shared import chunkify, work_csv
from step7_persons.prep_persons import persons_preparation
from step7_persons.affiliations import affiliations, persons_files_import, persons_api_simplify, persons_results_clean

CSV_DATE='20250121'
persons_preparation(CSV_DATE)

PATH_PERSONS=f"{PATH_API}persons/"
perso_part = pd.read_pickle(f"{PATH_CLEAN}persons_participants.pkl")
perso_app = pd.read_pickle(f"{PATH_CLEAN}persons_applicants.pkl")

pp = pd.concat([perso_part.drop_duplicates(), perso_app.drop_duplicates()], ignore_index=True)
pp['contact2']=pp.contact.str.replace('-', ' ')

# requests openalex
affiliations(pp, PATH_PERSONS, CSV_DATE)

oth=persons_files_import('other', PATH_PERSONS)
em=persons_files_import('erc', PATH_PERSONS)

oth=persons_api_simplify(oth)
em=persons_api_simplify(em)

oth=persons_results_clean(oth)
em=persons_results_clean(em)

lvar=['project_id', 'generalPic', 'role', 'contact',
       'title_clean', 'gender', 'email', 'tel_clean', 'domaine_email',
       'orcid_id', 'birth_country_code', 'nationality_country_code',
       'host_country_code', 'sending_country_code', 'iso2', 'stage', 'contact2',
       'country_code', 'shift', 'call_year', 'thema_code', 'destination_code',
       'entities_id', 'entities_name', 'id_secondaire', 'country_code_mapping']

mask=((pp.country_code=='FRA')|(pp.nationality_country_code=='FRA')|(pp.destination_code.isin(['COG', 'PF', 'STG', 'ADG', 'POC','SyG', 'PERA', 'SJI'])))&(~(pp.contact.isnull()&pp.orcid_id.isnull()))
df=pp.loc[mask, lvar].sort_values(['country_code','orcid_id'], ascending=False).drop_duplicates()

df=df.merge(oth, how='inner', left_on=['contact2', 'country_code'], right_on=['display_name','iso3'])
df=df[~df.astype(str).duplicated()]
df['years']=df['years'].map(lambda liste: ';'.join(str(x) for x in liste))
df['filt']=df.apply(lambda x: x['call_year'] in x['years'], axis=1)
