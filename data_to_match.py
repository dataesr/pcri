import pandas as pd
pd.options.mode.copy_on_write = True
from IPython.display import HTML

from matcher import matcher
from step7_persons.persons import persons_preparation
from step8_referentiels.referentiels import ref_externe_preparation
from step9_affiliations.prep_entities import entities_preparation
from step9_affiliations.affiliations import persons_affiliation
from step9_affiliations.organismes_cleaning import organismes_back
CSV_DATE='20241011'

### one time
# organismes_back('2024')
# persons_preparation(CSV_DATE)

# ref_externe_preparation()
lab_a_ident = entities_preparation()
 ### si reprise du code en cours chargement des pickles -> entities_all
# keep = pd.read_pickle(f'{PATH}participants/data_for_matching/structure_fr.pkl')
# struct_et = pd.read_pickle(f'{PATH}participants/data_for_matching/struct_et.pkl')

def data_import():
    from config_path import PATH,  PATH_CLEAN
    perso = pd.read_pickle(f"{PATH_CLEAN}perso_app.pkl")
    print(f"size perso init: {len(perso)}")
    ref_all = pd.read_pickle("C:/Users/zfriant/OneDrive/Matching/Echanges/HORIZON/data_py/ref_all.pkl")
    print(f"size ref_all init: {len(ref_all)}")
    entities_all = pd.read_pickle(f'{PATH}participants/data_for_matching/entities_all.pkl')
    print(f"size entities_all init: {len(entities_all)}")
    return perso, ref_all, entities_all
perso, ref_all, entities_all = data_import()

perso = perso[['project_id', 'generalPic', 'pic', 'role', 'first_name', 'last_name',
       'title', 'gender','researcher_id', 'orcid_id',
       'google_scholar_id', 'scopus_author_id', 'stage', 'nb', 'country_code',
       'call_year', 'thema_name_en', 'destination_name_en', 'tel_clean',
       'domaine_email', 'contact']].drop_duplicates()
print(f"size perso for merging: {len(perso)}")


test = entities_all.merge(perso, how='left', on=['project_id', 'generalPic'])

# r2 = persons_affiliation(perso, entities_all, country='FRA')
# voire comment traiter le retour; pour l'instant ne peut plus utiliser l'api (trop de requetes)

aff_by_tel = perso.loc[~perso.tel_clean.isnull()].merge(ref_all.loc[~ref_all.tel_clean.isnull()], how='inner', on='tel_clean')
print(f"size aff_by_tel: {len(aff_by_tel)}")