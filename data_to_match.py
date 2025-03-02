import pandas as pd, pickle
pd.options.mode.copy_on_write = True
from IPython.display import HTML

from api_requests.matcher import matcher
from step8_referentiels.referentiels import ref_externe_preparation
from step9_affiliations.prep_entities import entities_preparation

from step9_affiliations.organismes_cleaning import organismes_back


### one time
# organismes_back('2024')


# ref_externe_preparation()

# entities_preparation()
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
    persons = pd.read_pickle(f"{PATH_CLEAN}persons_current.pkl")
    print(f"size persons: {len(persons)}")
    return perso, ref_all, entities_all
perso, ref_all, entities_all, persons = data_import()

perso = perso[['project_id', 'generalPic', 'pic', 'role', 'first_name', 'last_name',
       'title', 'gender','researcher_id', 'orcid_id',
       'google_scholar_id', 'scopus_author_id', 'stage', 'nb', 'country_code',
       'call_year', 'thema_name_en', 'destination_name_en', 'tel_clean',
       'domaine_email', 'contact']].drop_duplicates()
print(f"size perso for merging: {len(perso)}")


### affil perso to struct -> search labo by openalex
entities_tmp = (
    entities_all.loc[((entities_all.country_code=='FRA')&(entities_all.rnsr_merged.str.len()==0)),
                    ['project_id','generalPic', 'pic', 'country_code', 'entities_id', 'entities_name']])
# entities_tmp=entities_all.loc[((entities_all.country_code=='FRA')&(entities_all.rnsr_merged.str.len()==0))|((entities_all.country_code!='FRA')&(entities_all.entities_id.str.contains('pic'))), ['project_id','generalPic', 'pic', 'country_code', 'entities_id', 'entities_name']]

# persons déjà traité juste faire merge avec refext et autres

### affil perso to ref_all by phone
# aff_by_tel = perso.loc[~perso.tel_clean.isnull()].merge(ref_all.loc[~ref_all.tel_clean.isnull()], how='inner', on='tel_clean')
# print(f"size aff_by_tel: {len(aff_by_tel)}")

