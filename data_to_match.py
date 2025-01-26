import pandas as pd
pd.options.mode.copy_on_write = True
from IPython.display import HTML
# from unidecode import unidecode

from main_library import *
from matcher import matcher
from step7_persons.persons import persons_preparation
from step8_referentiels.referentiels import ref_externe_preparation
from step9_affiliations.prep_entities import entities_preparation
from step9_affiliations.affiliations import persons_affiliation
CSV_DATE='20241011'

persons_preparation(CSV_DATE)
# ref_externe_preparation()
# entities_preparation()

def data_import():
    from config_path import PATH,  PATH_CLEAN
    perso = pd.read_pickle(f"{PATH_CLEAN}perso_app.pkl")
    ref_all = pd.read_pickle("C:/Users/zfriant/OneDrive/Matching/Echanges/HORIZON/data_py/ref_all.pkl")

    entities_all = pd.read_pickle(f'{PATH}participants/data_for_matching/entities_all.pkl')
    return perso, ref_all, entities_all

perso, ref_all, entities_all = data_import()
r2 = persons_affiliation(perso, entities_all)