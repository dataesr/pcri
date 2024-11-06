import pandas as pd
from main_library import *

#################
# 1 - si nouvelle actualisation utiliser MAIN_FIRST_PROCESS
#################

# si traitement déjà effectués
### si besoin de charger les permiers traitements sns recommencer depuis le debut
        
projects = pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl")         
participation = pd.read_pickle(f"{PATH_CLEAN}participation_current.pkl") 
countries = pd.read_pickle(f"{PATH_CLEAN}country_current.pkl") 
entities_info = pd.read_pickle(f"{PATH_CLEAN}entities_info_current2.pkl") 
calls = pd.read_csv(f"{PATH_CONNECT}calls.csv", sep=";", parse_dates=['call_deadline'])

entities_part = ent(participation, entities_info, projects)
collaboration = collab(participation, projects, countries)

#step5 - si nouvelle actualisation ou changement dans nomenclatures
h20, FP7, FP6, h20_p, FP7_p, FP6_p = framework_load()
h20 = h20.reindex(sorted(h20.columns), axis=1)

projects_all = projects_ods(projects, participation, calls, countries, h20_p, FP6_p, FP7_p)
collab_signed_ods(collaboration)
msca_collab_ods(collaboration)
msca_collab(collaboration)