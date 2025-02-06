import pandas as pd
pd.options.mode.copy_on_write = True
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

# step4
entities_part = ent(participation, entities_info, projects)
collaboration = collab(participation, projects, countries)

# step5 - si nouvelle actualisation ou changement dans nomenclatures
h20, FP7, FP6, h20_p, FP7_p, FP6_p = framework_load()
h20 = h20.reindex(sorted(h20.columns), axis=1)

# step6
projects_all = projects_ods(projects, participation, calls, countries, h20_p, FP6_p, FP7_p)
collab_signed_ods(collaboration)
collaboration.drop(columns=['ecorda_date', 'abstract', 'free_keywords']).to_csv(PATH_CONNECT+"collaboration_current.csv", index=False, encoding="UTF-8", sep=";", na_rep='')
msca_collab_ods(collaboration)
msca_collab(collaboration)
collab_evolution(collaboration)

entities_participation = entities_preparation(entities_part, h20)
entities_participation.to_pickle(f"{PATH_CLEAN}entities_participation_current.pkl")
print(f"size entities_participation: {len(entities_participation)}")
entities_ods(entities_participation)

# entities_participation = entreprise_cat_cleaning(entities_participation)
(entreprise_cat_cleaning(entities_participation)
    .drop(columns=['ecorda_date','action_code2','action_name2','beneficiary_subv',
                'free_keywords', 'abstract', 'acronym', 'generalPic'])
    .to_csv(f"{PATH_CONNECT}entities_participation_current.csv", sep=";", 
            index=False, encoding='UTF-8', na_rep='', decimal='.'))


collab_ent = entities_collab(entities_participation)

part = synthese_preparation(participation, countries)
projects_current = projects_participations(projects, part)
synthese(projects_current)

calls_current(projects_current, calls)
calls_all = calls_all(projects)

msca_erc = msca_erc_projects(FP6, FP7, h20, projects, part)
msca_ods(msca_erc)
erc_ods(msca_erc)
me_resume = msca_erc_resume(msca_erc)
msca_evol_ods(me_resume)
erc_evol_ods(me_resume)

me_entities = msca_erc_ent(entities_participation)
msca_entities(me_entities)
erc_entities(me_entities)

resume(projects_current)
pc = evol_preparation(FP6, FP7, h20, projects_current)
evolution_FP(pc, countries)
evolution_type(FP6, FP7, h20, projects_current)