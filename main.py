import pandas as pd
pd.options.mode.copy_on_write = True
from main_library import *


#################
# 1 - si nouvelle actualisation utiliser MAIN_FIRST_PROCESS
#################

NEW_UPDATE=False

#################################
# si traitement déjà effectués
### si besoin de charger les permiers traitements sns recommencer depuis le debut

projects = pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl")
participation = pd.read_pickle(f"{PATH_CLEAN}participation_current.pkl").drop(columns=['app_fund', 'part_fund', 'n_pic_cc'])
countries = pd.read_pickle(f"{PATH_CLEAN}country_current.pkl")
entities_info = pd.read_pickle(f"{PATH_CLEAN}entities_info_current2.pkl")
calls = pd.read_csv(f"{PATH_CONNECT}calls.csv", sep=";", parse_dates=['call_deadline'])

##############################################################
# provioire renommage country_source en mapping pour l'instant
for i in [ participation, entities_info]:
    i.rename(columns={'country_code_source':'country_code_mapping', 'country_name_source':'country_name_mapping'}, inplace=True)


# step4
entities_part = ent(participation, entities_info, projects)
collaboration = collab(participation, projects, countries)

# step5 - si nouvelle actualisation ou changement dans nomenclatures
h20, FP7, FP6, h20_p, FP7_p, FP6_p = framework_load()
h20 = h20.reindex(sorted(h20.columns), axis=1)

# for i in [h20, FP7, FP6, h20_p, FP7_p, FP6_p]:
#     if 'country_code_mapping' in i.columns:
#         (i.rename(columns={'country_code_mapping':'country_code_source',
#                           'country_name_mapping':'country_name_source'}, inplace=True))

if NEW_UPDATE==True:
    project_list = list(set(h20_p.project_id))+list(set(FP7_p.project_id))+list(set(FP6_p.project_id))+list(set(projects.loc[projects.stage=='successful'].project_id))
    check_proj_id(project_list)
## If TRUE, load results in OVH cloud

# step6
projects_all = projects_ods(projects, participation, calls, countries, h20_p, FP6_p, FP7_p)

collab_signed_ods(collaboration)
collaboration.drop(columns=['ecorda_date', 'abstract', 'free_keywords']).to_csv(PATH_CONNECT+"collaboration_current.csv", index=False, encoding="UTF-8", sep=";", na_rep='')
msca_collab_ods(collaboration)
msca_collab(collaboration)
# collab_evolution(collaboration)

# provisoire
h20 = h20.rename(columns={'insee_cat_code':'cat_entreprise_code', 'insee_cat_name':'cat_entreprise_name' })

entities_participation = entities_preparation(entities_part, h20)
entities_participation.to_pickle(f"{PATH_CLEAN}entities_participation_current.pkl")

mongo_bulk_insert_df(entities_participation[entities_participation['framework']=='Horizon Europe'], batch_size=10_000)

print(f"size entities_participation: {len(entities_participation)}")
entities_ods('h20', entities_participation)
entities_ods('horizon', entities_participation)

# entities_participation = entreprise_group_cleaning(entities_participation)
(entities_participation.drop(columns=['ecorda_date','action_code2','action_name2', 
                'free_keywords', 'abstract', 'acronym', 'call_deadline', 'topic_name','topic_code',
                'category_id', 'entities_name_source', 'entities_acronym_source', 
                'numero_national_de_structure', 'nutsCode', 'participation_nuts', 'participation_linked',
                'paysage_category', 'paysage_category_id', 'ror_category', 'siren_all',
                'source_id', 'numero_national_de_structure', 'structure_name'])
    .to_csv(f"{PATH_CONNECT}entities_participation_current.csv", sep=";", 
            index=False, encoding='UTF-8', na_rep='', decimal='.'))

entities_operateur(entities_participation)


entities_collab(entities_participation, tab=True)
# collab_ent = entities_collab(entities_participation, tab=False)

# df=collab_ent.loc[(collab_ent.framework=='Horizon Europe')&(collab_ent.stage=='successful')]
# df=df.loc[(df.country_code=='FRA')&(df.country_code_collab!='FRA'), 
#         ['call_year', 'project_id', 'action_code',
#        'programme_name_fr', 'thema_code', 'thema_name_fr', 'destination_code',
#        'entities_id', 'entities_name', 'entities_acronym', 
#        'entities_id_collab', 'entities_name_collab', 'entities_acronym_collab', 
#        'country_code_collab', 'country_name_fr_collab']]
# df['entities_fr'] = np.where(df.entities_acronym.isnull(), df.entities_name, df.entities_acronym)

# project_count = (
#     df.groupby(['entities_id_collab', 'country_code_collab'])['project_id']
#     .nunique()
#     .reset_index()
#     .rename(columns={'project_id': 'project_count'})
# )

# # 2. Trier par ordre décroissant du nombre unique de project_id
# project_count = project_count.sort_values(by='project_count', ascending=False)

# grouped = df.groupby([
#     'entities_id_collab',
#     'entities_name_collab',
#     'country_code_collab',
#     'country_name_fr_collab',
#     'entities_id',
#     'entities_fr',
#     'call_year'
# ])['project_id'].nunique().unstack(fill_value=0)

# # 2. Calculer le total par groupe d'index
# grouped['total'] = grouped.sum(axis=1)

# result = grouped.reset_index().merge(project_count, how='left', on=['entities_id_collab', 'country_code_collab'])



#############################
part = synthese_preparation(participation, countries)
projects_current = projects_participations(projects, part)

synthese(projects_current)

resume(projects_current)

pc = evol_preparation(FP6, FP7, h20, projects_current)
evolution_FP(pc, countries)
evolution_type(FP6, FP7, h20, projects_current)

calls_current(projects_current, calls)
calls_all = calls_all(projects)

msca_erc = msca_erc_projects(FP6, FP7, h20, projects, part)
msca_erc = msca_erc.loc[~((msca_erc.framework=='FP7')&(msca_erc.thema_code=='ERC'))]
msca_ods(msca_erc)
erc_ods(msca_erc)
me_resume = msca_erc_resume(msca_erc)
msca_evol_ods(me_resume)
erc_evol_ods(me_resume)

me_entities = msca_erc_ent(entities_participation)
msca_entities(me_entities)
erc_entities(me_entities)




##############################



# scanr_update(entities_participation)

#####################
# persons
# script persons.py
# ATTENTION ! long api requests
# revise load results