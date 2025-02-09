import pandas as pd, pickle, numpy as np
pd.options.mode.copy_on_write = True
from config_path import PATH_CLEAN, PATH_API
from functions_shared import chunkify, work_csv
from step7_persons.prep_persons import persons_preparation
from step7_persons.affiliations import persons_affiliation

CSV_DATE='20250121'
# persons_preparation(CSV_DATE)

PATH_PERSONS=f"{PATH_API}persons/"
perso_part = pd.read_pickle(f"{PATH_CLEAN}persons_participants.pkl")
perso_app = pd.read_pickle(f"{PATH_CLEAN}persons_applicants.pkl")

#provisoire
project=pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl") 
perso_part=perso_part.merge(project[['project_id', 'stage', 'destination_code', 'thema_code']], how ='left', on=['project_id', 'stage'])
perso_app=perso_app.merge(project[['project_id', 'stage', 'destination_code', 'thema_code']], how ='left', on=['project_id', 'stage'])


#PREPRATION data for request openalex
lvar=['contact','orcid_id','country_code','destination_code','thema_code','nationality_country_code']
pp = pd.concat([perso_part[lvar].drop_duplicates(), perso_app[lvar].drop_duplicates()], ignore_index=True)

mask=((pp.country_code=='FRA')|(pp.nationality_country_code=='FRA')|(pp.destination_code.isin(['COG', 'PF', 'STG', 'ADG', 'POC','SyG', 'PERA', 'SJI'])))&(~(pp.contact.isnull()&pp.orcid_id.isnull()))
pp=pp.loc[mask].sort_values(['country_code','orcid_id'], ascending=False).drop_duplicates()
print(f"size pp: {len(pp)}, info sur pp with orcid: {len(pp.loc[pp.orcid_id.isnull()])}")

#provisoire
# pp=pers_api[['name','orcid']].drop_duplicates().merge(pp, how='outer', left_on=['name', 'orcid'], right_on=['contact', 'orcid_id'], indicator=True).query('_merge=="right_only"').drop(columns=['name','orcid','country_code','destination_code']).drop_duplicates()

# pp=pp[['contact', 'orcid_id']].fillna('').drop_duplicates()
# data_chunks=list(chunkify(pp, 2000))
# for i in range(0, len(data_chunks)):
#     print(f"Loop {i}, size data_chunks: {len(data_chunks)}")
#     # print(type(data_chunks))
#     df_temp = data_chunks[i]
#     persons_affiliation(df_temp, i, PATH_PERSONS)

#Return openalex results
pers_api=[]
# for i in range(0, len(data_chunks)):
for i in range(0, 28):
    with open(f"{PATH_PERSONS}persons_author_{i}.pkl", 'rb') as f:
        globals()[f"pers_api{i}"] = pickle.load(f)
        if globals()[f"pers_api{i}"]==[]:
            print(f"- empty list: {globals()[f"pers_api{i}"]}")
        else:
            pers_api.extend(globals()[f"pers_api{i}"])
# with open(f'{PATH_PERSONS}persons_authors_{CSV_DATE}.pkl', 'wb') as f:
#     pickle.dump(pers_api, f)

pers_api=pd.json_normalize(pers_api)
pers_api=pers_api[~pers_api.astype(str).duplicated()]

# remove name null and author found by orcid but without institutions
pers_api=pers_api[(~pers_api.name.isnull())]
pers_api=pers_api[~((pers_api.match=='orcid')&(pers_api.affiliations.str.len()==0))]
pers_api=pers_api.explode('affiliations')
pers_api=pers_api.join(pd.json_normalize(pers_api['affiliations'], max_level=1))

pers_api.columns = pers_api.columns.str.replace(r"[.*_]+", '_', regex=True)
pers_api = (pers_api
            .rename(columns={'institution_display_name':'institution_name',
                            'institution_country_code':'country_code'})
            .drop(columns=['institution_type','institution_lineage','affiliations',
                            'topics','x_concepts','topics','x_concepts',
                            'ids_openalex','ids_scopus','ids_twitter']))

pers_api['display_name_alternatives']=pers_api['display_name_alternatives'].map(lambda x: ';'.join(filter(None, x)))
pers_api=pers_api[~pers_api.astype(str).duplicated()]

for i in ['ids_orcid', 'institution_ror']:
    pers_api.loc[~pers_api[i].isnull(), i] = pers_api.loc[~pers_api[i].isnull()][i].str.split("/").str[-1]

# # author_orcid = pd.read_pickle(f"C:/Users/zfriant/OneDrive/PCRI/participants/data_for_matching/persons_author_orcid.pkl")
# # author_name = pd.read_pickle(f"C:/Users/zfriant/OneDrive/PCRI/participants/data_for_matching/persons_author.pkl")


# author_orcid = author_orcid.loc[author_orcid.affiliations.str.len()>0, ['name', 'orcid', 'affiliations', 'ids.orcid']]
# author_orcid = author_orcid.explode('affiliations')
# # author_orcid['nb'] = author_orcid.groupby(['name']).transform('size')
