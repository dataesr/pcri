import pandas as pd, pickle, numpy as np, warnings, time
warnings.filterwarnings("ignore", "FutureWarning: Setting an item of incompatible dtype is deprecated and will raise an error in a future version of pandas")
pd.options.mode.copy_on_write = True
from config_path import PATH_CLEAN, PATH_API
from functions_shared import chunkify, work_csv
from step7_persons.prep_persons import persons_preparation
# from step7_persons.affiliations import persons_affiliation
from api_process.openalex import get_author_from_openalex

CSV_DATE='20250121'
# persons_preparation(CSV_DATE)

PATH_PERSONS=f"{PATH_API}persons/"
perso_part = pd.read_pickle(f"{PATH_CLEAN}persons_participants.pkl")
perso_app = pd.read_pickle(f"{PATH_CLEAN}persons_applicants.pkl")

# #provisoire
# project=pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl") 
# perso_part=perso_part.merge(project[['project_id', 'stage', 'destination_code', 'thema_code']], how ='left', on=['project_id', 'stage'])
# perso_app=perso_app.merge(project[['project_id', 'stage', 'destination_code', 'thema_code']], how ='left', on=['project_id', 'stage'])


#PREPRATION data for request openalex
lvar=['contact','orcid_id','country_code', 'iso2','destination_code','thema_code','nationality_country_code']
pp = pd.concat([perso_part[lvar].drop_duplicates(), perso_app[lvar].drop_duplicates()], ignore_index=True)

mask=((pp.country_code=='FRA')|(pp.nationality_country_code=='FRA')|(pp.destination_code.isin(['COG', 'PF', 'STG', 'ADG', 'POC','SyG', 'PERA', 'SJI'])))&(~(pp.contact.isnull()&pp.orcid_id.isnull()))
pp=pp.loc[mask].sort_values(['country_code','orcid_id'], ascending=False).drop_duplicates()
print(f"size pp: {len(pp)}, info sur pp with orcid: {len(pp.loc[pp.orcid_id.isnull()])}")

def request_openalex(df, iso2):
    print(time.strftime("%H:%M:%S"))
    rlist=[]
    n = 0
    for _, row in df.iterrows():
        n=n+1
        if n % 100 == 0: 
            print(f"{n}", end=',')
        if iso2==True:
            res=get_author_from_openalex(row['orcid_id'], row['contact'], row['iso2'])
            rlist.extend(res)
        else:
            res=get_author_from_openalex(row['orcid_id'], row['contact'], '')
            rlist.extend(res)

    if n % 3000 == 0:
        with open(f'{PATH_PERSONS}persons_authors_{str(n/1000)}.pkl', 'wb') as f:
            pickle.dump(rlist, f)
    print(time.strftime("%H:%M:%S"))
    return rlist

#parola joge-a
erc_msca=pp.loc[pp.thema_code.isin(['ERC', 'MSCA']), ['contact', 'orcid_id']].drop_duplicates()
print(f"size erc_msca: {len(erc_msca)}")
em=request_openalex(erc_msca, iso2=False)
with open(f'{PATH_PERSONS}persons_authors_erc_{CSV_DATE}.pkl', 'wb') as f:
    pickle.dump(em, f)

tmp1=pp.loc[~pp.thema_code.isin(['ERC', 'MSCA']), ['contact', 'orcid_id', 'iso2']].drop_duplicates()
print(f"size erc_msca: {len(tmp1)}")
other=request_openalex(tmp1, iso2=True)
with open(f'{PATH_PERSONS}persons_authors_other_{CSV_DATE}.pkl', 'wb') as f:
    pickle.dump(other, f)

#Return openalex results
pers_api=[]
# for i in range(0, len(data_chunks)):
for i in range(0, 14):
    with open(f"{PATH_PERSONS}persons_author_{i}.pkl", 'rb') as f:
        globals()[f"pers_api{i}"] = pickle.load(f)
        if globals()[f"pers_api{i}"]==[]:
            print(f"- empty list: {globals()[f"pers_api{i}"]}")
        else:
            pers_api.extend(globals()[f"pers_api{i}"])
with open(f'{PATH_PERSONS}persons_authors_{CSV_DATE}.pkl', 'wb') as f:
    pickle.dump(pers_api, f)

# with open(f'{PATH_PERSONS}_persons_authors_{CSV_DATE}.pkl', 'rb') as f:
#     pers_api1=pickle.load(f)

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
                            'topics', 'ids_openalex','ids_scopus','ids_twitter']))

pers_api['display_name_alternatives']=pers_api['display_name_alternatives'].map(lambda x: ';'.join(filter(None, x)))
pers_api=pers_api[~pers_api.astype(str).duplicated()]

for i in ['ids_orcid', 'institution_ror']:
    pers_api.loc[~pers_api[i].isnull(), i] = pers_api.loc[~pers_api[i].isnull()][i].str.split("/").str[-1]

#provisoire
pp=pers_api[['name','orcid']].drop_duplicates().merge(pp, how='outer', left_on=['name', 'orcid'], right_on=['contact', 'orcid_id'], indicator=True).query('_merge=="right_only"').drop(columns=['name','orcid','country_code','destination_code']).drop_duplicates()