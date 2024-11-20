from functions_shared import unzip_zip, gps_col, num_to_string
from constant_vars import FRAMEWORK, ZIPNAME
from config_path import PATH_SOURCE, PATH_CLEAN
import pandas as pd

lien=pd.read_pickle(f"{PATH_CLEAN}lien.pkl")
entities = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", "legalEntities.json", 'utf8')
entities = pd.DataFrame(entities)
print(f"- first size entities: {len(entities)}")
entities = gps_col(entities)

entities = entities.loc[~entities.generalPic.isnull()]

c = ['pic', 'generalPic']
entities[c] = entities[c].map(num_to_string)
print(f"- size entities {len(entities)}")

# selection des obs de entities liées aux participants/applicants
entities = entities.loc[entities.generalPic.isin(lien.generalPic.unique())]
print(f"- new size entities: {len(entities)}")  

pic_no_entities = list(set(lien.generalPic.unique()) - set(entities.generalPic.unique()))
if len(pic_no_entities) >0:
    print(f"- pic lien not in entities: {len(pic_no_entities)}")

# contrôle nombre d'obs avec les pic coutry et state
PicState = entities[['generalPic', 'generalState', 'countryCode']]
n_state=PicState.groupby(['generalPic',  'countryCode']).filter(lambda x: x['generalState'].count() > 1.)

if (len(n_state)>0):
    print(f"1 - ++state pour un pic/country; régler ci-dessous {len(n_state)}")
    gen_state = ['VALIDATED', 'DECLARED', 'DEPRECATED', 'SLEEPING', 'SUSPENDED', 'BLOCKED']

    if len(entities.generalState.unique()) > len(gen_state):
        print(f"2 - Attention ! un generalState nouveau dans entities -> {set(entities.generalState.unique())-set(gen_state)}")
    else:
        entities=entities.groupby(['generalPic', 'countryCode']).apply(lambda x: x.sort_values('generalState', key=lambda col: pd.Categorical(col, categories=gen_state, ordered=True))).reset_index(drop=True)
    print(f"3 - size entities: {len(entities)}")

# control country
PicState = entities[['generalPic', 'generalState', 'countryCode']]
n_country=PicState.groupby(['generalPic', 'generalState']).filter(lambda x: x['countryCode'].nunique() > 1.)

if  (len(n_country)>0):
    print(f"1 - PROBLEME !!! ++country pour un pic/state {len(n_country)}")

#############################
entities_single=entities.groupby(['generalPic', 'countryCode']).head(1)
print(entities_single.generalState.value_counts())
print(f"- size entities_single:{len(entities_single)},\n- pic_unique entities_single:{entities_single.generalPic.nunique()},\n- pic_unique lien:{lien.generalPic.nunique()}")
if len(set(lien.generalPic.unique())):
pic_no_entities = list(set(lien.generalPic.unique()) - set(entities.generalPic.unique()))