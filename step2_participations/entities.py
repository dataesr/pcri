from functions_shared import unzip_zip, gps_col, num_to_string
from constant_vars import FRAMEWORK, ZIPNAME
from config_path import PATH_SOURCE
import pandas as pd


def entities_missing_country(df):

    if any(df['countryCode'].isnull()):
        print(f"2 - ATTENTION missing {len(df['countryCode'].isnull())} countryCode")
        df.loc[df['countryCode'].isnull(), 'countryCode'] = df.loc[df['countryCode'].isnull(), 'countryCode_y']
        df.drop(columns='countryCode_y', inplace=True)
    if len(df.loc[df.countryCode.isnull()])>0:
        print(f"3 - ATTENTION ! missing again countryCode {df.loc[df.countryCode.isnull(), ['generalPic']].drop_duplicates()}")
    else:
        print(f'4 - RESOLU -> sans country\n- size entities with cc: {len(df)}')
    return df

def entities_load(lien):

    entities = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", "legalEntities.json", 'utf8')
    entities = pd.DataFrame(entities)
    print(f"- first size entities: {len(entities)}")
    entities = gps_col(entities)

    entities = entities.loc[~entities.generalPic.isnull()]

    c = ['pic', 'generalPic']
    entities[c] = entities[c].map(num_to_string)
    print(f"- size entities {len(entities)}")

    # app1/part + lien pour ajout cc et selection des generalPic+pic de entities
    ap=(lien.loc[lien.inProposal==True, ['generalPic', 'proposal_participant_pic', 'proposal_countryCode']]
        .drop_duplicates()
        .rename(columns={'proposal_participant_pic':'pic', 'proposal_countryCode':'countryCode'}))
    pp=(lien.loc[lien.inProject==True, ['generalPic', 'participant_pic', 'countryCode']]
        .drop_duplicates()
        .rename(columns={'participant_pic':'pic'}))
    tmp=pd.concat([ap, pp], ignore_index=True).drop_duplicates()
    print(f"- size lien ap+pp+cc (tmp): {len(tmp)}")
    entities=(tmp.merge(entities, how='left', on=['generalPic', 'pic'])
              .rename(columns={'countryCode_x': 'countryCode'})
              )
    print(f"- size tmp+entities: {len(entities)}")

    if len(tmp)!=len(entities):
        print(f"1 - ATTENTION missing generalPic into entities")

    #traietement des cc manquants dans entities à partir de cc ajouté
    entities = entities_missing_country(entities)
    print(f"- END size entities: {len(entities)}")  

    pic_no_entities = list(set(lien.generalPic.unique()) - set(entities.generalPic.unique()))
    if len(pic_no_entities) >0:
        print(f"- pic lien not in entities: {len(pic_no_entities)}")
    else:
        print("- Tous les pics de lien sont dans entities")
    return entities

def entities_cleaning(df):
    print("### ENTITIES cleaning")
    # contrôle nombre d'obs avec les pic coutry et state
    PicState=df[['generalPic', 'generalState', 'country_code_mapping']]
    n_state=PicState.groupby(['generalPic',  'country_code_mapping']).filter(lambda x: x['generalState'].count() > 1.)

    if (len(n_state)>0):
        print(f"1 - ++state pour un pic/country; régler ci-dessous {len(n_state)}")
        gen_state=['VALIDATED', 'DECLARED', 'DEPRECATED', 'SLEEPING', 'SUSPENDED', 'BLOCKED']

        if len(df.generalState.unique()) > len(gen_state):
            print(f"2 - Attention ! un generalState nouveau dans entities -> {set(df.generalState.unique())-set(gen_state)}")
        else:
            df=df.groupby(['generalPic', 'country_code_mapping']).apply(lambda x: x.sort_values('generalState', key=lambda col: pd.Categorical(col, categories=gen_state, ordered=True)), include_groups=True).reset_index(drop=True)
            df=df.groupby(['generalPic', 'country_code_mapping']).head(1)
            print(f"3 - size entities after cleaning: {len(df)}")
    df.to_pickle(f"{PATH_SOURCE}entities.pkl")
    return df

def entities_single_create(df, lien):

    print("\n### ENTITIES SINGLE")
    entities_single=df.groupby(['generalPic', 'country_code_mapping']).head(1)
    print(f"- size entities after one selection pic+cc: {len(entities_single)}")

    print(f"\n- {entities_single.generalState.value_counts()}")
    if (entities_single.generalPic.nunique())==(lien.generalPic.nunique()):
        print(f"\n1 - nombre de pics OK")
    #si pas le m^me nombre de pics entre lien et entities
    elif len(set(lien.generalPic.unique()))>len(set(df.generalPic.unique())):
        pic_lien=list(set(lien.generalPic.unique()) - set(df.generalPic.unique()))
        print(f"\n2 - pic_lien absent de entities_single {pic_lien}; faire code")

    tmp=entities_single.groupby(['generalPic', 'country_code_mapping']).filter(lambda x: x['generalPic'].count() > 1.)
    if not tmp.empty:
        print(f"1 - ATTENTION doublon generalPic revoir code ci-dessous si besoin")
           
    print(f"- size entities_single:{len(entities_single)}")
    return entities_single

def entities_info_create(entities_single, lien):
    print("\n### ENTITIES INFO")
    entities_info = (entities_single
                     .drop(['pic', 'cedex', 'countryCode_y','lastUpdateDate'], axis=1)
                     .drop_duplicates())

    if len(entities_info[['generalPic', 'country_code_mapping']].drop_duplicates())!=len(lien[['generalPic', 'country_code_mapping']].drop_duplicates()):
        print(f"1- ATTENTION ! size genPic+cc -> entities_info : {len(entities_info[['generalPic', 'country_code_mapping']].drop_duplicates())},  lien:{len(lien[['generalPic', 'country_code_mapping']].drop_duplicates())}")
    else:
        pass
    print(f"- size entities_info: {len(entities_info)}")
    return entities_info