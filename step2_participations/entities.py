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

def entities_load():
    df = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", "legalEntities.json", 'utf8')
    df = pd.DataFrame(df)
    print(f"- first size entities: {len(df)}")
    rep=[{'stage_process':'_loading', 'entities_size':len(df)}]

    df = gps_col(df)

    df = df.loc[~df.generalPic.isnull()]

    c = ['pic', 'generalPic']
    df[c] = df[c].map(num_to_string)
    print(f"- size entities {len(df)}")
    rep.append({'stage_process':'process1', 'entities_size':len(df)})
        
    if len(df[df.generalState.isnull()])>0:
        print("- entities source generalState -> new state (processing into entities_single)")
    else:
        print("- ok entities source generalState not null")
    return df, rep

def entities_merge_partApp(df, app1, part):
    print("## Entities megre App+part")
    # app1/part + lien pour ajout cc et selection des generalPic+pic de entities
    ap=(app1[['generalPic', 'participant_pic', 'countryCode']]
        .drop_duplicates()
        .rename(columns={'participant_pic':'pic'}))
    pp=(part[['generalPic', 'participant_pic', 'countryCode']]
        .drop_duplicates()
        .rename(columns={'participant_pic':'pic'}))
    tmp=pd.concat([ap, pp], ignore_index=True).drop_duplicates()
    print(f"- size lien ap+pp+cc (tmp): {len(tmp)}")
    rep=[{'stage_process':'process2_PicAppPart', 'entities_size':len(tmp)}]

    entities = (tmp.merge(df, how='left', on=['generalPic', 'pic'], suffixes=('','_y'))
              .drop(columns='countryCode_y')
              )
    print(f"- size tmp+entities: {len(entities)}")
    rep.append({'stage_process':'process4_mergeEntities', 'entities_size':len(tmp)})

    if len(tmp[['generalPic', 'countryCode']].drop_duplicates())!=len(entities[['generalPic', 'countryCode']].drop_duplicates()):
        print(f"1 - ATTENTION missing generalPic into entities\ntmp={len(tmp[['generalPic', 'countryCode']].drop_duplicates())}, entities={len(entities[['generalPic', 'countryCode']].drop_duplicates())}")

    #traietement des cc manquants dans entities à partir de cc ajouté
    entities = entities_missing_country(entities)
    print(f"- END size entities: {len(entities)}")  
    rep.append({'stage_process':'process10_all', 'entities_size':len(tmp)})


    if len(entities[entities.generalState.isnull()])>0:
        print("- entities cleaned generalState -> new state (processing into entities_single)")
    else:
        print("- ok entities cleaned generalState not null")

    pic_no_entities = list(set(tmp.generalPic.unique()) - set(entities.generalPic.unique()))
    if len(pic_no_entities) >0:
        print(f"- pic lien not in entities: {len(pic_no_entities)}")
    else:
        print("- Tous les pics de lien sont dans entities")
    return entities, rep

def entities_single_create(df, lien):
    print("### ENTITIES SINGLE")
    # contrôle nombre d'obs avec les pic coutry et state
    PicState=df[['generalPic', 'generalState', 'country_code_mapping']]
    n_state=PicState.groupby(['generalPic',  'country_code_mapping']).filter(lambda x: x['generalState'].count() > 1.)

    if (len(n_state)>0):
        print(f"1 - ++state pour un pic/country; régler ci-dessous {len(n_state)}")
        gen_state=['VALIDATED', 'DECLARED', 'DEPRECATED', 'SLEEPING', 'SUSPENDED', 'BLOCKED']

        if len(df.generalState.dropna().unique()) > len(gen_state):
            print(f"2 - Attention ! un generalState nouveau dans entities -> {set(df.generalState.unique())-set(gen_state)}")
        else:
            df=df.groupby(['generalPic', 'country_code_mapping']).apply(lambda x: x.sort_values('generalState', key=lambda col: pd.Categorical(col, categories=gen_state, ordered=True)), include_groups=True).reset_index(drop=True)
            df=df.groupby(['generalPic', 'country_code_mapping']).head(1)
            print(f"3 - size entities after cleaning: {len(df)}")
    df.to_pickle(f"{PATH_SOURCE}entities_single.pkl")

    print(f"\n- {df.generalState.value_counts()}")
    if (df.generalPic.nunique())==(lien.generalPic.nunique()):
        print(f"\n1 - nombre de pics OK")
    #si pas le m^me nombre de pics entre lien et entities
    elif len(set(lien.generalPic.unique()))>len(set(df.generalPic.unique())):
        pic_lien=list(set(lien.generalPic.unique()) - set(df.generalPic.unique()))
        print(f"\n2 - pic_lien absent de entities_single {pic_lien}; faire code")

    tmp=df.groupby(['generalPic', 'country_code_mapping']).filter(lambda x: x['generalPic'].count() > 1.)
    if not tmp.empty:
        print(f"1 - ATTENTION doublon generalPic revoir code ci-dessous si besoin")
           
    print(f"- size entities_single:{len(df)}")
    return df

def entities_info_create(entities_single, lien):
    print("\n### ENTITIES INFO")
    entities_info = (entities_single
                     .drop(['pic', 'cedex','lastUpdateDate'], axis=1)
                     .drop_duplicates())

    if len(entities_info[['generalPic', 'country_code_mapping']].drop_duplicates())!=len(lien[['generalPic', 'country_code_mapping']].drop_duplicates()):
        print(f"1- ATTENTION ! size genPic+cc -> entities_info : {len(entities_info[['generalPic', 'country_code_mapping']].drop_duplicates())},  lien:{len(lien[['generalPic', 'country_code_mapping']].drop_duplicates())}")
    else:
        pass
    print(f"- size entities_info: {len(entities_info)}")
    return entities_info