from functions_shared import unzip_zip, gps_col, num_to_string, entities_choose_status
from paths import PATH_CLEAN
import pandas as pd


def entities_missing_country(df):
    """
    add missing countryCode in entities from countryCode of lien, if generalPic is the same and countryCode is not null in lien    
    """
    if any(df['countryCode'].isnull()):
        print(f"2 - ⚠️ missing {len(df['countryCode'].isnull())} countryCode")
        df.loc[df['countryCode'].isnull(), 'countryCode'] = df.loc[df['countryCode'].isnull(), 'countryCode_y']
        df.drop(columns='countryCode_y', inplace=True)
    if len(df.loc[df.countryCode.isnull()])>0:
        print(f"3 - ⚠️ ! missing again countryCode {df.loc[df.countryCode.isnull(), ['generalPic']].drop_duplicates()}")
    else:
        print(f'4 - SOLVED -> without country\n- size entities with cc: {len(df)}')
    return df


def entities_load(source):
    """
    entities is is the repository of legal_entities=PIC
    load entities, unzip, clean and add gps col, keep only entities with generalPic not null, convert pic and generalPic to string
    
    """
    df = unzip_zip(source, "legalEntities.json", 'utf8')
    df = pd.DataFrame(df)
    print(f"- first size entities: {len(df)}")
    rep=[{'stage_process':'_loading', 'entities_size':len(df)}]

    df = gps_col(df)

    df = df.loc[~df.generalPic.isnull()]

    c = ['pic', 'generalPic']
    df[c] = df[c].map(num_to_string)
    print(f"- size entities {len(df)}")
    rep.append({'stage_process':'process1', 'entities_size':len(df)})
        
    # several status for pic, check if generalState is null for some pic, if yes, print and keep in mind for next step of cleaning    
    if len(df[df.generalState.isnull()])>0:
        print("- entities source generalState -> new state (processing into entities_single)")
    else:
        print("- ok entities source generalState not null")
    return df, rep


def entities_merge_partApp(df, app1, part):
    """
    link between app1/part and entities to add cc and select generalPic+pic of entities, 
    then merge with entities to keep only pic+generalPic with countryCode (cc) in entities, 
    then add missing cc in entities from cc of lien
    generalState is not null in entities, if not, print and keep in mind for next step of cleaning
    pic in lien not in entities, if yes, print and keep in mind for next step of cleaning
    """
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
              .drop(columns=['countryCode_y', 'pic'])
              )
    print(f"- size tmp+entities: {len(entities)}")
    rep.append({'stage_process':'process4_mergeEntities', 'entities_size':len(tmp)})

    if len(tmp[['generalPic', 'countryCode']].drop_duplicates())!=len(entities[['generalPic', 'countryCode']].drop_duplicates()):
        print(f"1 - ⚠️ missing generalPic into entities\ntmp={len(tmp[['generalPic', 'countryCode']].drop_duplicates())}, entities={len(entities[['generalPic', 'countryCode']].drop_duplicates())}")

    # process for missing cc in entities
    entities = entities_missing_country(entities)
    print(f"- END size entities: {len(entities)}")  
    rep.append({'stage_process':'process3_entitiesAll', 'entities_size':len(tmp)})

    # check if generalState is null for some pic, if yes, print and keep in mind for next step of cleaning
    if len(entities[entities.generalState.isnull()])>0:
        print("- entities cleaned generalState -> new state (processing into entities_single)")
    else:
        print("- ok entities cleaned generalState not null")

    pic_no_entities = list(set(tmp.generalPic.unique()) - set(entities.generalPic.unique()))
    if len(pic_no_entities) >0:
        print(f"- pic no linked to entities: {len(pic_no_entities)}")
    else:
        print("- PIC=ENTITIES")
    return entities, rep

def entities_single_create(df, lien, framework=None):
    print("### ENTITIES SINGLE")
    # contrôle nombre d'obs avec les pic coutry et state
    PicState=df[['generalPic', 'generalState', 'country_code_source']]
    n_state=PicState.groupby(['generalPic',  'country_code_source']).filter(lambda x: x['generalState'].count() > 1.)
    df['n_state'] = df.groupby(['generalPic',  'country_code_source'])['generalState'].transform('count')

    df = entities_choose_status(df, ['generalPic', 'country_code_source'])

    # if any(df['n_state']>1):
    #     print(f"1 - ++state pour un pic/country; régler ci-dessous {len(n_state)}")
    #     gen_state=['VALIDATED', 'DECLARED', 'SLEEPING', 'SUSPENDED', 'BLOCKED', 'DEPRECATED', 'Undefined']

    #     if len(df.generalState.dropna().unique()) > len(gen_state):
    #         print(f"2 - ⚠️ ! un generalState nouveau dans entities -> {set(df.generalState.unique())-set(gen_state)}")
    #     else:
    #         tmp= df[df['n_state']>1]
    #         tmp = tmp.groupby(['generalPic', 'country_code_source']).apply(lambda x: x.sort_values('generalState', key=lambda col: pd.Categorical(col, categories=gen_state, ordered=True)), include_groups=True).reset_index(drop=True)
    #         tmp = tmp.groupby(['generalPic', 'country_code_source']).head(1)
    #         print(f"3 - size entities after cleaning: {len(df)}")
        
    # df = pd.concat([df[df['n_state']==1], tmp], ignore_index=True).drop(columns='n_state')
    # print(f"- size entities_single: {len(df)}")

    print(f"\n- {df.generalState.value_counts()}")
    if (df.generalPic.nunique())==(lien.generalPic.nunique()):
        print(f"\n1 - nombre de pics OK")
    #si pas le m^me nombre de pics entre lien et entities
    elif len(set(lien.generalPic.unique()))>len(set(df.generalPic.unique())):
        pic_lien=list(set(lien.generalPic.unique()) - set(df.generalPic.unique()))
        print(f"\n2 - pic_lien absent de entities_single {pic_lien}; faire code")
        add = lien.loc[lien['generalPic'].isin(pic_lien), ['generalPic', 'country_code_source']].drop_duplicates()
        if len(add)>0:
            print(f"-pic absent de entities_single ajout de {len(add)} pic")
            df = pd.concat([df, add], ignore_index=True)
    
    file_name = f"{framework}_entities_single.pkl" if framework != None else "entities_single.pkl"
    df.to_pickle(f"{PATH_CLEAN}{file_name}")

    tmp=df.groupby(['generalPic', 'country_code_source']).filter(lambda x: x['generalPic'].count() > 1.)
    if not tmp.empty:
        print(f"1 - ⚠️ doublon generalPic revoir code ci-dessous si besoin")
           
    print(f"- size entities_single:{len(df)}")
    return df

def entities_info_create(entities_single, lien):
    print("\n### ENTITIES INFO")
    entities_info = (entities_single
                     .drop(['cedex','lastUpdateDate'], axis=1)
                     .drop_duplicates()
                     .merge(lien[['generalPic', 'country_code_source']].drop_duplicates(), 
                            how='left', 
                            on=['generalPic', 'country_code_source'],
                            indicator=True)
                    .rename(columns={'_merge':'merge_entitiesLien'})
                     )

    if len(entities_info[['generalPic', 'country_code_source']].drop_duplicates())!=len(lien[['generalPic', 'country_code_source']].drop_duplicates()):
        print(f"1- ⚠️ ! size genPic+cc -> entities_info : {len(entities_info[['generalPic', 'country_code_source']].drop_duplicates())},  lien:{len(lien[['generalPic', 'country_code_source']].drop_duplicates())}")
        print(f"2- check if genPic+cc in lien not in entities_info: {set(lien[['generalPic', 'country_code_source']].drop_duplicates().apply(tuple, axis=1)) - set(entities_info[['generalPic', 'country_code_source']].drop_duplicates().apply(tuple, axis=1))}")
    else:
        pass
    print(f"- size entities_info: {len(entities_info)}")
    return entities_info