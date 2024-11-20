from functions_shared import unzip_zip, gps_col, num_to_string
from constant_vars import FRAMEWORK, ZIPNAME
from config_path import PATH_SOURCE
import pandas as pd

def entities_missing_country(df, app1, part):
    df.loc[df.generalPic=='996567331', 'countryCode'] = 'FR'

    genCountry = pd.concat([app1[['generalPic', 'countryCode']].drop_duplicates(), part[['generalPic', 'countryCode']].drop_duplicates()], ignore_index=True)
    genCountry['nb'] = genCountry.groupby('generalPic')['countryCode'].transform('count')
    genCountry = genCountry.loc[genCountry.nb<2].drop(columns='nb')

    # traitement des codes country manquants dans participants_info
    if len(df.loc[df.countryCode.isnull()])>0:
        no_country = (df.loc[df.countryCode.isnull(), ['generalPic']]
                    .merge(genCountry[['generalPic', 'countryCode']], how='inner', on='generalPic')
                    .rename(columns={ 'countryCode':'cc'})
                    .drop_duplicates())
        print(f"2- ATTENTION ! entities_info sans code_country {len(no_country)}")
        df = df.merge(no_country, how='left', on='generalPic')
        df.loc[df.countryCode.isnull(), 'countryCode'] = df.cc
        df.drop(columns=['cedex', 'cc'], inplace=True)
        if len(df.loc[df.countryCode.isnull()])>0:
            print(f"3- ATTENTION ! il reste des sans code_country {df.loc[df.countryCode.isnull(), ['generalPic']].drop_duplicates()}")
        else:
            print('4- RESOLU -> sans country')
    return df

def entities_load(lien, app1, part):

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

    #add missing country inta entities
    entities = entities_missing_country(entities, app1, part)

    # contrôle nombre d'obs avec les pic coutry et state
    PicState=entities[['generalPic', 'generalState', 'countryCode']]
    n_state=PicState.groupby(['generalPic',  'countryCode']).filter(lambda x: x['generalState'].count() > 1.)

    if (len(n_state)>0):
        print(f"1 - ++state pour un pic/country; régler ci-dessous {len(n_state)}")
        gen_state=['VALIDATED', 'DECLARED', 'DEPRECATED', 'SLEEPING', 'SUSPENDED', 'BLOCKED']

        if len(entities.generalState.unique()) > len(gen_state):
            print(f"2 - Attention ! un generalState nouveau dans entities -> {set(entities.generalState.unique())-set(gen_state)}")
        else:
            entities=entities.groupby(['generalPic', 'countryCode']).apply(lambda x: x.sort_values('generalState', key=lambda col: pd.Categorical(col, categories=gen_state, ordered=True))).reset_index(drop=True)
            print(f"3 - size entities: {len(entities)}")

    # control country
    PicState=entities[['generalPic', 'generalState', 'countryCode']]
    n_country=PicState.groupby(['generalPic', 'generalState']).filter(lambda x: x['countryCode'].nunique() > 1.)

    if  (len(n_country)>0):
        print(f"1 - PROBLEME !!! ++country pour un pic/state {len(n_country)}")
    return entities

def entities_single(entities, lien, part, app1):
    print("\n### ENTITIES SINGLE")
    entities_single=entities.groupby(['generalPic', 'countryCode']).head(1)
    print(f"- size entities_single:{len(entities_single)}\n{entities_single.generalState.value_counts()}")
    print(f"\n- pic_unique entities_single:{entities_single.generalPic.nunique()},\n- pic_unique lien:{lien.generalPic.nunique()}")
    if len(set(lien.generalPic.unique()))>len(set(entities.generalPic.unique())):
        pic_lien=list(set(lien.generalPic.unique()) - set(entities.generalPic.unique()))
        print(f"pic_lien absent de entities_single {pic_lien}")

    tmp=entities_single.groupby(['generalPic', 'countryCode']).filter(lambda x: x['generalPic'].count() > 1.)
    if not tmp.empty:
        print(f"1 - ATTENTION doublon generalPic revoir code ci-dessous si besoin")
        # for i, row in tmp.iterrows():
        #     tmp.at[i, 'isNa']=row.isnull().values.sum()
        # tmp2=tmp.loc[tmp.groupby(["generalPic"])['isNa'].idxmin()][['generalPic', 'calculated_pic']]
        # tmp=tmp[~(tmp["generalPic"].isin(tmp2.generalPic.unique())&tmp["calculated_pic"].isin(tmp2.calculated_pic.unique()))][['generalPic', 'calculated_pic']]
        # del tmp2

        # entities_single = entities_single[~(entities_single["generalPic"].isin(tmp.generalPic.unique())&entities_single["calculated_pic"].isin(tmp.calculated_pic.unique()))]
        # print(f'- après suppression des doublons:{len(entities_single)}')
    
    # traitement des generalPic absents de entities
    temp = lien.loc[~lien.generalPic.isin(entities_single.generalPic.unique()), ['generalPic']].drop_duplicates()
    print(f"- nbre generalPic manquant dans entities: {len(temp)}")

    if len(temp)>0:
        temp1 = temp.merge(part, how='inner', on=['generalPic'])[['generalPic',  'participant_pic','name', 'countryCode', 'legalEntityTypeCode', 'isSme']]
        temp2 = (temp.loc[~temp.generalPic.isin(temp1.generalPic.unique())]
                .merge(app1[['generalPic', 'participant_pic', 'name', 'countryCode', 'legalEntityTypeCode', 'isSme']] ,
                        how='inner', on=['generalPic']))
        temp = pd.concat([temp1, temp2], ignore_index=True)
        temp = temp.drop_duplicates(subset=['generalPic','countryCode'], keep="first")
        temp = temp.rename(columns={'participant_pic':'calculated_pic', 'name':'legalName'})

        entities_single = pd.concat([temp, entities_single], ignore_index=True)
        
    print(f"- longueur entities_single:{len(entities_single)}, nb de generalPic unique de lien:{Nlien_genPic_single}")
    return entities_single

def entities_info(entities_single, lien):
    print("\n### ENTITIES INFO")
    entities_info = (entities_single
                     .drop(['pic'], axis=1)
                     .merge(lien[['generalPic']], how="inner", on=['generalPic'])
                     .drop_duplicates())

    if len(entities_info)!=len(lien['generalPic'].unique()):
        print(f"1- ATTENTION ! longueur finale de entities_info : {len(entities_info)}, longueur lien:{lien['generalPic'].nunique()}")
    else:
        pass
    print(f"- size entities_info: {len(entities_info)}")
    return entities_info
    