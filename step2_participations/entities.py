from functions_shared import unzip_zip, gps_col, num_to_string
from constant_vars import FRAMEWORK, ZIPNAME
from config_path import PATH_SOURCE
import pandas as pd

def entities_load(lien):
    print("### LOADING ENTITIES")
    entities = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", "legalEntities.json", 'utf8')
    entities = pd.DataFrame(entities)
    entities = gps_col(entities)
    
    entities = entities.loc[~entities.generalPic.isnull()]
    
    c = ['pic', 'generalPic']
    entities[c] = entities[c].map(num_to_string)
    print(f"size entities {len(entities)}")

    # selection des obs de entities liées aux participants/applicants
    lien_genCalcPic = lien[['generalPic', 'calculated_pic']].drop_duplicates()
    entities = lien_genCalcPic.merge(entities, how='inner', left_on=['generalPic','calculated_pic'], right_on=['generalPic','pic']).drop_duplicates()
    
    if entities is not None:
        Nlien_genPic_single = lien['generalPic'].nunique() 
        Nlien_genCalcPic = len(lien_genCalcPic)
        Nentities_genPic_single = entities['generalPic'].nunique()
        print(f"1 - nb generalPic +calc_pic dans lien:{Nlien_genCalcPic}, nb genPic unique dans lien:{Nlien_genPic_single}, nb genPic unique dans entities:{Nentities_genPic_single}")
        
        if Nlien_genPic_single != Nentities_genPic_single:
            print(f"2 - si Nlien_genPic_single dans lien diff de Nentities_genPic_single dans entities\n \n-> {lien.loc[~lien.generalPic.isin(entities.generalPic.unique()), ['generalPic']].nunique()}")
        
        # contrôle nombre d'obs avec les pic coutry et state
        genPicState = entities[['generalPic', 'pic', 'generalState', 'countryCode']]
        n_country=genPicState.groupby(['generalPic', 'pic']).filter(lambda x: x['countryCode'].nunique() > 1.)
        n_state=genPicState.groupby(['generalPic', 'pic', 'countryCode']).filter(lambda x: x['generalState'].count() > 1.)
        if (len(n_state)>0) | (len(n_country)>0):
            print(f'3 - attention il y a des doublons à traiter au niveau des entities : ++state {len(n_state)}, ++country {len(n_country)}')

        lien_genCalcPic = lien[['generalPic', 'calculated_pic']].drop_duplicates()
        entitiesCountry = entities.groupby(['generalPic', 'generalState']).filter(lambda x: x['countryCode'].nunique() > 1.)
        if len(entitiesCountry)>0:
            print(f'4 - attention ++ pays pour un genPic+state {len(entitiesCountry)}')
        else:
            gen_state = ['VALIDATED', 'DECLARED', 'DEPRECATED', 'SLEEPING', 'SUSPENDED', 'BLOCKED']

            if len(entities.generalState.unique()) > len(gen_state):
                print(f"5 - Attention ! un generalState nouveau dans entities -> ajout à gen_state")
            else:
                entities=entities.groupby(['generalPic']).apply(lambda x: x.sort_values('generalState', key=lambda col: pd.Categorical(col, categories=gen_state, ordered=True))).reset_index(drop=True)
        print(f"size entities: {len(entities)}")
        return entities


def entities_single(entities, lien, part, app1):
    print("### ENTITIES SINGLE")
    Nlien_genPic_single = lien['generalPic'].nunique()
    entities_single=entities.groupby(['generalPic']).head(1)
    print(entities_single.generalState.value_counts())
    print(f"7 - longueur de entities_single:{len(entities_single)},\n- nb generalPic unique de entities_single:{len(entities_single.generalPic.unique())},\n- nb de generalPic unique de lien:{Nlien_genPic_single}")
    
    
    tmp=entities_single.groupby(['generalPic']).filter(lambda x: x['generalPic'].count() > 1.)
    if not tmp.empty:
        for i, row in tmp.iterrows():
            tmp.at[i, 'isNa']=row.isnull().values.sum()
        tmp2=tmp.loc[tmp.groupby(["generalPic"])['isNa'].idxmin()][['generalPic', 'calculated_pic']]
        tmp=tmp[~(tmp["generalPic"].isin(tmp2.generalPic.unique())&tmp["calculated_pic"].isin(tmp2.calculated_pic.unique()))][['generalPic', 'calculated_pic']]
        del tmp2

    entities_single = entities_single[~(entities_single["generalPic"].isin(tmp.generalPic.unique())&entities_single["calculated_pic"].isin(tmp.calculated_pic.unique()))]
    print(f'8 - après suppression des doublons:{len(entities_single)}')
    
    # traitement des generalPic absents de entities
    temp = lien.loc[~lien.generalPic.isin(entities_single.generalPic.unique()), ['generalPic']].drop_duplicates()
    print(f"nbre generalPic manquant dans entities: {len(temp)}")

    if len(temp)>0:
        temp1 = temp.merge(part, how='inner', on=['generalPic'])[['generalPic',  'participant_pic','name', 'countryCode', 'legalEntityTypeCode', 'isSme']]
        temp2 = (temp.loc[~temp.generalPic.isin(temp1.generalPic.unique())]
                .merge(app1[['generalPic', 'participant_pic', 'name', 'countryCode', 'legalEntityTypeCode', 'isSme']] ,
                        how='inner', on=['generalPic']))
        temp = pd.concat([temp1, temp2], ignore_index=True)
        temp = temp.drop_duplicates(subset=['generalPic','countryCode'], keep="first")
        temp = temp.rename(columns={'participant_pic':'calculated_pic', 'name':'legalName'})

        entities_single = pd.concat([temp, entities_single], ignore_index=True)
        
    print(f"longueur entities_single:{len(entities_single)}, nb de generalPic unique de lien:{Nlien_genPic_single}")
    return entities_single


def entities_info(entities_single, lien, app1, part):
    print("### ENTITIES INFO")
    entities_info = (entities_single
                     .drop(['pic', 'calculated_pic'], axis=1)
                     .merge(lien[['generalPic']], how="inner", on=['generalPic'])
                     .drop_duplicates())

    if len(entities_info)!=len(lien['generalPic'].unique()):
        print(f"ATTENTION ! longueur finale de entities_info : {len(entities_info)}, longueur lien:{lien['generalPic'].nunique()}")
    else:
        pass

    entities_info.loc[entities_info.generalPic=='996567331', 'countryCode'] = 'FR'

    genCountry = pd.concat([app1[['generalPic', 'countryCode']].drop_duplicates(), part[['generalPic', 'countryCode']].drop_duplicates()], ignore_index=True)
    genCountry['nb'] = genCountry.groupby('generalPic')['countryCode'].transform('count')
    genCountry = genCountry.loc[genCountry.nb<2].drop(columns='nb')

    # traitement des codes country manquants dans participants_info
    if len(entities_info.loc[entities_info.countryCode.isnull()])>0:
        no_country = (entities_info.loc[entities_info.countryCode.isnull(), ['generalPic']]
                    .merge(genCountry[['generalPic', 'countryCode']], how='inner', on='generalPic')
                    .rename(columns={ 'countryCode':'cc'})
                    .drop_duplicates())
        print(f"ATTENTION ! entities_info sans code_country {len(no_country)}")
        entities_info = entities_info.merge(no_country, how='left', on='generalPic')
        entities_info.loc[entities_info.countryCode.isnull(), 'countryCode'] = entities_info.cc
        entities_info = entities_info.drop(columns=['cedex', 'cc'])
        if len(entities_info.loc[entities_info.countryCode.isnull()])>0:
            print(f"ATTENTION ! il reste des sans code_country {entities_info.loc[entities_info.countryCode.isnull(), ['generalPic']].drop_duplicates()}")
        else:
            print('RESOLU -> sans country')
    return entities_info
    