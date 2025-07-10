# table topics'''

from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE, PATH_CLEAN
from functions_shared import unzip_zip
import pandas as pd, numpy as np, json, re

def topics_divisions(chemin):
    print("### TOPICS")
    data = unzip_zip(ZIPNAME, chemin, 'topics.json', 'utf8')
    print(f'1 - topics -> {len(data)}')
    topics = pd.DataFrame(data)[["topicCode","topicDescription"]].drop_duplicates() 

    # DIVISIONS 
    data = unzip_zip(ZIPNAME, chemin, 'topicLbDivisions.json', 'utf8')
    print(f'2 - divisions -> {len(data)}')

    df = pd.DataFrame(data).drop(['lastUpdateDate'], axis=1)
    df['tmp'] = np.where(df.isPrincipal == True, 1 , 0)
    table = pd.pivot_table(df,index=['topicCode'],columns=['divisionAbbreviation'],values=['tmp'],aggfunc=pd.Series.nunique,margins=True,dropna=True)
    if [table['tmp']['All']>1]==True:
        pd.DataFrame(table).to_csv("/he_data/traitement_topic_horizon.csv", sep=";", encoding="utf-8", na_rep="")
        print('3 - verifier les doublons topic/division isPrincipal dans he_data/traitement_topic_horizon.csv')

    df = df[df.isPrincipal == True]
    df = df.dropna(axis=1, how='all').drop(['lvl1Code','lvl1Description','isPrincipal','tmp'], axis=1).drop_duplicates()

    topics_divisions = df.merge(topics, how='left', on='topicCode')

    divisions = df[['lvl2Code', 'lvl2Description', 'lvl3Code', 'lvl3Description', 'lvl4Code', 'lvl4Description']].drop_duplicates()

    # add niveau prog pilier
    horizon = (topics_divisions
               .rename(columns={"lvl2Code": "pilier_code", "lvl2Description": "pilier_name_en", "lvl3Code": "programme_code", 
                                "lvl3Description": "programme_name_en",'topicDescription': 'topic_name'})
              .drop(columns=['lvl4Code','lvl4Description', 'divisionAbbreviation', 'divisionDescription', 'framework'])) 

    destination = pd.read_json(open('data_files/destination.json', 'r+', encoding='utf-8'))
    destination = pd.DataFrame(destination)

    ########################################################

    #ERC
    ERC = topics_divisions.loc[topics_divisions['lvl3Code']=="HORIZON.1.1", ['topicCode']].assign(thema_code='ERC')
    typ = ["POC", "COG", "STG", "ADG", "PERA", "SyG", "SJI"]

    for i in typ:
        ERC.loc[ERC.topicCode.str.contains(i), 'destination_code'] = i

    if any(pd.isna(ERC.destination_code.unique())):
        print(f'erc : destination_code à null après traitement\n{ERC[ERC.destination_code.isnull()].topicCode.unique()}')
        ERC.loc[ERC.destination_code.isnull(), 'destination_code'] = 'ERC-OTHER'

    ############################################################################
    # MSCA
    MSCA = topics_divisions.loc[topics_divisions['lvl3Code']=="HORIZON.1.2", ['topicCode']].assign(thema_code='MSCA')
    typ = ["COFUND", "SE", "PF", "DN","CITIZENS"]

    for i in typ:
        MSCA.loc[MSCA.topicCode.str.contains(i), 'destination_code'] = i

    if any(pd.isna(MSCA.destination_code.unique())):
        print(f'MSCA : destination_code à null après traitement\n{MSCA[MSCA.destination_code.isnull()].topicCode.unique()}')
        MSCA.loc[MSCA.destination_code.isnull(), 'destination_code'] = 'MSCA-OTHER'  

    #######################################################################################################""
    #INFRA
    INFRA = topics_divisions.loc[topics_divisions['lvl3Code']=="HORIZON.1.3", ['topicCode']].assign(thema_code='INFRA')
    inf={'EOSC':'INFRAEOSC',
    'DEV':'INFRADEV',
    'SERV':'INFRASERV',
    'TECH':'INFRATECH',
    '-NET-':'INFRANET'
    }

    for k,v in inf.items():
        INFRA.loc[INFRA.topicCode.str.contains(k), 'destination_code'] = v
    if any(pd.isna(INFRA.destination_code.unique())):
        print(f'INFRA : destination_code à null après traitement\n{INFRA[INFRA.destination_code.isnull()].topicCode.unique()}')
        INFRA.loc[INFRA.destination_code.isnull(), 'destination_code'] = 'DESTINATION-OTHER'

    # ####################################################################################
    # # CLUSTER

    CLUSTER = topics_divisions.loc[(topics_divisions.lvl2Code=='HORIZON.2')&(topics_divisions.topicCode.str.contains('-CL\\d{1}-|-HLTH-', regex=True))]
    CLUSTER = CLUSTER[['topicCode']]

    CLUSTER['destination_code'] = CLUSTER['topicCode'].str.split('-').str.get(3)
    CLUSTER.loc[~CLUSTER.destination_code.isin(destination.destination_code.unique()), 'destination_code'] = np.nan

    cl={'-HLTH-':'HEALTH-OTHER',
    '-CL2-':'CCSI-OTHER',
    '-CL3-':'CSS-OTHER',
    '-CL4-':'DIS-OTHER',
    '-CL5-':'CEM-OTHER',
    '-CL6-':'BIOENV-OTHER'
    }
    for k,v in cl.items():
        CLUSTER.loc[(CLUSTER.destination_code.isnull())&(CLUSTER.topicCode.str.contains(k)), 'destination_code'] = v
    if any(pd.isna(CLUSTER.destination_code.unique())):
        print('CLUSTER : attention encore destination_code à null après traitement')

    CLUSTER['temp']=CLUSTER.topicCode.str.split('-').str.get(1)
    l_cluster=pd.DataFrame.from_dict({'HLTH':'CLUSTER 1', 'CL2':'CLUSTER 2', 'CL3':'CLUSTER 3', 'CL4':'CLUSTER 4','CL5':'CLUSTER 5', 'CL6':'CLUSTER 6'}, orient='index', columns=['thema_code']).reset_index()
    CLUSTER = CLUSTER.merge(l_cluster, how='left', left_on='temp', right_on='index').drop(columns=['index', 'temp'])

    mask=(CLUSTER.thema_code=='CLUSTER 4')
    CLUSTER.loc[mask&(CLUSTER.destination_code.isin(['RESILIENCE','TWIN'])), 'thema_code'] = CLUSTER.thema_code+'-Industry'
    CLUSTER.loc[mask&(CLUSTER.destination_code.isin(['DATA','DIGITAL','HUMAN'])), 'thema_code'] = CLUSTER.thema_code+'-Digital'
    CLUSTER.loc[mask&(CLUSTER.destination_code.isin(['SPACE'])), 'thema_code'] = CLUSTER.thema_code+'-Space'

    mask=(CLUSTER.thema_code=='CLUSTER 5')
    CLUSTER.loc[mask&(CLUSTER.destination_code.isin(['D'+str(i) for i in range(1, 2)])), 'thema_code'] = CLUSTER.thema_code+'-Climate'
    CLUSTER.loc[mask&(CLUSTER.destination_code.isin(['D'+str(i) for i in range(2, 5)])), 'thema_code'] = CLUSTER.thema_code+'-Energy'
    CLUSTER.loc[mask&(CLUSTER.destination_code.isin(['D'+str(i) for i in range(5, 7)])), 'thema_code'] = CLUSTER.thema_code+'-Mobility'


    ################################################################
    #### autres pilier 2
    # MISSION
    miss = (topics_divisions
        .loc[(topics_divisions.lvl2Code=='HORIZON.2')&(topics_divisions.topicCode.str.contains('MISS')),
                ['topicCode','lvl3Code']]
        .assign(programme_code='MISSION'))

    m=["OCEAN",
        "SOIL",
        "CIT",
        "CLIMA",
        "CANCER",
        "UNCAN"]


    for k in m:
        pattern=str("^"+k)
        mask = (miss.topicCode.str.split('-').str[3].str.contains(pattern, na=True))
        miss.loc[mask, 'thema_code'] = k
        # miss.loc[mask, 'programme_code'] = v

    miss.loc[miss.thema_code=="UNCAN", 'thema_code'] = "CANCER"

    if any((miss.programme_code=='MISSION')&(miss.thema_code.isnull())):
        miss.loc[miss.thema_code.isnull(), 'thema_code'] = 'MISS-OTHER'  
        # miss.loc[miss.programme_code.isnull(), 'programme_code'] = miss.lvl3Code 
    # traitement niveau programme pour les MISSIONS
    miss = (miss
        .merge(horizon[['pilier_code', 'pilier_name_en', 'topicCode', 'topic_name']], how='left', on='topicCode')
        .assign(programme_name_en='Mission')
        .drop_duplicates())
    ########################################################################

    # JU-JTI
    spec={
    'CHIPS':'JU-JTI',
    'JU':'JU-JTI',
    'JTI':'JU-JTI',
    'EUSPA':'EUSPA',
    'SESAR':'JU-JTI'
    }

    # certains call n'ont pas JU/JTI dans le libellé
    top = topics_divisions.loc[(topics_divisions.lvl2Code=='HORIZON.2')&(topics_divisions.topicCode.str.upper().str.contains('|'.join([*spec]))), ['topicCode']]

    for k,v in spec.items():
        top.loc[top.topicCode.str.upper().str.contains(k), 'thema_code'] = v
    if any(pd.isna(top.thema_code.unique())):
        print('top_hor2 : thema_code à null après traitement')

    top = top.assign(destination_code=np.nan)
    for i in ['SESAR', 'CLEAN-AVIATION', 'IHI', 'KDT', 'CBE', 'EDCTP3', 'EUROHPC', 'SNS', 'ER', 'CHIPS']:  
        pattern=str(i.upper()+"-")
        mask = (top.thema_code=='JU-JTI')&(top.destination_code.isnull())&(top.topicCode.str.upper().str.contains(pattern))
        top.loc[mask, 'destination_code'] = i

    for i in ['CLEANH2']:  
        pattern=str("-"+i+"-")
        mask = (top.thema_code=='JU-JTI')&(top.destination_code.isnull())&(top.topicCode.str.contains(pattern))
        top.loc[mask, 'destination_code'] = i

    top.loc[top.destination_code=='KDT', 'destination_code'] = 'CHIPS'    
    top.loc[top.destination_code=='ER', 'destination_code'] = 'EU-RAIL'  
    top.loc[top.thema_code=='EUSPA', 'destination_code'] = 'EUSPA'


    if any(pd.isna(top.destination_code.unique())):
        print(f'top_hor2 : destination_code à null après traitement\n{top[top.destination_code.isnull()]}')
        top.loc[top.destination_code.isnull(), 'destination_code'] = 'DESTINATION-OTHER'

    # #############################################################################################################
    # horizon 3
    HOR3 = topics_divisions.loc[topics_divisions.lvl2Code=='HORIZON.3', ['topicCode', 'lvl3Code']]

    spec={'PATHFINDER':'PATHFINDER',
    'TRANSITION':'TRANSITION',
    'ACCELERATOR':'ACCELERATOR',  
    'CONNECT':'CONNECT',
    'SCALEUP':'SCALEUP',
    'INNOVSMES':'INNOVSMES',   
    'CLIMATE':'KIC-CLIMATE',
    'DIGITAL':'KIC-DIGITAL',
    'HEALTH':'KIC-HEALTH',
    'FOOD':'KIC-FOOD',
    'MANUFACTURING':'KIC-MANUFACTURING',
    'URBANMOBILITY':'KIC-URBANMOBILITY',
    'RAWMATERIALS':'KIC-RAWMATERIALS',
    'INNOENERGY':'KIC-INNOENERGY',
    'CCSI':'KIC-CCSI',
    'PRIZE':'PRIZE',
    'EITWOMENLEADERSHIP':'PRIZE'
    }

    for k,v in spec.items():
        HOR3.loc[HOR3.topicCode.str.upper().str.contains(k), 'thema_code'] = v   
    if any(pd.isna(HOR3.thema_code.unique())):
        print(f"HOR3 : thema_code à null après traitement\n{HOR3[HOR3.thema_code.isnull()].sort_values('topicCode').topicCode.unique()}")
        HOR3.loc[(HOR3.lvl3Code=='HORIZON.3.1')&(HOR3.thema_code.isnull()), 'thema_code'] = 'EIC-OTHER'
        HOR3.loc[(HOR3.lvl3Code=='HORIZON.3.2')&(HOR3.thema_code.isnull()), 'thema_code'] = 'EIE-OTHER'
        HOR3.loc[(HOR3.lvl3Code=='HORIZON.3.3')&(HOR3.thema_code.isnull()), 'thema_code'] = 'EIT-OTHER'
        

    spec={
    'CHALLENGE':'CHALLENGES',
    'OPEN':'OPEN',
    'EITWOMENLEADERSHIP':'EPWI',
    'RISINGINNOVATOR':'EPWI',
    'WOMENINNOVATOR':'EPWI',   
    'EPWI':'EPWI',
    'INNOVATIONPROCUREMENT':'EUIPA',
    'EIPA':'EUIPA',
    'EUIPA':'EUIPA',
    'EUSIC':'EUSIC',
    'SOCIALINNOVATION':'EUSIC',
    'HUMANITARIAN':'HUMANITARIAN',
    'ICAPITAL':'ICAPITAL'
    }
    for k,v in spec.items():
        HOR3.loc[(HOR3.thema_code=='PRIZE')&(HOR3.topicCode.str.upper().str.contains(k)), 'destination_code'] = v
        HOR3.loc[(HOR3.destination_code.isnull())&(HOR3.topicCode.str.upper().str.contains(k)), 'destination_code'] = v
    if any(pd.isna(HOR3.destination_code.unique())):
        HOR3.loc[(HOR3.lvl3Code=='HORIZON.3.1')&(HOR3.destination_code.isnull()), 'destination_code'] = 'DESTINATION-OTHER'
        HOR3.loc[(HOR3.destination_code.isnull()), 'destination_code'] = HOR3.thema_code
     #####################################################################################

    # horizon 4
    HOR4 = topics_divisions.loc[topics_divisions.lvl2Code=='HORIZON.4', ['topicCode', 'lvl3Code']]
    spec={
    'ACCESS':'ACCESS',
    'TALENTS':'TALENTS',
    'TECH':'INFRATECH',
    'COST':'COST',
    'GENDER':'GENDER',
    '-ERA-':'ERA',
    'PRIZE':'PRIZE',
    'EURATOM':'EURATOM'
    }

    for k,v in spec.items():
        HOR4.loc[HOR4.topicCode.str.upper().str.contains(k), 'thema_code'] = v
    if any(pd.isna(HOR4.thema_code.unique())):
        print(f"HOR4 : thema_code à null après traitement\n{HOR4[HOR4.thema_code.isnull()].sort_values('topicCode').topicCode.unique()}")
        HOR4.loc[(HOR4.lvl3Code=='HORIZON.4.1')&(HOR4.thema_code.isnull()), 'thema_code'] = 'WIDENING-OTHER'
        HOR4.loc[(HOR4.lvl3Code=='HORIZON.4.2')&(HOR4.thema_code.isnull()), 'thema_code'] = 'ERA-OTHER'
    
    spec={
    'GENDER':'GENDER',
    'IMPACT':'IMPACT'
    }
    for k,v in spec.items():
        HOR4.loc[(HOR4.thema_code=='PRIZE')&(HOR4.topicCode.str.upper().str.contains(k)), 'destination_code'] = v
    
    HOR4.loc[(HOR4.destination_code.isnull()), 'destination_code'] = HOR4.thema_code 
    ##############################################################################

    #traitement des programmes hors mission
    tab = pd.concat([HOR3, HOR4, CLUSTER, top, INFRA, ERC, MSCA], ignore_index=True)
    tab = tab.merge(horizon, how='inner', on='topicCode')
    tab = tab.mask(tab == '')
    
    #add mission
    tab = pd.concat([tab, miss], ignore_index=True).drop(columns=['lvl3Code'])
    tab = tab.mask(tab == '')

    # traitement thema_code -> null
    reste = horizon.loc[~horizon.topicCode.isin(tab.topicCode.unique())]
    tab = pd.concat([tab, reste], ignore_index=True)

    tab.loc[tab.destination_code.isnull(), 'destination_code'] = tab.thema_code
    tab.loc[tab.thema_code.isnull(), 'thema_code'] = 'THEMA-OTHER'
    tab.loc[tab.destination_code.isnull(), 'destination_code'] = 'DESTINATION-OTHER'

    thema_lib = pd.read_json(open('data_files/thema.json', 'r+', encoding='utf-8'))
    thema_lib = pd.DataFrame(thema_lib)
    tab = tab.merge(thema_lib, how='left', on='thema_code').drop(columns='dest_h20')
    # tab.loc[tab.thema_name_en.isnull(), 'thema_name_en'] = tab.programme_name_en
    tab = tab.merge(destination, how='left', on='destination_code')
    tab = tab.mask(tab == '')

    tab.loc[(tab.destination_name_en.isnull())&(tab.thema_code==tab.destination_code), 'destination_name_en'] = tab.thema_name_en

    data = pd.read_json(open('data_files/programme_fr.json', 'r+', encoding='utf-8'))
    data=pd.DataFrame(data)

    tab = tab.merge(data[['programme_code','programme_name_fr']], how='left',on='programme_code')
    tab = tab.merge(data[['pilier_code','pilier_name_fr']], how='left', on='pilier_code')

    tab.loc[(tab.thema_name_fr.isnull()), 'thema_name_fr'] = tab['programme_name_fr']

    if not tab.columns[tab.isnull().any()].empty:
        print(f"- attention des cellules sont vides dans tab: {tab.columns[tab.isnull().any()]}")


    for i in tab.columns:
        if tab[i].dtype == 'object':
            tab[i] = tab[i].map(str.strip, na_action='ignore')
        else:
            pass
    return tab

def merged_topics(df):
    topics = topics_divisions(f"{PATH_SOURCE}{FRAMEWORK}/")

    top_code = list(set(df.topicCode))
    top_code = [item for item in top_code if not(pd.isnull(item)) == True]
    topics = (topics[topics['topicCode'].isin(top_code)]
            .drop_duplicates())

    # création du champs DESTINATION = lib+code
    topics.loc[(topics.destination_code.str.contains('^HORIZON', regex=True)) | (topics.destination_code.isnull()), 'destination'] = topics.thema_code
    topics.loc[(topics.destination.isnull())&(~topics.destination_code.isnull())&(topics.destination_lib.isnull()), 'destination'] = topics.destination_code
    topics.loc[(topics.destination.isnull())&(~topics.destination_code.isnull())&
                (~topics.destination_code.str.replace('-',' ').isin(topics.destination_lib.str.upper())), 'destination'] = topics['destination_lib']+" - "+topics['destination_code']

    topics.loc[(topics.destination.isnull())&(~topics.destination_lib.isnull())&
                (topics.destination_code.str.replace('-',' ').isin(topics.destination_lib.str.upper())), 'destination'] = topics.destination_lib

    y=topics.loc[topics.destination.isnull(), ['destination_lib','destination_code', 'destination']].drop_duplicates()
    if len(y)>1:
        print(y)

    df = (df
            .merge(pd.DataFrame(topics), how='left', on='topicCode')
            .drop_duplicates()
            .drop(columns='destination_code')
            .rename(columns={'destination':'destination_code', 'topicCode':'topic_code'}))
    print(f"size merged after add topics: {len(df)}")


    if len(df[df['programme_code'].isnull()])>0:
        print(f"ATTENTION ! programme_code manquant")
    topics.to_csv(f"{PATH_CLEAN}topics_current.csv", index=False, encoding="UTF-8", sep=";", na_rep='')

    return df