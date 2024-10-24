import pandas as pd
import numpy as np
from config_path import *
from functions_shared import *
from constant_vars import ZIPNAME, FRAMEWORK

# ent = pd.read_pickle(f"{PATH_CLEAN}entities_info_current2.pkl")
# part = pd.read_pickle(f"{PATH_CLEAN}participation_current.pkl")
# lien = pd.read_pickle(f"{PATH_CLEAN}lien.pkl")
# # print(part.columns)

# # ent=ent.loc[ent['entities_id']=='Y7ch7', ['generalPic',  'businessName']]

# # part.loc[(part.stage=='successful')&(part.generalPic.isin(ent.generalPic.unique()))][['generalPic']].value_counts()
# x=part.loc[(part.stage=='successful')&(part.generalPic.isin(ent.generalPic.unique()))]
# # work_csv(x, 'entreprise_fr')

# print(lien)

# lien.loc[lien.project_id=='101120060']

# ent.loc[ent['generalPic'].isin(['999957869', '953181171'])]


# df = pd.DataFrame({'col1':[1.0,2,3,4,5,6],
#                     'col2':[1.0,2.0,3.0,4.0,5.0,6.0],
#                     'col3':[1.1,2.1,3.1,4.1,5.1,6.1]})

casc = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'projects_cascadingProjects.json', 'utf8')
casc_part = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'projects_participants_cascadingParticipants.json', 'utf8')
# ['cascadingProjectNbr', 'callId', 'toaCode', 'acronym', 'status',
#        'startDate', 'endDate', 'signatureDate', 'title', 'abstract',
#        'freeKeywords', 'url', 'topicCode', 'uniqueProgPartAbbr', 'duration',
#        'totalCost', 'totalGrant', 'euContribution', 'nationalContribution',
#        'otherContribution', 'nbParticipants', 'projectNbr', 'partnershipName',
#        'partnershipType', 'partnershipWebpage', 'eitArea', 'eitSegment',
#        'eitActivityCategory', 'framework', 'lastUpdateDate']

# casc_part = pd.DataFrame(casc_part)
# casc = pd.DataFrame(casc)
# casc[['projectNbr']].value_counts()
# casc = del_list_in_col(casc, 'freeKeywords', 'freekw')
# # tmp = tmp.loc[tmp.freekw!=''].groupby(['cascadingProjectNbr', 'projectNbr']).agg(lambda x: ';'.join(map(str, filter(None, x.dropna().unique())))).reset_index()


# # with pd.ExcelWriter(f"{PATH_WORK}cascading_projects.xlsx") as writer:
# #     pd.DataFrame(casc).to_excel(writer, sheet_name='casc', index=False)
# #     pd.DataFrame(casc_part).to_excel(writer, sheet_name='casc_part', index=False)
# part = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'projects_participants.json', 'utf8')
# part = pd.DataFrame(part)
# part['projectNbr'] = part['projectNbr'].astype(str)
# part = part[part.projectNbr.isin(casc.projectNbr.unique())]

# leg = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'legalEntities.json', 'utf8')
# leg=pd.DataFrame(leg)
    # the filename should mention the extension 'npy'



# proj = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'projects.json', 'utf8')
# proj=pd.DataFrame(proj)
# proj_cols= proj.columns.to_list()
# # [col for col in proj.columns if proj[col].isnull().all()]
# # proj_cols.to_pickle("data_files/columns.pkl")

# np.save("data_files/proj_columns.npy", proj_cols)
# np.load("data_files/columns.npy").tolist()

# prop = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'proposals.json', 'utf8')
# prop = pd.json_normalize(prop)
# prop_cols= prop.columns.to_list()
# # [col for col in proj.columns if proj[col].isnull().all()]
# # proj_cols.to_pickle("data_files/columns.pkl")

# np.save("data_files/proposals_columns.npy", prop_cols)

from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE
from functions_shared import unzip_zip
import pandas as pd, numpy as np


print("### TOPICS")
data = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'topics.json', 'utf8')
print(f'1 - topics -> {len(data)}')
topics = pd.DataFrame(data)[["topicCode","topicDescription"]].drop_duplicates() 

# DIVISIONS 
data = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'topicLbDivisions.json', 'utf8')
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

destination = pd.read_json(open('data_files/destination.json', 'r+', encoding='utf-8'))
destination = pd.DataFrame(destination).drop(columns='dest_h20')

########################################################

#ERC
ERC = topics_divisions.loc[topics_divisions['lvl3Code']=="HORIZON.1.1", ['topicCode']].assign(thema_code='ERC')
typ = ["POC", "COG", "STG", "ADG", "PERA", "SyG", "SJI"]

for i in typ:
    ERC.loc[ERC.topicCode.str.contains(i), 'destination_code'] = i

if any(pd.isna(ERC.destination_code.unique())):
    print(f'erc : destination_code à null après traitement\n{ERC[ERC.destination_code.isnull()].topicCode.unique()}')
    ERC.loc[ERC.destination_code.isnull(), 'destination_code'] = 'ERC-OTHERS'

############################################################################
# MSCA
MSCA = topics_divisions.loc[topics_divisions['lvl3Code']=="HORIZON.1.2", ['topicCode']].assign(thema_code='MSCA')
typ = ["COFUND", "SE", "PF", "DN","CITIZENS"]

for i in typ:
    MSCA.loc[MSCA.topicCode.str.contains(i), 'destination_code'] = i

if any(pd.isna(MSCA.destination_code.unique())):
    print(f'MSCA : destination_code à null après traitement\n{MSCA[MSCA.destination_code.isnull()].topicCode.unique()}')
    MSCA.loc[MSCA.destination_code.isnull(), 'destination_code'] = 'MSCA-OTHERS'  

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
    INFRA.loc[INFRA.destination_code.isnull(), 'destination_code'] = 'DESTINATION-OTHERS'

# ####################################################################################


# # CLUSTER

CLUSTER = topics_divisions.loc[(topics_divisions.lvl2Code=='HORIZON.2')&(topics_divisions.topicCode.str.contains('-CL\\d{1}-|-HLTH-', regex=True))]
CLUSTER = CLUSTER[['topicCode']]

CLUSTER['destination_code'] = CLUSTER['topicCode'].str.split('-').str.get(3)
CLUSTER.loc[~CLUSTER.destination_code.isin(destination.destination_code.unique()), 'destination_code'] = np.nan

cl={'-HLTH-':'HEALTH-OTHERS',
'-CL2-':'CCSI-OTHERS',
'-CL3-':'CSS-OTHERS',
'-CL4-':'DIS-OTHERS',
'-CL5-':'CEM-OTHERS',
'-CL6-':'BIOENV-OTHERS'
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

miss.loc[miss.thema_code=="UNCAN", 'destination_code'] = "CANCER"

if any((miss.programme_code=='MISSION')&(miss.thema_code.isnull())):
    miss.loc[miss.thema_code.isnull(), 'thema_code'] = 'MISS-OTHERS' 
    # miss.loc[miss.programme_code.isnull(), 'programme_code'] = miss.lvl3Code 

########################################################################

# JU-JTI
spec={
'JU':'JU-JTI',
'JTI':'JU-JTI',
'EUSPA':'EUSPA',
'SESAR':'JU-JTI'
}

top = topics_divisions.loc[(topics_divisions.lvl2Code=='HORIZON.2')&(topics_divisions.topicCode.str.contains('|'.join([*spec]))), ['topicCode']]

for k,v in spec.items():
    top.loc[top.topicCode.str.contains(k), 'thema_code'] = v
if any(pd.isna(top.thema_code.unique())):
    print('top_hor2 : thema_code à null après traitement')

top = top.assign(destination_code=np.nan)
for i in ['SESAR', 'CLEAN-AVIATION', 'IHI', 'KDT', 'CBE', 'EDCTP3', 'EUROHPC', 'SNS', 'ER', 'Chips']:  
    pattern=str(i+"-")
    mask = (top.thema_code=='JU-JTI')&(top.destination_code.isnull())&(top.topicCode.str.contains(pattern))
    top.loc[mask, 'destination_code'] = i

for i in ['CLEANH2']:  
    pattern=str("-"+i+"-")
    mask = (top.thema_code=='JU-JTI')&(top.destination_code.isnull())&(top.topicCode.str.contains(pattern))
    top.loc[mask, 'destination_code'] = i

top.loc[top.destination_code=='KDT', 'destination_code'] = 'Chips'    

top.loc[top.thema_code=='EUSPA', 'destination_code'] = 'EUSPA'


if any(pd.isna(top.destination_code.unique())):
    print(f'top_hor2 : destination_code à null après traitement\n{top[top.destination_code.isnull()]}')
    top.loc[top.destination_code.isnull(), 'destination_code'] = 'DESTINATION-OTHERS'

# #############################################################################################################
# horizon 3
HOR3 = topics_divisions.loc[topics_divisions.lvl2Code=='HORIZON.3', ['topicCode', 'lvl3Code']]

spec={'PATHFINDER':'PATHFINDER',
'BOOSTER':'TRANSITION',
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
'CCSI':'KIC-CCSI'
}


for k,v in spec.items():
    HOR3.loc[HOR3.topicCode.str.contains(k), 'destination_code'] = v
if any(pd.isna(HOR3.destination_code.unique())):
    print(f"HOR3 : destination_code à null après traitement\n{HOR3[HOR3.destination_code.isnull()].sort_values('topicCode').topicCode.unique()}")
    HOR3.loc[HOR3.destination_code.isnull(), 'destination_code'] = 'DESTINATION-OTHERS'
    #####################################################################################

# horizon 4
HOR4 = topics_divisions.loc[topics_divisions.lvl2Code=='HORIZON.4', ['topicCode', 'lvl3Code']]
spec={
'ACCESS':'ACCESS',
'TALENTS':'TALENTS',
'TECH':'INFRATECH',
'COST':'COST',
'EURATOM':'EURATOM',
'GENDER':'GENDER',
'-ERA-':'ERA'
}

for k,v in spec.items():
    HOR4.loc[HOR4.topicCode.str.contains(k), 'destination_code'] = v
if any(pd.isna(HOR4.destination_code.unique())):
    print(f"HOR4 : destination_code à null après traitement\n{HOR4[HOR4.destination_code.isnull()].sort_values('topicCode').topicCode.unique()}")
    HOR4.loc[HOR4.destination_code.isnull(), 'destination_code'] = 'DESTINATION-OTHERS'

# ########
#thema_code pour HOR3 et 4
tab = pd.concat([HOR3, HOR4], ignore_index=True)

# autres themas que horizon 2
thema_other={'HORIZON.1.3':'INFRA',
        'HORIZON.3.1':'EIC',
        'HORIZON.3.2':'EIE',
        'HORIZON.3.3':'EIT',
        'HORIZON.4.1':'Widening',
        'HORIZON.4.2':'ERA'}

# remplir thema
for k,v in thema_other.items():
    tab.loc[tab.lvl3Code==k, 'thema_code'] = v
tab.drop(columns='lvl3Code', inplace=True)
##############################################################################

# add niveau prog pilier
horizon = (topics_divisions
            .rename(columns={"lvl2Code": "pilier_code", "lvl2Description": "pilier_name_en", "lvl3Code": "programme_code", 
                            "lvl3Description": "programme_name_en",'topicDescription': 'topic_name'})
            .drop(columns=['lvl4Code','lvl4Description', 'divisionAbbreviation', 'divisionDescription', 'framework'])) 


#traitement des programmes hors mission
tab = pd.concat([tab, CLUSTER, top, INFRA, ERC, MSCA], ignore_index=True)
tab = tab.merge(horizon, how='inner', on='topicCode')
tab = tab.mask(tab == '')

# traitement niveau programme pour les MISSIONS
miss = (miss
        .merge(horizon[['topicCode', 'topic_name']], how='left', on='topicCode')
        .merge(horizon[['pilier_code', 'pilier_name_en', 'programme_code', 'programme_name_en']], 
                how='left', on='programme_code')
        .drop(columns='lvl3Code')
        .drop_duplicates())

# tab = pd.concat([tab, miss], ignore_index=True)  
# tab = tab.mask(tab == '')

# # traitement thema_code -> null
# reste = horizon.loc[~horizon.topicCode.isin(tab.topicCode.unique())]
# tab = pd.concat([tab, reste], ignore_index=True) 
# tab.loc[tab.thema_code.isnull(), 'thema_code'] = 'THEMA-OTHERS'

# thema_lib = pd.read_json(open('data_files/thema.json', 'r+', encoding='utf-8'))
# thema_lib = pd.DataFrame(thema_lib)

# tab = tab.merge(thema_lib, how='left', on='thema_code')
# tab.loc[tab.thema_name_en.isnull(), 'thema_name_en'] = tab.programme_name_en
# tab = tab.merge(destination, how='left', on='destination_code')

# data = pd.read_json(open('data_files/programme_fr.json', 'r+', encoding='utf-8'))
# data=pd.DataFrame(data)


# tab = tab.merge(data[['programme_code','programme_name_fr']], how='left',on='programme_code')
# tab = tab.merge(data[['pilier_code','pilier_name_fr']], how='left', on='pilier_code')

# tab.loc[(tab.thema_name_fr.isnull()), 'thema_name_fr'] = tab['programme_name_fr']

# if not tab.columns[tab.isnull().any()].empty:
#     print(f"attention des cellules sont vides dans horizon: {tab.columns[tab.isnull().any()]}")
