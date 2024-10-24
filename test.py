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

