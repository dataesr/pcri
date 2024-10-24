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

# casc = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'projects_cascadingProjects.json', 'utf8')
# casc_part = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'projects_participants_cascadingParticipants.json', 'utf8')
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



# proj = unzip_zip('HE_2024-06-07.json.zip', f"{PATH_SOURCE}{FRAMEWORK}/", 'projects_participants.json', 'utf8')
# proj=pd.DataFrame(proj)
# proj_cols= proj.columns.to_list()
# #


# np.save("data_files/participants_columns.npy", proj_cols)
# # np.load("data_files/columns.npy").tolist()

# app = unzip_zip('HE_2024-06-07.json.zip', f"{PATH_SOURCE}{FRAMEWORK}/", 'proposals_applicants.json', 'utf8')
# app = pd.json_normalize(app)
# prop_cols= app.columns.to_list()


# np.save("data_files/applicants_columns.npy", prop_cols)

from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE
from functions_shared import unzip_zip, columns_comparison, gps_col
import pandas as pd, numpy as np




print("### PARTICIPANT")
part = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", "projects_participants.json", 'utf8')
    
if part:
    part = pd.DataFrame(part)
    
    # new columns 
    columns_comparison(part, 'participants_columns')    

    tot_pid = len(part[['projectNbr','orderNumber', 'generalPic', 'participantPic', 'partnerRole', 'partnerType']].drop_duplicates())
    part = part.rename(columns={"projectNbr": "project_id", "participantPic": "participant_pic", 
                                'partnerRole': 'role', 'participantLegalName': 'name'})
    part = part.assign(stage='successful')  


    # remove participant with partnerRemovalStatus not null
    print(f"- subv_net avant traitement: {'{:,.1f}'.format(part['netEuContribution'].sum())}")
    length_removalstatus=len(part[~part['partnerRemovalStatus'].isnull()])
    print(f"- suppression de {length_removalstatus}, \nmodalités: {part[~part['partnerRemovalStatus'].isnull()][['partnerRemovalStatus']].value_counts()}")
    if part['partnerRemovalStatus'].nunique()==2:
        part = part[part['partnerRemovalStatus'].isnull()]
    else:
        print(f"- Nouvelle modalité dans partnerRemovalStatus : {part['partnerRemovalStatus'].unique()}")
        part = part[part['partnerRemovalStatus'].isnull()]

    
    c = ['project_id', 'orderNumber', 'generalPic', 'participant_pic']
    part.loc[:,c] = part.loc[:,c].astype(int, errors='ignore').astype(str) 
    
    # controle des projets entre projects et participants
    tmp=(part[['project_id']].drop_duplicates()
        .merge(projects.loc[projects.stage=='successful', ['project_id', 'call_id', 'acronym']], how='outer', on='project_id', indicator=True))
    if not tmp.query('_merge == "right_only"').empty:
        print("- projets dans projects sans participants")   
        with pd.ExcelWriter(f"{PATH_SOURCE}bugs_found.xlsx",  mode='a',  if_sheet_exists='replace') as writer:  
            tmp.query('_merge == "right_only"').drop(columns='_merge').to_excel(writer, sheet_name='proj_without_part')
        
    elif not tmp.query('_merge == "left_only"').empty:
        print("- projets dans participants et pas dans projects")    
        with pd.ExcelWriter(f"{PATH_SOURCE}bugs_found.xlsx",  mode='a',  if_sheet_exists='replace') as writer:  
            tmp.query('_merge == "left_only"').drop(columns='_merge').to_excel(writer, sheet_name='part_without_info_proj')        
    
    part = gps_col(part)

    
    # traitement ROLE
    if part['role'].nunique()==2:
        part['role'] = part['role'].str.lower()
        part['role'] = np.where(part['role']=='participant', 'partner', 'coordinator')
    else:
        print(f"- Attention ! il existe une modalité en plus dans la var ROLE dans les participants {part['role'].unique()}")

    if part['partnerType'].nunique()==3:    
        part['partnerType'] = part['partnerType'].str.lower().str.replace('_', ' ')
    else:
        print(f"- Attention ! il existe une modalité en plus dans la var partnerType des participants {part['partnerType'].unique()}")

    
    #création de erc_role
    proj_erc = projects.loc[(projects.stage=='successful')&(projects.thema_code=='ERC'), ['project_id', 'destination_code', 'action_code']]
    part = part.merge(proj_erc, how='left', on='project_id').drop_duplicates()
    part = part.assign(erc_role='partner')
    part.loc[(part.destination_code=='SyG')&(part.partnerType=='beneficiary')&(pd.to_numeric(part.orderNumber).astype('int')<5), 'erc_role'] = 'PI'
    part.loc[(part.destination_code!='SyG')&(part.role=='coordinator'), 'erc_role'] = 'PI'
    part.loc[(part.destination_code=='ERC-OTHERS'), 'erc_role'] = np.nan
    part = part.drop(columns=['destination_code','action_code']).drop_duplicates()  


    cont_sum = '{:,.1f}'.format(part['euContribution'].sum())
    net_cont_sum = '{:,.1f}'.format(part['netEuContribution'].sum())
    
    
    print(f"- result - dowloaded:{tot_pid}, retained part:{len(part)}, pb:{tot_pid-len(part)}, somme euContribution:{cont_sum}, somme netEuContribution:{net_cont_sum}")
    print(f"- montant normalement définif des subv_net (suite lien avec projects propres): {'{:,.1f}'.format(part.loc[part.project_id.isin(projects.loc[projects.stage=='successful'].project_id.unique()), 'netEuContribution'].sum())}")
    