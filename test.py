import pandas as pd
import numpy as np
from config_path import *
from functions_shared import *
from constant_vars import ZIPNAME, FRAMEWORK

# # ent = pd.read_pickle(f"{PATH_CLEAN}entities_info_current2.pkl")
# # part = pd.read_pickle(f"{PATH_CLEAN}participation_current.pkl")
# # lien = pd.read_pickle(f"{PATH_CLEAN}lien.pkl")
# # # print(part.columns)

# # # ent=ent.loc[ent['entities_id']=='Y7ch7', ['generalPic',  'businessName']]

# # # part.loc[(part.stage=='successful')&(part.generalPic.isin(ent.generalPic.unique()))][['generalPic']].value_counts()
# # x=part.loc[(part.stage=='successful')&(part.generalPic.isin(ent.generalPic.unique()))]
# # # work_csv(x, 'entreprise_fr')

# # print(lien)

# # lien.loc[lien.project_id=='101120060']

# # ent.loc[ent['generalPic'].isin(['999957869', '953181171'])]


# # df = pd.DataFrame({'col1':[1.0,2,3,4,5,6],
# #                     'col2':[1.0,2.0,3.0,4.0,5.0,6.0],
# #                     'col3':[1.1,2.1,3.1,4.1,5.1,6.1]})

# # casc = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'projects_cascadingProjects.json', 'utf8')
# # casc_part = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'projects_participants_cascadingParticipants.json', 'utf8')
# # ['cascadingProjectNbr', 'callId', 'toaCode', 'acronym', 'status',
# #        'startDate', 'endDate', 'signatureDate', 'title', 'abstract',
# #        'freeKeywords', 'url', 'topicCode', 'uniqueProgPartAbbr', 'duration',
# #        'totalCost', 'totalGrant', 'euContribution', 'nationalContribution',
# #        'otherContribution', 'nbParticipants', 'projectNbr', 'partnershipName',
# #        'partnershipType', 'partnershipWebpage', 'eitArea', 'eitSegment',
# #        'eitActivityCategory', 'framework', 'lastUpdateDate']

# # casc_part = pd.DataFrame(casc_part)
# # casc = pd.DataFrame(casc)
# # casc[['projectNbr']].value_counts()
# # casc = del_list_in_col(casc, 'freeKeywords', 'freekw')
# # # tmp = tmp.loc[tmp.freekw!=''].groupby(['cascadingProjectNbr', 'projectNbr']).agg(lambda x: ';'.join(map(str, filter(None, x.dropna().unique())))).reset_index()


# # # with pd.ExcelWriter(f"{PATH_WORK}cascading_projects.xlsx") as writer:
# # #     pd.DataFrame(casc).to_excel(writer, sheet_name='casc', index=False)
# # #     pd.DataFrame(casc_part).to_excel(writer, sheet_name='casc_part', index=False)
# # part = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'projects_participants.json', 'utf8')
# # part = pd.DataFrame(part)
# # part['projectNbr'] = part['projectNbr'].astype(str)
# # part = part[part.projectNbr.isin(casc.projectNbr.unique())]

# # leg = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'legalEntities.json', 'utf8')
# # leg=pd.DataFrame(leg)
#     # the filename should mention the extension 'npy'



# proj = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'projects.json', 'utf8')
# proj=pd.DataFrame(proj)

# prop = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'proposals.json', 'utf8')
# prop = pd.json_normalize(prop)   

# app = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'proposals_applicants.json', 'utf8')
# app = pd.json_normalize(app)

# part = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", "projects_participants.json", 'utf8')
# part = pd.DataFrame(part)

# topics = pd.read_csv(f"{PATH_CLEAN}topics_current.csv", sep=';')
# top=topics.loc[topics.thema_code=='EIC']

# pp = (prop.loc[prop.topicCode.str.contains('EIC-'),['topicCode', 'callDeadlineDate', 'typeOfActionCode', 'proposalNbr', 'acronym', 'stageExitStatus', 'eicPanels', 'scientificPanel']]
# .merge(app[['proposalNbr', 'orderNumber', 'generalPic', 'applicantPic',
#        'applicantPicLegalName', 'role', 'countryCode', 
#        'requestedGrant']], how='inner', on='proposalNbr')
#        .assign(stage='evaluated'))
# pp.rename(columns={'proposalNbr':'project_id','stageExitStatus':'status_code', 'scientificPanel':'panel_code', 
#     'requestedGrant':'fund', 'applicantPic':'pic', 'applicantPicLegalName':'name'}, inplace=True)
     

# pt = (proj.loc[proj.topicCode.str.contains('EIC-'),['topicCode', 'callDeadlineDate',
#         'typeOfActionCode', 'projectNbr', 'acronym', 'projectStatus']]
#     .merge(part[['projectNbr', 'orderNumber', 'generalPic', 'participantPic', 
#              'partnerRemovalStatus',
#        'participantLegalName', 'partnerType', 'partnerRole', 'countryCode', 
#        'netEuContribution']], how='inner', on='projectNbr')
#        .assign(stage='laureat'))
# pt.rename(columns={"projectNbr": "project_id",  "projectStatus":"status_code",
#                     'participantPic':'pic', 'participantLegalName':'name', 
#                     'partnerRole':'role', 'netEuContribution':'fund'}, inplace=True)

# x=pd.concat([pp, pt], ignore_index=True)
# x=x.merge(topics, how='left', on='topicCode')
# x.to_csv(f"{PATH_WORK}eic_ecorda.csv", sep=';', encoding='utf-8', na_rep='')


from main_library import *

# ref_source = ref_source_load('ref')
# lid_source, unknow_list = ID_entities_list(ref_source)
paysage_id=pd.read_pickle(f"{PATH_SOURCE}paysage_id.pkl")
paysage_id = pd.DataFrame(paysage_id)
paysage_id = paysage_id[~paysage_id.id_paysage.isnull()]
x=pd.DataFrame([i['api_id'] for i in lid_source if i['source_id'] in ['paysage']], columns=["id_source"])
paysage_id = pd.concat([paysage_id, x], ignore_index=True) 
paysage_id.loc[paysage_id.id_paysage.isnull(), 'id_paysage'] = paysage_id.id_source
paysage_id['nb'] = paysage_id.groupby('id_source')['id_paysage'].transform('count')
paysage_id = (paysage_id.loc[~((paysage_id.nb>1)&(paysage_id.status==False))]
        .drop(columns=['status', 'nb'])
        .drop_duplicates())
doublon=list(paysage_id.loc[(paysage_id.groupby('id_source')['id_paysage'].transform('count')>1)].id_paysage)
try:
    for i in doublon:
        url1=f'https://api.paysage.dataesr.ovh/structures/{str(i)}'
        rinit = requests.get(url1, headers=paysage_headers, verify=False)
        r = rinit.json()
        print({i, r.get('structureStatus')})
        if r.get('structureStatus')=='inactive':
            paysage_id=paysage_id[paysage_id.id_paysage!=i]
        elif r.get('structureStatus') is None:
            print(f"vérifier et ajouter un statut dans paysage pour: {i}")
    # except:
except requests.exceptions.HTTPError as http_err:
    print(f"\n{i} -> HTTP error occurred: {http_err}")
    doublon.append(str(i))
except requests.exceptions.RequestException as err:
    print(f"\n{i} -> Error occurred: {err}")
    doublon.append(str(i))
except Exception as e:
    print(f"\n{i} -> An unexpected error occurred: {e}")
    doublon.append(str(i))
    #     paysage_id = x
    #     paysage_id['id_paysage'] = paysage_id.id_source

#provisoire essayer de régler ce problème à la source
paysage_id=paysage_id[paysage_id.id_paysage!='im9o8']
print(f"1bis - resultat id entities paysagés {len(paysage_id[~paysage_id.id_paysage.isnull()])}")


paysage_relat = paysage_id['id_paysage'].dropna().unique().astype(str).tolist()
print(f"2 - size de paysage relat à vérifier {len(paysage_relat)}")

# #successor
paysage_successor=[]
n=0
for i in paysage_relat:
    time.sleep(0.2)
    n=n+1
    if n % 100 == 0: 
        print(f"{n}", end=',')
        
    try:
        url2=f'https://api.paysage.dataesr.ovh/relations?filters[relationTag]=structure-predecesseur&filters[relatedObjectId]={str(i)}&limit=500'
        rinit = requests.get(url2, headers=paysage_headers, verify=False)
        pages = rinit.json().get('totalCount')
        r = rinit.json()['data']
        if r:
            for page in range(0,pages):
                response={'id_paysage':r[page].get('relatedObject').get('id'), 'id_s0':r[page].get('resourceId'), 'end_date':r[page].get('endDate'), 'start_date':r[page].get('startDate'), 'active':r[page].get('active')}
                paysage_successor.append(response)
                if r[page].get('resourceId') not in paysage_relat:
                    paysage_relat.append(r[page].get('resourceId'))  

    except requests.exceptions.HTTPError as http_err:
        print(f"\n{i} -> HTTP error occurred: {http_err}")
        paysage_relat.append(str(i))
    except requests.exceptions.RequestException as err:
        print(f"\n{i} -> Error occurred: {err}")
        paysage_relat.append(str(i))
    except Exception as e:
        print(f"\n{i} -> An unexpected error occurred: {e}")
        paysage_relat.append(str(i))