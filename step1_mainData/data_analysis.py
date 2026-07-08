
import pandas as pd
import functions_shared as bugs_excel
from config_path import PATH_WORK

def data_analysis(prop, app, proj, part):
    """
    compare proposals and applicants ID -> if bugs excel file prop_without_app / app_without_info_prop in data_work
    compare projects and participants ID -> if bugs excel file proj_without_part / part_without_info_proj in data_work
    Returns:
        call_to_integrate - list of callId to integrate in proposals table 
        call_miss - list of callId with missing project_id in proposals table
        proj_to_prop - dataframe with project_id, callId and status_code of projects just to integrate PROP info
    """
    print("### DATA ANALYSIS -> compare tables records")
    tmp1 = prop[['project_id', 'callId', 'stageExitStatus']].merge(app[['project_id']].drop_duplicates(), how='outer', on='project_id', indicator=True)
    tmp1.rename(columns={"_merge":'propApp_merge'}, inplace=True)
    
    # check proposals between proposals and applicants
    if all(tmp1.query('propApp_merge == "both"')):
        print('1- PROP = APP')
    elif not tmp1.query('propApp_merge == "right_only"').empty:
        print(f"2- PROP > APP -> {tmp1[tmp1['propApp_merge'] == 'right_only'].groupby(['callId'])['project_id'].nunique()} project_id not in applicants")
        t=tmp1.query('propApp_merge == "right_only"').drop(columns='propApp_merge')
        bugs_excel(t, PATH_WORK, 'prop_without_app')
        print("- decide if these project_id should be remove from PROP")
    elif not tmp1.query('propApp_merge == "left_only"').empty:
        print(f"3- PROP < APP -> {tmp1[tmp1['propApp_merge'] == 'left_only']['project_id'].nunique()} project_id not in proposals")
        t=tmp1.query('propApp_merge == "left_only"').drop(columns='propApp_merge')
        bugs_excel(t, PATH_WORK, 'app_without_info_prop')

        print("- check if these project_id are not in PROJ and integrate info in PROP otherwise remove them from applicants -> proj_to_prop")
        proj_to_prop = proj[proj['project_id'].isin(tmp1[tmp1['propApp_merge'] == 'left_only']['project_id'].unique())][['project_id', 'callId', 'status_code']]



    tmp2 = proj[['project_id', 'callId', 'status_code']].merge(part[['project_id']].drop_duplicates(), how='outer', on='project_id', indicator=True)
    tmp2.rename(columns={"_merge":'projPart_merge'}, inplace=True)
    
    # check projects between projects and participants
    if all(tmp2.query('projPart_merge == "both"')):
        print('1- PROJ = PART')
    elif not tmp2.query('projPart_merge == "right_only"').empty:
        print(f"2- PROJ > PART -> {tmp2[tmp2['projPart_merge'] == 'right_only'].groupby(['callId'])['project_id'].nunique()} project_id not in participants") 
        t=tmp2.query('projPart_merge == "right_only"').drop(columns='projPart_merge')
        bugs_excel(t, PATH_WORK, 'proj_without_part')  
    elif not tmp2.query('projPart_merge == "left_only"').empty:
        print(f"3- PROJ < PART -> {tmp2[tmp2['projPart_merge'] == 'left_only']['project_id'].nunique()} project_id not in projects")
        t=tmp2.query('projPart_merge == "left_only"').drop(columns='projPart_merge')
        bugs_excel(t, PATH_WORK, 'part_without_info_proj')
        
    print('\n### MISSING PROPOSALS')
    """
    check if all project_id in projects table are in proposals table, if not print the number of missing project_id and the callId associated to these missing project_id
    2. save in an excel file the list of missing project_id by callId
    3. number of project_id missing by callId already in proposals table -> save in temp/proj_no_proposals.csv
    4. flag call to integrate in proposals table and 
    Args:
        df - proposals dataframe
        proj - projects dataframe
        extractDate - date of the data extraction to name the excel file with missing project_id
    Returns:
        call_to_integrate - list of callId to integrate in proposals table 
        call_miss - list of callId with missing project_id in proposals table
    """
    # verification que tous les projets de proj sont aussi dans prop -> prefix des colonnes
    tmp3 = tmp2[~tmp2['project_id'].isin(tmp1.project_id.unique())]
    if tmp3.empty:
        print('1- PROP = PROJ') 
    else:
        print(f"2- result: {tmp3.project_id.nunique()} missing signed projects in proposals")
        call_miss = tmp3.callId.unique()
        print(f"3- missing proposals by callId:\n{tmp3.callId.value_counts()}\n")
        
        extractDate = open("temp/extractDate.txt").read()

        with pd.ExcelWriter(f"{PATH_WORK}missing_proposals_{extractDate}.xlsx") as writer:
            for i in call_miss:
                proj[(proj['callId']==i)&(~proj['project_id'].isin(tmp1.project_id.unique()))].to_excel(writer, sheet_name=f'{i}', index=False)

        pcm = tmp1[tmp1['callId'].isin(call_miss)].callId.value_counts().reset_index(name='tot_proposals')
        print(f"- callId already in proposals: {pcm}\n")
        ppm = tmp3.groupby('callId')['project_id'].count().reset_index(name='tot_project')
        res = pd.merge(pcm, ppm, how='outer', on='callId')
        res = res.assign(inProposal=lambda x: x.tot_proposals.notnull().astype(bool),
                        tx_calcul=lambda x: x.tot_proposals.notnull().astype(bool))
        res.to_csv(f"temp/proj_no_proposals.csv", sep=';', index=False)
        call_to_integrate = res.loc[res['inProposal'], 'callId'].to_list()

    return (
        locals().get("call_to_integrate", []),
        locals().get("call_miss", []),
        locals().get("proj_to_prop", [])
    )