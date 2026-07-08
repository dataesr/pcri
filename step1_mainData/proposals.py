def proposals_status(df, proj_id_signed, stage_p):
    """
    1. check if all status in stageExitStatus are in stage_p list, if not print the new status and check if they should be integrated in stage_p list
    2. tag in_project to identify proposals successfull even if not MAIN
    3. status_evaluation -> status of proposals at evaluation
    4. remove proposals with stageExitStatus INELIGIBLE, INADMISSIBLE, DUPLICATE, WITHDRAWN
    5. assign stage 'evaluated' to all proposals and drop stageExitStatus column

    Args:
        df - proposals dataframe
        proj_id_signed - list of project_id with signed grant agreement in projects table
        stage_p - list of possible status in stageExitStatus column
    Returns:
        df - cleaned proposals dataframe with status_code and stage columns
        rep - list of dict with reporting information on the cleaning process
    """
    print("\n### PROPOSALS Status")
    if len(df.stageExitStatus.unique()) != len(stage_p):
        return print(f"- STATUS - {len(df.stageExitStatus.unique())-len(stage_p)} statut de proposition a été ajouté à stageExitStatus ;\n vérifier s'il faut l'intégrer aux projets ELIGIBLE {df.loc[~df.stageExitStatus.isin(stage_p), 'stageExitStatus']}\n")
    else:
        pass

    df.loc[df.project_id.isin(proj_id_signed), 'in_project'] = True
    df.loc[df['in_project'].isnull(), 'in_project'] = False
    # df.loc[df.project_id.isin(proj_id_signed), 'status_code'] = 'MAIN'
    # df.loc[(df.stageExitStatus=="MAIN")&(df.status_code!='MAIN'), 'status_code'] = 'MAIN'

    l = ['INELIGIBLE', 'INADMISSIBLE', 'DUPLICATE','WITHDRAWN']
    df = df.loc[(~df.stageExitStatus.isin(l))&(~df.stageExitStatus.isnull())]
    # df.loc[df.status_code.isnull(), 'status_code'] = df.stageExitStatus
    df = (df
          .rename(columns={'stageExitStatus': 'status_evaluation'})
          .assign(stage='evaluated'))

    print(f"- after cleaning -> size prop1 without inadmissible/inegible/etc : {len(df)}")
    rep=[{'stage_process': 'process2_status', 'proposal_size': len(df)}]
    return df, rep

# def proposals_id_missing(df, proj, extractDate):
#     """
#     check if all project_id in projects table are in proposals table, if not print the number of missing project_id and the callId associated to these missing project_id
#     2. save in an excel file the list of missing project_id by callId
#     3. number of project_id missing by callId already in proposals table -> save in temp/proj_no_proposals.csv
#     4. flag call to integrate in proposals table and 
#     Args:
#         df - proposals dataframe
#         proj - projects dataframe
#         extractDate - date of the data extraction to name the excel file with missing project_id
#     Returns:
#         call_to_integrate - list of callId to integrate in proposals table 
#         call_miss - list of callId with missing project_id in proposals table
#     """
#     print('\n### MISSING PROPOSALS')
#     # verification que tous les projets de proj sont aussi dans prop -> prefix des colonnes
#     if proj[~proj['project_id'].isin(df.project_id.unique())].empty:
#         print('1- ok pas de projets manquants dans proposals') 
#     else:    
#         print(f"2- result: {len(proj[~proj['project_id'].isin(df.project_id.unique())].project_id.unique())} projets signés absents de la table des propositions")
#         call_miss = proj[~proj['project_id'].isin(df.project_id.unique())].callId.unique()
#         print(f"3- missing proposals by callId:\n{proj[~proj['project_id'].isin(df.project_id.unique())].callId.value_counts()}\n")
        
#         with pd.ExcelWriter(f"{PATH_WORK}missing_proposals_{extractDate}.xlsx") as writer:
#             for i in call_miss:
#                 proj[(proj['callId']==i)&(~proj['project_id'].isin(df.project_id.unique()))].to_excel(writer, sheet_name=f'{i}', index=False)

        
#         pcm = df[df['callId'].isin(call_miss)].callId.value_counts().reset_index(name='tot_proposals')
#         print(f"- callId already in proposals: {pcm}\n")
#         ppm = proj[~proj['project_id'].isin(df.project_id.unique())].groupby('callId')['project_id'].count().reset_index(name='tot_project')
#         res = pd.merge(pcm, ppm, how='outer', on='callId')
#         res = res.assign(inProposal=lambda x: x.tot_proposals.notnull().astype(bool),
#                         tx_calcul=lambda x: x.tot_proposals.notnull().astype(bool))
#         res.to_csv(f"temp/proj_no_proposals.csv", sep=';', index=False)
#         return res.loc[res['inProposal'], 'callId'].to_list(), call_miss

def proj_id_miss_fixed(df, proj, call_to_integrate):
    """
    list projects in calls to integrate and add them to proposals table with status_evaluation = UNKNOWN and stage evaluated, and drop columns not in proposals table
    """
    import numpy as np

    if len(proj[~proj['project_id'].isin(df.project_id.unique())])>0:
        return (proj[(~proj['project_id'].isin(df.project_id.unique()))&(proj['callId'].isin(call_to_integrate))]
            .assign(in_project=True, status_evaluation='UNKNOWN', status_code=np.nan, stage='evaluated')
            .drop(columns=['otherContribution', 'totalGrant', 'ecSignatureDate', 'nationalContribution', 'startDate', 'endDate', 'url']))