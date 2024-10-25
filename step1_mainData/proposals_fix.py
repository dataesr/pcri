from config_path import PATH_WORK
import pandas as pd


def proposals_status(df, proj_id_signed, stage_p):   

    if len(df.stageExitStatus.unique()) != len(stage_p):
        return print(f"STATUS - {len(df.stageExitStatus.unique())-len(stage_p)} statut de proposition a été ajouté à stageExitStatus ;\n vérifier s'il faut l'intégrer aux projets ELIGIBLE {df.loc[~df.stageExitStatus.isin(stage_p), 'stageExitStatus']}\n")
    else:
        pass

    df.loc[df.project_id.isin(proj_id_signed), 'proposalStatus'] = 'MAIN'
    df.loc[(df.stageExitStatus=="MAIN")&(df.proposalStatus!='MAIN'), 'proposalStatus'] = 'MAIN'

    l = ['INELIGIBLE', 'INADMISSIBLE', 'DUPLICATE','WITHDRAWN']
    df = df.loc[(~df.stageExitStatus.isin(l))&(~df.stageExitStatus.isnull())]
    df.loc[df.proposalStatus.isnull(), 'proposalStatus'] = df.stageExitStatus
    df = df.assign(stage='evaluated')

    print(f"after cleaning - size prop1 without inadmissible/inegible/etc : {len(df)}\n")







def proposals_id_missing(prop1, proj, extractDate):
    print('### MISSING PROPOSALS')
    # verification que tous les projets de proj sont aussi dans prop -> prefix des colonnes
    if proj[~proj['project_id'].isin(prop1.project_id.unique())].empty:
        print('- ok pas de projets manquants dans proposals') 
    else:    
        print(f"- result: {len(proj[~proj['project_id'].isin(prop1.project_id.unique())].project_id.unique())} projets signés absents de la table des propositions")
        call_miss = proj[~proj['project_id'].isin(prop1.project_id.unique())].callId.unique()
        print(f"- missing proposals by callId:\n{proj[~proj['project_id'].isin(prop1.project_id.unique())].callId.value_counts()}\n")
        
        with pd.ExcelWriter(f"{PATH_WORK}missing_proposals_{extractDate}.xlsx") as writer:
            for i in call_miss:
                proj[(proj['callId']==i)&(~proj['project_id'].isin(prop1.project_id.unique()))].to_excel(writer, sheet_name=f'{i}', index=False)

        # extraction des projets absents des propositions création des vars proposals manquantes pour ajout à MERGED
        print(f"- callId already in proposals: {prop1[prop1['callId'].isin(call_miss)].callId.value_counts()}\n")
        proj[~proj['project_id'].isin(prop1.project_id.unique())].groupby('callId')['project_id'].nunique().to_csv(f"{PATH_WORK}proj_no_proposals.csv", sep=';')
        return prop1[prop1['callId'].isin(call_miss)].callId.unique()

def proj_id_miss_fixed(prop1, proj, call_to_integrate):
    if len(proj[~proj['project_id'].isin(prop1.project_id.unique())])>0:
        return (proj[(~proj['project_id'].isin(prop1.project_id.unique()))&(proj['callId'].isin(call_to_integrate))]
            .assign(status_code='MAIN', stage='evaluated')
            .drop(columns=['otherContribution', 'totalGrant', 'ecSignatureDate', 'nationalContribution', 'startDate', 'endDate', 'url']))