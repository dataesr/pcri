from config_path import PATH_WORK
import pandas as pd


def proposals_status(df, proj_id_signed, stage_p):   
    print("\n### PROPOSALS Status")
    if len(df.stageExitStatus.unique()) != len(stage_p):
        return print(f"- STATUS - {len(df.stageExitStatus.unique())-len(stage_p)} statut de proposition a été ajouté à stageExitStatus ;\n vérifier s'il faut l'intégrer aux projets ELIGIBLE {df.loc[~df.stageExitStatus.isin(stage_p), 'stageExitStatus']}\n")
    else:
        pass

    df.loc[df.project_id.isin(proj_id_signed), 'status_code'] = 'MAIN'
    df.loc[(df.stageExitStatus=="MAIN")&(df.status_code!='MAIN'), 'status_code'] = 'MAIN'

    l = ['INELIGIBLE', 'INADMISSIBLE', 'DUPLICATE','WITHDRAWN']
    df = df.loc[(~df.stageExitStatus.isin(l))&(~df.stageExitStatus.isnull())]
    df.loc[df.status_code.isnull(), 'status_code'] = df.stageExitStatus
    df = df.assign(stage='evaluated').drop(columns='stageExitStatus')

    print(f"- after cleaning -> size prop1 without inadmissible/inegible/etc : {len(df)}")
    rep=[{'stage_process': 'process2_status', 'proposal_size': len(df)}]
    return df, rep

def proposals_id_missing(df, proj, extractDate):
    print('\n### MISSING PROPOSALS')
    # verification que tous les projets de proj sont aussi dans prop -> prefix des colonnes
    if proj[~proj['project_id'].isin(df.project_id.unique())].empty:
        print('1- ok pas de projets manquants dans proposals') 
    else:    
        print(f"2- result: {len(proj[~proj['project_id'].isin(df.project_id.unique())].project_id.unique())} projets signés absents de la table des propositions")
        call_miss = proj[~proj['project_id'].isin(df.project_id.unique())].callId.unique()
        print(f"3- missing proposals by callId:\n{proj[~proj['project_id'].isin(df.project_id.unique())].callId.value_counts()}\n")
        
        with pd.ExcelWriter(f"{PATH_WORK}missing_proposals_{extractDate}.xlsx") as writer:
            for i in call_miss:
                proj[(proj['callId']==i)&(~proj['project_id'].isin(df.project_id.unique()))].to_excel(writer, sheet_name=f'{i}', index=False)

        # extraction des projets absents des propositions création des vars proposals manquantes pour ajout à MERGED
        print(f"- callId already in proposals: {df[df['callId'].isin(call_miss)].callId.value_counts()}\n")
        proj[~proj['project_id'].isin(df.project_id.unique())].groupby('callId')['project_id'].nunique().to_csv(f"{PATH_WORK}proj_no_proposals.csv", sep=';')
        return df[df['callId'].isin(call_miss)].callId.unique(), call_miss

def proj_id_miss_fixed(df, proj, call_to_integrate):
    if len(proj[~proj['project_id'].isin(df.project_id.unique())])>0:
        return (proj[(~proj['project_id'].isin(df.project_id.unique()))&(proj['callId'].isin(call_to_integrate))]
            .assign(status_code='MAIN', stage='evaluated')
            .drop(columns=['otherContribution', 'totalGrant', 'ecSignatureDate', 'nationalContribution', 'startDate', 'endDate', 'url']))