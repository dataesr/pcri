from config_path import PATH_CONNECT

def poj_id_missing(prop1, proj):
    # verification que tous les projets de proj sont aussi dans prop -> prefix des colonnes
    if proj[~proj['project_id'].isin(prop1.project_id.unique())].empty:
        print('4 - ok pas de projets manquants dans proposals') 
    else:    
        print(f"5 - {len(proj[~proj['project_id'].isin(prop1.project_id.unique())].project_id.unique())} projets signés absents de la table des propositions")
        call_miss = proj[~proj['project_id'].isin(prop1.project_id.unique())].callId.unique()
        print(f"{call_miss}")
        print(prop1[prop1['callId'].isin(call_miss)].callId.value_counts())
        # extraction des projets absents des propositions création des vars proposals manquantes pour ajout à MERGED
        proj[~proj['project_id'].isin(prop1.project_id.unique())].groupby('callId')['project_id'].nunique().to_csv(f"{PATH_CONNECT}proj_no_proposals.csv", sep=';')
        return prop1[prop1['callId'].isin(call_miss)].callId.unique()

def proj_id_miss_fixed(prop1, proj, integrate_call):
    if len(proj[~proj['project_id'].isin(prop1.project_id.unique())])>0:
        return (proj[(~proj['project_id'].isin(prop1.project_id.unique()))&(proj['callId'].isin(integrate_call))]
            .assign(status_code='MAIN', stage='evaluated')
            .drop(columns=['otherContribution', 'totalGrant', 'ecSignatureDate', 'nationalContribution', 'startDate', 'endDate', 'url']))