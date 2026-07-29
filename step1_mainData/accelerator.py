from functions_shared import unzip_zip, work_csv
import pandas as pd, numpy as np


def prop_accelerator_process(source_json, app1, projects, limit_min, limit_max):
    """
    Analysis and fix for accelerator projects. 
    compare accelerator projects in the main dataset with in a separate dataset.
    fix the grant accelerator in app1 with accelarator dataset acc.
    Args:
        app1 (pd.DataFrame): main dataset with project_id, role, requestedGrant and other columns
        projects (pd.DataFrame): main dataset with project_id, stage, thema_code and other columns
        limit_min (float): minimum limit for grant accelerator, if grantRequested < limit_min, it will be set to 0 and an alerte will be printed
        limit_max (float): maximum limit for grant accelerator, if grantRequested > limit_max, it will be set to 0 and an alerte will be printed
    Returns:
        app1 (pd.DataFrame): main dataset with project_id, role, requestedGrant and other columns, with grant accelerator fixed
    """

    acc_folio = unzip_zip(source_json, 'proposals_eicFundPortfolio.json', 'utf8')
    acc_folio = pd.DataFrame(acc_folio).assign(project_id=lambda x: x['proposalNbr'].astype(str))
    print(f"size acc_folio: {len(acc_folio)}")

    # check if accelerator project rows are same in the other dataset
    p = projects.loc[(projects['stage']=='evaluated')&(projects['project_id'].isin(acc_folio['project_id'].unique()))]
    print(f"- check thema_code={p.thema_code.unique()}/n- size projects accelerator: {len(p)}")

    if len(p)>len(acc_folio):
        print("- ⚠️ ! more accelerator projects in the main dataset than in the accelerator dataset, check project_id and thema_code")

    # compare the two datasets and check the grant requested for the coordinator        
    acc = app1.loc[app1['project_id'].isin(acc_folio['project_id'].unique())].merge(acc_folio[['project_id','grantRequested']], how='inner', on='project_id')
    acc.loc[acc['role'].str.lower() != 'coordinator', 'grantRequested'] = np.nan
    print(f"size acc: {len(acc)}")

    # clean values: if grant requested <=1.00, set to 0, and convert to int
    for s in ['requestedGrant', 'grantRequested']:
        acc.loc[acc[s]<=1.00, s] = 0
        acc[s] = acc[s].astype(str).str.split('.').str[0].astype(float)

    # assign a new column 'fund_by_ent' for saving no coordinator requestedGrant
    oth = acc[acc['role'].str.lower() != 'coordinator'].project_id.unique()
    acc.loc[acc['project_id'].isin(oth), 'fund_by_ent'] = acc['requestedGrant']

    # group requestedGrant by project and sum the grant requested for coordinator role
    grant_sum = acc.groupby('project_id')['requestedGrant'].transform('sum')

    acc['requestedGrant'] = acc['requestedGrant'].where(
        acc['role'].str.lower() != 'coordinator',
        grant_sum
    )
    acc.loc[acc['role'].str.lower() != 'coordinator', 'requestedGrant'] = 0

    # check if grantRequested is oultier value and check the printed message to fix it in the code
    acc['grant_fix_by_ce'] = acc['grantRequested']
    # test the limit for grant_fix_by_ce

    def get_alerte(val):
        if pd.isna(val):
            return 'missing value'
        elif val < limit_min:
            return f'low value (< {limit_min:,})'
        elif val > limit_max:
            return f'high value (> {limit_max:,})'
        else:
            return None

    acc['alerte'] = acc['grant_fix_by_ce'].apply(get_alerte)

    # Filtrer uniquement les lignes en alerte
    alertes = (acc.loc[acc['alerte'].notna(), ['project_id', 'role', 'grantRequested', 'alerte']]
               .merge(p[['project_id', 'acronym']], how='left', on='project_id')
    )
    if not alertes.empty:
        print(f"- {len(alertes)} projets en alerte pour grant_fix_by_ce:\n{alertes}")
        work_csv(alertes, 'alertes_grants_accelerator')
        alertes[['project_id', 'role', 'grantRequested', 'alerte']].assign(grant_fix=alertes['grantRequested']).to_json('data_files/EIC_acc_fix.json', orient='records')
        print("- fix eic grants in data_files/EIC_acc_fix.json")
    
    # grants fixed
    # fix = pd.read_json('data_files/EIC_acc_fix.json', orient='records', dtype={'project_id': str})
    if any(acc['grant_fix_by_ce']== 25000000.00):
        print(f"- ⚠️ ! some grant_fix_by_ce are 25M and are going to be fixed to 2.5M, check project_id and grantRequested values {acc.loc[acc['grant_fix_by_ce']== 25000000.00].project_id.unique()}")
        acc.loc[acc['grant_fix_by_ce']== 25000000.00, 'grant_fix_by_ce'] = 2500000.0

    # check difference between requestedGrant and grant_fix_by_ce, if difference > 1.0, print project_id, role, requestedGrant, grant_fix_by_ce and diff
    acc.loc[acc['grant_fix_by_ce'].isnull(), 'grant_fix_by_ce'] = 0.
    acc['diff'] = acc['requestedGrant'].fillna(0)-acc['grant_fix_by_ce'].fillna(0)
    pb = acc[(acc['diff']<-1.0) | (acc['diff']>1.0)]
    print(f"- difference between applicants and accelerator dataset: {len(pb)} projects\n- check values:\n {pb[['project_id', 'role', 'requestedGrant', 'grant_fix_by_ce', 'diff']]}")


    app1 = (app1.merge(acc[['project_id', 'role', 'orderNumber', 'generalPic', 'countryCode', 'grant_fix_by_ce', 'fund_by_ent']], 
                       how='left', on=['project_id', 'role', 'orderNumber', 'generalPic', 'countryCode']))
    
    app1.loc[app1['grant_fix_by_ce'].notnull(), 'requestedGrant'] = app1.loc[app1['grant_fix_by_ce'].notnull(), 'grant_fix_by_ce']
    app1.drop(columns=['grant_fix_by_ce'], inplace=True)
    print(f"- size app1 after accelerator fixed: {len(app1)}")
    return app1

    