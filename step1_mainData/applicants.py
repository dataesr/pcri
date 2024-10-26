
from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE

def app_role_type (df):
    print("### applicants ROLE")
    import numpy as np
    
    df.loc[:,'role'] = df.loc[:,'role'].str.lower() 
  
    if df['role'].nunique()==5:  
        df = df.assign(partnerType='applicant')
        df.loc[df.role.isin(['affiliated','associated']),'partnerType'] = df.role+' partner'
        df.loc[df.role=='host', 'partnerType'] = 'host'        
        df.loc[df['role'] != 'coordinator', 'role'] = 'partner'
    else:
        print(f"- Attention ! il existe une modalité en plus dans la var ROLE dans les applicants {df['role'].unique()}")
    return df

def part_miss_app(tmp, df):
    import pandas as pd
    if len(tmp)>0:
        print(f"ATTENTION ! vont être ajoutés les participants absents de proposals applicants {len(tmp)}")
        
        selector_d=[
            'project_id',
            'orderNumber', 
            'generalPic',
            'participant_pic',
            'name',
            'role',
            'countryCode',
            'nutsCode',
            'gps_loc',
            'legalEntityTypeCode',
            'isSme',
            'totalCosts',
            'netEuContribution']
        
        tmp = tmp[selector_d].rename(columns={'totalCosts':'budget', 'netEuContribution':'requestedGrant'}) 
        # tmp.loc[tmp.partnerType=='beneficiary', 'partnerType'] = 'applicant'
        df = pd.concat([df, tmp], ignore_index = True)
        print(f"- size app1 after add missing part1: {len(df)}, subv: {'{:,.1f}'.format(df['requestedGrant'].sum())}")
        # print(f"4 - montant definitif des subv_dem (suite lien avec projects propres): {'{:,.1f}'.format(app1.loc[app1.project_id.isin(projects.project_id.unique()), 'requestedGrant'].sum())}")
        return df