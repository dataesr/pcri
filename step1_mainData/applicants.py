
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