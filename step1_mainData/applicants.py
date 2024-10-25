
from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE

def role(df):

 
    # traitement ROLE
    if df['role'].nunique()==5:
        df['role'] = df['role'].str.lower()   
        df = df.assign(partnerType='applicant')
        df.loc[df.role.isin(['affiliated','associated']),'partnerType'] = df.role+' partner'
        df.loc[df.role=='host', 'partnerType'] = 'host'        
        df.loc[df['role'] != 'coordinator', 'role'] = 'partner'
    else:
        print(f"Attention ! il existe une modalité en plus dans la var ROLE dans les applicants {df['role'].unique()}")

def erc_role():
     #création erc_role
    proj_erc = projects.loc[(projects.stage=='evaluated')&(projects.thema_code=='ERC'), ['project_id', 'destination_code','action_code']]
#     proj_erc['project_id'] = proj_erc['project_id'].astype('int')
    df = df.merge(proj_erc, how='left', on='project_id').drop_duplicates()
    df = df.assign(erc_role='partner')
    df.loc[(df.destination_code=='SyG')&((df.partnerType=='host')|(df.role=='coordinator')), 'erc_role'] = 'PI'
    df.loc[(df.destination_code!='SyG')&(df.role=='coordinator'), 'erc_role'] = 'PI'
    df.loc[(df.destination_code=='ERC-OTHERS'), 'erc_role'] = np.nan
    df = df.drop(columns=['destination_code', 'action_code']).drop_duplicates()       
        
    
    
        
