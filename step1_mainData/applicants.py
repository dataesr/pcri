
from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE

def role():
    app1 = app1.loc[app1.project_id.isin(projects.project_id.unique())] 
    print(f"0 - size app1 sans les exclus: {len(app1)}")
 
    # traitement ROLE
    if app1['role'].nunique()==5:
        app1['role'] = app1['role'].str.lower()   
        app1 = app1.assign(partnerType='applicant')
        app1.loc[app1.role.isin(['affiliated','associated']),'partnerType'] = app1.role+' partner'
        app1.loc[app1.role=='host', 'partnerType'] = 'host'        
        app1.loc[app1['role'] != 'coordinator', 'role'] = 'partner'
    else:
        print(f"Attention ! il existe une modalité en plus dans la var ROLE dans les applicants {app1['role'].unique()}")

     #création erc_role
    proj_erc = projects.loc[(projects.stage=='evaluated')&(projects.thema_code=='ERC'), ['project_id', 'destination_code','action_code']]
#     proj_erc['project_id'] = proj_erc['project_id'].astype('int')
    app1 = app1.merge(proj_erc, how='left', on='project_id').drop_duplicates()
    app1 = app1.assign(erc_role='partner')
    app1.loc[(app1.destination_code=='SyG')&((app1.partnerType=='host')|(app1.role=='coordinator')), 'erc_role'] = 'PI'
    app1.loc[(app1.destination_code!='SyG')&(app1.role=='coordinator'), 'erc_role'] = 'PI'
    app1.loc[(app1.destination_code=='ERC-OTHERS'), 'erc_role'] = np.nan
    app1 = app1.drop(columns=['destination_code', 'action_code']).drop_duplicates()       
        
    
    
        
