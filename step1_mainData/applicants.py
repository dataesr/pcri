
from constant_vars import FRAMEWORK
from paths import PATH_SOURCE
import numpy as np, pandas as pd
from functions_shared import bugs_excel

def app_role_type (df, projects):
    """
    organize the role and partnerType variables in the applicants dataframe on the participants model, 
    and create a new variable erc_role for ERC projects.
    The function also checks for any inconsistencies in the role and partnerType variables,
    and prints out any issues found. 
    Finally, it returns the updated dataframe with the new variables.   
    
    """
    print("### applicants ROLE")
    df.loc[:,'role'] = df.loc[:,'role'].str.lower()
    df.loc[df['role']=='participant', 'role'] = 'partner'
  
    if df['role'].nunique()==5:  
        df = df.assign(partnerType='applicant')
        df.loc[df.role.isin(['affiliated','associated']),'partnerType'] = df.role+' partner'
        df.loc[df.role=='host', 'partnerType'] = 'host'        
        df.loc[df['role'] != 'coordinator', 'role'] = 'partner'
    else:
        print(f"- ⚠️ ! check ROLE more than 5 modalities for applicants {df['role'].unique()}")
    
    #ERC role -> pi (STG, COG, ADG, POC, SYG), other ; ROLE ->  pi, co-pi (SYG coordinator), other
    proj_erc = projects.loc[(projects.stage=='evaluated')&(projects.thema_code=='ERC'), ['project_id', 'destination_code']]
    temp = df.merge(proj_erc, how='left', on='project_id').drop_duplicates()
    temp.loc[~temp.destination_code.isnull(), 'erc_role'] = 'other'
    temp.loc[(temp.destination_code=='SyG')&((temp.partnerType=='host')|(temp.role=='coordinator')), 'erc_role'] = 'pi'
    temp.loc[(~temp.destination_code.isnull())&(~temp.destination_code.isin(['SyG', 'ERC-OTHER', 'SJI']))&(temp.role=='coordinator'), 'erc_role'] = 'pi'
    temp.loc[(temp.destination_code=='SyG')&(temp.role=='coordinator'), 'role'] = 'co-pi'
    temp.loc[(temp.erc_role=='pi')&(temp.role!='co-pi'), 'role'] = 'pi'
    temp.loc[temp.destination_code.isin(['ERC-OTHER', 'SJI']), 'erc_role'] = np.nan

    df = pd.concat([df.loc[~df.project_id.isin(temp.project_id.unique())], temp])

    return df.drop(columns=['destination_code'])

def part_miss_app(tmp, df):
    if len(tmp)>0:
        print(("\n### add to APPLICANT from PARTICIPANT"))
        print(f"1- ⚠️ ! vont être ajoutés les participants absents de proposals applicants {len(tmp)}")
        
        selector_d=[
            'project_id',
            'orderNumber', 
            'generalPic',
            'participant_pic',
            'name',
            'role',
            'countryCode',
            'nutsCode',
            'gps_source',
            'legalEntityTypeCode',
            'isSme',
            'totalCosts',
            'netEuContribution']
        
        tmp = tmp[selector_d].rename(columns={'totalCosts':'budget', 'netEuContribution':'requestedGrant'}) 
        df = pd.concat([df, tmp], ignore_index = True)
        print(f"- size app1 after add missing part1: {len(df)}, subv: {'{:,.1f}'.format(df['requestedGrant'].sum())}")
        # print(f"4 - montant definitif des subv_dem (suite lien avec projects propres): {'{:,.1f}'.format(app1.loc[app1.project_id.isin(projects.project_id.unique()), 'requestedGrant'].sum())}")
        return df
    
def check_multiA_by_proj(df):
    """
    check for duplicates in the applicants dataframe based on the combination of project_id, orderNumber, generalPic, participant_pic, role, and partnerType.
    If duplicates are found, they are printed out and saved to an Excel file for further investigation
    """
    print("\n### check if applicants/projets unique by pic/orderNumber/role/partnerType")
    df = df.assign(n_app = 1)
    df['n_app'] = df.groupby(['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'partnerType'], dropna = False).pipe(lambda x: x.n_app.transform('sum'))
    verif=pd.DataFrame(df[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'role', 'partnerType', 'name', 'requestedGrant', 'budget', 'countryCode']])[df['n_app']>1]
    bugs_excel(verif, PATH_SOURCE, 'double_app_prop+pic')
    if len(verif)>0:
        print(f"- ⚠️ ! {len(verif)} records duplicated in excel bugs_found in path_source")
    else:
        print("- no double applicant by project/pic/orderNumber/role/partnerType")
    return df

