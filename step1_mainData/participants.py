from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE
import pandas as pd, numpy as np



def proj_part_link():
    # controle des projets entre projects et participants
    tmp=(part[['project_id']].drop_duplicates()
        .merge(projects.loc[projects.stage=='successful', ['project_id', 'call_id', 'acronym']], how='outer', on='project_id', indicator=True))
    if not tmp.query('_merge == "right_only"').empty:
        print("- projets dans projects sans participants")   
        with pd.ExcelWriter(f"{PATH_SOURCE}bugs_found.xlsx",  mode='a',  if_sheet_exists='replace') as writer:  
            tmp.query('_merge == "right_only"').drop(columns='_merge').to_excel(writer, sheet_name='proj_without_part')
        
    elif not tmp.query('_merge == "left_only"').empty:
        print("- projets dans participants et pas dans projects")    
        with pd.ExcelWriter(f"{PATH_SOURCE}bugs_found.xlsx",  mode='a',  if_sheet_exists='replace') as writer:  
            tmp.query('_merge == "left_only"').drop(columns='_merge').to_excel(writer, sheet_name='part_without_info_proj')        
    

def part_role_type(df):
    print("### Participants ROLE")
    df.loc[:,'role'] = df.loc[:,'role'].str.lower()
# traitement ROLE
    if df['role'].nunique()==2:
        df['role'] = df['role'].str.lower()
        df['role'] = np.where(df['role']=='participant', 'partner', 'coordinator')
    else:
        print(f"- Attention ! il existe une modalité en plus dans la var ROLE dans les participants {df['role'].unique()}")

    if (df['partnerType'].nunique(dropna=False)==3)|any(df['partnerType'].isnull()):
        df['partnerType'] = df['partnerType'].str.lower().str.replace('_', ' ')
        if any(df['partnerType'].isnull()):
            print(f"- Attention ! sans partnerType: {len(df[df['partnerType'].isnull()])} participants pour {'{:,.1f}'.format(df.loc[df['partnerType'].isnull(), 'netEuContribution'].sum())} de financement\n")
            print(df.loc[df['partnerType'].isnull()][['role','fundAgencyName']].value_counts())
            fund_l = ['CLIMATE-KIC HOLDING BV', 'EIT DIGITAL', 'EIT HEALTH EV', 'KIC INNOENERGY SE', 'EIT RAW MATERIALS GMBH', 'EIT FOOD', 'EIT MANUFACTURING ASBL', 'EIT KIC URBAN MOBILITY SL']
            new=df.loc[df['partnerType'].isnull()].fundAgencyName.unique()
            if list(set(new)-set(fund_l)):
                print(f"- Attention a traiter ->  nouveau participant sans partnerType: {list(set(new)-set(fund_l))}")
            df.loc[(df.partnerType.isnull())&(df.fundAgencyName.isin(fund_l)), 'partnerType'] = 'beneficiary'
    else:
        print(f"- Attention ! il existe une modalité en plus dans la var partnerType des participants {df.loc[~df['partnerType'].isnull()].partnerType.value_counts()}")
    return df


