from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE
from functions_shared import unzip_zip, columns_comparison, gps_col
import pandas as pd, numpy as np


def participant_load(projects):

    print("### PARTICIPANT")
    part = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", "projects_participants.json", 'utf8')
        
    if part:
        part = pd.DataFrame(part)
        
        # new columns 
        columns_comparison(part, 'participants_columns')    

        tot_pid = len(part[['projectNbr','orderNumber', 'generalPic', 'participantPic', 'partnerRole', 'partnerType']].drop_duplicates())
        part = part.rename(columns={"projectNbr": "project_id", "participantPic": "participant_pic", 
                                    'partnerRole': 'role', 'participantLegalName': 'name'})
        part = part.assign(stage='successful')  
    

        # remove participant with partnerRemovalStatus not null
        print(f"- subv_net avant traitement: {'{:,.1f}'.format(part['netEuContribution'].sum())}")
        length_removalstatus=len(part[~part['partnerRemovalStatus'].isnull()])
        print(f"- suppression de {length_removalstatus}, \nmodalités: {part[~part['partnerRemovalStatus'].isnull()][['partnerRemovalStatus']].value_counts()}")
        if part['partnerRemovalStatus'].nunique()==2:
            part = part[part['partnerRemovalStatus'].isnull()]
        else:
            print(f"- Nouvelle modalité dans partnerRemovalStatus : {part['partnerRemovalStatus'].unique()}")
            part = part[part['partnerRemovalStatus'].isnull()]

        
        c = ['project_id', 'orderNumber', 'generalPic', 'participant_pic']
        for i in c:
            if part[i].dtype == 'float64':
                part[i] = part[i].astype(int, errors='ignore').astype(str) 
            else:
                part[i] = part[i].astype(str) 
        
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
        
        part = gps_col(part)
    
        
        # traitement ROLE
        if part['role'].nunique()==2:
            part['role'] = part['role'].str.lower()
            part['role'] = np.where(part['role']=='participant', 'partner', 'coordinator')
        else:
            print(f"- Attention ! il existe une modalité en plus dans la var ROLE dans les participants {part['role'].unique()}")

        if (part['partnerType'].nunique(dropna=False)==3)|any(part['partnerType'].isnull()):
            part['partnerType'] = part['partnerType'].str.lower().str.replace('_', ' ')
            if any(part['partnerType'].isnull()):
                print(f"- Attention il y a {len(part[part['partnerType'].isnull()])} participants sans partnerType")
        else:
            print(f"- Attention ! il existe une modalité en plus dans la var partnerType des participants {part.loc[~part['partnerType'].isnull()].partnerType.value_counts()}")

        
        #création de erc_role
        proj_erc = projects.loc[(projects.stage=='successful')&(projects.thema_code=='ERC'), ['project_id', 'destination_code', 'action_code']]
        part = part.merge(proj_erc, how='left', on='project_id').drop_duplicates()
        part = part.assign(erc_role='partner')
        part.loc[(part.destination_code=='SyG')&(part.partnerType=='beneficiary')&(pd.to_numeric(part.orderNumber, errors='coerce')<5.), 'erc_role'] = 'PI'
        part.loc[(part.destination_code!='SyG')&(part.role=='coordinator'), 'erc_role'] = 'PI'
        part.loc[(part.destination_code=='ERC-OTHERS'), 'erc_role'] = np.nan
        part = part.drop(columns=['destination_code','action_code']).drop_duplicates()  


        cont_sum = '{:,.1f}'.format(part['euContribution'].sum())
        net_cont_sum = '{:,.1f}'.format(part['netEuContribution'].sum())
        
        
        print(f"- result - dowloaded:{tot_pid}, retained part:{len(part)}, pb:{tot_pid-len(part)}, somme euContribution:{cont_sum}, somme netEuContribution:{net_cont_sum}")
        print(f"- montant normalement définif des subv_net (suite lien avec projects propres): {'{:,.1f}'.format(part.loc[part.project_id.isin(projects.loc[projects.stage=='successful'].project_id.unique()), 'netEuContribution'].sum())}")
        return part