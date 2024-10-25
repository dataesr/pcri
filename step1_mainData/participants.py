from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE
import pandas as pd, numpy as np


def role_type(df):
    print("### Participants ROLE")

# traitement ROLE
    if df['role'].nunique()==2:
        df['role'] = df['role'].str.lower()
        df['role'] = np.where(df['role']=='participant', 'partner', 'coordinator')
    else:
        print(f"- Attention ! il existe une modalité en plus dans la var ROLE dans les participants {df['role'].unique()}")

    if (df['partnerType'].nunique(dropna=False)==3)|any(df['partnerType'].isnull()):
        df['partnerType'] = df['partnerType'].str.lower().str.replace('_', ' ')
        if any(df['partnerType'].isnull()):
            print(f"- Attention il y a {len(df[df['partnerType'].isnull()])} participants sans partnerType")
    else:
        print(f"- Attention ! il existe une modalité en plus dans la var partnerType des participants {df.loc[~df['partnerType'].isnull()].partnerType.value_counts()}")
    return df


def erc_role(df, proj_erc):    
    #création de erc_role
    print("### Participants ERC")
    df = df.merge(proj_erc, how='left', on='project_id').drop_duplicates()
    df = df.assign(erc_role='partner')
    df.loc[(df.destination_code=='SyG')&(df.partnerType=='beneficiary')&(pd.to_numeric(df.orderNumber, errors='coerce')<5.), 'erc_role'] = 'PI'
    df.loc[(df.destination_code!='SyG')&(df.role=='coordinator'), 'erc_role'] = 'PI'
    df.loc[(df.destination_code=='ERC-OTHERS'), 'erc_role'] = np.nan
    df = df.drop(columns=['destination_code','action_code']).drop_duplicates()
    return df