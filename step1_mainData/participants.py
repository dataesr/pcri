from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE
from functions_shared import bugs_excel
import pandas as pd, numpy as np

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
    print(f"- size part after role: {len(df)}")
    return df

def check_multiP_by_proj(df):
    print("### check unicité des participants/projets")
    df = df.assign(n_part = 1)
    df['n_part'] = df.groupby(['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'partnerType'], dropna = False).pipe(lambda x: x.n_part.transform('sum'))
    verif=pd.DataFrame(df[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'role', 'partnerType', 'name', 'euContribution', 'netEuContribution', 'countryCode']])[df['n_part']>1]
    bugs_excel(verif, PATH_SOURCE, 'double_part_proj+pic')
    print(f"- ATTENTION ! {len(verif)} lignes problématiques voire fichier bugs_found")
    return df


# def part_var_null(part, app1):
#     # verification des vars avec null/empty mais qui doivent être complétées
#     print(f"\n- var with null: {part.columns[part.isnull().any()].tolist()}")
#     if ('orderNumber' in part.columns[part.isnull().any()].tolist()) | ('partnerType' in part.columns[part.isnull().any()].tolist()):
#         print(f"- nb part concernés{part.loc[(part.orderNumber.isnull())|(part.partnerType.isnull())].project_id.value_counts()}")

#     ap=app1.loc[app1.project_id.isin(part.loc[(part.orderNumber.isnull())|(part.partnerType.isnull())].project_id.unique())]