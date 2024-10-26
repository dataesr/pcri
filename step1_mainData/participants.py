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
    return df


def checking_unique_part(df):
    print("### check unicité de chaque participant")
    #unicité de ['project_id', 'orderNumber', 'generalPic', 'participant_pic']
    vl = ['project_id', 'orderNumber', 'generalPic', 'participant_pic']   
    df = df.assign(n_pogp = 1)
    df['n_pogp'] = df.groupby(vl, dropna = False).pipe(lambda x: x.n_pogp.transform('sum'))
    if len(df[df['n_pogp']>1])>0:
        print("- ATTENTION ! doublon dans part1 avec project_id', 'orderNumber', 'generalPic', 'participant_pic -> Intégrer la var partnerType")

        vl = ['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'partnerType']   
        df = df.assign(n_pogpp = 1)
        df['n_pogpp'] = df.groupby(vl, dropna = False).pipe(lambda x: x.n_pogpp.transform('sum'))
        if len(df[df['n_pogpp']>1])>0:
            print("- ATTENTION ! doublon dans part1 avec project_id', 'orderNumber', 'generalPic', 'participant_pic, 'partnerType'")
            
def check_multiP_by_proj(df):
    print("### check unicité des participants/projets")
    df = df.assign(n_part = 1)
    df['n_part'] = df.groupby(['project_id', 'generalPic', 'participant_pic'], dropna = False).pipe(lambda x: x.n_part.transform('sum'))
    verif=pd.DataFrame(df[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'role', 'partnerType', 'name', 'euContribution', 'netEuContribution', 'countryCode']])[df['n_part']>1]
    # with pd.ExcelWriter(f"{PATH_SOURCE}bugs_found.xlsx") as writer:  
    #     verif.to_excel(writer, sheet_name='double_part_proj+pic')
    bugs_excel(verif, PATH_SOURCE, 'double_part_proj+pic')
    print(f"- ATTENTION ! {len(verif)} lignes problématiques voire fichier bugs_found")
    return df