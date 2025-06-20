from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE
from functions_shared import bugs_excel
import pandas as pd, numpy as np

def part_role_type(df, projects):
    print("### Participants ROLE")
    # df.loc[:,'role'] = df.loc[:,'role'].str.lower()
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

    proj_erc = projects.loc[(projects.stage=='successful')&(projects.thema_code=='ERC'), ['project_id', 'destination_code']]
    temp = df.merge(proj_erc, how='inner', on='project_id').drop_duplicates()
    temp.loc[~temp.destination_code.isnull(), 'erc_role'] = 'other'
    temp.loc[(temp.destination_code=='SyG')&(temp.partnerType=='beneficiary')&(pd.to_numeric(temp.orderNumber, errors='coerce')<5.), 'erc_role'] = 'pi'
    temp.loc[(~temp.destination_code.isnull())&(~temp.destination_code.isin(['SyG', 'ERC-OTHER']))&(temp.role=='coordinator'), 'erc_role'] = 'pi'
    temp.loc[(temp.destination_code=='SyG')&(temp.role=='coordinator'), 'role'] = 'co-pi'
    temp.loc[(temp.erc_role=='pi')&(temp.role!='co-pi'), 'role'] = 'pi'
    temp.loc[(temp.destination_code=='ERC-OTHER')|(temp.destination_code.isnull()), 'erc_role'] = np.nan

    df = pd.concat([df.loc[~df.project_id.isin(temp.project_id.unique())], temp])

    rep={'stage_process':'process3_keep_withProj', 'participant_size':len(df)}

    return df.drop(columns=['destination_code'])

def check_multiP_by_proj(df):
    print("\n### check unicité des participants/projets")
    df = df.assign(n_part = 1)
    df['n_part'] = df.groupby(['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'partnerType'], dropna = False).pipe(lambda x: x.n_part.transform('sum'))
    verif=pd.DataFrame(df[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'role', 'partnerType', 'name', 'euContribution', 'netEuContribution', 'countryCode']])[df['n_part']>1]
    bugs_excel(verif, PATH_SOURCE, 'double_part_proj+pic')
    if len(verif)>0:
        print(f"- ATTENTION ! {len(verif)} lignes problématiques voire fichier bugs_found")
    return df


# def part_var_null(part, app1):
#     # verification des vars avec null/empty mais qui doivent être complétées
#     print(f"\n- var with null: {part.columns[part.isnull().any()].tolist()}")
#     if ('orderNumber' in part.columns[part.isnull().any()].tolist()) | ('partnerType' in part.columns[part.isnull().any()].tolist()):
#         print(f"- nb part concernés{part.loc[(part.orderNumber.isnull())|(part.partnerType.isnull())].project_id.value_counts()}")

#     ap=app1.loc[app1.project_id.isin(part.loc[(part.orderNumber.isnull())|(part.partnerType.isnull())].project_id.unique())]