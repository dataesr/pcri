from config_path import PATH_SOURCE, PATH_REF
from constant_vars import FRAMEWORK, ZIPNAME
from functions_shared import unzip_zip
import pandas as pd, numpy as np


def nuts_clean(df, var):
        df.loc[df['nutsCode'].isnull(), 'nuts_code'] = df[var]
        df.loc[(df['nutsCode'].isnull())&(df[var].isnull()), 'nuts_code'] = df['nutsCode']
        df.loc[(df['nutsCode'].isnull())&(df['nutsCode']==df[var]), 'nuts_code'] = df['nutsCode']
        df.loc[(df['nutsCode'].isnull())&(df[var].str[:2]!=df['nutsCode'].str[:2]), 'nuts_code'] = df[var]+';'+df['nutsCode']
        df.loc[(df['nutsCode'].isnull())&(df[var].str.len()==df['nutsCode'].str.len()), 'nuts_code'] = df['nutsCode']
        df.loc[(df['nutsCode'].isnull())&(df[var].str.len()>df['nutsCode'].str.len()), 'nuts_code'] = df[var]
        df.loc[(df['nutsCode'].isnull())&(df[var].str.len()<df['nutsCode'].str.len()), 'nuts_code'] = df['nutsCode']
        return df

def nuts_department():
        # bosser les nuts en amont pour récupérer le max d'infos sans attendre traitement departments
        pp_app = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'proposals_applicants_departments.json', 'utf8')
        pp_app = pd.DataFrame(pp_app)
        pp_app = (pp_app.loc[~pp_app.nutsCode.isnull(), ['proposalNbr', 'generalPic', 'applicantPic', 'nutsCode']]
                .rename(columns={'proposalNbr':'project_id', 'applicantPic':'proposal_participant_pic'})
                .astype(str)
                .drop_duplicates()
                )
        pp_app = pp_app.loc[pp_app.nutsCode!='-1']
        print(f"- size pp_app: {len(pp_app)}")

        pp_part = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'projects_participants_departments.json', 'utf8')
        pp_part = pd.DataFrame(pp_part)
        pp_part = (pp_part.loc[~pp_part.nutsCode.isnull(),['projectNbr', 'generalPic', 'participantPic', 'nutsCode']]
                .rename(columns={'projectNbr':'project_id', 'participantPic':'calculated_pic'})
                .astype(str)
                .drop_duplicates()
                )
        print(f"- size pp_part: {len(pp_part)}")
        return pp_app, pp_part

def nuts_lien(app1, part, lien):
    # from step1_mainData.nuts import nuts_department
    print("\n### LOADING DEPARTMENT")
    pp_app, pp_part = nuts_department()

    print("\n### NUTS avec LIEN")
    nuts_a=(app1[['project_id', 'orderNumber', 'generalPic', 'participant_pic','nutsCode']]
            .rename(columns={'orderNumber':'proposal_orderNumber',
                                'participant_pic':'proposal_participant_pic', 
                                'nutsCode':'nuts_app'})
            .fillna('')
            )

    nuts_p=(part[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'nutsCode']]
            .rename(columns={'nutsCode':'nuts_part', 'participant_pic':'calculated_pic'})
            .drop_duplicates()
            .fillna('')
            )

    print(f"- size nuts_a: {len(nuts_a)}, size nuts_p: {len(nuts_p)}")
    # size nuts_a: 463523, size nuts_p: 103661
    # pp_app: 347165,  pp_part: 88103
    l=len(nuts_a) 
    nuts_a = nuts_a.merge(pp_app, how='left', on=['project_id', 'generalPic', 'proposal_participant_pic'])
    nuts_a.loc[nuts_a.nutsCode.isnull(), 'nuts_code'] = nuts_a.nuts_app
    nuts_a.loc[(nuts_a.nuts_code.isnull())&(nuts_a.nuts_app.isnull()), 'nuts_code'] = nuts_a.nutsCode
    nuts_a.loc[(nuts_a.nuts_code.isnull())&(nuts_a.nutsCode==nuts_a.nuts_app), 'nuts_code'] = nuts_a.nutsCode
    nuts_a.loc[(nuts_a.nuts_code.isnull())&(nuts_a.nuts_app.str[:2]!=nuts_a.nutsCode.str[:2]), 'nuts_code'] = nuts_a.nuts_app+';'+nuts_a.nutsCode
    nuts_a.loc[(nuts_a.nuts_code.isnull())&(nuts_a.nuts_app.str.len()==nuts_a.nutsCode.str.len()), 'nuts_code'] = nuts_a.nutsCode
    nuts_a.loc[(nuts_a.nuts_code.isnull())&(nuts_a.nuts_app.str.len()>nuts_a.nutsCode.str.len()), 'nuts_code'] = nuts_a.nuts_app
    nuts_a.loc[(nuts_a.nuts_code.isnull())&(nuts_a.nuts_app.str.len()<nuts_a.nutsCode.str.len()), 'nuts_code'] = nuts_a.nutsCode
    nuts_a = (nuts_a.drop(columns=['nuts_app','nutsCode'])
                    .groupby(['project_id', 'proposal_orderNumber', 'generalPic','proposal_participant_pic'])
                    .agg(lambda x: ';'.join(x))
                    .reset_index())
    if len(nuts_a) != l:
            print("1- ATTENTION ! pp_app avec doublon -> revoir le code groupby")
    
    l=len(nuts_p)
    nuts_p = nuts_p.merge(pp_part, how='left', on=['project_id', 'generalPic', 'calculated_pic'])
    nuts_p.loc[nuts_p.nutsCode.isnull(), 'nuts_code'] = nuts_p.nuts_part
    nuts_p.loc[(nuts_p.nuts_code.isnull())&(nuts_p.nuts_part.isnull()), 'nuts_code'] = nuts_p.nutsCode
    nuts_p.loc[(nuts_p.nuts_code.isnull())&(nuts_p.nutsCode==nuts_p.nuts_part), 'nuts_code'] = nuts_p.nutsCode
    nuts_p.loc[(nuts_p.nuts_code.isnull())&(nuts_p.nuts_part.str[:2]!=nuts_p.nutsCode.str[:2]), 'nuts_code'] = nuts_p.nuts_part+';'+nuts_p.nutsCode
    nuts_p.loc[(nuts_p.nuts_code.isnull())&(nuts_p.nuts_part.str.len()==nuts_p.nutsCode.str.len()), 'nuts_code'] = nuts_p.nutsCode
    nuts_p.loc[(nuts_p.nuts_code.isnull())&(nuts_p.nuts_part.str.len()>nuts_p.nutsCode.str.len()), 'nuts_code'] = nuts_p.nuts_part
    nuts_p.loc[(nuts_p.nuts_code.isnull())&(nuts_p.nuts_part.str.len()<nuts_p.nutsCode.str.len()), 'nuts_code'] = nuts_p.nutsCode
    nuts_p = (nuts_p.drop(columns=['nuts_part','nutsCode'])
                    .groupby(['project_id', 'orderNumber', 'generalPic', 'calculated_pic'])
                    .agg(lambda x: ';'.join(x))
                    .reset_index())
    if len(nuts_p) != l:
            print("2- ATTENTION ! pp_part avec doublon -> revoir le code groupby")

    # lien = lien.reset_index()
    lien = (lien.merge(nuts_p, how='left', on=['project_id', 'orderNumber', 'generalPic', 'calculated_pic'])
            .merge(nuts_a, how='left', on=['project_id', 'proposal_orderNumber', 'generalPic', 'proposal_participant_pic'])
            .drop_duplicates())
    
    lien['participation_nuts'] = lien[['nuts_code_x', 'nuts_code_y']].stack().groupby(level=0).agg(';'.join)
    lien['participation_nuts'] = lien['participation_nuts'].apply(lambda x: ';'.join(set(x.split(';'))))

    lien.rename(columns={'nuts_code_x':'nuts_participant', 'nuts_code_y':'nuts_applicants'}, inplace=True)
    print(f"- size lien: {len(lien)}")
    return lien