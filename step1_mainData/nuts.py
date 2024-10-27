from config_path import PATH_SOURCE
from constant_vars import FRAMEWORK, ZIPNAME
from functions_shared import unzip_zip
import pandas as pd

def nuts_department():
        # bosser les nuts en amont pour récupérer le max d'infos sans attendre traitement departments
        pp_app = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'proposals_applicants_departments.json', 'utf8')
        pp_app = pd.DataFrame(pp_app)
        pp_app = (pp_app[['proposalNbr', 'generalPic', 'applicantPic', 'nutsCode']]
                .rename(columns={'proposalNbr':'project_id', 'applicantPic':'applicant_pic'})
                .astype(str)
                .drop_duplicates()
                .groupby(['project_id', 'generalPic', 'applicant_pic'])
                .agg(lambda x: ';'.join(x))
                .reset_index())
        print(f"- size pp_app: {len(pp_app)}")

        pp_part = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'projects_participants_departments.json', 'utf8')
        pp_part = pd.DataFrame(pp_part)
        pp_part = (pp_part[['projectNbr', 'generalPic', 'participantPic', 'nutsCode']]
                .rename(columns={'projectNbr':'project_id', 'participantPic':'participant_pic'})
                .astype(str)
                .drop_duplicates()
                .groupby(['project_id', 'generalPic', 'participant_pic'])
                .agg(lambda x: ';'.join(x))
                .reset_index())
        print(f"- size pp_part: {len(pp_part)}")
        return pp_app, pp_part

def nuts_lien(app1, part, lien):
    # from step1_mainData.nuts import nuts_department
        print("### LOADING DEPARTMENT")
        pp_app, pp_part = nuts_department()

        print("### NUTS avec LIEN")
        nuts_a=(app1[['project_id', 'orderNumber', 'generalPic', 'participant_pic','nutsCode']]
                .rename(columns={'orderNumber':'proposal_orderNumber','participant_pic':'applicant_pic', 'nutsCode':'nuts_app'})
                .drop_duplicates())

        nuts_p=(part[['project_id', 'orderNumber', 'generalPic', 'participant_pic','nutsCode']]
                .rename(columns={'nutsCode':'nuts_part'})
                .drop_duplicates())

        print(f"size nuts_a: {len(nuts_a)}, size nuts_p: {len(nuts_p)}")
        # size nuts_a: 463523, size nuts_p: 103661
        # pp_app: 347165,  pp_part: 88103
        if len(nuts_a) == len(nuts_a.merge(pp_app, how='left', on=['project_id', 'generalPic', 'applicant_pic'])):   
                nuts_a = nuts_a.merge(pp_app, how='left', on=['project_id', 'generalPic', 'applicant_pic'])
        else:
                print("- ATTENTION ! pp_app avec doublon -> revoir le code groupby")
        
        if len(nuts_p) == len(nuts_p.merge(pp_part, how='left', on=['project_id', 'generalPic', 'participant_pic'])):   
                nuts_p = nuts_p.merge(pp_part, how='left', on=['project_id', 'generalPic', 'participant_pic'])
        else:
                print("- ATTENTION ! pp_part avec doublon -> revoir le code groupby")

        # lien=(lien.merge(nuts_p, how='left', on=['project_id', 'orderNumber', 'generalPic', 'calculated_pic'])
        # .merge(nuts_a, how='left', on=['project_id', 'proposal_orderNumber', 'generalPic', 'proposal_participant_pic']))