import pandas as pd, numpy as np, os
from functions_shared import unzip_zip, del_list_in_col, columns_comparison, gps_col, num_to_string, bugs_excel
from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE, PATH_CONNECT, PATH_CLEAN

def date_load():
    # creation de extractDate avec la date d'extraction d'ecorda format -> '2022-12-11'
    date = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'extractionDate.json', 'utf8')
    extractDate = list(set([i['extraction_date'] for i in date if i['framework']==FRAMEWORK]))[0]
    print(f"### LAST DATE of EXTRACTED DATA\n{[i for i in date if i['framework']==FRAMEWORK]}\n") 
    pd.DataFrame([i for i in date if i['framework']==FRAMEWORK]).to_json(f"{PATH_CONNECT}extractionDate.json", orient='records')
    return extractDate

def projects_load():
    print('### LOADING PROJECTS data')
    proj = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'projects.json', 'utf8')

    if proj is not None:
        proj = pd.DataFrame(proj)

        # new columns 
        columns_comparison(proj, 'projects_columns')

        tot_pid = proj.projectNbr.nunique()
        proj.rename(columns={"projectNbr": "project_id", "projectStatus":"status_code",'numberOfParticipants':'number_involved',
                            'totalCost':'total_cost', 'euContribution':'eu_reqrec_grant'}, inplace=True)
        proj = del_list_in_col(proj, 'freeKeywords', 'freekw')
        proj = proj.drop_duplicates()
        proj['stage'] = 'successful'
        
        empty_cols=['partnershipName', 'partnershipType']
        
        if empty_cols==[col for col in proj.columns if proj[col].isnull().all()]:
            proj.drop(empty_cols, axis=1, inplace=True)
        else:
            print(f"1 - Attention ! vérifier les variables manquantes->{[col for col in proj.columns if proj[col].isnull().all()]}\n")
        
        proj.drop(columns=['comL2LocalKey', 'linkedFpaProjectNbr', 'contractVersion', 'masterCallId', 'ecHiearchyResp', 'uniqueProgrammePart'], inplace=True)
        
        proj['project_id'] = proj['project_id'].map(num_to_string)

        print(f'result - dowloaded projects:{tot_pid}, retained projects:{len(proj)}, pb:{tot_pid-len(proj)}\n- liste des colonnes conservées: {proj.columns}\n')
        return proj
    

def proposals_load():
    print('### LOADING PROPOSALS data')
    prop = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'proposals.json', 'utf8')

    if prop is not None:
        prop = pd.json_normalize(prop)

        # new columns 
        columns_comparison(prop, 'proposals_columns')

        tot_ppid = prop.proposalNbr.nunique()
        prop = prop.rename(columns={"proposalNbr": "project_id", "stage":"stage_call"})
        keep_eval = ['project_id','expertScore.total', 'expertScore.excellence', 'expertScore.impact', 'expertScore.quality', 'isEligibile', 'rank', 'stageExitStatus', 'isProject', 'isAboveTreshold']
        prop[prop.columns[prop.columns.isin(keep_eval)]].to_json(f"{PATH_CLEAN}proposal_evaluation.json", orient='records')
        prop = prop.assign(score=np.where(prop['expertScore.total'].isnull(), False, True))
        prop = prop.drop(['expertScore.total','expertScore.excellence','expertScore.impact','expertScore.quality','rank','isProject','isEligibile'],  axis=1)
        prop = del_list_in_col(prop, 'freeKeywords', 'freekw')
        prop = del_list_in_col(prop, 'eicPanels', 'eic_panels')
        prop.loc[:, "eic_panels"] = prop.loc[:, "eic_panels"].str.replace(' / ', '|')
        
        prop.rename(columns={'proposalStatus':'status_code', 'scientificPanel':'panel_code', 'budget':'total_cost', 
                            'requestedGrant':'eu_reqrec_grant', 'numberOfApplicants':'number_involved'}, inplace=True)
        
        prop=prop.drop(columns=['isAboveTreshold','mgaTypeDescription','isSeoDuplicate','mgaTypeCode',
                                'stage_call', 'ecHiearchyResp', 'masterCallId', 'uniqueProgrammePart', 'score'])
        
        empty_cols=['isSeo']
        
        if empty_cols==[col for col in prop.columns if prop[col].isnull().all()]:
            prop.drop(empty_cols, axis=1, inplace=True)
        elif empty_cols!=[col for col in prop.columns if prop[col].isnull().all()]:
            print(f"empty cols - Attention ! vérifier les variables manquantes->{[col for col in prop.columns if prop[col].isnull().all()]}")
        
        prop['project_id'] = prop['project_id'].map(num_to_string)
        prop = prop.drop_duplicates()

        print(f'result - dowloaded proposals:{tot_ppid}, retained proposals:{len(prop)}, pb:{tot_ppid-len(prop)}')
        return prop

def participants_load(proj):
    print("### LOADING PARTICIPANTS data")
    part = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", "projects_participants.json", 'utf8')
        
    if part:
        part = pd.DataFrame(part)
        
        # new columns 
        columns_comparison(part, 'participants_columns')    

        empty_cols=['partnershipName', 'partnerSgaStatus']
    
        if empty_cols==[col for col in part.columns if part[col].isnull().all()]:
            part.drop(empty_cols, axis=1, inplace=True)
        else:
            print(f"Attention ! vérifier les variables manquantes->{[col for col in part.columns if part[col].isnull().all()]}")
       
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

        c = ['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'parentPic']
        part[c] = part[c].map(num_to_string)
        
        # controle des projets entre projects et participants
        tmp=(part[['project_id']].drop_duplicates()
            .merge(proj[['project_id', 'callId', 'acronym']], how='outer', on='project_id', indicator=True))
        if not tmp.query('_merge == "right_only"').empty:
            print("- projets dans projects sans participants") 
            t=tmp.query('_merge == "right_only"').drop(columns='_merge')
            bugs_excel(t, PATH_SOURCE, 'proj_without_part')
            # if not os.path.exists(f"{PATH_SOURCE}bugs_found.xlsx"):
            #     with pd.ExcelWriter(f"{PATH_SOURCE}bugs_found.xlsx") as writer:
            #         tmp.query('_merge == "right_only"').drop(columns='_merge').to_excel(writer, sheet_name='proj_without_part')
            
        elif not tmp.query('_merge == "left_only"').empty:
            print("- projets dans participants et pas dans projects")
            t=tmp.query('_merge == "left_only"').drop(columns='_merge')
            bugs_excel(t, PATH_SOURCE, 'part_without_info_proj')
            # with pd.ExcelWriter(f"{PATH_SOURCE}bugs_found.xlsx") as writer:  
            #     tmp.query('_merge == "left_only"').drop(columns='_merge').to_excel(writer, sheet_name='part_without_info_proj')  

        part = gps_col(part)

        cont_sum = '{:,.1f}'.format(part['euContribution'].sum())
        net_cont_sum = '{:,.1f}'.format(part['netEuContribution'].sum())
        
        print(f"- result - dowloaded:{tot_pid}, retained part:{len(part)}, pb:{tot_pid-len(part)}, somme euContribution:{cont_sum}, somme netEuContribution:{net_cont_sum}")
        # print(f"- montant normalement définif des subv_net (suite lien avec projects propres): {'{:,.1f}'.format(part.loc[part.project_id.isin(projects.loc[projects.stage=='successful'].project_id.unique()), 'netEuContribution'].sum())}")
        return part
    

def applicants_load(prop):
    print("### LOADING APPLICANTS data")
    app = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", "proposals_applicants.json", 'utf8')

    print(f"- size app au chargement: {len(app)}")

    if app is not None:
        app = pd.DataFrame(app)

        # new columns 
        columns_comparison(app, 'applicants_columns')  

        if len([col for col in app.columns if app[col].isnull().all()])>0:
            print(f"- Attention colonnes vides dans applicants ; faire code: {[col for col in app.columns if app[col].isnull().all()]}")

        tot_pid = len(app[['proposalNbr','orderNumber', 'generalPic', 'applicantPic', 'role']].drop_duplicates())
        app = app.rename(columns={"proposalNbr": "project_id", "applicantPic": "participant_pic", 
                                'applicantPicLegalName': 'name'})

        c = ['project_id', 'orderNumber', 'generalPic', 'participant_pic']
        app[c] = app[c].map(num_to_string)     
        
        # controle des projets entre projects et applicants
        tmp = app[['project_id']].drop_duplicates().merge(prop[['project_id', 'callId']], how='outer', on='project_id', indicator=True)
        if not tmp.query('_merge == "right_only"').empty:
            print("- project_id dans proposals sans applicants")
            t=tmp.query('_merge == "right_only"').drop(columns='_merge')
            bugs_excel(t, PATH_SOURCE, 'prop_without_app')
            # with pd.ExcelWriter(f"{PATH_SOURCE}bugs_found.xlsx") as writer:  
            #     tmp.query('_merge == "right_only"').drop(columns='_merge').to_excel(writer, sheet_name='prop_without_app')
        elif not tmp.query('_merge == "left_only"').empty:
            print(f"- project_id uniques dans applicants et pas dans proposals")
            t=tmp.query('_merge == "left_only"').drop(columns='_merge')
            bugs_excel(t, PATH_SOURCE, 'app_without_info_prop')
            # with pd.ExcelWriter(f"{PATH_SOURCE}bugs_found.xlsx") as writer:  
            #     tmp.query('_merge == "left_only"').drop(columns='_merge').to_excel(writer, sheet_name='app_without_info_prop')        
                
        app = gps_col(app)

        app_sum = '{:,.1f}'.format(app['requestedGrant'].sum())

        print(f"- result - dowloaded:{tot_pid}, retained app:{len(app)}, pb:{tot_pid-len(app)}, , somme requestedGrant:{app_sum}")
        # print(f"- montant des subv_dem (suite lien avec projects propres): {'{:,.1f}'.format(app.loc[app.project_id.isin(projects.loc[projects.stage=='evaluated'].project_id.unique()), 'requestedGrant'].sum())}")
        return app