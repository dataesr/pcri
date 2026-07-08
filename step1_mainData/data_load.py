import pandas as pd, numpy as np, os
from functions_shared import unzip_zip, del_list_in_col, columns_comparison, gps_col, num_to_string, bugs_excel, clean_keyword, work_csv
from constant_vars import FRAMEWORK
from config_path import PATH_CONNECT, PATH_CLEAN

def date_load(source):
    """
    extract date of last update from extractionDate.json file in the source zip and save it in data_connect/extractionDate.json
    create extractDate format -> '2022-12-11'
    """
    date = unzip_zip(source, 'extractionDate.json', 'utf8')
    extractDate = list(set([i['extraction_date'] for i in date if i['framework']==FRAMEWORK]))[0]
    print(f"### LAST DATE of EXTRACTED DATA\n{[i for i in date if i['framework']==FRAMEWORK]}\n") 
    pd.DataFrame([i for i in date if i['framework']==FRAMEWORK]).to_json(f"{PATH_CONNECT}extractionDate.json", orient='records')
    with open("temp/extractDate.txt", "w", encoding="utf-8") as f:
        f.write(extractDate)
    return extractDate

def projects_load(source):

    """
    1. load projects data
    2. check projec duplicates by projectNbr and keep only the last update by lastUpdateDate
    3. check new columns -> data_files/projects_columns.json
    4. rename columns
    5. clean freeKeywords column 
    6. convert list in columns into string with | separator
    """

    print('### LOADING PROJECTS data')
    proj = unzip_zip(source, 'projects.json', 'utf8')

    if proj is not None:
        proj = pd.DataFrame(proj)
        proj['lastUpdateDate'] = pd.to_datetime(proj['lastUpdateDate'])

        # check if projectNbr is null or duplicated and keep only the last update by projectNbr -> csv with projectNbr null in data_work/projects_with_null_projectNbr and remove them from projects df
        if proj['projectNbr'].isnull().sum()>0:
            print(f"WARNING ! in projects load, {proj['projectNbr'].isnull().sum()} projectNbr null -> into data_work 'projects_with_null_projectNbr' ")    
            work_csv(proj[proj['projectNbr'].isnull()], 'projects_with_null_projectNbr')
            proj = proj.dropna(subset=['projectNbr'])
            
        tot_pid = proj.projectNbr.nunique()

        rep = [{'stage_process': '_loading', 'project_size': len(proj)}]

        if (len(proj.groupby('projectNbr').agg({'lastUpdateDate':'count'}).reset_index().query('lastUpdateDate>1'))>0) | (proj.projectNbr.isnull().sum()>0):
            proj=proj.sort_values(['projectNbr', 'lastUpdateDate'], ascending=[True, False]).drop_duplicates('projectNbr')
            print(f"ATTENTION ! proj load : {tot_pid}, after remove old records by lastUpdateDate {len(proj)}")
            print(f"new size : {len(proj)}")
            rep.append({'stage_process': '_without_old_data', 'project_size': len(proj)})
            tot_pid = proj.projectNbr.nunique()
            if len(proj.groupby('projectNbr').size().reset_index(name='row_count').query('row_count>1')>0):
                return print(f"ATTENTION ! project duplicated:\n{proj.groupby('projectNbr').size().reset_index(name='row_count').query('row_count>1')}")
                

        # check new columns -> data_files/projects_columns.json
        columns_comparison(proj, 'projects_columns')


        proj.rename(columns={"projectNbr": "project_id", "projectStatus":"status_code",'numberOfParticipants':'number_involved',
                            'totalCost':'total_cost', 'euContribution':'eu_reqrec_grant'}, inplace=True)
        
        proj['freeKeywords'] = proj['freeKeywords'].apply(lambda lst: [clean_keyword(k) for k in lst])
        proj = del_list_in_col(proj, 'freeKeywords', 'freekw')
        proj = proj.drop_duplicates()
        proj['stage'] = 'successful'
        
        empty_cols=['partnershipName', 'partnershipType']
        
        if empty_cols==[col for col in proj.columns if proj[col].isnull().all()]:
            proj.drop(empty_cols, axis=1, inplace=True)
        else:
            print(f"1- Attention ! vérifier les variables manquantes->{[col for col in proj.columns if proj[col].isnull().all()]}\n")
        
        cols_to_drop = ['comL2LocalKey', 'linkedFpaProjectNbr', 'contractVersion', 'masterCallId', 
                        'ecHiearchyResp', 'uniqueProgrammePart']
        proj.drop(columns=[col for col in cols_to_drop if col in proj.columns], inplace=True)
        # proj.drop(columns=['comL2LocalKey', 'linkedFpaProjectNbr', 'contractVersion', 'masterCallId', 'ecHiearchyResp', 'uniqueProgrammePart'], inplace=True)
        
        proj['project_id'] = proj['project_id'].map(num_to_string)

        if tot_pid != len(proj):
            print("ATTENTION ! projects losted between load and first treatment")
            return
        else:
            print(f'- result -> dowloaded projects:{tot_pid}, retained projects:{len(proj)}, pb:{tot_pid-len(proj)}\n- liste des colonnes conservées:\n{proj.columns}')
            rep.append({'stage_process': 'process1', 'project_size': len(proj)})
            return proj, rep
    

def proposals_load(source):
    """
    1. load proposals data
    2. check proposal duplicated by proposalNbr and keep only the last update by lastUpdateDate
    3. check new columns -> data_files/proposals_columns.json
    4. rename columns
    5. keep only evaluation variables and save them in data_clean/proposal_evaluation.json
    6. clean freeKeywords column
    7. convert list in columns into string with | separator
    8. remove variables not usefull for the analysis and empty columns     
    """

    print('\n### LOADING PROPOSALS data')
    prop = unzip_zip(source, 'proposals.json', 'utf8')

    if prop is not None:
        prop = pd.json_normalize(prop)
        prop['lastUpdateDate'] = pd.to_datetime(prop['lastUpdateDate'])
        tot_ppid = prop.proposalNbr.nunique()
        rep = [{'stage_process': '_loading', 'proposal_size': len(prop)}]

        if len(prop.groupby('proposalNbr').agg({'lastUpdateDate':'count'}).reset_index().query('lastUpdateDate>1'))>0:
            prop = prop.sort_values(['proposalNbr', 'lastUpdateDate'], ascending=[True, False]).drop_duplicates('proposalNbr')
            print(f"ATTENTION ! prop load : {tot_ppid}, after remove old records by lastUpdateDate {len(prop)}")
            print(f"new size : {len(prop)}")
            rep.append({'stage_process': '_without_old_data', 'proposal_size': len(prop)})
            tot_ppid = prop.proposalNbr.nunique()
            if len(prop.groupby('proposalNbr').size().reset_index(name='row_count').query('row_count>1')>0):
                return print(f"ATTENTION ! proposals duplicated:\n{prop.groupby('proposalNbr').size().reset_index(name='row_count').query('row_count>1')}")
                

        # new columns 
        columns_comparison(prop, 'proposals_columns')

        prop = prop.rename(columns={"proposalNbr": "project_id", "stage":"stage_call"})
        keep_eval = ['project_id','expertScore.total', 'expertScore.excellence', 'expertScore.impact', 'expertScore.quality', 'isEligibile', 'rank', 'stageExitStatus', 'isProject', 'isAboveTreshold']
        prop[prop.columns[prop.columns.isin(keep_eval)]].to_json(f"{PATH_CLEAN}proposal_evaluation.json", orient='records')
        prop = prop.assign(score=np.where(prop['expertScore.total'].isnull(), False, True))

        cols_to_drop = ['expertScore.total','expertScore.excellence','expertScore.impact','expertScore.quality','rank','isProject','isEligibile']
        prop.drop(columns=[col for col in cols_to_drop if col in prop.columns], inplace=True)
        
        prop['freeKeywords'] = prop['freeKeywords'].apply(lambda lst: [clean_keyword(k) for k in lst])
        prop = del_list_in_col(prop, 'freeKeywords', 'freekw')
        prop = del_list_in_col(prop, 'eicPanels', 'eic_panels')
        prop.loc[:, "eic_panels"] = prop.loc[:, "eic_panels"].str.replace(' / ', '|')
        
        prop.rename(columns={'scientificPanel':'panel_code', 'budget':'total_cost', 'proposalsStep':'erc_evaluation_step',
                            'requestedGrant':'eu_reqrec_grant', 'numberOfApplicants':'number_involved'}, inplace=True)
        
        cols_to_drop = ['isAboveTreshold','mgaTypeDescription','isSeoDuplicate','mgaTypeCode',
                        'ecHiearchyResp', 'masterCallId', 'uniqueProgrammePart', 'score']
        prop.drop(columns=[col for col in cols_to_drop if col in prop.columns], inplace=True)
        
        empty_cols=['isSeo']
        
        if empty_cols==[col for col in prop.columns if prop[col].isnull().all()]:
            prop.drop(empty_cols, axis=1, inplace=True)
        elif empty_cols!=[col for col in prop.columns if prop[col].isnull().all()]:
            print(f"1- empty cols -> Attention ! vérifier les variables manquantes->{[col for col in prop.columns if prop[col].isnull().all()]}")
        
        prop['project_id'] = prop['project_id'].map(num_to_string)
        prop = prop.drop_duplicates()

        if tot_ppid != len(prop):
            print("ATTENTION ! proposals losted between load and first treatment")
            return
        else:
            print(f'- result -> dowloaded proposals:{tot_ppid}, retained proposals:{len(prop)}, pb:{tot_ppid-len(prop)}')
            rep.append({'stage_process': 'process1', 'proposal_size': len(prop)})
            return prop, rep

def participants_load(source):
    """
    1. load participants data
    2. check duplicated records by projectNbr, orderNumber, generalPic, participantPic, partnerRole, partnerType and keep only the last update by lastUpdateDate
    2. check new columns -> data_files/participants_columns.json
    3. remove variables not usefull for the analysis and empty columns
    4. rename columns
    5. remove participants with partnerRemovalStatus not null -> check new modalities ?
    6. convert numeric variables into string to avoid problem of merge with projects and proposals data (projectNbr, orderNumber, generalPic, participantPic, parentPic) and replace empty string by null value

    """
    print("\n### LOADING PARTICIPANTS data")
    part = unzip_zip(source, "projects_participants.json", 'utf8')

    if part:
        part = pd.DataFrame(part)
        part['lastUpdateDate'] = pd.to_datetime(part['lastUpdateDate'])
        tot_pid = len(part)
        rep = [{'stage_process': '_loading', 'participant_size': len(part)}]

        if len(part.groupby(['projectNbr','orderNumber', 'generalPic', 'participantPic', 'partnerRole', 'partnerType']).agg({'lastUpdateDate':'count'}).reset_index().query('lastUpdateDate>1'))>0:
            part = (part.sort_values(['projectNbr','orderNumber', 'generalPic', 'participantPic', 'partnerRole', 'partnerType', 'lastUpdateDate'], 
                                     ascending=[True,True,True,True,True,True,False])
                                     .drop_duplicates(['projectNbr','orderNumber', 'generalPic', 'participantPic', 'partnerRole', 'partnerType']))
            print(f"ATTENTION ! proj load : {tot_pid}, after remove old records by lastUpdateDate {len(part)}")
            print(f"new size : {len(part)}")
            rep.append({'stage_process': '_without_old_data', 'participant_size': len(part)})
            tot_pid = len(part[['projectNbr','orderNumber', 'generalPic', 'participantPic', 'partnerRole', 'partnerType']].drop_duplicates())
            if len(part.groupby(['projectNbr','orderNumber', 'generalPic', 'participantPic', 'partnerRole', 'partnerType']).size().reset_index(name='row_count').query('row_count>1')>0):
                return print(f"ATTENTION ! participant duplicated:\n{part.groupby(['projectNbr','orderNumber', 'generalPic', 'participantPic', 'partnerRole', 'partnerType']).size().reset_index(name='row_count').query('row_count>1')}")
                

        # new columns 
        columns_comparison(part, 'participants_columns')    

        #empty columns
        empty_cols=[col for col in part.columns if part[col].isnull().all()]
        if empty_cols:
            part.drop(empty_cols, axis=1, inplace=True)
            print(f"Empty cols dropped, check the list -> {empty_cols}")
       

        tot_pid = len(part[['projectNbr','orderNumber', 'generalPic', 'participantPic', 'partnerRole', 'partnerType']].drop_duplicates())
        part = part.rename(columns={"projectNbr": "project_id", "participantPic": "participant_pic", 
                                    'partnerRole': 'role', 'participantLegalName': 'name'})

        # remove participant with partnerRemovalStatus not null
        print(f"- subv_net avant traitement: {'{:,.1f}'.format(part['netEuContribution'].sum())}")
        length_removalstatus=len(part[~part['partnerRemovalStatus'].isnull()])
        print(f"- suppression de {length_removalstatus},\nmodalités: {part[~part['partnerRemovalStatus'].isnull()][['partnerRemovalStatus']].value_counts()}")
        if part['partnerRemovalStatus'].nunique()==2:
            part = part[part['partnerRemovalStatus'].isnull()]
        else:
            print(f"2- Nouvelle modalité dans partnerRemovalStatus : {part['partnerRemovalStatus'].unique()}")
            part = part[part['partnerRemovalStatus'].isnull()]

        rep.append({'stage_process': 'process2_status', 'participant_size': len(part)})

        c = ['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'parentPic']
        part[c] = part[c].fillna('').map(num_to_string)
        
        part = part.mask(part == '')

        part = gps_col(part)

        cont_sum = '{:,.1f}'.format(part['euContribution'].sum())
        net_cont_sum = '{:,.1f}'.format(part['netEuContribution'].sum())
        
        print(f"- result -> dowloaded:{tot_pid}, retained part:{len(part)}, pb:{tot_pid-len(part)}, somme euContribution:{cont_sum}, somme netEuContribution:{net_cont_sum}")
        # print(f"- montant normalement définif des subv_net (suite lien avec projects propres): {'{:,.1f}'.format(part.loc[part.project_id.isin(projects.loc[projects.stage=='successful'].project_id.unique()), 'netEuContribution'].sum())}")
        rep.append({'stage_process': 'process1', 'participant_size': len(part)})
        return part, rep
    

def applicants_load(source):
    """
    1. load applicants data
    2. check duplicated records by proposalNbr, orderNumber, generalPic, applicantPic, role and keep only the last update by lastUpdateDate
    3. check new columns -> data_files/applicants_columns.json
    4. remove variables not usefull for the analysis and empty columns
    5. rename columns
    6. convert numeric variables into string to avoid problem of merge with applicants and proposals data
    
    """
    print("\n### LOADING APPLICANTS data")
    app = unzip_zip(source, "proposals_applicants.json", 'utf8')

    print(f"- size app au chargement: {len(app)}")

    if app is not None:
        app = pd.DataFrame(app)
        app['lastUpdateDate'] = pd.to_datetime(app['lastUpdateDate'])
        tot_pid = len(app)
        rep = [{'stage_process': '_loading', 'applicant_size': len(app)}]

        if len(app.groupby(['proposalNbr','orderNumber', 'generalPic', 'applicantPic', 'role']).agg({'lastUpdateDate':'count'}).reset_index().query('lastUpdateDate>1'))>0:
            app = (app.sort_values(['proposalNbr','orderNumber', 'generalPic', 'applicantPic', 'role', 'lastUpdateDate'], 
                                     ascending=[True,True,True,True,True,False])
                                     .drop_duplicates(['proposalNbr','orderNumber', 'generalPic', 'applicantPic', 'role']))
            print(f"ATTENTION ! proj load : {tot_pid}, after remove old records by lastUpdateDate {len(app)}")
            print(f"new size : {len(app)}")
            rep.append({'stage_process': '_without_old_data', 'applicant_size': len(app)})
            tot_pid = len(app[['proposalNbr','orderNumber', 'generalPic', 'applicantPic', 'role']].drop_duplicates())
            if len(app.groupby(['proposalNbr','orderNumber', 'generalPic', 'applicantPic', 'role']).size().reset_index(name='row_count').query('row_count>1')>0):
                return print(f"ATTENTION ! participant duplicated:\n{app.groupby(['proposalNbr','orderNumber', 'generalPic', 'applicantPic', 'role']).size().reset_index(name='row_count').query('row_count>1')}")
                

        # new columns 
        columns_comparison(app, 'applicants_columns')  

        empty_cols=[col for col in app.columns if app[col].isnull().all()]
        if empty_cols:
            print(f"Empty cols, check the list: {empty_cols}")

        tot_pid = len(app[['proposalNbr','orderNumber', 'generalPic', 'applicantPic', 'role']].drop_duplicates())
        app = app.rename(columns={"proposalNbr": "project_id", "applicantPic": "participant_pic", 
                                'applicantPicLegalName': 'name'})

        print(f"- var with null: {app.columns[app.isnull().any()].tolist()}")
        c = ['project_id', 'orderNumber', 'generalPic', 'participant_pic']
        app[c] = app[c].map(num_to_string)     
                      
        app = gps_col(app)

        app_sum = '{:,.1f}'.format(app['requestedGrant'].sum())

        print(f"- result -> dowloaded:{tot_pid}, retained app:{len(app)}, pb:{tot_pid-len(app)}, , somme requestedGrant:{app_sum}")
        rep.append({'stage_process': 'process1', 'applicant_size': len(app)})
        return app, rep
    

    def cascading_projects(source):
        print("\n### LOADING APPLICANTS data")
        casc_pp = unzip_zip(source, "proposals_cascadingProposals.json", 'utf8')
        casc_app = unzip_zip(source, "proposals_applicants_cascadingApplicants.json", 'utf8')
        casc_p = unzip_zip(source, "projects_cascadingProjects.json", 'utf8')
        casc_part = unzip_zip(source, "projects_participants_cascadingParticipants.json", 'utf8')
    
    