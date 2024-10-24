import pandas as pd, numpy as np
from functions_shared import unzip_zip, del_list_in_col
from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE, PATH_CONNECT, PATH_CLEAN

def date_load():
    # creation de extractDate avec la date d'extraction d'ecorda format -> '2022-12-11'
    date = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'extractionDate.json', 'utf8')
    extractDate = list(set([i['extraction_date'] for i in date if i['framework']==FRAMEWORK]))[0]
    print(f"### LAST DATE of EXTRACTED DATA\n{[i for i in date if i['framework']==FRAMEWORK]}\n") 
    pd.DataFrame([i for i in date if i['framework']==FRAMEWORK]).to_json(f"{PATH_CONNECT}extractionDate.json", orient='records')
    return extractDate

def proj_load():
    print('### LOADING PROJECTS DATA')
    proj = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'projects.json', 'utf8')
    if proj is not None:
    #     # liste de projects pour verif dans proposals (project_liste.json)
        proj = pd.DataFrame(proj)
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
        
        print(f'result - dowloaded projects:{tot_pid}, retained projects:{len(proj)}, pb:{tot_pid-len(proj)}\n- liste des colonnes conservées: {proj.columns}\n')
        return proj
    

def prop_load(proj_id_signed):
    print('### LOADING PROPOSALS DATA')
    prop = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'proposals.json', 'utf8')

    def status_test(nb_status):
        if len(prop.stageExitStatus.unique()) != nb_status:
            stat =  ['REJECTED' ,'NO_MONEY' ,'MAIN', 'RESERVE', 'INELIGIBLE', 'WITHDRAWN', 'INADMISSIBLE', None]
            return print(f"STATUS - {len(prop.stageExitStatus.unique())-nb_status} statut de proposition a été ajouté à stageExitStatus ;\n vérifier s'il faut l'intégrer aux projets ELIGIBLE {prop.loc[~prop.stageExitStatus.isin(stat), 'stageExitStatus']}\n")
        else:
            pass

    if prop is not None:
        prop = pd.json_normalize(prop)
        tot_ppid = prop.proposalNbr.nunique()
        prop = prop.rename(columns={"proposalNbr": "project_id", "stage":"stage_call"})
        keep_eval = ['project_id','expertScore.total', 'expertScore.excellence', 'expertScore.impact', 'expertScore.quality', 'isEligibile', 'rank', 'stageExitStatus', 'isProject', 'isAboveTreshold']
        prop[prop.columns[prop.columns.isin(keep_eval)]].to_json(f"{PATH_CLEAN}proposal_evaluation.json", orient='records')
        prop = prop.assign(score=np.where(prop['expertScore.total'].isnull(), False, True))
        prop = prop.drop(['expertScore.total','expertScore.excellence','expertScore.impact','expertScore.quality','rank','isProject','isEligibile'],  axis=1)
        prop = del_list_in_col(prop, 'freeKeywords', 'freekw')
        prop = del_list_in_col(prop, 'eicPanels', 'eic_panels')
        prop.loc[:, "eic_panels"] = prop.loc[:, "eic_panels"].str.replace(' / ', '|')
        
        prop = prop.drop_duplicates()
        print(f'result - dowloaded proposals:{tot_ppid}, retained proposals:{len(prop)}, pb:{tot_ppid-len(prop)}')
        
        status_test(8)
        prop.loc[prop.project_id.isin(proj_id_signed), 'proposalStatus'] = 'MAIN'
        prop.loc[(prop.stageExitStatus=="MAIN")&(prop.proposalStatus!='MAIN'), 'proposalStatus'] = 'MAIN'
        l = ['INELIGIBLE', 'INADMISSIBLE', 'DUPLICATE','WITHDRAWN']
        prop1 = prop.loc[(~prop.stageExitStatus.isin(l))&(~prop.stageExitStatus.isnull())]
        prop1.loc[prop1.proposalStatus.isnull(), 'proposalStatus'] = prop1.stageExitStatus
        prop1 = prop1.assign(stage='evaluated')
        
        prop1.rename(columns={'proposalStatus':'status_code', 'scientificPanel':'panel_code', 'budget':'total_cost', 
                            'requestedGrant':'eu_reqrec_grant', 'numberOfApplicants':'number_involved'}, inplace=True)
        
        prop1=prop1.drop(columns=['isAboveTreshold','mgaTypeDescription','isSeoDuplicate','mgaTypeCode','stageExitStatus',
                                'stage_call', 'ecHiearchyResp', 'masterCallId', 'uniqueProgrammePart', 'score'])
        
        empty_cols=['isSeo']
        
        if empty_cols==[col for col in prop1.columns if prop1[col].isnull().all()]:
            prop1.drop(empty_cols, axis=1, inplace=True)
        elif empty_cols!=[col for col in prop1.columns if prop1[col].isnull().all()]:
            print(f"empty cols - Attention ! vérifier les variables manquantes->{[col for col in prop1.columns if prop1[col].isnull().all()]}")
        
    print(f"after cleaning - size prop1 without inadmissible/inegible/etc : {len(prop1)}\n")
    return prop, prop1