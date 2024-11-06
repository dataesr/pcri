import pandas as pd
from config_path import PATH_SOURCE, PATH_WORK
from functions_shared import zipfile_ods, order_columns

def projects_ods(projects, participation, calls, countries, h20_p, FP6_p, FP7_p):
    ###projects info for ODS
    cc = countries.drop(columns=['countryCode', 'country_name_mapping','country_code_mapping']).drop_duplicates()
    part= (participation.loc[participation.stage=='successful', ['project_id', 'country_code', 'country_code_mapping', 'participation_nuts', 'region_1_name', 'region_2_name', 'regional_unit_name']].drop_duplicates()
    .merge(cc[['country_code','country_name_fr']], how='left', on='country_code')
    .merge(countries[['country_code_mapping', 'country_name_mapping']], how='left', on='country_code_mapping')
    .groupby(['project_id'], as_index = False).agg(lambda x: ';'.join(map(str, filter(None, x)))))

    #recuperation des données proposals à afficher
    tmpP=(projects.loc[projects.stage=="evaluated", ['project_id','submission_date', 'total_cost', 'eu_reqrec_grant', 'number_involved']]
        .rename(columns={'total_cost':'proposal_budget', 'eu_reqrec_grant':'proposal_requestedgrant', 'number_involved':'proposal_numberofapplicants'}))

    tmp=projects.loc[(projects.stage=='successful')&(projects.status_code!='UNDER_PREPARATION') ]

    tmp=(tmp.merge(calls[['call_id', 'call_deadline', 'expectedNbrProposals', 'call_budget']].drop_duplicates(), 
                how='left', on=['call_id', 'call_deadline'])
            .merge(part, how='inner', on='project_id'))


    tmp=(tmp[['framework', 'thema_name_fr',  'destination_name_en', 'project_id', 'acronym','title', 'abstract', 
            'total_cost', 'eu_reqrec_grant', 'number_involved', 'project_webpage', 
            'call_year','call_deadline', 'call_id', 'topic_code', 'expectedNbrProposals', 'call_budget', 
            'status_code', 'signature_date', 'start_date','end_date', 'duration', 
            'pilier_name_en', 'pilier_name_fr', 'programme_name_en', 'programme_name_fr', 'thema_code', 
            'thema_name_en', 'destination_code', 'destination_detail_code', 'destination_detail_name_en', 
            'action_code', 'action_name', 'action_code2', 'action_name2', 'topic_name',
            'panel_code', 'panel_name', 'panel_regroupement_code', 'panel_regroupement_name','panel_description',
            'free_keywords', 'eic_panels', 'participation_nuts', 'region_1_name', 'region_2_name', 'regional_unit_name',
            'country_code', 'country_name_fr', 'country_code_mapping', 'country_name_mapping', 
            'ecorda_date']]
        .rename(columns={
        'eu_reqrec_grant':'project_eucontribution',
        'total_cost':'project_totalcost',
    #     'freekw': 'free_keywords',
        'number_involved':'project_numberofparticipants',
        'action_code2':'action_detail_code', 
        'action_name2':'action_detail_name',
        'expectedNbrProposals':'proposal_expected_number'})

        )

    tmp=tmp.merge(tmpP, how='left', on='project_id')
    
    tmp.reset_index(drop=True, inplace=True)
    h20_p.reset_index(drop=True, inplace=True)
    FP7_p.reset_index(drop=True, inplace=True)
    FP6_p.reset_index(drop=True, inplace=True)
    tmp = pd.concat([tmp, h20_p, FP7_p, FP6_p], axis=0, join='outer')

    tmp = tmp.loc[tmp.status_code!='REJECTED']

    for i in ['title','abstract', 'free_keywords', 'eic_panels', 'project_webpage', 'topic_name']:
        tmp[i] = tmp[i].str.replace('\\n|\\t|\\r|\\s+', ' ', regex=True).str.strip()

    tmp['free_keywords'] = tmp['free_keywords'].str.lower()

    for i in ['duration']:
        tmp[i] = tmp[i].astype('float')
        
    tmp['call_budget'] = tmp.call_budget.astype('str').str.replace(r'\\.[0-9]*', '', regex=True).astype('float')

    cordis = pd.read_csv(f"{PATH_SOURCE}cordis_status.csv", sep=',', dtype = {'project_id':'str'}).drop_duplicates()
    tmp = tmp.merge(cordis[cordis.cordis_webPage_status==200], how='left', on='project_id')
    tmp.loc[tmp.cordis_webPage_status==200, 'cordis_project_webpage'] = "https://cordis.europa.eu/project/id/"+tmp.project_id.astype('str').str.strip()
    
    tmp.mask(tmp=='', inplace=True)    

    pg = pd.read_table('data_json/pilier_global.txt', sep='\t')
    tmp = tmp.merge(pg, how='left', on='pilier_name_en')

    # attention si changement de nom de vars -> la modifier aussi dans pcri_info_columns_order
    projects_all = order_columns(tmp, 'proj_info')

    if any(projects_all['project_id'].value_counts()[projects_all['project_id'].value_counts()> 1]):
        print(projects_all['project_id'].value_counts()[projects_all['project_id'].value_counts()> 1])

    projects_all.to_pickle(f"{PATH_WORK}projects_all_FW.pkl")
    # projects_all.to_csv(f"{PATH_ODS}fr-esr-all-projects-signed-informations.csv", sep=';', encoding='utf-8', index=False, na_rep='', decimal=",")
    zipfile_ods(projects_all, 'fr-esr-all-projects-signed-informations')
    return projects_all