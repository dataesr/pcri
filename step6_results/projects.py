import pandas as pd, os
from config_path import PATH_SOURCE, PATH_WORK
from functions_shared import zipfile_ods, cols_order, load_last_file_csv, cols_select

def projects_ods(projects, participation, calls, countries, h20_p, FP6_p, FP7_p):
    ###projects info for ODS
    cc = countries[['countryCode_iso3', 'country_name_en', 'country_name_fr']].drop_duplicates()
    part= (participation.loc[participation.stage=='successful', 
                ['project_id', 'country_code', 'country_code_mapping', 
                 'participation_nuts', 'region_1_name', 'region_2_name', 
                 'regional_unit_name']].drop_duplicates()
    .merge(cc[['countryCode_iso3','country_name_fr']]
           .rename(columns={'countryCode_iso3':'country_code'}), how='left', on='country_code')
    .merge(cc[['countryCode_iso3', 'country_name_en']]
           .rename(columns={'countryCode_iso3':'country_code_mapping', 'country_name_en':'country_name_mapping'}), 
           how='left', on='country_code_mapping')
    .groupby(['project_id'], as_index = False).agg(lambda x: ';'.join(map(str, filter(None, x)))))

    #recuperation des données proposals à afficher
    tmpP=(projects.loc[projects.stage=="evaluated", 
                ['project_id','submission_date', 'total_cost', 'eu_reqrec_grant', 
                 'number_involved']]
        .rename(columns={'total_cost':'proposal_budget', 
                         'eu_reqrec_grant':'proposal_requestedgrant', 
                         'number_involved':'proposal_numberofapplicants'}))

    tmp=projects.loc[(projects.stage=='successful')&(projects.status_code!='UNDER_PREPARATION') ]

    tmp=(tmp.merge(calls[['call_id', 'call_deadline', 'expectedNbrProposals', 'call_budget']].drop_duplicates(), 
                how='left', on=['call_id', 'call_deadline'])
            .merge(part, how='inner', on='project_id'))

    cols_h=cols_select('horizon', 'proj_info')
    rename_map=cols_h[cols_h.horizon.notna()].set_index('horizon')['vars'].to_dict()
    tmp=tmp.rename(columns=rename_map)
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


    file_prefix = 'data_cordis_check_cordis'
    cordis = load_last_file_csv(PATH_SOURCE, file_prefix, sep=',')
    cordis['project_id']=cordis['project_id'].astype('str')
    tmp = tmp.merge(cordis[cordis.cordis_webPage_status==200].drop_duplicates(), how='left', on='project_id')
    tmp.loc[tmp.cordis_webPage_status==200, 'cordis_project_webpage'] = "https://cordis.europa.eu/project/id/"+tmp.project_id.astype('str').str.strip()
    
    tmp.mask(tmp=='', inplace=True)    

    pg = pd.read_table('data_files/pilier_global.txt', sep='\t')
    tmp = tmp.merge(pg, how='left', on='pilier_name_en').drop_duplicates()

    # attention si changement de nom de vars -> la modifier aussi dans pcri_info_columns_order
    projects_all = cols_order(tmp, 'proj_info')

    if any(projects_all['project_id'].value_counts()[projects_all['project_id'].value_counts()> 1]):
        print(f"- si ++ rows for a project_id, not normal: \n{projects_all['project_id'].value_counts()[projects_all['project_id'].value_counts()> 1]}")

    print(projects_all.drop_duplicates().framework.value_counts(dropna=False))
    # projects_all.to_pickle(f"{PATH_WORK}projects_all_FW.pkl")
    # projects_all.to_csv(f"{PATH_ODS}fr-esr-all-projects-signed-informations.csv", sep=';', encoding='utf-8', index=False, na_rep='', decimal=",")
    zipfile_ods(projects_all, 'fr-esr-all-projects-signed-informations')
    return projects_all