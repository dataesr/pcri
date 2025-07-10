from main_library import *
import copy
pd.options.mode.copy_on_write = True

# if new update change constant_vars.py

UPDATE_PROJECT=False
UPDATE_PARTICIPANT=False
UPDATE_FP=False

if UPDATE_PROJECT==True:
    #################################
#     # If new year to load
#     wp_year='2025'
#     url=f'https://research-and-innovation.ec.europa.eu/funding/funding-opportunities/funding-programmes-and-open-calls/horizon-europe/horizon-europe-work-programmes_en#pre-publication-of-work-programme-{wp_year}'
#     calls_by_wp(url, wp_year)

#     get_topic_info_europa('HORIZON')

    ################################

    ## data load / adjustements*
    reporting = []

    extractDate = date_load()

    proj, rep = projects_load()
    proj_id_signed = proj.project_id.unique()
    reporting.extend(rep)

    prop, rep = proposals_load()
    reporting.extend(rep)

    proj = proj_add_cols(prop, proj)

    stage_p = ['REJECTED' ,'NO_MONEY' ,'MAIN', 'RESERVE', 'INELIGIBLE', 'WITHDRAWN', 'INADMISSIBLE', None]
    prop1, rep = proposals_status(prop, proj_id_signed, stage_p) 
    reporting.extend(rep) 
    # np.save("data_files/applicants_columns.npy", prop_cols)

    ###########################################
    # proposals fix
    # projects missing from proposals
    call_to_integrate, call_miss = proposals_id_missing(prop1, proj, extractDate)

    # project data missing in proposals if call already in dproposals then add this
    proj1 = proj_id_miss_fixed(prop1, proj, call_to_integrate)
    call_miss = list(set(call_miss)-set(call_to_integrate))
    with open('data_files/calls_missing_proposals.txt', 'w+') as f:
        for items in call_miss:
            f.write('%s\n' %items)
        print("File written successfully")
    f.close() 
    # proj = proj.loc[~proj.callId.isin(call_miss)]

    # merge proj + prop
    print('### MERGED PROPOSALS/PROJECTS')
    if len(proj1)==0:
        df = pd.concat([proj, prop1], ignore_index= True)
    else:
        df = pd.concat([prop1, proj1, proj], ignore_index = True)

    df = df.loc[~((df.status_code=='REJECTED')&(df.stage=='successful'))]
    print(f"- result - merged all: {len(df)},\n{df[['stage','status_code']].value_counts()}")
    reporting.extend([{'stage_process':'process3_add_miss_proj', 'proposal_size':len(df[df.stage=='evaluated'])},
                    {'stage_process': 'process2_status', 'project_size': len(df[df.stage=='successful'])},
                    {'stage_process': 'process4_merge', 'merded_size': len(df)}])

    merged = copy.deepcopy(df)
    merged = dates_year(merged)
    reporting.append({'stage_process':'process5_date_clean', 'merged_size':len(merged)})
    merged = strings_v(merged)
    merged = url_to_clean(merged)
    merged.mask(merged=='', inplace=True)
    merged = empty_str_to_none(merged)      
    merged.rename(columns={
        'freekw':'free_keywords',
        'callDeadlineDate':'call_deadline', 
        'callId':'call_id', 
        'submissionDate':'submission_date',
        'startDate':'start_date',
        'endDate':'end_date', 
        'ecSignatureDate':'signature_date'}, inplace=True)

    if any(merged.loc[merged.stage=='successful', 'project_id'].value_counts()[merged.loc[merged.stage=='successful', 'project_id'].value_counts()> 1]):
        print(merged.loc[merged.stage=='successful', 'project_id'].value_counts()[merged.loc[merged.stage=='successful', 'project_id'].value_counts()> 1])

    if UPDATE_PROJECT==True:
        panel_lib_update("ERC_panel_structure_2024_calls")
        
    merged = merged_panels(merged)
    reporting.append({'stage_process':'process6_panels', 'merged_size':len(merged)})
    merged = merged_topics(merged)
    reporting.append({'stage_process':'process7_topics', 'merged_size':len(merged)})
    merged = merged_actions(merged)
    reporting.append({'stage_process':'process8_actions', 'merged_size':len(merged)})
    merged = euro_partnerships(merged)
    reporting.append({'stage_process':'process9_europs', 'merged_size':len(merged)})

    # calls list
    calls = call(PATH_SOURCE+FRAMEWORK+'/')

    print("\n### CALLS+MERGED")
    if len(merged.loc[merged.call_id.isnull()])>0:
            print(f"1 - ATTENTION : manque des call_id: {merged.loc[merged.call_id.isnull(), 'project_id']}")
    else:
        call_id = merged[['call_id', 'call_deadline']].drop_duplicates()
        print(f"2 - CALL_ID de merged -> nb call+deadline: {len(call_id)}, nb call unique: {call_id.call_id.nunique()} ")

    calls = calls_to_check(calls, call_id)

    projects = projects_complete_cleaned(merged, extractDate)
    reporting.extend([{'stage_process':'process10_projects_all', 'merged_size':len(projects)},
    {'stage_process':'process10_projects_all', 'project_size':len(projects[projects.stage=='successful'])},
    {'stage_process':'process10_projects_all', 'proposal_size':len(projects[projects.stage=='evaluated'])}])

    projects = pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl")
    json.dump(reporting, open('reporting.json', 'w', encoding='utf-8'), indent=4)
else:
    projects = pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl")
    reporting = json.load(open('reporting.json', 'r', encoding='utf-8'))

#############################################################
##### PARTICIPATIONS
if UPDATE_PARTICIPANT == True:
    part, rep = participants_load(projects.loc[projects.stage=='successful', ['project_id', 'call_id', 'acronym']])
    reporting.extend(rep)
    # conserve uniquement les projets présents dans proposals et applicants
    part = part.loc[part.project_id.isin(projects.project_id.unique())]
    print(f"- size part hors proj manquant: {len(part)}")

    # Role, partnerType, erc_role
    part = part_role_type(part, projects)
    reporting.append({'stage_process':'process5_role_erc', 'participant_size':len(part)})

    #### APPLICANTS
    app, rep = applicants_load(projects.loc[projects.stage=='evaluated', ['project_id', 'call_id', 'acronym']])
    reporting.extend(rep)
    # conserve uniquement les projets présents dans proposals et applicants
    app1 = app.loc[app.project_id.isin(projects.project_id.unique())] 
    print(f"- size app1 hors proj exclus: {len(app1)}")
    reporting.append({'stage_process':'process3_keep_withProj', 'applicant_size':len(app1)})

    # get participant for project missed into poposals and add to applicants
    app_missing_pid = projects.loc[(projects.stage=='evaluated')&(~projects.project_id.isin(app1.project_id.unique())), 'project_id'].unique()
    tmp = part[part.project_id.isin(app_missing_pid)]
    app1 = part_miss_app(tmp, app1)
    reporting.append({'stage_process':'process3_add_miss_proj', 'applicant_size':len(app1)})

    #redressement accelerator
    acc_folio = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'proposals_eicFundPortfolio.json', 'utf8')
    acc_folio = pd.DataFrame(acc_folio)
    print(f"size acc_folio: {len(acc_folio)}")
    acc = (app1.loc[(app1.project_id.isin(acc_folio.proposalNbr.unique()))&(app1.role=='Coordinator'),['project_id', 'role']]
        .merge(acc_folio[['proposalNbr','grantRequested']], how='inner', left_on='project_id', right_on='proposalNbr')
        .drop(columns='proposalNbr'))
    print(f"size acc: {len(acc)}")
    app1 = app1.merge(acc, how='left', on=['project_id', 'role'])
    app1.loc[app1.requestedGrant.isnull(), 'requestedGrant'] = app1.grantRequested
    app1.drop(columns=['grantRequested'], inplace=True)
    reporting.append({'stage_process':'process4_eic', 'applicant_size':len(app1)})

    # Role, partnerType, erc_role
    app1 = app_role_type(app1, projects)
    reporting.append({'stage_process':'process5_role_erc', 'applicant_size':len(app1)})

    del app

    ####
    # verification Etat des participations
    part = check_multiP_by_proj(part)
    app1 = check_multiA_by_proj(app1)

    ########################################
    ### STEP2
    # ENTITIES
    entities, rep = entities_load()
    reporting.extend(rep)
    entities, rep = entities_merge_partApp(entities, app1, part)
    reporting.extend(rep)

    # countries
    list_codeCountry = list(set(entities.countryCode.to_list()+app1.countryCode.to_list()+part.countryCode.to_list()))
    countries, countryCode_err = country_load(FRAMEWORK, list_codeCountry)

    if any(countryCode_err):
        print(f"Attention fix country_code missing {countryCode_err}")

    cc_code = countries[['countryCode', 'countryCode_iso3']].drop_duplicates().rename(columns={'countryCode_iso3':'country_code_mapping'})
    app1 = app1.merge(cc_code, how='left', on='countryCode', indicator=True)
    part = part.merge(cc_code, how='left', on='countryCode', indicator=True)
    entities = entities.merge(cc_code, how='left', on='countryCode', indicator=True)
    reporting.extend([{'stage_process':'process4_entitiesWithCC', 'entities_size':len(entities)},
                    {'stage_process':'process4_entitiesWithCC', 'applicant_size':len(app1)},
                    {'stage_process':'process4_entitiesWithCC', 'participant_size':len(part)}])


    for i in [app1, part, entities]:
        if any(i['_merge']=='left_only'):
            print(i.loc[i['_merge']=='left_only'].countryCode.unique())
        i.drop(columns='_merge', inplace=True)


        # LIEN
    lien = merged_partApp(app1, part)
    reporting.append({'stage_process':'process2_PicAppPart', 'lien_size':len(lien)})
    lien = nuts_lien(app1, part, lien)
    reporting.append({'stage_process':'process2_wthNuts', 'lien_size':len(lien)})
    lien.to_pickle(f"{PATH_CLEAN}lien.pkl")

#############################################################################
    entities_single = entities_single_create(entities, lien)
    reporting.append({'stage_process':'process5_status', 'entites_size':len(entities_single)})
    json.dump(reporting, open('reporting.json', 'w', encoding='utf-8'), indent=4)
else:
    entities_single = pd.read_pickle(f"{PATH_SOURCE}entities_single.pkl")
    countries = pd.read_pickle(f"{PATH_CLEAN}country_current.pkl")
    lien = pd.read_pickle(f"{PATH_CLEAN}lien.pkl")
    reporting = json.load(open('reporting.json', 'r', encoding='utf-8'))

entities_info = entities_info_create(entities_single, lien)
reporting.append({'stage_process':'process5_status', 'entities_size':len(entities_single)})

### step3

# ##################################
if UPDATE_PARTICIPANT==True:
    # nouvelle actualisation ; à executer UNE FOIS
    ref_source = ref_source_load('ref')
    result, check_id_liste, identification = first_update(ref_source, entities_info, countries)

    IDchecking_results(result, check_id_liste, identification)
    #################
    # vérifier dans excel les nouveaux ID PATH_WORK/_check_id_result.xlsx
    ##################################################################
    id_verified = ID_resultChecked()
    new_ref_source(id_verified, ref_source, extractDate, part, app1, entities_single, countries)

# ########################################################################################################

# chargement du nouveau ref_source
ref_source = ref_source_load('ref')
ref, genPic_to_new = ref_source_2d_select(ref_source, 'HE')
entities_tmp, rep = entities_tmp_create(entities_info, countries, ref)
reporting.extend(rep)
print(f"size entities_tmp: {len(entities_tmp)}")
entities_tmp = entities_for_merge(entities_tmp)

### Executer uniquement si besoin
if UPDATE_PARTICIPANT==True:
    lid_source, unknow_list = ID_entities_list(ref_source)
    ror_getRefInfo(lid_source, countries)
    get_siret_siege(lid_source)
    siren_siret = pd.read_pickle(f"{PATH_HARVEST}siren_siret.pkl")
    paysage_id = ID_to_IDpaysage(lid_source, siren_siret)
    paysage = paysage_getRefInfo(paysage_id, df_old=False)
    paysage_category = IDpaysage_category(paysage)
    paysage_mires = get_mires()
    sirene = get_sirene(lid_source, sirene_old=None)

#############################################################################################################
NEW_SEARCH=False
if NEW_SEARCH==True:
    df_ID = pd.DataFrame({'id_source':['Uay3M'], 'id_paysage':['Uay3M']})
    paysage = new_search('paysage', df_ID)


### merge entities_tmp + referentiel
# ROR
### si besoin de charger ror pickle
ror = pd.read_pickle(f"{PATH_REF}ror_df.pkl")
entities_tmp = merge_ror(entities_tmp, ror)
reporting.append({'stage_process':'process_ror', 'entities_size':len(entities_tmp)})

# PAYSAGE
### si besoin de charger paysage pickle
paysage = pd.read_pickle(f"{PATH_REF}paysage_df.pkl")
if UPDATE_PARTICIPANT==True:
    paysage_category = IDpaysage_category(paysage)
paysage_category = pd.read_pickle(f"{PATH_HARVEST}paysage_category.pkl")
cat_filter = category_paysage(paysage_category)
entities_tmp = merge_paysage(entities_tmp, paysage, cat_filter)
reporting.append({'stage_process':'process_paysage', 'entities_size':len(entities_tmp)})

# SIRENE
### si besoin de charger paysage pickle
sirene = pd.read_pickle(f"{PATH_REF}sirene_df.pkl")
sirene = naf_etab_sirene(sirene)
entities_tmp = merge_sirene(entities_tmp, sirene)
reporting.append({'stage_process':'process_sirene', 'entities_size':len(entities_tmp)})

entities_tmp.loc[(~entities_tmp.id.isnull())&(entities_tmp.entities_id.isnull()), 'entities_id'] = entities_tmp.id

if any(entities_tmp.siren.str.contains(';', na=False)):
    print("ATTENTION faire code pour traiter deux siren différents -> ce qui serait bizarre qu'il y ait 2 siren")

# IDENT with '-' : traitement des identifiants avec '-' pour regrouper multi-pic non identifiés
entities_tmp = IDpic(entities_tmp)
entities_tmp = entities_tmp.merge(get_source_ID(entities_tmp, 'entities_id'), how='left', on='entities_id')
reporting.append({'stage_process':'process_picDash', 'entities_size':len(entities_tmp)})

### groupe entreprises
# groupe = groupe_treatment('groupe_prov', 'groupe')
### si besoin de charger groupe 
groupe = pd.read_pickle(f"{PATH_REF}groupe.pkl")
print(f"taille de entities_tmp avant groupe:{len(entities_tmp)}")
entities_tmp = merge_groupe(entities_tmp, groupe)
reporting.append({'stage_process':'process_groupe', 'entities_size':len(entities_tmp)})


#983764495  
entities_tmp = entities_clean(entities_tmp)
entities_check_null(entities_tmp)

# traitement catégorie
entities_tmp = category_woven(entities_tmp, sirene)
entities_tmp = category_agreg(entities_tmp)

entities_info = entities_info_add(entities_tmp, entities_info, countries)
entities_info = cordis_type(entities_info)

entities_info = add_countries_info(entities_info, countries)
entities_info = mires(entities_info)

del ref_source

#check entities with pic_id
print("### check enties fr avec id commençant par pic")
pd.set_option("display.max_rows", None, "display.max_columns", None)
print(entities_info[(entities_info.country_code=='FRA')&(entities_info.entities_id.str.contains('pic'))][['entities_id', 'entities_name']])
reporting.append({'stage_process':'process_entities_info', 'entities_size':len(entities_info)})

file_name = f"{PATH_CLEAN}entities_info_current2.pkl"
with open(file_name, 'wb') as file:
    pd.to_pickle(entities_info, file)

# entities_info = pd.read_pickle(f"{PATH_CLEAN}entities_info_current2.pkl")

# STEP4 - INDICATEURS
proj_erc = (projects.loc[projects.action_code=='ERC', ['project_id', 'destination_code']]
            .drop_duplicates())
part_step = participations_calc(lien, genPic_to_new, proj_erc, entities_info)
# part_step = entities_with_lien(entities_info, lien, genPic_to_new)
proj_no_coord = proj_no_coord(projects)
# part_prop = applicants_calcul(part_step, app1, proj_erc)
# part_prop = applicants_calcul(part_step, projects, proj_erc)
# part_proj = participants_calcul(part_step, part, proj_erc)
participation = participations_complete(part_step, proj_no_coord)
del part_step

#step5 - si nouvelle actualisation ou changement dans nomenclatures:
if UPDATE_FP==True:
    H2020_process()
    FP7_process()
    FP6_process()