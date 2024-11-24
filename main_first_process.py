from main_library import *

################################
## data load / adjustements*
extractDate = date_load()

### pour l'instant ne fonctionne pas
## demander à Eric de relancer la machine sur sandbox
# get_call_info()

# get_call_info_europa()

proj = projects_load()
proj_id_signed = proj.project_id.unique()

prop = proposals_load()
proj = proj_add_cols(prop, proj)

stage_p =  ['REJECTED' ,'NO_MONEY' ,'MAIN', 'RESERVE', 'INELIGIBLE', 'WITHDRAWN', 'INADMISSIBLE', None]
prop1 = proposals_status(prop, proj_id_signed, stage_p)  
# np.save("data_files/applicants_columns.npy", prop_cols)

###########################################
# proposals fix
# projects missing from proposals
call_to_integrate, call_miss = proposals_id_missing(prop1, proj, extractDate)

# project data missing in proposals if call already in proposals then add this
proj1 = proj_id_miss_fixed(prop1, proj, call_to_integrate)
call_miss = list(set(call_miss)-set(call_to_integrate))
proj = proj.loc[~proj.callId.isin(call_miss)]

# merge proj + prop
print('### MERGED PROPOSALS/PROJECTS')
if len(proj1)==0:
    prop2=pd.concat([proj,prop1], ignore_index= True)
else:
    prop2 = pd.concat([prop1, proj1, proj], ignore_index = True)

prop2 = prop2.loc[~((prop2.status_code=='REJECTED')&(prop2.stage=='successful'))]
print(f"- result - merged all: {len(prop2)},\n{prop2[['stage','status_code']].value_counts()}")

merged = copy.deepcopy(prop2)
merged = dates_year(merged)
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

merged = merged_panels(merged)
merged = merged_topics(merged)
merged = merged_actions(merged)

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

# projects = pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl")

#############################################################
##### PARTICIPATIONS
part = participants_load(proj)
# conserve uniquement les projets présents dans proposals et applicants
part = part.loc[part.project_id.isin(projects.project_id.unique())]
print(f"- size part hors proj manquant: {len(part)}")
part = part_role_type(part)
part = erc_role(part, projects)

#### APPLICANTS
app = applicants_load(prop)
# conserve uniquement les projets présents dans proposals et applicants
app1 = app.loc[app.project_id.isin(projects.project_id.unique())] 
print(f"- size app1 hors proj exclus: {len(app1)}")

app_missing_pid = projects.loc[(projects.stage=='evaluated')&(~projects.project_id.isin(app1.project_id.unique())), 'project_id'].unique()
tmp = part[part.project_id.isin(app_missing_pid)]
app1 = part_miss_app(tmp, app1)

#redressement accelerator
acc_folio = pd.read_csv(f"{PATH_SOURCE}{FRAMEWORK}/eic_fund_portfolio.csv", sep=';', dtype={'PROPOSAL_NBR':str})
print(f"size acc_folio: {len(acc_folio)}")
acc = (app1.loc[(app1.project_id.isin(acc_folio.PROPOSAL_NBR.unique()))&(app1.role=='Coordinator'),['project_id', 'role']]
       .merge(acc_folio[['PROPOSAL_NBR','GRANT_REQUESTED']], how='inner', left_on='project_id', right_on='PROPOSAL_NBR')
       .drop(columns='PROPOSAL_NBR'))
print(f"size acc: {len(acc)}")
app1 = app1.merge(acc, how='left', on=['project_id', 'role'])
app1.loc[app1.requestedGrant.isnull(), 'requestedGrant'] = app1.GRANT_REQUESTED
app1.drop(columns=['GRANT_REQUESTED'], inplace=True)

app1 = app_role_type(app1)
app1 = erc_role(app1, projects)

del app

####
# verification Etat des participations
part = check_multiP_by_proj(part)
app1 = check_multiA_by_proj(app1)

########################################
### STEP2
# ENTITIES
entities = entities_load(app1, part)

# countries
list_codeCountry = list(set(entities.countryCode.to_list()+app1.countryCode.to_list()+part.countryCode.to_list()))
countries, countryCode_err = country_load(FRAMEWORK, list_codeCountry)
# countries fix old code and add iso3
for b in [app1, part, entities]:
    b = country_old(b)

cc_code = (countries[['countryCode', 'country_code_mapping']]
        .rename(columns={'countryCode':'iso2', 'country_code_mapping':'iso3'})
        .drop_duplicates())
app1 = pd.merge(app1, cc_code, how='left', left_on='countryCode', right_on='iso2').drop(columns='iso2').rename(columns={'iso3':'country_code_mapping'})
part = pd.merge(part, cc_code, how='left', left_on='countryCode', right_on='iso2').drop(columns='iso2').rename(columns={'iso3':'country_code_mapping'})
entities = pd.merge(entities, cc_code, how='left', left_on='countryCode', right_on='iso2').drop(columns='iso2').rename(columns={'iso3':'country_code_mapping'})

# LIEN
lien = merged_partApp(app1, part)
lien = nuts_lien(app1, part, lien)
lien.to_pickle(f"{PATH_CLEAN}lien.pkl")

# entities = (entities.merge(cc_code, how='left', left_on='countryCode', right_on='iso2').drop(columns='iso2')
#             .rename(columns={'iso3':'country_code_mapping'}))

#ENTITIES +LIEN
entities_single = entities_single_create(entities, lien)
# entities_single = pd.read_pickle(f"{PATH_SOURCE}entities_single.pkl")
entities_info = entities_info_create(entities_single, lien)

### step3

# ##################################
# # nouvelle actualisation ; à executer UNE FOIS
# ref_source = ref_source_load('ref')
# result, check_id_liste, identification = first_update(ref_source, entities_info, countries)

# # vérifier dans excel les nouveaux ID PATH_WORK/_check_id_result.xlsx
# IDchecking_results(result, check_id_liste, identification)

# id_verified = ID_resultChecked()
# new_ref_source(id_verified, ref_source, extractDate, part, app1, entities_single, countries)

# ########################################################################################################

# chargement du nouveau ref_source
ref_source = ref_source_load('ref')
ref, genPic_to_new = ref_source_2d_select(ref_source, 'HE')
entities_tmp = entities_tmp_create(entities_info, countries, ref)
print(f"size entities_tmp: {len(entities_tmp)}")
entities_tmp = entities_for_merge(entities_tmp)

### Executer uniquement si besoin
# lid_source, unknow_list = ID_entities_list(ref_source)
# ror = ror_getRefInfo(lid_source)
# siren_siret = get_siret_siege(lid_source)
# paysage_id = ID_to_IDpaysage(lid_source, siren_siret)
# paysage, paysage_mires = paysage_getRefInfo(lid_source, siren_siret, paysage_old=None)
# paysage_category = IDpaysage_category(paysage)
# sirene = get_sirene(lid_source, sirene_old=None)

#############################################################################################################


### merge entities_tmp + referentiel
# ROR
### si besoin de charger ror pickle
ror = pd.read_pickle(f"{PATH_REF}ror_df.pkl")
entities_tmp = merge_ror(entities_tmp, ror, countries)

# PAYSAGE
### si besoin de charger paysage pickle
paysage = pd.read_pickle(f"{PATH_REF}paysage_df.pkl")
# paysage_category = IDpaysage_category(paysage)
paysage_category = pd.read_pickle(f"{PATH_SOURCE}paysage_category.pkl")
cat_filter = category_paysage(paysage_category)
entities_tmp = merge_paysage(entities_tmp, paysage, cat_filter)

# SIRENE
### si besoin de charger paysage pickle
sirene = pd.read_pickle(f"{PATH_REF}sirene_df.pkl")
entities_tmp = merge_sirene(entities_tmp, sirene)

entities_tmp.loc[(~entities_tmp.id.isnull())&(entities_tmp.entities_id.isnull()), 'entities_id'] = entities_tmp.id

if any(entities_tmp.siren.str.contains(';', na=False)):
    print("ATTENTION faire code pour traiter deux siren différents -> ce qui serait bizarre qu'il y ait 2 siren")

# IDENT with '-' : traitement des identifiants avec '-' pour regrouper multi-pic non identifiés
entities_tmp = IDpic(entities_tmp)
entities_tmp = entities_tmp.merge(get_source_ID(entities_tmp, 'entities_id'), how='left', on='entities_id')

### groupe entreprises
# groupe = groupe_treatment('groupe_prov', 'groupe')
### si besoin de charger groupe 
groupe = pd.read_pickle(f"{PATH_REF}groupe.pkl")
print(f"taille de entities_tmp avant groupe:{len(entities_tmp)}")
entities_tmp = merge_groupe(entities_tmp, groupe)


entities_tmp = entities_clean(entities_tmp)
entities_check_null(entities_tmp)

# traitement catégorie
entities_tmp = category_woven(entities_tmp, sirene)
entities_tmp = category_agreg(entities_tmp)
entities_info = entities_info_add(entities_tmp, entities_info, countries)
entities_info = cordis_type(entities_info)

entities_info = fix_countries(entities_info, countries)
entities_info = mires(entities_info)

del ref_source

#check entities with pic_id
print("### check enties fr avec id commençant par pic")
pd.set_option("display.max_rows", None, "display.max_columns", None)
print(entities_info[(entities_info.country_code=='FRA')&(entities_info.entities_id.str.contains('pic'))][['entities_id', 'entities_name']])

file_name = f"{PATH_CLEAN}entities_info_current2.pkl"
with open(file_name, 'wb') as file:
    pd.to_pickle(entities_info, file)

# STEP4 - INDICATEURS

part_step = entities_with_lien(entities_info, lien, genPic_to_new)
proj_no_coord = proj_no_coord(projects)
part_prop = applicants_calcul(part_step, app1)
part_proj = participants_calcul(part_step, part)
participation = participations_complete(part_prop, part_proj, proj_no_coord)
del part_proj, part_prop

gc.collect()
#step5 - si nouvelle actualisation ou changement dans nomenclatures
# H2020_process()
# FP7_process()
# FP6_process()

# project_list = list(set(h20_p.project_id))+list(set(FP7_p.project_id))+list(set(FP6_p.project_id))+list(set(projects.loc[projects.stage=='successful'].project_id))
# check_proj_id(project_list)