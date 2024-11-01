
from main_library import *

################################
## data load / adjustements*
extractDate = date_load()

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
call_to_integrate = proposals_id_missing(prop1, proj, extractDate)

# project data missing in proposals if call already in proposals then add this
proj1 = proj_id_miss_fixed(prop1, proj, call_to_integrate)

# merge proj + prop
print('### MERGED PROPOSALS/PROJECTS')
if len(proj1)==0:
    prop2=pd.concat([proj,prop1], ignore_index= True)
else:
    prop2 = pd.concat([prop1, proj1, proj], ignore_index = True)

prop2 = prop2.loc[~((prop2.status_code=='REJECTED')&(prop2.stage=='successful'))]
print(f"result - merged all: {len(prop2)}, {prop2[['stage','status_code']].value_counts()}\n")

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

print("### CALLS+MERGED")
if len(merged.loc[merged.call_id.isnull()])>0:
        print(f"1 - ATTENTION : manque des call_id: {merged.loc[merged.call_id.isnull(), 'project_id']}")
else:
    call_id = merged[['call_id', 'call_deadline']].drop_duplicates()
    print(f"2 - CALL_ID de merged -> nb call+deadline: {len(call_id)}, nb call unique: {call_id.call_id.nunique()} ")

calls = calls_to_check(calls, call_id)

projects = projects_complete_cleaned(merged, extractDate)

#############################################################
##### PARTICIPANTS
part = participants_load(proj)
part = part_role_type(part)
part = erc_role(part, projects)

#### APPLICANTS
app = applicants_load(prop)
# conserve uniquement les projets présents dans proposals et applicants
app1 = app.loc[app.project_id.isin(projects.project_id.unique())] 
print(f"- size app hors proj exclus: {len(app1)}")
app_missing_pid = projects.loc[(projects.stage=='evaluated')&(~projects.project_id.isin(app1.project_id.unique())), 'project_id'].unique()
tmp = part[part.project_id.isin(app_missing_pid)]
app1 = part_miss_app(tmp, app1)

app1 = app_role_type(app1)
app1 = erc_role(app1, projects)
del app

####
# verification Etat des participations
checking_unique_part(part)
part = check_multiP_by_proj(part)
app1 = check_multiA_by_proj(app1)


# STEP2
lien = merged_partApp(app1, part)
lien = nuts_lien(app1, part, lien)
lien.to_pickle(f"{PATH_CLEAN}lien.pkl")

entities = entities_load(lien)
entities_single = entities_single(entities, lien, part, app1)
entities_info = entities_info(entities_single, lien, app1, part)

list_codeCountry = entities_info.countryCode.unique()
countries = country_load(FRAMEWORK, list_codeCountry)

# step3

# ##################################
# # nouvelle actualisation ; à executer UNE FOIS
# ref_source = ref_source_load('ref')
# result, check_id_liste, identification = first_update(ref_source, entities_info, countries)

# # vérifier dans excel les nouveaux ID PATH_WORK/_check_id_result.xlsx
# IDchecking_results(result, check_id_liste, identification)

# id_verified = ID_resultChecked()
# new_ref_source(id_verified, ref_source, extractDate, part,app1, entities_single, countries)

# ########################

# chargement du nouveau ref_source
ref_source = ref_source_load('ref')
ref = ref_source_2d_select(ref_source, 'HE')
entities_tmp = entities_tmp_create(entities_info, countries, ref)
print(f"size entities_tmp: {len(entities_tmp)}")
entities_tmp = entities_for_merge(entities_tmp)

### Executer uniquement si besoin
lid_source, unknow_list = ID_entities_list(ref_source)
# ror, paysage, paysage_category, paysage_mires, sirene = ID_getRefInfo(lid_source) 

### merge entities_tmp + referentiel
# ROR
### si besoin de charger ror pickle
ror = pd.read_pickle(f"{PATH_REF}ror_df.pkl")
entities_tmp = merge_ror(entities_tmp, ror)

# PAYSAGE
### si besoin de charger paysage pickle
paysage = pd.read_pickle(f"{PATH_REF}paysage_df.pkl")
cat_filter = category_paysage(paysage)
entities_tmp = merge_paysage(entities_tmp, paysage, cat_filter)

# SIRENE
### si besoin de charger paysage pickle
sirene = pd.read_pickle(f"{PATH_REF}sirene_df.pkl")
entities_tmp = merge_sirene(entities_tmp, sirene)

entities_tmp.loc[(~entities_tmp.id.isnull())&(entities_tmp.entities_id.isnull()), 'entities_id'] = entities_tmp.id

### groupe entreprises
# groupe = groupe_treatment('groupe_prov', 'groupe')
# wb=openpyxl.load_workbook(f"{PATH_REF}groupe_prov.xlsm").sheetnames[1:]
if any(entities_tmp.siren.str.contains(';', na=False)):
    print("ATTENTION faire code pour traiter deux siren différents -> ce qui serait bizarre qu'il y ait 2 siren")

### si besoin de charger groupe 
groupe = pd.read_pickle(f"{PATH_REF}groupe.pkl")
print(f"taille de entities_tmp avant groupe:{len(entities_tmp)}")
entities_tmp = merge_groupe(entities_tmp, groupe)

# IDENT with '-' : traitement des identifiants avec '-' pour regrouper multi-pic non identifiés
entities_tmp = IDpic(entities_tmp)

entities_tmp = entities_clean(entities_tmp)
entities_check_null(entities_tmp)

# traitement catégorie
entities_tmp = category_cleaning(entities_tmp, sirene)
entities_tmp = category_woven(entities_tmp)

entities_info = entities_info_add(entities_tmp,entities_info)
entities_info = cordis_type(entities_info)
entities_info = add_fix_countries(entities_info, countries)

### si besoin de charger groupe 
entities_info = mires(entities_info)

#check entities with pic_id
print("### check enties fr avec id commençant par pic")
pd.set_option("display.max_rows", None, "display.max_columns", None)
print(entities_info[(entities_info.country_code=='FRA')&(entities_info.entities_id.str.contains('pic'))][['entities_id', 'entities_name']])

file_name = f"{PATH_CLEAN}entities_info_current2.pkl"
with open(file_name, 'wb') as file:
    pd.to_pickle(entities_info, file)