from main_library import *
from remote_process.grist import *
from remote_process import orcid_runner
from remote_process.provinces_runner import *

import copy, datetime
pd.options.mode.copy_on_write = True

# If new update change constant_vars.py
FETCH_PERSO_SCANR = False
FETCH_GEO_DATA = False # last extraction carried out between 2026-07 and 2026-09
FETCH_WEB_DATA = False  # True -> to fetch data from tenders portal and save in data_wp
LOAD_DATA = False  # True -> to load data from json, False -> to fetch data from json and save in json
UPDATE_PROJECT = False  # True -> to update projects and proposals, False -> to load last version of projects and proposals
UPDATE_PARTICIPATION = False  # True -> to update participants and applicants, False -> to load last version of participants and applicants
UPDATE_ENTITIES = False  # True -> to update entities, False -> to load last version of entities
CHECK_NEW_ENTITIES = False  # True -> to check new entities and update ref_source, False -> to load last version of ref_source
CHECK_ID_BY_API = False
UPDATE_REF_AND_PAYSAGE = False  # After finding new ids and fixing some, load new ror, sirene and update paysage app
UPDATE_GR = False
UPDATE_PERSONS = False
UPDATE_FP = False  # True -> to update FP6, FP7, H2020 data, False -> to load last version of FP6, FP7, H2020 data

ZIPNAME = last_data_zip(PATH_SOURCE, FRAMEWORK, "json")
SOURCE_JSON = f"{PATH_SOURCE}{FRAMEWORK}/{ZIPNAME}"
extractDate = date_load(SOURCE_JSON)
CSV_PERSONS = "20260616"


#======================================================
# extracting info on WP, geo, scanr persons

if FETCH_PERSO_SCANR == True:

    download_with_resume(
    "https://scanr-data.s3.gra.io.cloud.ovh.net/production/persons_denormalized.jsonl.gz",
    f"{PATH}referentiel/persons_denormalized.jsonl.gz"
    )


if FETCH_GEO_DATA==True:
    """
    -> Lancé en arrière-plan
    charge les fichiers zip de geoname par pays
    prépare un dataset complet avec tous les niveaux de geoloc
    """
    start()


if FETCH_WEB_DATA==True:
    wp_year='2026'
    
    get_topic_from_eu_portal() #==> extract all topics closed/open/upcoming from eu poratl and save in data_wp/topic_info_harvest.json
    
    url = f"https://ec.europa.eu/info/funding-tenders/opportunities/docs/2021-2027/horizon/wp-call/{wp_year}/wp_horizon-erc-{wp_year}_en.pdf"
    erc_wp_panel(wp_year, url) # ==> extract panel info from ERC WP and save in data_harvest/erc_panels.json
    panel_lib_update() # ==> update panels.json with new info from ERC WP and save in data_files/panels.json


#====================================================
# loading source data 

if LOAD_DATA==True:
    reporting = []

    proj, rep = projects_load(SOURCE_JSON)
    proj_id_signed = proj['project_id'].unique()
    reporting.extend(rep)

    prop, rep = proposals_load(SOURCE_JSON)
    reporting.extend(rep)

    part, rep = participants_load(SOURCE_JSON)
    reporting.extend(rep)

    app, rep = applicants_load(SOURCE_JSON)
    reporting.extend(rep)

    entities, rep = entities_load(SOURCE_JSON)
    reporting.extend(rep)


#===========================================================
# starting to clean

if UPDATE_PROJECT==True:
     ## step1 -> data load / adjustements*

    # projects missing from proposals => list missing projects into excel file missing_proposals_{extractDate}.xlsx
    # temp/proj_no_proposals.csv -> flag callId to integrate and exclude from calculations

    call_to_integrate, call_miss, proj_to_prop = data_analysis(prop, app, proj, part)


    # add cols from proposals to projects (panel, freekw) if missing in projects

    proj = proj_add_cols(prop, proj) 

    # proposals status : check status, remove ineligible, inadmissible, duplicate, withdrawn, assign stage 'evaluated' to all proposals

    stage_p = ['REJECTED' ,'NO_MONEY' ,'MAIN', 'RESERVE', 'INELIGIBLE', 'WITHDRAWN', 'INADMISSIBLE', None]
    prop1, rep = proposals_status(prop, proj_id_signed, stage_p) 
    reporting.extend(rep) 


    # update proposals with missing projects from projects table and flag callId to integrate in proposals table

    proj1 = proj_id_miss_fixed(prop1, proj, call_to_integrate)
    
    # create MERGED -> merge proj + prop
    print('### MERGED PROPOSALS/PROJECTS')
    if len(proj1)==0:
        df = pd.concat([proj, prop1], ignore_index= True)
    else:
        df = pd.concat([prop1, proj1, proj], ignore_index = True)

    # remove rejected projects with stage successful in projects

    df = df.loc[~((df['status_code']=='REJECTED')&(df['stage']=='successful'))]
    print(f"- result - merged all: {len(df)},\n{df[['stage','status_code']].value_counts()}")

    reporting.extend(
        [
            {"stage_process": "process3_add_miss_proj", "proposal_size": len(df[df["stage"] == "evaluated"])},
            {"stage_process": "process2_status", "project_size": len(df[df["stage"] == "successful"])},
            {"stage_process": "process4_merge", "merded_size": len(df)},
        ]
    )

    top_call = topics_portal_clean() # info by topic (fix year of wp)
    merged = copy.deepcopy(df)
    merged = dates_year(merged, top_call)
    reporting.append({'stage_process':'process5_date_clean', 'merged_size':len(merged)})
    merged = strings_v(merged)
    merged = url_to_clean(merged) # clean url project website
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


    # checking duplicated successful project

    duplicate_counts = merged.loc[merged["stage"] == "successful", "project_id"].value_counts()
    duplicates = duplicate_counts[duplicate_counts > 1]

    if not duplicates.empty:
        print(duplicates)

    
    # add panels, topics, actions, tag euro partnerships
    
    merged = merged_panels(merged)
    reporting.append({'stage_process':'process6_panels', 'merged_size':len(merged)})
    merged = merged_topics(SOURCE_JSON, merged)
    reporting.append({'stage_process':'process7_topics', 'merged_size':len(merged)})
    merged = merged_actions(SOURCE_JSON, merged)
    reporting.append({'stage_process':'process8_actions', 'merged_size':len(merged)})
    merged = euro_partnerships(merged)
    reporting.append({'stage_process':'process9_europs', 'merged_size':len(merged)})


    # calls list
    calls = call(SOURCE_JSON)

    print("\n### CALLS+MERGED")
    # check if call_id in MERGED match with call in calls
    if len(merged.loc[merged['call_id'].isnull()])>0:
            print(f"1 - ⚠️ : manque des call_id: {merged.loc[merged['call_id'].isnull(), 'project_id']}")
    else:
        call_id = merged[['call_id', 'call_deadline']].drop_duplicates()
        print(f"2 - CALL_ID de merged -> nb call+deadline: {len(call_id)}, nb call unique: {call_id['call_id'].nunique()} ")

    calls = calls_to_check(calls, call_id)


    # add script -> contrôler et remplir les variables null dans successful et pas dans proposals comme abstrcat

    projects = projects_complete_cleaned(merged, extractDate) # create => data_clean/projects_current.pkl"

    reporting.extend([{'stage_process':'process10_projects_all', 'merged_size':len(projects)},
    {'stage_process':'process10_projects_all', 'project_size':len(projects[projects['stage']=='successful'])},
    {'stage_process':'process10_projects_all', 'proposal_size':len(projects[projects['stage']=='evaluated'])}])
    json.dump(reporting, open('reporting.json', 'w', encoding='utf-8'), indent=4)
else:
    # if already cleansing, just load the last version of projects and reporting => if UPDATE_PROJECT==False
    projects = pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl")
    reporting = json.load(open('reporting.json', 'r', encoding='utf-8'))


#==============================================================================================================
# PARTICIPATIONS
if UPDATE_PARTICIPATION == True:

    #### APPLICANTS
    # keep only project_id in proposals and applicants
    app1 = app.loc[app['project_id'].isin(projects['project_id'].unique())] 
    print(f"- size app1 hors proj exclus: {len(app1)}")
    reporting.append({'stage_process':'process3_keep_withProj', 'applicant_size':len(app1)})

    # get participant for project missed into poposals and add to applicants
    mask_missed = (projects["stage"] == "evaluated") & (~projects["project_id"].isin(app1["project_id"].unique()))
    app_missing_pid = projects.loc[mask_missed, "project_id"].unique()

    tmp = part[part['project_id'].isin(app_missing_pid)]
    app1 = part_miss_app(tmp, app1)
    reporting.append({'stage_process':'process3_add_miss_proj', 'applicant_size':len(app1)})

    #fix accelerator project (limit in k€)
    app1 = prop_accelerator_process(SOURCE_JSON, app1, projects, 150, 3000)
    reporting.append({'stage_process':'process4_eic', 'applicant_size':len(app1)})

    # Role, partnerType, erc_role
    app1 = app_role_type(app1, projects)
    reporting.append({'stage_process':'process5_role_erc', 'applicant_size':len(app1)})

    # part with generalPic null
    if any(part[part['generalPic'].isnull()]):
        part = part_pic_null(part, entities)

    # Role, partnerType, erc_role
    part = part_role_type(part, projects)
    reporting.append({'stage_process':'process5_role_erc', 'participant_size':len(part)})

    del app

    ####
    # verification Etat des participations
    part = check_multiP_by_proj(part)
    app1 = check_multiA_by_proj(app1)

    #=============================================
    ### STEP2
    # ENTITIES
    entities, rep = entities_merge_partApp(entities, app1, part)
    reporting.extend(rep)

    # countries
    """ 
    country_code_source : code source from entities, app1, part -> iso3
    countryCode : code source from entities, app1, part -> iso2
        
    """

    # list all countryCode in entities, app1, part to check if missing in country list and add missing countryCode in country list if needed
    list_codeCountry = list(
        set(entities["countryCode"].to_list() + app1["countryCode"].to_list() + part["countryCode"].to_list())
    )

    countries, countryCode_err = country_load(SOURCE_JSON, list_codeCountry)

    # if countryCode missing in country list, add to function my_country_code and reload
    if any(countryCode_err):
        print(f"🚨 - fix country_code missing {countryCode_err}")

    cc_code = (
        countries[["countryCode", "countryCode_iso3"]]
        .drop_duplicates()
        .rename(columns={"countryCode_iso3": "country_code_source"})
    )

    app1 = app1.merge(cc_code, how='left', on='countryCode', indicator=True)
    part = part.merge(cc_code, how='left', on='countryCode', indicator=True)
    entities = entities.merge(cc_code, how='left', on='countryCode', indicator=True)

    reporting.extend(
        [
            {"stage_process": "process4_entitiesWithCC", "entities_size": len(entities)},
            {"stage_process": "process4_entitiesWithCC", "applicant_size": len(app1)},
            {"stage_process": "process4_entitiesWithCC", "participant_size": len(part)},
        ]
    )

    for i in [app1, part, entities]:
        if any(i['_merge']=='left_only'):
            print(i.loc[i['_merge']=='left_only', ['countryCode']].unique())
        i.drop(columns='_merge', inplace=True)


    # LIEN
    """
    merge app1 + part -> lien
    add nuts code to lien
        """
    lien = merged_partApp(app1, part)
    ambigus = lien[lien.base_only == 'AMBIGU_a_verifier']
    reporting.append({'stage_process':'process2_PicAppPart', 'lien_size':len(lien)})
    lien = nuts_lien(SOURCE_JSON, app1, part, lien)
    reporting.append({'stage_process':'process2_wthNuts', 'lien_size':len(lien)})
    lien.to_pickle(f"{PATH_CLEAN}lien.pkl")

    """
    select one record par pic by filtering on generalStatus -> def entities_single_create
    """
    entities_single = entities_single_create(entities, lien)
    reporting.append({'stage_process':'process5_status', 'entites_size':len(entities_single)})
    json.dump(reporting, open('reporting.json', 'w', encoding='utf-8'), indent=4)
else:
    entities_single = pd.read_pickle(f"{PATH_CLEAN}entities_single.pkl")
    countries = pd.read_pickle(f"{PATH_CLEAN}country_current.pkl")
    lien = pd.read_pickle(f"{PATH_CLEAN}lien.pkl")
    reporting = json.load(open('reporting.json', 'r', encoding='utf-8'))


#======================================================================
"""
step3

process to affiliate an entity to an repository's ID like SIRENE ROR... 
and check if ID exist in paysage or not, 
if not check in source API, then update ref_source with new ID 
if verified and update paysage with new ID if verified and not in paysage
"""

if UPDATE_ENTITIES==True:

    """
    Creation base entities
    """
    entities_info = entities_info_create(entities_single, lien)
    entities_info = entities_add_country(entities_info, countries)
    entities_info = entities_clean_name(entities_info)
    entities_info = entities_clean_address(entities_info)

    reporting.append({'stage_process':'process5_status', 'entities_size':len(entities_single)})


    # list identifiers in paysage
    sl = ['siret', 'ror', 'rnsr', 'rna']
    paysage_identifiers = paysage_id_extract(sl)
    paysage_identifiers = paysage_id_extract_prepare(paysage_identifiers)


    if CHECK_NEW_ENTITIES==True:    
        # UPDATE ; only needs to be run once
        ref_source = ref_source_load('ref')
        # fix ROR ID with 'R0' at the beginning ; old method
        ref_source.loc[ref_source['id'].str.startswith('R0', na=False), 'id'] = ref_source.loc[ref_source['id'].str.startswith('R0', na=False), 'id'].str[1:]
        entities_tmp = entities_first_preparation(ref_source, entities_info) # ref_source_1ere_select
        check_id_df, identification = identification_update(SOURCE_JSON, entities_tmp) # list ID to check and all records tabe
        
        # identifiant in paysage or not -> inPayseg True/False
        paysage_res = check_id_in_paysage(check_id_df, 'check_id', paysage_identifiers)
        
        # id missing into paysage -> check in source api
        sid_df = paysage_res.loc[(paysage_res['in_paysage']==False)&(paysage_res['source_id'].notnull()), ['check_id', 'source_id']].sort_values(['source_id', 'check_id'], ascending=False).drop_duplicates()
        print(f"## {len(sid_df)} identifiers no paysage to ckeck")

        # check existing ID
        if CHECK_ID_BY_API==True:
            print(time.strftime("%H:%M:%S"))  
            res=[]
            for sl in sid_df['source_id'].unique().tolist():
                id_list = list(sid_df.loc[sid_df['source_id']==sl, 'check_id'].unique())
                result = check_id_by_source(sl, id_list)
                res.extend(result)
            print(time.strftime("%H:%M:%S"))

            #======================================================================
            IDchecking_results(res, paysage_res, identification)
            # vérifier dans excel les nouveaux ID PATH_WORK/_check_id_result.xlsx
            # fix errors, confirm ID from link or vat
            #======================================================================

        id_verified = ID_resultChecked(paysage_identifiers)
        new_ref_source(id_verified, ref_source, extractDate, lien, entities_single, countries)
        # add data_work/ref_extarct_date.csv into data_ref/_pic_id_entites.xlsx
        # try to find ID for new foreign (universities, public organizations, european or international orga)


    """
        chargement du nouveau ref_source
        ref_source = ref_source_load('ref')
    """

    # if NEW UPDATE maj paysage with struct successful UPTADE_PAYSAGE, load_url to update ror
    if UPDATE_REF_AND_PAYSAGE==True:
        frameworks = ['HE', 'H20']
        ref_id, genPic_to_new = entities_repository_select_maj(frameworks, countries, load_url=False, UPDATE_PAYSAGE=False)
    else:
        ref_id = idsG['From_pic_to_id']
        genPic_to_new = idsG['From_oldpic_to_new']

    pic = maj_ref_by_pic(entities_info, countries, genPic_to_new, ref_id)


    """
    reload link between from_id_to_ref and paysage IDs after updating paysage app
    add paysage_id to ref_id
    """
    ref_with_paysage = merge_id_to_ref(ref_id, 'from_id_to_ref')

    ###  CREATE ENTITIES_TMP
    entities_tmp, rep = entities_tmp_create(entities_info, ref_with_paysage)
    entities_tmp = entities_for_merge(entities_tmp)

    # new source_id and check bugs between siren and ror if need to fix -> fix_bug=True
    entities_tmp = source_ID_new_and_check(entities_tmp, 'id_extend', fix_bug=True)


    # if NEW UPDATE -> PAYSAGE_GET_INFO=TRUE -> reload paysage IDs with new entities
    paysage_cj, cat, cat_filter = paysage_repository(PAYSAGE_GET_INFO=False)


    entities_tmp = merge_repositories(entities_tmp, paysage_cj, cat, cat_filter)


    entities_tmp = entities_info_add(entities_tmp, entities_info)

    # PIC
    entities_tmp = merge_pic(entities_tmp, pic, cat, paysage_cj)

    #======================================================
    ### groupe entreprises

    if UPDATE_GR==True:
        groupe = groupe_treatment('groupe_prov', 'groupe')
    ### si besoin de charger groupe
    #======================================================

    entities_tmp = entities_groupe(entities_tmp, framework=None)

    entities_tmp = entities_categories(entities_tmp)

    entities_info = entities_finalize(entities_tmp, countries, framework=None)

    # check entities_info and its vars 
    summary, duplicate_rows = check_dataframe(entities_info, ['generalPic', 'country_code', 'entities_name'])

    file_name = f"{PATH_CLEAN}entities_info_current2.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(entities_info, file)

else:
    entities_info = pd.read_pickle(f"{PATH_CLEAN}entities_info_current2.pkl")
    countries = pd.read_pickle(f"{PATH_CLEAN}country_current.pkl")
    lien = pd.read_pickle(f"{PATH_CLEAN}lien.pkl")
    reporting = json.load(open('reporting.json', 'r', encoding='utf-8'))

#================================================================
"""
    STEP4 - INDICATEURS
"""
proj_erc = (projects.loc[projects['action_code']=='ERC', ['project_id', 'destination_code']]
            .drop_duplicates())
part_step = participations_calc(lien, proj_erc, entities_info)
proj_no_coord = dom_without_coord(projects)


"""
    clean coordination for projects without coord
    create isejo for entities ZOE or ZOI
    add RNSR from gilberinette
    add rnsr detected from source_id
"""
participation = participations_isejo_coord(part_step, proj_no_coord)

participation =  rnsr_add(participation)


    # 1. Construire le dictionnaire generalPic -> liste d'id_secondaire valides (pattern RNSR, dédupliqués, sans NaN)
nns_map = (
        entities_info.dropna(subset=['id_secondaire'])
        .loc[entities_info['id_secondaire'].apply(is_valid_rnsr)]
        .groupby('generalPic')['id_secondaire']
        .apply(lambda x: list(dict.fromkeys(x)))
        .to_dict()
    )

participation['numero_national_de_structure'] = participation.apply(
    maj_nns, axis=1, mapping=nns_map
)


del part_step


#======================================================================
"""
step7 - persons script 
"""
if UPDATE_PERSONS==True:

    persons_preparation(CSV_PERSONS)  

    # 1. Liste complète en un seul appel
    df = persons_choose()

    # 2. scanR en premier (un seul passage sur le dump local)
    scanr_res = scanr_lookup(df, dump_path=f"{PATH}referentiel/persons_denormalized.jsonl.gz")
    resolved_scanr, remaining = split_scanr_resolved(df, scanr_res)

    # 3. Démarre le job ORCID en arrière-plan (non bloquant)
    orcid_runner.start(remaining, label='all')

    # 3. Pendant que ORCID tourne, IdRef s'exécute en synchrone (parallélisme gratuit)
    idref_res = idref_runner(
        remaining[["last_name", "first_name", "orcid_id"]].drop_duplicates(),
        label="all", orcid_col="orcid_id",
    )

    # 4. Attendre que le job ORCID en arrière-plan soit VRAIMENT terminé
    #    avant de lire son checkpoint (sinon résultat partiel)
    while orcid_runner.is_running(label='all'):
        print("Job ORCID toujours en cours...")
        time.sleep(30)

    orcid_runner.status(label='all')  # bilan final

    # 5. Une fois les deux terminés : agrégation + fusion
    perso_orcid_full = affiliation_orcid()
    remaining_keys = set(
        remaining["last_name"].str.strip().str.lower() + "||" +
        remaining["first_name"].str.strip().str.lower()
    )
    perso_orcid_full["_key"] = (
        perso_orcid_full["last_name"].str.strip().str.lower() + "||" +
        perso_orcid_full["first_name"].str.strip().str.lower()
    )
    perso_orcid_remaining = perso_orcid_full[perso_orcid_full["_key"].isin(remaining_keys)].drop(columns="_key")


    # 6. Fusion ORCID+IdRef pour les "remaining", puis ajout des scanR-résolus
    merged_remaining = merge_orcid_idref(perso_orcid_remaining, idref_res)
    perso_complete = pd.concat([resolved_scanr, merged_remaining], ignore_index=True)
    perso_complete.to_pickle(f"{PATH_HARVEST}persons/perso_complete_{datetime.date.today().isoformat()}.pkl")

    print(f"\nBilan final : {len(resolved_scanr)} résolus par scanR, "
        f"{len(merged_remaining)} traités via ORCID/IdRef, "
        f"{len(perso_complete)} au total.")

    
    


"""
step9 - entities_affiliations 
prepare files for moulinnette
"""

entities_preparation(SOURCE_JSON)


"""
Finalisation de participation 
#### add rnsr
## si besoin actualisation lancer entities_in_house.py
 - script pour ajouter les nouveaux rnsr à participation
- add RNSR
- add landscape

"""
rnsr, other = inHouse_unit(rnsr_exist=True)

nns = (participation
        .loc[
        participation['numero_national_de_structure'].notna(), 
        ['numero_national_de_structure']]
        .drop_duplicates()
)


nns['num_nat_struct'] = nns['numero_national_de_structure'].str.split(';')
nns = nns.explode('num_nat_struct')
nns['num_nat_struct'] = nns['num_nat_struct'].str.strip()


# faire script pour intégrer les nouveax nns apres gilberinette
update_doc_grist(geoG, 'geo')
r_fix = geoG['Rnsr_address_fix']
r_fix = r_fix.mask(r_fix == '')

r = r_fix[['num_nat_struct', 'adresse', 'code_postal', 'ville',
       'pays']]

r = (rnsr.merge(r, 
        how='left', 
        left_on='numero_national_de_structure', 
        right_on='num_nat_struct', 
        suffixes=('', '_clean')
    ).drop(columns=['numero_national_de_structure'])
)

nns = nns.merge(r, 
        how='left', 
        on='num_nat_struct'
    )

mask = nns['code_postal_clean'].isnull() | nns['ville'].isnull()

if not nns[mask].empty:
    print(f"🚨missing address in rnsr_address_fix grist:  {nns.loc[mask, ['num_nat_struct', 'code_postal', 'commune', 'code_postal_clean', 'ville']].drop_duplicates()}")

nns = (nns
       .assign(country_code_source = nns['pays'])
       .rename(columns={
                'adresse_clean': 'street',
                'code_postal_clean': 'postalCode',
                'ville': 'city',
                'pays': 'countryCode'})
        .drop_duplicates()
    )

nns = country_iso_shift(nns, 'country_code_source', True)
nns = entities_add_country(nns, countries)
nns = entities_clean_address(nns)

cols = ['geo_unit_code']
nns = (nns
        .groupby('numero_national_de_structure', dropna=False)[cols]
        .agg(lambda x: '; '.join(x.dropna().astype(str).unique()))
        .reset_index()
)
print(len(nns))

participation = participation.merge(nns, 
                how='left', 
                on='numero_national_de_structure')

file_name = f"{PATH_CLEAN}participation_current.pkl"
with open(file_name, 'wb') as file:
    pd.to_pickle(participation, file)




# controle des variables - null non null, valeur dupliquée
cols=["stage", "generalPic", "country_code"]
pcheck, pdup = check_dataframe(participation, required_columns=cols)




    # mongo_start(None, perso, None, 'persons')

    
#step5 - si nouvelle actualisation ou changement dans nomenclatures:
if UPDATE_FP==True:
    H2020_process()
    FP7_process()
    FP6_process()