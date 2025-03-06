def entities_preparation():
    import pandas as pd, time, re, numpy as np
    pd.options.mode.copy_on_write = True
    from IPython.display import HTML
    from functions_shared import stop_word, unzip_zip, prep_str_col, work_csv, adr_tag
    from constant_vars import ZIPNAME, FRAMEWORK
    from config_path import PATH_MATCH, PATH_SOURCE, PATH_CLEAN, PATH_ORG, PATH_WORK
    from api_process.matcher import matcher

    print(f"### IMPORT datasets")
    participation = pd.read_pickle(f"{PATH_CLEAN}participation_current.pkl") 
    # participation = pd.read_pickle(f"{PATH_CLEAN}participation_complete.pkl")
    entities_info = pd.read_pickle(f"{PATH_CLEAN}entities_info_current2.pkl")
    # # entities = pd.read_pickle(f"{PATH_WORK}entities_participation_current.pkl")
    proj = pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl")
    nuts = pd.read_pickle("data_files/nuts_complet.pkl")

    countries = pd.read_pickle(f"{PATH_CLEAN}country_current.pkl")
    lien = pd.read_pickle(f"{PATH_CLEAN}lien.pkl")
    perso = pd.read_pickle(f"{PATH_CLEAN}persons_current.pkl")

    pp_app = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'proposals_applicants_departments.json', 'utf8')
    pp_app = pd.DataFrame(pp_app)
    pp_app = pp_app.rename(columns={'proposalNbr':'project_id', 'applicantPic':'pic','departmentApplicantName':'department'}).astype(str)
    pp_app = pp_app.replace({'None': np.nan})
    print(f"- size pp_app: {len(pp_app)}")

    pp_part = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'projects_participants_departments.json', 'utf8')
    pp_part = pd.DataFrame(pp_part)
    pp_part = pp_part.rename(columns={'projectNbr':'project_id', 'participantPic':'pic','departmentParticipantName':'department'}).astype(str)
    pp_part = pp_part.replace({'None': np.nan})
    print(f"- size pp_part: {len(pp_part)}")

########
   
    def prep(stage, df, countries, lien):

        test = df.merge(countries[['countryCode', 'country_code_mapping','country_code']], how='left', on='countryCode')
        test = test.assign(stage=stage).drop(columns=['countryCode','orderNumber', 'departmentUniqueId','framework', 'lastUpdateDate' ]).drop_duplicates()
    #     test['nb'] = test.groupby(['project_id', 'generalPic', 'pic'])['department'].transform('count')

        if stage=='evaluated':
            tmp=(lien.loc[lien.inProposal==True, ['project_id', 'generalPic', 'proposal_orderNumber','proposal_participant_pic', 'calculated_pic', 'nuts_applicants', 'n_app']]
                .rename(columns={'nuts_applicants':'entities_nuts', 'proposal_participant_pic':'pic', 'proposal_orderNumber':'orderNumber', 'n_app':'ent_nb'}))
            tmp=tmp.merge(test, how='inner', on=['project_id',  'generalPic',  'pic'])
        elif  stage=='successful':
            tmp=(lien.loc[lien.inProject==True, ['project_id', 'generalPic', 'orderNumber', 'participant_pic', 'calculated_pic', 'nuts_participant', 'n_part']]
            .rename(columns={'nuts_participant':'entities_nuts', 'participant_pic':'pic', 'n_part':'ent_nb'}))
            tmp=tmp.merge(test, how='inner', on=['project_id',  'generalPic',  'pic'])
        
        tmp.entities_nuts=tmp.apply(lambda x: ','.join(x.strip() for x in x.entities_nuts if x.strip()), axis=1)
        return tmp.sort_values('project_id').drop_duplicates()

    #######
    print("### departments datasets cleaning")
    app=prep('evaluated', pp_app, countries, lien)
    part=prep('successful', pp_part, countries, lien)
    print(f"- app {len(app)}, part {len(part)}")

    lp = part[['project_id', 'generalPic', 'pic', 'country_code_mapping']].drop_duplicates()
    app = app.merge(lp, how='left', indicator=True).query('_merge=="left_only"').drop(columns='_merge')

    #######
    print(f"\n## merge app+part-> struct")
    struct = pd.concat([app, part], ignore_index=True)
    struct['nb_stage'] = struct.groupby(['project_id', 'generalPic', 'country_code', 'orderNumber','calculated_pic','stage'])['department'].transform('count')
    struct = (struct
                .rename(columns={'country_code_mapping':'country_code_mapping_dept', 'country_code':'country_code_dept', 'nutsCode':'department_nuts'}))
    print(f"- size struct {len(struct)}")

    if len(participation[['stage','project_id','generalPic','orderNumber', 'country_code','country_code_mapping']].drop_duplicates())!=len(participation[['stage','project_id','generalPic','orderNumber', 'country_code','country_code_mapping','role','participates_as']].drop_duplicates()):
        print("- Attention doublon d'une participation avec ajout de role+participates_as")

    ########

    print("## merge struct (department) with participation")
    part = participation[['project_id','generalPic','orderNumber', 'country_code','country_code_mapping','stage']].drop_duplicates()
    print(f"- size part {len(part)}")
    part1 = (part
            .merge(struct, 
                how='inner', 
                left_on=['stage','project_id', 'generalPic', 'orderNumber','country_code_mapping'],  
                right_on=['stage','project_id', 'generalPic', 'orderNumber', 'country_code_mapping_dept'])
            .drop_duplicates())
    print(f"- size part1 merge_on['stage','project_id', 'generalPic', 'orderNumber','country']:{len(part1)}")

    part2 = (part.merge(part1[['project_id','generalPic','orderNumber', 'country_code','country_code_mapping','stage']], 
                        how='outer', on=['project_id','generalPic','orderNumber', 'country_code','country_code_mapping','stage'], indicator=True)
            .query('_merge == "left_only"')
            .drop('_merge', axis=1)
            .drop_duplicates()
            .merge(struct, 
                how='inner', 
                left_on=['stage','project_id', 'generalPic', 'orderNumber'],  
                right_on=['stage','project_id', 'generalPic', 'orderNumber'])
            .drop_duplicates())
    print(f"- size part2 merge_on['stage','project_id', 'generalPic', 'orderNumber']:{len(part2)}")

    part2 = pd.concat([part1, part2], ignore_index=True, axis=0)

    part3 = (part.merge(part2[['project_id','generalPic','orderNumber', 'country_code','country_code_mapping','stage']], 
                        how='outer', on=['project_id','generalPic','orderNumber', 'country_code','country_code_mapping','stage'], indicator=True)
            .query('_merge == "left_only"')
            .drop('_merge', axis=1)
            .merge(struct.drop(columns='generalPic'), 
                how='inner', 
                left_on=['stage','project_id', 'orderNumber'],  
                right_on=['stage','project_id', 'orderNumber'])
            .drop_duplicates())
    print(f"- size part3 merge_on['stage','project_id','orderNumber']:{len(part3)}")

    part3 = pd.concat([part2, part3], ignore_index=True, axis=0)
    print(f"- size part3:{len(part3)}")

    part4 = (part.merge(part3[['project_id','generalPic','orderNumber', 'country_code','country_code_mapping','stage']], 
                        how='outer', on=['project_id','generalPic','orderNumber', 'country_code','country_code_mapping','stage'], indicator=True)
            .query('_merge == "left_only"')
            .drop('_merge', axis=1)
            .merge(part3.drop(columns='stage'), 
                how='inner', on=['project_id','generalPic','orderNumber', 'country_code','country_code_mapping']))

    print(f"- size part4 merge_on with part3 ['project_id','generalPic','orderNumber', 'country_code','country_code_mapping']:{len(part4)}")

    part4 = pd.concat([part4, part3], ignore_index=True, axis=0)
    print(f"- size part4:{len(part4)}")

    part5 = (part.merge(part4[['project_id','generalPic','orderNumber', 'country_code','country_code_mapping','stage']], 
                        how='outer', on=['project_id','generalPic','orderNumber', 'country_code','country_code_mapping','stage'], indicator=True)
            .query('_merge == "left_only"')
            .drop('_merge', axis=1))

    print(f"- size part5 reste participation:{len(part5)}")

    part5 = pd.concat([part4, part5], ignore_index=True, axis=0)
    print(f"- size part5:{len(part5)}")

    part5['nb'] = part5.groupby(['stage', 'project_id', 'generalPic', 'orderNumber'])['stage'].transform('count')
    part5['nb2'] = part5.groupby(['stage', 'project_id', 'generalPic', 'orderNumber','country_code_mapping'])['stage'].transform('count')
    part5[['country_code','country_code_mapping']] = part5[['country_code','country_code_mapping']].fillna(part5.groupby(['stage', 'project_id', 'generalPic', 'orderNumber'])[['country_code','country_code_mapping']].ffill())
    print(f"- size part5 {len(part5)}")

    # #remove participation duplicates on ['stage', 'project_id', 'generalPic', 'orderNumber'] and department empty
    part5 = part5.loc[~((part5.nb>1)&(part5.department.isnull()))]
    print(f"- end size with department -> part5 {len(part5)}")

    ##########
    ("## merge participation+entities_info")
    structure = (part5
                .merge(entities_info[['generalPic', 'legalName', 'businessName',
                'category_woven', 'city', 'country_code_mapping', 'country_code',  'country_name_fr', 
                'id_secondaire', 'entities_id', 'entities_name',  'entities_acronym', 'operateur_num', 'postalCode', 
                'street', 'webPage']], 
                how='left', on=['generalPic', 'country_code_mapping', 'country_code'])
                .merge(proj[['project_id', 'call_year']].drop_duplicates(), how='left', on=['project_id'])
                .drop(columns=['nb_stage', 'nb', 'nb2'])
                .drop_duplicates()
                )

    structure = structure.loc[~structure.entities_name.isnull()].drop_duplicates()
    print(f"- size structure + part5: {len(structure)}")

    print("## duplicate vars for cleaning")
    cols = ['department', 'entities_acronym', 'entities_name', 'legalName', 'businessName']
    for i in cols:
        structure[f"{i}_dup"] = structure.loc[:,i]

    if any(structure.call_year.isnull()):
        print(f"- vérification de l'année (corriger les nuls si existants):\n{structure.call_year.value_counts(dropna=False)}")

    ##########
    print("#### CLEANING")
    print("# city, postalcode")
    cols = ['department_dup', 'legalName_dup', 'businessName_dup', 'entities_acronym_dup','entities_name_dup','street','city']
    structure = prep_str_col(structure, cols)

    cedex="cedax|cedrex|cdexe|cdex|credex|cedex|cedx|cede|ceddex|cdx|cex|cexex|edex"
    structure.loc[structure.postalCode.isnull(), 'postalCode'] = structure.loc[structure.postalCode.isnull()].city.str.extract(r"(\d+)")
    structure['city'] = structure.city.str.replace(r"\d+", ' ', regex=True).str.strip()
    structure.loc[structure.country_code=='FRA', 'city'] = structure.city.str.replace(cedex, ' ', regex=True).str.strip()
    structure.loc[structure.country_code=='FRA', 'city'] = structure.city.str.replace(r"^france$", '', regex=True).str.strip()

    ##########
    # creation entities_full = entities_name + entities_acronym et department_tag
    tmp = structure.loc[(structure.legalName_dup!=structure.businessName_dup)&(~structure.businessName_dup.isnull()), ['generalPic',  'country_code', 'legalName_dup', 'businessName_dup']]
    tmp['entities_full'] = [x1 if x2 in x1 else x1+' '+x2 for x1, x2 in zip(tmp['legalName_dup'], tmp['businessName_dup'])]

    if len(structure.drop_duplicates())!=len(structure.merge(tmp[['generalPic', 'country_code', 'legalName_dup', 'businessName_dup', 'entities_full']].drop_duplicates(), how='left', on=['generalPic','businessName_dup', 'legalName_dup','country_code']).drop_duplicates()):
        print("- Attention risque de doublon si merge de tmp et structure")
    else:
        structure = structure.merge(tmp[['generalPic', 'country_code','legalName_dup', 'businessName_dup', 'entities_full']].drop_duplicates(), how='left', on=['generalPic','legalName_dup', 'businessName_dup', 'country_code']).drop_duplicates()
        structure.loc[structure.entities_full.isnull(), 'entities_full'] = structure.entities_name_dup.str.lower()

    #############
    print("## identification Entreprise/association from name")
    societe = pd.read_table('data_files/societe.txt', header=None)
    structure.loc[structure.entities_full.apply(lambda x: True if re.search(r"(?=\b("+'|'.join(list(set(societe[0])))+r")\b)", x) else False), 'org1'] = 'societe'
    societe = societe.loc[societe[0]!='group']
    structure.loc[(~structure.department_dup.isnull())&(structure.department_dup.apply(lambda x: True if re.search(r"(?=\b("+'|'.join(list(set(societe[0])))+r")\b)", str(x)) else False)), 'org1'] = 'societe'
    structure.loc[structure.category_woven=='Entreprise', 'org1'] = 'societe'

    las = r"(\bas(s?)ocia[ctz][aionj]+)|\b(ev|udruga|sdruzhenie|asbl|aisbl|vzw|biedriba|kyokai|mittetulundusuhing|ry|somateio|egyesulet(e?)|stowarzyszenie|udruzenje|zdruzenie|sdruzeni(e?))\b|([a-z]*)(verband|vereniging|asotsiatsiya|zdruzenje)\b|([a-z]*)(verein|forening|yhdistys)([a-z]*)"
    structure.loc[structure.entities_full.apply(lambda x: True if re.search(las , x) else False), 'org2'] = 'association'
    structure.loc[structure.category_woven=='Institutions sans but lucratif (ISBL)', 'org2'] = 'association'

    structure['typ_from_lib'] = structure[['org1','org2']].stack().groupby(level=0).agg(' '.join)
    structure.drop(columns=['org1','org2'], inplace=True)

    print("## stop word to delete")
    stop_word(structure, 'country_code', ['entities_full', 'department_dup'])

    structure['entities_full'] = structure['entities_full_2'].apply(lambda x: ' '.join([s for s in x if s.strip()]))
    structure.loc[(~structure.department_dup.isnull()), 'department_dup'] = structure.loc[(~structure.department_dup.isnull()), 'department_dup_2'].apply(lambda x: ' '.join([s for s in x if s.strip()]))

    structure.drop(columns=['department_dup_2', 'entities_full_2'], inplace=True)
    structure.mask(structure=='', inplace=True)

    ##################################
    #########################
    #################
    ### FRANCE

    print("## create dataset structure_fr")
    structure_fr = structure.loc[structure.country_code=='FRA']
    print(f"size structure_fr: {len(structure_fr)}")

    #############

    def qualif_organisation(x):
        import re
        lpattern = ["cnrs", "inria", "inrae", "ifremer", "inserm", "cea", "ens", "fnsp", "cirad", "ird", "chu", "universite", 
                "pasteur", "curie", "irsn", "onera", "agrocampus", "ed","ecole"]
        ifremer =   r"(ifremer)|(in.* fran.* re.* ex.* mer)"
        cnrs =      r"(ce.* na.* (de )?(la )?re.* sc.[a-z]*)|(fr.* na.* sc.* re.* ce.[a-z]*)|(cnrs)"
        inria =     r"(in.* na.* (de )?re.* (en )?in.* (et )?(en )?au.[a-z]*)|(inria)"
        inrae =     r"(in.* na.* (de )?re.* ag.[a-z]*)|(inra)|(inrae)|(irstea)"
        inserm =    r"(in.* na.* (de )?(la )?sa.* (et )?(de )?(la )?re.* me.[a-z]*)|(inserm)"
        cea =       r"(co.* (a )?l?\'?en.* at.[a-z]*)|(\bcea\b)"
        ens =       r"(ec.* no.* sup[a-z]*)|(\bens\b)"
        fnsp =      r"(fo.* na.* (des )?sc.* po.[a-z]*)|(fnsp)|(sciences po)"
        cirad =     r"(ce.* (de )?co.* in.* (en )?re.* ag.* (pour )?(le )?dev.[a-z]*)|(cirad)"
        ird =       r"(in.* (de )?re.[a-z]* (pour )?(le )?dev.[a-z]*)|\b(ird)\b|(i r d)"
        chu =       r"((ce.*|ctre|group.*) hos.* (univ.[a-z]*)?)|(univ.* hosp.[a-z]*)|\b(chu|chr|chru)\b|(hospice)"
        universite =r"(univ(ersite|ersity|ersitaire))"
        pasteur =   r"(ins([a-z]*|\.*) pasteur( de)?( lille)?)|(pasteur inst([a-z]*))"
        curie =     r"(inst([a-z]*|\.*) curie)|(curie inst([a-z]*))"
        irsn =      r"(in.* (de )?radio.[a-z]* (et )?(de )?sur.[a-z]* nuc.[a-z]*)|(irsn)"
        onera =     r"(onera)|(off.* na.* (d )?etu.* (et )?(de )?rech.* aero.*)"
        agrocampus =r"(agrocampus)"
        ed =        r"(doct.* sch.*)|(ec.* doct.*)|\b(ed)\b"
        ecole =     r"(ecole)"

        org = []
        for pattern_name in lpattern:
            y = re.search(pattern_name, x)
            if y:
                org.append(pattern_name)
        return org

    print("## Identification organisme from pattern -> org_from_lib")
    structure_fr['org1'] = structure_fr.apply(lambda x: qualif_organisation(x['department_dup']) if isinstance(x['department_dup'], str) else [], axis=1)
    structure_fr['org2'] = structure_fr.apply(lambda x: qualif_organisation(x['entities_full']) if isinstance(x['entities_full'], str) else [], axis=1)
    structure_fr['org3'] = structure_fr.apply(lambda x: qualif_organisation(x['entities_name_dup']) if isinstance(x['entities_name_dup'], str) else [], axis=1)

    structure_fr['org_from_lib'] = structure_fr.apply(lambda x: sorted(set(x['org1'] + x['org2'] + x['org3'])), axis=1)
    # structure_fr['org_from_lib'] = structure_fr['org_from_lib'].apply(lambda x: ' '.join(x))

    structure_fr.drop(columns=['org1', 'org2', 'org3'], inplace=True)
    structure_fr.mask(structure_fr=='', inplace=True)


    print("## Identification labos from pattern -> lab_from_lib")
    structure_fr=structure_fr.assign(dep_tag=structure_fr.department_dup, lab_tag=structure_fr.entities_full)
    cols = ['dep_tag', 'lab_tag']
    structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace('international research lab', "irl", regex=False))
    structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace('joint research unit', "jru", regex=False))
    structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace('joint research unit', "jru", regex=False))
    structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace('equipe accueil', "ea", regex=False))
    structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace(r"\bumr(\s?s\s?)(u(\s?)|inserm(\s?))?(?=(\d+)?)|\bu\\s?inserm(\s?)|\bunit(e?)(?=(\s?u?\s?\d+))|\binserm\s?(umr\s?(s?)|jru)\s?(u?)|\binserm(u?)\s?(?=\d+)|\binserm\s?un\s?umr\s?u?", "u", regex=True))
    for s in ['umr','upr','uar','irl','emr','umi','usr','fre','gdr','fr']:
        structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace(r"(?<=\b"+s+r")\s?[a-z]+\s?(?=\d+)", " ", regex=True))
    structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace(r"\bu\s?cnrs|\bum\s+r|\bcnrs\s?(?=\d+)|\bjru\s?(cnrs|umr)", "umr", regex=True))
    structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace(r"\bjru\s?(umi)", "umi", regex=True))
    structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace(r"(\bce[a-z]* inv[a-z]* cl[a-z]*)|(\bcl[a-z]* inv[a-z]* ce[a-z]*)|(\bce[]* cl[a-z]* inv[a-z]*)", "cic", regex=True))
    structure_fr.loc[structure_fr.org_from_lib.map(lambda x: "inserm" in x), cols] = structure_fr.loc[structure_fr.org_from_lib.map(lambda x: "inserm" in x), cols].apply(lambda x: x.str.replace(r"\bjru\b", 'u', regex=True))
    structure_fr.loc[structure_fr.org_from_lib.map(lambda x: "cnrs" in x), cols] = structure_fr.loc[structure_fr.org_from_lib.map(lambda x: "cnrs" in x), cols].apply(lambda x: x.str.replace(r"\bjru\b", 'umr', regex=True))

    llab = ["umr", "ua", "umrs", "umr s","ea", "u", "gdr", "fre", "fr", "frc", "fed", "je", "us", "ums",
            "upr","upesa","ifr","umr a","umemi","epi","eac", "ertint", "ur", "ups", "umr m", "umr t",
            "uar","ert","usr","ura","umr d","rtra","ue","ers","cic","ep","umi", "unit", 'emr', 'irl', 'jru']

    def labo_sigle(x):
        sig = []
        for i in llab:
            pattern = r"\b("+i+r")(?=\b|\d+)\s?[a-z]*\s?(\d+)"
            y = re.search(pattern, x)
            if y:
                sig.append(''.join(y.groups()))
        return sig

    structure_fr['org1'] = structure_fr.apply(lambda x: labo_sigle(x['dep_tag']) if isinstance(x['dep_tag'], str) else [], axis=1)
    structure_fr['org2'] = structure_fr.apply(lambda x: labo_sigle(x['lab_tag']) if isinstance(x['lab_tag'], str) else [], axis=1)
    structure_fr['lab_from_lib'] = structure_fr.apply(lambda x: list(set(x['org1'] + x['org2'])), axis=1)
    # structure_fr['lab_from_lib'] = structure_fr['lab_from_lib'].apply(lambda x: ';'.join(x))
    structure_fr.drop(columns=['org1', 'org2', 'dep_tag', 'lab_tag'], inplace=True)
    structure_fr.mask(structure_fr=='', inplace=True)

    print(f"size structure_fr: {len(structure_fr)}")

    ######################################################
    #RETOUR ORGANISMES
    print("### add data from organisme")
    organisme_back = pd.read_pickle(f"{PATH_ORG}organisme_back.pkl").drop_duplicates()
    organisme_back = organisme_back.drop(columns=['lib_back', 'location_back']).drop_duplicates()
    print(f"- size imported dataset: {len(organisme_back)}")

    stage_proj = structure_fr[['stage', 'project_id']].drop_duplicates()
    organisme1 = (organisme_back
                  .merge(stage_proj, how='inner', on=['project_id'])
                  .drop(columns=['proposal_orderNumber'])
                  .query("stage=='successful'")
                  .drop_duplicates())
    print(f"- identification orga for successful: {len(organisme1)}")

    organisme_back.loc[organisme_back.proposal_orderNumber.isnull(), 'proposal_orderNumber'] = organisme_back.orderNumber
    organisme2 = (organisme_back
                  .merge(stage_proj, how='inner', on='project_id')
                  .drop(columns=['orderNumber'])
                  .rename(columns={'proposal_orderNumber':'orderNumber'})
                  .query("stage=='evaluated'")
                  .drop_duplicates())
    print(f"- identification orga for evaluated: {len(organisme2)}")

    oback = pd.concat([organisme1, organisme2], ignore_index=True)
    oback = (oback.groupby(['stage','project_id', 'generalPic', 'pic', 'orderNumber'], dropna=False)
            .agg(lambda x: ';'.join(map(str, filter(None, x.dropna().unique())))).reset_index())

    oback[['labo_back', 'org_back']] = oback[['labo_back', 'org_back']].apply(lambda x: x.str.lower())
    # oback['labo_back'] = oback.labo_back.str.split(';').tolist()
    print(f"- size oback: {len(oback)}")
    oback.mask(oback=='', inplace=True)
    
    
    ###########################################################################################################

    # MERGE ORGANISMES ET STRUCTURE

    tmp = structure_fr.merge(oback, how='outer', on=['stage','project_id','generalPic', 'pic'], indicator=True, suffixes=('','_y'))
    keep = tmp.loc[tmp._merge!='right_only'].drop(columns='orderNumber_y') #suppr les lignes oback en +

    for i in ['rnsr_back','labo_back','org_back', 'city_back']:
        keep[i] = keep[i].apply(lambda x: x.split(';') if isinstance(x, str) else [])  

    print(f"size keep: {len(keep)}")

    keep['org_merged'] = keep.apply(lambda x: list(set(x['org_back'] + x['org_from_lib'])), axis=1)
    keep.mask(keep=='', inplace=True)

    keep['lab_merged'] = keep.apply(lambda x: list(set(x['labo_back'] + x['lab_from_lib'])), axis=1)

    pattern=r'^[0-9]{9}[A-Z]{1}($|;)'
    keep.loc[keep.id_secondaire.str.contains('0', na=True), 'id_secondaire'] = np.nan
    keep['id_secondaire'] = keep['id_secondaire'].apply(lambda x: x.split(';') if isinstance(x, str) else [])
    keep['rnsr_merged'] = keep.id_secondaire.apply(lambda x: [i for i in x if re.search(pattern, i)])
    keep['rnsr_merged'] = keep.apply(lambda x: list(set(x['rnsr_merged'] + x['rnsr_back'])), axis=1)
    keep.mask(keep=='', inplace=True)

    ################
    labo = keep.loc[((keep.org_merged.str.len() > 0)|(~keep.operateur_num.isnull())), 
        ['call_year','stage','project_id', 'generalPic', 'entities_full', 'department_dup',
        'typ_from_lib', 'org_merged', 'rnsr_merged', 'lab_merged',
        'cp_back', 'city_back', 'operateur_num','category_woven']]
    print(f"size labo dataset: {len(labo)}")

    lab_a_ident = (labo.loc[(keep.rnsr_merged.str.len() == 0), 
                            ['project_id', 'generalPic', 'call_year','department_dup','entities_full','lab_merged', 'city_back']]
                )

    # lab_a_ident['org_merged'] = lab_a_ident['org_merged'].astype(str)          
    lab_a_ident = (lab_a_ident
                    .set_index(['call_year','department_dup','entities_full'])
                    .explode('lab_merged').explode('city_back')
                    .reset_index()
                    .drop_duplicates()
                )
    print(f"size lab_a_ident: {len(lab_a_ident)}")

    #################
    # 1er step matching by id
    ident_by_id = lab_a_ident.loc[~lab_a_ident.lab_merged.isnull(), ['call_year', 'entities_full', 'lab_merged', 'city_back']].drop_duplicates()
    # ident_by_id

    #######
    org = ident_by_id.rename(columns={"city_back": "city", "lab_merged": "labo", 'entities_full':'supervisor'})
    for f in ['supervisor', 'labo', 'city']:
        org[f] = org[f].fillna('')
    org.head()

    df=org.assign(match = None)
    lab_id=pd.DataFrame()

    ######

    typ="rnsr"
    now = time.strftime("%H:%M:%S")

    for i, row in df.iterrows():
        query="{} {} {}".format(row['city'], row['labo'], row['supervisor'])
        strategies = [[['rnsr_code_number', 'rnsr_supervisor_name', 'rnsr_city']],
                    [['rnsr_code_number', 'rnsr_supervisor_name']],
                    [['rnsr_code_number', 'rnsr_city']]]
        matcher(df, i, typ, query, strategies, year=row['call_year'])

    ###

    x=df.loc[df.match.isnull()]
    lab_id=pd.concat([df.loc[~df.match.isnull()], lab_id], ignore_index=True)
    print(len(lab_id))

    lab_id['match2'] = lab_id['match'].astype(str)
    lab_id['match2'] = lab_id.groupby('labo', as_index=False).pipe(lambda x: x['match2'].transform('nunique'))
    if len(lab_id.loc[lab_id.match2>1, ['labo','match']].sort_values('labo'))>1:
        print('un même identifiant de labo a ++ de rnsr: a vérifier')
    else: print('ok')

    #save first step into matcher
    lab_id = lab_id.drop(columns=['q', 'match2'])
    lab_id.mask(lab_id=='', inplace=True)
    work_csv(lab_id, 'ident_lab1')

    #####
    # 2d step matching by name
    lab_ident1 = lab_a_ident.merge(lab_id, how='left', left_on=['entities_full', 'call_year', 'lab_merged', 'city_back'], right_on=['supervisor', 'call_year', 'labo', 'city'], indicator=True)
    lab_a_ident = lab_ident1.loc[lab_ident1._merge=='left_only'].drop(columns=['_merge', 'match', 'labo', 'city', 'supervisor'])
    ident_by_lib = lab_a_ident[['call_year', 'department_dup', 'lab_merged', 'entities_full', 'city_back']]
    ident_by_lib['labo'] = ident_by_lib[[ 'department_dup', 'lab_merged']].stack().groupby(level=0).apply(lambda x: ' '.join(x))
    org = ident_by_lib.rename(columns={"city_back": "city", 'entities_full':'supervisor'}).drop_duplicates()
    print(len(org))
    for f in ['supervisor', 'labo', 'city']:
        org[f] = org[f].fillna('')

    df=org.assign(match = None)

    #######################
    typ="rnsr"
    now = time.strftime("%H:%M:%S")

    for i, row in df.iterrows():
        query="{} {} {}".format(row['city'], row['labo'], row['supervisor'])

        strategies = [
            [['rnsr_acronym', 'rnsr_name', 'rnsr_supervisor_name']],
    #                   [['rnsr_acronym', 'rnsr_name' 'rnsr_supervisor_name']],
    #                   [['rnsr_name', 'rnsr_supervisor_name', 'rnsr_city']],
                    [[ 'rnsr_name', 'rnsr_supervisor_name']],
                    [['rnsr_acronym', 'rnsr_supervisor_name']]
        ]
        matcher(df, i, typ, query, strategies, year=row['call_year'])

    x=df.loc[df.match.isnull()]
    lab_id=pd.concat([df.loc[~df.match.isnull()], lab_id], ignore_index=True)

    lab_id = lab_id.drop(columns=['q'])
    lab_id.mask(lab_id=='', inplace=True)
    lab_id.to_pickle(f"{PATH_WORK}match_lab2.pkl", compression='gzip')

    # lab_id=pd.read_csv(f"{PATH_WORK}ident_lab2.csv", sep=';')
    # lab_id['call_year']=lab_id['call_year'].astype(str)
    # lab_id['match'] = lab_id.match.apply(lambda x: ast.literal_eval(x))
    ####################################################

    lab_ident2 = lab_a_ident.merge(lab_id, how='left', left_on=['entities_full', 'call_year', 'lab_merged', 'city_back', 'department_dup'], right_on=['supervisor', 'call_year',  'lab_merged', 'city', 'department_dup'], indicator=True)
    lab_a_ident = lab_ident2.loc[lab_ident2._merge=='left_only'].drop(columns=['_merge', 'match', 'labo', 'city', 'supervisor'])
    lab_a_ident = lab_a_ident.loc[~lab_a_ident.department_dup.isnull()]
    print(len(lab_a_ident))

    df=structure.loc[structure.country_code!='FRA', ['entities_full', 'country_code']].drop_duplicates()
    df=df.fillna('')

    ###################
    #normalement code pour identifier la langue des libellés et affilier avec ror; voir autre repo gitHub he_provisoire
   ##############################################################
   
    print(lab_ident1.columns)
    print(lab_ident2.columns)
    # print(lab_ident3.columns)

    # lab_ident3 -> libelle traduit ne fonctionne pas avec googletrans pour l'instant

    lab_ident = (pd.concat([lab_ident1.loc[lab_ident1._merge=='both'].drop(columns=['supervisor', 'labo', 'city', '_merge']), 
                        lab_ident2.loc[lab_ident2._merge=='both'].drop(columns=['supervisor', 'labo', 'city', '_merge']), 
    #                        lab_ident3.loc[~lab_ident3.match.isnull()]
                        ],
                        ignore_index=True)
    #             .drop(columns=['department_dup_trad', '_merge', 'lang'])
                )
    lab_ident.loc[lab_ident.match.str.len()>1, 'resultat'] = 'a controler'
    # lab_ident.match = lab_ident.match.apply(lambda x: list(x))

    print(len(lab_ident))

    lab_ident = (lab_ident.groupby(['call_year', 'entities_full', 'department_dup', 'project_id', 'generalPic'], as_index = False)
    .agg({'match':'sum', 'resultat': lambda x: ' '.join(x.fillna('').unique()).strip()})          
    )
    lab_ident.mask(lab_ident=='', inplace=True)

    #40739 
    keep=keep.merge(lab_ident, how='left', on=['call_year', 'entities_full', 'department_dup', 'project_id', 'generalPic'])
    print(len(keep))
    for i in ['rnsr_merged', 'match']:
        keep.loc[keep[i].isnull(), i] =keep.loc[keep[i].isnull(), i].apply(lambda x: [])

    keep['rnsr_merged'] = keep.apply(lambda x: list(set(x['rnsr_merged'] + x['match'])), axis=1)

    print(f"size keep: {len(keep)}\nkep columns -> check if orderNumber_x/y and fix it:\n{keep.columns}")

    keep.to_pickle(f'{PATH_MATCH}structure_fr.pkl')


    ##############################
    # etranger

    struct_et = structure.loc[structure.country_code!='FRA']
    print(f"longueur struct etranger: {len(struct_et)}")
    df = (struct_et.loc[struct_et.entities_id.str.contains(r'^[^R0]', regex=True), 
                        ['entities_full', 'entities_id', 'country_code_mapping', 'country_code', 'city']]
        .drop_duplicates()
        .assign(match=None)
        )

    typ="ror"
    now = time.strftime("%H:%M:%S")

    for i, row in df.iterrows():
        query="{} {} {}".format(row['city'], row['country_code'], row['entities_full'])

        strategies = [
            [['ror_name', 'ror_country']],
            [['ror_name', 'ror_acronym', 'ror_country', 'ror_city']],
            [['ror_name', 'ror_country', 'ror_city']]
        ]
        matcher(df, i, typ, query, strategies)

    df = df.loc[~df.match.isnull(), ['match']]
    df.to_pickle(f"{PATH_WORK}match_ror.pkl", compression='gzip')
    struct_et = pd.concat([struct_et, df], axis=1)
    struct_et.loc[struct_et.match.str.len()>1, 'resultat'] = 'a controler'
    struct_et.mask(struct_et=='', inplace=True)

    struct_et.to_pickle(f'{PATH_MATCH}struct_et.pkl')


    ############################################
    print("### create entities_all")

    entities_all = pd.concat([keep,  struct_et], ignore_index=True, axis=0)
    print(f"size entities_all: {len(entities_all)}")


    ########
    print("## add PERSO")
    perso = (perso[['project_id', 'generalPic', 'stage', 'tel_clean', 'email',
       'domaine_email', 'contact', 'num_nat_struct']]
       .drop_duplicates()
       .mask(perso == ''))

    print(f"size perso for merging: {len(perso)}")
    var_perso=['tel_clean', 'domaine_email', 'contact', 'num_nat_struct', 'email']
    perso=(perso.groupby(['project_id', 'generalPic', 'stage'], as_index=False)[var_perso]
        .agg(lambda x: ';'.join( x.dropna().unique()))
        .drop_duplicates())

    print(f"size entities_all before perso: {len(entities_all)}")
    tmp=(entities_all.drop(columns='_merge')
        .merge(perso, how='left', on=['project_id','generalPic', 'stage'], indicator=True))
    print(f"size entities_all after perso: {len(tmp)}")

    tmp1=tmp[tmp._merge=='both']

    var_perso.append('_merge')
    var_perso.remove('contact')
    tmp2=(tmp[tmp._merge=='left_only']
        .drop(columns=var_perso)
        .merge(perso.drop(columns=['stage'])
        .drop_duplicates(), how='inner', on=['project_id','generalPic', 'contact']))

    if len(tmp2)>0:
        # tmp=pd.concat([tmp[tmp._merge=='left_only'], tmp1, tmp2], ignore_index=True)
        print(f"A verifier code si tmp2 n'est pas null: {len(tmp)}")
    else:
        entities_all=pd.concat([tmp[tmp._merge=='left_only'], tmp1], ignore_index=True)
        print(f"size entities_all after perso clean: {len(tmp)}")

    entities_all=entities_all.mask(entities_all=='')
    entities_all.loc[entities_all.rnsr_back.str.len()>0, 'source_rnsr'] = 'orga'
    entities_all.loc[(entities_all.source_rnsr.isnull())&(entities_all.rnsr_merged.str.len()>0), 'source_rnsr'] = 'corda'
    entities_all.loc[(entities_all.source_rnsr.isnull())&(~entities_all.num_nat_struct.isnull()), 'source_rnsr'] = 'openalex'
    entities_all.loc[entities_all.source_rnsr=='openalex', 'resultat'] = 'a controler'

    entities_all['num_nat_struct'] = entities_all['num_nat_struct'].map(lambda x: x.split(';') if isinstance(x, str) else [])
    entities_all.loc[entities_all.rnsr_merged.isnull(), 'rnsr_merged'] = entities_all.loc[entities_all.rnsr_merged.isnull(),'rnsr_merged'].apply(lambda x: [])

    entities_all.loc[(entities_all.source_rnsr=='corda')|(entities_all.source_rnsr=='openalex'), 'rnsr_merged'] = entities_all.apply(lambda x: list(set(x['rnsr_merged'] + x['num_nat_struct'])), axis=1)


    ########
    print("## geoloc cleaning")
    stop_word(entities_all, 'country_code', ['street'])

    tmp=entities_all.loc[(entities_all.country_code=='FRA')&(~entities_all.postalCode.isnull()), ['postalCode']].drop_duplicates()
    tmp['code_postal'] = tmp.postalCode.str.replace(r"\D*", '', regex=True).str.strip()
    tmp['code_postal'] = tmp.code_postal.map(lambda x: np.nan if len(x)!=5. else x)

    entities_all = pd.concat([entities_all, tmp.drop(columns='postalCode')], axis=1)
    entities_all.loc[entities_all.code_postal.isnull(), 'code_postal'] = entities_all.loc[entities_all.code_postal.isnull(), 'postalCode']

    HTML(entities_all.loc[(entities_all.country_code=='FRA')&(~entities_all.city.isnull()), ['city']].drop_duplicates().sort_values('city').to_html())

    # tmp = entities_all.loc[entities_all.country_code=='FRA', ['street_2']]
    # tmp = adr_tag(tmp, ['street_2'])
    # entities_all = pd.concat([entities_all.drop(columns='street_2'), tmp], axis=1)

    tmp = entities_all[['country_code','street_2']]
    tmp = adr_tag(tmp, ['street_2'])
    entities_all = pd.concat([entities_all.drop(columns='street_2'), tmp], axis=1)


    entities_all.loc[entities_all.country_code.isin(['FRA','BEL','LUX']), 'city'] = entities_all.city.str.replace(r"\bst\b", 'saint', regex=True).str.strip()
    entities_all.loc[entities_all.country_code.isin(['FRA','BEL','LUX']), 'city'] = entities_all.city.str.replace(r"\bste\b", 'sainte', regex=True).str.strip()

    entities_all.loc[~entities_all.city.isnull(), 'city_tag'] = entities_all.loc[~entities_all.city.isnull()].city.str.replace(r"\s+", '-', regex=True)

    entities_all.to_pickle(f'{PATH_MATCH}entities_all.pkl')