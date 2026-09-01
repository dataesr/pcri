# table topics'''

from paths import PATH_SOURCE, PATH_CLEAN, PATH_WP
from functions_shared import unzip_zip, clean_text, convert_to_paris_date
import pandas as pd, numpy as np, json, ast, re
from dateutil import parser as dateparser
from zoneinfo import ZoneInfo


def safe_extract(value):
    if isinstance(value, list):
        return value[0] if len(value) > 0 else None
    return value

def explode_budget_overview(df, debug=False):
    rows = []
    unmatched = []

    for idx, row in df.iterrows():
        identifier = safe_extract(row['identifier'])
        types_of_action = safe_extract(row['typesOfAction'])
        call_identifier = safe_extract(row['callIdentifier'])
        budget_raw = safe_extract(row['budgetOverview'])

        if budget_raw is None or (isinstance(budget_raw, float) and math.isnan(budget_raw)):
            continue

        if isinstance(budget_raw, dict):
            budget = budget_raw
        elif isinstance(budget_raw, str):
            try:
                budget = json.loads(budget_raw)
            except json.JSONDecodeError:
                continue
        else:
            continue

        topic_map = budget.get('budgetTopicActionMap', {})

        # Nombre total d'entrées dans toute la map (tous topics confondus)
        total_entries = sum(len(v) for v in topic_map.values())

        matched_any = False

        for topic_id, entries in topic_map.items():
            for entry in entries:
                action_str = entry.get('action', '')

                if ' - ' in action_str:
                    action_id_part, action_type_part = action_str.split(' - ', 1)
                else:
                    action_id_part, action_type_part = None, action_str

                match = False

                # Cas 1 : identifiant présent dans l'action
                if action_id_part and identifier and (
                    action_id_part == identifier or identifier.startswith(action_id_part)
                ):
                    match = True
                # Cas 2 : correspondance via typesOfAction
                elif types_of_action and action_type_part.strip() == types_of_action.strip():
                    match = True
                # Cas 3 (fallback) : un seul topic/une seule entrée dans TOUTE la map
                # -> ne peut appartenir qu'à cette ligne, pas d'ambiguïté possible
                elif total_entries == 1:
                    match = True

                if match:
                    matched_any = True
                    rows.append({
                        'identifier': identifier,
                        'callIdentifier': call_identifier,
                        'typesOfAction': types_of_action,
                        'topicId': topic_id,
                        'action': action_str,
                        'expectedGrants': entry.get('expectedGrants'),
                        'minContribution': entry.get('minContribution'),
                        'maxContribution': entry.get('maxContribution'),
                        'plannedOpeningDate': entry.get('plannedOpeningDate'),
                        'deadlineModel': entry.get('deadlineModel'),
                        'deadlineDates': entry.get('deadlineDates'),
                        **{f'budget_{y}': v for y, v in entry.get('budgetYearMap', {}).items()}
                    })

        if not matched_any:
            unmatched.append({'index': idx, 'identifier': identifier, 'typesOfAction': types_of_action})

    if debug and unmatched:
        print(f"{len(unmatched)} lignes sans correspondance :")
        for u in unmatched:
            print(u)

    return pd.DataFrame(rows)


def action_clean(info):
    """
    action -> types_of_action
    
    """
    # mapping "phrase complète" -> code, utilisé seulement en dernier recours
    act_map = {
        'HORIZON Research and Innovation Actions': 'RIA',
        'HORIZON Coordination and Support Actions': 'CSA',
        'HORIZON Innovation Actions': 'IA',
        'HORIZON EIC Grants': 'EIC',
        'HORIZON Recognition Prize': 'RPR',
        'HORIZON Inducement Prize': 'IPR',
        'HORIZON ERC Grants': 'ERC',
    }

    # codes "officiels" qu'on cherche comme token dans action_tmp
    action_codes = ['RIA', 'IA', 'CSA', 'MSCA', 'COFUND', 'PCP', 'PPI', 'FPA', 'EIC', 'RPR', 'ERC', 'IPR', 'KICS']

    def normalize(s):
        if pd.isna(s):
            return s
        return re.sub(r'\s+', ' ', str(s).strip())

    act_map_norm = {normalize(k): v for k, v in act_map.items()}

    def extract_action_code(types_of_action, action_tmp, topic_code=None):
        toa_norm = normalize(types_of_action)

        # 1. EIT -> KICS
        if isinstance(toa_norm, str) and 'EIT' in toa_norm.upper():
            return 'KICS'

        # 2. token dans action_tmp (ou typesOfAction si action_tmp est vide)
        text = action_tmp if pd.notna(action_tmp) else types_of_action
        if pd.notna(text):
            tokens = re.split(r'[-\s]+', str(text).upper())
            for code in action_codes:
                if code in tokens:
                    return code

        # 3. correspondance exacte typesOfAction -> act_map
        if toa_norm in act_map_norm:
            return act_map_norm[toa_norm]

        # 4. fallback topicCode : UNIQUEMENT si typesOfAction est NaN
        if pd.isna(types_of_action):
            tc_norm = normalize(topic_code)
            if tc_norm in act_map_norm:
                return act_map_norm[tc_norm]

        return np.nan

    info['action_code'] = info.apply(
        lambda r: extract_action_code(r['typesOfAction'], r['action_tmp'], r.get('topicCode')),
        axis=1
    )

    # nettoyage topicCode si besoin (comme dans ta version originale)
    info.loc[info['topicCode'].isin(act_map.keys()), 'topicCode'] = np.nan

    print(f"- size info after action process: {len(info)}")

    return info


def check_topic_in_call():

    return



def topics_portal_clean():
    """
    1. load topic_info_harvest from tenders portal API
    2. filter on type 1 = direct calls
    3.keep columns of interest + explode lists
    4. filter actions
    5. explode budgetYearMap
    6. extract call_year from call_id and call_lib -> topics which year does not match with wp
    7. topics_year.json fix missing year for some topics
    8. save in data_clean/topic_call_info.pkl    
    """
    top = json.load(open(f"{PATH_WP}topic_info_harvest.json"))
    top=pd.DataFrame(top)
    top['budgeted'] = np.where(top['budgetOverview'].notna(), True, False)
    print(f"- size top base: {len(top)}")


    result = explode_budget_overview(top, debug=True)


    cols = ['identifier', 'title', 'type', 'typesOfAction', 'callIdentifier', 'callTitle', 'startDate', 'deadlineDate']
    # Explode each column (if lists)
    for col in cols:
        tmp = tmp.explode(col)
        tmp[col] = tmp[col].str.strip()
    check=tmp.loc[(~tmp['identifier'].isin(result['identifier'].unique()))&(tmp['type']!='8')]
    print(f"🔶 type calls with budget info \n{tmp[tmp['_rows'].notna()].type.value_counts()}")


    info['topicCode'] = (
            info["action"].str.split(" - ").str[0]
            .str.replace('\xa0', ' ', regex=False)
            .str.strip()
            .str.replace(r'\s+', ' ', regex=True)
        )
    info['action_tmp'] = (
            info["action"].str.split(" - ").str[1]
            .str.replace('\xa0', ' ', regex=False)
            .str.strip()
            .str.replace(r'\s+', ' ', regex=True)
        )

    info = action_clean(info)

    # info.loc[(info['callIdentifier'].str.startswith('ERC-'))&(~info['topicCode'].str.startswith('ERC-')), 'topicCode'] = info.loc[(info['callIdentifier'].str.startswith('ERC-'))&(~info['topicCode'].str.startswith('ERC-')), 'callIdentifier']

    
    info = info.drop(columns=["_rows", "budgetTopicActionMap", "action", 'action_tmp', 'typesOfAction'], errors="ignore")
    
    # Explosion de budgetYearMap : gère str ET dict
    def parse_budget_year_map(x):
        if isinstance(x, str):
            x = json.loads(x)
        if isinstance(x, dict):
            return [{"budget_year_map_year": k, "budget_year_map_budget": v} for k, v in x.items()]
        return [{}]
    
    info["budgetYearMap"] = info["budgetYearMap"].apply(parse_budget_year_map)
    info = info.explode("budgetYearMap").reset_index(drop=True)
    
    info["budget_year_map_year"]   = info["budgetYearMap"].apply(lambda x: x.get("budget_year_map_year")   if isinstance(x, dict) else None)
    info["budget_year_map_budget"] = info["budgetYearMap"].apply(lambda x: x.get("budget_year_map_budget") if isinstance(x, dict) else None)
    info["budget_year_map_budget"] = pd.to_numeric(info["budget_year_map_budget"], errors="coerce") 

    info = info.drop(columns=["budgetYearMap"])

    def parse_date_list(raw):

        PARIS = ZoneInfo("Europe/Paris")
        # gère le cas où raw est déjà une liste/array (pas un scalaire)
        if isinstance(raw, (list, tuple)):
            items = raw
        else:
            if pd.isna(raw):
                return raw
            if str(raw).strip().lower() == "none":
                return None
            try:
                items = ast.literal_eval(raw)
            except (ValueError, SyntaxError, TypeError):
                items = [raw]

        if items is None:
            return None
        if not isinstance(items, (list, tuple)):
            items = [items]

        harmonized = []
        for item in items:
            if item is None:
                continue
            item = str(item).strip()
            if not item or item.lower() == "none":
                continue
            try:
                dt = dateparser.parse(item, dayfirst=False, yearfirst=True)
            except (ValueError, OverflowError):
                continue
            dt = dt.replace(tzinfo=PARIS) if dt.tzinfo is None else dt.astimezone(PARIS)
            harmonized.append(dt.strftime("%Y-%m-%d"))

        return harmonized if harmonized else None

    info['deadlineDates'] = info['deadlineDates'].apply(parse_date_list)
    info['deadlineDates'] = info['deadlineDates'].apply(lambda x: ';'.join(x) if isinstance(x, list) else x)

    
    cols = [c for c in info.columns if c != 'budget_year_map_budget' and c != 'budget_year_map_year']
    info = (info.groupby(cols, dropna=False)['budget_year_map_budget'].sum()
          .reset_index()
    )

    check_dup = info[info.duplicated(subset=['topicCode', 'callIdentifier', 'action_code'], keep=False)]
    if not check_dup.empty:
        print(f"- ⚠️ ! duplicate topicCode/callIdentifier/action_code in info: {check_dup[['topicCode', 'callIdentifier', 'action_code']]}")

    

    check_dup = info[info.duplicated(subset=['topicCode', 'callIdentifier', 'action_code', 'destination_code'], keep=False)]
    if not check_dup.empty:
        print(f"- ⚠️ ! duplicate topicCode/callIdentifier/action_code in info: {check_dup[['topicCode', 'callIdentifier', 'action_code']]}")
        cols_to_group = [c for c in check_dup.columns if c != 'budget_year_map_budget']
        check_dup = check_dup.groupby(cols_to_group, dropna=False).agg({'budget_year_map_budget':'sum'}).reset_index()

    info = info.merge(check_dup[['topicCode', 'action_code', 'destination_code']].drop_duplicates(), 
                      on=['topicCode', 'action_code', 'destination_code'], 
                      how='left', 
                      indicator=True)
    info = info[info['_merge'] == 'left_only'].drop(columns=['_merge'])
    info = pd.concat([info, check_dup], ignore_index=True)
    print(f"- size info after sum dup: {len(info)}")

    # tous calls type=1,8 add call_year from callIdentifier and callTitle
    tmp['call_year'] = tmp['callIdentifier'].str.extract(r'(\d{4})')
    tmp['call_year_wp'] = tmp['callTitle'].str.extract(r"\(WP (\d{4})\)")

    tmp.loc[tmp['type']=='8', 'call_year'] = tmp.loc[tmp['type']=='8', 'identifier'].str.extract(r'(\d{4})', expand=False)

    if any(tmp['call_year'].isnull()):
        fix_year = json.load(open('data_files/topics_year.json'))
        tmp.loc[tmp['identifier'].isin(fix_year.keys()), 'call_year'] = tmp.loc[tmp['identifier'].isin(fix_year.keys()), 'identifier'].map(fix_year)
        if any(tmp['call_year'].isnull()):
            print(f"- ⚠️ ! missing call_year by type projects for: {tmp[tmp['call_year'].isnull()].type.value_counts()}")


    for d in ['startDate', 'deadlineDate']:
        tmp[d] = convert_to_paris_date(tmp[d])
        
    tmp['call_open_date'] = tmp['startDate'].dt.normalize()
    tmp['call_deadline'] = tmp['deadlineDate'].dt.normalize()
    tmp['call_deadline_ma'] = tmp['call_deadline'].dt.strftime('%Y-%m')

    cols_call = ['identifier', 'type', 'callIdentifier', 'callTitle', 'call_open_date', 'call_deadline', 'call_year', 'call_year_wp', 'call_deadline_ma', 'link']
    df = tmp[cols_call].drop_duplicates()

    cols_to_fill = [c for c in df.columns if c != 'callIdentifier']
    df[cols_to_fill] = (
        df.groupby('callIdentifier')[cols_to_fill]
        .transform(lambda s: s.ffill().bfill())
    )

    df = df.drop_duplicates().reset_index(drop=True)
    print(f"size df == 1 : {len(df)}")

    # inner join between info and df
    call_i = info.groupby(['callIdentifier', 'deadlineModel'])['deadlineDates'].nunique().reset_index(name='nb')
    df_i = df.merge(call_i, how='inner', on=['callIdentifier'])
    df_i['nb_rows'] = df_i.groupby(['callIdentifier', 'identifier'])['call_deadline'].transform('size')

    # masque des lignes concernées par la règle
    mask_multi = (df_i['nb'] == 1) & (df_i['nb_rows'] > 1)

    # lignes non concernées : on les garde telles quelles
    df_keep_as_is = df_i[~mask_multi]

    # lignes concernées : pour chaque callIdentifier, ne garder que la deadline la plus tardive
    df_multi_resolved = (
        df_i[mask_multi]
        .sort_values(['call_deadline', 'identifier'])
        .groupby(['callIdentifier', 'identifier'], as_index=False)
        .tail(1)
    )

    # recombiner
    df_i = pd.concat([df_keep_as_is, df_multi_resolved], ignore_index=True)

    df_i = (df_i.drop(columns=['nb', 'nb_rows', 'deadlineModel'], errors='ignore')
            .merge(info, how='outer', 
                        left_on=['callIdentifier', 'identifier'],
                        right_on=['callIdentifier', 'topicCode'],
                        indicator=True)
    )

    print(f"- size df_i after merge with info: {len(df_i)}\n- check topic or call alone: {df_i._merge.value_counts()}")

    df_i.loc[df_i['_merge']=='left_only', 'topicCode'] = df_i.loc[df_i['_merge']=='left_only', 'identifier'] 
    df_i.drop(columns=['identifier'], inplace=True)

    cols_to_fill = [x for x in cols_call if x not in ('identifier', 'callIdentifier')]
    df_i[cols_to_fill] = (
        df_i.groupby('callIdentifier')[cols_to_fill]
        .transform(lambda s: s.ffill().bfill())
    )
    print(f"- size df_i after merge with info: {len(df_i)}\n- check topic or call alone: {df_i._merge.value_counts()}")


    df_i = df_i.assign(type_call = 'Direct call').drop(columns=['link', '_merge'])

    # casc = tmp[tmp.type=='8']

    # df.loc[df['type']=='1', 'type_funding'] = 'Direct call'
    # df.loc[df['type']=='8', 'type_funding'] = 'Cascading funding'

    # print(f"- size tops type 1: {len(df)}\n- multi topics: {df['topicCode'].value_counts(dropna=False)}")
    # if len(df.groupby('topicCode')['call_year'].nunique().reset_index(name='nb').query('nb>1'))>1:
    #     print(f"- number of year by topicCode if multi it's a prob: {df.groupby('topicCode')['call_year'].nunique().reset_index(name='nb').query('nb>1')}")
    
    df_i.to_pickle(f"{PATH_CLEAN}topic_call_info.pkl")
    return df_i


def top_div_load(source):
    data = unzip_zip(source, 'topics.json', 'utf8')
    print(f'1 - topics -> {len(data)}')
    topics = pd.DataFrame(data)[["topicCode","topicDescription"]].drop_duplicates() 

    # DIVISIONS 
    top_div = unzip_zip(source, 'topicLbDivisions.json', 'utf8')
    print(f'2 - divisions -> {len(top_div)}')

    return topics, top_div


def topics_divisions(topics, top_div):
    """
    create nomenclature thema/destination from topic/division
     - filter on isPrincipal = True to keep only main topic/division link (some topics are linked to several divisions but only 1 is principal)
    each step detailed in the code with print to check the results and identify potential issues 

    data to use for the nomenclature thema/destination: destination.json, thema.json, programme_fr.json
    if new useful destination_code or thema_code are identified:
        - add pattern in dictionary in the code to assign them to the right thema/destination
        - add them in the source json and in the data_files json (destination.json, thema.json) with the corresponding lib to be able to merge with the topic/division data and create the final nomenclature
    """
    print("### TOPICS")

    df = pd.DataFrame(top_div).drop(['lastUpdateDate'], axis=1)
    df['tmp'] = np.where(df.isPrincipal == True, 1 , 0)
    table = pd.pivot_table(df,index=['topicCode'],columns=['divisionAbbreviation'],values=['tmp'],aggfunc=pd.Series.nunique,margins=True,dropna=True)
    if [table['tmp']['All']>1]==True:
        pd.DataFrame(table).to_csv("/he_data/traitement_topic_horizon.csv", sep=";", encoding="utf-8", na_rep="")
        print('3 - verifier les doublons topic/division isPrincipal dans he_data/traitement_topic_horizon.csv')

    df = df[df.isPrincipal == True]
    df = df.dropna(axis=1, how='all').drop(['lvl1Code','lvl1Description','isPrincipal','tmp'], axis=1).drop_duplicates()

    topics_divisions = df.merge(topics, how='left', on='topicCode')

    divisions = df[['lvl2Code', 'lvl2Description', 'lvl3Code', 'lvl3Description', 'lvl4Code', 'lvl4Description']].drop_duplicates()

    # add niveau prog pilier
    horizon = (topics_divisions
               .rename(columns={"lvl2Code": "pilier_code", "lvl2Description": "pilier_name_en", "lvl3Code": "programme_code", 
                                "lvl3Description": "programme_name_en",'topicDescription': 'topic_name'})
              .drop(columns=['lvl4Code','lvl4Description', 'divisionAbbreviation', 'divisionDescription', 'framework'])) 

    horizon['topic_name'] = horizon['topic_name'].apply(clean_text)

    destination = pd.read_json(open('data_files/destination.json', 'r+', encoding='utf-8'))
    destination = pd.DataFrame(destination)

    ########################################################

    #ERC
    ERC = topics_divisions.loc[topics_divisions['lvl3Code']=="HORIZON.1.1", ['topicCode']].assign(thema_code='ERC')
    typ = ["POC", "COG", "STG", "ADG", "PERA", "SyG", "SJI"]

    for i in typ:
        ERC.loc[ERC.topicCode.str.contains(i), 'destination_code'] = i

    if any(pd.isna(ERC.destination_code.unique())):
        print(f'erc : destination_code à null après traitement\n{ERC[ERC.destination_code.isnull()].topicCode.unique()}')
        ERC.loc[ERC.destination_code.isnull(), 'destination_code'] = 'ERC-OTHER'

    ############################################################################
    # MSCA
    MSCA = topics_divisions.loc[topics_divisions['lvl3Code']=="HORIZON.1.2", ['topicCode']].assign(thema_code='MSCA')
    typ = ["COFUND", "SE", "PF", "DN","CITIZENS"]

    for i in typ:
        MSCA.loc[MSCA.topicCode.str.contains(i), 'destination_code'] = i

    if any(pd.isna(MSCA.destination_code.unique())):
        print(f'MSCA : destination_code à null après traitement\n{MSCA[MSCA.destination_code.isnull()].topicCode.unique()}')
        MSCA.loc[MSCA.destination_code.isnull(), 'destination_code'] = 'MSCA-OTHER'  

    #######################################################################################################""
    #INFRA
    INFRA = topics_divisions.loc[topics_divisions['lvl3Code']=="HORIZON.1.3", ['topicCode']].assign(thema_code='INFRA')
    inf={'EOSC':'INFRAEOSC',
    'DEV':'INFRADEV',
    'SERV':'INFRASERV',
    'TECH':'INFRATECH',
    '-NET-':'INFRANET'
    }

    for k,v in inf.items():
        INFRA.loc[INFRA.topicCode.str.contains(k), 'destination_code'] = v
    if any(pd.isna(INFRA.destination_code.unique())):
        print(f'INFRA : destination_code à null après traitement\n{INFRA[INFRA.destination_code.isnull()].topicCode.unique()}')
        INFRA.loc[INFRA.destination_code.isnull(), 'destination_code'] = 'DESTINATION-OTHER'

    # ####################################################################################
    # # CLUSTER

    CLUSTER = topics_divisions.loc[(topics_divisions.lvl2Code=='HORIZON.2')&(topics_divisions.topicCode.str.contains('-CL\\d{1}-|-HLTH-', regex=True))]
    CLUSTER = CLUSTER[['topicCode']]

    CLUSTER['destination_code'] = CLUSTER['topicCode'].str.split('-').str.get(3)
    CLUSTER.loc[~CLUSTER.destination_code.isin(destination.destination_code.unique()), 'destination_code'] = np.nan

    cl={'-HLTH-':'HEALTH-OTHER',
    '-CL2-':'CCSI-OTHER',
    '-CL3-':'CSS-OTHER',
    '-CL4-':'DIS-OTHER',
    '-CL5-':'CEM-OTHER',
    '-CL6-':'BIOENV-OTHER'
    }
    for k,v in cl.items():
        CLUSTER.loc[(CLUSTER.destination_code.isnull())&(CLUSTER.topicCode.str.contains(k)), 'destination_code'] = v
    if any(pd.isna(CLUSTER.destination_code.unique())):
        print('CLUSTER : ⚠️ encore destination_code à null après traitement')

    CLUSTER['temp']=CLUSTER.topicCode.str.split('-').str.get(1)
    l_cluster=pd.DataFrame.from_dict({'HLTH':'CLUSTER 1', 'CL2':'CLUSTER 2', 'CL3':'CLUSTER 3', 'CL4':'CLUSTER 4','CL5':'CLUSTER 5', 'CL6':'CLUSTER 6'}, orient='index', columns=['thema_code']).reset_index()
    CLUSTER = CLUSTER.merge(l_cluster, how='left', left_on='temp', right_on='index').drop(columns=['index', 'temp'])

    mask=(CLUSTER.thema_code=='CLUSTER 4')
    CLUSTER.loc[mask&(CLUSTER.destination_code.isin(['RESILIENCE','TWIN'])), 'thema_code'] = CLUSTER.thema_code+'-Industry'
    CLUSTER.loc[mask&(CLUSTER.destination_code.isin(['DATA','DIGITAL','HUMAN'])), 'thema_code'] = CLUSTER.thema_code+'-Digital'
    CLUSTER.loc[mask&(CLUSTER.destination_code.isin(['SPACE'])), 'thema_code'] = CLUSTER.thema_code+'-Space'

    mask=(CLUSTER.thema_code=='CLUSTER 5')
    CLUSTER.loc[mask&(CLUSTER.destination_code.isin(['D'+str(i) for i in range(1, 2)])), 'thema_code'] = CLUSTER.thema_code+'-Climate'
    CLUSTER.loc[mask&(CLUSTER.destination_code.isin(['D'+str(i) for i in range(2, 5)])), 'thema_code'] = CLUSTER.thema_code+'-Energy'
    CLUSTER.loc[mask&(CLUSTER.destination_code.isin(['D'+str(i) for i in range(5, 7)])), 'thema_code'] = CLUSTER.thema_code+'-Mobility'


    ################################################################
    #### autres pilier 2
    # MISSION
    miss = (topics_divisions
        .loc[(topics_divisions.lvl2Code=='HORIZON.2')&(topics_divisions.topicCode.str.contains('MISS')),
                ['topicCode','lvl3Code']]
        .assign(programme_code='MISSION'))

    m=["OCEAN",
        "SOIL",
        "CIT",
        "CLIMA",
        "CANCER",
        "UNCAN"]


    for k in m:
        pattern=str("^"+k)
        mask = (miss.topicCode.str.split('-').str[3].str.contains(pattern, na=True))
        miss.loc[mask, 'thema_code'] = k
        # miss.loc[mask, 'programme_code'] = v

    miss.loc[miss.thema_code=="UNCAN", 'thema_code'] = "CANCER"

    if any((miss.programme_code=='MISSION')&(miss.thema_code.isnull())):
        miss.loc[miss.thema_code.isnull(), 'thema_code'] = 'MISS-OTHER'  
        # miss.loc[miss.programme_code.isnull(), 'programme_code'] = miss.lvl3Code 
    # traitement niveau programme pour les MISSIONS
    miss = (miss
        .merge(horizon[['pilier_code', 'pilier_name_en', 'topicCode', 'topic_name']], how='left', on='topicCode')
        .assign(programme_name_en='Mission')
        .drop_duplicates())
    ########################################################################

    # JU-JTI
    spec={
    'CHIPS':'JU-JTI',
    'JU':'JU-JTI',
    'JTI':'JU-JTI',
    'EUSPA':'EUSPA',
    'SESAR':'JU-JTI'
    }

    # certains call n'ont pas JU/JTI dans le libellé
    top = topics_divisions.loc[(topics_divisions.lvl2Code=='HORIZON.2')&(topics_divisions.topicCode.str.upper().str.contains('|'.join([*spec]))), ['topicCode']]

    for k,v in spec.items():
        top.loc[top.topicCode.str.upper().str.contains(k), 'thema_code'] = v
    if any(pd.isna(top.thema_code.unique())):
        print('top_hor2 : thema_code à null après traitement')

    top = top.assign(destination_code=np.nan)
    for i in ['SESAR', 'CLEAN-AVIATION', 'IHI', 'KDT', 'CBE', 'EDCTP3', 'EUROHPC', 'SNS', 'ER', 'CHIPS']:  
        pattern=str(i.upper()+"-")
        mask = (top.thema_code=='JU-JTI')&(top.destination_code.isnull())&(top.topicCode.str.upper().str.contains(pattern))
        top.loc[mask, 'destination_code'] = i

    for i in ['CLEANH2']:  
        pattern=str("-"+i+"-")
        mask = (top.thema_code=='JU-JTI')&(top.destination_code.isnull())&(top.topicCode.str.contains(pattern))
        top.loc[mask, 'destination_code'] = i

    top.loc[top.destination_code=='KDT', 'destination_code'] = 'CHIPS'    
    top.loc[top.destination_code=='ER', 'destination_code'] = 'EU-RAIL'  
    top.loc[top.thema_code=='EUSPA', 'destination_code'] = 'EUSPA'


    if any(pd.isna(top.destination_code.unique())):
        print(f'top_hor2 : destination_code à null après traitement\n{top[top.destination_code.isnull()]}')
        top.loc[top.destination_code.isnull(), 'destination_code'] = 'DESTINATION-OTHER'

    # #############################################################################################################
    # horizon 3
    HOR3 = topics_divisions.loc[topics_divisions.lvl2Code=='HORIZON.3', ['topicCode', 'lvl3Code']]

    spec={'PATHFINDER':'PATHFINDER',
    'TRANSITION':'TRANSITION',
    'ACCELERATOR':'ACCELERATOR',  
    'CONNECT':'CONNECT',
    'SCALEUP':'SCALEUP',
    'INNOVSMES':'INNOVSMES',   
    'CLIMATE':'KIC-CLIMATE',
    'DIGITAL':'KIC-DIGITAL',
    'HEALTH':'KIC-HEALTH',
    'FOOD':'KIC-FOOD',
    'MANUFACTURING':'KIC-MANUFACTURING',
    'URBANMOBILITY':'KIC-URBANMOBILITY',
    'RAWMATERIALS':'KIC-RAWMATERIALS',
    'INNOENERGY':'KIC-INNOENERGY',
    'CCSI':'KIC-CCSI',
    'PRIZE':'PRIZE',
    'EITWOMENLEADERSHIP':'PRIZE'
    }

    for k,v in spec.items():
        HOR3.loc[HOR3.topicCode.str.upper().str.contains(k), 'thema_code'] = v   
    if any(pd.isna(HOR3.thema_code.unique())):
        print(f"HOR3 : thema_code à null après traitement\n{HOR3[HOR3.thema_code.isnull()].sort_values('topicCode').topicCode.unique()}")
        HOR3.loc[(HOR3.lvl3Code=='HORIZON.3.1')&(HOR3.thema_code.isnull()), 'thema_code'] = 'EIC-OTHER'
        HOR3.loc[(HOR3.lvl3Code=='HORIZON.3.2')&(HOR3.thema_code.isnull()), 'thema_code'] = 'EIE-OTHER'
        HOR3.loc[(HOR3.lvl3Code=='HORIZON.3.3')&(HOR3.thema_code.isnull()), 'thema_code'] = 'EIT-OTHER'
        

    spec={
    'CHALLENGE':'CHALLENGES',
    'OPEN':'OPEN',
    'EITWOMENLEADERSHIP':'EPWI',
    'RISINGINNOVATOR':'EPWI',
    'WOMENINNOVATOR':'EPWI',   
    'EPWI':'EPWI',
    'INNOVATIONPROCUREMENT':'EUIPA',
    'EIPA':'EUIPA',
    'EUIPA':'EUIPA',
    'EUSIC':'EUSIC',
    'SOCIALINNOVATION':'EUSIC',
    'HUMANITARIAN':'HUMANITARIAN',
    'ICAPITAL':'ICAPITAL'
    }
    
    for k,v in spec.items():
        HOR3.loc[(HOR3.thema_code=='PRIZE')&(HOR3.topicCode.str.upper().str.contains(k)), 'destination_code'] = v
        HOR3.loc[(HOR3.destination_code.isnull())&(HOR3.topicCode.str.upper().str.contains(k)), 'destination_code'] = v
    if any(pd.isna(HOR3.destination_code.unique())):
        HOR3.loc[(HOR3.lvl3Code=='HORIZON.3.1')&(HOR3.destination_code.isnull()), 'destination_code'] = 'DESTINATION-OTHER'
        HOR3.loc[(HOR3.destination_code.isnull()), 'destination_code'] = HOR3.thema_code
     #####################################################################################

    # horizon 4
    HOR4 = topics_divisions.loc[topics_divisions.lvl2Code=='HORIZON.4', ['topicCode', 'lvl3Code']]
    spec={
    'ACCESS':'ACCESS',
    'TALENTS':'TALENTS',
    'TECH':'INFRATECH',
    'COST':'COST',
    'GENDER':'GENDER',
    '-ERA-':'ERA',
    'PRIZE':'PRIZE',
    'EURATOM':'EURATOM'
    }

    for k,v in spec.items():
        HOR4.loc[HOR4.topicCode.str.upper().str.contains(k), 'thema_code'] = v
    if any(pd.isna(HOR4.thema_code.unique())):
        print(f"HOR4 : thema_code à null après traitement\n{HOR4[HOR4.thema_code.isnull()].sort_values('topicCode').topicCode.unique()}")
        HOR4.loc[(HOR4.lvl3Code=='HORIZON.4.1')&(HOR4.thema_code.isnull()), 'thema_code'] = 'WIDENING-OTHER'
        HOR4.loc[(HOR4.lvl3Code=='HORIZON.4.2')&(HOR4.thema_code.isnull()), 'thema_code'] = 'ERA-OTHER'
    
    spec={
    'GENDER':'GENDER',
    'IMPACT':'IMPACT'
    }
    for k,v in spec.items():
        HOR4.loc[(HOR4.thema_code=='PRIZE')&(HOR4.topicCode.str.upper().str.contains(k)), 'destination_code'] = v
    
    HOR4.loc[(HOR4.destination_code.isnull()), 'destination_code'] = HOR4.thema_code 
    ##############################################################################

    #traitement des programmes hors mission
    tab = pd.concat([HOR3, HOR4, CLUSTER, top, INFRA, ERC, MSCA], ignore_index=True)
    tab = tab.merge(horizon, how='inner', on='topicCode')
    tab = tab.mask(tab == '')
    
    #add mission
    tab = pd.concat([tab, miss], ignore_index=True).drop(columns=['lvl3Code'])
    tab = tab.mask(tab == '')

    # traitement thema_code -> null
    reste = horizon.loc[~horizon.topicCode.isin(tab.topicCode.unique())]
    tab = pd.concat([tab, reste], ignore_index=True)

    tab.loc[tab.destination_code.isnull(), 'destination_code'] = tab.thema_code
    tab.loc[tab.thema_code.isnull(), 'thema_code'] = 'THEMA-OTHER'
    tab.loc[tab.destination_code.isnull(), 'destination_code'] = 'DESTINATION-OTHER'

    thema_lib = pd.read_json(open('data_files/thema.json', 'r+', encoding='utf-8'))
    thema_lib = pd.DataFrame(thema_lib)
    tab = tab.merge(thema_lib, how='left', on='thema_code').drop(columns='dest_h20')
    # tab.loc[tab.thema_name_en.isnull(), 'thema_name_en'] = tab.programme_name_en
    tab = tab.merge(destination, how='left', on='destination_code')
    tab = tab.mask(tab == '')

    tab.loc[(tab.destination_name_en.isnull())&(tab.thema_code==tab.destination_code), 'destination_name_en'] = tab.thema_name_en

    data = pd.read_json(open('data_files/programme_fr.json', 'r+', encoding='utf-8'))
    data=pd.DataFrame(data)

    tab = tab.merge(data[['programme_code','programme_name_fr']], how='left',on='programme_code')
    tab = tab.merge(data[['pilier_code','pilier_name_fr']], how='left', on='pilier_code')

    tab.loc[(tab.thema_name_fr.isnull()), 'thema_name_fr'] = tab['programme_name_fr']

    if not tab.columns[tab.isnull().any()].empty:
        print(f"- ⚠️ des cellules sont vides dans tab: {tab.columns[tab.isnull().any()]}")


    for i in tab.columns:
        if tab[i].dtype == 'object':
            tab[i] = tab[i].map(str.strip, na_action='ignore')
        else:
            pass
    return tab

def merged_topics(source, df):

    """
    1. load topics and divisions from source json
    2. apply function topics_divisions to create nomenclature thema/destination
    3. filter on topicCode in df   
    4. merge with MERGED (df) on topicCode
    5. save data_clean/topics_current.csv"
    """


    topics, top_div = top_div_load(source)
    topics = topics_divisions(topics, top_div)

    top_code = list(set(df.topicCode))
    top_code = [item for item in top_code if not(pd.isnull(item)) == True]
    topics = (topics[topics['topicCode'].isin(top_code)]
            .drop_duplicates())

    # création du champs DESTINATION = lib+code
    topics.loc[(topics.destination_code.str.contains('^HORIZON', regex=True)) | (topics.destination_code.isnull()), 'destination'] = topics.thema_code
    topics.loc[(topics.destination.isnull())&(~topics.destination_code.isnull())&(topics.destination_lib.isnull()), 'destination'] = topics.destination_code
    topics.loc[(topics.destination.isnull())&(~topics.destination_code.isnull())&
                (~topics.destination_code.str.replace('-',' ').isin(topics.destination_lib.str.upper())), 'destination'] = topics['destination_lib']+" - "+topics['destination_code']

    topics.loc[(topics.destination.isnull())&(~topics.destination_lib.isnull())&
                (topics.destination_code.str.replace('-',' ').isin(topics.destination_lib.str.upper())), 'destination'] = topics.destination_lib

    y=topics.loc[topics.destination.isnull(), ['destination_lib','destination_code', 'destination']].drop_duplicates()
    if len(y)>1:
        print(y)

    df = (df
            .merge(pd.DataFrame(topics), how='left', on='topicCode')
            .drop_duplicates()
            .drop(columns='destination_code')
            .rename(columns={'destination':'destination_code', 'topicCode':'topic_code'}))
    print(f"size merged after add topics: {len(df)}")


    if len(df[df['programme_code'].isnull()])>0:
        print(f"- ⚠️ ! programme_code manquant")
    topics.to_csv(f"{PATH_CLEAN}topics_current.csv", index=False, encoding="UTF-8", sep=";", na_rep='')

    return df