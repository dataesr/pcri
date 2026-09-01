from paths import PATH_CONNECT, PATH_CLEAN
from constant_vars import FRAMEWORK
import requests, pandas as pd, numpy as np, datetime as dt

def calls_current(projects_current, calls):
    print("### CALLS current")

    call_id = (projects_current
        .groupby(['programme_name_fr','thema_code', 'thema_name_fr', 'thema_name_en', 
                'destination_code','destination_name_en', 'call_deadline', 'is_ejo',
                'call_id','call_year','role','action_code', 'extra_joint_organization',
                'country_code','country_name_fr','stage', 'status_code'],  dropna = False)
        .agg({'calculated_fund':'sum', 'beneficiary_fund':'sum', 'number_involved':'sum', 
              'project_id': 'nunique'})
        .reset_index()
        .merge(calls.drop(columns=['call_year','missionCancer', 'missionCities', 
                                   'missionClimate', 'missionOcean', 'missionSoil'])
        .drop_duplicates(), how='left', on=['call_id', 'call_deadline'])
            )

    call_id.to_csv(f"{PATH_CONNECT}calls_current.csv", index=False, encoding="UTF-8", sep=";", na_rep='')


def calls_all(projects):
    print("### CALLS ALL")
    """
    compare call for proposals on UE portal and call in ecorda database
    check closed call in base or not.
    group data under call level
    """
    #########################
    # topics + call in ecorda
    tops = (projects
        .groupby(['call_id', 'call_year', 'topic_code', 'action_code', 'call_deadline', 'stage'])
        .agg(nb_proj=('project_id', 'nunique'), cost=('total_cost', 'sum'))
        .reset_index()
        .pivot_table(
            index=['call_id', 'call_year', 'topic_code', 'action_code', 'call_deadline'],
            columns='stage',
            values=['nb_proj', 'cost'],
            fill_value=np.nan
        )
    )

    # Aplatir le MultiIndex de colonnes -> ex: project_id_stage_evaluated, total_cost_stage_successful
    tops.columns = ['_'.join(map(str, col)).strip() for col in tops.columns.to_flat_index()]

    tops = tops.reset_index().assign(inBase=True, closed=True)

    tops['call_id'] = tops['call_id'].str.upper()
    tops['end_date'] = tops["call_deadline"].dt.strftime('%Y-%m')
    print(f"- size tops before drop calldeadline: {len(tops)}")

    tops = tops.drop(columns=['call_deadline', 'cost_evaluated', 'nb_proj_evaluated']).drop_duplicates()
    print(f"- size tops after drop calldeadline: {len(tops)}")

    cols_group = [c for c in tops.columns if c not in ['topic_code', 'cost_successful', 'nb_proj_successful', 'end_date']]
    tops_rg = tops.groupby(cols_group,
        as_index=False, dropna=False
    ).agg(
        nb_proj_successful=('nb_proj_successful', 'sum'),
        cost_successful=('cost_successful', 'sum'),
        nb_topics_included=('topic_code', 'nunique'),
        end_date=('end_date', 'max')
    )

    nb_dup = tops_rg.duplicated(subset=['call_id', 'action_code'], keep=False)
    if nb_dup.sum()>0:
        print(f"🚨 {nb_dup.sum()} lignes impliquées dans un doublon\n{tops_rg[nb_dup].call_id.unique()}")

    print(f"- size tops_rg topic grouped under call: {len(tops_rg)}") 

    #############
    # portal info
    call_info_date = pd.read_pickle(f"{PATH_CLEAN}topic_call_info.pkl")
    
    call_info_date = call_info_date.rename(
        columns={
            "topicCode": "topic_code",
            "callIdentifier": "call_id"
        }
    ).drop(columns=["minContribution", "maxContribution", "call_open_date", "destination_code", "type"])

    # fillna action_code with tops action_code
    mapping = tops.set_index('topic_code')['action_code'].to_dict()
    # Remplir uniquement les action_code null
    mask = call_info_date['action_code'].isnull()
    call_info_date.loc[mask, 'action_code'] = call_info_date.loc[mask, 'topic_code'].map(mapping)

    # replace call_year by call_year_wp (WP year and not call_id year)
    call_info_date.loc[call_info_date['call_year_wp'].notna(), 'call_year'] = call_info_date.loc[call_info_date['call_year_wp'].notna(), 'call_year_wp']
    call_info_date['call_id'] = call_info_date['call_id'].str.upper()

    call_info_date['call_deadline_ma'] = pd.to_datetime(call_info_date['call_deadline_ma'], format='%Y-%m')
    print(f"- size call_info_date: {len(call_info_date)}") 


    cols_to_fill = ['deadlineModel', 'deadlineDates']
    call_info_date[cols_to_fill] = (
        call_info_date.groupby('call_id')[cols_to_fill]
        .transform(lambda s: s.ffill().bfill())
    )

    # count topic_code by call_id
    ct = (call_info_date[call_info_date['call_id']!=call_info_date['topic_code']]
            .groupby(['call_id', 'action_code'], as_index=False, dropna=False)
            .agg(
                nb_top_tmp=('topic_code', 'nunique')
            ))

    #group data by call_id
    cols_group = [c for c in call_info_date.columns if c not in ['topic_code', 'expectedGrants', 'budget_year_map_budget', 'call_deadline_ma', 'call_year_wp']]
    call_rg = call_info_date.groupby(cols_group,
        as_index=False, dropna=False
    ).agg(
        expectedGrants=('expectedGrants', 'sum'),
        budget_year_map_budget=('budget_year_map_budget', 'sum'),
        nb_open_topics=('topic_code', 'nunique'),
        call_deadline_ma=('call_deadline_ma', 'max')
    )

    call_rg = pd.merge(call_rg, ct, how='left', on=['call_id', 'action_code'])
    call_rg.loc[(call_rg['nb_top_tmp'].notnull())&(call_rg['nb_open_topics']!=call_rg['nb_top_tmp']), 'nb_open_topics'] = call_rg.loc[(call_rg['nb_top_tmp'].notnull())&(call_rg['nb_open_topics']!=call_rg['nb_top_tmp']), 'nb_top_tmp'] 

    call_rg['call_deadline_ma'] = call_rg['call_deadline_ma'].dt.strftime('%Y-%m')

    nb_dup = call_rg.duplicated(subset=['call_id', 'action_code'], keep=False)
    if nb_dup.sum()>0:
        print(f"🚨 {nb_dup.sum()} lignes impliquées dans un doublon\n{call_rg[nb_dup].call_id.unique()}")

    print(f"- size topic grouped under call: {len(call_rg)}") 

    ################
    # merge des call
    call_level = pd.merge(call_rg, tops_rg, 
                how='outer', 
                on=['call_id', 'call_year', 'action_code'],
                indicator=True)
    
    print(f"- call_level: {call_level._merge.value_counts()}")

    nb_dup = call_level.duplicated(subset=['call_id', 'action_code'], keep=False)
    if nb_dup.sum()>0:
        print(f"🚨 {nb_dup.sum()} lignes impliquées dans un doublon\n{call_level[nb_dup].call_id.unique()}")

    # fill na value
    mask = (call_level['_merge']=='right_only')
    call_level.loc[mask&(call_level['call_deadline_ma'].isnull()), 'call_deadline_ma'] = call_level.loc[mask&(call_level['call_deadline_ma'].isnull()), 'end_date'] 

    call_level.loc[(call_level['_merge']=='left_only')&(call_level['inBase'].isnull()), 'inBase'] = False

    today = pd.Timestamp.now(tz="UTC").normalize()
    call_level.loc[(call_level.closed.isnull()), 'closed'] = call_level["call_deadline"] < today
    call_level.loc[(call_level.closed.isnull()), 'closed'] = False
    call_level = call_level.loc[call_level.closed==True]

    mapping = {'both': 'both', 'right_only': 'ecorda', 'left_only': 'portal'}
    call_level['origin'] = call_level['_merge'].map(mapping)

    print(f"- size call_level: {len(call_level)}")

    #extraction liste complète des call-id
    def get_calls(framework:list):
        print('### get calls in europa info')
        url='https://ec.europa.eu/info/funding-tenders/opportunities/data/referenceData/topicdictionary.json?lang=en'
        r = requests.get(url)
        response=r.json()['callsTenders']

        df=[]
        for i in response:
            if 'context' in i and any(x in i['context'] for x in framework):
                df.append(i)
        return pd.DataFrame(df)

    calls_liste = get_calls(['HORIZON'])

    calls_liste = pd.DataFrame(calls_liste).rename(columns={'label':'call_temp'}).drop(columns=['value', 'type', 'context'])
    calls_liste['year'] = calls_liste['call_temp'].str.extract('(?<=-)(\\d{4})')
    calls_liste['call_temp'] = calls_liste['call_temp'].str.upper()

    ### callTitle null
    # Créer un dictionnaire de correspondance call_temp -> description
    mapping = calls_liste.set_index('call_temp')['description'].to_dict()
    # Remplir uniquement les callTitle null
    mask = call_level['callTitle'].isnull()
    call_level.loc[mask, 'callTitle'] = call_level.loc[mask, 'call_id'].map(mapping)


    missing = calls_liste[~calls_liste['call_temp'].isin(call_level['call_id'].unique())]
    if not missing.empty:
        print(f"⚠️ missing call in call_level")
        calls_liste = (calls_liste
                       .rename(columns={'call_temp':'call_id', 'description':'callTitle', 'year':'call_year'})
                       .assign(inBase=False, closed='unknown', origin='reference data')
        )
        call_level = pd.concat([call_level, calls_liste], ignore_index=True)

    call_level['gap_topic'] = call_level['nb_topics_included'].sub(
    call_level['nb_open_topics'], fill_value=0
    )
    call_level['gap_fund'] = call_level['cost_successful'].sub(
    call_level['budget_year_map_budget'], fill_value=0
    )
    call_level['gap_proj'] = call_level['nb_proj_successful'].sub(
    call_level['expectedGrants'], fill_value=0
    )

    call_level.loc[(call_level.inBase==True)&(call_level.origin=='ecorda'), 'status'] = 'complete'
    call_level.loc[(call_level.inBase==True)&(call_level.gap_topic>=0), 'status'] = 'complete'
    call_level.loc[(call_level.inBase==True)&(call_level.gap_topic<0), 'status'] = 'incomplete'
    call_level.loc[(call_level.inBase==False), 'status'] = 'unavailable'

    l=['type_call', 'call_id', 'callTitle', 'call_year', 'call_deadline', 'call_deadline_ma', 'deadlineModel',
       'deadlineDates', 'action_code', 'inBase', 'closed', 'status',
       'nb_open_topics', 'nb_topics_included', 'gap_topic',
       'expectedGrants', 'nb_proj_successful', 'gap_proj',
       'budget_year_map_budget', 'cost_successful', 'gap_fund']
    
    cols = [c for c in l if c in call_level.columns]
    call_level = call_level[cols]

    for i in calls_liste.call_temp.unique():
        calls_all.loc[(calls_all.call_id.isnull())&(calls_all.topic_code.str.contains(i)), 'call_id'] = i
    
    if any(calls_all[calls_all.call_id.isnull()]):
        print(f"- liste of topics which call_id is null: {calls_all[calls_all.call_id.isnull()].topic_code.unique()}")
    calls_all.loc[calls_all.call_id.isnull(), 'call_id'] = calls_all.topic_code
    calls_all.loc[calls_all.call_year.isnull(), 'call_year'] = calls_all.year
        
    if not calls_all.loc[calls_all.call_year.isnull()].empty:
        print(f"- call year si encore des valeurs manquantes; extraire année de l'open date\n{calls_all.loc[calls_all.call_year.isnull()]}")
        calls_all.loc[calls_all.call_year.isnull(), 'call_year'] = calls_all.loc[calls_all.call_year.isnull()].call_open_date.dt.strftime('%Y')

    calls_all['call_acro'] = calls_all.call_id.str.replace('^HORIZON-|^HOIRZON-', '', regex=True)
    calls_all['theme'] = calls_all.call_acro.str.split('-').str[0]
    for i in ['JU', 'JTI', 'SESAR', 'CHIPS']:
        calls_all.loc[calls_all.call_acro.str.upper().str.contains(i, na=False), 'theme'] = 'JU-JTI'

    calls_all.loc[calls_all.theme=='MISS', 'theme'] = 'Mission'
    calls_all['theme'] = calls_all.theme.str.replace('CL', 'Cluster')

    for i in ['MSCA', 'ERC']:
        if i=='MSCA':
            pat=r"COFUND|DN|PF|SE|CITIZENS"
        elif i=='ERC':
            pat=r"ADG|STG|COG|POC|SyG"
        calls_all.loc[(calls_all.theme==i)&(calls_all.topic_code.str.contains(pat, na=False, regex=True)), 'action_code'] = i
    
    calls_all = calls_all.drop(columns=['year']).drop_duplicates()

    calls_all['nb']=calls_all.groupby('call_id')['inBase'].transform('nunique')

    calls_all.loc[(calls_all.inBase==True)&(calls_all.nb==1), 'status'] = 'complete'
    calls_all.loc[(calls_all.nb>1), 'status'] = 'incomplete'
    calls_all.loc[(calls_all.inBase==False)&(calls_all.nb==1), 'status'] = 'unavailable'

    calls_all = (
        calls_all
        .groupby(['call_id', 'call_year', 'topic_code', 'theme', 'action_code',
                'expectedGrants', 'inBase', 'closed', 'status'], dropna=False)
        .agg(
            end_dates=('end_date', lambda x: ';'.join(x.unique().astype(str))),
            nb_successful=('stage_successful', lambda x: x.sum(min_count=1)),
            nb_evaluated=('stage_evaluated', lambda x: x.sum(min_count=1))
        )
        .reset_index()
    )

    calls_all['gap']=calls_all['expectedGrants']-calls_all['nb_successful']

    calls_all.sort_values('call_deadline').to_csv(f"{PATH_CONNECT}calls_liste.csv", index=False, encoding="UTF-8", sep=";", na_rep='')
    return calls_all
