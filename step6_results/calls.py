from config_path import PATH_CONNECT, PATH_WORK, PATH_SOURCE, PATH_REF
from constant_vars import FRAMEWORK
import requests, pandas as pd, numpy as np, datetime as dt

def calls_current(projects_current, calls):
    print("### CALLS current")

    call_id = (projects_current
        .groupby(['programme_name_fr','thema_code', 'thema_name_fr', 'thema_name_en', 
                'destination_code','destination_name_en', 'call_deadline', 'is_ejo',
                'call_id','call_year','role','action_code', 'extra_joint_organization',
                'country_code','country_name_fr','stage', 'status_code'],  dropna = False)
        .agg({'calculated_fund':'sum', 'beneficiary_subv':'sum', 'number_involved':'sum', 
              'project_id': 'nunique'})
        .reset_index()
        .merge(calls.drop(columns=['call_year','missionCancer', 'missionCities', 
                                   'missionClimate', 'missionOcean', 'missionSoil'])
        .drop_duplicates(), how='left', on=['call_id', 'call_deadline'])
            )

    call_id.to_csv(f"{PATH_CONNECT}calls_current.csv", index=False, encoding="UTF-8", sep=";", na_rep='')


def calls_all(projects):
    print("### CALLS ALL")
    # topics + call in ecorda
    tops=(projects[['call_id','call_year', 'topic_code', 'call_deadline']]
        .drop_duplicates()
        .assign(inBase=True, closed=True))

    call_info_date = pd.read_pickle(f"{PATH_SOURCE}{FRAMEWORK}/call_info_harvest.pkl")
    call_info_date = pd.DataFrame(call_info_date)
    call_info_date['call_deadline'] = pd.to_datetime(call_info_date['deadline'], format='%d %B %Y')
    call_info_date['call_open_date'] = pd.to_datetime(call_info_date['open_date'], format='%d %B %Y')
    call_info_date['year'] = call_info_date['topic_code'].str.extract('(?<=-)(\\d{4})(?=-)')
  
    call_info_date = (call_info_date
                    .loc[call_info_date.type=='Call for proposal']
                    .drop(columns=['open_date','deadline', 'type'])
                    .drop_duplicates())
    print(len(call_info_date))

    calls_all = call_info_date.merge(tops, how='outer', on=['topic_code', 'call_deadline'])
    calls_all['topic_code'] = calls_all.topic_code.str.replace('SYG', 'SyG')
    calls_all['topic_code'] = calls_all.topic_code.str.replace('CIRCBIO', 'CircBio')
    calls_all.loc[calls_all.inBase.isnull(), 'inBase'] = False
    calls_all.loc[(calls_all.closed.isnull())&(calls_all.call_deadline<pd.to_datetime(dt.date.today(), format='%d %B %Y')), 'closed'] = True
    calls_all.loc[(calls_all.closed.isnull()), 'closed'] = False
    calls_all = calls_all.loc[calls_all.closed==True]

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
    calls_liste['call_temp'] = calls_liste.call_temp.str.replace('SYG', 'SyG')
    calls_liste['call_temp'] = calls_liste.call_temp.str.replace('CIRCBIO', 'CircBio')

    for i in calls_liste.call_temp.unique():
        calls_all.loc[(calls_all.call_id.isnull())&(calls_all.topic_code.str.contains(i)), 'call_id'] = i
    
    if any(calls_all[calls_all.call_id.isnull()]):
        print(f"{calls_all[calls_all.call_id.isnull()].topic_code.unique()}")
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
    
    calls_all.drop(columns=['year'], inplace=True)

    calls_all['nb']=calls_all.groupby('call_id')['inBase'].transform('nunique')

    calls_all.loc[(calls_all.inBase==True)&(calls_all.nb==1), 'status'] = 'complete'
    calls_all.loc[(calls_all.nb>1), 'status'] = 'incomplete'
    calls_all.loc[(calls_all.inBase==False)&(calls_all.nb==1), 'status'] = 'unavailable'


    calls_all.sort_values('call_deadline').to_csv(f"{PATH_CONNECT}calls_liste.csv", index=False, encoding="UTF-8", sep=";", na_rep='')
    return calls_all
