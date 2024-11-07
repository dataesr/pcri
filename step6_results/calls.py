from config_path import PATH_CONNECT, PATH_WORK, PATH_SOURCE, PATH_REF
from constant_vars import FRAMEWORK
import requests, pandas as pd

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
    tops=(projects[['thema_code', 'thema_name_en', 'call_id','call_year', 'topic_code', 'call_deadline']]
        .drop_duplicates()
        .assign(inBase=True, closed=True))

    call_info_date = pd.read_pickle(f"{PATH_SOURCE}{FRAMEWORK}/call_info_harvest.pkl")
    call_info_date = pd.DataFrame(call_info_date)
    call_info_date['call_deadline'] = pd.to_datetime(call_info_date['deadline'], format='%d %B %Y')
    call_info_date['call_open_date'] = pd.to_datetime(call_info_date['open_date'], format='%d %B %Y')
    call_info_date['call_year'] = call_info_date['topic_code'].str.extract('(?<=-)(\\d{4})(?=-)')

    call_info_date = (call_info_date
                    .loc[call_info_date.type=='Call for proposal']
                    .drop(columns=['open_date','deadline', 'type'])
                    .drop_duplicates())
    print(len(call_info_date))

    calls_all = call_info_date.merge(tops, how='outer', on=['topic_code', 'call_year'])
    calls_all['call_deadline'] = np.where(calls_all.call_deadline_y.isnull(), calls_all.call_deadline_x, calls_all.call_deadline_y)
    calls_all = calls_all.drop(columns=['call_deadline_x', 'call_deadline_y']).drop_duplicates()
    calls_all['topic_code'] = calls_all.topic_code.str.replace('SYG', 'SyG')
    calls_all['topic_code'] = calls_all.topic_code.str.replace('CIRCBIO', 'CircBio')
    calls_all.loc[calls_all.inBase.isnull(), 'inBase'] = False
    calls_all.loc[(calls_all.closed.isnull())&(calls_all.call_deadline<pd.to_datetime(dt.date.today(), format='%d %B %Y')), 'closed'] = True
    calls_all.loc[(calls_all.closed.isnull()), 'closed'] = False


    #extraction liste complète des call-id
    def get_calls(framework:list):
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
        calls_all.loc[(calls_all.call_id.isnull())&(calls_all.closed==True)&(calls_all.topic_code.str.contains(i)), 'call_id'] = i

    add = pd.read_excel(f"{PATH_REF}_call_topic_add.xlsx", dtype=str)
    calls_all = calls_all.merge(add, how='left', on='topic_code')
    calls_all.loc[(calls_all.call_id.isnull())&(~calls_all.call_temp.isnull()), 'call_year'] = calls_all.year
    calls_all.loc[(calls_all.call_id.isnull())&(~calls_all.call_temp.isnull()), 'call_id'] = calls_all.call_temp
    
        
    if not calls_all.loc[(calls_all.call_id.isnull())&(calls_all.closed==True)].empty:
        calls_liste[['call_temp', 'year']].drop_duplicates().to_csv(f"{PATH_WORK}call_liste.csv", sep=';')
        calls_all.loc[(calls_all.call_id.isnull())&(calls_all.closed==True), ['topic_code']].drop_duplicates().sort_values('topic_code').to_csv(f"{PATH_WORK}topic_liste.csv", sep=';')
        print('topics clôturés sans call_id ; aller vérifier dans datas_work')
        
    if not calls_all.loc[calls_all.call_year.isnull()].empty:
        print(calls_all.loc[calls_all.call_year.isnull()])
    # call_info_date.loc[call_info_date.call_year.isnull(), 'call_year'] = call_info_date[call_info_date.call_year.isnull()].call_deadline.dt.year
    for i in [ 'call_id']:
        calls_all['call_acro'] = calls_all[i].str.replace('^HORIZON-', '', regex=True)
    calls_all.drop(columns=['year', 'call_temp'], inplace=True)

    calls_all.sort_values('call_deadline').to_csv(PATH_CONNECT+"calls_liste.csv", index=False, encoding="UTF-8", sep=";", na_rep='')
    return calls_all