
from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE, PATH_CONNECT
from functions_shared import unzip_zip
import requests, pandas as pd, numpy as np

def call(chemin):
    print("\n### CALLS")
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

    data = unzip_zip(ZIPNAME, chemin, "calls.json", 'utf8')
    data=pd.DataFrame(data)

    calls = data.drop(columns={'masterCallId', 'lastUpdateDate'}).drop_duplicates() 

    calls['call_year'] = calls['callId'].str.extract('(?<=-)(\d{4})')

    calls['callId_new'] = calls['callId'].str.replace('(-[0-9]{2,4})*','',regex=True)
    calls['callId_new'] = calls['callId_new'].str.replace('^HORIZON-','',regex=True)
        

    calls['workProg_new'] = np.where(pd.isna(calls['workProgrammeCode']), calls['callId'], calls['workProgrammeCode'])
    calls['workProg_new'] = calls['workProg_new'].str.replace('(-[0-9]{4})*','',regex=True)
    calls['workProg_new'] = np.where(calls['workProg_new']=='HORIZON', calls['callId_new'].str.split('-').str[0] ,calls['workProg_new'])
    
    for w in ['^HORIZON(-?)', '(-?)HORIZONTAL(-?)']: 
        calls.loc[calls['workProg_new'].str.contains('HORIZON'), 'workProg_new'] = calls['workProg_new'].str.replace(w,'', regex=True)
    
    calls.loc[calls['callId_new'].str.contains('MSCA'),'workProg_new'] = 'MSCA'
    calls.rename(columns={'callId':'call_id', 'callDeadlineDate':'call_deadline', 'callFunding':'call_budget'}, inplace=True)
    calls['call_deadline'] = calls['call_deadline'].astype('datetime64[ns]')
    return calls


def calls_to_check(calls, call_id):
    print("\n### CALLS to CHECK")
    
    calls = (calls
            .merge(call_id, how='left', left_on=['call_id', 'call_deadline'], right_on=['call_id', 'call_deadline'])
            .drop_duplicates())
    print(f"- CALLS de base calls with merge call_id ->\nnb call+deadline: {len(calls)}, nb call unique: {calls.call_id.nunique()} ")

    if len(calls)!=len(call_id):
        x=call_id[['call_id', 'call_deadline']].drop_duplicates()
        y=calls[['call_id', 'call_deadline']].drop_duplicates()

        if len(x)>len(y):
            calls_no_info = x.merge(y, how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
            print(f"1- si call_id pas dans calls :\n{calls_no_info}")
        elif len(y)>len(x):
            calls_no_info = y.merge(x, how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
            print(f"2- si calls pas dans call_id :\n{calls_no_info}")
        calls_no_info.to_csv(f"{PATH_CONNECT}calls_no_info.csv", index=False, encoding="UTF-8", sep=";", na_rep='')
        
        print(f"3- calls with multi deadline\n{calls.groupby('call_id').filter(lambda x: x['workProgrammeCode'].count() > 1.)[['call_id', 'call_deadline', 'call_budget']]}")

    calls['call_deadline'] = calls['call_deadline'].astype('datetime64[ns]')

    print(f"- nbre call unique:{calls['call_id'].nunique()}, nbre proposals:{calls['expectedNbrProposals'].sum()}, fonds:{'{:,.1f}'.format(calls['call_budget'].sum())}")
    # calls.to_csv(PATH_CONNECT+"calls.csv", index=False, encoding="UTF-8", sep=";", na_rep='')
    return calls