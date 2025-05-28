from config_path import PATH_CLEAN, PATH_SOURCE
from constant_vars import FRAMEWORK
from functions_shared import work_csv
import pandas as pd, numpy as np

def dates_year(df):
    print("\n### DATES and YEAR")

    dt=pd.read_pickle(f"{PATH_SOURCE}{FRAMEWORK}/topic_info_harvest.pkl")
    dt=pd.DataFrame(dt)
    dt['call_year'] = dt.open_date.str.split().str[-1]

    dt = (df[['callId','topicCode']].drop_duplicates()
                  .merge(dt[['topic_code', 'call_year']].drop_duplicates(),
                  how='left', left_on='topicCode', right_on='topic_code'))

    y=(dt[['callId', 'call_year']].drop_duplicates()
            .groupby('callId', dropna=False, as_index=False)
            .agg(
                nb_tot=('call_year','size'), 
                nb_null=('call_year', lambda x: 1 if x.isnull().any() else 0)
    ))

    check_year=dt[['callId', 'call_year']].drop_duplicates()

    if any(y.nb_tot>1):
        print(f"1 - ++ YEARS for a same callId  because topic diff: {check_year[check_year.callId.isin(y[y.nb_tot>1].callId.unique())].callId.unique()}")
        if any((y.nb_tot>1)&(y.nb_null>0)&(y.nb_tot!=y.nb_null)):
            check_year=check_year.loc[~((check_year.callId.isin(y[(y.nb_tot>1)&(y.nb_null>0)&(y.nb_tot!=y.nb_null)].callId.unique()))&(check_year.call_year.isnull()))]
        if any((y.nb_tot>1)&(y.nb_null==0)):
            print(f"1b -Attention ! ++ years for a same call: {y[(y.nb_tot>1)&(y.nb_null==0)].callId.unique()}")
    if any(y.nb_tot==y.nb_null):
        print(f"2 - without YEAR for callID: {y[y.nb_tot==y.nb_null].callId.nunique()}")
        work_csv(y[y.nb_tot==y.nb_null][['callId']].drop_duplicates(), 'callId_year_empty')

    # création var commune de statut/ call
    check_year['call_year_tmp'] = check_year['callId'].str.extract('(\\d{4})')
    check_year.loc[(check_year.call_year_tmp<'2025')|(check_year.call_year.isnull()), 'call_year'] = check_year.loc[(check_year.call_year_tmp<'2025')|(check_year.call_year.isnull())].call_year_tmp
    check_year.loc[(check_year.call_year.isnull()), 'call_year'] = check_year.loc[(check_year.call_year.isnull())].call_year_tmp
    df = df.merge(check_year[['callId', 'call_year']], how='left', on='callId')
    print(f"######ATTENTION###### {df[df.call_year.isnull()].callId.unique()} call without year -> penser à aller chercher l'année dans les WP (à mettre en place)")

    # traitement YEAR
    if any(df['call_year'].isnull()):
        print(f"1- year none for \n{df[df['call_year'].isnull()].callId.value_counts()}")
    else:
        print(f"2- calldeadline OK -> year:\n{df[['stage','call_year']].value_counts(dropna=False)}\n")

    # test call continu -> call open until 2027
    if any(df['call_year'][df['call_year']>'2026']):
        print(f"3- CALL continu ; utiliser la date de calldeadline:\n{df['callId'][df['call_year']>'2021'].unique()}\n")

    for d in ['callDeadlineDate',  'startDate', 'endDate', 'ecSignatureDate', 'submissionDate']:
        df[d] = df[d].astype('datetime64[ns]')
    return df    


def strings_v(df):
    for i in ['title','abstract', 'freekw', 'eic_panels', 'url']:
        df[i]=df[i].str.replace('\\n|\\t|\\r|\\s+', ' ', regex=True).str.strip()   
    return df

def empty_str_to_none(df):
    for x in df.columns:
        if pd.api.types.infer_dtype(df[x])=='string':
            df[x]=np.where(df[x].isnull(), None, df[x])
    return df


def projects_complete_cleaned(df, extractDate):
    print(f"\n### CREATING FINAL PROJECTS\nsize:{len(df)}")
    df = df.assign(framework='Horizon Europe', ecorda_date=pd.to_datetime(extractDate))
    df = df.reindex(sorted(df.columns), axis=1)

    file_name = f"{PATH_CLEAN}projects_current.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(df, file)
    return df