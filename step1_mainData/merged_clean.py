from config_path import PATH_CLEAN, PATH_SOURCE
from constant_vars import FRAMEWORK
from functions_shared import work_csv
import pandas as pd, numpy as np

def dates_year(df):
    print("\n### DATES and YEAR")

    # year extract from call_id
    temp = df[['callId','topicCode']].drop_duplicates()
    temp['call_year'] = temp['callId'].str.extract('(\\d{4})') 

    # path since 2025
    wp = pd.read_pickle(f"{PATH_SOURCE}{FRAMEWORK}/calls_by_wp.pkl").explode('calls')
    temp = temp.merge(wp, how='left', left_on='topicCode', right_on='calls').drop_duplicates()
    temp.loc[~temp.year.isnull(), 'call_year'] = temp.loc[~temp.year.isnull()].year


    # calcul le nombre d'année unique par call et s'il en manque 
    def year_calc(df):
        y = (df[['callId', 'call_year']].drop_duplicates()
                .groupby('callId', dropna=False, as_index=False)
                .agg(
                    nb_tot=('call_year','size'), 
                    nb_null=('call_year', lambda x: 1 if x.isnull().any() else 0)
        ))
        return df[['callId', 'call_year']].drop_duplicates().sort_values('callId').merge(y, how='left', on='callId')


    check_year = year_calc(temp)
    check_year = check_year.loc[~((check_year.nb_tot>1)&(check_year.call_year.isnull()))]
    check_year = year_calc(check_year)

    if any(check_year.nb_tot>1):
        return print(f"1 - ++ YEARS for a same callId  because topic diff: {check_year[check_year.nb_tot>1].callId.unique()}")
     
    df = df.merge(check_year[['callId', 'call_year']].drop_duplicates(), how='left', on='callId')

    # call_year missed -> use topic haverst from portal 
    if any(df.call_year.isnull()):
        dt=pd.read_pickle(f"{PATH_SOURCE}{FRAMEWORK}/topic_info_harvest.pkl")
        dt=pd.DataFrame(dt)
        dt['year'] = dt.open_date.str.split().str[-1]
        df = df.merge(dt[['topic_code', 'year']].drop_duplicates(), how='left', left_on='topicCode', right_on='topic_code')
        df.loc[df.call_year.isnull(), 'call_year'] = df.loc[df.call_year.isnull()].year
        df.drop(columns=['topic_code', 'year'], inplace=True)

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
    
    print(f"- size after year cleaned: {len(df)}")
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

# def key_words(df):
