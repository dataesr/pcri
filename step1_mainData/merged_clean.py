from paths import PATH_CLEAN, PATH_SOURCE, PATH_WP
from constant_vars import FRAMEWORK
from functions_shared import work_csv
import pandas as pd, numpy as np, json

def dates_year(df, top_call):
    """
    1. link merged (df) with top_call
    2. clean year in merged with top_call 
    3. check if each callId has only one year and if any year is missing
     - if not, raise error with callId with multiple year or missing year  

     return merged with clean year and date in datetime format  
    """

    print("\n### DATES and YEAR")

    ##### year extract from call_id #####

    temp = df[['callId','topicCode']].drop_duplicates()
    temp['year'] = temp['callId'].str.extract('(\\d{4})')

    temp = pd.merge(temp, top_call[['topicCode', 'call_year', 'call_year_wp']].drop_duplicates(), how='left', on='topicCode', indicator=True)
    
    if any(temp['call_year'].isnull()):
        print(f"- topicCode not in tenders portal: {list(temp.loc[temp['call_year'].isnull(), 'callId'].unique())}")
    
    # fix year from topic code (for year wrong in call_id or call_id without year)
    temp.loc[temp['call_year'].isnull(), 'call_year'] = temp.loc[temp['call_year'].isnull(), 'year']
    temp.loc[temp['call_year_wp'].notnull(), 'call_year'] = temp.loc[temp['call_year_wp'].notnull(), 'call_year_wp']

    # counter for each callId the number of year and if any year is null
    def year_calc(df):
        y = (df[['callId', 'call_year']].drop_duplicates()
                .groupby('callId', dropna=False, as_index=False)
                .agg(
                    nb_tot=('call_year','size'), 
                    null_exist=('call_year', lambda x: True if x.isnull().any() else False)
        ))
        return df[['callId', 'call_year']].drop_duplicates().sort_values('callId').merge(y, how='left', on='callId')

    # check if each callId has only one year and if any year is missing
    check_year = year_calc(temp)

    if len(check_year.loc[(check_year['nb_tot']>1)|(check_year['null_exist']==True)])>1:
        raise ValueError(f"1 - ++ YEARS for a same callId because topic diff: {check_year.loc[(check_year['nb_tot']>1)|(check_year['null_exist']==True)].callId.unique()}")
    else:
        print(f"2 - YEAR OK for all callId: {check_year[['call_year']].value_counts(dropna=False)}")
        df = df.merge(check_year[['callId', 'call_year']].drop_duplicates(), how='left', on='callId')
        print(f"- size after year added: {len(df)}")

    # # test call continu -> call open until 2027
    # if any(df['call_year'][df['call_year']>'2026']):
    #     print(f"3- CALL continu ; utiliser la date de calldeadline:\n{df['callId'][df['call_year']>'2021'].unique()}\n")

    for d in ['callDeadlineDate', 'startDate', 'endDate', 'ecSignatureDate', 'submissionDate']:
        df[d] = df[d].astype('datetime64[ns]')
    
    print(f"- size after year cleaned: {len(df)}")
    return df    

# def deadline_model(df, top_call):

#     df = pd.merge(df, top_call[['topicCode', 'deadline_model']].drop_duplicates(), how='left', on='topicCode', indicator=True)

#     return df

def strings_v(df):
    """
    remove special caracters and extra spaces in string columns (title, abstract, freekw, eic_panels, url)
    """
    for i in ['title','abstract', 'freekw', 'eic_panels', 'url']:
        df[i]=df[i].str.replace('\\n|\\t|\\r|\\s+', ' ', regex=True).str.strip()
    return df

def empty_str_to_none(df):
    """
    convert empty string to None in string columns
    """
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
