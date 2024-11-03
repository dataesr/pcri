from config_path import PATH_CLEAN
import pandas as pd, numpy as np

def dates_year(df):
    print("\n### DATES and YEAR")
    # création var commune de statut/ call
    df['call_year'] = df['callId'].str.extract('(\\d{4})')

    # traitement YEAR
    if any(df['call_year'].isnull()):
        print(f"1- si year none : \n{df['call_year'].value_counts(dropna=False)}")
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