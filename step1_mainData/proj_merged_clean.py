def dates_year(df):
    print("### DATES and YEAR")
    # création var commune de statut/ call
    df['call_year'] = df['callId'].str.extract('(\d{4})')

    # traitement YEAR
    if any(df['call_year'].isnull()):
        print(f"- si year none : \n{df['call_year'].value_counts(dropna=False)}")
    else:
        print(f"- calldeadline OK -> year:\n{df[['stage','call_year']].value_counts(dropna=False)}\n")

    # test call continu -> call open until 2027
    if any(df['call_year'][df['call_year']>'2026']):
        print(f"- CALL continu ; utiliser la date de calldeadline:\n{df['callId'][df['call_year']>'2021'].unique()}\n")

    for d in ['callDeadlineDate',  'startDate', 'endDate', 'ecSignatureDate', 'submissionDate']:
        df[d] = df[d].astype('datetime64[ns]')
    return df    


def strings_v(df):
    for i in ['title','abstract', 'freekw', 'eic_panels', 'url']:
        df[i]=df[i].str.replace('\n|\t|\r|\s+', ' ', regex=True).str.strip()

    for c in ['project_id']:
        df[c] = df[c].astype(str)
        if any(df['project_id'].str.contains('.')):
            df[c] = df[c].str.replace('.0', '', regex=False)
    
    return df

def empty_str_to_none(df):
    import pandas as pd, numpy as np
    for x in df.columns:
        if pd.api.types.infer_dtype(df[x])=='string':
            df[x]=np.where(df[x].isnull(), None, df[x])
    return df