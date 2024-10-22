#traitement URL
def url_to_clean(df):
    import numpy as np
    from functions_shared import website_to_clean
    for i,row in df.iterrows():
        if row.loc['url'] is not None:
            df.at[i, 'project_webpage'] = website_to_clean(row['url']) 
    return df