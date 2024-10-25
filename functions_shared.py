# load json file in a zipfile
def unzip_zip(namezip, path, data, encode):
    import zipfile, json
    import pandas as pd
    if 'json' in data:
        with zipfile.ZipFile(f"{path}{namezip}", 'r', metadata_encoding=encode) as z:
            return json.load(z.open(data, 'r'))
    if 'csv' in data:
        with zipfile.ZipFile(f"{path}{namezip}", 'r', metadata_encoding=encode) as z:
            return pd.read_csv(z.open(data), low_memory=False, dtype='str')


#convert column of lists in strings column
def del_list_in_col(df, var_old:str, var_new:str):
    df[var_new] = None
    for i, row in df.iterrows():
        if row[var_old]!=[]:
            df.at[i, var_new] = "|".join(str(e) for e in row[var_old] if e is not None)
    return df.drop(var_old, axis=1)

def work_csv(df, file_csv_name):
    from config_path import PATH_WORK
    name = file_csv_name
    return df.to_csv(f"{PATH_WORK}{name}.csv", sep=';', na_rep='', encoding='utf-8', index=False)

def website_to_clean(web_var: str):
    import re
    pat=re.compile(r"((((https|http)://)?(www\.)?)([\w\d#@%;$()~_?=&]+\.)+([a-z]{2,3}){1}([\w\d:#@%/;$()~_?\+-=\\\.&]+))")
    y= re.search(pat, str(web_var))
    if y is not None:
        return y.group()
    
def columns_comparison(df, namefile):
    import numpy as np
    old_cols = np.load(f"data_files/{namefile}.npy").tolist()
    new_cols = df.columns.to_list()
    if any(set(new_cols) - set(old_cols)):
        print(f"- new cols: {set(new_cols) - set(old_cols)}")
    else:
        print("- no new columns")

def gps_col(df):
    import re
    df=df.assign(gps_loc=None)
    for i,row in df.iterrows():
        if row.loc['location'].get('latitude') is not None:
            df.at[i, 'gps_loc'] = re.search(r'^-?\d+\.?\d{,5}', str(row.loc['location'].get('latitude')))[0]+ "," +re.search(r'^-?\d+\.?\d{,5}', str(row.loc['location'].get('longitude')))[0]
    return df.drop('location', axis=1).drop_duplicates()  

def num_to_string(var):
    try:
        float(var)
        return var.astype(int, errors='ignore').astype(str).replace('.0', '')
    except:
        return str(var).replace('.0', '')