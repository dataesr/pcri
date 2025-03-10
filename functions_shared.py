# load json file in a zipfile
def unzip_zip(namezip, path, data, encode):
    import pandas as pd
    import zipfile, json
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
    print("#FCT gps_col")
    df=df.assign(gps_loc=None)
    for i,row in df.iterrows():
        if row.loc['location'].get('latitude') is not None:
            df.at[i, 'gps_loc'] = re.search(r"^-?\d+\.?\d{,5}", str(row.loc['location'].get('latitude')))[0]+ "," +re.search(r"^-?\d+\.?\d{,5}", str(row.loc['location'].get('longitude')))[0]
    return df.drop('location', axis=1).drop_duplicates()  

def num_to_string(var):
    try:
        float(var)
        return var.astype(int, errors='ignore').astype(str).replace('.0', '')
    except:
        return str(var).replace('.0', '')
    
def erc_role(df, projects):
    import pandas as pd, numpy as np
    print("### ERC ROLE")
    proj_erc = projects.loc[projects.thema_code=='ERC', ['stage', 'project_id', 'destination_code', 'action_code']]
    df = df.merge(proj_erc, how='left', on='project_id').drop_duplicates()
    df = df.assign(erc_role='partner')

    # if 'app1' in globals():
    df.loc[(df.stage=='evaluated')&(df.destination_code=='SyG')&((df.partnerType=='host')|(df.role=='coordinator')), 'erc_role'] = 'PI'
    # elif 'part' in globals():
    df.loc[(df.stage=='successful')&(df.destination_code=='SyG')&(df.partnerType=='beneficiary')&(pd.to_numeric(df.orderNumber, errors='coerce')<5.), 'erc_role'] = 'PI'
    
    df.loc[(df.destination_code!='SyG')&(df.role=='coordinator'), 'erc_role'] = 'PI'
    df.loc[(df.destination_code=='ERC-OTHERS'), 'erc_role'] = np.nan
    df = df.drop(columns=['destination_code', 'action_code']).drop_duplicates()       
    print(f"- size after erc_role: {len(df)}")
    return df

def bugs_excel(df, chemin, name_sheet):
    import pandas as pd, os
    print("#FCT bugs_excel")
    chemin=f"{chemin}bugs_found.xlsx"
    if not os.path.exists(chemin):
        with pd.ExcelWriter(chemin) as writer:
            df.to_excel(writer, sheet_name=name_sheet)
    else:
        with pd.ExcelWriter(chemin, mode='a', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name=name_sheet)

def order_columns(df, xl_sheetname):
    import pandas as pd
    from config_path import PATH_ODS
    xl_path = f"{PATH_ODS}colonnes_ordres_par_jeux_ods.xlsx"
    colorder = pd.read_excel(xl_path, sheet_name=xl_sheetname, dtype={'order':int})
    colorder=colorder.sort_values('order')
    colorder=colorder.vars.unique()
    return df.reindex(columns=colorder)


def zipfile_ods(df, file_export):
    import zipfile
    from config_path import PATH_ODS
    with zipfile.ZipFile(f'{PATH_ODS}{file_export}.zip', 'w', compression=zipfile.ZIP_DEFLATED) as z:
        with z.open(f'{file_export}.csv', 'w', force_zip64=True) as f:
            df.to_csv(f, sep=';', encoding='utf-8', index=False, na_rep='', decimal=".")


def entreprise_cat_cleaning(df):
    import numpy as np
    df.loc[(df.flag_entreprise==True)&(~df.groupe_id.isnull()), 'entities_id'] = df.groupe_id
    df.loc[(df.flag_entreprise==True)&(~df.groupe_id.isnull()), 'entities_name'] = df.groupe_name
    if 'groupe_acronym' in df.columns:
        df.loc[(df.flag_entreprise==True)&(~df.groupe_id.isnull())&(~df.entities_acronym.isnull()), 'entities_acronym'] = df.groupe_acronym
        df.loc[(df.flag_entreprise==True)&(~df.groupe_id.isnull())&(df.groupe_acronym.isnull()), 'entities_acronym'] = np.nan
    df.loc[(df.entities_id.str.contains('^gent', na=False))&(df.insee_cat_code.isnull()), 'insee_cat_code'] = 'GE'
    df.loc[(df.entities_id.str.contains('^gent', na=False))&(df.insee_cat_name.isnull()), 'insee_cat_name'] = 'Grandes entreprises'
    for i in ['groupe_id', 'groupe_name', 'groupe_acronym']:
        if i in df.columns:
            df = df.drop(columns=i)
    df = df.rename(columns={'insee_cat_code':'entreprise_type_code',
            'insee_cat_name':'entreprise_type_name'})
    return df

def tokenization(text):
    if isinstance(text, str):
        tokens = text.split()
        return tokens

def prep_str_col(df, cols):
    from unidecode import unidecode
    from functions_shared import tokenization

    df[cols] = df[cols].apply(lambda x: x.str.lower())
    
    ## caracteres speciaux
    for i in cols:
        df.loc[~df[i].isnull(), i] = df[i].astype('str').apply(unidecode)
        df.loc[~df[i].isnull(), i] = df[i].str.replace('&', 'and')
        df.loc[~df[i].isnull(), i] = df.loc[~df[i].isnull(), i].apply(lambda x: tokenization(x)).apply(lambda x: [s.replace('.','') for s in x]).apply(lambda x: ' '.join(x))
    
    punct="'|–|,|\\.|:|;|\\!|`|=|\\*|\\+|\\-|‑|\\^|_|~|\\[|\\]|\\{|\\}|\\(|\\)|<|>|@|#|\\$"
    
    # # #
    df[cols] = df[cols].apply(lambda x: x.str.replace(punct, ' ', regex=True))    
    df[cols] = df[cols].apply(lambda x: x.str.replace(r"\n|\t|\r|\xc2|\xa9|\s+", ' ', regex=True).str.strip())
    df[cols] = df[cols].apply(lambda x: x.str.lower().str.replace('n/a|ndeg', ' ', regex=True).str.strip())
    df[cols] = df[cols].apply(lambda x: x.str.lower().str.replace('/', ' ').str.strip())
    df[cols] = df[cols].apply(lambda x: x.str.lower().str.replace('\\', ' ').str.strip())
    df[cols] = df[cols].apply(lambda x: x.str.lower().str.replace('"', ' ').str.strip())
    df[cols] = df[cols].apply(lambda x: x.str.replace(r"\s+", ' ', regex=True).str.strip())

    return df

def stop_word(df, cc_iso3 ,cols_list):
    import re, pandas as pd
    from functions_shared import tokenization
    
    stop_word=pd.read_json('data_files/stop_word.json')

    for col_ref in cols_list:
        df[f'{col_ref}_2'] = df[col_ref].apply(lambda x: tokenization(x))

        for i, row in stop_word.iterrows():
            if row['iso3']=='ALL':
                w = r"\b"+row['word'].strip()+r"\b"
                df.loc[~df[f'{col_ref}_2'].isnull(), f'{col_ref}_2'] = df.loc[~df[f'{col_ref}_2'].isnull(), f'{col_ref}_2'].apply(lambda x: [re.sub(w, '',  s) for s in x]).apply(lambda x: list(filter(None, x)))
            else:
                mask = df[cc_iso3]==row['iso3']
                w = r"\b"+row['word'].strip()+r"\b"
                df.loc[mask&(~df[f'{col_ref}_2'].isnull()), f'{col_ref}_2'] = df.loc[mask&(~df[f'{col_ref}_2'].isnull()), f'{col_ref}_2'].apply(lambda x: [re.sub(w, '',  s) for s in x]).apply(lambda x: list(filter(None, x)))

def adr_tag(df, cols_list):
    import json, re, pandas as pd
    from text_to_num import alpha2digit
    
    adr = json.load(open('data_files/ad.json'))

    for col_ref in cols_list:
        tmp = df.loc[~df[col_ref].isnull(), [col_ref]]
        for i in adr :
            for (k,v) in i.items():
                tmp[col_ref] = tmp[col_ref].apply(lambda x: [re.sub('^'+k+'$', v, s) for s in x])
                tmp[col_ref] = tmp[col_ref].apply(lambda x: list(filter(None, x)))

        df = pd.concat([df.drop(columns=[col_ref]), tmp], axis=1) 

        tmp[f'{col_ref}_tag'] = df.loc[~df[col_ref].isnull()][col_ref].apply(lambda x: ' '.join(x))
        with open("data_files/adresse_pattern.txt", "r") as pats:
             for n, line in enumerate(pats, start=1):       
                pat = line.rstrip('\n')
                tmp[f'{col_ref}_tag'] = tmp[f'{col_ref}_tag'].str.replace(pat,'', regex=True)

        tmp[f'{col_ref}_tag'] = tmp[f'{col_ref}_tag'].apply(lambda x: alpha2digit(x, 'fr'))
        tmp[f'{col_ref}_tag'] = tmp[f'{col_ref}_tag'].str.replace('[0-9]+','', regex=True)
        tmp[f'{col_ref}_tag'] = tmp[f'{col_ref}_tag'].str.strip()
        
        df = pd.concat([df.drop(columns=[col_ref]), tmp], axis=1)

        df.loc[(df.country_code!='FRA')&(~df[f'{col_ref}_tag'].isnull()), f'{col_ref}_tag'] = df.loc[(df.country_code!='FRA')&(~df[f'{col_ref}_tag'].isnull())][f'{col_ref}_tag'].str.split(' ').apply(lambda x: ' '.join([w for w in x if len(w) > 2]))

    return df

def chunkify(df, chunk_size: int):
    print(f"size df: {df.shape}")
    start = 0
    length = df.shape[0]
    # n_col = df.shape[1]
    
    # If DF is smaller than the chunk, return the DF
    if length <= chunk_size:
        yield df[:]
        return
    # if n_col <= 60:
    #     print(f"nb of cols: {n_col}")
    #     yield df[:]
    #     return

    # Yield individual chunks
    while start + chunk_size <= length:
        yield df[start:chunk_size + start]
        start = start + chunk_size

    # Yield the remainder chunk, if needed
    if start < length:
        yield df[start:]

def country_iso_shift(df, var, iso2_to3=True):
    import warnings
    warnings.filterwarnings("ignore", "This pattern is interpreted as a regular expression, and has match groups")
    from functions_shared import my_country_code
    countries = my_country_code()
    
    if iso2_to3:
        df = df.merge(countries[['iso3', 'iso2']].drop_duplicates(), how='left', left_on=var, right_on='iso2')
        df.loc[~df.iso3.isnull(), var] = df.loc[~df.iso3.isnull(), 'iso3']
        df.drop(columns=['iso2', 'iso3'], inplace=True)
        if any(df[var].str.len()<3):
            print(f"ATTENTION ! un {var} non reconnu dans df {df.loc[df[var].str.len()<3, [var]]}")
    else:
        df = df.merge(countries[['iso3', 'iso2']].drop_duplicates(), how='left', left_on=var, right_on='iso3')
        df.loc[~df.iso2.isnull(), var] = df.loc[~df.iso2.isnull(), 'iso2']
        df.drop(columns=['iso2', 'iso3'], inplace=True)
        if any(df[var].str.len()>2):
            print(f"ATTENTION ! un {var} non reconnu dans df {df.loc[df[var].str.len()>2, [var]]}")
    return df

def my_country_code():
    import pycountry, pandas as pd, json, numpy as np
    pycountry.countries.add_entry(alpha_2="XK", alpha_3="XKX", name="Kosovo")
    pycountry.countries.add_entry(alpha_2="UK", alpha_3="GBR", name="United Kingdom")
    pycountry.countries.add_entry(alpha_2="EL", alpha_3="GRC", name="Greece")
    pycountry.countries.add_entry(alpha_2="AN", alpha_3="ANT", name="Netherlands Antilles (former 2011)")
    dict1 = [c.__dict__['_fields'] for c in list(pycountry.countries)]
    df = (pd.DataFrame(dict1)[['alpha_2', 'alpha_3', 'name']]
                .rename(columns={'alpha_2':'iso2', 'alpha_3':'iso3', 'name':'country_name_en'})
                .drop_duplicates()
                .assign(parent_iso2=np.nan)
        )

    list_var=['iso2']
    ccode=json.load(open("data_files/countries_parent.json"))
    for c in list_var:
        for k,v in ccode.items():
            df.loc[df[c]==k, 'parent_iso2'] = v

    df.loc[df.parent_iso2.isnull(), 'parent_iso2'] = df.loc[df.parent_iso2.isnull(), 'iso2']
    df=(df.merge(df[['iso2','iso3']].drop_duplicates().rename(columns={'iso2':'parent_iso2','iso3':'parent_iso3'}), 
                    how='left', on='parent_iso2'))

    print(f"- def(my_country_code) size df: {len(df)}")
    return df

def prop_string(tab, cols):
    from unidecode import unidecode
    tab[cols] = tab[cols].map(lambda s:s.casefold() if type(s) == str else s)
            
    for i in cols:
        tab.loc[~tab[i].isnull(), i] = tab.loc[~tab[i].isnull(), i].str.replace(r"[^\w\s]+", " ", regex=True)
        tab.loc[~tab[i].isnull(), i] = tab.loc[~tab[i].isnull(), i].apply(unidecode)
    return tab

def com_iso3():
    import pandas as pd
    url='https://docs.google.com/spreadsheet/ccc?key=1FwPq5Qw7Gbgj_sBD6Za4dfDDk6ydozQ99TyRjLkW5d8&output=xls'
    com_iso = pd.read_excel(url, sheet_name='LES_COMMUNES', dtype=str, na_filter=False)
    com_iso=com_iso[['COM_CODE', 'ISO_3']].drop_duplicates()
    com_iso.columns=com_iso.columns.str.lower()
    return com_iso