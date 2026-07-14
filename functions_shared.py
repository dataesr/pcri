def timing(st):
    import time
    return "[{:.2f}s]".format(time.time() - st)


def last_data_zip(path, framework, type):
    import glob, os

    folder = f"{path}{framework}/"
    if type == 'json':
        zipname = "HE_*.json.zip"
    else:
        zipname = "HE_*.csv.zip"
    
    return os.path.basename(
            max(
                glob.glob(folder + zipname),
                key=os.path.getmtime
            )
        )
    

# load json file in a zipfile
def unzip_zip(source, data, encode):
    import pandas as pd
    import zipfile, json
    if 'json' in data:
        with zipfile.ZipFile(source, 'r', metadata_encoding=encode) as z:
            return json.load(z.open(data, 'r'))
    if 'csv' in data:
        with zipfile.ZipFile(source, 'r', metadata_encoding=encode) as z:
            return pd.read_csv(z.open(data), low_memory=False, dtype='str')


#convert column of lists in strings column
def del_list_in_col(df, var_old:str, var_new:str):
    df[var_new] = None
    for i, row in df.iterrows():
        if row[var_old]!=[]:
            df.at[i, var_new] = "|".join(str(e) for e in row[var_old] if e is not None)
    return df.drop(var_old, axis=1)

def work_csv(df, file_csv_name):
    from paths import PATH_WORK
    name = file_csv_name
    return df.to_csv(f"{PATH_WORK}{name}.csv", sep=';', na_rep='', encoding='utf-8', index=False)

def clean_keyword(keyword):
    import re
    # Convertir en minuscules
    keyword = keyword.lower().replace("&", "and")
    # Supprimer les caractères spéciaux
    keyword = re.sub(r'[^a-zA-Z0-9\s]', ' ', keyword)
    return keyword


def website_to_clean(web_var: str):
    import re
    pat=re.compile(r"((((https|http)://)?(www\.)?)([\w\d#@%;$()~_?=&]+\.)+([a-z]{2,3}){1}([\w\d:#@%/;$()~_?\+-=\\\.&]+))")
    y= re.search(pat, str(web_var))
    if y is not None:
        return y.group()
    
def columns_comparison(df, source):
    path = "data_files/cols_by_table.json"
    j = json.load(open(path, "r", encoding="utf-8"))
    if source not in j:
        raise KeyError(f"source '{source}' introuvable.")
    
    old_cols = j[source]
    new_cols = df.columns.to_list()
    new_entries = set(new_cols) - set(old_cols)

    if new_entries:
        print(f"- new cols: {new_entries}")
        j[source].extend(new_entries)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(j, f, indent=4, ensure_ascii=False)
        print(f"- ajoutées dans '{source}': {new_entries}")
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

def bugs_excel(df, chemin, name_sheet):
    """
    save a dataframe in an excel file in the folder bugs_found with the name of the sheet as name_sheet
    """
    import pandas as pd, os
    chemin=f"{chemin}bugs_found.xlsx"
    if not os.path.exists(chemin):
        with pd.ExcelWriter(chemin) as writer:
            df.to_excel(writer, sheet_name=name_sheet)
    else:
        with pd.ExcelWriter(chemin, mode='a', if_sheet_exists='replace') as writer:
            df.to_excel(writer, sheet_name=name_sheet)


def cols_select(FP, xl_sheetname):
    import pandas as pd
    from paths import PATH_ODS
    xl_path = f"{PATH_ODS}colonnes_ordres_par_jeux_ods.xlsx"
    df = pd.read_excel(xl_path, sheet_name=xl_sheetname, dtype={'order':int})
    return df[['vars', FP, 'order']]

def cols_order(df, xl_sheetname):
    import pandas as pd
    from paths import PATH_ODS
    xl_path = f"{PATH_ODS}colonnes_ordres_par_jeux_ods.xlsx"
    colorder = pd.read_excel(xl_path, sheet_name=xl_sheetname, dtype={'order':int})
    colorder=colorder.sort_values('order')
    colorder=colorder.vars.unique()
    return df.reindex(columns=colorder)

def select_cols_FP(FP, file_ods):   
    cols_h=cols_select(FP, file_ods)
    select=cols_h.loc[cols_h[FP].notna(), FP].unique()
    return select

def rename_cols_FP(FP, file_ods):   
    cols_h=cols_select(FP, file_ods)
    rename_map=cols_h[cols_h[FP].notna()].set_index(FP)['vars'].to_dict()
    return rename_map

def df_order_cols_FP(FP, file_ods, df):   
    cols_h=cols_select(FP, file_ods)
    order_map=cols_h.sort_values('order').vars.unique()

    l=[]
    for i in order_map:
        if i in df.columns:
            l.append(i)
    return df.reindex(columns=l)

def zipfile_ods(df, file_export):
    import zipfile
    from paths import PATH_ODS
    with zipfile.ZipFile(f'{PATH_ODS}{file_export}.zip', 'w', compression=zipfile.ZIP_DEFLATED) as z:
        with z.open(f'{file_export}.csv', 'w', force_zip64=True) as f:
            df.to_csv(f, sep=';', encoding='utf-8', index=False, na_rep='', decimal=".")


def entreprise_group_cleaning(df):
    import numpy as np
    df.loc[(df.entreprise_flag==True)&(~df.groupe_id.isnull()), 'entities_id'] = df.groupe_id
    df.loc[(df.entreprise_flag==True)&(~df.groupe_id.isnull()), 'entities_name'] = df.groupe_name
    if 'groupe_acronym' in df.columns:
        df.loc[(df.entreprise_flag==True)&(~df.groupe_id.isnull()), 'entities_acronym'] = df.groupe_acronym
        # df.loc[(df.entreprise_flag==True)&(~df.groupe_id.isnull())&(df.groupe_acronym.isnull()), 'entities_acronym'] = np.nan
    df.loc[(df.entities_id.str.contains('^gent', na=False))&(df['cat_entreprise_code'].isnull()), 'cat_entreprise_code'] = 'GE'
    df.loc[(df.entities_id.str.contains('^gent', na=False))&(df['cat_entreprise_name'].isnull()), 'cat_entreprise_name'] = 'Grandes entreprises'
    for i in ['groupe_id', 'groupe_name', 'groupe_acronym']:
        if i in df.columns:
            df = df.drop(columns=i)
    return df

def tokenization(text):
    if isinstance(text, str):
        tokens = text.split()
        return tokens

def prep_str_col(df, cols):
    from unidecode import unidecode
    from functions_shared import tokenization

    punct=r"'|–|,|\\.|:|;|\\!|`|=|\\*|\\+|\\-|‑|\\^|_|~|\\[|\\]|\\{|\\}|\\(|\\)|<|>|@|#|\\$"
    
    ## caracteres speciaux
    for i in cols:
        if i in df.columns:
            df.loc[~df[i].isnull(), i] = df.loc[~df[i].isnull(), i].str.lower()
            df.loc[~df[i].isnull(), i] = df.loc[~df[i].isnull(), i].astype('str').apply(unidecode)
            df.loc[~df[i].isnull(), i] = df.loc[~df[i].isnull(), i].str.replace('&', 'and')
            df.loc[~df[i].isnull(), i] = df.loc[~df[i].isnull(), i].apply(lambda x: tokenization(x)).apply(lambda x: [s.replace('.','') for s in x]).apply(lambda x: ' '.join(x))
        

            df[i] = df[i].str.replace(punct, ' ', regex=True)
            df[i] = df[i].str.replace(r"\n|\t|\r|\xc2|\xa9|\s+", ' ', regex=True).str.strip()
            df[i] = df[i].str.lower().replace('n/a|ndeg', ' ', regex=True).str.strip()
            df[i] = df[i].str.replace('/', ' ', regex=True).str.strip()
            df[i] = df[i].str.replace(r"\\", ' ', regex=True).str.strip()
            df[i] = df[i].str.replace('"', ' ').str.strip()
            df[i] = df[i].str.replace(r"\s+", ' ', regex=True).str.strip()

    return df


def stop_word(df, cc_iso3 ,cols_list):
    import pandas as pd
    stop_word=pd.read_json('data_files/stop_word.json')

    for col_ref in cols_list:
        if col_ref in df.columns:
            print(f"-{col_ref}")
            tmp=df[[cc_iso3,col_ref]]
            tmp[col_ref] = tmp[col_ref].str.split()
            tmp=tmp.explode(col_ref).reset_index()
            tmp = tmp.mask(tmp=='')

            tmp = (tmp[~tmp[col_ref].isnull()]
                    .merge(stop_word.loc[stop_word.iso3=='ALL'], 
                        how='left', left_on=col_ref, right_on='word', indicator=True)
                    .query('_merge=="left_only"')[['index', cc_iso3, col_ref]])
            tmp = (tmp.merge(stop_word, 
                            how='left', left_on=[cc_iso3, col_ref], right_on=['iso3', 'word'], 
                            indicator=True).query('_merge=="left_only"')[['index', col_ref]])

            tmp = tmp.groupby('index').agg(lambda x: ' '.join(x)).rename(columns={col_ref:f'{col_ref}_2'})

            df= df.merge(tmp, how='left', left_index=True, right_index=True)
    return df

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
    pycountry.countries.add_entry(alpha_2="AN", alpha_3="ANT", name="Netherlands Antilles (Disestablished 2011)")
    pycountry.countries.add_entry(alpha_2="CP", alpha_3="CPT", name="Clipperton Island")
    pycountry.countries.add_entry(alpha_2="AX", alpha_3="ALA", name="Åland Islands")
    pycountry.countries.add_entry(alpha_2="MF", alpha_3="MAF", name="Saint Martin (French part)")
    pycountry.countries.add_entry(alpha_2="ZZ", alpha_3="ZZZ", name="Not available")
    pycountry.countries.add_entry(alpha_2="YU", alpha_3="YUG", name="Serbia and Montenegro")
    pycountry.countries.add_entry(alpha_2="EU", alpha_3="ZOE", name="European organisations area")
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

def load_last_file_csv(path_folder, file_prefix, sep):
    import os, pandas as pd

    matching_files = []

    for file_name in os.listdir(path_folder):
        file_path = os.path.join(path_folder, file_name)
        
        # Check if it's a file, its name starts with the desired prefix, and it's a CSV file
        if os.path.isfile(file_path) and file_name.startswith(file_prefix) and file_name.endswith('.csv'):
            matching_files.append(file_path)

    # If there are matching files, proceed to find the most recent one
    if matching_files:
        # Sort the files by their last modification time (most recent first)
        most_recent_file = max(matching_files, key=os.path.getmtime)

    return pd.read_csv(most_recent_file, sep=sep)

def FP_suivi(df):
    import pandas as pd, json, numpy as np

    if 'action_next_fp' in df.columns:
        df.loc[~df.action_next_fp.isnull(), 'action_code'] = df.loc[~df.action_next_fp.isnull()].action_next_fp 
        df.drop(columns=['action_next_fp'], inplace=True)

    destination = pd.DataFrame(json.load(open('data_files/destination.json', 'r+', encoding='utf-8'))).drop(columns=['destination_lib', 'destination_name_fr'])
    
    if 'destination_next_fp' in df.columns:
        #msca
        df.loc[df.action_code=='MSCA', 'destination_detail_code'] = df.loc[df.action_code=='MSCA', 'destination_next_fp']
        df.loc[df.action_code=='MSCA', 'destination_code'] = df.loc[df.action_code=='MSCA', 'destination_detail_code']
        df.loc[(df.action_code=='MSCA')&(df.destination_code!='MSCA-OTHER'), 'destination_code'] = df.loc[(df.action_code=='MSCA')&(df.destination_code!='MSCA-OTHER')].destination_detail_code.str.split('-').str[0]
        df.loc[(df.thema_code=='MSCA')&(df.action_code!='MSCA'), 'destination_code'] = np.nan
        
        # ERC
        df.loc[(df.thema_code=='ERC')&(df.action_code!='ERC'), 'destination_code'] = np.nan

        df = (df.drop(columns='destination_name_en')
                .merge(destination, how='left', on='destination_code')
                .merge(destination
                        .rename(columns={'destination_code':'destination_detail_code', 
                                        'destination_name_en':'destination_detail_name_en'}),
                    how='left', on='destination_detail_code')
        )

        df.drop(columns=['destination_next_fp'], inplace=True)

    if 'euro_partnerships_type_next_fp' in df.columns:
        df.loc[~df.euro_partnerships_type_next_fp.isnull(), 'euro_partnerships_type'] = df.loc[~df.euro_partnerships_type_next_fp.isnull(), 'euro_partnerships_type_next_fp']
        df.drop(columns=['euro_partnerships_type_next_fp'], inplace=True)
        
    return df


def remove_file_by_pattern(path_folder, pat):
    import os   

    # Parcourir tous les fichiers dans le dossier
    for fichier in os.listdir(path_folder):
        # Vérifier si le fichier correspond au motif
        if pat.match(fichier):
            chemin_complet = os.path.join(path_folder, fichier)
            try:
                # Supprimer le fichier
                os.remove(chemin_complet)
                print(f"Fichier supprimé : {chemin_complet}")
            except Exception as e:
                print(f"Erreur lors de la suppression de {chemin_complet}: {e}")

def last_file_into_folder_by_pat(path, pat, extension):
    import os
    files_list = [f for f in os.listdir(path) if pat.search(f) and f.endswith(f'.{extension}')]

    # Trouver le fichier le plus récent
    latest_file = max(files_list, key=lambda f: os.path.getmtime(os.path.join(path, f)))

    # Chemin complet vers le fichier le plus récent
    print(f"Le fichier {extension} le plus récent est : {os.path.join(path, latest_file)}")
    return os.path.join(path, latest_file)

def length_code_geo(var):
    if var is None:
        return None
    if len(str(var)) != 5:
        return '0' * (5 - len(str(var))) + str(var)
    else:
        return str(var)
    
def get_gs(sheet_name: str, vars_list: list=None):
    import pandas as pd, os, json
    url=f"https://docs.google.com/spreadsheet/ccc?key={os.environ.get('GOOGLE_KEY')}&output=xls"
    df_c = pd.read_excel(url, sheet_name=sheet_name, dtype=str, na_filter=False)

    names=[('G_PAYS', 'country_gs_insee')]
    for name_old, name_new in names:
        if sheet_name == name_old:
            json.dump(df_c[vars_list].to_dict('records'), open(f'data_files/{name_new}.json', 'w', encoding='utf-8'), ensure_ascii=False, indent=4)
    if vars_list == None:
        return df_c
    else:
        return df_c[vars_list]


def convert_lambert_to_gps(x_col, y_col):
    """
    Convertit les colonnes Lambert d'un DataFrame en coordonnées GPS (WGS84).

    Args:
        x_col (str): Nom de la colonne des abscisses (x) en Lambert.
        y_col (str): Nom de la colonne des ordonnées (y) en Lambert.
    Returns:
        pd.DataFrame: DataFrame avec les colonnes 'gps' ajoutées.
    """

def convert_lambert_to_gps(x_col, y_col):
    """
    Convertit les colonnes Lambert en coordonnées GPS (WGS84) et retourne une chaîne de caractères.
    Gère les valeurs NaN en retournant une chaîne vide.

    Args:
        x_col (str/float): Valeur de l'abscisse (x) en Lambert.
        y_col (str/float): Valeur de l'ordonnée (y) en Lambert.

    Returns:
        str: Coordonnées GPS sous forme de chaîne "longitude,latitude" ou "" si NaN.
    """
    import pandas as pd
    from pyproj import Transformer, CRS
    
    # Vérifier si x_col ou y_col est NaN
    if pd.isna(x_col) or pd.isna(y_col):
        return ""

    try:
        # Convertir les entrées en float
        X = float(x_col)
        Y = float(y_col)
    except (ValueError, TypeError):
        return ""

    # Créer un transformateur Lambert → WGS84
    transformer = Transformer.from_crs(CRS('EPSG:2154'), CRS('EPSG:4326'))

    # Appliquer la transformation
    latitude, longitude = transformer.transform(X, Y)

    # Retourner sous forme de chaîne "longitude,latitude"
    return f"{latitude:.3f},{longitude:.3f}"

def upper_word_in_text(word, text):
    words = text.split()
    for i, w in enumerate(words):
        if word in text:
            words[i] = w.capitalize()
    return ' '.join(words)

def upper_word_in_text(text, words):
    text_split = text.split()
    for i, w in enumerate(text_split):
        if w.lower() in words:
            text_split[i] = w.capitalize()
    return ' '.join(text_split)

import json

def extract_json_from_file(file_path):
    with open(file_path, 'r', encoding='utf-8') as file:
        content = file.read()

    # Trouver le début et la fin du JSON valide
    start_index = content.find('[')
    end_index = content.rfind(']') + 1

    if start_index == -1 or end_index == 0:
        raise ValueError("Aucune liste JSON valide trouvée dans le fichier.")

    json_str = content[start_index:end_index]

    # Charger la liste de dictionnaires
    try:
        data = json.loads(json_str)
        return data
    except json.JSONDecodeError as e:
        raise ValueError(f"Erreur lors de la lecture du JSON : {e}")
    
def clean_invisible_chars(s: str):
    if isinstance(s, str):
        # Remplace les caractères invisibles par un espace
        s = s.replace('\r', ' ').replace('\n', ' ').replace('\t', ' ')
        # Supprime les espaces multiples et les espaces en début/fin
        s = ' '.join(s.split())
        return s
    return s

def check_if_only_charact_special(s):
    import re
    return bool(re.fullmatch(r'[^a-zA-Z0-9]+', str(s)))

def clean_quotation_marks(s):
    # Supprimer toutes les guillemets
    import re
    return re.sub(r"[^\w\s]", " ", str(s), flags=re.UNICODE)

def clean_if_only_at_start(s):
    import re
    return re.sub(r'^[^a-zA-Z0-9]+', '', s)

def trace_chain(child, mapping):
    import pandas as pd
    seen=set()
    current=child
    while current in mapping and pd.notna(mapping[current]) and mapping[current] not in seen:
        seen.add(current)
        current = mapping[current]
    return current

def capitalize_if_all_upper(s):
    if isinstance(s, str) and s.isupper():
        return s.capitalize()
    return s


def clean_text(text: str) -> str:
    import re
    import unicodedata
    """
    Clean and normalize English text.
    - Normalize unicode (NFC)
    - Remove invisible / control characters
    - Normalize all unicode spaces to ASCII space
    - Strip extra whitespace
    """

    # 1. Normalize unicode
    text = unicodedata.normalize("NFC", text)

    # 2. Remove control characters (keep \n and \t)
    text = "".join(
        ch for ch in text
        if unicodedata.category(ch) != "Cc"
        or ch in ("\n", "\t")
    )

    # 3. Remove zero-width characters
    text = re.sub(r"[\u200b-\u200f\u202a-\u202e\ufeff]", "", text)

    # 3b. ← NEW: replace ALL unicode whitespace variants with a plain space
    #     [^\S\n\t] = "whitespace that is NOT newline and NOT tab"
    #     catches \xa0 (nbsp), \u202f (narrow nbsp), \u2009 (thin), etc.
    text = re.sub(r"[^\S\n\t]", " ", text)

    # 4. Normalize line endings
    text = text.replace("\r\n", "\n").replace("\r", "\n")

    # 5. Collapse multiple spaces / tabs on a line
    text = re.sub(r"[ \t]+", " ", text)

    # 6. Strip each line
    lines = [line.strip() for line in text.splitlines()]

    # 7. Collapse excess blank lines
    text = re.sub(r"\n{3,}", "\n\n", "\n".join(lines))

    return text.strip()

def diagnose_column_int(df, col):
    print(f"\n📊 Colonne '{col}' — dtype: {df[col].dtype}")
    for i, v in df[col].items():
        type_name = type(v).__name__
        try:
            int(v)
            convertible = ""
        except (ValueError, TypeError):
            convertible = "❌"
            print(f"  ligne {i}: {repr(v)} ({type_name}) {convertible}")


def create_archive_zip(path_folder, archive_name=None, extension_file=".pkl"):
    import os
    import zipfile
    from datetime import date

    # Liste des fichiers .pkl à archiver
    fichiers_pkl = [f for f in os.listdir(path_folder) if f.endswith(".pkl")]

    if fichiers_pkl:
        print(f"ATTENTION ! create_archive_zip() will archive all files with the extension '{extension_file}' in the folder '{path_folder}' and then delete them after archiving.")
        # Nom de l'archive avec la date du jour
        date_du_jour = date.today().strftime("%Y%m%d")
        nom_archive = f"{archive_name}_{date_du_jour}.zip"
        chemin_archive = os.path.join(path_folder, nom_archive)


        # Création du zip contenant tous les fichiers .pkl du dossier
        with zipfile.ZipFile(chemin_archive, "w", zipfile.ZIP_DEFLATED) as archive:
            for fichier in fichiers_pkl:
                chemin_fichier = os.path.join(path_folder, fichier)
                archive.write(chemin_fichier, arcname=fichier)

        # Suppression des fichiers .pkl une fois archivés
        for fichier in fichiers_pkl:
            os.remove(os.path.join(path_folder, fichier))

        print(f"Archive created : {chemin_archive}")
        print(f"{len(fichiers_pkl)} file(s) .pkl deleted.")
    else:
        print("No files with the extension '.pkl' found in the folder. No archive created.")
