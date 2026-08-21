import pandas as pd 

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


# Détecte les valeurs nulles (NaN, None) OU vides (chaîne vide, espaces)
def check_missing(df, cols: list):
    alerts = []
    for col in cols:
        # masque : null OU (string vide/espaces après strip)
        mask = df[col].isna() | (df[col].astype(str).str.strip() == '')
        n_missing = mask.sum()
        if n_missing > 0:
            alerts.append({
                'colonne': col,
                'nb_lignes_vides': n_missing,
                'index_lignes': df[mask].index.tolist()
            })

    if alerts:
        print("⚠️ ALERTE : valeurs manquantes détectées !")
        for a in alerts:
            print(f"  - {a['colonne']}: {a['nb_lignes_vides']} valeur(s) manquante(s) "
                f"(lignes: {a['index_lignes'][:10]}{'...' if len(a['index_lignes'])>10 else ''})")
    else:
        print("✅ Aucune valeur manquante dans les colonnes vérifiées.")


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
    df=df.assign(gps_source=None)
    for i,row in df.iterrows():
        if row.loc['location'].get('latitude') is not None:
            df.at[i, 'gps_source'] = re.search(r"^-?\d+\.?\d{,5}", str(row.loc['location'].get('latitude')))[0]+ "," +re.search(r"^-?\d+\.?\d{,5}", str(row.loc['location'].get('longitude')))[0]
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


def entities_choose_status(df: pd.DataFrame, cols: list) -> pd.DataFrame:
    
    """Trie et filtre les entités selon l'ordre de priorité de generalState."""
    gen_state = [
        "VALIDATED",
        "DECLARED",
        "SLEEPING",
        "SUSPENDED",
        "BLOCKED",
        "DEPRECATED",
        "Undefined",
    ]

    unique_states = set(df["generalState"].unique())
    diff_states = unique_states - set(gen_state)

    if len(df["generalState"].dropna().unique()) > len(gen_state):
        print(f"⚠️ ! new generalState in entities -> {diff_states}")
    else:

        df["generalState"] = pd.Categorical(
            df["generalState"], categories=gen_state, ordered=True
        )
        df = df.sort_values(cols + ["generalState"]).reset_index(drop=True)
        df = df.groupby(cols).head(1)

        print(f"3 - size entities after cleaning: {len(df)}")

    return df


def cols_select_mongo(FP, xl_sheetname):
    import pandas as pd
    from paths import PATH_ODS
    xl_path = f"{PATH_ODS}colonnes_ordres_par_jeux_ods.xlsx"
    df = pd.read_excel(xl_path, sheet_name=xl_sheetname)
    col_name_list = df.loc[df['mongo'] == 'x', FP].values.flatten()
    return sorted({str(x) for x in col_name_list if pd.notna(x)}, key=str.lower)


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

    punct = r"'|–|,|\.|:|;|!|`|=|\*|\+|\-|-|\^|_|~|\[|\]|\{|\}|\(|\)|<|>|@|#|\$"

    for col in cols:

        if col not in df.columns:
            continue

        s = df[col].astype('string').str.lower()

        s = s.map(
            lambda x: unidecode(x) if pd.notna(x) else x
        )

        s = s.str.replace('&', 'and', regex=False)
        s = s.str.replace(r'\.', '', regex=True)
        s = s.str.replace(punct, ' ', regex=True)
        s = s.str.replace(r'[/\\]', ' ', regex=True)
        s = s.str.replace('"', ' ', regex=False)
        s = s.str.replace(
            r'\n|\t|\r|\xc2|\xa9|\s+',
            ' ',
            regex=True
        )
        s = s.str.replace(r'n/a|ndeg', ' ', regex=True)
        s = s.str.strip()
        s = s.str.replace(r'\s+', ' ', regex=True)

        df[col] = s

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
    import json
    import re
    import pandas as pd
    from text_to_num import alpha2digit

    # ------------------------------------------------------------
    # Chargement des abréviations d'adresse
    # ad.json = {"av": "avenue", "bd": "boulevard", ...}
    # ------------------------------------------------------------
    with open("data_files/ad.json", encoding="utf-8") as f:
        adr = json.load(f)

    # Vérification simple du format
    if not isinstance(adr, dict):
        raise ValueError(
            "data_files/ad.json doit être un dictionnaire "
            "{'abbreviation': 'replacement'}"
        )

    for col_ref in cols_list:

        if col_ref not in df.columns:
            continue

        # --------------------------------------------------------
        # On ne travaille que sur les valeurs non nulles
        # --------------------------------------------------------
        mask = df[col_ref].notna()

        if not mask.any():
            continue

        tmp = df.loc[mask, [col_ref]].copy()

        # --------------------------------------------------------
        # IMPORTANT :
        # adr_tag attend des listes de mots.
        #
        # Si street contient déjà des listes -> on les conserve.
        # Si street contient une chaîne -> on la transforme en liste.
        # --------------------------------------------------------
        tmp[col_ref] = tmp[col_ref].apply(
            lambda x: (
                x
                if isinstance(x, list)
                else str(x).split()
            )
        )

        # --------------------------------------------------------
        # Remplacement des abréviations
        # --------------------------------------------------------
        for k, v in adr.items():

            pattern = rf"^{re.escape(k)}$"

            tmp[col_ref] = tmp[col_ref].apply(
                lambda words: [
                    re.sub(pattern, v, word)
                    for word in words
                ]
            )

        # --------------------------------------------------------
        # Création du tag adresse
        # --------------------------------------------------------
        tmp[f'{col_ref}_tag'] = tmp[col_ref].apply(
            lambda x: ' '.join(x)
        )

        # --------------------------------------------------------
        # Suppression des éléments définis dans
        # adresse_pattern.txt
        # --------------------------------------------------------
        with open(
            "data_files/adresse_pattern.txt",
            encoding="utf-8"
        ) as pats:

            for line in pats:
                pat = line.rstrip('\n')

                if pat:
                    tmp[f'{col_ref}_tag'] = (
                        tmp[f'{col_ref}_tag']
                        .str.replace(
                            pat,
                            '',
                            regex=True
                        )
                    )

        # --------------------------------------------------------
        # Conversion des nombres en mots
        # puis suppression des nombres
        # --------------------------------------------------------
        tmp[f'{col_ref}_tag'] = (
            tmp[f'{col_ref}_tag']
            .apply(lambda x: alpha2digit(x, 'fr'))
            .str.replace(r'[0-9]+', '', regex=True)
            .str.replace(r'\s+', ' ', regex=True)
            .str.strip()
        )

        # --------------------------------------------------------
        # Pour les pays hors France :
        # suppression des mots trop courts
        # --------------------------------------------------------
        mask_foreign = (
            df.loc[mask, 'country_code'].ne('FRA')
            & tmp[f'{col_ref}_tag'].notna()
        )

        tmp.loc[mask_foreign, f'{col_ref}_tag'] = (
            tmp.loc[
                mask_foreign,
                f'{col_ref}_tag'
            ]
            .str.split()
            .apply(
                lambda words: ' '.join(
                    w for w in words
                    if len(w) > 2
                )
            )
        )

        # --------------------------------------------------------
        # Réintégration dans le dataframe
        # --------------------------------------------------------
        df.loc[mask, col_ref] = tmp[col_ref]
        df.loc[mask, f'{col_ref}_tag'] = tmp[f'{col_ref}_tag']

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
            print(f"- ⚠️ ! un {var} non reconnu dans df {df.loc[df[var].str.len()<3, [var]]}")
    else:
        df = df.merge(countries[['iso3', 'iso2']].drop_duplicates(), how='left', left_on=var, right_on='iso3')
        df.loc[~df.iso2.isnull(), var] = df.loc[~df.iso2.isnull(), 'iso2']
        df.drop(columns=['iso2', 'iso3'], inplace=True)
        if any(df[var].str.len()>2):
            print(f"- ⚠️ ! un {var} non reconnu dans df {df.loc[df[var].str.len()>2, [var]]}")
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

# def com_iso3():
#     import pandas as pd
#     from remote_process.grist import communesG
#     url='https://docs.google.com/spreadsheet/ccc?key=1FwPq5Qw7Gbgj_sBD6Za4dfDDk6ydozQ99TyRjLkW5d8&output=xls'
#     com_iso = pd.read_excel(url, sheet_name='LES_COMMUNES', dtype=str, na_filter=False)
#     com_iso=com_iso[['COM_CODE', 'ISO_3']].drop_duplicates()
#     com_iso.columns=com_iso.columns.str.lower()
#     return com_iso

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

    
def get_gs(sheet_name: str, vars_list: list = None) -> pd.DataFrame:
    """Récupère une feuille Google Sheet et exporte en JSON si nécessaire."""
    google_key = os.environ.get("GOOGLE_KEY")
    url = f"https://docs.google.com/spreadsheet/ccc?key={google_key}&output=xls"

    df_c = pd.read_excel(
        url, sheet_name=sheet_name, dtype=str, na_filter=False
    )

    names = [("G_PAYS", "country_gs_insee")]
    for name_old, name_new in names:
        if sheet_name == name_old:
            file_path = f"data_files/{name_new}.json"
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(
                    df_c[vars_list].to_dict(orient="records"),
                    f,
                    ensure_ascii=False,
                    indent=4,
                )

    if vars_list is None:
        return df_c

    return df_c[vars_list]


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
        print(f"- ⚠️ ! create_archive_zip() will archive all files with the extension '{extension_file}' in the folder '{path_folder}' and then delete them after archiving.")
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


import pandas as pd

def check_dataframe(
    df: pd.DataFrame,
    required_columns: list[str] | None = None,
    show_dtypes: bool = True,
    check_duplicates: bool = True,
    show_duplicate_rows: bool = True,
    duplicate_subset: list[str] | None = None,
    max_duplicate_rows_shown: int = 20,
) -> pd.DataFrame:
    """
    Script générique de vérification de qualité d'un DataFrame pandas.
    
    Fonctionnalités :
    - Résumé par variable : nombre de lignes remplies / nulles (sans le détail des valeurs)
    - Signalement des colonnes obligatoires (non-nullables) qui contiennent des nulls
    - Détection des colonnes 100% vides, des doublons, et des types de données
    - Utilisable avec n'importe quel DataFrame et n'importe quelle liste de colonnes obligatoires
    
    Usage :
        from check_dataframe import check_dataframe
        summary, duplicate_rows = check_dataframe(df, required_columns=["id", "nom", "date"])
    
    Paramètres
    ----------
    df : pd.DataFrame
        Le DataFrame à analyser.
    required_columns : list[str], optionnel
        Liste des colonnes qui ne doivent JAMAIS contenir de valeurs nulles.
        Si l'une d'elles contient des nulls, une alerte est affichée.
    show_dtypes : bool
        Affiche le type de données de chaque colonne.
    check_duplicates : bool
        Vérifie la présence de lignes dupliquées.
    show_duplicate_rows : bool
        Si True, affiche le détail des lignes dupliquées (pas seulement le compte).
    duplicate_subset : list[str], optionnel
        Sous-ensemble de colonnes à utiliser pour détecter les doublons
        (par défaut : toutes les colonnes).
    max_duplicate_rows_shown : int
        Nombre maximum de lignes dupliquées affichées (pour éviter un flood console).
 
    Retour
    ------
    tuple[pd.DataFrame, pd.DataFrame | None]
        - summary : tableau récapitulatif (rempli / nul / % nul / obligatoire / dtype)
        - duplicate_rows : DataFrame contenant toutes les occurrences des lignes
          dupliquées (None si pas de doublons ou check_duplicates=False)
    """
    required_columns = required_columns or []
    n_rows = len(df)
 
    print("=" * 70)
    print(f"RAPPORT DE QUALITÉ DES DONNÉES — {n_rows} lignes, {df.shape[1]} colonnes")
    print("=" * 70)
 
    summary_rows = []
    alerts = []
 
    for col in df.columns:
        n_null = df[col].isna().sum()
        n_filled = n_rows - n_null
        pct_null = round(100 * n_null / n_rows, 2) if n_rows else 0.0
        is_required = col in required_columns
 
        summary_rows.append(
            {
                "colonne": col,
                "remplies": n_filled,
                "nulles": n_null,
                "% nul": pct_null,
                "obligatoire": is_required,
                "dtype": str(df[col].dtype) if show_dtypes else None,
            }
        )
 
        if is_required and n_null > 0:
            alerts.append(
                f"⚠️  '{col}' est déclarée OBLIGATOIRE mais contient {n_null} valeur(s) nulle(s) "
                f"({pct_null}%)."
            )
 
        if n_null == n_rows and n_rows > 0:
            alerts.append(f"⚠️  '{col}' est entièrement vide (100% de nulls).")
 
    summary = pd.DataFrame(summary_rows)
    if not show_dtypes:
        summary = summary.drop(columns=["dtype"])
 
    # Affichage du résumé par variable (remplies / nulles, sans détail des valeurs)
    print("\n--- Résumé par variable ---")
    print(summary.to_string(index=False))
 
    # Vérification des colonnes manquantes dans le DataFrame
    missing_cols = [c for c in required_columns if c not in df.columns]
    if missing_cols:
        alerts.append(f"⚠️  Colonnes obligatoires absentes du DataFrame : {missing_cols}")
 
    # Vérification des doublons
    duplicate_rows = None
    if check_duplicates:
        dup_mask = df.duplicated(subset=duplicate_subset, keep=False)
        n_dup = df.duplicated(subset=duplicate_subset).sum()  # nb de doublons "en trop"
 
        if n_dup > 0:
            subset_msg = f" (sur {duplicate_subset})" if duplicate_subset else ""
            alerts.append(f"⚠️  {n_dup} ligne(s) dupliquée(s) détectée(s){subset_msg}.")
 
            # Toutes les occurrences des lignes dupliquées (originales + doublons)
            duplicate_rows = df[dup_mask].sort_values(
                by=duplicate_subset if duplicate_subset else list(df.columns)
            )
 
            if show_duplicate_rows:
                print(f"\n--- Détail des lignes dupliquées{subset_msg} ---")
                n_shown = min(len(duplicate_rows), max_duplicate_rows_shown)
                print(duplicate_rows.head(max_duplicate_rows_shown).to_string())
                if len(duplicate_rows) > max_duplicate_rows_shown:
                    print(
                        f"... ({len(duplicate_rows) - n_shown} ligne(s) supplémentaire(s) "
                        f"non affichée(s), voir la valeur de retour 'duplicate_rows')"
                    )
 
    # Affichage des alertes
    print("\n--- Alertes ---")
    if alerts:
        for a in alerts:
            print(a)
    else:
        print("✅ Aucune anomalie détectée.")
 
    print("=" * 70)
 
    return summary, duplicate_rows


def convert_to_paris_date(column):
    return (
        pd.to_datetime(column, utc=True, errors="coerce")
        .dt.tz_convert("Europe/Paris")
    )


def rnsr_address_split(df):
    # -*- coding: utf-8 -*-
    """
    rnsr_address_split(df) : prend un DataFrame contenant une colonne 'adresse_full'
    (et optionnellement 'code_postal' / 'ville') et retourne un NOUVEAU DataFrame
    avec les colonnes ajoutées :
        - nom          : nom d'organisme / service détecté avant l'adresse (s'il y en a un)
        - adresse      : numéro + voie (rue, avenue, campus, bâtiment...)
        - code_postal  : code postal (FR en priorité, sinon meilleur repérage possible)
        - ville
        - pays         : uniquement s'il est explicitement mentionné dans le texte
        - a_verifier   : indique les lignes qu'il vaut mieux relire à la main

    Les données sources sont un texte libre très hétérogène (adresses françaises et
    étrangères, formats variés, doublons de segments...). La fonction applique donc des
    règles heuristiques et NON une analyse garantie à 100%.

    Utilisation :
        rnsr = rnsr_address_split(rnsr)
    """
    from paths import PATH_WORK
    import re
    import os
    import json
    import unicodedata
    import pandas as pd

    # --- Dictionnaire d'abréviations d'adresse (ad.json) ---------------------------
    # cherche ad.json à côté du script, puis dans PATH_WORK ; à défaut utilise une
    # copie intégrée (le fichier reste éditable indépendamment du script).
    _AD_JSON_FALLBACK = {
        "chem": "chemin", "che": "chemin", "av": "avenue", "adj": "adjudant",
        "bat": "batiment", "bd": "boulevard", "bld": "boulevard", "bvd": "boulevard",
        "blvd": "boulevard", "bloulevard": "boulevard", "cdt": "commandant",
        "all": "allee", "al": "allee", "imp": "impasse", "dr": "docteur",
        "g": "general", "gal": "general", "gl": "general", "prof": "professeur",
        "rgt": "regiment", "rte": "route", "st": "saint", "pl": "place",
        "ld": "lieudit", "lieu": "lieudit", "ltd": "lieutenant",
        "zi": "zoneindustrielle", "fbg": "faubourg", "za": "zoneactivite",
    }

    def _load_ad_dict():
        candidats = []
        try:
            script_dir = os.path.dirname(os.path.abspath(__file__))
            candidats.append(os.path.join(script_dir, "data_files", "ad.json"))
            candidats.append(os.path.join(script_dir, "ad.json"))
        except NameError:
            pass
        candidats.append(os.path.join(PATH_WORK, "data_files", "ad.json"))
        candidats.append(os.path.join(PATH_WORK, "ad.json"))
        candidats.append(os.path.join("data_files", "ad.json"))
        candidats.append("ad.json")
        for chemin in candidats:
            try:
                with open(chemin, encoding="utf-8") as f:
                    return json.load(f)
            except (FileNotFoundError, OSError):
                continue
        return _AD_JSON_FALLBACK

    AD_DICT = _load_ad_dict()
    # une regex par abréviation : mot entier, point final optionnel, insensible à la casse
    AD_PATTERNS = [
        (re.compile(r"\b" + re.escape(k) + r"\.?\b", re.IGNORECASE), v)
        for k, v in AD_DICT.items()
    ]

    def _expand_abbreviations(text: str) -> str:
        """Remplace les abréviations d'adresse (ad.json) par leur forme complète
        (ex: 'bd' -> 'boulevard', 'st' -> 'saint')."""
        if not text:
            return text
        for pattern, full in AD_PATTERNS:
            text = pattern.sub(full, text)
        return text

    # supprime le numéro de voie (+ bis/ter/quater) : ne garde que la voie et son nom,
    # à partir du premier mot-clé de type de voie rencontré
    NUMERO_STRIP_RE = re.compile(
        r"^.*?\b(rue|avenue|boulevard|impasse|chemin|all[ée]e|place|route|cours|quai|esplanade|parvis)\b\s*",
        re.IGNORECASE,
    )

    # --- Référentiels heuristiques -------------------------------------------------

    # mots-clés "forts" : types de voie non ambigus, où l'on peut tolérer un tiret
    # comme séparateur avant le numéro (ex: '118 - route de Narbonne')
    STREET_WORDS_STRONG = (
        r"rue|avenue|av\.?|bd\.?|boulevard|chemin|route|place|all[ée]e|impasse|quai|"
        r"cours|esplanade|espl\.?|rond[- ]point|faubourg|voie|square|passage|"
        r"promenade|drive|road|street|blvd|parvis"
    )
    # mots-clés "faibles" : descriptifs de site, ambigus avec un numéro d'université/UFR
    # (ex: 'UNIVERSITE LYON 2 - Campus...') -> seule la virgule est tolérée comme séparateur
    STREET_WORDS_WEAK = (
        r"domaine|campus|faubourg|zone|residence|r[ée]sidence|lotissement|hameau|"
        r"lieu[- ]dit|parc"
    )
    STREET_WORDS = STREET_WORDS_STRONG + "|" + STREET_WORDS_WEAK

    STREET_KEYWORDS = re.compile(r"\b(" + STREET_WORDS + r")\b", re.IGNORECASE)

    # motif "numéro (éventuellement en plage type '9 - 11') + voie" : tiret ou virgule
    # tolérés comme séparateur, réservé aux types de voie non ambigus
    NUMERO_VOIE = re.compile(
        r"\b\d{1,4}(?:\s*-\s*\d{1,4})?(?:\s*(?:bis|ter|quater))?\s*(?:,|-)?\s*(?:"
        + STREET_WORDS_STRONG
        + r")\b",
        re.IGNORECASE,
    )
    # même motif, mots-clés faibles : uniquement une virgule (ou rien) comme séparateur,
    # pour éviter de confondre 'LYON 2 - Campus' avec un numéro de voie
    NUMERO_VOIE_WEAK = re.compile(
        r"\b\d{1,4}(?:\s*-\s*\d{1,4})?(?:\s*(?:bis|ter|quater))?\s*,?\s*(?:"
        + STREET_WORDS_WEAK
        + r")\b",
        re.IGNORECASE,
    )

    # organismes fréquents -> indique probablement un "nom" plutôt qu'une "adresse"
    ORG_KEYWORDS = re.compile(
        r"\b("
        r"cnrs|inserm|inra|inrae|chu|chr|aphm|aphp|universit[ée]|institut|facult[ée]|"
        r"laboratoire|hopital|h[ôo]pital|centre|ecole|[ée]cole|umr|ur\d|upr|dgos|"
        r"association|fondation|groupement|hospices|clinique|ufr|umr\d|service|technopole|"
        r"technop[ôo]le"
        r")\b",
        re.IGNORECASE,
    )

    COUNTRIES = [
        "france", "allemagne", "etats-unis", "états-unis", "usa", "canada", "japon",
        "japan", "chine", "singapour", "singapore", "australie", "royaume-uni",
        "pays-bas", "chili", "mexique", "bresil", "brésil", "senegal", "sénégal",
        "koweit", "koweït", "liban", "russie", "italie", "suisse", "belgique",
        "espagne", "autriche", "inde", "thailande", "thaïlande", "hong-kong",
        "hong kong", "coree du sud", "corée du sud", "coree", "corée",
        "nouvelle-zelande", "nouvelle-zélande", "argentine", "perou", "pérou",
        "polynesie francaise", "polynésie française",
    ]

    def _strip_accents(s: str) -> str:
        return "".join(
            c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c)
        )

    def _extract_pays(text: str):
        """Cherche un nom de pays explicite en fin de chaîne ou dans un segment isolé."""
        segments = re.split(r",| - ", text)
        for seg in reversed(segments):
            seg_clean = _strip_accents(seg).strip(" .-").lower()
            for token in re.split(r"\s*-\s*", seg_clean):
                token = token.strip()
                if token in COUNTRIES:
                    pays_original = seg.strip(" .-")
                    for sub in re.split(r"(\s*-\s*)", seg):
                        if _strip_accents(sub).strip(" .-").lower() == token:
                            pays_original = sub.strip(" .-")
                    new_text = text.replace(seg, "", 1)
                    return pays_original.title(), new_text
        return "", text

    BATIMENT_TOKEN = re.compile(r"^b[âa]t(?:iment)?\.?\s", re.IGNORECASE)
    IMMEUBLE_TOKEN = re.compile(r"^immeuble\.?\s", re.IGNORECASE)

    # 'bat'/'bâtiment'/'batiment' suivi d'un code contenant un chiffre (ex: 'Bat 3R1 B2',
    # 'Bâtiment 3'), même accolé au milieu d'un segment sans virgule/tiret ('...Sabatier Bat 3R1')
    BAT_DIGIT_INLINE = re.compile(
        r"\bb[âa]t(?:iment)?\.?\s*(?:[A-Za-z]*\d[A-Za-z0-9]*)(?:\s+[A-Za-z]*\d[A-Za-z0-9]*)*",
        re.IGNORECASE,
    )

    # 'immeuble' + nom propre qui suit (ex: 'Immeuble Deurbroucq'), même collé sans
    # espace après un tiret ('Ouest-Immeuble Deurbroucq')
    IMMEUBLE_INLINE = re.compile(
        r"\bimmeuble\.?\s*(?:[A-ZÀ-Ÿ][A-Za-zÀ-ÿ'\-]*)(?:\s+[A-ZÀ-Ÿ][A-Za-zÀ-ÿ'\-]*)*",
        re.IGNORECASE,
    )

    # étage : 'X étage', '3ème étage', 'RDC'... suivi ou non d'un numéro
    ETAGE_INLINE = re.compile(
        r"\b\d+\s*(?:er|ère|re|nd|eme|ème|e)?\s*(?:etage|étage)\b|\bau\s+\d+\s*(?:er|ère|re|nd|eme|ème|e)?\s*(?:etage|étage)\b",
        re.IGNORECASE,
    )

    # 'case' + numéro (ex: 'case 925', 'case postale 12') — pas un numéro de rue
    CASE_INLINE = re.compile(
        r"\bcase(?:\s+postale)?\.?\s*n?°?\s*\d+\b", re.IGNORECASE
    )

    # boîtes/services postaux : BP (boîte postale), CS (courrier suivi / case postale),
    # TSA (tri sélectif de l'acheminement), CE (case entreprise) — toujours suivis d'un
    # numéro de service, ex: 'B.P. 53', 'CS 90032', 'TSA 51274', 'CE 1455'.
    # Ce ne sont pas des numéros de voie.
    POSTAL_BOX_TOKEN = re.compile(
        r"^(?:b\.?\s?p\.?|c\.?\s?s\.?|t\.?\s?s\.?\s?a\.?|c\.?\s?e\.?)\s*n?°?\s*\d",
        re.IGNORECASE,
    )
    POSTAL_BOX_INLINE = re.compile(
        r"\b(?:b\.?\s?p\.?|c\.?\s?s\.?|t\.?\s?s\.?\s?a\.?|c\.?\s?e\.?)\s*n?°?\s*\d[\d\s]*\b",
        re.IGNORECASE,
    )

    def _strip_noise_tokens(before: str) -> str:
        """Retire les mentions parasites qui ne font pas partie du numéro de voie :
        - bâtiment (avec ou sans numéro), immeuble (+ nom propre) ;
        - boîte/service postal : BP, CS, TSA, CE + numéro ;
        - étage (ex: '3ème étage') ;
        - case / case postale + numéro.
        Traite aussi bien les segments entiers que les mentions accolées sans
        ponctuation à l'intérieur d'un segment."""
        before = BAT_DIGIT_INLINE.sub("", before)
        before = IMMEUBLE_INLINE.sub("", before)
        before = ETAGE_INLINE.sub("", before)
        before = CASE_INLINE.sub("", before)
        before = POSTAL_BOX_INLINE.sub("", before)
        before = re.sub(r"\s{2,}", " ", before)
        tokens = [t.strip() for t in re.split(r",| - ", before) if t.strip()]
        tokens = [
            t for t in tokens
            if not BATIMENT_TOKEN.match(t + " ")
            and not IMMEUBLE_TOKEN.match(t + " ")
            and not POSTAL_BOX_TOKEN.match(t)
        ]
        return " - ".join(tokens)

    def _dedupe_tokens(tokens):
        """Supprime les segments dupliqués/quasi dupliqués consécutifs
        (ex: '20 rue X, rue X')."""
        cleaned = []
        for t in tokens:
            t_norm = _strip_accents(t).lower().strip()
            if cleaned:
                prev_norm = _strip_accents(cleaned[-1]).lower().strip()
                if t_norm and (t_norm in prev_norm or prev_norm in t_norm):
                    if len(t) > len(cleaned[-1]):
                        cleaned[-1] = t
                    continue
            cleaned.append(t)
        return cleaned

    def _split_nom_adresse(before: str):
        """Sépare (nom d'organisme, adresse numéro+voie) à partir du texte
        précédant le code postal / la ville.

        La coupure est cherchée directement sur le texte (pas seulement au
        début d'un segment séparé par une virgule), pour gérer les cas où
        le nom d'organisme et le numéro de voie sont accolés sans ponctuation
        (ex : "Laboratoire de Ploufragan-Plouzané-Niort 41 rue de Beaucemaine").
        """
        before = _strip_noise_tokens(before)
        tokens = [t.strip() for t in re.split(r",| - ", before) if t.strip()]
        tokens = _dedupe_tokens(tokens)
        clean_before = " - ".join(tokens).strip(" -,")

        if not clean_before:
            return "", ""

        # 1) motif "numéro + voie" (types de voie non ambigus : rue, avenue...)
        m = NUMERO_VOIE.search(clean_before)
        # 1bis) motif "numéro + voie" faible (campus, domaine...) : virgule seulement,
        # pour ne pas confondre avec un numéro d'université/UFR suivi d'un tiret
        if not m:
            m = NUMERO_VOIE_WEAK.search(clean_before)
        if m and m.start() > 0:
            nom = clean_before[: m.start()].strip(" -,.")
            adresse = clean_before[m.start():].strip(" -,.")
            return nom, adresse
        if m and m.start() == 0:
            # l'adresse commence dès le début, pas de nom devant
            return "", clean_before.strip(" -,.")

        # 2) pas de numéro trouvé : mot-clé de voie seul (ex: "Place du Maréchal...")
        m2 = STREET_KEYWORDS.search(clean_before)
        if m2 and m2.start() > 0:
            nom = clean_before[: m2.start()].strip(" -,.")
            adresse = clean_before[m2.start():].strip(" -,.")
            return nom, adresse
        if m2 and m2.start() == 0:
            return "", clean_before.strip(" -,.")

        # 3) aucun repère de voie : si le début ressemble à un organisme connu,
        # on le met en 'nom' et on laisse le reste en 'adresse' (sans certitude)
        if tokens and ORG_KEYWORDS.search(tokens[0]):
            nom = tokens[0]
            adresse = " - ".join(tokens[1:]).strip(" -,.")
        else:
            nom = ""
            adresse = clean_before

        return nom, adresse

    def _parse_adresse(raw: str):
        result = {
            "nom": "",
            "adresse": "",
            "code_postal": "",
            "ville": "",
            "pays": "",
            "a_verifier": "",
        }
        text = (raw or "").strip()
        if not text:
            result["a_verifier"] = "oui (adresse_full vide)"
            return result

        # retire les parenthèses et leur contenu (ex: '(contact : ... email@...)')
        text = re.sub(r"\([^)]*\)", "", text).strip()
        text = re.sub(r"\s{2,}", " ", text)

        pays, text = _extract_pays(text)
        result["pays"] = pays
        text = re.sub(r"\s*,\s*,", ",", text)
        text = text.strip(" ,-")

        # Cas standard : "..., CODE_POSTAL, VILLE" en fin de chaîne
        m = re.search(r",\s*(\d{5})\s*,\s*([^,]+?)\s*$", text)
        if m:
            before = text[: m.start()]
            result["code_postal"] = m.group(1)
            result["ville"] = m.group(2).strip(" .-")
            nom, adresse = _split_nom_adresse(before)
            result["nom"], result["adresse"] = nom, adresse
            return result

        # Cas "CODE_POSTAL, VILLE" seul (pas de partie avant)
        m = re.search(r"^(\d{5})\s*,\s*([^,]+)$", text)
        if m:
            result["code_postal"] = m.group(1)
            result["ville"] = m.group(2).strip(" .-")
            return result

        # Cas "CODE_POSTAL VILLE" collés en fin de chaîne (pas de virgule)
        m = re.search(r"(?:^|[,\-\s])(\d{5})\s+([A-ZÀ-Ÿ][A-Za-zÀ-ÿ' \-]{2,})$", text)
        if m:
            before = text[: m.start(1)]
            result["code_postal"] = m.group(1)
            ville_brute = m.group(2).strip(" .-")
            # la ville et le pays peuvent être accolés sans virgule (ex: "Avignon France")
            mots = ville_brute.split()
            if len(mots) > 1:
                for k in range(len(mots) - 1, 0, -1):
                    candidat = " ".join(mots[k:])
                    if _strip_accents(candidat).strip(" .-").lower() in COUNTRIES:
                        if not result["pays"]:
                            result["pays"] = candidat.title()
                        ville_brute = " ".join(mots[:k])
                        break
            result["ville"] = ville_brute.strip(" .-")
            nom, adresse = _split_nom_adresse(before)
            result["nom"], result["adresse"] = nom, adresse
            result["a_verifier"] = "oui (format cp/ville atypique)"
            return result

        # Aucun code postal identifié : on tente au moins nom/adresse,
        # et on marque la ligne à vérifier manuellement.
        nom, adresse = _split_nom_adresse(text)
        result["nom"], result["adresse"] = nom, adresse
        result["a_verifier"] = "oui (pas de code postal identifié)"
        return result

    def _safe_str(value):
        """Convertit une valeur de cellule (potentiellement NaN) en chaîne propre."""
        if value is None:
            return ""
        try:
            if pd.isna(value):
                return ""
        except (TypeError, ValueError):
            pass
        return str(value).strip()

    def _normaliser(text: str) -> str:
        """Passe en minuscule et retire la ponctuation (garde lettres/chiffres/accents
        et espaces), en conservant un seul espace entre les mots."""
        if not text:
            return text
        text = re.sub(r"[^\w\s]", " ", text, flags=re.UNICODE)
        text = re.sub(r"\s{2,}", " ", text)
        return text.strip().lower()

    nom_col, adresse_col, cp_col, ville_col, pays_col, verif_col = [], [], [], [], [], []

    for _, row in df.iterrows():
        parsed = _parse_adresse(_safe_str(row.get("adresse_full", "")))
        code_postal = parsed["code_postal"] or _safe_str(row.get("code_postal", ""))
        ville = parsed["ville"] or _safe_str(row.get("ville", ""))

        nom_col.append(_normaliser(parsed["nom"]))
        adresse_expansee = _expand_abbreviations(parsed["adresse"])
        adresse_sans_numero = NUMERO_STRIP_RE.sub(r"\1 ", adresse_expansee)
        adresse_col.append(_normaliser(adresse_sans_numero))
        cp_col.append(code_postal)
        ville_col.append(_normaliser(ville))
        pays_col.append(_normaliser(parsed["pays"]))
        verif_col.append(parsed["a_verifier"])

    out = df.copy()
    out["nom"] = nom_col
    out["adresse"] = adresse_col
    out["code_postal"] = cp_col
    out["ville"] = ville_col
    out["pays"] = pays_col
    out["a_verifier"] = verif_col

    n_verif = sum(1 for v in verif_col if v)
    print(f"{len(out)} lignes traitées, {n_verif} marquées 'a_verifier'")

    out.to_csv(f"{PATH_WORK}rnsr_address_splite.csv")

    return out