import geonamescache, pgeocode, pandas as pd, re, ast
from unidecode import unidecode
from thefuzz import fuzz
from functions_shared import work_csv
from remote_process.localisation_api import fr_geocode
from remote_process.grist import geoG, communesG, add_records_to_grist, update_doc_grist
from config_url import grist_url
from config_path import PATH_HARVEST
# ============================================================
# INITIALISATION UNE SEULE FOIS (hors fonction)
# ============================================================

# Cache pgeocode par pays
_nominatim_cache = {}

def get_nominatim(country_code):
    if country_code not in _nominatim_cache:
        try:
            _nominatim_cache[country_code] = pgeocode.Nominatim(country_code)
        except Exception:
            _nominatim_cache[country_code] = None
    return _nominatim_cache[country_code]

# Index geonamescache
gc = geonamescache.GeonamesCache()
name_index = {}

for c in gc.get_cities().values():
    alt_names = c.get('alternatenames', [])
    if isinstance(alt_names, str):
        alt_names = alt_names.split(',')
    for alt in alt_names:
        alt = alt.strip()
        if alt:
            key = (alt.lower(), c['countrycode'])
            if key not in name_index:
                name_index[key] = c['name']

for c in gc.get_cities().values():
    key = (c['name'].lower(), c['countrycode'])
    name_index[key] = c['name']

# ============================================================
# FONCTION (utilise les variables globales)
# ============================================================

def normalize_city(row):
    city        = row['city']
    country     = row['ISO_3166_2']
    postal_code = row['postalCode']

    if not isinstance(city, str) or not isinstance(country, str):
        return pd.Series({'city_clean': None, 'city_matched': False, 'city_match_source': None})

    city       = city.strip()
    country    = country.strip()
    city_ascii = re.sub(r"\s{2,}", " ", unidecode(city)).strip().casefold()

    # 1. Lookup via code postal
    postal_city = None
    best_match  = None
    best_score  = 0

    if isinstance(postal_code, str) and postal_code.strip():
        nomi = get_nominatim(country)
        if nomi is not None:
            try:
                postal_clean = re.sub(r'\s+', '', postal_code.strip())
                result = nomi.query_postal_code(postal_clean)
                if result is not None and pd.notna(result.get('place_name')):
                    postal_city = result['place_name']
            except Exception:
                pass

    # 2. Si code postal trouvé → splitter et chercher le meilleur score
    if postal_city is not None:
        candidates = [c.strip() for c in re.split(r'[,;]', postal_city) if c.strip()]

        for candidate in candidates:
            candidate_ascii = unidecode(candidate).lower()
            score = fuzz.token_sort_ratio(city_ascii, candidate_ascii)

            if city_ascii in candidate_ascii or candidate_ascii in city_ascii:
                score = max(score, 90)

            if score > best_score:
                best_score = score
                best_match = candidate

        if best_score >= 90:
            return pd.Series({
                'city_clean':        best_match,
                'city_matched':      True,
                'city_match_source': f'postal+city (score={best_score})'
            })
        elif best_score >= 70:
            return pd.Series({
                'city_clean':        best_match,
                'city_matched':      True,
                'city_match_source': f'postal+fuzzy (score={best_score})'
            })
        else:
            return pd.Series({
                'city_clean':        None,
                'city_matched':      False,
                'city_match_source': f'postal_city_mismatch (score={best_score}) postal→{best_match}'
            })

    # 3. Lookup geonames exact
    key = (city.lower(), country)
    if key in name_index:
        return pd.Series({
            'city_clean':        name_index[key],
            'city_matched':      True,
            'city_match_source': 'geonames_exact'
        })

    # 4. Lookup geonames avec unidecode
    key_ascii = (city_ascii, country)
    if key_ascii in name_index:
        return pd.Series({
            'city_clean':        name_index[key_ascii],
            'city_matched':      True,
            'city_match_source': 'geonames_ascii'
        })

    # 5. Aucun résultat fiable
    return pd.Series({
        'city_clean':        None,
        'city_matched':      False,
        'city_match_source': 'no_match'
    })


def geoloc_init_clean_by_country(tmp):
    
    tmp.loc[(tmp['city'].str.contains(',', na=False)), 'city'] = tmp.loc[(tmp['city'].str.contains(',', na=False)), 'city'].str.split(',').str[0]


    # USA
    tmp.loc[tmp.ISO_3166_2=='US', 'postalCode'] = tmp.loc[tmp.ISO_3166_2=='US', 'postalCode'].str.lower().str.replace('[a-z#,\.]+', '', regex=True).str.strip()
    tmp.loc[tmp.ISO_3166_2=='US', 'postalCode'] = tmp.loc[tmp.ISO_3166_2=='US', 'postalCode'].str.split('[-\s]+').str[0]
    tmp.loc[(tmp.ISO_3166_2=='US')&(tmp['postalCode'].str.len()>5), 'postalCode'] = tmp.loc[(tmp.ISO_3166_2=='US')&(tmp['postalCode'].str.len()>5), 'postalCode'].str[:5] 
    tmp.loc[(tmp.ISO_3166_2=='US') & (tmp['postalCode']==''), 'postalCode'] = (
    tmp.loc[(tmp.ISO_3166_2=='US') & (tmp['postalCode']==''), 'city'].str.extract(r'(\d{5})', expand=False))

    #BRESIL
    tmp.loc[(tmp.ISO_3166_2=='BR') & (tmp['postalCode']=='28030-130'), 'city'] = 'campos dos goytacazes'



    # CANADA
    corrections = {
    'k1a oc5': 'ottawa'}
    mask = (
    (tmp.ISO_3166_2 == 'CA') & 
    (tmp['postalCode_source'].notnull()) & 
    (tmp['postalCode_source'].isin(corrections))
    )
    tmp.loc[mask, 'city'] = tmp.loc[mask, 'postalCode_source'].map(corrections)

    tmp.loc[(tmp.ISO_3166_2=='CA')&(tmp['postalCode'].notnull()), 'postalCode'] = (
        tmp.loc[(tmp.ISO_3166_2=='CA')&(tmp['postalCode'].notnull()), 'postalCode'].astype(str).str.split(' ').str[0]
    )

    tmp.loc[(tmp.ISO_3166_2=='CA')&(tmp['postalCode'].notnull())&(tmp['postalCode'].str.len()>3), 'postalCode'] = (
        tmp.loc[(tmp.ISO_3166_2=='CA')&(tmp['postalCode'].notnull())&(tmp['postalCode'].str.len()>3), 'postalCode'].str[:3])
    
    tmp.loc[(tmp.ISO_3166_2=='CA') & (tmp['city']=='montreal') & (~tmp['postalCode'].str[0].str.lower().eq('h')), 'postalCode'] = 'h1b' 
    corrections = {
    r'\bk11p\b': 'k1p',
    r'\baic\b': 'a1c'
    }

    for pattern, replacement in corrections.items():
        tmp.loc[(tmp.ISO_3166_2=='CA'), 'postalCode'] = tmp.loc[(tmp.ISO_3166_2=='CA'), 'postalCode'].str.replace(pattern, replacement, regex=True)


    # SUISSE
    corrections = {
        '80005':'8005'
    }
    for pattern, replacement in corrections.items():
        tmp.loc[(tmp.ISO_3166_2=='CH'), 'postalCode'] = tmp.loc[(tmp.ISO_3166_2=='CH'), 'postalCode'].str.replace(pattern, replacement, regex=True)

    tmp.loc[(tmp.ISO_3166_2=='CH') & (tmp['postalCode']=='none'), 'postalCode'] = (
    tmp.loc[(tmp.ISO_3166_2=='CH') & (tmp['postalCode']=='none'), 'city']
    .str.extract(r'(\d{4})', expand=False)
    )
    tmp.loc[(tmp.ISO_3166_2=='CH') & (tmp['postalCode'].str.len()>4), 'postalCode'] = (
    tmp.loc[(tmp.ISO_3166_2=='CH') & (tmp['postalCode'].str.len()>4), 'postalCode']
    .str.extract(r'(\d{4})', expand=False)
    )


    # CHINA
    tmp.loc[tmp.ISO_3166_2=='CN', 'city'] = tmp.loc[tmp.ISO_3166_2=='CN', 'city'].str.lower().str.replace('[、]+', ' ', regex=True).str.strip()
    tmp.loc[tmp.ISO_3166_2=='CN', 'postalCode'] = tmp.loc[tmp.ISO_3166_2=='CN', 'postalCode'].str.lower().str.replace('[a-z\.]+', '', regex=True).str.strip()
    tmp.loc[(tmp.ISO_3166_2=='CN')&(tmp['postalCode'].str.len()==6), 'postalCode'] = tmp.loc[(tmp.ISO_3166_2=='CN')&(tmp['postalCode'].str.len()==6), 'postalCode'].str[0:3] + '000'
    tmp.loc[(tmp.ISO_3166_2=='CN')&(tmp['postalCode']=='1700')&(tmp['city']=='sichuan'), 'postalCode'] = '610000'
    tmp.loc[(tmp.ISO_3166_2=='HK')|(tmp['city'].str.contains('hong( ?)kong', regex=True)), 'nutsCode'] = 'HK'
    tmp.loc[(tmp.ISO_3166_2=='CN') & (tmp['postalCode']=='571737'), 'city'] = 'danzhou'
    tmp.loc[(tmp.ISO_3166_2=='CN') & (tmp['postalCode']=='265600'), 'city'] = 'penglai'
    tmp.loc[(tmp.ISO_3166_2=='CN') & (tmp['postalCode']=='312500'), 'city'] = 'xinchang'


    # IRELAND
    tmp.loc[(tmp.ISO_3166_2=='IE')&(tmp['postalCode'].notnull()), 'postalCode'] = tmp.loc[(tmp.ISO_3166_2=='IE')&(tmp['postalCode'].notnull()), 'postalCode'].astype(str).str[0:3]
    
    mask = (tmp.ISO_3166_2=='IE') & (tmp['postalCode'].str.contains('^\\d+$', na=False, regex=True)) & (tmp['city'].str.contains('dublin', case=False, na=False))
    tmp.loc[mask, 'postalCode'] = tmp.loc[mask, 'postalCode'].apply(
        lambda x: 'd0' + x if len(x) == 1 else 'd' + x
    )

    # tmp.loc[(tmp.ISO_3166_2=='IE')&(tmp[city].str.contains('dublin', case=False, na=False)), 'nutsCode'] = 'IE061'


    # India
    tmp.loc[(tmp.ISO_3166_2=='IN')&(tmp['postalCode'].notnull()), 'postalCode'] = tmp.loc[(tmp.ISO_3166_2=='IN')&(tmp['postalCode'].notnull()), 'postalCode'].str.replace(' ', '')


    # GB
    tmp.loc[(tmp.ISO_3166_2=='GB')&(tmp['postalCode'].notnull()), 'postalCode'] = (
        tmp.loc[(tmp.ISO_3166_2=='GB')&(tmp['postalCode'].notnull()), 'postalCode'].str.split(' ').str[0])
    tmp.loc[(tmp.ISO_3166_2=='GB')&(tmp['postalCode'].str.len()>3), 'postalCode'] = (
        tmp.loc[(tmp.ISO_3166_2=='GB')&(tmp['postalCode'].str.len()>3), 'postalCode'].str[:4])
    
    tmp.loc[(tmp.ISO_3166_2=='GB')&(tmp['postalCode'].str.len()<2), 'postalCode'] = None

    tmp.loc[(tmp.ISO_3166_2=='GB')&(tmp['city'].str.contains('\bayr\b', regex=True)), 'city'] = 'ayr'     


    # lituania
    tmp.loc[(tmp.ISO_3166_2=='LT')&(tmp['postalCode'].notnull()), 'postalCode'] = tmp.loc[(tmp.ISO_3166_2=='LT')&(tmp['postalCode'].notnull()), 'postalCode'].str.replace(r'\D', '', regex=True)

    # maroc
    tmp.loc[(tmp.ISO_3166_2=='MA')&(tmp['postalCode']=='20100'), 'city'] = 'casablanca'

    # Moldova
    tmp.loc[(tmp.ISO_3166_2=='MD')&(tmp['postalCode'].notnull()), 'postalCode'] = tmp.loc[(tmp.ISO_3166_2=='MD')&(tmp['postalCode'].notnull()), 'postalCode'].str.replace(r'\D', '', regex=True)
    
    # Malawi
    tmp.loc[(tmp.ISO_3166_2=='MW')&(tmp['postalCode']=='265')&(tmp['city']=='malawi'), 'postalCode'] = '105200'

    
    # mexico
    tmp.loc[(tmp.ISO_3166_2=='MX')&(tmp['postalCode']=='06068'), 'postalCode'] = '06500'

    # new zealand
    tmp.loc[(tmp.ISO_3166_2=='NZ')&(tmp['postalCode']=='8150'), 'postalCode'] = '7647'

    # serbia
    # tmp.loc[(tmp.ISO_3166_2=='RS')&(tmp['postalCode']=='3318000'), 'nutsCode'] = 'RS225'

    # suede
    tmp.loc[(tmp.ISO_3166_2=='SE')&(tmp['postalCode'].notnull()), 'postalCode'] = tmp.loc[(tmp.ISO_3166_2=='SE')&(tmp['postalCode'].notnull()), 'postalCode'].str.replace(r'\D', '', regex=True)

    # turkey
    tmp.loc[(tmp.ISO_3166_2=='TR')&(tmp['postalCode']=='05440'), 'city'] = 'sakarya'

    # SPAIN
    tmp.loc[(tmp.ISO_3166_2=='ES')&(tmp['postalCode'].str.contains('tabernas')), 'postalCode'] = '04200'

    # japan
    # Case 1: has dash → keep last 3 chars before the dash
    mask_dash = (tmp.ISO_3166_2 == 'JP') & (tmp['postalCode'].str.contains('-', na=False))
    tmp.loc[mask_dash, 'postalCode'] = (
        tmp.loc[mask_dash, 'postalCode']
        .str.split('-').str[0]
        .str[-3:]
    )

    # Case 2: no dash → keep first 3 chars
    mask_no_dash = (tmp.ISO_3166_2 == 'JP') & (~tmp['postalCode'].str.contains('-', na=False))
    tmp.loc[mask_no_dash, 'postalCode'] = (
        tmp.loc[mask_no_dash, 'postalCode']
        .str.replace(' ', '', regex=False)
        .str[:3]
    )

    return tmp

def french_localisation(df):

    fix_fr=geoG['Fr_loc_to_comcode']

    df['postalCode'] = df['postalCode'].astype(str).str.replace(r'\D+', '', regex=True)
    df['cp_ville'] = (
        df[['postalCode', 'city']]
        .apply(lambda x: ' '.join(
            v.lower()
            for v in x.astype(str) 
            if v.strip().lower() not in ('nan', 'none', 'null', '')
        ), axis=1)
    )

    
    test = (df[['street', 'cp_ville', 'postalCode', 'city']]
            .drop_duplicates()
            .merge(fix_fr[['cp_ville', 'com_code', 'drop_loc']]
            .drop_duplicates(), how='left', on='cp_ville')
    )
    test = test.loc[(test['com_code'].isnull())&(test['drop_loc']!=True)].drop_duplicates()
    if not test.empty:
        final_df = fr_geocode(test)
        final_df = final_df.assign(be_checked=True)
        add_records_to_grist(final_df, grist_url, 'pcri', 'geo', 'Fr_loc_to_comcode')
        print(f"- ATTENTION ! check {len(final_df)} new records added to Grist for French localisation")
        update_doc_grist(geoG, 'geo')
        fix_fr = geoG['Fr_loc_to_comcode']

    df = (pd.merge(df, 
                    fix_fr.drop(columns=['score', 'drop_loc', 'match_step']).drop_duplicates(),
                    how='left', on='cp_ville')
                     )

    print(f"- size entities_tmp after merge with com_code: {len(df)}")
    return df


def geoloc_load_by_country(path_load, extension):
    import os
    pat = re.compile(r'^geo_foreign_[A-Z]{2}')
    files_list = [f for f in os.listdir(path_load) if pat.search(f) and f.endswith(f'.{extension}')]
    df = pd.DataFrame()
    for f in files_list:
        df = pd.concat([df,pd.read_pickle(f"{path_load}{f}")], ignore_index=True)
    return df

def safe_to_dict(x):
    if pd.isna(x):
        return {}
    if isinstance(x, dict):
        return x
    if isinstance(x, str):
        s = x.strip()
        if not s:
            return {}
        try:
            return ast.literal_eval(s)   # handles your single-quote dict strings
        except Exception:
            return {}
    return {}



def geoloc_foreign_back():

    geo = geoloc_load_by_country(f"{PATH_HARVEST}geoloc/by_countries/", 'pkl')
    print(f"- size geo foreigns {len(geo)}")


    loc_dict = geo["location"].apply(safe_to_dict)

    # 2) extract list under 'postalCodes'
    geo2 = geo.copy()
    geo2["postalCodes"] = loc_dict.apply(lambda d: d.get("postalCodes", []) if isinstance(d, dict) else [])
    # 3) explode -> one row per postalCodes match
    geo2 = geo2.explode("postalCodes", ignore_index=True)
    # 4) normalize the exploded dicts into columns
    pc_cols = pd.json_normalize(geo2["postalCodes"]).add_prefix("loc_")
    # 5) keep location + drop helper column + join normalized columns
    geo2 = pd.concat([geo2.drop(columns=["postalCodes"]), pc_cols], axis=1)

    if 'loc_placeName' in geo2.columns:

        geo2 = geo2.drop(columns=['location','loc_lng', 'loc_postalCode']).drop_duplicates()
        geo2['loc_placeName'] = geo2['loc_placeName'].apply(lambda x: re.sub(r"\s{2,}", " ", unidecode(x)).strip().casefold() if isinstance(x, str) else x)
            
        def match_condition(row):
            city = row['city_clean_lower']
            place = row['loc_placeName']
            
            # Si l'un des deux est NaN, pas de match
            if pd.isna(city) or pd.isna(place):
                return False
            
            city = str(city).strip()
            place = str(place).strip()
            
            return (city == place) or (city in place) or (place in city)

        # Masque booléen des lignes qui matchent
        mask = geo2.apply(match_condition, axis=1)

        # Création de la colonne concaténée uniquement si match, sinon NaN
        geo2['geo_admin_new'] = None
        geo2.loc[mask, 'geo_admin_new'] = (
            geo2.loc[mask, 'ISO_3166_2'].astype(str) + '-' + geo2.loc[mask, 'loc_ISO3166-2'].astype(str)
        )

        geo2 = (geo2[['ISO_3166_2', 'postalCode', 'city_clean_lower', 'loc_adminName1', 'geo_admin_new']]
                .drop_duplicates()
                .assign(drop_loc=True, be_checked=True)
        )

    else:
        geo2 = (geo2[['ISO_3166_2', 'postalCode', 'city_clean_lower']]
                .drop_duplicates()
                .assign(
                    geo_admin_new='',
                    drop_loc=True, 
                    be_checked=True,
                    loc_adminName1='')
        )


    if not geo2.empty:
        print(f"- ATTENTION ! check {len(geo2)} new records added to Grist for foreign localisation")
        add_records_to_grist(geo2, grist_url, 'pcri', 'geo', 'From_pcity_to_geo')
    else:
        print("- No new records to add to Grist for foreign localisation")
    
def geo_subdivision():
    from iso3166_2 import Subdivisions
    iso = Subdivisions()
    rows = []
    for country_code, subdivisions in iso.all.items():
        for subdiv_code, details in subdivisions.items():
            row = {'countryCode': country_code, 'subdivCode': subdiv_code}
            row.update(details)
            rows.append(row)

    return pd.DataFrame(rows).drop(columns=['flag'])