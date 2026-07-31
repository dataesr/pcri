import pandas as pd, numpy as np, re, json, os, threading
from datetime import datetime
from paths import PATH_HARVEST
from unidecode import unidecode
from functions_shared import clean_invisible_chars, work_csv, check_if_only_charact_special, clean_quotation_marks, create_archive_zip, trace_chain
from remote_process.localisation_api import geonames_api
from step3_entities.entities_localisation_clean import normalize_city, geoloc_init_clean_by_country, french_localisation, geoloc_foreign_back, geo_subdivision
from remote_process.grist import geoG, communesG, update_doc_grist

def entities_clean_name(df):

    # remove invible chars
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].apply(clean_invisible_chars)
    
    print("### ENTITIES cleaning name")
    # x=(df.loc[df.entities_name.isnull()]
    #                 .drop_duplicates())
    # print(f"- size df without entities_name: {len(x)}")
    if not df.empty:
        p=r'\\00'
        if any(df.loc[(df.legalName.str.contains(p, na=True))]):
            print(f"legalName contains car spec: {df.loc[(df.legalName.str.contains(p, na=True))].legalName}")

        y=df.loc[(df.businessName.str.contains("00",  na=True))]
        for i, row in y.iterrows():
            try:
                y.at[i, 'businessName'] = row.businessName.replace('\\', '\\u').encode().decode('unicode_escape')
            except:
                y.at[i, 'businessName'] = np.nan
        df = (df
            .merge(y[['generalPic', 'country_code_source']], 
            how='outer', on=['generalPic', 'country_code_source'], indicator=True)
            .query('_merge!="both"')
            .drop(columns='_merge'))
        df = pd.concat([df, y], ignore_index=True)

        df.loc[df.businessName.str.contains(r'^\\d+$', na=True), 'businessName'] = np.nan
        df.loc[df.legalName.str.lower()==df.businessName.str.lower(), 'businessName'] = np.nan

        # liste=['legalName', 'businessName']
        # for i in liste:
        df['legalName'] = df['legalName'].apply(lambda v: v.capitalize().strip() if isinstance(v, str) else v)
        # df['entities_acronym'] = df['businessName']

        print(f"- End size df without entities_name: {len(df)}")
        
        # df = (df
        #                 .merge(x[['generalPic', 'entities_id', 'country_code_source']], 
        #                        how='outer', on=['generalPic', 'entities_id', 'country_code_source'], indicator=True)
        #                 .query('_merge!="both"')
        #                 .drop(columns='_merge'))
        # df = pd.concat([df, x], ignore_index=True)
    
        print(f"- End size df: {len(df)}")
    return df

def maj_load_and_clean():
        """
        clean maj loaded
        - invisible character, special charater
        - add 'ND' for replace nan value and do link with entities_info
        """
        maj = geoG['From_pcity_to_geo'].drop_duplicates()
        mask = maj['geo_admin_new'].apply(lambda x: check_if_only_charact_special(x))
        maj.loc[mask, 'geo_admin_new'] = ''
        maj['geo_admin_new'] = maj['geo_admin_new'].apply(lambda x: clean_invisible_chars(x))
        
        for c in ['postalCode', 'city_clean_lower']:
            maj.loc[(maj[c].isnull())|(maj[c]==''), c] = 'ND'
        maj = maj.mask(maj=='')
        return maj.drop_duplicates()


def entities_clean_address(df):
    print("### ENTITIES cleaning address")

    for i in ['street', 'postalCode', 'city']:
        df[i] = df[i].apply(lambda x: '' if check_if_only_charact_special(x) else x)
        df[i] = df[i].apply(lambda x: clean_quotation_marks(x))
        df[i] = df[i].str.replace(r'^[0 ]+$', '', regex=True).str.strip()
        df[i] = df[i].fillna('').str.lower().str.replace(r'\s+', ' ', regex=True).str.strip()
        

    # df['postalCode_source'] = df['postalCode']

    NA_VALUES = ["", "\"", "\'", "NA", "N/A", "NULL", "null", "None", "none", "non", "nan", "NaN", "#N/A", "-", "n/a", "na",  "n a", "NAN", "Null", "NULL", "None", "none", "NaN", "not applicable", "no postal code", "not available", "not used", "xxxxx", "yemen has no postal"]
    df.loc[df['postalCode'].str.lower().isin(NA_VALUES), 'postalCode'] = ""
    
    stopwords = ["p o", "po box", "box", 'cedex', 'cédex', 'cdx']
    for col in ['postalCode', 'city']:
        df[col] = (
            df[col]
                .str.replace(
                    r'\b(?:{})\b\s*\d*'.format('|'.join(stopwords)),
                    '',
                    regex=True,
                    flags=re.IGNORECASE
                )
                .str.replace(r'\s+', ' ', regex=True)
                .str.strip()
            )


    df = df.assign(ISO_3166_2=df['countryCode'], postalCode_source=df['postalCode'])
    df.loc[df['countryCode']=='UK', 'ISO_3166_2'] = 'GB'
    df.loc[df['countryCode']=='EL', 'ISO_3166_2'] = 'GR'

    df = geoloc_init_clean_by_country(df)

    with open('data_files/country_city_clean.json', 'r') as f:
        city_mappings = json.load(f)

    for country, mappings in city_mappings.items():
        for pattern, replacement in mappings.items():
            mask = (df['ISO_3166_2'] == country) & (df['city'].str.contains(pattern, na=False))
            df.loc[mask, 'city'] = replacement


    corrections_pc = json.load(open("data_files/country_city_pc.json"))
    def find_correction(city, corrections):
        if isinstance(city, str):
            city = city.strip()
            for key, value in corrections.items():
                if key.strip() in city:
                    return value
        return None


    def apply_corrections(tmp, corrections_pc):
        for country_code, corrections in corrections_pc.items():
            mask = tmp['ISO_3166_2'] == country_code
            tmp.loc[mask, 'postalCode'] = (
                tmp.loc[mask].apply(
                    lambda row: find_correction(row['city'], corrections) or row['postalCode'], axis=1
                )
            )
        return tmp

    df = apply_corrections(df, corrections_pc)


    tmp = df[['country_code','postalCode_source', 'ISO_3166_2', 'postalCode', 'city', 'street']].drop_duplicates()
    print(f"- size data to match geo infos postalcode and city")
    tmp[['city_clean', 'city_matched', 'city_match_source']] = tmp.apply(normalize_city, axis=1)

    tmp.loc[tmp['city_clean'].isna(), 'city_clean'] = tmp.loc[tmp['city_clean'].isna(), 'city']
    tmp['city_clean_lower'] = tmp['city_clean'].str.strip().str.casefold()
    tmp['city_clean_lower'] = tmp['city_clean_lower'].apply(lambda x: re.sub(r"\s{2,}", " ", unidecode(x)).strip() if isinstance(x, str) else x)

    # france com_code
    tmpfr = tmp.loc[tmp['country_code']=='FRA']
    french_localisation(tmpfr)
    update_doc_grist(geoG, 'geo')

    fix_fr = geoG['Fr_loc_to_comcode']
    tmpfr = (pd.merge(tmpfr, 
                    fix_fr.drop(columns=['score', 'drop_loc', 'match_step']).drop_duplicates(),
                    how='left', on='cp_ville')
                     )
    print(f"- size entities_tmp after merge with com_code: {len(tmpfr)}")

    # foreign
    maj = maj_load_and_clean()

    check = maj.drop(columns='loc_adminName1').drop_duplicates().groupby(['postalCode', 'ISO_3166_2', 'city_clean_lower'], dropna=False).size().loc[lambda x: x > 1]
    if any(check):
        print(f"- ⚠️ ! duplicated rows in from_pcity_to_geo ; to bef fixed in grist:\n{check}")
    else:
        print('- ok no duplicated foreign rows')

    
    for c in ['postalCode', 'city_clean_lower']:
        tmp.loc[(tmp[c].isnull())|(tmp[c]==''), c] = 'ND'
    tmp = tmp.mask(tmp=='')
    
    tmp = (pd.merge(tmp, maj, 
                    how='left', 
                    on=['postalCode', 'city_clean_lower', 'ISO_3166_2'],
                    indicator=True)
    )
    
    tmp1 = (tmp.loc[(tmp['country_code']!='FRA')&
                   (tmp['_merge']=='left_only'),
                   ['country_code','ISO_3166_2', 'postalCode', 'city_clean_lower']]
                   .assign(location=None)
                   .drop_duplicates()
                   .copy()
    )

    # delete rows with postalCode+city == null
    tmp1 = tmp1.loc[~((tmp1['postalCode']=='ND')&(tmp1['city_clean_lower']=='ND'))]
    print(f"- size tmp1 foreigns to check {len(tmp1)}")
    print(tmp1)


    def run_geonames_background(tmp1):
        
        start_time = datetime.now()
        print(f"- début des requêtes geonames en arrière-plan : {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

        for cc in tmp1['ISO_3166_2'].unique():
            tmp_cc = tmp1.loc[tmp1['ISO_3166_2'] == cc]
            print(f"-- country {cc} with {len(tmp_cc)} rows to request")
            geonames_api(tmp_cc, cc)
        print("- background geonames requests terminées")

        end_time = datetime.now()
        duration = end_time - start_time
        print(f"- fin des requêtes geonames en arrière-plan : {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"- durée totale : {duration}")


    # IF need to get admin_reg for foreign cities req = True
    if not tmp1.empty:
        geo_dir = f'{PATH_HARVEST}geoloc/by_countries'
        create_archive_zip(geo_dir, 'geoloc_foreign', '.pkl')
        print("- ⚠️ : lancement des requêtes geonames api en arrière-plan (thread)")


        # récap du nombre de lignes à matcher par pays
        counts = tmp1['ISO_3166_2'].value_counts()
        print(f"- {len(counts)} pays à traiter, {len(tmp1)} lignes au total:")
        for cc, n in counts.items():
            print(f"  · {cc}: {n} lignes")

        thread = threading.Thread(target=run_geonames_background, args=(tmp1,))
        thread.start()
        # pas de .join() ici -> le script continue tout de suite

        # Vérifie si des résultats geonames existent déjà (run précédent terminé)
        pkl_files = [f for f in os.listdir(geo_dir) if f.startswith('geo_foreign_') and f.endswith('.pkl')]

        if pkl_files:
            print(f"- {len(pkl_files)} fichiers pkl trouvés, exécution de geoloc_foreign_back")
            geoloc_foreign_back()
            update_doc_grist(geoG, 'geo')
            maj = maj_load_and_clean()
        else:
            print("- pas de fichiers pkl disponibles pour le moment, geoloc_foreign_back sera exécuté au prochain run")

    ######################################
    # merged french cities with com_code
    df = (pd.merge(df, 
                   tmpfr[['country_code', 'city', 'postalCode_source', 'com_code']].drop_duplicates(), 
                   how='left', on=['country_code', 'city', 'postalCode_source'])
        )
    print(f"- size entities_tmp after merge with com_code: {len(df)}")

    # merge foreign
    maj = maj[['postalCode', 'city_clean_lower', 'ISO_3166_2', 'geo_admin_new', 'drop_loc']].drop_duplicates()
    tmp = (pd.merge(tmp[['postalCode_source', 'ISO_3166_2', 'postalCode', 'city', 'city_clean', 'city_clean_lower']].drop_duplicates(), 
                    maj, how='left', on=['postalCode', 'city_clean_lower', 'ISO_3166_2'])
        )
    print(f"- size tmp after merge with geocode: {len(tmp)}")

    for t in [tmp, df]:
        for c in ['postalCode', 'city']:
            t.loc[(t[c].isnull())|(t[c]==''), c] = 'ND'
        t = t.mask(t=='')

    df = pd.merge(df, 
                  tmp[['ISO_3166_2', 'city', 'postalCode', 'city_clean', 'geo_admin_new', 'drop_loc']].drop_duplicates(), 
                  how='left', on=['ISO_3166_2', 'city', 'postalCode'])

    print(f"- size entities_tmp after merge with geocode: {len(df)}")

    ########################################
    # merge ref for info
    com = communesG['Commune']
    com.columns = com.columns.str.casefold()
    com = (com[['com_code', 'com_nom', 'dep_code', 'reg_code', 'geoname_code']]
           .rename(columns={'geoname_code':'geo_unit_code'})
           .drop_duplicates()
           )
    df = pd.merge(df, com, how='left', on='com_code')
    df.loc[df['geo_unit_code'].isnull(), 'geo_unit_code'] = df.loc[df['geo_unit_code'].isnull(), 'geo_admin_new']
    df.loc[df['com_nom'].notna(), 'city_clean'] = df.loc[df['com_nom'].notna(), 'com_nom']

    ########
    # ajout des noms des subdivisions
    sub_div = geo_subdivision()
    sub_div['latlng'] = sub_div['latLng'].apply(lambda x: ','.join(f"{v:.4f}" for v in x))


    df = (pd.merge(df, 
                   sub_div[['subdivCode', 'name', 'latlng', 'parentCode']], 
                   how='left', left_on='geo_unit_code', right_on='subdivCode') 
    )
    df = (df.rename(columns={'name':'geo_unit_name', 'latlng':'geo_unit_latlng', 'parentCode':'geo_2_code'})
            .drop(columns='subdivCode')
            )
    
    df.loc[df['geo_2_code'].isnull(), 'geo_2_code'] = df.loc[df['geo_2_code'].isnull(), 'geo_unit_code']
    df.loc[df['geo_2_code'].isnull(), 'geo_2_code'] = df.loc[df['geo_2_code'].isnull(), 'ISO_3166_2']


    df = pd.merge(df, 
                  sub_div[['subdivCode', 'name', 'latlng']]
                  .rename(columns={'subdivCode':'geo_2_code', 'name':'geo_2_name', 'latlng':'geo_2_latlng'}), 
                   how='left', on='geo_2_code') 


    p=dict(zip(sub_div['subdivCode'], sub_div['parentCode']))
    sub_div['geo_3_code'] = sub_div['subdivCode'].apply(lambda x: trace_chain(x, p))

    df = pd.merge(df, 
                sub_div[['subdivCode', 'geo_3_code']]
                .rename(columns={'subdivCode':'geo_unit_code'}), 
                how='left', on='geo_unit_code') 
    
    df = pd.merge(df, 
                  sub_div[['subdivCode', 'name', 'latlng']]
                  .rename(columns={'subdivCode':'geo_3_code', 'name':'geo_3_name', 'latlng':'geo_3_latlng'}), 
                   how='left', on='geo_3_code') 

    print(f"- ended size entities_info : {len(df)}")

    return df.drop(columns=['drop_loc', 'geo_admin_new', 'ISO_3166_2', 'com_nom'])


def entities_add_country(df, countries):
    return(df
           .merge(countries[['countryCode_iso3', 'country_name_en', 'country_code']]
                  .drop_duplicates()
                  .rename(columns={'countryCode_iso3':'country_code_source'}), 
                  how='left', on='country_code_source')
           .rename(columns={'country_name_en':'country_name_source'})
    )


def entities_check_null(df):
    print("\n## check entities null")
    for i in ['entities_name', 'entities_id']:
        if len(df[df[i].isnull()])>0:
            print(f"{len(df[df[i].isnull()])} {i} manquants\n {df[df[i].isnull()]}") 

    test=df[['entities_id','entities_name', 'entities_acronym']].drop_duplicates()
    test['nb']=test.groupby(['entities_id','entities_name'], dropna=False)['entities_acronym'].transform('count')
    acro_to_delete=test[test.nb>1].entities_id.unique()
    if acro_to_delete.size>0:
        print(acro_to_delete)

    if any(test.entities_id.isnull())|any(test.entities_id=='nan'):
        print(f"{test.loc[(test.entities_id.isnull())|(test.entities_id=='nan')].to_dict('records')}")


def entities_info_add(entities_tmp, entities_info):
    print("\n### entities_info + entities_tmp")
        #ajout des infos country à participants_info
    # entities_info = (entities_info
    #                 .merge(countries[['countryCode_iso3', 'country_name_en', 'country_code']]
    #                        .rename(columns={'country_name_en':'country_name_source', 'countryCode_iso3':'country_code_source'}), 
    #                        how='left', on='country_code_source')
    #                 .drop_duplicates())
    cols_to_keep = [col for col in entities_tmp.columns if col not in entities_info.columns or col in ['generalPic', 'country_code_source']]
    tmp = entities_tmp[cols_to_keep].drop_duplicates()
    print(f"- size entities_tmp to add: {len(tmp)}")
    
    entities_info = (
        pd.merge(entities_info, tmp,
        how='left', on=['generalPic', 'country_code_source'])
        .drop_duplicates()
        )
    print(f"- size entities_info + entities_tmp: {len(entities_info)}")
    return entities_info


def add_countries_info(df, countries, framework=None):
    print("\n### entities_info + countries")
    #ajout des infos country à participants_info
    
    if framework==None:
        # correction des ecoles françaises à l'etranger
        l=['951736453','996825642','994591926','996825642','953002303', '998384626', '879924055']
        df.loc[df.generalPic.isin(l), 'country_code'] = 'FRA'
        cc = (countries.drop(columns=['countryCode', 'countryCode_parent', 'country_code'])
            .rename(columns={'countryCode_iso3':'country_code'})
            .drop_duplicates())
        df = (df.drop(columns='country_name_fr')
            .merge(cc, how='left', on='country_code')
            .rename(columns={'ZONAGE':'extra_joint_organization'})
            .drop_duplicates())

        df.drop(columns=df.columns[df.columns.str.contains('2020')], inplace=True)

    elif framework=='H20':
        cc=(countries[['countryCode_iso3', 'country_name_en',
        'country_association_code_2020', 'country_association_name_2020_en', 'country_group_association_code_2020',
        'country_group_association_name_2020_en', 'country_group_association_name_2020_fr', 'country_name_fr', 'article1',
        'article2']]
        .drop_duplicates()
        .rename(columns={'countryCode_iso3': 'country_code',
                            'country_association_code_2020':'country_association_code',
                            'country_association_name_2020_en':'country_association_name_en', 
                            'country_group_association_code_2020':'country_group_association_code',
                            'country_group_association_name_2020_en':'country_group_association_name_en',
                            'country_group_association_name_2020_fr':'country_group_association_name_fr'}))

        undef=pd.DataFrame(json.load(open('data_files/countries_undef.json', 'r+', encoding='UTF-8'))).drop(columns=['country_code_source', 'country_name_source'])
        cc=pd.concat([cc, undef], ignore_index=True)

        df = df.merge(cc, how='left', on='country_code')


    print(f"- longueur entities_info après ajout calculated_country : {len(df)}\n{df.columns}\n- 🔶 columns with Nan\n {df.columns[df.isnull().any()]}")
    return df
