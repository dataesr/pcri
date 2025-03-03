
def ref_externe_preparation():
    import pandas as pd, pycountry, re, json, numpy as np
    from text_to_num import alpha2digit

    # from IPython.display import HTML
    # from pathlib import Path
    from config_path import PATH, PATH_MATCH

    # from step7_referentiels.countries import ref_countries
    from functions_shared import work_csv, prep_str_col, stop_word, my_country_code
    from step8_referentiels.ror import ror_import, ror_prep
    from step8_referentiels.sirene import sirene_prep, sirene_refext
    from step8_referentiels.rnsr import rnsr_import, rnsr_prep
    from step8_referentiels.paysage import paysage_prep
    DUMP_PATH=f'{PATH}referentiel/'


    # pycountry.countries.add_entry(alpha_2="XK", alpha_3="XXK", name="Kosovo")
    # pycountry.countries.add_entry(alpha_2="UK", alpha_3="GBR", name="United Kingdom")
    # pycountry.countries.add_entry(alpha_2="EL", alpha_3="GRC", name="Greece")
    # tmp = [c.__dict__['_fields'] for c in list(pycountry.countries)]
    # countries = (pd.DataFrame(tmp)[['alpha_2', 'alpha_3', 'name']]
    #             .rename(columns={'alpha_2':'iso2', 'alpha_3':'iso3', 'name':'country_name_en'})
    #             .drop_duplicates()
    # )

    my_countries=my_country_code()
    print(len(my_countries))

    ROR_ZIPNAME = ror_import(DUMP_PATH)
    ror = ror_prep(DUMP_PATH, ROR_ZIPNAME, my_countries)

    ####
    sirene_refext(DUMP_PATH) # -> sirene_ref_moulinette.pkl
    sirene = sirene_prep(DUMP_PATH, my_countries)

    ### Extraction des données rnsr de dataESR
    rnsr_import(DUMP_PATH)
    rnsr = rnsr_prep(DUMP_PATH)

    work_csv(rnsr.loc[(rnsr.code_postal.isnull())|(rnsr.ville.isnull()), ['num_nat_struct', 'nom_long','adresse_full', 'code_postal', 'ville']].drop_duplicates(), 'rnsr_adresse_a_completer')
    add_ad = pd.read_csv(f"{DUMP_PATH}rnsr_adresse_manquante.csv",  sep=';', encoding='ANSI', dtype={'cp_corr':str})
    add_ad = add_ad[['num_nat_struct', 'cp_corr', 'city_corr', 'country_corr']].drop_duplicates()

    rnsr = rnsr.merge(add_ad, how='left', on='num_nat_struct')
    rnsr.loc[~rnsr.cp_corr.isnull(), 'code_postal'] = rnsr.cp_corr
    rnsr.loc[~rnsr.city_corr.isnull(), 'ville'] = rnsr.city_corr
    rnsr.loc[~rnsr.country_corr.isnull(), 'country_code_map'] = rnsr.country_corr

    ######
    # paysage
    paysage = paysage_prep(DUMP_PATH)

    ######
    # table all
    ref_all = pd.concat([ror, rnsr, sirene, paysage], ignore_index=True)
    ref_all = ref_all.drop(columns=['country.country_name', 'Lieudit_BP', 'COG', 'aliases','cp_corr','city_corr','country_corr'])
    ref_all.mask(ref_all=='', inplace=True)
    ref_all = ref_all.sort_values(['ref', 'num_nat_struct', 'siren', 'numero_paysage', 'numero_ror'])

    url='https://docs.google.com/spreadsheet/ccc?key=1FwPq5Qw7Gbgj_sBD6Za4dfDDk6ydozQ99TyRjLkW5d8&output=xls'
    df_geo = pd.read_excel(url, sheet_name='LES_COMMUNES', dtype=str, na_filter=False)
    ref_all = ref_all.merge(df_geo[['COM_CODE', 'ISO_3']], how='left', left_on='com_code', right_on='COM_CODE')
    ref_all.loc[~ref_all.ISO_3.isnull(), 'country_code_map'] = ref_all.ISO_3

    ref_all.loc[ref_all.country_code_map.isnull(), ['ref']].value_counts()

    #lowercase / exochar / unicode / punct
    ref_cols=['nom_long', 'sigle', 'ville', 'adresse', 'adresse_full']
    ref_all = prep_str_col(ref_all, ref_cols)

    y = ref_all.loc[(~ref_all.tel.isnull())&(ref_all.country_code_map=='FRA'), ['tel']]
    y['tel_clean']=y.tel.apply(lambda x:[re.sub(r'[^0-9]+', '', i) for i in x])
    y['tel_clean']=y.tel_clean.apply(lambda x: [re.sub(r'^(33|033)', '', i).rjust(10, '0') for i in x])
    y['tel_clean']=y.tel_clean.apply(lambda x: [i[0:10] if (len(i)>10) and (i[0:1]=='0') else i for i in x])
    y['tel_clean']=y.tel_clean.apply(lambda x:[re.sub(r'^0+$', '', i) for i in x])
    y['tel_clean']=y.tel_clean.apply(lambda x: ';'.join(set(x))).str.strip()

    ref_all = pd.concat([ref_all, y[['tel_clean']]], axis=1)

    tmp = ref_all.loc[(ref_all.nom_long!=ref_all.sigle)&(~ref_all.sigle.isnull()), ['nom_long',  'sigle']]
    tmp['nom_entier'] = [x1 if x2 in x1 else x1+' '+x2 for x1, x2 in zip(tmp['nom_long'], tmp['sigle'])]
    ref_all = pd.concat([ref_all, tmp[['nom_entier']]], axis=1)

    ref_all.loc[ref_all.nom_entier.isnull(), 'nom_entier'] = ref_all.nom_long

    #suppression des mots vides comme le la les et... pour "toutes les langues"
    stop_word(ref_all, 'country_code_map', ['nom_long', 'nom_entier', 'adresse', 'adresse_full'])


    # extraction du code postal du champs ville 
    ref_all.loc[ref_all.code_postal.isnull(), 'code_postal'] = ref_all.ville.str.extract(r"(\d+)")

    #traitement spécifique adresse_full du rnsr
    tmp = ref_all[~ref_all['adresse_full_2'].isnull()][['adresse_full_2']]
    tmp.adresse_full_2 = tmp.adresse_full_2.apply(lambda x: list(filter(None, x))).apply(lambda x: ' '.join(x))
    tmp[['cp_temp', 'ville_temp']] = tmp['adresse_full_2'].str.extract(r"\s(\d{5})\s?([a-z]+(?:\s?[a-z]+)*)", expand=True)

    def match(adr):
        x = re.search(r"(\b\d{1,4})\s([a-z]+\s?)+", adr)
        if x :
            return(x.group())
        
    tmp['adresse_temp'] = tmp['adresse_full_2'].apply(match)    
    # tmp[['test1', 'test2'] ]= tmp['adresse_full_2'].str.extract(r"(\b\d{1,4})\s([a-z]+\s?)+", expand=True)
    ref_all = pd.concat([ref_all, tmp.drop(columns='adresse_full_2')], axis=1)

    ref_all.loc[ref_all.code_postal.isnull(), 'code_postal'] = ref_all.cp_temp
    ref_all.loc[ref_all.ville.isnull(), 'ville'] = ref_all.ville_temp

    ref_all.loc[ref_all.adresse_2.isnull(), 'adresse_2'] = ref_all.adresse_temp
    ref_all.drop(columns=['cp_temp', 'ville_temp', 'adresse_temp'], inplace=True)

    # nettoyage de ville
    cedex="cedax|cedrex|cdexe|cdex|credex|cedex|cedx|cede|ceddex|cdx|cex|cexex|edex"
    ref_all['ville'] = ref_all.ville.str.replace('\\d+', ' ', regex=True).str.strip()
    ref_all.loc[(ref_all.country_code_map=='FRA')|(ref_all.iso2.isin(pays_fr)), 'ville'] = ref_all.ville.str.replace(cedex, ' ', regex=True).str.strip()
    ref_all.loc[(ref_all.country_code_map=='FRA')|(ref_all.iso2.isin(pays_fr)), 'ville'] = ref_all.ville.str.replace('^france$', '', regex=True).str.strip()
    ref_all.loc[(ref_all.country_code_map=='FRA')|(ref_all.iso2.isin(pays_fr)), 'ville'] = ref_all.ville.str.replace(r"\bst\b", 'saint', regex=True).str.strip()
    ref_all.loc[(ref_all.country_code_map=='FRA')|(ref_all.iso2.isin(pays_fr)), 'ville'] = ref_all.ville.str.replace(r"\bste\b", 'sainte', regex=True).str.strip()
    ref_all.loc[(ref_all.country_code_map=='FRA')|(ref_all.iso2.isin(pays_fr)), 'ville_tag'] = ref_all.loc[ref_all.country_code_map=='FRA', 'ville'].str.strip().str.replace(r'\s+', '-', regex=True)

    # code postal - > département
    mask=((ref_all.country_code_map=='FRA')|(ref_all.iso2.isin(pays_fr)))&(~ref_all.code_postal.isnull())&(ref_all.code_postal.str.len()!=5)
    if len(ref_all.loc[mask])>0:
        print(f"probleme avec le cp à corriger si possible à la source: {ref_all.loc[mask].code_postal.unique()}")
        print(f"structure avec cp mais à corriger: {ref_all.loc[mask&(ref_all.code_postal.str.contains(r"\d+")), ['ref','code_postal', 'ville', 'numero_paysage', 'num_nat_struct']]}")

    ref_all.loc[(ref_all.country_code_map=='FRA')&(~ref_all.code_postal.isnull()), 'code_postal'] = ref_all.loc[(ref_all.country_code_map=='FRA')&(~ref_all.code_postal.isnull()), 'code_postal'].str.replace(r"\D+", '', regex=True)
    ref_all.loc[~ref_all.code_postal.isnull(), 'departement'] = ref_all.code_postal.str[0:2]


    #########
    adr = json.load(open('data_files/ad.json'))

    for col_ref in ['adresse_2', 'adresse_full_2']:
        tmp = ref_all.loc[~ref_all[col_ref].isnull(), [col_ref]]
        for i in adr :
            for (k,v) in i.items():
                tmp[col_ref] = tmp[col_ref].apply(lambda x: [re.sub('^'+k+'$', v, s) for s in x])
                tmp[col_ref] = tmp[col_ref].apply(lambda x: list(filter(None, x)))
        
        ref_all = pd.concat([ref_all.drop(columns=col_ref), tmp], axis=1) 
        
        tmp[f'{col_ref}_tag'] = ref_all.loc[~ref_all[col_ref].isnull()][col_ref].apply(lambda x: ' '.join(x))
        with open("data_files/adresse_pattern.txt", "r") as pats:
            for n, line in enumerate(pats, start=1):       
                pat = line.rstrip('\n')
                tmp[f'{col_ref}_tag'] = tmp[f'{col_ref}_tag'].str.replace(pat,'', regex=True)
                
        tmp[f'{col_ref}_tag'] = tmp[f'{col_ref}_tag'].apply(lambda x: alpha2digit(x, 'fr'))
                
        ref_all = pd.concat([ref_all.drop(columns=col_ref), tmp], axis=1)

    ##################
    ref_all.rename(columns={'nom_long':'libelle1'}, inplace=True)
    ref_all['nom_entier_2'] = ref_all.nom_entier_2.apply(lambda x: ' '.join(x) if isinstance(x, list) else x)
    ref_all['etabs_rnsr'] = ref_all.etabs_rnsr.apply(lambda x: ';'.join(x) if isinstance(x, list) else np.nan)
    ref_all['email'] = ref_all.email.apply(lambda x: ';'.join(x) if isinstance(x, list) else np.nan)
    ref_all.loc[ref_all.adresse.isnull(), 'adresse'] = ref_all.adresse_full
    ref_all.loc[ref_all.adresse_2_tag.isnull(), 'adresse'] = ref_all.adresse_full_2_tag


    ############################
    # traitement pays

    if len(ref_all.loc[ref_all.country_code_map.isnull(),['ref']].drop_duplicates())>0:
        print(ref_all.loc[ref_all.country_code_map.isnull(),['ref']].drop_duplicates().value_counts())

    ref_all = ref_all.merge(countries[['iso3', 'country_name_en']].drop_duplicates(), how='left', left_on='country_code_map', right_on='iso3')

    if any(ref_all.loc[ref_all.country_name_en.isnull()]):
        print(ref_all.loc[(ref_all.country_name_en.isnull())&(~ref_all.country_code_map.isnull()), ['country_code_map', 'nom_entier', 'numero_paysage','ref']].drop_duplicates())
        work_csv(ref_all.loc[(ref_all.country_name_en.isnull())&(~ref_all.country_code_map.isnull()), ['numero_paysage','country_code_map', 'nom_entier']].drop_duplicates(), 'paysage_pb_iso3')
        work_csv(ref_all.loc[(ref_all.country_name_en.isnull())&(ref_all.country_code_map.isnull()), ['ref','country_code_map', 'nom_entier']].drop_duplicates(), 'paysage_without_iso3')


    ###############
    # descriptif moulinette

    champs=pd.read_table(
    'C:/Users/zfriant/OneDrive/Matching/Echanges/PCRDT_TEST/ListeChamps.txt', sep='\t')
    print(champs.table.unique())
    for i in ['refext']:
        print(f"var name:\n{champs.loc[champs.table==i, 'code_champ'].tolist()}\n")
        print(f"var numerique:\n{champs.loc[(champs.table==i) & (champs.type=='num'), 'code_champ'].tolist()}")

    ref_all = ref_all[['ref', 'num_nat_struct', 'numero_ror', 'siren', 'siret', 'numero_rna', 'numero_paysage',  'sigle', 
            'code_postal',  'ville', 'adresse', 'label_num_ro_rnsr', 'an_fermeture', 'country_code_map', 'country_name_en', 'nom_entier',  'libelle1',
            'nom_entier_2',  'adresse_2_tag', 'ville_tag', 'etabs_rnsr', 'email','web', 'tel_clean' ]].drop_duplicates()

    print(f"{ref_all.info()}\nsize ref_all: {len(ref_all)}")


    ref_all['p_key'] = range(1, len(ref_all) + 1)
    ref_all.mask(ref_all=='', inplace=True)

    ref_all.to_pickle(f"{PATH_MATCH}ref_all.pkl")