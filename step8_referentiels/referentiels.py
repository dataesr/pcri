def referentiels_load(snaf, ror_load=False, rnsr_load=False, sirene_load=False, sirene_subset=False):
    from step8_referentiels.ror import ror_import
    from step8_referentiels.sirene import sirene_import, sirene_concat
    from step8_referentiels.rnsr import rnsr_import
    from config_path import PATH, PATH_MATCH
    DUMP_PATH=f'{PATH}referentiel/'

    if ror_load==True:
        ror_import(DUMP_PATH)

    if sirene_load==True:
        naf_list=snaf[~snaf.naf_et.isnull()].naf_et.unique()
        sirene_import(f'{PATH}referentiel/', naf_list) # -> sirene_ref_moulinette.pkl

    if sirene_subset==True:
        sirene_concat(DUMP_PATH)

    if rnsr_load==True:
        rnsr_import(DUMP_PATH)


def ref_externe_preparation(snaf, rnsr_adr_corr=False ):
    import pandas as pd, re, json, numpy as np, os, time
    from text_to_num import alpha2digit

    # from IPython.display import HTML
    # from pathlib import Path
    from config_path import PATH, PATH_MATCH

    # from step7_referentiels.countries import ref_countries
    from functions_shared import work_csv, prep_str_col, stop_word, my_country_code, com_iso3, timing
    from step8_referentiels.ror import ror_prep
    from step8_referentiels.sirene import sirene_prep
    from step8_referentiels.rnsr import rnsr_prep
    from step8_referentiels.paysage import paysage_prep
    DUMP_PATH=f'{PATH}referentiel/'

    print(time.strftime("%H:%M:%S"))
    start_time=time.time()

    my_countries=my_country_code()
    com_iso=com_iso3()
    print(len(my_countries))

    ######
    # paysage
    paysage = paysage_prep(DUMP_PATH, my_countries, com_iso)
    check_time = timing(start_time)
    print(f"prep paysage: {check_time}")
    step_time=time.time()


    ror_zipname = ''.join([i for i in os.listdir(DUMP_PATH) if re.search('ror', i)]) 
    ror = ror_prep(DUMP_PATH, ror_zipname, my_countries)
    rnsr = rnsr_prep(DUMP_PATH, my_countries, com_iso, rnsr_adr_corr)

    check_time = timing(step_time)
    print(f"prep ror rnsr: {check_time}")
    step_time=time.time()

    ######
    # sirene
    sirene = sirene_prep(DUMP_PATH, snaf, my_countries, com_iso)
    check_time = timing(step_time)
    print(f"prep sirene: {check_time}")
    step_time=time.time()
    

    ######
    df_list = [rnsr, paysage, ror, sirene]
    ref_all=pd.DataFrame()

    for tab in df_list:
        if 'label_num_ro_rnsr' in tab.columns:
            print("## label_num_ro_rnsr cleaning")
            tab.loc[~tab.label_num_ro_rnsr.isnull(), 'label_num_ro_rnsr'] = tab.loc[~tab.label_num_ro_rnsr.isnull()].label_num_ro_rnsr.str.lower().replace(';', ' ')

        print("## string cleaning")
        #lowercase / exochar / unicode / punct
        ref_cols = ['nom_long', 'sigle', 'ville', 'adresse', 'adresse_full']
        tab = prep_str_col(tab, ref_cols)

        check_time = timing(step_time)
        print(f"prep str columns: {check_time}")
        step_time=time.time()

        print("## create nom_enier [nom_long+sigle]")
        tmp = tab.loc[(tab.nom_long!=tab.sigle)&(~tab.sigle.isnull()), ['nom_long', 'sigle']]
        tmp['nom_entier'] = [x1 if x2 in x1 else x1+' '+x2 for x1, x2 in zip(tmp['nom_long'], tmp['sigle'])]
        tab = pd.concat([tab, tmp[['nom_entier']]], axis=1)
        tab.loc[tab.nom_entier.isnull(), 'nom_entier'] = tab.nom_long

        print("## stopword")
        #suppression des mots vides comme le la les et... pour "toutes les langues"
        # result ['nom_long_2', 'nom_entier_2', 'adresse_2', 'adresse_full_2']
        ref_cols = ['nom_long', 'nom_entier', 'adresse', 'adresse_full']
        tab = stop_word(tab, 'country_code_map', ref_cols)

        check_time = timing(step_time)
        print(f"stop word: {check_time}")
        step_time=time.time()


        tab.rename(columns={'nom_long':'libelle1'}, inplace=True)
        tab['nom_entier_2'] = tab.nom_entier_2.apply(lambda x: ' '.join(x) if isinstance(x, list) else x)

        check_time = timing(step_time)
        print(f"nom_entier: {check_time}")
        step_time=time.time()

        if 'adresse_full_2' in tab.columns:
            print("## adresse rnsr")  
            #traitement spécifique adresse_full du rnsr
            tmp = tab[~tab['adresse_full_2'].isnull()][['adresse_full_2']]
            tmp.adresse_full_2 = tmp.adresse_full_2.apply(lambda x: list(filter(None, x))).apply(lambda x: ' '.join(x))
            tmp[['cp_temp', 'ville_temp']] = tmp['adresse_full_2'].str.extract(r"\s(\d{5})\s?([a-z]+(?:\s?[a-z]+)*)", expand=True)

            def match(adr):
                x = re.search(r"(\b\d{1,4})\s([a-z]+\s?)+", adr)
                if x :
                    return(x.group())
        
            tmp['adresse_temp'] = tmp['adresse_full_2'].apply(match)    
            tab = pd.concat([tab, tmp.drop(columns='adresse_full_2')], axis=1)
        
            print("## code postal into city")
            # extraction du code postal du champs ville 
            tab.loc[tab.code_postal.isnull(), 'code_postal'] = tab.ville.str.extract(r"(\d+)")

            print("## cp/ville into temp rnsr")
            tab.loc[tab.code_postal.isnull(), 'code_postal'] = tab.cp_temp
            tab.loc[tab.ville.isnull(), 'ville'] = tab.ville_temp

            tab.loc[tab.adresse_2.isnull(), 'adresse_2'] = tab.adresse_temp
            tab.loc[tab.adresse.isnull(), 'adresse'] = tab.adresse_full
            tab.drop(columns=['cp_temp', 'ville_temp', 'adresse_temp','adresse_full', 'adresse_full_2'], inplace=True)

        if 'adresse_2' in tab.columns:
            adr = json.load(open('data_files/ad.json'))
            adr = pd.DataFrame(adr.items(), columns=['in', 'out'])
            print("## adresse again")  
            tmp = tab.loc[~tab['adresse_2'].isnull(), ['adresse_2']].drop_duplicates()
            tmp['ad_tmp'] = tmp.adresse_2.str.split()
            tmp = tmp.explode('ad_tmp')
            tmp = tmp.merge(adr, how='left', left_on='ad_tmp', right_on='in')
            tmp.loc[~tmp.out.isnull(), 'ad_tmp'] = tmp.loc[~tmp.out.isnull(), 'out']
            tmp = tmp.groupby('adresse_2', as_index=False).agg(lambda x: list(filter(None, x))).drop(columns=['in', 'out'])
                
            # tab = tab.merge(tmp, how='left', on='adresse_2')
        
            tmp['adresse_2_tag'] = tmp.loc[~tmp['ad_tmp'].isnull()]['ad_tmp'].apply(lambda x: ' '.join(x))
            with open("data_files/adresse_pattern.txt", "r") as pats:
                for n, line in enumerate(pats, start=1):       
                    pat = line.rstrip('\n')
                    tmp['adresse_2_tag'] = tmp['adresse_2_tag'].str.replace(pat,'', regex=True)
                        
            tmp['adresse_2_tag'] = tmp['adresse_2_tag'].apply(lambda x: alpha2digit(x, 'fr'))
            tmp['adresse_2_clean'] = tmp.loc[~tmp['ad_tmp'].isnull()]['ad_tmp'].apply(lambda x: ' '.join(x))
            tab = tab.merge(tmp, how='left', on='adresse_2').drop(columns='ad_tmp')

        
        check_time = timing(step_time)
        print(f"adresse clean: {check_time}")
        step_time=time.time()


        print("## city")  
        # nettoyage de ville
        cedex="cedax|cedrex|cdexe|cdex|credex|cedex|cedx|cede|ceddex|cdx|cex|cexex|edex"
        tab['ville'] = tab.ville.str.replace('\\d+', ' ', regex=True).str.strip()
        tab.loc[(tab.country_code=='FRA'), 'ville'] = tab.loc[tab.country_code=='FRA', 'ville'].str.replace(cedex, ' ', regex=True).str.strip()
        tab.loc[(tab.country_code=='FRA'), 'ville'] = tab.loc[tab.country_code=='FRA', 'ville'].str.replace('^france$', '', regex=True).str.strip()
        tab.loc[(tab.country_code=='FRA'), 'ville'] = tab.loc[tab.country_code=='FRA', 'ville'].str.replace(r"\bst\b", 'saint', regex=True).str.strip()
        tab.loc[(tab.country_code=='FRA'), 'ville'] = tab.loc[tab.country_code=='FRA', 'ville'].str.replace(r"\bste\b", 'sainte', regex=True).str.strip()
        tab['ville_tag'] = tab['ville'].str.strip().str.replace(r'\s+', '-', regex=True)

        if 'code_postal' in tab.columns:
            print("## code postal to department")  
            # code postal - > département
            mask=(tab.country_code=='FRA')&(~tab.code_postal.isnull())&(tab.code_postal.str.len()!=5)
            if len(tab.loc[mask])>0:
                print(f"probleme avec le cp à corriger si possible à la source: {tab.loc[mask].code_postal.unique()}")
                print(f"structure avec cp mais à corriger: {tab.loc[mask&(tab.code_postal.str.contains(r"\d+")), ['ref','code_postal', 'ville']]}")

            tab.loc[(tab.country_code=='FRA')&(~tab.code_postal.isnull()), 'code_postal'] = tab.loc[(tab.country_code=='FRA')&(~tab.code_postal.isnull()), 'code_postal'].str.replace(r"\D+", '', regex=True)
            tab.loc[~tab.code_postal.isnull(), 'dep_code'] = tab.code_postal.str[0:2]

        check_time = timing(step_time)
        print(f"cp, city, dept: {check_time}")
        step_time=time.time()

        ############################
        # traitement pays
        print("## country") 
        if len(tab.loc[tab.country_code_map.isnull(),['ref']].drop_duplicates())>0:
            print(f"country_code_map isnull: {tab.loc[tab.country_code_map.isnull(),['ref']].value_counts()}")

        if any(tab.loc[tab.country_name_en.isnull()]):
            print(tab.loc[(tab.country_name_en.isnull())&(~tab.country_code.isnull()), ['country_code_map', 'ref']].drop_duplicates())
            if 'numero_paysage' in tab.columns:
                work_csv(tab.loc[(tab.country_name_en.isnull())&(~tab.country_code.isnull()), ['numero_paysage','country_code_map', 'nom_entier']].drop_duplicates(), 'paysage_pb_iso3')
                work_csv(tab.loc[(tab.country_name_en.isnull()), ['ref','country_code_map', 'nom_entier']].drop_duplicates(), 'ref_without_iso3')

        
        #################################
        if 'tel' in tab.columns:
            print("## tel cleaning")
            y = tab.loc[(~tab.tel.isnull())&(tab.country_code=='FRA'), ['tel']]
            y['tel_clean']=y.tel.apply(lambda x:[re.sub(r'[^0-9]+', '', i) for i in x])
            y['tel_clean']=y.tel_clean.apply(lambda x: [re.sub(r'^(33|033)', '', i).rjust(10, '0') for i in x])
            y['tel_clean']=y.tel_clean.apply(lambda x: [i[0:10] if (len(i)>10) and (i[0:1]=='0') else i for i in x])
            y['tel_clean']=y.tel_clean.apply(lambda x:[re.sub(r'^0+$', '', i) for i in x])
            y['tel_clean']=y.tel_clean.apply(lambda x: ';'.join(set(x))).str.strip()

            tab = pd.concat([tab, y[['tel_clean']]], axis=1)

        if 'email' in tab.columns:
            tab['email'] = tab.email.apply(lambda x: ' '.join(x) if isinstance(x, list) else np.nan)

        check_time = timing(step_time)
        print(f"tel/mail clean: {check_time}")
        step_time=time.time()

        ######
        # table all
        print("## ref_all concat")
        ref_all = pd.concat([ref_all, tab], ignore_index=True)

    ref_all.mask(ref_all=='', inplace=True)
    ref_all = ref_all.sort_values(['ref', 'num_nat_struct', 'siren', 'numero_paysage', 'numero_ror'])

    ref_all = (ref_all.loc[~ref_all.nom_entier_2.isnull(), 
        ['ref', 'num_nat_struct', 'numero_ror', 'siren', 'siret', 'numero_rna', 'numero_paysage',  
        'sigle', 'nom_entier',  'libelle1', 'nom_entier_2', 'adresse_2_tag', 'adresse_2_clean', 'ville_tag', 
        'etabs_rnsr', 'email','web', 'tel_clean',
        'code_postal',  'ville', 'adresse', 'label_num_ro_rnsr', 'an_fermeture', 'dep_code',
        'country_code_map', 'country_code', 'country_name_en']
        ]
        .drop_duplicates())


    ref_all['p_key'] = range(1, len(ref_all) + 1)
    ref_all = ref_all.reset_index(drop=True)

    print("## save final dataset") 
    ref_all.to_parquet(f"{PATH_MATCH}ref_all.parquet.gzip")
    ref_all.to_pickle(f"{PATH_MATCH}ref_all.pkl")

    check_time = timing(start_time)
    print(f"elapse all ref_all preparation: {check_time}")
