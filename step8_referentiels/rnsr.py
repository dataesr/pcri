# traitement RNSR
def rnsr_prep(DUMP_PATH, countries, load_dump=False):
    import pandas as pd, sys, numpy as np, stopwordsiso, re
    from functions_shared import work_csv, prep_str_col, rnsr_address_split
    from remote_process.rnsr import rnsr_dump_load
    from remote_process.grist import add_records_to_grist, geoG
    from config_url import grist_url
    from constant_vars import PERIODE_FRAMEWORK

    print("### RNRS preparation")

    if load_dump:
        rnsr = rnsr_dump_load()
        rnsr = pd.json_normalize(rnsr)
        rnsr.to_pickle(f"{DUMP_PATH}rnsr_all.pkl")
    else:
        rnsr = pd.read_pickle(f"{DUMP_PATH}rnsr_all.pkl")

    
    year_end = int(PERIODE_FRAMEWORK.split('-')[0])

    mask = ~rnsr['date_end'].isnull()
    rnsr.loc[mask, 'date_end'] = rnsr.loc[mask, 'date_end'].astype(int)
    rnsr = rnsr.loc[((rnsr['date_end'].isnull()) | (rnsr['date_end'] >= year_end)) & (~rnsr.name.isnull())]

    print(f"size rnsr: {len(rnsr)}")
    print(f"list of end date: {rnsr.date_end.unique()}")

    #===================================================
    # create tutelle acronym

    fr_stopwords = stopwordsiso.stopwords("fr")

    def to_acronym(name):
        if not isinstance(name, str) or not name.strip():
            return name
        # Si c'est déjà un acronyme (tout en majuscules, sans espace) -> on garde tel quel
        if name.isupper() and " " not in name:
            return name
        # Extraction des mots (lettres/chiffres, accents inclus)
        words = re.findall(r"[A-Za-zÀ-ÖØ-öø-ÿ0-9]+", name)
        # Suppression des stopwords français
        words_filtered = [w for w in words if w.lower() not in fr_stopwords]
        if not words_filtered:  # sécurité si tout a été filtré (ex: nom = "de la")
            words_filtered = words
        return "".join(w[0].upper() for w in words_filtered)

    # Application sur la colonne (qui contient des listes de tutelles)
    rnsr["etabs_rnsr"] = rnsr["tutelle_name"].apply(
        lambda lst: [to_acronym(n) for n in lst] if isinstance(lst, list) else to_acronym(lst)
    )

    rnsr = (rnsr
            .assign(ref='rnsr')
            .rename(columns=
                    {'rnsr':'num_nat_struct',
                    'name':'nom_long',
                    'acronym':'sigle',
                    'date_end':'an_fermeture',
                    'sigles_rnsr':'label_num_ro_rnsr',
                    'street': 'adresse',
                    'city':'ville',
                    'cp':'code_postal'})
        )[['num_nat_struct', 'an_fermeture', 'nom_long', 'sigle', 'label_num_ro_rnsr', 
            'etabs_rnsr', 'ville', 'adresse', 'code_postal',
            'adresse_full', 'tel', 'email', 'ref']]
    
    tmp = rnsr[['etabs_rnsr']].explode('etabs_rnsr')
    tmp = prep_str_col(tmp, ['etabs_rnsr'])
    tmp['etabs_rnsr'] = tmp.etabs_rnsr.str.replace(r"\s+", '-', regex=True)
    tmp = tmp.groupby(level=0).agg(lambda x: ' '.join(x.dropna()))

    rnsr = (rnsr
            .drop(columns='etabs_rnsr')
            .merge(tmp, 
                   how='left', 
                   left_index=True, 
                   right_index=True)
    )

    print(f"- size rnsr: {len(rnsr)}")

    #====================================================================
    # add rnsr address fix

    cols = ['num_nat_struct', 'adresse', 'code_postal', 'ville', 'pays']

    fix_ad = geoG['Rnsr_address_fix'][cols]

    cols = [c for c in cols if c not in ('num_nat_struct', 'pays')]

    rnsr = (rnsr.drop(columns = cols)
        .merge(fix_ad, 
               how='left', 
               on=['num_nat_struct'],
               indicator=True
        )
    )
    
    #=============================================
    # check address and fix new address in grist

    tmp = rnsr.loc[rnsr['_merge']=='left_only']

    if len(tmp)>0:

        # tmp = rnsr_address_split(tmp)

        tmp = tmp[
            ['num_nat_struct', 
            'adresse_full', 
            'adresse', 
            'code_postal', 
            'ville', 
            'pays', 
            'a_verifier']
            ].assign(be_checked=True)

        add_records_to_grist(tmp, grist_url, 'pcri', 'geo', 'Rnsr_address_fix')

        sys.exit("🚨 ! fix rnsr adresses into grist/geo/Rnsr_address_fix")
    else:
        print("RNSR not need to fix address")


    rnsr.loc[rnsr['pays'].isnull(), 'pays'] = 'FRA'
    rnsr = rnsr.rename(
        columns={
            'pays': 'country_code_map'
            }
        ).drop(columns = '_merge')

    rnsr.mask(rnsr=='', inplace=True)

    print(f"rnsr end size: {len(rnsr)}")

    return rnsr