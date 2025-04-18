# traitement RNSR
def rnsr_import(DUMP_PATH):
    import pandas as pd, re, requests
    from config_api import scanr_headers

    def get_rnsr(max_results):
        import math, requests
        url = 'http://185.161.45.213/organizations/organizations?where={"rnsr":{"$exists":true}}&'
        url_suffix = 'max_results={}&page={}'
        r = requests.get(url + url_suffix.format(1,1), headers=scanr_headers,  verify=False)
        nb_results = r.json().get("meta").get("total")
        max_page = math.ceil(nb_results / max_results)

        result = []    
        for page in range(1, max_page + 1):
            r = requests.get(url + url_suffix.format(max_results,page), headers=scanr_headers,  verify=False)
            try:
                result += r.json()['data']
                print(page, end=',')     
            except:
                print(page, end=',')    

        return (result)

    ### Extraction des données rnsr de dataESR
    max_results = 100
    r = get_rnsr(max_results)

    ### Prog extraction données rnsr par API selon le nb de pages ; RNSR COMPLET

    to_keep=r
    len(to_keep)

    delete = ["_id", 'addresses', 'alias', 'descriptions', "supervisors", 'created_at','dates', 'emails', 'etag', 
              'foreign', 'websites', 'hrefs','leaders','modified_at', 'names', 'panels', 'phones', 'rnsr_domains', 
              'parents', 'external_ids', 'social_medias', 'active', 'relations', 'external_links', 'evaluations', 
              'website_check', 'nature', 'contract', 'badges', 'certifications', 'keywords_fr', 'keywords_en', 
              'human_ressources', 'sector', 'legal_category', 'code_numbers', 'predecessors']

    # keep = ["tutelle_nom",'an_fin','an_debut','tutelle_id','tutelle_source','nature_tutelle','an_creation','an_fermeture','id',
    #         'acronym','libelle','nat','tel','email','ville','com_code','code_pays','cp','adresse','gps','sigles_rnsr','types']
    translation_str = str.maketrans(".'/-()_", '       ')

    rnsr = [] 
    for p in to_keep:
        elem = {k: v for k, v in p.items() if (v and v != "NaT")}

        elem["tutelle_name"] = []    
        elem["tutelle_end"] = []
        elem["tutelle_start"] = []
        elem["tutelle_id"] = []
        elem["tutelle_source"] = []    
        elem["tutelle_nature"] = []   
        if elem.get("supervisors"):
            for supervisor in elem["supervisors"]:    
                    elem["tutelle_id"].append(supervisor.get("id", '#'))
                    elem["tutelle_name"].append(supervisor.get("name", '#'))
                    elem["tutelle_source"].append(supervisor.get("source_code", "#"))      
                    elem["tutelle_nature"].append(supervisor.get("supervision_type", '#'))
                    elem["tutelle_start"].append(supervisor.get("start_date", '#')[0:4])
                    elem["tutelle_end"].append(supervisor.get("end_date", '#')[0:4])  

        if elem.get("dates"): 
            for date in elem["dates"]:
                if date.get("start_date"): 
                    elem["date_start"] = date.get("start_date")[0:4]
                if date.get("end_date"): 
                    elem["date_end"] = date.get("end_date")[0:4]
                else:
                    elem["date_end"] = None                


        if elem.get("names"):
            for name in elem['names']:
                if name.get("acronym_fr"): 
                    elem["acronym"] = name.get("acronym_fr")        
                else:
                    elem["acronym"] = None
                elem["name"] = name.get("name_fr")

        if elem.get("nature"):
            for nt in elem['nature']:
                if nt.get('value'):
                    elem["nat"] = nt.get('value')
                else:
                    elem["nat"] = '#'               

        elem['tel'] = []
        if elem.get('phones'):
            for ph in elem['phones']:
                elem['tel'].append(ph.get('phone', '#'))

        elem['email'] = []
        if elem.get('emails'):
            for mail in elem['emails']:
                elem['email'].append(mail.get('email', '#'))

        if elem.get('addresses'):
            for ad in elem['addresses']:
                if ad.get("status") == 'main':
                    elem['city'] = ad.get("city")
                    elem['com_code'] = ad.get("city_code")
                    elem['country_code'] = ad.get("country_code")
                    elem['cp'] = ad.get("post_code")
                    elem['street_num'] = ad.get('housenumber')
                    elem['street'] = ad.get('street')
                    elem['adresse_full'] = ad.get("input_address")
                    if ad.get("geocoded") is True:
                        elem['gps'] = str(ad["coordinates"]["coordinates"]).strip("'[]")

        elem['predecessor'] = []
        elem['succession_type'] = []
        if elem.get('predecessors'):
            for pred in elem["predecessors"]:    
                    elem["predecessor"].append(pred.get("id", '#'))
                    elem["succession_type"].append(pred.get("succession_type", "#"))


        elem["sigles_rnsr"] = ';'.join([code.translate(translation_str).strip() for code in elem.get("code_numbers", [])])
        elem["sigles_rnsr"] = re.sub(r"\s+", '', elem["sigles_rnsr"])


#         l = ['succession_type',"predecessor","tutelle_id","tutelle_name","tutelle_end","tutelle_start","tutelle_source","tutelle_nature",'tel','email', 'types']
#         for e in l:
#             if elem.get(e):
#                 elem[e] = ';'.join([code for code in elem.get(e) if code is not None])

        for field in delete:
            if elem.get(field):
                elem.pop(field)

        elem = {k: v for k, v in elem.items() if (v and v != "NaT")}
        rnsr.append(elem)

    tut=[]
    for i in rnsr:
        if i.get('tutelle_id'):
            t=i.get('tutelle_id')
            tut.extend(t)

    tut=list(set(tut))
    print(f"size liste de tutelle: {len(tut)}")
    tutelle=[]
    for i in tut:
        url = f'http://185.161.45.213/organizations/organizations/{i}'
        r = requests.get(url, headers=scanr_headers,  verify=False)
        res = r.json()
        if res.get('active')==True:
            try:
                if res.get('names')[0]['acronym_fr']:
                    tutelle.append({"tutelle_id":i,
                                "tutelle_acronym":res.get('names')[0]['acronym_fr']})
                else:
                    tutelle.append({"tutelle_id":i,
                                "tutelle_acronym":res.get('names')[0]['acronym_en']})
            except:
                pass

    tutelle = pd.json_normalize(tutelle)
    rnsr = pd.json_normalize(rnsr)
    rnsr = rnsr.explode('tutelle_id')
    rnsr = rnsr.merge(tutelle, how='left', on='tutelle_id')
    tmp = rnsr.loc[~rnsr.tutelle_acronym.isnull(), ['id', 'tutelle_acronym']]
    tmp = tmp.groupby('id').agg({"tutelle_acronym": lambda x: list(set(x))}).reset_index()
    rnsr = rnsr.drop(columns=['tutelle_acronym', 'tutelle_id'])
    rnsr = rnsr[~rnsr.astype(str).duplicated()].merge(tmp, how='left', on='id')
    rnsr.to_pickle(f"{DUMP_PATH}rnsr_complet.pkl")


def rnsr_prep(DUMP_PATH, countries, com_iso, corr=False):
    import pandas as pd, sys, numpy as np
    from functions_shared import work_csv, prep_str_col
    print("### RNRS preparation")
    rnsr = pd.read_pickle(f"{DUMP_PATH}rnsr_complet.pkl")

    rnsr.loc[~rnsr.date_end.isnull(), 'date_end'] = rnsr.loc[~rnsr.date_end.isnull()].date_end.astype(int)
    print(f"size rnsr: {len(rnsr)}")
    print(f"list of end date: {rnsr.date_end.unique()}")

    rnsr.loc[rnsr.tutelle_acronym.isnull(), 'tutelle_acronym'] = rnsr.loc[rnsr.tutelle_acronym.isnull(), 'tutelle_name']
    rnsr = (rnsr
            .loc[((rnsr.date_end.isnull())|(rnsr.date_end>2019))&(~rnsr.name.isnull())]
            .assign(adresse=rnsr.street_num+' '+rnsr.street, ref='rnsr')
            .rename(columns=
                    {'rnsr':'num_nat_struct',
                    'name':'nom_long',
                    'acronym':'sigle',
                    'date_end':'an_fermeture',
                    'sigles_rnsr':'label_num_ro_rnsr',
                    'tutelle_acronym':'etabs_rnsr',
                    'city':'ville',
                    'cp':'code_postal'})
                            
        )[['num_nat_struct', 'an_fermeture', 'nom_long', 'sigle', 'label_num_ro_rnsr', 
            'etabs_rnsr', 'ville', 'com_code', 'adresse', 'code_postal',
            'adresse_full', 'tel', 'email', 'ref']]
    
    tmp = rnsr[['etabs_rnsr']].explode('etabs_rnsr')
    tmp = prep_str_col(tmp, ['etabs_rnsr'])
    tmp['etabs_rnsr'] = tmp.etabs_rnsr.str.replace(r"\s+", '-', regex=True)
    tmp = tmp.groupby(level=0).agg(lambda x: ' '.join(x.dropna()))
    rnsr = rnsr.drop(columns='etabs_rnsr').merge(tmp, how='left', left_index=True, right_index=True)

    if corr==True:
        if len(rnsr.loc[(rnsr.code_postal.isnull())|(rnsr.ville.isnull())])>0:
            work_csv(rnsr.loc[(rnsr.code_postal.isnull())|(rnsr.ville.isnull()), ['num_nat_struct', 'nom_long','adresse_full', 'code_postal', 'ville']].drop_duplicates(), 'rnsr_adresse_a_completer')
            sys.exit("ATTENTION ! fix rnsr adresses into data_work -> rnsr_adresse_a_completer")
        else:
            print("RNSR not need to fix address")

    add_ad = pd.read_csv(f"{DUMP_PATH}rnsr_adresse_manquante.csv",  sep=';', encoding='ANSI', dtype={'cp_corr':str})
    add_ad = add_ad[['num_nat_struct', 'cp_corr', 'city_corr', 'country_corr']].drop_duplicates()

    if len(rnsr.loc[(rnsr.adresse.isnull())&(~rnsr.adresse_full.isnull())])>0:
        print(rnsr.loc[(rnsr.ville.isnull())&(rnsr.adresse.isnull())&(~rnsr.adresse_full.isnull()), ['adresse_full', 'ville']])



    rnsr = rnsr.merge(add_ad, how='left', on='num_nat_struct')
    rnsr.loc[~rnsr.cp_corr.isnull(), 'code_postal'] = rnsr.cp_corr
    rnsr.loc[~rnsr.city_corr.isnull(), 'ville'] = rnsr.city_corr
    # rnsr.loc[~rnsr.country_corr.isnull(), 'iso3'] = rnsr.country_corr

    print("- com_iso")
    # com_iso=com_iso3()
    rnsr = rnsr.merge(com_iso, how='left', on='com_code')
    rnsr.loc[~rnsr.country_corr.isnull(), 'iso_3'] = rnsr.loc[~rnsr.country_corr.isnull(), 'country_corr']

    rnsr = rnsr.merge(countries[['iso3', 'parent_iso3']], how='left', left_on='iso_3', right_on='iso3')
    rnsr.loc[rnsr.parent_iso3.isnull(), 'parent_iso3'] = 'FRA'
    rnsr = rnsr.rename(columns={'iso3':'country_code_map', 'parent_iso3':'country_code'})
    rnsr = rnsr.merge(countries[['iso3', 'country_name_en']], how='left', left_on='country_code', right_on='iso3')
    
    rnsr.drop(columns=['cp_corr','city_corr','country_corr','iso_3', 'iso3'], inplace=True)
    rnsr.mask(rnsr=='', inplace=True)

    print(f"rnsr end size: {len(rnsr)}")
    return rnsr