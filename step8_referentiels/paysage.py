def paysage_import(dataset):
    import pandas as pd, requests
    from config_api import ods_headers
    
    url = f"https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/{dataset}/exports/json"
    response = requests.get(url, headers=ods_headers)
    result=response.json()
    return pd.DataFrame(result)

def paysage_prep(DUMP_PATH, countries):
    import pandas as pd, numpy as np
    from urllib.parse import urlparse
    # traitement PAYSAGE

    dataset='structures-de-paysage-v2'
    df = paysage_import(dataset)

    paysage = df.mask(df=='')

    paysage['sigle'] = np.where(paysage.acronymfr.isnull(), paysage.shortname, paysage.acronymfr)
    paysage.loc[paysage.sigle.isnull(), 'sigle'] = paysage.acronymlocal
    paysage['nom_long'] = np.where(paysage.officialname.isnull(), paysage.usualname, paysage.officialname)
    if any(paysage['nom_long'].isnull()):
        print(len(paysage[paysage['nom_long'].isnull()]))

        
    paysage = (paysage
            .rename(columns={'id':'numero_paysage',
                            'address':'adresse',
                            'postalcode' : 'code_postal',
                            'locality':'ville',
                            'cityid':'com_code'})
            .assign(an_fermeture=df.closuredate.str[0:4], ref='paysage')
            )[['nom_long','numero_paysage','an_fermeture','sigle','adresse','code_postal','ville', 'iso3', 'com_code','ref']]

    paysage=paysage.merge(countries[['iso3', 'parent_iso3']].drop_duplicates(), how='left', on='iso3')
    paysage.rename(columns={'iso3':'country_code_map', 'parent_iso3':'country_code'})
    paysage=paysage.merge(countries[['iso3', 'country_name_en']].drop_duplicates(), left_on='country_code', right_on='iso3')
    

# paysage.loc[paysage.country_code_map.isnull(), 'country_code_map'] = 'FRA'

    paysage.loc[~paysage.an_fermeture.isnull(), 'an_fermeture'] = paysage.loc[~paysage.an_fermeture.isnull()].an_fermeture.astype(int)
    paysage = paysage[(paysage.an_fermeture.isnull())|(paysage.an_fermeture > 2019)]

    # identifiants
    dataset='fr-esr-paysage_structures_identifiants'
    ident = paysage_import(dataset)

    ident['id_value'] = ident.id_value.astype(str)
    ident.loc[ident.id_type=='ror', 'id_value'] = 'R'+ident.id_value

    s = (ident.loc[ident.id_type=='siret',  ['id_paysage', 'id_type', 'id_value']]
        .assign(id_type='siren', id_value = ident.id_value.str[0:9])
        .drop_duplicates())

    ident = (ident
            .loc[ident.id_type.isin(['ror', 'rnsr','siret', 'rna', 'cnrs-unit']), ['id_paysage', 'id_type', 'id_value']]
            .sort_values('id_type'))
 
    ident = (pd.concat([ident, s], ignore_index=True)
                    .pivot_table(index='id_paysage',
                            columns='id_type',
                            values='id_value',
                            aggfunc=lambda x: ';'.join(x),
                            dropna=True)
            )

    ident = ident.rename(columns={'cnrs-unit':'label_num_ro_rnsr',
                                'ror':'numero_ror',
                                'rnsr':'num_nat_struct',
                                'rna':'numero_rna'})

    paysage = paysage.merge(ident, how='left', left_on='numero_paysage', right_on='id_paysage')

    # site web
    dataset='fr-esr-paysage_structures_websites'
    result = paysage_import(dataset)

    web = pd.DataFrame(result).loc[result.type=='website', ['id_structure_paysage','url','language']].drop_duplicates().sort_values('id_structure_paysage')
    web.language = web.language.str.lower()
    print(len(web))

    dup = web.groupby(['id_structure_paysage']).filter(lambda x: x['url'].count() > 1.)
    dup['web']=dup["url"].astype(str).apply(lambda x: urlparse(x).netloc)

    web = web.merge(dup, how='left', on=['id_structure_paysage', 'url',  'language'])
    web.loc[web.web.isnull(), 'web'] = web.url
    web = web.drop(columns=['url', 'language']).drop_duplicates().groupby('id_structure_paysage', as_index=False).agg(lambda x: ';'.join(x))
    paysage = paysage.merge(web, how='left', left_on='numero_paysage', right_on='id_structure_paysage').drop(columns='id_structure_paysage')

    paysage.mask(paysage=='', inplace=True)

    paysage.to_pickle(f"{DUMP_PATH}paysage_moulinette.pkl")
    return paysage