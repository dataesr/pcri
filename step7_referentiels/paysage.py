
def paysage_import():
    from config_api import ods_headers
    import requests, pandas as pd
    # traitement PAYSAGE

    dataset='structures-de-paysage-v2'
    url = f"https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/{dataset}/exports/json"
    response = requests.get(url, headers=ods_headers)
    result=response.json()
    df = pd.DataFrame(result)

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
                            'iso3':'country_code_map'})
            .assign(an_fermeture=df.closuredate.str[0:4], ref='paysage')
            )[['nom_long', 'numero_paysage', 'an_fermeture', 'sigle', 'adresse', 'code_postal', 'ville', 'country_code_map', 'ref']]

    paysage.loc[~paysage.an_fermeture.isnull(), 'an_fermeture'] = paysage.loc[~paysage.an_fermeture.isnull()].an_fermeture.astype(int)

    paysage.loc[paysage.country_code_map.isnull(), 'country_code_map'] = 'FRA'

    paysage = paysage[(paysage.an_fermeture.isnull())|(paysage.an_fermeture > 2019)]

    # identifiants
    dataset='fr-esr-paysage_structures_identifiants'
    url = f"https://data.enseignementsup-recherche.gouv.fr/api/explore/v2.1/catalog/datasets/{dataset}/exports/json"
    response = requests.get(url, headers=ods_headers)
    result=response.json()
    ident = pd.DataFrame(result)
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