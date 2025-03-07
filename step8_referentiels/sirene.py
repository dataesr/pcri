
# traitement SIRENE
def sirene_refext(DUMP_PATH):
    import requests, pandas as pd
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)  
    # from io import (BytesIO, StringIO)
    from config_api import sirene_headers
    from config_path import PATH_REF

    def get_last_info_siret(x):
        tmp = [e for e in x if e.get('dateFin') is None]
        tmp = sorted(tmp, key=lambda k: k['dateDebut'], reverse=True)
        if len(tmp)>0:
            return tmp[0]
        tmp = sorted(x, key=lambda k: k['dateFin'], reverse=True)
        if len(tmp)>0:
            return tmp[0]
        return {}

    def harvest_data():
        for e in range(rinit.json().get("header").get("nombre")):
            r2 = rinit.json()['etablissements'][e]
            period = r2.get("nombrePeriodesEtablissement")
            response_for_this_siret = [] 
            for j in range(period):
                try:
                    rj = r2.get('periodesEtablissement')[j]
                    response_for_this_siret.append(rj)
                except:
                    pass
            response_siret = get_last_info_siret(response_for_this_siret)
            r2.pop('periodesEtablissement')
            r2.update(response_siret)
            return r2

    S_PKL = pd.read_pickle(f'{PATH_REF}sirene_df.pkl').naf_et.unique()
    CHAMPS="""siren, dateCreationUniteLegale, siret, dateCreationEtablissement, sigleUniteLegale, denominationUniteLegale, 
    nomUniteLegale,
    prenom1UniteLegale, categorieEntreprise,categorieJuridiqueUniteLegale,activitePrincipaleUniteLegale,
    identifiantAssociationUniteLegale,
    codeCommuneEtablissement,numeroVoieEtablissement,typeVoieEtablissement, libelleVoieEtablissement,complementAdresseEtablissement,
    codePostalEtablissement,libelleCommuneEtablissement,codePaysEtrangerEtablissement,libelleCommuneEtrangerEtablissement,
    enseigne1Etablissement,enseigne2Etablissement,enseigne3Etablissement,denominationUsuelleEtablissement,activitePrincipaleEtablissement,
    dateDebut,nombrePeriodesEtablissement,statutDiffusionEtablissement""".replace('\n','')

    result = []

    for i in S_PKL:
        q=f'periode(etatAdministratifEtablissement:A AND activitePrincipaleEtablissement:{i}) AND etablissementSiege:true'
        url=f'https://api.insee.fr/entreprises/sirene/siret?q={q}&nombre=1000&champs={CHAMPS}&curseur=*' 
        rinit = requests.get(url,  headers=sirene_headers, verify=False)
        max_rec = rinit.json().get("header").get("total")
        print(f"{rinit.json().get('header')}, {q}")


        if rinit.status_code == 200:
            if max_rec <= 1000:
                result.append(harvest_data())


            if max_rec > 1000:   
                while rinit.json().get("header").get("nombre") > 0:
                    result.append(harvest_data())

                    cur = rinit.json().get('header').get('curseurSuivant')
                    url=f'https://api.insee.fr/entreprises/sirene/siret?q={q}&nombre=1000&champs={CHAMPS}&curseur={cur}' 
                    rinit = requests.get(url,  headers=sirene_headers, verify=False)
                    
    pd.json_normalize(result).to_pickle(f"{DUMP_PATH}sirene_ref_moulinette.pkl")


def sirene_prep(DUMP_PATH, countries):
    import pandas as pd
    df = pd.read_pickle(f"{DUMP_PATH}sirene_ref_moulinette.pkl")

    sirene = df.loc[df['statutDiffusionEtablissement']!='P']

    sirene = (sirene.assign(ens=df[['enseigne1Etablissement', 'enseigne2Etablissement', 'enseigne3Etablissement']]
                        .fillna('')
                        .agg(' '.join, axis=1)
                        .str.strip())
            )

    sirene = (sirene.assign(nom_perso=sirene[['uniteLegale.nomUniteLegale', 'uniteLegale.prenom1UniteLegale']]
                            .fillna('')
                            .agg(' '.join, axis=1)
                            .str.strip())
                )

    for i in ['denominationUsuelleEtablissement', 'ens']:
        sirene.loc[sirene['uniteLegale.denominationUniteLegale'].isnull(), 'uniteLegale.denominationUniteLegale'] = sirene[i]

    sirene.loc[~sirene['nom_perso'].isnull(), 'uniteLegale.denominationUniteLegale'] = sirene['uniteLegale.denominationUniteLegale']+' '+sirene['nom_perso']
        
    if len(sirene.loc[sirene['uniteLegale.denominationUniteLegale'].isnull()])>0:
        print(f"siren without denomination_UL: {sirene.loc[sirene['uniteLegale.denominationUniteLegale'].isnull()]}")
    # sirene['nom_long'] = [x1 if x2 in x1 else x1+' '+x2 for x1, x2 in zip(tmp['uniteLegale.denominationUniteLegale'], tmp['entities_acronym_source_dup'])]

    sirene = sirene.assign(adresse=sirene[['adresseEtablissement.numeroVoieEtablissement', 
                                        'adresseEtablissement.typeVoieEtablissement',
                                        'adresseEtablissement.libelleVoieEtablissement']]
                            .fillna('')
                            .agg(' '.join, axis=1)
                            .str.strip())


    sirene.loc[sirene['adresseEtablissement.libelleCommuneEtablissement'].isnull(), 'adresseEtablissement.libelleCommuneEtablissement'] = sirene['adresseEtablissement.libelleCommuneEtrangerEtablissement']

    sirene = (sirene[
        ['siren', 'siret', 'dateFin',
        'uniteLegale.denominationUniteLegale','uniteLegale.sigleUniteLegale', 
        'uniteLegale.identifiantAssociationUniteLegale',
        'adresseEtablissement.complementAdresseEtablissement', 'adresse',
        'adresseEtablissement.codePostalEtablissement',
        'adresseEtablissement.libelleCommuneEtablissement',  
        'adresseEtablissement.codeCommuneEtablissement',
        'adresseEtablissement.codePaysEtrangerEtablissement']]
        .rename(columns={'dateFin':'an_fermeture',
                        'uniteLegale.identifiantAssociationUniteLegale':'numero_rna',
                        'uniteLegale.denominationUniteLegale':'nom_long',
                        'uniteLegale.sigleUniteLegale':'sigle',
                        'adresseEtablissement.complementAdresseEtablissement':'Lieudit_BP',
                        'adresseEtablissement.codePostalEtablissement':'code_postal',
                        'adresseEtablissement.libelleCommuneEtablissement':'ville',
                        'adresseEtablissement.codeCommuneEtablissement':'com_code',
                        'adresseEtablissement.codePaysEtrangerEtablissement':'COG'
                        })
        .assign(ref='sirene')
        .drop_duplicates())

    sirene.mask(sirene=='', inplace=True)

    country_s = pd.read_csv(f"{DUMP_PATH}v_pays_territoire_2024.csv", sep=',', dtype=str)[['COG', 'CRPAY','CODEISO2']]
    country_s=country_s.merge(country_s[['COG', 'CODEISO2']].drop_duplicates(), how='left', left_on='CRPAY', right_on='COG', suffixes=('','_'))
    country_s.loc[~country_s.CRPAY.isnull(), 'CODEISO2'] = country_s.CODEISO2_
    country_s.drop(columns=['COG_', 'CODEISO2_', 'CRPAY'], inplace=True)
    sirene = sirene.merge(country_s, how='left', on='COG').rename(columns={'CODEISO2':'iso2'})
    sirene = sirene.merge(countries[['iso2', 'iso3', 'parent_iso3']], how='left', on='iso2')
    sirene = sirene.merge(countries[['parent_iso3', 'country_name_en']], how='left', on='parent_iso3')
    sirene.loc[sirene.parent_iso3.isnull(), 'parent_iso3'] = 'FRA'
    sirene.rename(columns={'iso3':'country_code_map', 'parent_iso3':'country_code'}, inplace=True)
    # sirene.loc[sirene.country_code_map.isnull(), 'country_code_map'] = 'FRA'
    if len(sirene[(~sirene.COG.isnull())&((sirene.iso2.isnull())|(sirene.country_code_map.isnull()))])>0:
        print(f"siren without country_code_map: {sirene[(~sirene.COG.isnull())&((sirene.iso2.isnull())|(sirene.country_code_map.isnull()))].siren.unique()}")

    sirene.mask(sirene=='', inplace=True)
    return sirene