
# traitement SIRENE

from retry import retry
@retry(delay=100, tries=3)
def sirene_import(DUMP_PATH):
    import requests, pandas as pd
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)  
    from config_api import sirene_headers
    from config_path import PATH_REF
    from retry import retry
    @retry(delay=100, tries=3)

    def get_last_info_siret(x):
        tmp = [e for e in x if e.get('dateFin') is None]
        tmp = sorted(tmp, key=lambda k: k['dateDebut'], reverse=True)
        if len(tmp)>0:
            return tmp[0]
        tmp = sorted(x, key=lambda k: k['dateFin'], reverse=True)
        if len(tmp)>0:
            return tmp[0]
        return {}

    def harvest_data(r):
        period = r.get("nombrePeriodesEtablissement")
        response_for_this_siret = [] 
        for j in range(period):
            try:
                rj = r.get('periodesEtablissement')[j]
                response_for_this_siret.append(rj)
            except:
                pass
        response_siret = get_last_info_siret(response_for_this_siret)
        r.pop('periodesEtablissement')
        r.update(response_siret)
        return r

    S_PKL = pd.read_pickle(f'{PATH_REF}sirene_df.pkl').sort_values('naf_et')
    S_PKL = S_PKL.loc[~S_PKL.naf_et.isnull()].naf_et.unique()

    CHAMPS="""siren, dateCreationUniteLegale, siret, dateCreationEtablissement, sigleUniteLegale, denominationUniteLegale, 
    nomUniteLegale,
    prenom1UniteLegale, categorieEntreprise,categorieJuridiqueUniteLegale,activitePrincipaleUniteLegale,
    identifiantAssociationUniteLegale,
    codeCommuneEtablissement,numeroVoieEtablissement,typeVoieEtablissement, libelleVoieEtablissement,complementAdresseEtablissement,
    codePostalEtablissement,libelleCommuneEtablissement,codePaysEtrangerEtablissement,libelleCommuneEtrangerEtablissement,
    enseigne1Etablissement,enseigne2Etablissement,enseigne3Etablissement,denominationUsuelleEtablissement,activitePrincipaleEtablissement,
    dateDebut,nombrePeriodesEtablissement,statutDiffusionEtablissement""".replace('\n','')

    for i in S_PKL:
        q=f'periode(etatAdministratifEtablissement:A AND activitePrincipaleEtablissement:{i}) AND etablissementSiege:true'
        url=f'https://api.insee.fr/entreprises/sirene/siret?q={q}&nombre=1000&champs={CHAMPS}&curseur=*' 
        rinit = requests.get(url,  headers=sirene_headers, verify=False)
        max_rec = rinit.json().get("header").get("total")
        print(f"{rinit.json().get('header')}, {q}")

        result = []
        if rinit.status_code == 200:
            r=rinit.json()['etablissements']
            nb=int(rinit.json().get("header").get("nombre"))
            try:
                if max_rec <= 1000:
                    for e in range(0, nb):
                        result.append(harvest_data(r[e]))

                if max_rec > 1000:   
                    while nb > 0:
                        for e in range(0, nb):
                            result.append(harvest_data(r[e]))

                        cur = rinit.json().get('header').get('curseurSuivant')
                        url=f'https://api.insee.fr/entreprises/sirene/siret?q={q}&nombre=1000&champs={CHAMPS}&curseur={cur}' 
                        rinit = requests.get(url,  headers=sirene_headers, verify=False)
                        r=rinit.json()['etablissements']
                        nb=int(rinit.json().get("header").get("nombre"))
            
            except requests.exceptions.HTTPError as http_err:
                print(f"\n{i} -> HTTP error occurred: {http_err}")
            except requests.exceptions.RequestException as err:
                print(f"\n{i} -> Error occurred: {err}")
            except Exception as e:
                print(f"\n{i} -> An unexpected error occurred: {e}")
                    
        pd.json_normalize(result).to_pickle(f"{DUMP_PATH}sirene/sirene_{i}.pkl")

def sirene_full(DUMP_PATH):
    import os, pandas as pd
    p=f"{DUMP_PATH}sirene/"
    file_import=[]
    for i in os.listdir(p):
        if os.path.isfile(os.path.join(p,i)) and 'sirene_' in i:
            delete=['47','55','56', '60', '68','77','78','79', '81','92','93','96', '97', '98']
            if (i.split('_')[1][0:2] not in delete) & (i.split('_')[1][0:5] not in delete) :
                print(i)
                df2=pd.read_pickle(f"{p}{i}")
                file_import.append(df2)
    x=pd.concat(file_import, ignore_index=True)
    x.to_pickle(f"{DUMP_PATH}sirene_ref.pkl")
    return x

def sirene_prep(DUMP_PATH, sirene_id_list, countries):
    import pandas as pd
    from functions_shared import com_iso3
    df = pd.read_pickle(f"{DUMP_PATH}sirene_ref.pkl")

    df = df.loc[df['statutDiffusionEtablissement']!='P']

    delete=["02",	"06",	"07",	"08",	"09",	"11",	"14",	"15",	"18",	"19",	"31",	"36",	
            "37",	"39",	"45",	"50",	"51",	"53",	"55",	"56",	"60",	"65",	"75",	"78",	
            "79",	"80",	"81",	"87",	"92",	"95",	"96",	"99"]
    sirene=df.loc[df.naf_et.str[0:2].isin(delete)]

    for i in sirene_id_list:
        if any(sirene.siren==i) | any(sirene.siret==i) | any(sirene['uniteLegale.identifiantAssociationUniteLegale']==i):
            pass
        else:
            sirene=pd.concat([sirene, df[(df.siren==i)|(df.siret==i)|(df['uniteLegale.identifiantAssociationUniteLegale']==i)]], ignore_index=True)
        

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

    sirene = sirene.merge(countries[['iso2', 'iso3']], how='left', on='iso2')
    com_iso=com_iso3()
    sirene = sirene.merge(com_iso, how='left', on='com_code')
    sirene.loc[~sirene.iso_3.isnull(), 'iso3'] = sirene.loc[~sirene.iso_3.isnull(), 'iso_3'] 
    sirene.loc[sirene.iso3.isnull(), 'iso3'] = 'FRA'

    sirene = sirene.merge(countries[['iso3','parent_iso3']], how='left', on='iso3')
    sirene = sirene.rename(columns={'iso3':'country_code_map', 'parent_iso3':'country_code'})        
    sirene = sirene.merge(countries[['iso3', 'country_name_en']].drop_duplicates(), how='left', left_on='country_code', right_on='iso3')

    if len(sirene[(~sirene.COG.isnull())&((sirene.iso2.isnull())|(sirene.country_code_map.isnull()))])>0:
        print(f"siren without country_code_map: {sirene[(~sirene.COG.isnull())&((sirene.iso2.isnull())|(sirene.country_code_map.isnull()))].siren.unique()}")

    
    sirene.drop(columns=['iso_3', 'iso3', 'iso2','COG','Lieudit_BP'], inplace=True)

    sirene.mask(sirene=='', inplace=True)
    return sirene