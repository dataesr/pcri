from config_api import sirene_headers
from step3_entities.ID_getSourceRef import sourcer_ID
from config_path import PATH_SOURCE, PATH_API, PATH_REF
import time, requests, pandas as pd
from dotenv import load_dotenv
load_dotenv()


def siren_liste(lid_source):
    sl=list(set([i['api_id'] for i in lid_source if i['source_id'] in ['siren']]))
    print(f"- nombre d'identifiants paysage à extraire: {len(sl)}")
    return sl
###############################

def get_siret_siege(lid_source):
    
    print("### harvest siret siege from siren")
    print(time.strftime("%H:%M:%S"))
    sl=siren_liste(lid_source)
    siren_siret=[]
    n=0

    for i in sl:
        time.sleep(0.2)
        n=n+1
        if n % 100 == 0: 
            print(f"{n}", end=',')
                
        url=f'https://api.insee.fr/entreprises/sirene/siret?q=siren:{i} AND etablissementSiege:true'  
        rinit = requests.get(url, headers=sirene_headers, verify=False)
        global rinit_status
        rinit_status = rinit.status_code
        if rinit_status == 200:
            siren_siret.append(rinit.json()['etablissements'][0].get('siret'))
    
    file_name = f"{PATH_API}siren_siret.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(siren_siret, file)
    print(len(siren_siret))
    print(time.strftime("%H:%M:%S"))
    return siren_siret
####################################

def get_sirene(lid_source, sirene_old=None):
    print("### SIRENE")
    print(time.strftime("%H:%M:%S"))
    sirene_liste = [i['api_id'] for i in lid_source if i['source_id'] in ['siren', 'siret','identifiantAssociationUniteLegale']]

    sirene_liste = sourcer_ID(list(set(sirene_liste)))  
    print(f"- nombre d'identifiants de entities avec sirene {len(sirene_liste)}")

    result = []
    n=0

    def get_last_info_siret(x):
        tmp = [e for e in x if e.get('date_fin') is None]
        tmp = sorted(tmp, key=lambda k: k['date_debut'], reverse=True)
        if len(tmp)>0:
            return tmp[0]
        tmp = sorted(x, key=lambda k: k['date_fin'], reverse=True)
        if len(tmp)>0:
            return tmp[0]
        return {}

    for i in sirene_liste:
        n=n+1
        if n % 100 == 0: 
            print(f"{n}", end=',')
        try:
            if i['source_id'] == 'siret':
                url = 'https://api.insee.fr/entreprises/sirene/siret?q=siret:' + str(i['api_id'])
            else:
                url='https://api.insee.fr/entreprises/sirene/siret?q=' + i['source_id'] + ':' + str(i['api_id']) + ' AND etablissementSiege:true'  
            rinit = requests.get(url, headers=sirene_headers, verify=False)
            
            global rinit_status
            rinit_status = rinit.status_code
            if rinit_status == 200:
        #         time.sleep(1.5)
                now = time.strftime("%H:%M:%S")
                r2 = rinit.json()['etablissements'][0]
        #         print(r2)
                response = {   
                    "siren": str(r2.get("siren")),
                    "siret": str(r2.get("siret")),
                    "siege": bool(r2.get("etablissementSiege")),    
                    "etat_ul": r2.get('uniteLegale').get("etatAdministratifUniteLegale"),
                    "sigle": r2.get('uniteLegale').get("sigleUniteLegale"),
                    "nom_ul": r2.get('uniteLegale').get("denominationUniteLegale"),
                    "nom_pp": r2.get('uniteLegale').get("nomUniteLegale"),
                    "prenom":r2.get('uniteLegale').get("prenom1UniteLegale"),
                    "cat": r2.get('uniteLegale').get("categorieEntreprise"),
                    "cat_an": r2.get('uniteLegale').get("anneeCategorieEntreprise"),
                    "cj": str(r2.get('uniteLegale').get("categorieJuridiqueUniteLegale")),
                    "naf_ul": r2.get('uniteLegale').get("activitePrincipaleUniteLegale"),                         
                    "rna": r2.get('uniteLegale').get("identifiantAssociationUniteLegale")
                    }

                period = r2.get("nombrePeriodesEtablissement")
                response_for_this_siret = [] 
                for j in range(period):
                    rj = r2.get('periodesEtablissement')[j]
                    responsej = {  
                        "etat_et": rj.get("etatAdministratifEtablissement"),
                        "ens1": rj.get('enseigne1Etablissement'),
                        "ens2": rj.get('enseigne2Etablissement'),
                        "ens3": rj.get('enseigne3Etablissement'),
                        "denom_us":rj.get('denominationUsuelleEtablissement'),
                        "naf_et": rj.get("activitePrincipaleEtablissement"),                              
                        "date_debut": rj.get("dateDebut"),
                        "date_fin": rj.get("dateFin")
                        }
                    response_for_this_siret.append(responsej)
                response_siret = get_last_info_siret(response_for_this_siret)
                response.update(response_siret)
        #         print(response_siret)
                result.append(response)

        except requests.exceptions.HTTPError as http_err:
            print(f"\n{i} -> HTTP error occurred: {http_err}")
            sirene_liste.append(str(i))
        except requests.exceptions.RequestException as err:
            print(f"\n{i} -> Error occurred: {err}")
            sirene_liste.append(str(i))
        except Exception as e:
            print(f"\n{i} -> An unexpected error occurred: {e}")
            sirene_liste.append(str(i)) 

    sirene=pd.DataFrame(result)
    print(f"\n- size sirene: {len(sirene)}")
    print(time.strftime("%H:%M:%S"))

    if 'sirene_old' in globals() or 'sirene_old' in locals():
        tmp=pd.concat([sirene, sirene_old], ignore_index=True).drop_duplicates()
        print(f"new size sirene:{len(tmp)}")
        
        file_name = f"{PATH_REF}sirene_df.pkl"
        with open(file_name, 'wb') as file:
            pd.to_pickle(tmp, file) 
    else:
        file_name = f"{PATH_REF}sirene_df.pkl"
        with open(file_name, 'wb') as file:
            pd.to_pickle(sirene, file)
    return sirene