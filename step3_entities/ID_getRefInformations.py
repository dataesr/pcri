from config_api import sirene_headers, scanr_headers, paysage_headers
from config_path import PATH_SOURCE, PATH_WORK, PATH_REF
from api_requests.ror import *
from api_requests.sirene import *
from api_requests.paysage import *
from dotenv import load_dotenv
load_dotenv()

def ID_getRefInfo(lid_source):
    print("### ROR data")
    r=get_ror(lid_source, ror_old=None)
    ror=ror_cleaning(r)
    file_name = f"{PATH_REF}ror_df.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(ror, file)

    siren_siret = get_siret_siege(lid_source)
    paysage, paysage_category, paysage_mires = get_paysage(lid_source, siren_siret, paysage_old=None)
    sirene = get_sirene(lid_source, sirene_old=None)

    return ror, paysage, paysage_category, paysage_mires, sirene

def ror_getRefInfo(lid_source):
    print("### ROR data")
    r=get_ror(lid_source, ror_old=None)
    ror=ror_cleaning(r)
    file_name = f"{PATH_REF}ror_df.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(ror, file)

def paysage_getRefInfo(lid_source, siren_siret, paysage_old=None):
    print("### PAYSAGE HARVEST")
    paysage_id=ID_to_IDpaysage(lid_source, siren_siret)
    paysage_id, doublon=IDpaysage_status(lid_source, paysage_id)
    paysage=IDpaysage_successor(paysage_id)
    paysage=IDpaysage_parent(paysage)
    paysage=IDpaysage_cj(paysage)
    paysage=IDpaysage_name(paysage)
    paysage=IDpaysage_siret(paysage)
    check_var_null(paysage)
    paysage_category=IDpaysage_category(paysage)
    paysage_mires=get_mires()

    x=paysage.loc[~paysage.id_clean.isin(paysage.id.unique())]
    x.loc[:,'id']=x.loc[:,'id_clean']
    x=x.drop_duplicates()
    paysage = pd.concat([paysage, x], ignore_index=True)

    if 'paysage_old' in globals() or 'paysage_old' in locals():
        tmp=pd.concat([paysage, paysage_old], ignore_index=True).drop_duplicates()
        print(f"1 - paysage_old + paysage -> new size :{len(tmp)}")
        
        file_name = f"{PATH_REF}paysage_df.pkl"
        with open(file_name, 'wb') as file:
            pd.to_pickle(tmp, file) 
    else:
        file_name = f"{PATH_REF}paysage_df.pkl"
        with open(file_name, 'wb') as file:
            pd.to_pickle(paysage, file)
    
    return paysage, paysage_category, paysage_mires