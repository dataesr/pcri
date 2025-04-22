import pandas as pd

from dotenv import load_dotenv
load_dotenv()


def ror_getRefInfo(lid_source, countries):
    from config_path import PATH_REF
    from api_process.ror import get_ror, ror_cleaning
    print("### ROR data")
    r=get_ror(lid_source, ror_old=None)
    ror=ror_cleaning(r)
    ror = (ror
        .merge(countries[['countryCode', 'countryCode_iso3']], 
                how='left', left_on='iso2', right_on='countryCode')
        .drop(columns=['countryCode', 'iso2'])
        .rename(columns={'countryCode_iso3':'country_code_mapping'}))
    file_name = f"{PATH_REF}ror_df.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(ror, file)

def paysage_getRefInfo(paysage_id, paysage_old=None):
    from config_path import PATH_REF
    from api_process.paysage import IDpaysage_cj,IDpaysage_name,IDpaysage_parent,IDpaysage_siret,IDpaysage_status,IDpaysage_successor,check_var_null,get_mires
    print("### PAYSAGE HARVEST")
    
    paysage_id, doublon=IDpaysage_status(paysage_id)
    paysage=IDpaysage_successor(paysage_id)
    paysage=IDpaysage_parent(paysage)
    paysage=IDpaysage_cj(paysage)
    paysage=IDpaysage_name(paysage)
    paysage=IDpaysage_siret(paysage)
    check_var_null(paysage)
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
    
    return paysage, paysage_mires

def ID_getRefInfo(lid_source):
    from config_path import PATH_REF
    from api_process.ror import get_ror, ror_cleaning
    from api_process.sirene import get_sirene, get_siret_siege

    print("### ROR data")
    r=get_ror(lid_source, ror_old=None)
    ror=ror_cleaning(r)
    file_name = f"{PATH_REF}ror_df.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(ror, file)

    siren_siret = get_siret_siege(lid_source)
    paysage, paysage_category, paysage_mires = paysage_getRefInfo(lid_source, siren_siret, paysage_old=None)
    sirene = get_sirene(lid_source, sirene_old=None)

    return ror, paysage, paysage_category, paysage_mires, sirene