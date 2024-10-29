import requests, time, re, copy
from config_api import sirene_headers, scanr_headers, paysage_headers
from config_path import PATH_SOURCE, PATH_WORK, PATH_REF
from Api_requests.ror import *
from dotenv import load_dotenv
load_dotenv()

def get_ror(lid_source):
    print("### ROR")
    ### traitement identifiant ROR
    ror_list = [e['api_id'][1:] for e in lid_source if e['source_id']=='ror']
    print(f"nombre d'identifiants ror à extraire: {len(ror_list)}")
    # ror2=ror_info(ror_list)

    ror_result=[]
    n=0

    for id in ror_list:
        n=n+1
        print(f"{n}", end=',')
        ror_result.append(ror_query(id))  

    while None in ror_result:
        ror_result.remove(None)   
        
    if ror_result:    
        file_name = f"{PATH_WORK}ror_current.pkl"
        with open(file_name, 'wb') as file:
            pd.to_pickle(ror_result, file)
            
    ror_result=ror_relation(ror_result)

    ror_df=ror_info(ror_result)
    ror_df=pd.json_normalize(ror_df)

    if ror_df.empty:
        print("ror_df est vide")
    else:
        if 'ror_old' in globals() or 'ror_old' in locals():
            r = pd.concat([ror_old, ror_df], ignore_index=True)
        else:
            r = copy.deepcopy(ror_df)
            
        file_name = f"{PATH_SOURCE}ror_df.pkl"
        with open(file_name, 'wb') as file:
            pd.to_pickle(r, file)
    return r