import pandas as pd
from config_path import PATH_HARVEST
from remote_process.paysage import IDpaysage_info,IDpaysage_parent,IDpaysage_siret,IDpaysage_successor,check_var_null
from dotenv import load_dotenv
load_dotenv()

def paysage_getRefInfo():

    print("### PAYSAGE HARVEST")
    

    paysage_successor = IDpaysage_successor()
    paysage_relation = IDpaysage_parent()
    # paysage=IDpaysage_cj(paysage)
    paysage_infos = IDpaysage_info()
    paysage_siret = IDpaysage_siret()

    paysage = pd.merge(paysage_infos[['id']], paysage_successor, how='left', on='id')
    paysage.loc[paysage['id_succ'].isnull(), 'id_succ'] = paysage['id']
    paysage = (pd.merge(paysage, 
                        paysage_relation.rename(columns={'id':'id_succ'}), 
                        how='left', on='id_succ')
    )
    paysage.loc[paysage['id_parent'].isnull(), 'id_parent'] = paysage['id_succ']

    paysage = paysage[['id', 'id_parent']].drop_duplicates().rename(columns={'id_parent':'id_clean', 'id':'id_source'})
    paysage = (pd.merge(paysage, 
                        paysage_infos, how='left', left_on='id_clean', right_on='id')
               .drop(columns=['id'])
               .drop_duplicates()
               )
    print(f"- size paysage: {len(paysage)}")


    # print(f"- size paysage after siret: {len(paysage)}")
    check_var_null(paysage)

    # if len(paysage_infos.loc[paysage_infos.cj_code.isnull()])>0:
    #     print(f"\nSANS CJ -> à compléter dans paysage\n{paysage_infos.loc[paysage_infos.cj_code.isnull()].id_parent.unique()}")
        # paysage_siret['nb'] = paysage_siret.groupby('id_extend')['id_clean'].transform('count')
        # if len(paysage[paysage.nb>1])>0:
        #     print(f"\ndoublons dans paysage à régler à la source -> {paysage[paysage.nb>1][['id_extend', 'id_clean', 'name_clean']]}")

    file_name = f"{PATH_HARVEST}paysage_df.pkl"
    # if df_old==True:
    #     paysage_old=pd.read_pickle(file_name)
    #     paysage=pd.concat([paysage, paysage_old], ignore_index=True).drop_duplicates()
    #     print(f"1 - paysage_old + paysage -> new size :{len(paysage)}")
        
    #     with open(file_name, 'wb') as file:
    #         pd.to_pickle(paysage, file) 
    # else:
    with open(file_name, 'wb') as file:
        pd.to_pickle(paysage, file)

    return paysage, paysage_siret

def ID_getRefInfo(lid_source):
    from config_path import PATH_REF
    from remote_process.ror import get_ror, ror_cleaning
    from remote_process.sirene import get_sirene, get_siret_siege

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

# def new_search(ref, df):
#     if ref=='paysage':
#         from step3_entities.ID_getRefInformations import paysage_getRefInfo
#         from remote_process.paysage import IDpaysage_category

#         paysage = paysage_getRefInfo(df, df_old=True)
#         print(f"1 - paysage_old + paysage -> new size :{len(paysage)}")
#         pc = paysage.loc[paysage['id_extend'].isin(list(df['id_extend']))]
#         paysage_category = IDpaysage_category(pc, df_old=True)
              
#         return paysage, paysage_category