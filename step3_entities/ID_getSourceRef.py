def sourcer_ID(df_id:list) -> list:
    import re
    print("### sourcer les identifiants pour getInformations")
    liste = list(set(df_id))
    source = {'^[0-9]{9}$':'siren', 
              '^[0-9]{14}$':'siret', 
              '^[W|w]([A-Z0-9]{8})[0-9]{1}$':'identifiantAssociationUniteLegale', 
             '^[0-9]{9}[A-Z]{1}$':'rnsr', 
             '^R0([a-zA-Z0-9]{6})[0-9]{2}$':'ror', 
             '^([a-zA-Z0-9]{5})$':'paysage', 
              '^pic[0-9]{9}$':'pic', 
              '^[0-9]{7}[A-Z]{1}':'uai', 
              '^grid':'grid', 
              '^F[0-9]{2}([a-zA-Z0-9]{7})':'finess'}
    
    id_liste = []
    for i in liste:    
        for k in source:
            if re.match(k, str(i), flags=0):
                id_liste.append({'api_id': i, 'source_id': source[k]})
    return id_liste


def get_source_ID(df, var_id):
    import pandas as pd
    l=list(set(df[var_id].unique()))   
    l=sourcer_ID(l)
    return pd.DataFrame(l).rename(columns={'api_id':var_id})