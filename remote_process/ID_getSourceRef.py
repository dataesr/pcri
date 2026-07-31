import pandas as pd, re

def sourcer_ID(df_list: list, var_id: str) -> pd.DataFrame:
    """
    Identify source pof ID from ID specific format
    """

    print("### sourcer les identifiants pour getInformations")
    source = {
        '^[0-9]{9}$': 'siren',
        '^[0-9]{9}[A-Z]{1}$': 'rnsr',
        '^R0([a-z0-9]{6})[0-9]{2}$': 'ror',
        '^0([a-z0-9]{6})[0-9]{2}$': 'ror',
        '^[a-zA-Z0-9]{5}$': 'paysage',
        '^pic[0-9]{9}$': 'pic',
        '^[0-9]{9}-[A-Z]{2,3}$': 'pic',
        '^[0-9]{7}[A-Z]{1}': 'uai',
        '^grid': 'grid',
        '^F[0-9]{2}([a-zA-Z0-9]{7})': 'finess',
        '^[0-9]{14}$': 'siret',
        '^[W|w]([A-Z0-9]{8})[0-9]{1}$': 'rna'
    }

    result = []
    for i in df_list:
        for k, v in source.items():
            if re.match(k, str(i), flags=0):
                result.append({var_id: i, 'source_id': v})
                break

    return pd.DataFrame(result)


def get_source_ID(df, var_id):
    """
    get source ID using the source_id function for var_id parameter
    and merge with the original df
    """
    l = list(set(df[var_id].str.strip().unique()))   
    l = sourcer_ID(l, var_id)
    l = pd.DataFrame(l)
    return pd.merge(df, l, how='left', on=var_id)


def fix_source_from_siren_to_ror(df, var):
    """
    fix confusion in source between SIREN and ROR for ID starting with 0 in non-French entities
    """
    df.loc[(df['country_code']!='FRA')&(df['source_id']=='siren')&(df[var].str.startswith('0', na=False)), 'source_id'] = 'ror'
    return df


def source_ID_new_and_check(df, var_id, fix_bug=False):
    df = df.rename(columns={'source_id':'source_id_source'})
    df = get_source_ID(df, var_id)
    if any(df['source_id_source']!=df['source_id']):
        mask = (df['source_id']!='paysage')&(df['source_id_source'].notnull())&(df['source_id_source']!=df['source_id'])    
        print(f"### 🔶 source_id different après merge ref_source\n{df.loc[mask, ['legalName', 'id_extend', 'source_id_source', 'source_id']]}")
        if fix_bug==True:
            print("### 🔶 the diffenrence are fixed by taking the old source_id")
            df.loc[mask, 'source_id'] = df.loc[mask, 'source_id_source']
    return df