import pandas as pd, numpy as np
from config_path import PATH_REF

def ref_source_load(sheet_load: str):
    ref_source = pd.read_excel(f"{PATH_REF}_id_pic_entities.xlsx", dtype=object, keep_default_na=False, sheet_name = sheet_load) 
    # ref_source = ref_source.rename(columns={'country_code': 'countryCode_parent'})
    ref_source['proposal'] = ref_source['proposal'].replace({'': np.nan})
    ref_source['project'] = ref_source['project'].replace({'': np.nan})
    ref_source.mask(ref_source=='', inplace=True)
    print(f"size of ref_source : {len(ref_source)}")

    if ref_source is not None:
        ref = ref_source.loc[(~ref_source.ZONAGE.isnull())|(~ref_source.id.isnull()),['generalPic', 'id', 'country_code_mapping', 'ZONAGE']].drop_duplicates()
        ref['id'] = ref['id'].astype(str)
        print(f"longueur de ref:{len(ref)}")

        ref['nb'] = ref.groupby(['generalPic', 'country_code_mapping'], dropna=False)['id'].transform('nunique')
        if len(ref[ref.nb>1]): 
            print(f"doublon generalPic+countryCode\n{ref[ref.nb>1]['id'].unique()}")

        ref.sort_values('id', inplace=True)
        ref['id'] = ref.groupby('generalPic')['id'].ffill()
        
        ref['nb'] = ref.groupby(['generalPic', 'country_code_mapping'], dropna=False)['id'].transform('nunique')
        if len(ref[ref.nb>1]): 
            print(f"doublon generalPic+countryCode\n{ref[ref.nb>1]['id'].unique()}")

    return ref_source