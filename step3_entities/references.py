import pandas as pd, numpy as np
from config_path import PATH_REF

def ref_source_load(sheet_load: str):
    print("\n### LOADING REF_SOURCE")
    if 'ref_source' not in globals():
        ref_source = pd.read_excel(f"{PATH_REF}_id_pic_entities.xlsx", dtype=object, keep_default_na=False, sheet_name = sheet_load) 
        ref_source.mask(ref_source=='', inplace=True)
        print(f"- size of ref_source : {len(ref_source)}")
    else:
        print('- ref_source already load')
    return ref_source

def ref_source_1ere_select(ref_source):
    print("### 1er - REF_SOURCE -> REF")
    ref = ref_source.loc[(~ref_source.ZONAGE.isnull())|(~ref_source.id.isnull()),['generalPic', 'id', 'id_secondaire', 'country_code_mapping', 'ZONAGE']].drop_duplicates()
    ref['id'] = ref['id'].astype(str)
    print(f"- longueur de ref:{len(ref)}")

    ref['nb'] = ref.groupby(['generalPic', 'country_code_mapping'], dropna=False)['id'].transform('nunique')
    if len(ref[ref.nb>1]): 
        print(f"1- doublon generalPic+countryCode\n{ref[ref.nb>1]['id'].unique()}")

    ref.sort_values('id', inplace=True)
    ref['id'] = ref.groupby('generalPic')['id'].ffill()
    
    ref['nb'] = ref.groupby(['generalPic', 'country_code_mapping'], dropna=False)['id'].transform('nunique')
    if len(ref[ref.nb>1]): 
        print(f"2- doublon generalPic+countryCode\n{ref[ref.nb>1]['id'].unique()}")
    return ref


def ref_source_2d_select(ref_source, FP_SELECT:str):
    print("### 2d - REF_SOURCE -> REF")

    #table correspondance old_pic to new
    ref_source = ref_source.rename(columns={"countryCode_parent":'country_code'})
    ref_source.loc[(ref_source.id.str.contains('-'))&(ref_source.pic_new.isnull()), 'pic_new'] = ref_source[(ref_source.id.str.contains('-'))&(ref_source.pic_new.isnull())].id.str.split('-').str[0]
    genPic_to_new = ref_source.loc[~ref_source.pic_new.isnull(), ['generalPic', 'pic_new', 'country_code_mapping']]
    print(f"- size remplacement pic: {len(genPic_to_new)}")

    ref = ref_source.loc[(ref_source.FP.str.contains(FP_SELECT)) & ((~ref_source.ZONAGE.isnull())|(~ref_source.id.isnull())|(~ref_source.id_secondaire.isnull())),['generalPic', 'id', 'id_secondaire', 'country_code_mapping', 'country_code', 'ZONAGE']].drop_duplicates()
    print(f"- longueur de ref:{len(ref)}")

    ref['nb'] = ref.groupby(['generalPic', 'country_code_mapping'], dropna=False)['id'].transform('nunique')
    if len(ref[ref.nb>1]): 
        print(f"1 - doublon generalPic+countryCode\n{ref[ref.nb>1][['generalPic', 'country_code_mapping']].drop_duplicates()}")
    print(f"- nb id: {ref.loc[~ref.id.isnull(), 'nb'].sum()}")
    
    ref.sort_values(['id','country_code_mapping'], inplace=True)
    ref['id'] = ref.groupby(['generalPic', 'country_code_mapping'])['id'].ffill()

    ref['nb'] = ref.groupby(['generalPic', 'country_code_mapping'], dropna=False)['id'].transform('nunique')
    if len(ref[ref.nb>1]): 
        print(f"2- doublon generalPic+country_code_mapping\n{ref[ref.nb>1][['generalPic', 'country_code_mapping']].drop_duplicates()}")
    print(f"- nb id after fill: {ref[~ref.id.isnull()].nb.sum()}")
    
    ref.loc[ref.id=='0', 'id'] = np.nan

    return ref.drop(columns='nb'), genPic_to_new