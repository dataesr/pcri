import pandas as pd, numpy as np, requests, time
from paths import PATH_REF
from config_api import paysage_headers

def ref_source_load(sheet_load: str):
    """
    ref_source contains all pic+country pairs since FP7 ; that work base is in local with more 500k rows
    for all pairs we try tpo find an ID in repository like SIRENE, ROR, RNSR, RNA...
    """
    print("\n### LOADING REF_SOURCE")
    ref_source = pd.read_excel(f"{PATH_REF}_id_pic_entities.xlsx", dtype=object, keep_default_na=False, decimal='.', sheet_name = sheet_load) 
    ref_source.mask(ref_source=='', inplace=True)
    ref_source['project'] = pd.to_numeric(ref_source['project'], errors='coerce')
    ref_source['proposal'] = pd.to_numeric(ref_source['proposal'], errors='coerce')
    print(f"- size of ref_source : {len(ref_source)}")
    return ref_source


def ref_source_1ere_select(ref_source):
    """
    from ref_source select only lines with id or zonage, 
    and keep only relevant columns to create ref table for first update of entities
    
    """
    print("## 1er - REF_SOURCE -> REF")
    ref = (ref_source.loc[(~ref_source.ZONAGE.isnull())|(~ref_source.id.isnull()),
                          ['generalPic', 'id', 'id_secondaire', 'country_code_source', 'countryCode_parent', 'ZONAGE']]
                    .rename(columns={'countryCode_parent':'country_code'})
                    .drop_duplicates())
    
    ref['id'] = ref['id'].astype(str)
    print(f"- size ref:{len(ref)}")

    ref['nb'] = ref.groupby(['generalPic', 'country_code_source'], dropna=False)['id'].transform('nunique')
    if len(ref[ref.nb>1]): 
        print(f"1- Duplicated generalPic+countryCode\n{ref[ref.nb>1]['id'].unique()}")

    ref.sort_values('id', inplace=True)
    ref['id'] = ref.groupby('generalPic')['id'].ffill()
    
    ref['nb'] = ref.groupby(['generalPic', 'country_code_source'], dropna=False)['id'].transform('nunique')
    if len(ref[ref.nb>1]): 
        print(f"2- Duplicated generalPic+countryCode\n{ref[ref.nb>1]['id'].unique()}")
    return ref


def ref_source_2d_select(ref_source, FP_SELECT:list):
    """
    keep ref for h2020 and HE with IDs or/and zonage not null
    create a table to manage pic_new 
    returns:
        ref selected 
        gen_pic_new
    
    """

    print("## 2d - REF_SOURCE -> REF")
        #table correspondance old_pic to new
    ref_source = ref_source.rename(columns={"countryCode_parent":'country_code'})
    ref_source.loc[(ref_source.id.str.contains('-'))&(ref_source.pic_new.isnull()), 'pic_new'] = ref_source[(ref_source.id.str.contains('-'))&(ref_source.pic_new.isnull())].id.str.split('-').str[0]
    ref_source.loc[ref_source['project'].isnull(), 'project'] = 0
    ref_source['project'] = ref_source['project'].astype(int)
    
    genPic_to_new = ref_source.loc[~ref_source.pic_new.isnull(), ['generalPic', 'pic_new', 'country_code_source', 'project']]
    print(f"- size remplacement pic: {len(genPic_to_new)}")

    ref = (ref_source
           .loc[(ref_source['FP'].str.contains("|".join(FP_SELECT), na=False)) & ((~ref_source['ZONAGE'].isnull())|(~ref_source.id.isnull())|(~ref_source.id_secondaire.isnull())),
                ['generalPic', 'id', 'id_secondaire', 'country_code_source', 'country_code', 'ZONAGE', 'source_id', 'project']]
                .drop_duplicates())
    print(f"- longueur de ref:{len(ref)}")

    ref['nb'] = ref.groupby(['generalPic', 'country_code_source'], dropna=False)['id'].transform('nunique')
    if len(ref[ref.nb>1]): 
        print(f"1 - doublon generalPic+countryCode\n{ref[ref.nb>1][['generalPic', 'country_code_source']].drop_duplicates()}")
    print(f"- nb id: {ref.loc[~ref.id.isnull(), 'nb'].sum()}")
    
    ref.sort_values(['id','country_code_source'], inplace=True)
    ref['id'] = ref.groupby(['generalPic', 'country_code_source'])['id'].ffill()

    ref['nb'] = ref.groupby(['generalPic', 'country_code_source'], dropna=False)['id'].transform('nunique')
    if len(ref[ref.nb>1]): 
        print(f"2- doublon generalPic+country_code_source\n{ref[ref.nb>1][['generalPic', 'country_code_source']].drop_duplicates()}")
    print(f"- nb id after fill: {ref[~ref.id.isnull()].nb.sum()}")
    
    ref.loc[ref.id=='0', 'id'] = np.nan

    return ref.drop(columns='nb'), genPic_to_new


def paysage_id_extract(source_list: list):
    """
    extract from paysage all identifiers of type in source_list, 
    and return a dataframe with all identifiers and their resourceId, active and endDate    
    """
    id_df=pd.DataFrame()
    for s in source_list:
        try:
            response = requests.get(f"https://api.paysage.dataesr.ovh/identifiers?filters[type]={s}", headers=paysage_headers)
            response.raise_for_status()
            result = response.json()
            nbTotal = result['totalCount']
            if nbTotal:
                response = requests.get(f"https://api.paysage.dataesr.ovh/identifiers?filters[type]={s}&limit={nbTotal}", headers=paysage_headers)
                response.raise_for_status()
                result = response.json()
                res = pd.DataFrame(result["data"])
                time.sleep(1)
        except requests.exceptions.HTTPError as errh:
            print("id > KO : ", response.status_code)
        id_df = pd.concat([id_df, res], ignore_index=True)
    return id_df


def paysage_id_extract_prepare(id_df):
    """
    prepare paysage extract to have a dataframe with all identifiers and their resourceId, active and endDate,
    convert siret into siren (9) and keep only one line per identifier with the most recent active one if several exist
    """
    tmp = id_df[id_df['type']=='siret'].assign(check_id=id_df['value'].str[:9]).drop(columns=['value'])
    id_df = id_df.rename(columns={"value": "check_id"})  
    id_df = pd.concat([id_df, tmp], ignore_index=True)
    id_df = (id_df[['check_id', 'resourceId', 'active', 'endDate']]
             .drop_duplicates()
             .sort_values(by=['check_id', 'active', 'endDate'], ascending=[True, False, False])
        )
    return id_df.drop_duplicates(subset='check_id', keep='first')