import pandas as pd, json, numpy as np
from config_url import grist_url
from paths import PATH_WORK, PATH_HARVEST
from remote_process.paysage import get_paysageObj
from remote_process.grist import add_records_to_grist, update_doc_grist, categoriesG


def legal_category():
    """
    load lelgal categories for each ids from paysage
    """
    r = get_paysageObj('legal-categories')
    return pd.json_normalize(r)[['id', 'inseeCode', 'longNameFr']]
 

def category_paysage_ref():
    """
    load all categories and select those to keep 
    update grist data cat_paysage
    
    """
    print("### CATEGORY paysage")

    res = get_paysageObj('categories')

    cat = [{'category_id':i.get('id'), 
            'category_name':i.get('usualNameFr')} 
            for i in res]
    cat = pd.DataFrame.from_dict(cat, 'columns')

    pc = categoriesG['Cat_paysage']
    miss_x = cat.loc[~cat.category_id.isin(pc.category_id.unique())]
    if len(miss_x)>0:
        print(f" ATTENTION - new category into cat_paysage -> check if you keep it or not\n{miss_x} in temp/new_cat")
        add_records_to_grist(miss_x, grist_url, 'pcri', 'categories', 'cat_paysage')
        update_doc_grist(categoriesG, 'categories')

    cat = pd.merge(cat, pc.loc[pc['keep']==True, ['category_id', 'category_priority']], how='inner', on='category_id')
    
    cat['category_name'] = cat['category_name'].str.replace(r"(\(.*\))", '', regex=True).str.strip()
    return (cat.rename(columns={'category_priority':'paysage_category_priority'})).sort_values('paysage_category_priority')
        


def category_paysage_by_struct(df, paysage_mires, cat):
    df = pd.DataFrame(df)
    df = pd.merge(df.drop_duplicates(), cat, how='inner', on='category_id')
    df = df.merge(paysage_mires[['category_id','operateur_name','operateur_num','operateur_lib']], how='left', on='category_id')

    ce = ['fm54m', '5xfts', 'od2su']
    tmp = df.loc[df['category_id'].isin(ce)]

    df = df[~df.isin(tmp.to_dict(orient='list')).all(axis=1)]

    tmp['cat'] = tmp['category_id'] 
    for i in ['category_id', 'category_name', 'paysage_category_priority']:
        tmp[i] = ''

    df = pd.concat([df, tmp], ignore_index=True)
    df = df.mask(df=='')

    def aggregate_group(df):
        # Trie le DataFrame du groupe selon 'paysage_category_priority'
        df_sorted = df.sort_values('paysage_category_priority')

        # Retourne une Series avec les colonnes souhaitées
        return pd.Series({
            'paysage_category_id': ';'.join(df_sorted['category_id'].dropna().astype(str)),
            'paysage_category_priority': ';'.join(df_sorted['paysage_category_priority'].dropna().astype(str)),
            'category_name': ';'.join(df_sorted['category_name'].dropna().astype(str)),
            'operateur_name': ';'.join(df_sorted['operateur_name'].dropna().astype(str).unique()),
            'operateur_num': ';'.join(df_sorted['operateur_num'].dropna().astype(str).unique()),
            'operateur_lib': ';'.join(df_sorted['operateur_lib'].dropna().astype(str).unique()),
            'cat': ';'.join(df_sorted['cat'].dropna().astype(str).unique())
        })

    p = (
        df
        .groupby('pid')
        .apply(aggregate_group)
        .reset_index()
    )

    p = p.mask(p=='')
    return p



def cat_entreprise(df):
  
    print("### CATEGORY cat entreprise")

    """
    process on category for entreprise GE, PME, ETI

    """
    #list des IDs paysage des catégories d'entreprises
    ce = ['fm54m', '5xfts', 'od2su']
    entreprise=json.load(open("data_files/cat_entreprise_lib.json", encoding='utf-8'))
    mapping = {k: list(v.keys())[0] for k, v in entreprise.items()}

    # convert id_cat_paysage to cat insee
    df.loc[df['cat'].notna(), 'cat_entreprise'] = (
    df.loc[df['cat'].notna(), 'cat'].map(mapping)
    )

    df = df.mask(df=='')

    #if source_id not paysage and paysage_category_id not s79DJ
    df.loc[(~df['paysage_category_id'].str.contains('s79DJ', na=False)), 'cat_entreprise'] = np.nan


    if any(df.loc[(df['source_id']=='paysage') & (~df['paysage_category_id'].str.contains('s79DJ', na=False)) & (df['paysage_category_id'].isin(ce))]):
        print(f"- check entities paysage with only cat_entreprise in category paysage\n{df.loc[(df['source_id']=='paysage') & (~df['paysage_category_id'].str.contains('s79DJ', na=False)) & (df['paysage_category_id'].isin(ce))]}")
        for i in['category_name', 'paysage_category_priority', 'paysage_category_id']:
            df.loc[df['paysage_category_id'].isin(ce), i] = np.nan


    df['cat_entreprise_name'] = df['cat_entreprise'].map(
    {list(v.keys())[0]: list(v.values())[0] for v in entreprise.values()}
    )

    df = df.rename(columns={'cat_entreprise':'cat_entreprise_code'}).drop(columns=['cat'])

    df.mask(df=='', inplace=True)
    print(f"- size entities_tmp after add cat_entreprise: {len(df)}")
    return df



def naf_etab_sirene(df):
    from paths import PATH
    import pandas as pd
    naf = pd.read_csv(f"{PATH}nomenclatures/naf/naf_nomenclature.csv", sep=';', encoding='ANSI')
    df = (df.merge(naf, how='left', left_on='naf_et', right_on='naf')
          .drop(columns='naf')
          .rename(columns={'naf_et': 'activity_code',
                           'naf_lib':'activity_name',
                           'naf1_gen':'activity_group_code', 
                           'naf1_lib':'activity_group_name'})
        )
    return df


def cordis_type(df):
    print("### CORDIS type")
    type_entity = json.load(open('data_files/legalEntityType.json', 'r', encoding='UTF-8'))
    type_entity = pd.DataFrame(type_entity).fillna(np.nan)
    df = (df.merge(type_entity, how='left', left_on='legalEntityTypeCode', right_on='cordis_type_entity_code')
                    .rename(columns={
                    'isSme':'cordis_is_sme'}))
    l=['legalStatus','legalEntityType', 'legalEntityTypeCode']
    for i in l:
        if i in df.columns:
            df.drop(columns=i, inplace=True)


    print(f"- size entities_info: {len(df)}")
    return df


def cj_to_cat(filename, df, vkey, vval, cond):
    f=json.load(open(f"data_files/{filename}.json", encoding='utf-8'))
    for i in f:
        for k,v in i.items():
            df.loc[cond & (df[vkey]==k), vval]=v
    return df


def category_woven(df):
    print("\n## category woven") 
    
    mask = (df['paysage_category_id'].isnull())
    if any(mask):
        print(f"ATTENTION : entities without paysage_category_id: try to fix it with cj_code")
        mapping=json.load(open(f"data_files/cj_code_to_paysage.json", encoding='utf-8'))
        df.loc[mask, 'paysage_category_id'] = df.loc[mask, 'cj_code'].map(mapping)
    
    # df['paysage_category_id'] = df['paysage_category_id'].fillna('')
    # df.loc[df['paysage_category_id'].notnull(), 'category_name'] = df.loc[df['paysage_category_id'].notnull(), 'paysage_category_id'].str.split(';').str[0]
    df.loc[df['paysage_category_id'].notnull(), 'category_woven'] = df.loc[df['paysage_category_id'].notnull(), 'category_name'].str.split(';').str[0]
    df.loc[df['category_woven'].isnull(), 'category_woven'] = df.loc[df['category_woven'].isnull(), 'category_name']


    mask = (df['category_woven'].isnull())
    if any(mask):
        print(f"ATTENTION : entities without category_woven: try to fix it with cj_code")
        df = cj_to_cat('cj_code_to_lib', df, 'cj_code', 'category_name', mask)
        df = cj_to_cat('cat_cj_lib', df, 'category_name', 'category_woven', mask)
        mask = (df['category_woven'].isnull())
        if any(mask):
            print(f"- missing category_woven at the end: {df.loc[mask].value_counts(['source_id', 'cj_code'], dropna=True)}")
        else:
            print(f"- missing only category_woven at the end for nan source and cj: {df.loc[mask].value_counts(['source_id', 'cj_code'], dropna=False)}")
    
    print(f"- taille de df après cat: {len(df)}")
    return df


def category_agreg(df):

    mask = (df['category_woven'].notnull())
    df.loc[df['paysage_category_id'].notna(), 'category_id'] = df.loc[df['paysage_category_id'].notna(), 'paysage_category_id'].str.split(';').str[0]
    df = cj_to_cat('cat_to_agreg', df, 'category_id', 'category_agregation', mask)
    df.loc[df['category_agregation'].isnull(), 'category_agregation'] = df.loc[df['category_agregation'].isnull(), 'category_name'] 

    if any(df['category_agregation'].isnull()):
        print("- add category to aggreg on the list cat_to_agreg")
        print(df[df['category_agregation'].isnull()].value_counts(['category_id', 'category_woven', 'category_agregation'], dropna=False).reset_index(name='freq'))

    if any(df['category_agregation'].isnull()):
        print(f"- without category_agregation: {df[df['category_agregation'].isnull()].value_counts(['source_id'], dropna=False)}")

    agreg=json.load(open("data_files/cat_agreg_lib.json", encoding='utf-8'))
    for i in agreg:
        for k,v in i.items():
            df.loc[df['category_agregation']==k, 'category_agregation']=v

    # entreprise
    df.loc[(df['category_agregation']=='Entreprise'), 'entreprise_flag'] = True
    df.loc[df['entreprise_flag'].isnull(), 'entreprise_flag'] = False

    l=['cat_entreprise_code', 'cat_entreprise_name']
    df.loc[df['entreprise_flag']==False, l] = np.nan
    
    return df