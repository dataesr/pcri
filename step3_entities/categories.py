import pandas as pd, json, numpy as np
from config_path import PATH_REF, PATH_WORK

def category_paysage(paysage):
    print("### CATEGORY paysage")
    x = (paysage[['id_clean','category_id', 'category_name', 'category_priority']]
            .assign(category_id=paysage.category_id.str.split(';'), 
                    category_name=paysage.category_name.str.split(';'), 
                    category_priority=paysage.category_priority.str.split(';')))
    x = (x.explode(['category_id', 'category_name','category_priority'])
         .loc[~x.category_id.isnull()]
         .drop_duplicates())
    x['category_priority'] = x.category_priority.str.replace('.0','', regex=False)
    pc = (pd.read_csv("data_files/categories_paysage.csv", sep=';', encoding='utf-8')
          .rename(columns={'usualNameFr':'paysage_category', 'id':'paysage_category_id'}))
    miss_x = x.loc[~x.category_id.isin(pc.paysage_category_id.unique())]
    if len(miss_x)>0:
        print(f"1- nouvelles catégories à intégrer à la liste de categories dans data_files")
        miss_x[['category_id', 'category_name']].drop_duplicates().to_csv(f"{PATH_WORK}new_cat.csv", sep=';', encoding='utf-8', index=False)
        exit()
    else:
        return (x.loc[x.category_id.isin(pc.loc[pc.non!='n'].paysage_category_id.unique())]
                .groupby('id_clean').agg(lambda x: ';'.join(x)).reset_index()
                .rename(columns={'category_name':'paysage_category', 'category_id':'paysage_category_id',
                    'category_priority':'paysage_category_priority'}))


def category_cleaning(entities_tmp, sirene):
    print("\n## category FR")
    # CAT1 : categorisation FR
    temp=sirene[['siren','cat', 'cat_an','cj']].drop_duplicates()
    cj=pd.read_excel(f'{PATH_REF}_category.xlsx', sheet_name='cj_code_to_lib', dtype='str')
    name=pd.read_excel(f'{PATH_REF}_category.xlsx', sheet_name='category_name', dtype='str')
    cat_category=pd.read_excel(f'{PATH_REF}_category.xlsx', sheet_name='cat_category', dtype='str')

    entities_tmp.loc[(entities_tmp.siren.isnull())&(~entities_tmp.paysage_siren.isnull()), 'siren'] = entities_tmp.paysage_siren
    entities_tmp=entities_tmp.merge(temp, how='left', on='siren')
    entities_tmp.loc[~entities_tmp.cj.isnull(), 'cj_code'] = entities_tmp.cj
    entities_tmp=entities_tmp.merge(cj, how='left', left_on='cj_code', right_on='code', suffixes=['', '_x'])
    entities_tmp.loc[~entities_tmp.siren_cj_x.isnull(), 'siren_cj'] = entities_tmp.siren_cj_x

    entities_tmp=(entities_tmp.merge(name, how='left', left_on='siren_cj', right_on='category')
        .rename(columns={'category_name':'cj_name'})
        .drop(columns=['category', 'cj', 'code', 'siren_cj_x'])
        .merge(cat_category,how='left', on='cat')
        .rename(columns={'cat':'insee_cat_code'}))

    if len(entities_tmp[entities_tmp.cj_name.isnull()])>1:
        print(entities_tmp.loc[(entities_tmp.cj_name.isnull()), ['siren_cj']].drop_duplicates())
    if any(entities_tmp.loc[(entities_tmp.insee_cat_name.isnull()), ['insee_cat_code']].drop_duplicates()):
        print(entities_tmp.loc[(entities_tmp.insee_cat_name.isnull()), ['insee_cat_code']].drop_duplicates())
    else:
        pass 

    #traitement des catégories assoc si identifiants commencent par W
    entities_tmp.loc[entities_tmp.entities_id.str.match('^[W|w]([A-Z0-9]{8})[0-9]{1}$', na=False), 'category_temp'] = 'Institutions sans but lucratif (ISBL)'
    entities_tmp.loc[entities_tmp.entities_name.str.contains('association', na=False), 'category_temp'] = 'Institutions sans but lucratif (ISBL)'

    entities_tmp.loc[entities_tmp.entities_id.str.match('^[0-9]{9}[A-Z]{1}$', na=False), 'category_temp'] = 'Structures internes de recherche académique (type UMR)'
    entities_tmp.loc[entities_tmp.category_temp.isnull(), 'category_temp'] = entities_tmp.cj_name

    entities_tmp.mask(entities_tmp=='', inplace=True)

    # var_category = ['insee_cat_code', 'paysage_cj_name', 'paysage_category_id','paysage_category', 'category_woven']

    print(f"- taille de entities_tmp après cat: {len(entities_tmp)}")
    return entities_tmp

def category_woven(entities_tmp):
    # CAT 2 : category_woven
    print("\n## category woven")
    x=entities_tmp[['source_id', 'entities_id', 'entities_name', 'category_temp', 'paysage_category', 'siren_cj']].drop_duplicates()
    x.loc[~x.paysage_category.isnull(), 'paysage_cat_1'] = x.paysage_category.str.split(';').str[0]

    x.loc[(x.paysage_cat_1.str.contains('^Entreprise', regex=True, na=False)), 'category_woven'] = 'Entreprise'
    x.loc[(x.category_woven.isnull())&(~x.paysage_cat_1.isnull()), 'category_woven'] = x.paysage_cat_1
    x.loc[(x.category_woven.isnull())&(x.category_temp.str.contains('^Entreprise', regex=True, na=False)), 'category_woven'] = 'Entreprise'
    x.loc[x.entities_id.str.contains('^gent', regex=True, na=False), 'category_woven'] = 'Entreprise'
    x.loc[(x.category_woven.isnull())&(~x.category_temp.isnull()), 'category_woven'] = x.category_temp
    
    
    # cat_ag=pd.read_excel()
    x.to_csv(f"{PATH_WORK}category_entities.csv", sep=';')
    entities_tmp = entities_tmp.merge(x[['entities_id','category_woven']], how='left', on='entities_id').drop_duplicates()
    print(f"- size entities_tmp: {len(entities_tmp)}")
    return entities_tmp

def cordis_type(df):
    print("### CORDIS type")
    type_entity = json.load(open('data_files/legalEntityType.json', 'r', encoding='UTF-8'))
    type_entity = pd.DataFrame(type_entity)
    df = (df.merge(type_entity, how='left', left_on='legalEntityTypeCode', right_on='cordis_type_entity_code')
                    .rename(columns={
                    'isSme':'cordis_is_sme'}))
    l=['legalStatus','legalEntityType', 'legalEntityTypeCode']
    for i in l:
        if i in df.columns:
            df.drop(columns=i, inplace=True)
    print(f"- size entities_info: {len(df)}")
    return df


def mires(df):
    print("\n### MIRES")
    if 'paysage_mires' not in globals() or 'paysage_mires' not in locals():
        paysage_mires = pd.read_pickle(f"{PATH_REF}operateurs_mires.pkl")
    
    df = df.merge(paysage_mires[['entities_id','operateur_name','operateur_num','operateur_lib']], how='left', on='entities_id')
    df = df.mask(df=='')
    df = df.reindex(sorted(df.columns), axis=1)
    return df