import pandas as pd, json, numpy as np
from config_path import PATH_REF, PATH_WORK
from constant_vars import FRAMEWORK

def category_paysage(df):
    print("### CATEGORY paysage")
    df['category_priority'] = df.category_priority.astype(str).str.replace('.0','', regex=False)
    pc = (pd.read_csv("data_files/cat_paysage.csv", sep=';', encoding='utf-8')
          .rename(columns={'usualNameFr':'paysage_category', 'id':'paysage_category_id'}))
    miss_x = df.loc[~df.category_id.isin(pc.paysage_category_id.unique())]
    if len(miss_x)>0:
        print(f"1- nouvelles catégories à intégrer à la liste de categories dans data_files")
        miss_x[['category_id', 'category_name']].drop_duplicates().to_csv(f"{PATH_WORK}new_cat.csv", sep=';', encoding='utf-8', index=False)
        exit()
    else:
        return (df.loc[df.category_id.isin(pc.loc[pc.non!='n'].paysage_category_id.unique())]
                .groupby('id_clean').agg(lambda x: ';'.join(x)).reset_index()
                .rename(columns={
                    'category_name':'paysage_category', 
                    'category_id':'paysage_category_id',
                    'category_priority':'paysage_category_priority'}))


def category_woven(df, sirene):
    print("\n## category woven")
    # CAT1 : categorisation FR

    temp=sirene[['siren', 'cat', 'cat_an', 'cj']].drop_duplicates()

    df.loc[(df.siren.isnull())&(~df.paysage_siren.isnull()), 'siren'] = df.paysage_siren
    df=df.merge(temp, how='left', on='siren')
    df.loc[~df.cj.isnull(), 'cj_code'] = df.cj

    cj_lib=json.load(open("data_files/cat_cj_code_to_lib.json"))
    for i in cj_lib:
        for k,v in i.items():
            df.loc[df.cj_code==k, 'siren_cj']=v
    
    df.loc[~df.siren_cj.isnull(), 'category_tmp'] = df.siren_cj
    #traitement des catégories assoc si identifiants commencent par W
    df.loc[df.entities_id.str.match('^[W|w]([A-Z0-9]{8})[0-9]{1}$', na=False), 'category_tmp'] = 'ISBL'
    df.loc[(df.source_id=='rnsr')|(df.paysage_category_id.str.split(';').str[0]=='z367d'), 'category_tmp'] = 'STRUCT'
    df.loc[~df.paysage_category_id.isnull(), 'category_tmp'] = df.loc[~df.paysage_category_id.isnull()].paysage_category_id.str.split(';').str[0]
    df.loc[~df.paysage_category_id.isnull(), 'category_woven'] = df.loc[~df.paysage_category_id.isnull()].paysage_category.str.split(';').str[0]

    cj_lib=json.load(open("data_files/cat_cj_lib.json", encoding='utf-8'))
    for i in cj_lib:
        for k,v in i.items():
            df.loc[df.category_tmp==k, 'category_woven']=v

    print(f"- categorization missing\n{df.loc[(df.source_id.isin(['paysage','siren','siret','rnsr']))&(df.category_woven.isnull()), ['source_id', 'entities_name', 'entities_id', 'siren_cj', 'paysage_category']]}")

    print(f"- taille de df après cat: {len(df)}")
    return df

def category_agreg(df):

    agreg=json.load(open("data_files/cat_to_agreg.json"))
    for i in agreg:
        for k,v in i.items():
            df.loc[df.category_tmp==k, 'category_agregation']=v
 
    df.loc[df.category_agregation.isnull(), 'category_agregation'] = df.siren_cj

    agreg=json.load(open("data_files/cat_agreg_lib.json", encoding='utf-8'))
    for i in agreg:
        for k,v in i.items():
            df.loc[df.category_agregation==k, 'category_agregation']=v


    # entreprise
    entreprise=json.load(open("data_files/cat_entreprise_lib.json", encoding='utf-8'))
    for i in entreprise:
        for k,v in i.items():
            df.loc[df.cat==k, 'insee_cat_name']=v
    df.rename(columns={'cat':'insee_cat_code'}, inplace=True)

    df.mask(df=='', inplace=True)

    df.loc[(df.siren_cj.isin(['ENT', 'ENT_ETR']))|(df.ror_category=='Company'), 'entreprise_flag'] = True
    df.loc[(df.category_agregation=='Entreprise'), 'entreprise_flag'] = True
    df.loc[df.entreprise_flag.isnull(), 'entreprise_flag'] = False

    l=['insee_cat_code', 'insee_cat_name']
    df.loc[df.entreprise_flag==False, l] = np.nan
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

def mires(df):
    print("\n### MIRES")
    if 'paysage_mires' not in globals() or 'paysage_mires' not in locals():
        paysage_mires = pd.read_pickle(f"{PATH_REF}operateurs_mires.pkl")
    
    df = df.merge(paysage_mires[['entities_id','operateur_name','operateur_num','operateur_lib']], how='left', on='entities_id')
    df = df.mask(df=='')
    df = df.reindex(sorted(df.columns), axis=1)
    return df