import pandas as pd
from config_path import PATH_WORK

def merge_ror(entities_tmp, ror):
    print("### merge ROR")
    entities_tmp = (entities_tmp
                    .merge(ror.drop(columns=['country_code']), how='left', left_on='id', right_on='id_source')
                    .drop(columns='id_source')
                    .drop_duplicates())
    print(f"size entities_tmp after add ror_info: {len(entities_tmp)}")
    if any(entities_tmp.groupby('generalPic')['generalPic'].transform('count')>1):
        entities_tmp[entities_tmp.groupby('generalPic')['generalPic'].transform('count')>1]
    return entities_tmp


def category_paysage(paysage):
    print("### CATEGORY paysage")
    x = (paysage[['id_clean','category_id', 'category_name', 'category_priority']]
            .assign(category_id=paysage.category_id.str.split(';'), 
                    category_name=paysage.category_name.str.split(';'), 
                    category_priority=paysage.category_priority.str.split(';')))
    x = (x.explode(['category_id','category_name','category_priority'])
         .loc[~x.category_id.isnull()]
         .drop_duplicates())
    x['category_priority'] = x.category_priority.str.replace('.0','', regex=False)
    pc = pd.read_csv("data_files/categories_paysage.csv", sep=';', encoding='utf-8').rename(columns={'usualNameFr':'paysage_category', 'id':'paysage_category_id'})
    miss_x = x.loc[~x.category_id.isin(pc.paysage_category_id.unique())]
    if len(miss_x)>0:
        print(f"- nouvelles catégories à intégrer à la liste de categories dans data_files")
        miss_x[['category_id', 'category_name']].drop_duplicates().to_csv(f"{PATH_WORK}new_cat.csv", sep=';', encoding='utf-8', index=False)
        exit()
    else:
        return x.loc[x.category_id.isin(pc.loc[pc.non!='n'].paysage_category_id.unique())].groupby('id_clean').agg(lambda x: ';'.join(x)).reset_index()

def merge_paysage(entities_tmp, paysage, cat_filter):
    print("### merge PAYSAGE")            

    paysage = (paysage
            .rename(columns={'id':'id_extend',
                                'id_clean':'entities_id', 
                                'name_clean':'entities_name', 
                                'acronym_clean':'entities_acronym', 
                                'cj_name':'paysage_cj_name',
                            'siren':'paysage_siren'})
            .drop(columns=['acro_tmp', 'category_id','category_priority', 'category_name'])
            .drop_duplicates()
            .merge(cat_filter, how='left', left_on='entities_id', right_on='id_clean')
            .drop(columns='id_clean'))

    paysage.loc[paysage.id_extend.str.len()==14, 'id_extend'] = paysage.id_extend.str[0:9]
    paysage = paysage.loc[~(paysage.entities_id.isin(['sJKd8','pG74N']))] # BioEnTech  792918765  

    entities_tmp=(entities_tmp
        .drop_duplicates()
        .merge(paysage, how='left', on='id_extend'))

    entities_tmp.loc[entities_tmp.entities_id.isnull(), 'entities_id'] = entities_tmp.id_clean
    entities_tmp.loc[entities_tmp.entities_name.isnull(), 'entities_name'] = entities_tmp.name_clean
    entities_tmp.loc[entities_tmp.entities_acronym.isnull(), 'entities_acronym'] = entities_tmp.acronym_clean

    # entities_tmp=entities_tmp.assign(id_multi=entities_tmp.id)
    # entities_tmp.loc[(~entities_tmp.id_clean.isnull())&(entities_tmp.id_clean!=entities_tmp.id), 'id_multi'] = entities_tmp.id_multi.fillna('') +' '+entities_tmp.id_clean.fillna('')
    # entities_tmp.loc[(~entities_tmp.entities_id.isnull())&(entities_tmp.entities_id!=entities_tmp.id), 'id_multi'] = entities_tmp.id_multi.fillna('') +' '+entities_tmp.entities_id.fillna('')

    entities_tmp = entities_tmp.drop(['id_clean','name_clean','acronym_clean'], axis=1).drop_duplicates()

    if any(entities_tmp.groupby('generalPic')['generalPic'].transform('count')>1):
        print(f"- doublons PIC{entities_tmp[entities_tmp.groupby('generalPic')['generalPic'].transform('count')>1]}")
        
    print(f"size entities_tmp after add paysage_info: {len(entities_tmp)}")
    return entities_tmp