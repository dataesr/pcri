import pandas as pd, numpy as np, json
from config_path import PATH_WORK
from step3_entities.references import paysage_id_extract, paysage_id_extract_prepare
from remote_process.ID_checkingRefExist import check_id_in_paysage

def merge_id_to_ref(df, var: str):
    """
    reload link between from_id_to_ref and paysage IDs after updating paysage app
    
    """
    paysage_identifiers = paysage_id_extract(list(df['source_id'].unique()))
    paysage_identifiers = paysage_id_extract_prepare(paysage_identifiers)
    df = check_id_in_paysage(df, var, paysage_identifiers)
    return df

def merge_paysage(entities_tmp, paysage, cat_filter):
    print(f"\n### merge PAYSAGE")            

    paysage = (paysage
            .rename(columns={'id_source':'id_extend',
                            'id_clean':'entities_id', 
                            'name_clean':'entities_name', 
                            'acronym_clean':'entities_acronym',
                            'category_name':'catname'
                            })
            .assign(link_to_ref=True)
            .drop_duplicates()
            .merge(cat_filter, how='left', left_on='entities_id', right_on='pid')
            .drop(columns=['pid', 'acronymEn', 'acronymLocal', 'cityid']))
    print(f"-1 size paysage to merge: {len(paysage)}")

    if any(paysage['paysage_category_id'].isnull()):
        paysage.loc[paysage['paysage_category_id'].isnull(), 'paysage_category_id'] = paysage.loc[paysage['paysage_category_id'].isnull(), 'category_id']
        paysage.loc[paysage['category_name'].isnull(), 'category_name'] = paysage.loc[paysage['category_name'].isnull(), 'catname']


    entities_tmp = (entities_tmp
        .drop_duplicates()
        .merge(paysage.drop(columns='catname'), 
               how='left', on='id_extend'))
    print(f"-2 size entities_merge+paysage: {len(entities_tmp[entities_tmp['source_id']=='paysage'])}")

    if any(entities_tmp['paysage_category_id'].isnull()):
        print(f"-3 ATTENTION, missing entities_tmp category: {entities_tmp.loc[entities_tmp['paysage_category_id'].isnull(), ['entities_id', 'cj_code']].drop_duplicates()}")

    if ('legalName' in entities_tmp.columns) & ('country_code' in entities_tmp.columns):
            if (len(entities_tmp.groupby(['generalPic', 'country_code', 'country_code_source', 'id_extend']).size().reset_index(name='nb').query('nb>1'))>0):
                print(f"-4 ATTENTION ! fix entities_tmp rows duplicated: {entities_tmp.groupby(['generalPic', 'id_extend', 'legalName', 'country_code', 'country_code_source']).size().reset_index(name='nb').query('nb>1')}")

    if ('legalName' in entities_tmp.columns) & (any(entities_tmp.groupby(['generalPic', 'country_code_source'])['generalPic'].transform('count')>1)):
        print(f"-5 PIC duplicated\n{entities_tmp[entities_tmp.groupby(['generalPic', 'country_code_source'])['generalPic'].transform('count')>1][['generalPic', 'legalName','country_code_source', 'id_first']].drop_duplicates()}")
        
    print(f"-6 End size entities_tmp+paysage_info: {len(entities_tmp)}")
    return entities_tmp.drop(columns='category_id')


def merge_ror(entities_tmp, ror, cat, paysage_cj):
    print("### merge ROR")

    ror = ror.merge(paysage_cj, how='left', left_on='cj', right_on='id')
    tmp = ror[['id_clean', 'paysageCat']].drop_duplicates()
    tmp = tmp.assign(paysageCat=tmp['paysageCat'].str.split(';')).explode('paysageCat')
    tmp = (pd.merge(tmp, cat, how='left', left_on='paysageCat', right_on='category_id')
           .drop(columns=['paysageCat'])
    )
    tmp = (tmp.rename(columns={'category_id':'paysage_category_id'})
              .groupby('id_clean', as_index=False).agg({
                'paysage_category_id': ';'.join,
                'category_name': ';'.join,
                'paysage_category_priority': lambda x: ';'.join(map(str, x))
            })
    )
    
    ror = pd.merge(ror, tmp, how='left', on='id_clean').drop(columns='paysageCat')

    ror = (ror[['id_source', 'id_clean', 'inseeCode', 'longNameFr', 'paysage_category_id',
        'category_name', 'paysage_category_priority', 'name_usual', 'acronym']]
        .drop_duplicates()
        .rename(columns={'inseeCode':'cj_code',
                        'longNameFr':'cj_name',
                        'name_usual':'entities_name',
                        'acronym':'entities_acronym',
                        'id_clean':'entities_id'
                            })
    )
    
    ids_select = set(ror['id_source'])
    entities_select = (entities_tmp[entities_tmp['id_extend'].isin(ids_select)]
                        .drop(columns=[ 'entities_id', 'entities_name', 'entities_acronym',
                                        'paysage_category_id', 'cj_code', 'cj_name',
                                        'category_name', 'paysage_category_priority'])
                                        )
    print(f"- size entities_select before ror: {len(entities_select)}")  

    entities_select = (entities_select          
                        .merge(ror, how='inner', left_on='id_extend', right_on='id_source')
                        .drop(columns='id_source')
                        .assign(link_to_ref=True))
    print(f"- size entities_select with ror: {len(entities_select)}")
    
    ids_select = set(entities_select['id_extend'])
    tmp = entities_tmp[~entities_tmp['id_extend'].isin(ids_select)]
    entities_tmp = pd.concat([tmp, entities_select], ignore_index=True).drop_duplicates()
    
    print(f"- End size entities_tmp+ror_info: {len(entities_tmp)}")
    if any(entities_tmp.groupby('generalPic')['generalPic'].transform('count')>1):
        entities_tmp[entities_tmp.groupby('generalPic')['generalPic'].transform('count')>1]
    return entities_tmp


def merge_sirene(entities_tmp, sirene, cat, paysage_cj):

    print("### merge SIRENE")
    sirene = (sirene[['siren', 'siret', 'siege', 'sigle', 'nom', 'inseeCode', 'paysageCat', 'cat']]
              .rename(columns={'cat':'cat_entreprise'})
    )
    sirene = pd.merge(sirene, paysage_cj, how='left', on='inseeCode').drop(columns=['id'])
    sirene = (pd.merge(sirene, cat,
                        how='left', left_on='paysageCat', right_on='category_id')
                        .drop(columns=['paysageCat'])
                        .rename(columns={
                            'category_id':'paysage_category_id',
                            'inseeCode':'cj_code',
                            'longNameFr':'cj_name',
                            'nom':'entities_name',
                            'sigle':'entities_acronym'})
    )
    
    # siret merge
    entities_select = (entities_tmp[entities_tmp['source_id']=='siret']
                        .drop(columns=[ 'entities_id', 'cj_code', 'cj_name', 
                                        'sector', 'entities_name', 'entities_acronym',
                                        'paysage_category_id', 'category_name', 'paysage_category_priority'])
                        .merge(sirene, how='inner', left_on='id_extend', right_on='siret')
                        .drop(columns=['siege', 'siren'])
                        .rename(columns={'siret':'entities_id'})
                        .assign(link_to_ref=True))
    
    entities_tmp = pd.concat([entities_tmp[~entities_tmp['id_extend'].isin(entities_select['entities_id'].unique())], entities_select], ignore_index=True).drop_duplicates()

    # siren merge
    entities_select = (entities_tmp[(entities_tmp['source_id']=='siren')&(entities_tmp['entities_id'].isnull())]
                    .drop(columns=[ 'entities_id', 'cj_code', 'cj_name', 'cat_entreprise',
                                    'sector', 'entities_name', 'entities_acronym',
                                    'paysage_category_id', 'category_name', 'paysage_category_priority']))
    print(f"- size entities_select siren before merge: {len(entities_select)}")

    entities_select = (pd.merge(entities_select, sirene[sirene['siege']==True], 
                               how='inner', left_on='id_extend', right_on='siren')
                    .drop(columns=['siret', 'siege'])
                    .rename(columns={'siren':'entities_id'})
                    .assign(link_to_ref=True)
                    )
    print(f"-1 size entities_select siren before merge: {len(entities_select)}")

    entities_tmp = pd.concat([entities_tmp[~entities_tmp['id_extend'].isin(entities_select['entities_id'].unique())], entities_select], ignore_index=True).drop_duplicates()
    print(f"-2 End size entities_tmp+sirene_info: {len(entities_tmp)}")

    if ('legalName' in entities_tmp.columns)&(any(entities_tmp.groupby('generalPic')['generalPic'].transform('count')>1)):
        print(f"-3 ++ rows per pic because ++ countries for a PIC ? :\n{entities_tmp[entities_tmp.groupby('generalPic')['generalPic'].transform('count')>1][['generalPic', 'legalName', 'country_code_source', 'id_first']]}")


    print(f"- End size entities_tmp+sirene: {len(entities_tmp)}")
    return entities_tmp


def merge_pic(entities_tmp, pic, cat, paysage_cj):

    print(f"- with identifiant but not linked : \n{entities_tmp.loc[entities_tmp.link_to_ref.isnull()]['source_id'].value_counts(dropna=False)}")
    tmp = entities_tmp.loc[entities_tmp.link_to_ref.isnull(), ['generalPic', 'id_extend', 'country_code_source']]

    pic = pd.merge(tmp, pic, how='inner', on=['generalPic', 'country_code_source']).drop_duplicates()
    pic['entities_id'] = np.where(pic['id_extend'].notnull(), pic['id_extend'], pic['pic_new'])
    pic['entities_name'] = pic['legalName']
    pic['entities_acronym'] = pic['businessName']
    mapping={'PRIVATE':'privé', 'INDIVIDUAL':'privé','PUBLIC':'public'}
    pic.loc[pic['legalType'].isin(mapping.keys()), 'sector'] = pic.loc[pic['legalType'].isin(mapping.keys()), 'legalType'].map(mapping)  

    pic = (pd.merge(pic, paysage_cj, how='left', left_on='cj', right_on='id')
           .rename(columns={'inseeCode':'cj_code',
                            'longNameFr':'cj_name'
                        })
            .drop(columns=['cj', 'id'])
    )


    # convert cordis type in paysageCat just for missing paysageCat
    mapping={'REC':'lcblh',
             'HES':'8rh6n',
             'PUB':'rslqh',
             'PRC':'2fy6x',
             'OTH':'7w3QE'
    }

    pic.loc[pic['paysageCat'].isnull(), 'paysageCat'] = (
    pic.loc[pic['paysageCat'].isnull(), 'legalEntityTypeCode'].map(mapping)
    )
 

    pic = (pd.merge(pic, cat, how='left', left_on='paysageCat', right_on='category_id')
             .rename(columns={'category_id':'paysage_category_id'})
             .drop(columns=['paysageCat'])
    )
    
    pic.loc[(pic['entities_id'].str.match('^[W|w]([A-Z0-9]{8})[0-9]{1}$', na=False)), 'source_id'] = 'rna'
    pic.loc[pic['source_id']=='rna', 'cj_code'] = '9220'
    pic.loc[pic['source_id']=='rna', 'cj_name'] = 'Association loi de 1901'
    pic.loc[pic['source_id']=='rnsr', 'cj_name'] = 'Sans personnalité juridique - secteur public'
    if len(pic.loc[(pic.country_code=='FRA')&(pic.cj_code.isnull())&(pic.cj_name.isnull())])>0:
        print(f"- For France -> cj missing, check if it's possible to provide information:\n{pic.loc[(pic.country_code=='FRA')&(pic.cj_code.isnull())&(pic.cj_name.isnull())].value_counts(['source_id', 'entities_id'], dropna=False)}")
    if len(pic.loc[(pic.country_code!='FRA')&(pic.cj_code.isnull())&(pic.cj_name.isnull())])>0:
        print(f"- For other -> cj missing, check if it's possible to provide information:\n{pic.loc[(pic.country_code!='FRA')&(pic.cj_code.isnull())&(pic.cj_name.isnull())].value_counts(['source_id', 'entities_id'], dropna=False)}")
    
        
    entities_tmp = entities_tmp.loc[entities_tmp['link_to_ref'].notnull()]
    entities_tmp = pd.concat([entities_tmp, pic], ignore_index=True).drop_duplicates()
    
    print(f"- End size entities_tmp+pic: {len(entities_tmp)}")
    return entities_tmp.drop(columns=(['pic_new', 'project'])).drop_duplicates()