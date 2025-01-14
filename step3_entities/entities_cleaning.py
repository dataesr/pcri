import pandas as pd, numpy as np

def entities_clean(entities_tmp):
    print("### ENTITIES TMP cleaning name")
    x=entities_tmp.loc[entities_tmp.entities_name.isnull(), ['generalPic', 'legalName', 'businessName', 'entities_id', 'country_code_mapping']].drop_duplicates()
    print(f"- size entities_tmp without entities_name: {len(x)}")
    if not x.empty:
        if any(x.loc[(x.legalName.str.contains("\\00",  na=True))]):
            print(x.loc[(x.legalName.str.contains("\\00",  na=True))].legalName)

        y=x.loc[(x.businessName.str.contains("00",  na=True))]
        for i, row in y.iterrows():
            try:
                y.at[i, 'businessName'] = row.businessName.replace("\\", "\\u").encode().decode('unicode_escape')
            except:
                y.at[i, 'businessName'] = np.nan
        x = (x
            .merge(y[['generalPic', 'entities_id', 'country_code_mapping']], 
            how='outer', on=['generalPic', 'entities_id', 'country_code_mapping'], indicator=True)
            .query('_merge!="both"').
            drop(columns='_merge'))
        x = pd.concat([x, y], ignore_index=True)

        x.loc[x.businessName.str.contains('^\\d+$', na=True), 'businessName'] = np.nan
        x.loc[x.legalName.str.lower()==x.businessName.str.lower(), 'businessName'] = np.nan

        # liste=['legalName', 'businessName']
        # for i in liste:
        x['entities_name'] = x['legalName'].apply(lambda v: v.capitalize().strip() if isinstance(v, str) else v)
        x['entities_acronym'] = x['businessName']

        print(f"- End size entities_tmp without entities_name: {len(x)}")
        
        entities_tmp = (entities_tmp
                        .merge(x[['generalPic', 'entities_id', 'country_code_mapping']], 
                               how='outer', on=['generalPic', 'entities_id', 'country_code_mapping'], indicator=True)
                        .query('_merge!="both"').
                        drop(columns='_merge'))
        entities_tmp = pd.concat([entities_tmp, x], ignore_index=True)
    
        print(f"- End size entities_tmp: {len(entities_tmp)}")
    return entities_tmp

def entities_check_null(entities_tmp):
    print("\n## check entities null")
    for i in ['entities_name', 'entities_id']:
        if len(entities_tmp[entities_tmp[i].isnull()])>0:
            print(f"{len(entities_tmp[entities_tmp[i].isnull()])} {i} manquants\n {entities_tmp[entities_tmp[i].isnull()]}") 

    test=entities_tmp[['entities_id','entities_name', 'entities_acronym']].drop_duplicates()
    test['nb']=test.groupby(['entities_id','entities_name'], dropna=False)['entities_acronym'].transform('count')
    acro_to_delete=test[test.nb>1].entities_id.unique()
    if acro_to_delete.size>0:
        print(acro_to_delete)

    if any(test.entities_id.isnull())|any(test.entities_id=='nan'):
        print(f"{test.loc[(test.entities_id.isnull())|(test.entities_id=='nan')]}")


def entities_info_add(entities_tmp, entities_info, countries):
    print("\n### entities_info + entities_tmp")
        #ajout des infos country à participants_info
    entities_info = (entities_info
                    .merge(countries[['country_code_mapping', 'country_name_mapping', 'country_code']], how='left', on='country_code_mapping'))
        
    entities_info = (entities_info
        .merge(entities_tmp[
            ['generalPic', 'id', 'ZONAGE', 'id_m', 'siren', 'country_code_mapping',
            'id_secondaire', 'entities_id',  'entities_name', 'entities_acronym', 
            'insee_cat_code', 'insee_cat_name',  'category_agregation',
            'paysage_category', 'flag_entreprise', 
            'ror_category', 'category_woven', 'source_id', 'sector',  
            'siret_closeDate', 'cat_an',
            'groupe_name','groupe_acronym', 'groupe_id', 'groupe_sector']],
        how='left', on=['generalPic', 'country_code_mapping'])
        # .drop(columns=['legalName', 'businessName'])
        )
    print(f"- size entities_info + entities_tmp: {len(entities_info)}")
    return entities_info


def fix_countries(df, countries):
    print("\n### entities_info + countries")
    #ajout des infos country à participants_info
    
    # correction des ecoles françaises à l'etranger
    l=['951736453','996825642','994591926','996825642','953002303', '998384626', '879924055']
    df.loc[df.generalPic.isin(l), 'country_code'] = 'FRA'
    cc = countries.drop(columns=['countryCode', 'country_name_mapping','country_code_mapping']).drop_duplicates()
    df = (df
            .merge(cc, how='left', on='country_code')
            .rename(columns={'ZONAGE':'extra_joint_organization'})
            .drop(columns=['countryCode_parent'])
            .drop_duplicates())

    print(f"- longueur entities_info après ajout calculated_country : {len(df)}\n{df.columns}\n- columns with Nan\n {df.columns[df.isnull().any()]}")
    return df