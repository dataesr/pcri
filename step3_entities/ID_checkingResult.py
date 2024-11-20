from config_path import PATH_WORK
import pandas as pd, numpy as np

def IDchecking_results(result, check_id_liste, identification):
    verif_id=pd.DataFrame(result, dtype=str)
    verif_id = verif_id.rename(columns={'id':'checked_id'})
    verif_id = (check_id_liste
                .merge(verif_id, how='inner', left_on='check_id', right_on='checked_id')
                .drop(columns='check_id')
                .drop_duplicates()
                .sort_values('generalPic'))
    print(f"nombre de pic unique verif_id: {verif_id.generalPic.nunique()}")

    verif_id['nb'] = verif_id.groupby(['generalPic','countryCode'])['checked_id'].transform('nunique')
    verif_id.loc[(verif_id.nb==1)&(verif_id.code=='200'), 'new_id'] = verif_id.loc[(verif_id.nb==1)&(verif_id.code=='200'), 'checked_id']
    verif_id.loc[(verif_id.new_id.isnull())&(verif_id.stock_id=='ref')&(verif_id.code=='200'), 'new_id'] = verif_id.loc[(verif_id.new_id.isnull())&(verif_id.stock_id=='ref')&(verif_id.code=='200'), 'checked_id']

    # verif_id = verif_id[~((verif_id.nb>1)&(verif_id.code!='200'))].drop(columns='nb')
    verif_id['nb'] = verif_id.groupby(['generalPic','countryCode'])['checked_id'].transform('nunique')
    print(f"nombre de pic unique verif_id: {verif_id.generalPic.nunique()}")

    unik = verif_id[~verif_id.new_id.isnull()|((verif_id.nb==1)&(verif_id.code!='200'))]
    print(f"nombre de pic unique unik: {unik.generalPic.nunique()}")

    multi = verif_id.loc[~verif_id.generalPic.isin(unik.generalPic.unique())]
    print(f"nombre de pic unique multi: {multi.generalPic.nunique()}")
    # for name, group in multi.groupby(['generalPic'], as_index=False):
    #     for i, row in group.iterrows():
    #         multi.loc[i, 'new_id'] = np.where((row['code']=='200'), row['checked_id'], row['new_id'])

    verif_id = pd.concat([unik, multi], ignore_index=True)

    verif_id = (verif_id
                .sort_values('generalPic')
                .groupby(['generalPic','countryCode', 'countryCode_parent'])[['checked_id', 'stock_id','source','code','new_id']]
                .agg(lambda col: ';'.join(col.astype(str).dropna().unique())).reset_index()
                .merge(identification[['id_secondaire', 'ZONAGE', 'generalPic', 'legalName',  'webPage', 'city', 'country_name_mapping', 'countryCode', 'countryCode_parent', 'country_code_mapping', 'vat', 'legalRegNumber']],
                    how='right', on=['generalPic', 'countryCode', 'countryCode_parent']))

    cols = verif_id.select_dtypes(object).columns
    verif_id[cols] = verif_id[cols].apply(lambda x: x.str.replace('\\n|\\t|\\r|\\s+', ' ', regex=True).str.strip())

    verif_id.loc[(verif_id.code=='200')&(verif_id.checked_id==verif_id.new_id), 'indicator_control'] = 'ok'
    # verif_id.loc[(verif_id.indicator_control.isnull())&(verif_id.code=='200')&(verif_id.checked_id==verif_id.new_id), 'indicator_control'] = 'ok'

    pd.DataFrame(verif_id).to_csv(f"{PATH_WORK}check_id_result.csv", sep=';', index=False, encoding='utf-8')
    print('- resultat à checker dans check_id_result.csv (path_work)\n- intégrer csv dans _check_id_result.xlsx\n- sauver le vieil onglet et coller dans new')

def ID_resultChecked():
    filename = '_check_id_result.xlsx'
    id_verified = pd.read_excel(f"{PATH_WORK}{filename}", dtype=object, keep_default_na=False, sheet_name='new')
    print(len(id_verified))
    id_verified.mask(id_verified=='', inplace=True)
    return id_verified

def new_ref_source(id_verified,ref_source,extractDate,part,app1,entities_single,countries):
    if 'id_secondaire' not in id_verified.columns :
        id_verified = id_verified.assign(id_secondaire=np.nan)
    if 'ZONAGE' not in id_verified.columns:
        id_verified = id_verified.assign(ZONAGE=np.nan)

    tmp = (id_verified[['generalPic','country_code_mapping','countryCode','ZONAGE','id_secondaire','new_id','source','code','legalName', 'webPage', 'city']]
    .rename(columns={'new_id':'id', 'webPage':'url', 'source':'source_id'})     
    .merge(ref_source[['generalPic', 'country_code_mapping', 'FP', 'ZONAGE','id_secondaire']],
                    how='left', on=['generalPic','country_code_mapping'], suffixes=['','_y']))
    
    tmp['FP'] = np.where((tmp['FP'].isnull())|(tmp['FP']==''), 'HE', tmp['FP'])
    tmp.loc[~tmp.FP.str.contains('HE'), 'FP'] = tmp.FP +' HE'
    tmp.FP=(tmp['FP']          # stack removes `nan`
    .str.split(' ')   # split by `', '`
    .explode()  
    .groupby(level=0)
    .apply(lambda x: ' '.join(x.sort_values(ascending=False).unique()))
    .reindex(tmp.index, fill_value=''))
    tmp=tmp.assign(last_control=extractDate)

    tmp.loc[tmp.ZONAGE.isnull(), 'ZONAGE'] =  tmp.ZONAGE_y
    tmp.loc[tmp.id_secondaire.isnull(), 'id_secondaire'] =  tmp.id_secondaire_y

    for tab in [part, app1]:
        if 'netEuContribution' in tab.columns:
            f='netEuContribution'
            v='project'
        elif 'requestedGrant' in tab.columns:
            f='requestedGrant'
            v='proposal'
        tmp1 = (tab[['generalPic','countryCode', f]]
                .groupby(['generalPic','countryCode'])
                .sum().reset_index()
                .drop_duplicates()
                .rename(columns={f:v}))
        tmp = tmp.merge(tmp1, how='left', on=['generalPic','countryCode'])

    tmp = (tmp
            .merge(entities_single[['generalPic', 'generalState', 'countryCode', 'isInternationalOrganisation']].drop_duplicates(), 
                    how='left')
            .drop(columns=['ZONAGE_y','id_secondaire_y','countryCode']))
        
    print(f"size tmp complet:{len(tmp)}, size tmp only generalPic+cc {len(tmp[['generalPic', 'country_code_mapping']].drop_duplicates())}")

    if len(tmp)!=len(tmp[['generalPic', 'country_code_mapping']].drop_duplicates()):
        print(tmp.groupby(['generalPic', 'country_code_mapping'], dropna=False).size().sort_values(ascending=False) )

    outer = ref_source.merge(tmp[['generalPic', 'country_code_mapping']].drop_duplicates(), how='outer', on=['generalPic', 'country_code_mapping'], indicator=True)
    anti_join = outer[(outer._merge=='left_only')].drop(['_merge'], axis=1)

    keep = outer.loc[(outer._merge!='left_only'), ['generalPic', 'country_code_mapping','proposal','project']]
    tmp = tmp.merge(keep, how='left', on=['generalPic', 'country_code_mapping'])

    tmp['proposal'] = tmp.loc[:,['proposal_x','proposal_y']].sum(axis=1)
    tmp['project'] = tmp.loc[:,['project_x','project_y']].sum(axis=1)

    tmp = (tmp
        .merge(countries[['country_code_mapping', 'country_name_mapping', 'country_code']], 
                    how='left', on='country_code_mapping')
        .drop(columns=['proposal_x','project_x','proposal_y','project_y'])
        .rename(columns={'country_code':'countryCode_parent'}))

    ref_source = pd.concat([anti_join, tmp], ignore_index=True).drop_duplicates()

    liste=['legalName', 'city']
    for i in liste:
        ref_source[i] = ref_source[i].apply(lambda x: x.lower().strip() if isinstance(x, str) else x)
    liste=['proposal', 'project']
    for i in liste:
        ref_source[i] = ref_source[i].replace('', np.nan, regex=False)
        ref_source[i] = ref_source[i].astype(float)

    ref_source=ref_source[
        ['generalPic', 'generalState', 'countryCode_parent', 'country_code_mapping', 
        'country_name_mapping', 'id_secondaire', 'ZONAGE', 'id',
        'legalName', 'city', 'url', 'project', 'proposal',  'FP', 'last_control',
        'comments', 'isInternationalOrganisation', 'vat', 'legalRegNumber', 
        'source_id', 'code']]    

    print(f"- End size new ref_source:{len(ref_source)}")
    ref_source.to_csv(f"{PATH_WORK}ref_{extractDate}.csv", sep=';', encoding='utf-8', index=False, na_rep='')
    print("# Nouveau REF_SOURCE\n- remplir des ID pour les nouveaux français")