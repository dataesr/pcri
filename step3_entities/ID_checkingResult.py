from paths import PATH_WORK
from functions_shared import work_csv
import pandas as pd, numpy as np

def IDchecking_results(result, paysage_res, identification):
    """
    check the result from check_id_by_source function
    
    """
    verif_id=pd.DataFrame(result, dtype=str).apply(lambda x: x.str.strip())
    # verif_id = verif_id.rename(columns={'id':'checked_id'})
    verif_id = pd.merge(paysage_res, verif_id, how='left', left_on='check_id', right_on='checked_id')

    verif_id.loc[verif_id['checked_id'].isna(), 'checked_id'] = verif_id.loc[verif_id['checked_id'].isna(), 'check_id']
    verif_id = verif_id.drop(columns='check_id').drop_duplicates().sort_values('generalPic')
    print(f"- nombre de pic unique verif_id: {verif_id.generalPic.nunique()}")


    verif_id['nb'] = verif_id.groupby(['generalPic','countryCode'], dropna=False)['checked_id'].transform('nunique')

    #value single
    mask_single = (verif_id['nb']==1)
    verif_id.loc[mask_single&(verif_id['resourceId'].notna()), 'new_id'] = verif_id.loc[mask_single&(verif_id['resourceId'].notna()), 'resourceId']
    verif_id.loc[mask_single&(verif_id['new_id'].isna())&(verif_id.code=='200'), 'new_id'] = verif_id.loc[mask_single&(verif_id['new_id'].isna())&(verif_id.code=='200'), 'checked_id']

    verif_id.loc[mask_single&(verif_id['new_id'].isna())&(verif_id['checked_id'].notna())&(verif_id['inPaysage']==False), 'error'] = 'fix_it'
    
    # multi value but choose stock=ref and code=200
    mask_multi = (verif_id['nb']>1)
    cond_ref200 = mask_multi & (verif_id['new_id'].isna()) & (verif_id['stock_id'].eq('ref')) & (verif_id['code'].astype(str).eq('200')|verif_id['code'].isna())

    pair_keeped = verif_id.loc[cond_ref200, ['generalPic', 'countryCode']].drop_duplicates()

    # fill new_id for the ref/200 rows
    verif_id.loc[cond_ref200, 'new_id'] = verif_id.loc[cond_ref200, 'checked_id']

    # --- FIX: build an index-aligned mask for matching pairs ---
    pairs_idx = pd.MultiIndex.from_frame(pair_keeped[['generalPic', 'countryCode']])
    df_idx    = pd.MultiIndex.from_frame(verif_id[['generalPic', 'countryCode']])
    mask_pairs = df_idx.isin(pairs_idx)

    # drop rows where new_id is null and stock_id != ref for those pairs
    verif_id = verif_id.loc[~(
        verif_id['new_id'].isna() &
        verif_id['stock_id'].ne('ref') &
        mask_pairs
    )].copy()

    
    verif_id['nb'] = verif_id.groupby(['generalPic','countryCode'], dropna=False)['checked_id'].transform('nunique')
    print(f"- nombre de pic unique verif_id: {verif_id['generalPic'].nunique()}")

    verif_id.loc[(verif_id['new_id'].isna())&(verif_id['resourceId'].notna()), 'new_id'] = verif_id.loc[(verif_id['new_id'].isna())&(verif_id['resourceId'].notna()), 'resourceId']

    # unik = verif_id[verif_id.new_id.notnull()|((verif_id.nb==1)&(verif_id.code!='200'))]
    # print(f"- nombre de pic unique unik: {unik.generalPic.nunique()}")

    # multi = verif_id.loc[~verif_id.generalPic.isin(unik.generalPic.unique())]
    # print(f"- nombre de pic unique multi: {multi.generalPic.nunique()}")
    # # for name, group in multi.groupby(['generalPic'], as_index=False):
    # #     for i, row in group.iterrows():
    # #         multi.loc[i, 'new_id'] = np.where((row['code']=='200'), row['checked_id'], row['new_id'])

    # verif_id = pd.concat([unik, multi], ignore_index=True)

    verif_id = (verif_id
                .fillna('')
                .sort_values('generalPic')
                .drop_duplicates()
                .groupby(['generalPic','countryCode', 'country_code'], dropna=False)[['checked_id', 'stock_id', 'in_paysage', 'active', 'source_id','code','new_id', 'error']]
                .agg(lambda col: ' '.join(col.astype(str).unique())).reset_index()
                .merge(identification[['id_secondaire','ZONAGE', 'generalPic', 'legalName',  'webPage', 'city', 'country_name_source', 'countryCode', 'country_code', 'country_code_source', 'vat', 'legalRegNumber']],
                    how='right', on=['generalPic', 'countryCode', 'country_code']))

    cols = verif_id.select_dtypes(object).columns
    verif_id[cols] = verif_id[cols].apply(lambda x: x.str.replace(r"\\n|\\t|\\r|\\s+", ' ', regex=True).str.strip())

    verif_id.loc[(verif_id.code=='200')&(verif_id.checked_id==verif_id.new_id), 'indicator_control'] = 'ok'
    # verif_id.loc[(verif_id.indicator_control.isnull())&(verif_id.code=='200')&(verif_id.checked_id==verif_id.new_id), 'indicator_control'] = 'ok'

    verif_id = verif_id.mask(verif_id=='')
    pd.DataFrame(verif_id).drop_duplicates().to_csv(f"{PATH_WORK}check_id_result.csv", sep=';', index=False, encoding='utf-8')
    print('- resultat à checker dans check_id_result.csv (path_work)\n- intégrer csv dans _check_id_result.xlsx\n- sauver le vieil onglet et coller dans new - 🚨 à importer les id en STRING !')

# def ID_resultChecked():
#     filename = '_check_id_result.xlsx'
#     id_verified = pd.read_excel(f"{PATH_WORK}{filename}", dtype=object, keep_default_na=False, sheet_name='new')
#     print(len(id_verified))
#     id_verified.mask(id_verified=='', inplace=True)
    # return id_verified.drop_duplicates()

def ID_resultChecked(paysage_identifiers):
    """
    load modified _check_id_result    
    """
    from remote_process.ID_getSourceRef import get_source_ID
    from remote_process.ID_checkingRefExist import check_id_in_paysage
    from step3_entities.references import paysage_id_extract, paysage_id_extract_prepare
    
    filename = '_check_id_result.xlsx'
    id_verified = pd.read_excel(f"{PATH_WORK}{filename}", dtype=object, keep_default_na=False, sheet_name='new')
    print(len(id_verified))

    id_verified = id_verified.mask(id_verified=='')

    id_verified.loc[(id_verified['in_paysage']==True)&(id_verified['new_id'].isnull()), 'new_id'] = id_verified.loc[(id_verified['in_paysage']==True)&(id_verified['new_id'].isnull()), 'checked_id']
    id_verified.loc[id_verified['new_id']=='nan', 'new_id'] = np.nan
    
    id_verified = id_verified[
        ['generalPic', 'country_code_source', 'countryCode', 'country_code', 'new_id',
                    'id_secondaire', 'ZONAGE']]
    
    id_verified['new_id'] = id_verified['new_id'].astype(str)
    id_verified['id2'] = id_verified['new_id'].str.strip().str.split(' ')
    id_verified = id_verified.explode('id2')
    id_verified = get_source_ID(id_verified, 'id2')
    
    # corrections des id qui ont été sourcés 'siren' alors que ce sont des ror
    mask = ((id_verified['country_code'] != 'FRA') &
            (id_verified['source_id'] == 'siren') &
            (id_verified['id2'].str.match(r'^0([a-z0-9]{6})[0-9]{2}$', na=False)))
    
    id_verified.loc[mask, 'source_id'] = 'ror'
    id_verified.loc[id_verified['source_id'] =='identifiantAssociationUniteLegale', 'source_id'] = 'rna'

    id_verified = check_id_in_paysage(id_verified, 'id2', paysage_identifiers)
    
    id_verified = (id_verified.fillna('')
                   .groupby(['generalPic', 'country_code_source', 'countryCode', 'new_id'])
                   .agg({
                        'source_id': lambda x: ' '.join(x.astype(str)),
                        'resourceId': lambda x: ' '.join(x.astype(str)),
                        'active': lambda x: ' '.join(x.astype(str)),
                        'endDate': lambda x: ' '.join(x.astype(str)),
                        'in_paysage': lambda x: ' '.join(x.astype(str))
                    })
                    .reset_index()
    )
    
    print(f"- size id_verified: {len(id_verified)}")
    return id_verified.drop_duplicates()



def new_ref_source(id_verified, ref_source, extractDate, lien, entities_single, countries):
    """
    UPDATE ref_source with new_id and add new entities pic+cc pairs

    check entities in _id_pic_entities.xlsx
    """
    if 'id_secondaire' not in id_verified.columns :
        id_verified = id_verified.assign(id_secondaire=np.nan)
    if 'ZONAGE' not in id_verified.columns:
        id_verified = id_verified.assign(ZONAGE=np.nan)

    id_verified = id_verified.mask(id_verified=='')
    tmp = (id_verified[['generalPic','country_code_source','countryCode','ZONAGE','id_secondaire',
                        'new_id','source_id','in_paysage', 'active']].drop_duplicates()
    .rename(columns={'new_id':'id'})     
    .merge(ref_source[['generalPic', 'pic_new',  'country_code_source', 'FP', 'ZONAGE','id_secondaire']],
                    how='left', on=['generalPic','country_code_source'], suffixes=['','_y'])
    .apply(lambda x: x.str.strip(), axis=1))

    tmp['FP'] = np.where((tmp['FP'].isnull())|(tmp['FP']==''), 'HE', tmp['FP'])
    tmp.loc[~tmp.FP.str.contains('HE'), 'FP'] = tmp.FP +' HE'
    tmp['FP']=(tmp['FP']          # stack removes `nan`
    .str.split(' ')   # split by `', '`
    .explode()  
    .groupby(level=0)
    .apply(lambda x: ' '.join(x.sort_values(ascending=False).unique()))
    .reindex(tmp.index, fill_value=''))
    tmp=tmp.assign(last_control=extractDate)

    print(f"- size id_verif+ref_source: {len(tmp)}")

    tmp.loc[tmp.ZONAGE.isnull(), 'ZONAGE'] =  tmp.loc[tmp.ZONAGE.isnull(), 'ZONAGE_y']
    tmp.loc[tmp.id_secondaire.isnull(), 'id_secondaire'] =  tmp.loc[tmp.id_secondaire.isnull(), 'id_secondaire_y']

    tmp1 = (lien.rename(columns={'netEuContribution' :'project', 'requestedGrant':'proposal'})
            .groupby(['generalPic','country_code_source'], dropna=False)[['project', 'proposal']]
            .sum()
            .reset_index()
            .drop_duplicates())

    tmp = tmp.merge(tmp1, how='left', on=['generalPic','country_code_source'])
    print(f"- size id_verif+ref_source + subv: {len(tmp)}")

    tmp = (tmp
        .merge(entities_single[['generalPic', 'generalState', 'country_code_source', 'legalName', 'webPage', 'city', 'isInternationalOrganisation']].drop_duplicates(), how='left')
        .drop(columns=['ZONAGE_y','id_secondaire_y','countryCode']))
        
    print(f"- size tmp complet:{len(tmp)}, size tmp only generalPic+cc {len(tmp[['generalPic', 'country_code_source']].drop_duplicates())}")

    if len(tmp)!=len(tmp[['generalPic', 'country_code_source']].drop_duplicates()):
        print(f"{tmp.groupby(['generalPic', 'country_code_source'], dropna=False).size().sort_values(ascending=False)}" )

    outer = ref_source.merge(tmp[['generalPic', 'country_code_source']].drop_duplicates(), how='outer', on=['generalPic', 'country_code_source'], indicator=True)
    anti_join = outer[(outer._merge=='left_only')].drop(['_merge'], axis=1)

    keep = outer.loc[(outer._merge!='left_only'), ['generalPic', 'country_code_source','proposal','project']]
    tmp = tmp.merge(keep, how='left', on=['generalPic', 'country_code_source'])

    tmp['proposal'] = tmp.loc[:,['proposal_x','proposal_y']].sum(axis=1)
    tmp['project'] = tmp.loc[:,['project_x','project_y']].sum(axis=1)

    tmp = (tmp
        .merge(countries[['countryCode_iso3', 'country_name_en', 'country_code']], 
                    how='left', left_on='country_code_source', right_on='countryCode_iso3')
        .drop(columns=['proposal_x','project_x','proposal_y','project_y','countryCode_iso3'])
        .rename(columns={'country_code':'countryCode_parent', 'country_name_en':'country_name_source'}))
    print(f"- size: {len(tmp)} \n{tmp.columns}")
    
    # pour paysage
    # work_csv((tmp.loc[(~tmp['id'].isnull())&(tmp['in_paysage'].str.contains('False'))]&(tmp['project']>0).groupby(['id', 'source_id'], dropna=False)[['proposal', 'project']].sum().reset_index()), 'id_for_paysage')

    ref_source = pd.concat([anti_join, tmp], ignore_index=True).drop_duplicates()

    liste=['legalName', 'city']
    for i in liste:
        ref_source[i] = ref_source[i].apply(lambda x: x.lower().strip() if isinstance(x, str) else x)
    liste=['proposal', 'project']
    for i in liste:
        ref_source[i] = ref_source[i].replace('', np.nan, regex=False)
        ref_source[i] = ref_source[i].astype(float)

    ref_source=ref_source[
        ['generalPic', 'pic_new', 'generalState', 'countryCode_parent', 'country_code_source', 
        'country_name_source', 'id_secondaire', 'ZONAGE', 'id',
        'legalName', 'city', 'url', 'project', 'proposal',  'FP', 'last_control',
        'comments', 'isInternationalOrganisation', 'vat', 'legalRegNumber', 
        'source_id', 'in_paysage', 'active']]

    ref_source.loc[~ref_source['isInternationalOrganisation'].isin([True, False]), 'isInternationalOrganisation'] = False

    print(f"- End size new ref_source:{len(ref_source)}")    


    # print(f"- End size new ref_source:{len(ref_source)}")
    ref_source.to_csv(f"{PATH_WORK}ref_{extractDate}.csv", sep=';', encoding='utf-8', index=False, na_rep='')
    print("# Nouveau REF_SOURCE\n- remplir des ID pour les nouveaux français")