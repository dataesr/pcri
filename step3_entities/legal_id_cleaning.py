import numpy as np, pandas as pd
from functions_shared import unzip_zip
from config_path import PATH_SOURCE
from constant_vars import ZIPNAME, FRAMEWORK

def legal_id_clean(entities_tmp):
    print('### clean REGNUMBER/VAT')
    # nettoyage id
    test= entities_tmp[entities_tmp['countryCode_parent']=='FR'][['generalPic', 'legalName', 'vat', 'legalRegNumber']].drop_duplicates()
    test['reg']=np.where(test['legalRegNumber'].str.contains('^\\D+$'), None, test['legalRegNumber'])
    test['reg']=test['reg'].str.strip().replace('RNA','')
    test['id_a_verif']=test['reg'].str.replace('\\s+','',regex=True)
    test['id_a_verif']=np.where(test['id_a_verif'].str.isnumeric(), test['id_a_verif'], None)
    test['id_a_verif']=np.where(test['id_a_verif'].str.len()==14, test['id_a_verif'].str[:9], test['id_a_verif'])
    test['id_a_verif']=np.where(test['id_a_verif'].str.len()==9, test['id_a_verif'], None)
    test = pd.concat([test, test['reg'].str.split('/|-', expand=True).add_prefix('x')], axis=1 )
    test['id_a_verif']=np.where((test['id_a_verif'].isnull()) & ((test['x0'].str.strip().str.isnumeric()) & ((test['x0'].str.strip().str.len()==9)|(test['x0'].str.strip().str.len()==14))), test['x0'].str[:9], test['id_a_verif'])
    test['id_a_verif']=np.where((test['id_a_verif'].isnull()) & (test['x0'].str.strip().str.contains('^W[0-9]{9}$')), test['x0'].str.strip(), test['id_a_verif'])
    test['id_a_verif']=np.where((test['id_a_verif'].isnull()) & (test['reg'].str.contains('^W[0-9]{9}$')), test['reg'], test['id_a_verif'])
    test['x2']=np.where((test['id_a_verif'].isnull()), test['reg'].str.replace('[^0-9]', '', regex=True), None)
    test['id_a_verif']=np.where((test['id_a_verif'].isnull()) & ((test['x2'].str.len()==9)|(test['x2'].str.len()==14)), test['x2'].str[:9], test['id_a_verif'])
    test['id_a_verif']=np.where((test['id_a_verif'].isnull()) & (test['vat'].str.contains('^FR')), test['vat'].str[4:13], test['id_a_verif'])

    test = test.rename(columns={'x1':'id_a_verif_2'})
    fr_id = test.drop(test.filter(regex='x|reg').columns, axis=1).drop(columns={'legalName', 'vat', 'legalRegNumber'})
    print(f"size fr_id {len(fr_id)}")

    n=fr_id.groupby(['generalPic']).filter(lambda x: x['id_a_verif'].count() > 1.)
    if len(n)>0:
        print(f'doublons dans fr_id')
        
    # test[test['generalPic'].isin(['888639214','889353328','950543353','896682066','905870391','887902790','898517403','890125933', '905006218', '953634452', '999894819', '991070050', '894743521', '888989190'])]
    identification = entities_tmp.merge(fr_id, how='left', on="generalPic")
    print(f"Size identification:{len(identification)}, size entities_tmp:{len(entities_tmp)}")
    return identification

def entities_link(entities_tmp):
    print("### clean LINKS entities")
    entitiesLinks = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'legalEntitiesLinks.json', 'utf8')

    entitiesLinks=pd.DataFrame(entitiesLinks).astype(str)
    entitiesLinks=entitiesLinks.merge(entities_tmp[['generalPic']].drop_duplicates(), how='inner', on='generalPic')
    links=entitiesLinks[entitiesLinks['dataset'].isin(['GRID', 'ROR', 'CNRS research group', 'French national research structure repertory', 'SIREN','SIRET', 'Repertoire national des associations'])]

    links=links.sort_values(['generalPic', 'dataset'], ascending=[True, True])
    links['freq'] = links.groupby('generalPic')['dataset'].transform('count')
    links=links.loc[~((links['freq']>1)&(links['dataset']=='GRID')), ['generalPic', 'freq', 'dataset', 'linkId']]

    links['freq'] = links.groupby('generalPic')['dataset'].transform('count')
    links.loc[links['dataset']=='ROR', 'linkId']='R'+links['linkId']
    links.loc[links['dataset']=='SIRET', 'linkId']=links['linkId'].str[0:9]
    links.loc[links['dataset']=='SIRET', 'dataset']='SIREN'
    links = links[['generalPic', 'linkId']].drop_duplicates()
    multiple=links.groupby(['generalPic'], as_index = False).agg(lambda x: ';'.join(filter(None, x)))
    return multiple


def list_to_check(identificaton):
    print("### create liste ID pour référentiel")
    check_id_liste = pd.concat([identificaton[['generalPic','countryCode','country_code_mapping', 'countryCode_parent', 'id']].rename(columns={'id':'check_id'}).assign(stock_id='ref'), 
                        identificaton[['generalPic','countryCode','country_code_mapping', 'countryCode_parent', 'id_a_verif']].rename(columns={'id_a_verif':'check_id'}).assign(stock_id='vat'), 
                        identificaton[['generalPic','countryCode','country_code_mapping', 'countryCode_parent', 'id_a_verif_2']].rename(columns={'id_a_verif_2':'check_id'}).assign(stock_id='vat'), 
                        identificaton[['generalPic','countryCode','country_code_mapping', 'countryCode_parent', 'linkId']].rename(columns={'linkId':'check_id'}).assign(stock_id='link')], 
                                                                                     ignore_index=True)
    check_id_liste = check_id_liste[~check_id_liste.check_id.isnull()].drop_duplicates()
    check_id_liste.loc[check_id_liste.check_id.str.contains(';'), 'check_id'] = check_id_liste['check_id'].str.split(';')
    check_id_liste = check_id_liste.explode('check_id').drop_duplicates()
    # check_id_liste = list(set(check_id_liste.check_id.str.split(';', expand=True).stack().reset_index(drop=True)))

    print(f" number of id to check:{len(check_id_liste.check_id.unique())}")
    return check_id_liste