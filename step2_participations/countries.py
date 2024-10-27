from constant_vars import FRAMEWORK, ZIPNAME
from config_path import *
from functions_shared import unzip_zip
import json, pandas as pd, numpy as np

def country_load(framework, liste_country):
    print("### LOADING COUNTRY")
    data = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", "countries.json", 'utf8')
    data = pd.DataFrame(data)
    
    df = (data.loc[data.framework==framework].drop_duplicates()
            [['isoCountryCode','countryCode', 'countryName', 'countryGroupAssociationCode','countryGroupAssociation']])
    
    mask = list(set(data.loc[(data.framework=='H2020') & (data.countryCode.isin(liste_country))].countryCode)-set(data.loc[(data.framework=='HORIZON') & (data.countryCode.isin(liste_country))].countryCode))
    if mask:
        diff = data.loc[data.countryCodeisin(mask), ['isoCountryCode','countryCode', 'countryName', 'countryGroupAssociationCode','countryGroupAssociation']]
        df = pd.concat([df, diff], ignore_index=True)

    countries = (df[['countryCode', 'countryName', 'isoCountryCode']]
                 .rename(columns={'isoCountryCode':'country_code_mapping', 'countryName':'country_name_mapping'}))

    with open('data_files/countries_parent.json', 'r+') as fp:
        countries_parent = json.load(fp)


    for k,v in countries_parent.items():
        countries.loc[countries.countryCode==k, 'countryCode_parent'] = v
    countries.loc[countries.countryCode_parent.isnull(), 'countryCode_parent'] = countries.countryCode

    countries = (countries
                 .merge(df[['countryCode', 'isoCountryCode', 'countryName']].rename(columns={'countryCode':'countryCode_parent'}), 
                                how='left', on='countryCode_parent')
                 .rename(columns={'isoCountryCode':'country_code', 'countryName':'country_name_en'}))

    df = (df[['countryCode', 'countryGroupAssociationCode', 'countryGroupAssociation']]
        .rename(columns={'countryGroupAssociationCode':'country_association_code',
                         'countryGroupAssociation':'country_association_name_en',
                         'countryCode':'countryCode_parent'}))
    df.loc[df.country_association_code.isin(['THIRD-OCEF', 'THIRD-OCAF', 'THIRD']), 'country_group_association_code'] = "THIRD"
    df.loc[df.country_association_code.isin(['ASSOCIATED', 'MEMBER-STATE']), 'country_group_association_code'] = "MEMBER-ASSOCIATED"
    df['country_group_association_name_en'] = np.where(df.country_group_association_code=='THIRD', "Other countries", "Member States or associated")

    df.loc[df.country_group_association_code=='MEMBER-ASSOCIATED', 'country_group_association_name_fr'] = 'Pays membres ou associés'
    df.loc[df.country_group_association_code=='THIRD', 'country_group_association_name_fr'] = 'Pays tiers'

    statut_len = 5
    if len(df['country_association_code'].drop_duplicates()) != statut_len:
        diff = len(df['country_association_code'].unique())-statut_len
        if diff>0:
            print(f"1- check status countries -> {len(df['country_association_code'].unique())-statut_len} new status")
        else:
            print(f"2 - info status countries -> {df['country_association_code'].nunique()-statut_len} status in data {df['country_association_code'].unique()}")

    countries = countries.merge(df, how='left', on='countryCode_parent')  

    with open('data_files/countries_fr.json', 'r+', encoding='UTF-8') as fp:
                countries_fr = json.load(fp)
    countries_fr = pd.DataFrame(countries_fr).rename(columns={'country_name':'country_name_fr', 'country_code':'countryCode_parent'})
    countries = countries.merge(countries_fr, how='left', on='countryCode_parent')        

    cc=['countryCode', 'countryCode_parent', 'country_code', 'country_association_code','country_group_association_code']
    cn_fr=['country_name_fr','country_group_association_name_fr']
    cn_en=['country_name_en', 'country_association_name_en','country_group_association_name_en']
    row=countries.shape[0]
    for code in ['ZOE','ZOI']:
        for i in cc:
            countries.loc[row,i]=code
        row=row+1

    for j in cn_fr:
        countries.loc[countries.countryCode=='ZOE', j]='Zone organisations européennes'
        countries.loc[countries.countryCode=='ZOI', j]='Zone organisations internationales'
    for j in cn_en:
        countries.loc[countries.countryCode=='ZOE', j]='European organisations area'
        countries.loc[countries.countryCode=='ZOI', j]='International organisations area'   

    with open('data_files/countries_genres.json', 'r+', encoding='UTF-8') as fp:
        article_fr = json.load(fp)
    article_fr = pd.DataFrame.from_dict(article_fr, orient='index').reset_index().rename(columns={'index':'country_code'})
    article_fr = article_fr.apply(lambda x: x.str.strip() if x.dtype.name == 'object' else x, axis=0)

    countries=countries.merge(article_fr, how='left', on='country_code').drop(columns='label')
   
    countries.to_csv(f"{PATH_CONNECT}country_current.csv", index=False, encoding="UTF-8", sep=";", na_rep='')
    file_name = f"{PATH_CLEAN}country_current.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(countries, file)

    # countries = countries.drop('countryCode_parent', axis=1).drop_duplicates()
    if len(countries.columns[countries.isnull().any()])>0:
        list_v = countries.columns[countries.isnull().any()]
        for v in list_v:
            print(f"des valeurs nulles pour ces lignes: {pd.DataFrame(countries[countries[v].isnull()][['countryCode',v]])}")
            
    return countries
