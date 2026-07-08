import pandas as pd, re

def get_pic(df, pic_new, countries):
    
    print(f"- size entities_info before pic: {len(df)}")
    pic = pd.merge(df, pic_new, how='left', on=['generalPic', 'country_code_source'])[['generalPic', 'pic_new', 'country_code_source', 'project']]
    # pic.loc[pic['pic_new'].notnull(), 'id_clean'] = pic.loc[pic['pic_new'].notnull(), 'pic_new']
    pic.loc[pic['pic_new'].isnull(), 'pic_new'] = pic.loc[pic['pic_new'].isnull(), 'generalPic']
    pic['project'] = pic.groupby(['pic_new', 'country_code_source'], dropna=False)['project'].transform('sum')
    pic = pic[['generalPic', 'pic_new', 'country_code_source', 'project']].rename(columns={'generalPic':'oldPic'}).drop_duplicates()


    pic = (pd.merge(pic, 
                   df,
                   how='left',
                   left_on=['pic_new', 'country_code_source'], 
                   right_on=['generalPic', 'country_code_source'])
                   .drop(columns=['generalPic'])
                   .rename(columns={'oldPic':'generalPic'})
                   .drop_duplicates())

    print(f"- size merge entities_info + new_pic: {len(pic)}")

    # Categorie juridique pour les étrangers
    NO_FRA=(pic['country_code']!='FRA')

    mapping = {
        "PUB":"y4k88y4k88y4k88",
        "HES":"y4k88y4k88y4k88",
        "PRC":"3jxtc3jxtc3jxtc",
        "REC":"3jxtc3jxtc3jxtc",
        "OTH":"3jxtc3jxtc3jxtc"
        }
    
    for k,v in mapping.items():
        pic.loc[NO_FRA&(pic['legalEntityTypeCode']==k), 'cj'] = v

    # categorie paysage
    mask=NO_FRA&(pic['legalEntityTypeCode']=='PUB')
    pic.loc[mask&(pic['legalType']!='PUBLIC')&(pic['isNonProfit']!=True), 'paysageCat'] = '2fy6x'
    pic.loc[mask&(pic['legalType']!='PUBLIC')&(pic['isNonProfit']==True), 'paysageCat'] = '4urre'    
    pic.loc[mask&(pic['paysageCat'].isnull()), 'paysageCat'] = 'rslqh'

    pic.loc[NO_FRA&(pic['legalEntityTypeCode']=='HES'), 'paysageCat'] = '8rh6n'
    pic.loc[NO_FRA&(pic['legalEntityTypeCode']=='REC'), 'paysageCat'] = 'mjy50'

    mask=NO_FRA&(pic['legalEntityTypeCode']=='PRC')&(pic['legalType']=='PUBLIC')
    pic.loc[mask&(pic['isNonProfit']==True),'paysageCat'] = '4urre'
    pic.loc[mask&(pic['isNonProfit']==False),'paysageCat'] = '2fy6x'
    pic.loc[mask&(pic['isNonProfit'].isnull()), 'paysageCat'] = 'rslqh'
    pic.loc[NO_FRA&(pic['legalEntityTypeCode']=='PRC')&(pic['paysageCat'].isnull()), 'paysageCat'] = '2fy6x'   
    
    mask=NO_FRA&(pic['legalEntityTypeCode']=='OTH')
    pic.loc[mask&(pic['legalType']!='PUBLIC')&(pic['isNonProfit']==True), 'paysageCat'] = '4urre'
    pic.loc[mask&(pic['paysageCat'].isnull()), 'paysageCat'] = 'xijgv'
 

    # pic = pd.merge(pic, countries[['countryCode', 'country_code']].drop_duplicates(), how='left', on='countryCode')
    pic = (pd.merge(pic, countries[['country_name_fr', 'countryCode_iso3']]
                   .drop_duplicates(), 
                   how='left', 
                   left_on='country_code', 
                   right_on='countryCode_iso3')
                   .drop(columns='countryCode_iso3')
    )
  
    if any(pic.groupby(['generalPic', 'pic_new', 'country_code_source']).size()>1):
        print(f"- gen+cc duplicated: {pic.groupby(['generalPic', 'pic_new', 'country_code_source']).size().reset_index(name='size').query('size>1')}")

    return pic