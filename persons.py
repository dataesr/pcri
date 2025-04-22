import pandas as pd, pickle, numpy as np, warnings, time, os
warnings.filterwarnings("ignore", "FutureWarning: Setting an item of incompatible dtype is deprecated and will raise an error in a future version of pandas")
pd.options.mode.copy_on_write = True
from config_path import PATH_CLEAN, PATH_API
from functions_shared import chunkify, work_csv
from step7_persons.prep_persons import persons_preparation
from step7_persons.affiliations import affiliations, persons_files_import, persons_api_simplify, persons_results_clean

CSV_DATE='20250321'

#######
persons_preparation(CSV_DATE)
#######

PATH_PERSONS=f"{PATH_API}persons/"
perso_part = pd.read_pickle(f"{PATH_CLEAN}persons_participants.pkl")
perso_app = pd.read_pickle(f"{PATH_CLEAN}persons_applicants.pkl")

pp = pd.concat([perso_part.drop_duplicates(), perso_app.drop_duplicates()], ignore_index=True)
pp['contact2']=pp.contact.str.replace('-', ' ')

####################################################################
# requests openalex
#PREPRATION data for request openalex
lvar=['contact2','orcid_id','country_code', 'iso2','destination_code','thema_code','nationality_country_code']
# toutes les structures participants françaises, contact français, tous projets individuels
mask=((pp.country_code=='FRA')|(pp.nationality_country_code=='FRA')|(pp.destination_code.isin(['COG', 'PF', 'STG', 'ADG', 'POC','SyG', 'PERA', 'SJI'])))&~((pp.contact2.isnull())&(pp.orcid_id.isnull()))
df=pp.loc[mask, lvar].sort_values(['country_code','orcid_id'], ascending=False).drop_duplicates()
print(f"size pp: {len(df)}, info sur pp with orcid: {len(df.loc[df.orcid_id.isnull()])}")

#############
# request OPENALEX
affiliations(df, PATH_PERSONS, CSV_DATE)
######################################################################

oth=persons_files_import('other', PATH_PERSONS)
em=persons_files_import('erc', PATH_PERSONS)

oth=persons_api_simplify(oth)
em=persons_api_simplify(em)

oth=persons_results_clean(oth)
em=persons_results_clean(em)

result=pd.concat([oth, em], ignore_index=True)
result=result[~result.astype(str).duplicated()]

#######################################################################

## provisoire
result=pd.read_pickle(f"{PATH_PERSONS}persons_{CSV_DATE}.pkl")
######################################################################

# merge match orcid
df=pp.merge(result[result.match=='orcid'], how='left', left_on=['orcid_id'], right_on=['orcid_openalex'], indicator=True)
df_orcid=df[df._merge=='both']
print(f" size match orcid: {len(df_orcid)}, size match orcid with entities equals: {len(df_orcid[(df_orcid.entities_id==df_orcid.numero_paysage)|(df_orcid.entities_id==df_orcid.numero_ror)])}")

def flter_year(df):
     df.replace(to_replace=[None, 'None'], value=np.nan, inplace=True)
     df['filt_year']=df.apply(lambda x: x['call_year'] in x['years'], axis=1)
     df.drop_duplicates(inplace=True)
     df=df.loc[(df.filt_year==True)].drop(columns='years').drop_duplicates()
     print(len(df))
     return df
df_orcid=flter_year(df_orcid)
print(f"size df_orcid same year: {len(df_orcid)}")

# find new id for pic id
df_picid=df_orcid.loc[(df_orcid.country_code==df_orcid.institution_country)&(df_orcid.entities_id.str.startswith('pic', na=False)), 
        ['generalPic', 'entities_name','id_secondaire', 'institution_ror', 'institution_name',
        'institution_country','numero_ror', 'numero_paysage', 'num_nat_struct']].drop_duplicates()

# data with nns
df_nns=df_orcid.loc[(~df_orcid.num_nat_struct.isnull())&(df_orcid.id_secondaire.isnull())].drop_duplicates()
print(f"size df_nns_orcid: {len(df_nns)}")

# liste complète orcid_id match
df_orcid=(pd.concat(
     [df_orcid.loc[(df_orcid.entities_id==df_orcid.numero_paysage)|(df_orcid.entities_id==df_orcid.numero_ror)], 
      df_nns], ignore_index=True)
      .drop_duplicates())
df_orcid=df_orcid.assign(orcid_clean=df_orcid.orcid_id, common=True)
print(f"size df_orcid: {len(df_orcid)}")

#####################################################
# full_name
df=(df.loc[df._merge!='both'].drop(columns=result.columns).drop_duplicates()
          .merge(result[result.match=='full_name'], 
           how='inner', 
           left_on=['contact2', 'country_code'], 
           right_on=['display_name','institution_country'])
           .drop(columns='_merge'))
print(f"size df name: {len(df)}, size match name with entities equals: {len(df[(df.entities_id==df.numero_paysage)|(df.entities_id==df.numero_ror)])}")

df_name=flter_year(df)
print(f"size df name same year: {len(df_name)}")

# select rows with entities equal
df1=(df_name.loc[(df_name.entities_id==df_name.numero_paysage)|(df_name.entities_id==df_name.numero_ror),
     ['generalPic', 'contact2', 'orcid_openalex', 'orcid_id']]
     .drop_duplicates())

# selectrows with nns conditions pic+contact with good entities match 
df2=df_name.merge(df1, how='inner', on=['generalPic', 'contact2', 'orcid_openalex', 'orcid_id'])
df2=df2.loc[~df2.num_nat_struct.isnull()]

df1=pd.concat([df_name.loc[(df_name.entities_id==df_name.numero_paysage)|(df_name.entities_id==df_name.numero_ror)], df2], ignore_index=True).drop_duplicates()

def orcid_clean(df):
     df['orcid_na']=np.where(df.orcid_id.isnull(), True, False)
     df['common']=np.where(df.orcid_id==df.orcid_openalex, True, False)
     df.loc[df.common==True, 'orcid_clean']=df.loc[df.common==True, 'orcid_id']
     df['orcid_id_nb']=df.groupby(['generalPic', 'contact2'], dropna=False)['orcid_id'].transform('nunique')
     df['orcid_openalex_nb']=df.groupby(['generalPic', 'contact2'], dropna=False)['orcid_openalex'].transform('nunique')

     df.loc[(df.common==False)&(df.orcid_openalex_nb==1), 'orcid_clean']=df.loc[(df.common==False)&(df.orcid_openalex_nb==1), 'orcid_openalex']
     df['orcid_clean'] = df.sort_values(['generalPic', 'contact2', 'orcid_clean']).groupby(['generalPic', 'contact2'], group_keys=True)['orcid_clean'].ffill()
     df.loc[(df.orcid_openalex_nb==0)&(df.orcid_id_nb==1), 'orcid_clean']=df.loc[(df.orcid_openalex_nb==0)&(df.orcid_id_nb==1), 'orcid_id']
     df['orcid_clean'] = df.sort_values(['generalPic', 'contact2', 'orcid_clean']).groupby(['generalPic', 'contact2'], group_keys=True)['orcid_clean'].ffill()
     df.loc[(df.orcid_openalex_nb>1)&(df.orcid_id_nb==1)&(df.common==False), 'orcid_clean']=df.loc[(df.orcid_openalex_nb>1)&(df.orcid_id_nb==1)&(df.common==False), 'orcid_id']
     df['orcid_clean'] = df.sort_values(['generalPic', 'contact2', 'orcid_clean']).groupby(['generalPic', 'contact2'], group_keys=True)['orcid_clean'].ffill()
      
     temp=df.loc[~df.orcid_clean.isnull(), ['generalPic', 'contact2', 'orcid_clean']].drop_duplicates().rename(columns={'contact2':'contact3', 'orcid_clean':'orcid_clean3'})
     temp1=df.loc[df.orcid_clean.isnull()].merge(temp, how='inner', on=['generalPic'])[['generalPic', 'contact2', 'contact3', 'orcid_clean3']]
     temp1['filt_name']=temp1.apply(lambda x: True if (x['contact2'] in x['contact3'])|(x['contact3'] in x['contact2']) else False, axis=1)
     temp1=temp1.loc[temp1.filt_name==True, ['generalPic', 'contact2', 'orcid_clean3']].drop_duplicates()
     df=df.merge(temp1, how='left', on=['generalPic', 'contact2'])
     df.loc[df.orcid_clean.isnull(), 'orcid_clean'] = df.loc[df.orcid_clean.isnull(), 'orcid_clean3']
     df.loc[~df.orcid_clean3.isnull(), 'orcid_merged_fuzzy_name']=True
     
     return df.drop(columns=['orcid_clean3'])

df1=orcid_clean(df1)
print(f"size df name : {len(df1)}")

pers_result_merged=(pd.concat([df_orcid, df1], ignore_index=True)[
                    ['stage','project_id', 'generalPic', 'role','title_clean', 'gender', 'email', 'tel_clean', 
                    'domaine_email','birth_country_code', 'nationality_country_code','sending_country_code',
                    'contact', 'country_code', 'institution_shift','entities_id', 
                    'country_code_mapping','contact2', 'match','orcid_clean', 'common','orcid_merged_fuzzy_name',
                     'num_nat_struct']]
                    .drop_duplicates())
print(f"size pers_result_merged : {len(pers_result_merged)}")
# pd.to_pickle(pers_result_merged, f"{PATH_CLEAN}persons_{CSV_DATE}.pkl")


temp=(pers_result_merged.groupby(['stage', 'project_id', 'generalPic', 'contact', 'contact2'], as_index=False)[
     ['orcid_clean', 'num_nat_struct']]
     # .agg(lambda x: list(set( x.dropna())))
     .agg(lambda x: ';'.join( x.dropna().unique()))
     .drop_duplicates()
     )

pp=pp.merge(temp, how='outer', on=['stage', 'project_id', 'generalPic', 'contact', 'contact2'], indicator=True)

pp1=pp[pp._merge=='both']
print(len(pp1))
pp2=(pp[pp._merge!='both'].drop(columns=['orcid_clean', 'num_nat_struct', '_merge'])
    .merge(temp[['project_id', 'contact', 'contact2', 'orcid_clean']], how='outer', on=['project_id', 'contact', 'contact2'], indicator=True)
    .query('_merge=="both"'))
pp2=pp2[~pp2.astype(str).duplicated()]
pp1=pd.concat([pp1,pp2], ignore_index=True)

res=(pp.drop(columns="_merge").merge(pp1[['stage', 'project_id', 'generalPic', 'contact', 'contact2']]
    .drop_duplicates(), how='outer', 
    on=['stage', 'project_id', 'generalPic', 'contact', 'contact2'], indicator=True)
    .query('_merge=="left_only"'))
res=pd.concat([res, pp1], ignore_index=True)
print(len(res))

pd.to_pickle(res, f"{PATH_CLEAN}persons_current.pkl")