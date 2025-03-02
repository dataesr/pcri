import pandas as pd, pickle, numpy as np, warnings, time, os
warnings.filterwarnings("ignore", "FutureWarning: Setting an item of incompatible dtype is deprecated and will raise an error in a future version of pandas")
pd.options.mode.copy_on_write = True
from config_path import PATH_CLEAN, PATH_API
from functions_shared import chunkify, work_csv
from step7_persons.prep_persons import persons_preparation
from step7_persons.affiliations import affiliations, persons_files_import, persons_api_simplify, persons_results_clean

CSV_DATE='20250121'

#######
# persons_preparation(CSV_DATE)
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

# lvar=['project_id', 'generalPic', 'role', 'contact',
#        'title_clean', 'gender', 'email', 'tel_clean', 'domaine_email',
#        'orcid_id', 'birth_country_code', 'nationality_country_code',
#        'host_country_code', 'sending_country_code', 'iso2', 'stage', 'contact2',
#        'country_code', 'shift', 'call_year', 'thema_code', 'destination_code',
#        'entities_id', 'entities_name', 'id_secondaire', 'country_code_mapping']

# mask=((pp.country_code=='FRA')|(pp.nationality_country_code=='FRA')|(pp.destination_code.isin(['COG', 'PF', 'STG', 'ADG', 'POC','SyG', 'PERA', 'SJI'])))&(~(pp.contact.isnull()&pp.orcid_id.isnull()))
# df=pp.loc[mask, lvar].sort_values(['country_code','orcid_id'], ascending=False).drop_duplicates()


# merge match orcid
df=pp.merge(result[result.match=='orcid'], how='left', left_on=['orcid_id'], right_on=['orcid_openalex'], indicator=True)
df_orcid=df[df._merge=='both']
print(len(df_orcid))

def test(df):
     df.replace(to_replace=[None, 'None'], value=np.nan, inplace=True)
     df['filt_year']=df.apply(lambda x: x['call_year'] in x['years'], axis=1)
     df.drop_duplicates(inplace=True)
     df=df.loc[(df.filt_year==True)].drop(columns='years').drop_duplicates()
     print(len(df))
     return df
df_orcid=test(df_orcid)

# data with nns
df_nns=(df_orcid.loc[(~df_orcid.num_nat_struct.isnull())&(df_orcid.id_secondaire.isnull()), 
     ['match', 'project_id','generalPic', 'contact2', 'orcid_id', 'orcid_openalex', 'num_nat_struct']]
     .drop_duplicates())

# find new id for pic id
df_picid=df_orcid.loc[(df_orcid.country_code==df_orcid.institution_country)&(df_orcid.entities_id.str.startswith('pic', na=False)), 
        ['generalPic', 'entities_name','id_secondaire', 'institution_ror', 'institution_name',
        'institution_country','numero_ror', 'numero_paysage', 'num_nat_struct']].drop_duplicates()


# liste complète orcid_id match
df_orcid=df_orcid.loc[(df_orcid.entities_id==df_orcid.numero_paysage)|(df_orcid.entities_id==df_orcid.numero_ror)]

#####################################################
# full_name
df=(df.loc[df._merge!='both'].drop(columns=result.columns).drop_duplicates()
          .merge(result[result.match=='full_name'], 
           how='inner', 
           left_on=['contact2', 'country_code'], 
           right_on=['display_name','institution_country'])
           .drop(columns='_merge'))
df_name=test(df)


# select rows with entities equal
df1=(df_name.loc[(df_name.entities_id==df_name.numero_paysage)|(df_name.entities_id==df_name.numero_ror),
     ['generalPic', 'contact2', 'orcid_openalex', 'orcid_id']]
     .drop_duplicates())

# selectrows with nns conditions pic+contact with good entities match 
df2=df_name.merge(df1, how='inner', on=['generalPic', 'contact2', 'orcid_openalex', 'orcid_id'])
df1=df2.loc[~df2.num_nat_struct.isnull()]

df2=pd.concat([df1, df2], ignore_index=True)

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
     return df

df1=orcid_clean(df1)


# complete  NNS datatable with full_name result -> to merge to entities (gilberinette)
df_nns=pd.concat([df_nns, df1.loc[~df1.num_nat_struct.isnull(), ['match','project_id','generalPic','contact2','orcid_id','orcid_openalex', 'orcid_clean','num_nat_struct']]
     .drop_duplicates()], ignore_index=True)


pers_result_merged=pd.concat([df_orcid, df1.loc[(df1.entities_id==df1.numero_paysage)|(df1.entities_id==df1.numero_ror)]])





temp=df3.loc[~df3.orcid_clean.isnull(), ['generalPic', 'contact2', 'orcid_clean']].drop_duplicates().rename(columns={'contact2':'contact3', 'orcid_clean':'orcid_clean3'})

temp1=df3.loc[df3.orcid_clean.isnull()].merge(temp, how='inner', on=['generalPic'])[['generalPic', 'contact2', 'contact3', 'last_name', 'orcid_clean3']]
temp1['filt_name']=temp1.apply(lambda x: True if (x['contact2'] in x['contact3'])|(x['contact3'] in x['contact2']) else False, axis=1)
temp1=temp1.loc[temp1.filt_name==True, ['generalPic', 'contact2', 'orcid_clean3']].drop_duplicates()

df3=df3.merge(temp1, how='left', on=['generalPic', 'contact2'])


#same entities
df1=df.loc[(df.entities_id==df.numero_paysage)|(df.entities_id==df.numero_ror)]


temp=df.groupby(['generalPic', 'contact'], dropna=False)['orcid_openalex'].nunique(dropna=False).reset_index()
temp=temp[temp.orcid_openalex>1].drop(columns='orcid_openalex')
df=df.merge(temp, how='left', on=['generalPic', 'contact'], indicator=True)
df.loc[df._merge=='both', 'orcid_openalex'] = df.loc[df._merge=='both'].sort_values(['generalPic', 'contact', 'orcid_openalex']).groupby(['generalPic', 'contact'], group_keys=True)['orcid_openalex'].ffill()
df.drop(columns='_merge', inplace=True)

df=df.assign(orcid_clean=np.where(df.orcid_openalex.isnull(), df.orcid_id, df.orcid_openalex))


#same entities
df1=df.loc[(df.entities_id==df.numero_paysage)]
df1=(df1.assign(orcid_clean=np.where(df1.orcid_openalex.isnull(), df1.orcid_id, df1.orcid_openalex))
     .drop(columns=['orcid_id', 'rows_by_name_orcid', 'orcid_openalex'])
     .drop_duplicates()
     [['project_id', 'generalPic', 'contact', 'domaine_email', 'entities_id', 'orcid_clean']])

# not same entities
df2=df.loc[(df.entities_id==df.numero_ror)]




#extract entities_id begun by PIC
work_csv(df.loc[df.entities_id.str.startswith('pic', na=False), 
        ['generalPic', 'entities_name','id_secondaire', 'institution_ror', 'institution_name',
        'institution_country','numero_ror', 'numero_paysage', 'num_nat_struct']].drop_duplicates(), 'verif_id_struct_from openalex')