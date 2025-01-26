import pandas as pd
pd.options.mode.copy_on_write = True
from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE, PATH_CLEAN, PATH_ORG, PATH_WORK
from functions_shared import *

CSV_DATE='20241011'


###############################
participation = pd.read_pickle(f"{PATH_CLEAN}participation_current.pkl") 
project = pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl") 

print(f"size participation: {len(participation)}")
######################
perso_app = unzip_zip(f'he_proposals_ecorda_pd_{CSV_DATE}.zip', f"{PATH_SOURCE}{FRAMEWORK}/", "applicant_persons.csv", 'utf-8')

perso_app = (perso_app.loc[perso_app.FRAMEWORK=='HORIZON',
['PROPOSAL_NBR', 'GENERAL_PIC', 'APPLICANT_PIC', 'ROLE', 'FIRST_NAME',
       'FAMILY_NAME', 'TITLE', 'GENDER', 'PHONE', 'EMAIL',
       'RESEARCHER_ID', 'ORCID_ID', 'GOOGLE_SCHOLAR_ID','SCOPUS_AUTHOR_ID']]
             .rename(columns=str.lower)
             .rename(columns={'proposal_nbr':'project_id', 'general_pic':'generalPic', 'applicant_pic':'pic', 'family_name':'last_name'})
             .assign(stage='evaluated'))

perso_app['nb'] = perso_app.groupby(['project_id', 'generalPic', 'pic'])['last_name'].transform('count')
perso_app.columns
print(f"size app: {len(perso_app)}\ncolumns:{perso_app.columns}")


perso_app = (perso_app
             .merge(participation.loc[participation.stage=='evaluated', ['project_id', 'generalPic', 'country_code']], how='inner', on=['project_id', 'generalPic'])
             .merge(project.loc[participation.stage=='evaluated', ['project_id', 'call_year', 'thema_name_en', 'destination_name_en']], how='inner', on=['project_id'])
)
print(f"size app lien avec participation clean : {len(perso_app)}\ncolumns:{perso_app.columns}")

##################
y = perso_app.loc[(perso_app.country_code=='FRA')&(~perso_app.phone.isnull()), ['phone']]
y['tel_clean']=y.phone.str.replace(r"(^\++[0-9]{1,3}\s+)", '', regex=True)
y['tel_clean']=y.tel_clean.str.replace(r"[^0-9]+", '', regex=True)
y['tel_clean']=y.tel_clean.str.replace(r"^(33|033|0033)", '', regex=True).str.rjust(10, '0')
y.loc[(y.tel_clean.str.len()>10)&(y.tel_clean.str[0:1]=='0'), 'tel_clean'] = y.tel_clean.str[0:10]
y['tel_clean']=y.tel_clean.str.replace(r"^0+$", '', regex=True)
# work_csv(y, 'tel_perso')
perso_app = pd.concat([perso_app, y[['tel_clean']]], axis=1)

#######################
mail_del=["gmail", "yahoo", "hotmail", "wanadoo", "aol", "free", "skynet", "outlook", "icloud", "googlemail"]

perso_app['domaine'] = perso_app.email.str.split('@').str[1].str.split('.').str[:-1].fillna('').apply(' '.join)
tmp = perso_app.loc[~perso_app.domaine.isnull(), ['domaine']]

for el in mail_del:
    m = r"^"+el+r"($|\s)"
    tmp.loc[tmp['domaine'].str.contains(m, case=True, flags=0, na=None, regex=True) == True, 'domaine_email'] = ''
    tmp.loc[tmp['domaine_email'].isnull(), 'domaine_email'] = tmp['domaine']

perso_app = pd.concat([perso_app, tmp], axis=1).drop(columns='domaine')

###############
def prop_contact(tab):
    from unidecode import unidecode
    cols = ['role', 'first_name', 'last_name','title', 'gender']
    tab[cols] = tab[cols].applymap(lambda s:s.casefold() if type(s) == str else s)
    for i in cols:
        tab[i] = tab[i].astype('str').apply(unidecode)
    return tab

perso_app = prop_contact(perso_app)

###########
for f in [ 'first_name', 'last_name']:
    perso_app[f] = perso_app[f].fillna('')
    perso_app[f] = perso_app[f].str.strip().str.replace(r"\s+", '-', regex=True)

perso_app['contact'] = perso_app.last_name.astype(str).str.lower() + ' ' + perso_app.first_name.astype(str).str.lower()
##########

perso_app.to_pickle(f"{PATH_WORK}perso_app.pkl")