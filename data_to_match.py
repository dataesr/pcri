import pandas as pd, time, re, numpy as np
pd.options.mode.copy_on_write = True
from IPython.display import HTML
# from unidecode import unidecode
from functions_shared import *
from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH, PATH_SOURCE, PATH_CLEAN, PATH_ORG, PATH_WORK
from matcher import matcher
from step9_affiliations.prep_entities import data_import, prep

participation, entities_info, proj, nuts, countries, lien, pp_app, pp_part = data_import()

#######
app=prep('evaluated', pp_app)
part=prep('successful', pp_part)
print(f"app {len(app)}, part {len(part)}")

lp = part[['project_id', 'generalPic', 'pic', 'country_code_mapping']].drop_duplicates()
app = app.merge(lp, how='left', indicator=True).query('_merge=="left_only"').drop(columns='_merge')

#######
struct = pd.concat([app, part], ignore_index=True)
struct['nb_stage'] = struct.groupby(['project_id', 'generalPic', 'country_code', 'orderNumber','calculated_pic','stage'])['department'].transform('count')
struct = (struct
             .rename(columns={'country_code_mapping':'country_code_mapping_dept', 'country_code':'country_code_dept', 'nutsCode':'department_nuts'}))
print(f"size structure {len(struct)}")

if len(participation[['stage','project_id','generalPic','orderNumber', 'country_code','country_code_mapping']].drop_duplicates())!=len(participation[['stage','project_id','generalPic','orderNumber', 'country_code','country_code_mapping','role','participates_as']].drop_duplicates()):
    print("Attention doublon d'une participation avec ajout de role+participates_as")


########
part = participation[['project_id','generalPic','orderNumber', 'country_code','country_code_mapping','stage']].drop_duplicates()
print(f"size part {len(part)}")
part = (part
        .merge(struct, 
               how='outer', 
               left_on=['stage','project_id', 'generalPic', 'orderNumber', 'country_code_mapping'], 
               right_on=['stage','project_id', 'generalPic', 'orderNumber', 'country_code_mapping_dept'],
               indicator=True)
        .drop_duplicates())
part['nb'] = part.groupby(['stage', 'project_id', 'generalPic', 'orderNumber'])['_merge'].transform('count')
part['nb2'] = part.groupby(['stage', 'project_id', 'generalPic', 'orderNumber'])['_merge'].transform('count')
part[['country_code','country_code_mapping']] = part[['country_code','country_code_mapping']].fillna(part.groupby(['stage', 'project_id', 'generalPic', 'orderNumber'])[['country_code','country_code_mapping']].ffill())
print(f"size part {len(part)}")

part = part.loc[~((part.nb2>1)&(part.department.isnull()))]
print(f"size part {len(part)}")

##########
structure = (part
             .merge(entities_info[['generalPic', 'legalName', 'businessName',
            'category_woven', 'city', 'country_code_mapping', 'country_code',  'country_name_fr', 
            'id_secondaire', 'entities_id', 'entities_name',  'entities_acronym', 'operateur_num', 'postalCode', 
            'street', 'webPage']], 
            how='left', on=['generalPic', 'country_code_mapping', 'country_code'])
            .merge(proj[['project_id', 'call_year']].drop_duplicates(), how='left', on=['project_id'])
            .drop(columns=['pic','_merge', 'nb_stage', 'nb', 'nb2'])
            .drop_duplicates()
            )

structure = structure.loc[~structure.entities_name.isnull()].drop_duplicates()
print(len(structure))

cols = ['department', 'entities_acronym', 'entities_name', 'legalName', 'businessName']
for i in cols:
    structure[f"{i}_dup"] = structure.loc[:,i]

if any(structure.call_year.isnull()):
    print(f"vérification de l'année (corriger les nuls si existants):\n{structure.call_year.value_counts(dropna=False)}")

##########
cols = ['department_dup', 'legalName_dup', 'businessName_dup', 'entities_acronym_dup','entities_name_dup','street','city']
structure = prep_str_col(structure, cols)

cedex="cedax|cedrex|cdexe|cdex|credex|cedex|cedx|cede|ceddex|cdx|cex|cexex|edex"
structure.loc[structure.postalCode.isnull(), 'postalCode'] = structure.city.str.extract(r"(\d+)")
structure['city'] = structure.city.str.replace(r"\d+", ' ', regex=True).str.strip()
structure.loc[structure.country_code=='FRA', 'city'] = structure.city.str.replace(cedex, ' ', regex=True).str.strip()
structure.loc[structure.country_code=='FRA', 'city'] = structure.city.str.replace(r"^france$", '', regex=True).str.strip()

##########
# creation entities_full = entities_name + entities_acronym et department_tag
tmp = structure.loc[(structure.legalName_dup!=structure.businessName_dup)&(~structure.businessName_dup.isnull()), ['generalPic',  'country_code', 'legalName_dup', 'businessName_dup']]
tmp['entities_full'] = [x1 if x2 in x1 else x1+' '+x2 for x1, x2 in zip(tmp['legalName_dup'], tmp['businessName_dup'])]

if len(structure.drop_duplicates())!=len(structure.merge(tmp[['generalPic', 'country_code', 'legalName_dup', 'businessName_dup', 'entities_full']].drop_duplicates(), how='left', on=['generalPic','businessName_dup', 'legalName_dup','country_code']).drop_duplicates()):
    print("Attention risque de doublon si merge de tmp et structure")
else:
    structure = structure.merge(tmp[['generalPic', 'country_code','legalName_dup', 'businessName_dup', 'entities_full']].drop_duplicates(), how='left', on=['generalPic','legalName_dup', 'businessName_dup', 'country_code']).drop_duplicates()
    structure.loc[structure.entities_full.isnull(), 'entities_full'] = structure.entities_name_dup.str.lower()

#############
societe = pd.read_table('data_files/societe.txt', header=None)
structure.loc[structure.entities_full.apply(lambda x: True if re.search(r"(?=\b("+'|'.join(list(set(societe[0])))+r")\b)", x) else False), 'org1'] = 'societe'
societe = societe.loc[societe[0]!='group']
structure.loc[(~structure.department_dup.isnull())&(structure.department_dup.apply(lambda x: True if re.search(r"(?=\b("+'|'.join(list(set(societe[0])))+r")\b)", str(x)) else False)), 'org1'] = 'societe'
structure.loc[structure.category_woven=='Entreprise', 'org1'] = 'societe'

las = r"(\bas(s?)ocia[ctz][aionj]+)|\b(ev|udruga|sdruzhenie|asbl|aisbl|vzw|biedriba|kyokai|mittetulundusuhing|ry|somateio|egyesulet(e?)|stowarzyszenie|udruzenje|zdruzenie|sdruzeni(e?))\b|([a-z]*)(verband|vereniging|asotsiatsiya|zdruzenje)\b|([a-z]*)(verein|forening|yhdistys)([a-z]*)"
structure.loc[structure.entities_full.apply(lambda x: True if re.search(las , x) else False), 'org2'] = 'association'
structure.loc[structure.category_woven=='Institutions sans but lucratif (ISBL)', 'org2'] = 'association'

structure['typ_from_lib'] = structure[['org1','org2']].stack().groupby(level=0).agg(' '.join)
structure.drop(columns=['org1','org2'], inplace=True)

# mots vide à suppr
stop_word(structure, 'country_code', ['entities_full', 'department_dup'])

structure['entities_full'] = structure['entities_full_2'].apply(lambda x: ' '.join([s for s in x if s.strip()]))
structure.loc[(~structure.department_dup.isnull()), 'department_dup'] = structure.loc[(~structure.department_dup.isnull()), 'department_dup_2'].apply(lambda x: ' '.join([s for s in x if s.strip()]))

structure.drop(columns=['department_dup_2', 'entities_full_2'], inplace=True)
structure.mask(structure=='', inplace=True)