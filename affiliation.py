import pandas as pd
from unidecode import unidecode
from functions_shared import *
from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE, PATH_CLEAN


participation = pd.read_pickle(f"{PATH_CLEAN}participation_current.pkl") 
entities_info = pd.read_pickle(f"{PATH_CLEAN}entities_info_current2.pkl")
# # entities = pd.read_pickle(f"{PATH_WORK}entities_participation_current.pkl")
proj = pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl")
nuts = pd.read_pickle("data_files/nuts_complet.pkl")

countries = pd.read_pickle(f"{PATH_CLEAN}country_current.pkl")
lien = pd.read_pickle(f"{PATH_CLEAN}lien.pkl")

pp_app = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'proposals_applicants_departments.json', 'utf8')
pp_app = pd.DataFrame(pp_app)
pp_app = pp_app.rename(columns={'proposalNbr':'project_id', 'applicantPic':'pic','departmentApplicantName':'department'}).astype(str)
print(len(pp_app))

pp_part = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'projects_participants_departments.json', 'utf8')
pp_part = pd.DataFrame(pp_part)
pp_part = pp_part.rename(columns={'projectNbr':'project_id', 'participantPic':'pic','departmentParticipantName':'department'}).astype(str)
print(len(pp_part))

########
def prep(stage, df):
    test = df.merge(countries[['countryCode', 'country_code_mapping','country_code']], how='left', on='countryCode')
    test = test.assign(stage=stage).drop(columns=['countryCode','orderNumber', 'departmentUniqueId','framework', 'lastUpdateDate' ]).drop_duplicates()
#     test['nb'] = test.groupby(['project_id', 'generalPic', 'pic'])['department'].transform('count')
    
    if stage=='evaluated':
        tmp=(lien.loc[lien.inProposal==True, ['project_id', 'generalPic', 'proposal_orderNumber','proposal_participant_pic', 'calculated_pic', 'nuts_applicants', 'n_app']]
            .rename(columns={'nuts_applicants':'entities_nuts', 'proposal_participant_pic':'pic', 'proposal_orderNumber':'orderNumber', 'n_app':'ent_nb'}))
        tmp=tmp.merge(test, how='inner', on=['project_id',  'generalPic',  'pic'])
    elif  stage=='successful':
        tmp=(lien.loc[lien.inProject==True, ['project_id', 'generalPic', 'orderNumber', 'participant_pic', 'calculated_pic', 'nuts_participant', 'n_part']]
           .rename(columns={'nuts_participant':'entities_nuts', 'participant_pic':'pic', 'n_part':'ent_nb'}))
        tmp=tmp.merge(test, how='inner', on=['project_id',  'generalPic',  'pic'])
    
    tmp.entities_nuts=tmp.apply(lambda x: ','.join(x.strip() for x in x.entities_nuts if x.strip()), axis=1)
    return tmp.sort_values('project_id').drop_duplicates()
    
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
             .merge(entities_info[['generalPic', 
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

cols = ['department', 'entities_acronym', 'entities_name']
for i in cols:
    structure[f"{i}_dup"] = structure.loc[:,i]

if any(structure.call_year.isnull()):
    print(f"vérification de l'année (corriger les nuls si existants):\n{structure.call_year.value_counts(dropna=False)}")


##########
def prep_str_col(df, cols):
    from functions_shared import tokenization

    df[cols] = df[cols].apply(lambda x: x.str.lower())
    
    ## caracteres speciaux
    for i in cols:
        df.loc[~df[i].isnull(), i] = df[i].astype('str').apply(unidecode)
        df.loc[~df[i].isnull(), i] = df[i].str.replace('&', 'and')
        df.loc[~df[i].isnull(), i] = df.loc[~df[i].isnull(), i].apply(lambda x: tokenization(x)).apply(lambda x: [s.replace('.','') for s in x]).apply(lambda x: ' '.join(x))
    
    punct="'|–|,|\\.|:|;|\\!|`|=|\\*|\\+|\\-|‑|\\^|_|~|\\[|\\]|\\{|\\}|\\(|\\)|<|>|@|#|\\$"
    
    # # #
    df[cols] = df[cols].apply(lambda x: x.str.replace(punct, ' ', regex=True))    
    df[cols] = df[cols].apply(lambda x: x.str.replace('\\n|\\t|\\r|\\xc2|\\xa9|\\s+', ' ', regex=True).str.strip())
    df[cols] = df[cols].apply(lambda x: x.str.lower().str.replace('n/a|ndeg', ' ', regex=True).str.strip())
    df[cols] = df[cols].apply(lambda x: x.str.lower().str.replace('/', ' ').str.strip())
    df[cols] = df[cols].apply(lambda x: x.str.lower().str.replace('\\', ' ').str.strip())
    df[cols] = df[cols].apply(lambda x: x.str.lower().str.replace('"', ' ').str.strip())
    df[cols] = df[cols].apply(lambda x: x.str.replace('\\s+', ' ', regex=True).str.strip())

    return df


##########
cols = ['department_dup','entities_acronym_dup','entities_name_dup','street','city']

structure = prep_str_col(structure, cols)

cedex="cedax|cedrex|cdexe|cdex|credex|cedex|cedx|cede|ceddex|cdx|cex|cexex|edex"
structure.loc[structure.postalCode.isnull(), 'postalCode'] = structure.city.str.extract('(\\d+)')
structure['city'] = structure.city.str.replace('\\d+', ' ', regex=True).str.strip()
structure.loc[structure.country_code=='FRA', 'city'] = structure.city.str.replace(cedex, ' ', regex=True).str.strip()
structure.loc[structure.country_code=='FRA', 'city'] = structure.city.str.replace('^france$', '', regex=True).str.strip()

##########
# creation entities_full = entities_name + entities_acronym et department_tag
tmp = structure.loc[(structure.entities_name_source_dup!=structure.entities_acronym_source_dup)&(~structure.entities_acronym_source_dup.isnull()), ['generalPic',  'country_code', 'entities_name_source_dup', 'entities_acronym_source_dup']]
tmp['entities_full'] = [x1 if x2 in x1 else x1+' '+x2 for x1, x2 in zip(tmp['entities_name_source_dup'], tmp['entities_acronym_source_dup'])]

if len(structure.drop_duplicates())!=len(structure.merge(tmp[['generalPic', 'country_code', 'entities_name_source_dup', 'entities_acronym_source_dup', 'entities_full']].drop_duplicates(), how='left', on=['generalPic','entities_name_source_dup', 'entities_acronym_source_dup','country_code']).drop_duplicates()):
    print("Attention risque de doublon si merge de tmp et structure")
else:
    structure = structure.merge(tmp[['generalPic', 'country_code','entities_name_source_dup', 'entities_acronym_source_dup', 'entities_full']].drop_duplicates(), how='left', on=['generalPic','entities_name_source_dup', 'entities_acronym_source_dup', 'country_code']).drop_duplicates()
    structure.loc[structure.entities_full.isnull(), 'entities_full'] = structure.entities_name_dup.str.lower()

#############
societe = pd.read_table('data_files/societe.txt', header=None)
structure.loc[structure.entities_full.apply(lambda x: True if re.search(r"(?=\\b("+'|'.join(list(set(societe[0])))+r")\\b)", x) else False), 'org1'] = 'societe'
societe = societe.loc[societe[0]!='group']
structure.loc[(~structure.department_dup.isnull())&(structure.department_dup.apply(lambda x: True if re.search(r"(?=\\b("+'|'.join(list(set(societe[0])))+r")\\b)", str(x)) else False)), 'org1'] = 'societe'
structure.loc[structure.category_woven=='Entreprise', 'org1'] = 'societe'

las = r"(\\bas(s?)ocia[ctz][aionj]+)|\\b(ev|udruga|sdruzhenie|asbl|aisbl|vzw|biedriba|kyokai|mittetulundusuhing|ry|somateio|egyesulet(e?)|stowarzyszenie|udruzenje|zdruzenie|sdruzeni(e?))\\b|([a-z]*)(verband|vereniging|asotsiatsiya|zdruzenje)\\b|([a-z]*)(verein|forening|yhdistys)([a-z]*)"
structure.loc[structure.entities_full.apply(lambda x: True if re.search(las , x) else False), 'org2'] = 'association'
structure.loc[structure.category_woven=='Institutions sans but lucratif (ISBL)', 'org2'] = 'association'

structure['typ_from_lib'] = structure[['org1','org2']].stack().groupby(level=0).agg(' '.join)
structure.drop(columns=['org1','org2'], inplace=True)