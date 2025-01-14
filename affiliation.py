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
def tokenization(text):
    if isinstance(text, str):
        tokens = text.split()
        return tokens
    
def prep_str_col(df, cols):
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
cols = ['department_dup', 'legalName_dup', 'businessName_dup', 'entities_acronym_dup','entities_name_dup','street','city']

structure = prep_str_col(structure, cols)

cedex="cedax|cedrex|cdexe|cdex|credex|cedex|cedx|cede|ceddex|cdx|cex|cexex|edex"
structure.loc[structure.postalCode.isnull(), 'postalCode'] = structure.city.str.extract('(\\d+)')
structure['city'] = structure.city.str.replace('\\d+', ' ', regex=True).str.strip()
structure.loc[structure.country_code=='FRA', 'city'] = structure.city.str.replace(cedex, ' ', regex=True).str.strip()
structure.loc[structure.country_code=='FRA', 'city'] = structure.city.str.replace('^france$', '', regex=True).str.strip()

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
structure.loc[structure.entities_full.apply(lambda x: True if re.search(r"(?=\\b("+'|'.join(list(set(societe[0])))+r")\\b)", x) else False), 'org1'] = 'societe'
societe = societe.loc[societe[0]!='group']
structure.loc[(~structure.department_dup.isnull())&(structure.department_dup.apply(lambda x: True if re.search(r"(?=\\b("+'|'.join(list(set(societe[0])))+r")\\b)", str(x)) else False)), 'org1'] = 'societe'
structure.loc[structure.category_woven=='Entreprise', 'org1'] = 'societe'

las = r"(\\bas(s?)ocia[ctz][aionj]+)|\\b(ev|udruga|sdruzhenie|asbl|aisbl|vzw|biedriba|kyokai|mittetulundusuhing|ry|somateio|egyesulet(e?)|stowarzyszenie|udruzenje|zdruzenie|sdruzeni(e?))\\b|([a-z]*)(verband|vereniging|asotsiatsiya|zdruzenje)\\b|([a-z]*)(verein|forening|yhdistys)([a-z]*)"
structure.loc[structure.entities_full.apply(lambda x: True if re.search(las , x) else False), 'org2'] = 'association'
structure.loc[structure.category_woven=='Institutions sans but lucratif (ISBL)', 'org2'] = 'association'

structure['typ_from_lib'] = structure[['org1','org2']].stack().groupby(level=0).agg(' '.join)
structure.drop(columns=['org1','org2'], inplace=True)

# mots vide à suppr

def stop_word(df, cc_iso3 ,cols_list):
    import re, pandas as pd
    
    stop_word=pd.read_json('data_files/stop_word.json')

    for col_ref in cols_list:
        df[f'{col_ref}_2'] = df[col_ref].apply(lambda x: tokenization(x))

        for i, row in stop_word.iterrows():
            if row['iso3']=='ALL':
                w = "\\b"+row['word'].strip()+"\\b"
                df.loc[~df[f'{col_ref}_2'].isnull(), f'{col_ref}_2'] = df.loc[~df[f'{col_ref}_2'].isnull(), f'{col_ref}_2'].apply(lambda x: [re.sub(w, '',  s) for s in x]).apply(lambda x: list(filter(None, x)))
            else:
                mask = df[cc_iso3]==row['iso3']
                w = "\\b"+row['word'].strip()+"\\b"
                df.loc[mask&(~df[f'{col_ref}_2'].isnull()), f'{col_ref}_2'] = df.loc[mask&(~df[f'{col_ref}_2'].isnull()), f'{col_ref}_2'].apply(lambda x: [re.sub(w, '',  s) for s in x]).apply(lambda x: list(filter(None, x)))


stop_word(structure, 'country_code', ['entities_full', 'department_dup'])

structure['entities_full'] = structure['entities_full_2'].apply(lambda x: ' '.join([s for s in x if s.strip()]))
structure.loc[(~structure.department_dup.isnull()), 'department_dup'] = structure.loc[(~structure.department_dup.isnull()), 'department_dup_2'].apply(lambda x: ' '.join([s for s in x if s.strip()]))

structure.drop(columns=['department_dup_2', 'entities_full_2'], inplace=True)
structure.mask(structure=='', inplace=True)

##################################
#########################
#################
### FRANCE


structure_fr = structure.loc[structure.country_code=='FRA']
print(len(structure_fr))


#############
lpattern = ["cnrs", "inria", "inrae", "ifremer", "inserm", "cea", "ens", "fnsp", "cirad", "ird", "chu", "universite", 
            "pasteur", "curie", "irsn", "onera", "agrocampus", "ed","ecole"]
pattern_ifremer = "(ifremer)|(in.* fran.* re.* ex.* mer)"
pattern_cnrs =   "(ce.* na.* (de )?(la )?re.* sc.[a-z]*)|(fr.* na.* sc.* re.* ce.[a-z]*)|(cnrs)"
pattern_inria =  "(in.* na.* (de )?re.* (en )?in.* (et )?(en )?au.[a-z]*)|(inria)"
pattern_inrae =   "(in.* na.* (de )?re.* ag.[a-z]*)|(inra)|(inrae)|(irstea)"
pattern_inserm = "(in.* na.* (de )?(la )?sa.* (et )?(de )?(la )?re.* me.[a-z]*)|(inserm)"
pattern_cea =    "(co.* (a )?l?\\'?en.* at.[a-z]*)|(\\bcea\\b)"
pattern_ens =    "(ec.* no.* sup[a-z]*)|(\\bens\\b)"
pattern_fnsp =   "(fo.* na.* (des )?sc.* po.[a-z]*)|(fnsp)|(sciences po)"
pattern_cirad =  "(ce.* (de )?co.* in.* (en )?re.* ag.* (pour )?(le )?dev.[a-z]*)|(cirad)"
pattern_ird =    "(in.* (de )?re.[a-z]* (pour )?(le )?dev.[a-z]*)|\\b(ird)\\b|(i r d)"
pattern_chu = "((ce.*|ctre|group.*) hos.* (univ.[a-z]*)?)|(univ.* hosp.[a-z]*)|\\b(chu|chr|chru)\\b|(hospice)"
pattern_universite =   "(univ(ersite|ersity|ersitaire))"
pattern_pasteur =   "(ins([a-z]*|\\.*) pasteur( de)?( lille)?)|(pasteur inst([a-z]*))"
pattern_curie =    "(inst([a-z]*|\\.*) curie)|(curie inst([a-z]*))"
pattern_irsn =   "(in.* (de )?radio.[a-z]* (et )?(de )?sur.[a-z]* nuc.[a-z]*)|(irsn)"
pattern_onera =  "(onera)|(off.* na.* (d )?etu.* (et )?(de )?rech.* aero.*)"
pattern_agrocampus = "(agrocampus)"
pattern_ed = "(doct.* sch.*)|(ec.* doct.*)|\\b(ed)\\b"
pattern_ecole = "(ecole)"

def qualif_organisation(x):
    org = []
    for j in lpattern:
        pattern = globals()[f"pattern_{j}"]
        y = re.search(pattern, x)
        if y:
            org.append(j)
    return org

structure_fr['org1'] = structure_fr.apply(lambda x: qualif_organisation(x['department_dup']) if isinstance(x['department_dup'], str) else [], axis=1)
structure_fr['org2'] = structure_fr.apply(lambda x: qualif_organisation(x['entities_full']) if isinstance(x['entities_full'], str) else [], axis=1)
structure_fr['org3'] = structure_fr.apply(lambda x: qualif_organisation(x['entities_name_dup']) if isinstance(x['entities_name_dup'], str) else [], axis=1)
 

structure_fr['org_from_lib'] = structure_fr.apply(lambda x: sorted(set(x['org1'] + x['org2'] + x['org3'])), axis=1)
# structure_fr['org_from_lib'] = structure_fr['org_from_lib'].apply(lambda x: ' '.join(x))

structure_fr.drop(columns=['org1', 'org2', 'org3'], inplace=True)
structure_fr.mask(structure_fr=='', inplace=True)


structure_fr=structure_fr.assign(dep_tag=structure_fr.department_dup, lab_tag=structure_fr.entities_full)
cols = ['dep_tag', 'lab_tag']
structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace('international research lab', "irl", regex=False))
structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace('joint research unit', "jru", regex=False))
structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace('joint research unit', "jru", regex=False))
structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace('equipe accueil', "ea", regex=False))
structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace(r"\\bumr(\\s?s\\s?)(u(\\s?)|inserm(\\s?))?(?=(\\d+)?)|\\bu\\s?inserm(\\s?)|\\bunit(e?)(?=(\\s?u?\\s?\\d+))|\\binserm\\s?(umr\\s?(s?)|jru)\\s?(u?)|\\binserm(u?)\\s?(?=\\d+)|\\binserm\\s?un\\s?umr\\s?u?", "u", regex=True))
for s in ['umr','upr','uar','irl','emr','umi','usr','fre','gdr','fr']:
    structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace(r'(?<=\\b'+s+')\\s?[a-z]+\\s?(?=\\d+)', " ", regex=True))
structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace(r"\\bu\\s?cnrs|\\bum\\s+r|\\bcnrs\\s?(?=\\d+)|\\bjru\\s?(cnrs|umr)", "umr", regex=True))
structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace(r"\\bjru\\s?(umi)", "umi", regex=True))
structure_fr[cols] = structure_fr[cols].apply(lambda x: x.str.replace(r"(\\bce[a-z]* inv[a-z]* cl[a-z]*)|(\\bcl[a-z]* inv[a-z]* ce[a-z]*)|(\\bce[]* cl[a-z]* inv[a-z]*)", "cic", regex=True))
structure_fr.loc[structure_fr.org_from_lib.map(lambda x: "inserm" in x), cols] = structure_fr.loc[structure_fr.org_from_lib.map(lambda x: "inserm" in x), cols].apply(lambda x: x.str.replace(r'\\bjru\\b', 'u', regex=True))
structure_fr.loc[structure_fr.org_from_lib.map(lambda x: "cnrs" in x), cols] = structure_fr.loc[structure_fr.org_from_lib.map(lambda x: "cnrs" in x), cols].apply(lambda x: x.str.replace(r'\\bjru\\b', 'umr', regex=True))

llab = ["umr", "ua", "umrs", "umr s","ea", "u", "gdr", "fre", "fr", "frc", "fed", "je", "us", "ums",
        "upr","upesa","ifr","umr a","umemi","epi","eac", "ertint", "ur", "ups", "umr m", "umr t",
        "uar","ert","usr","ura","umr d","rtra","ue","ers","cic","ep","umi", "unit", 'emr', 'irl', 'jru']

def labo_sigle(x):
    sig = []
    for i in llab:
        pattern = r"\b("+i+r")(?=\b|\d+)\s?[a-z]*\s?(\d+)"
        # pattern=r"\b("+i+r")((\s?\d+)|\s[a-z]*\s(\d+))"
        y = re.search(pattern, x)
        if y:
            sig.append(''.join(y.groups()))
    return sig

structure_fr['org1'] = structure_fr.apply(lambda x: labo_sigle(x['dep_tag']) if isinstance(x['dep_tag'], str) else [], axis=1)
structure_fr['org2'] = structure_fr.apply(lambda x: labo_sigle(x['lab_tag']) if isinstance(x['lab_tag'], str) else [], axis=1)
structure_fr['lab_from_lib'] = structure_fr.apply(lambda x: list(set(x['org1'] + x['org2'])), axis=1)
# structure_fr['lab_from_lib'] = structure_fr['lab_from_lib'].apply(lambda x: ';'.join(x))
structure_fr.drop(columns=['org1', 'org2', 'dep_tag', 'lab_tag'], inplace=True)
structure_fr.mask(structure_fr=='', inplace=True)
