import pandas as pd, pickle
pd.options.mode.copy_on_write = True
from IPython.display import HTML

from matcher import matcher
from step7_persons.persons import persons_preparation
from step8_referentiels.referentiels import ref_externe_preparation
from step9_affiliations.prep_entities import entities_preparation
from step9_affiliations.affiliations import persons_affiliation
from step9_affiliations.organismes_cleaning import organismes_back
CSV_DATE='20250121'

### one time
# organismes_back('2024')
persons_preparation(CSV_DATE)

# ref_externe_preparation()

# entities_preparation()
 ### si reprise du code en cours chargement des pickles -> entities_all
# keep = pd.read_pickle(f'{PATH}participants/data_for_matching/structure_fr.pkl')
# struct_et = pd.read_pickle(f'{PATH}participants/data_for_matching/struct_et.pkl')

def data_import():
    from config_path import PATH,  PATH_CLEAN
    perso = pd.read_pickle(f"{PATH_CLEAN}perso_app.pkl")
    print(f"size perso init: {len(perso)}")
    ref_all = pd.read_pickle("C:/Users/zfriant/OneDrive/Matching/Echanges/HORIZON/data_py/ref_all.pkl")
    print(f"size ref_all init: {len(ref_all)}")
    entities_all = pd.read_pickle(f'{PATH}participants/data_for_matching/entities_all.pkl')
    print(f"size entities_all init: {len(entities_all)}")
    return perso, ref_all, entities_all
perso, ref_all, entities_all = data_import()

perso = perso[['project_id', 'generalPic', 'pic', 'role', 'first_name', 'last_name',
       'title', 'gender','researcher_id', 'orcid_id',
       'google_scholar_id', 'scopus_author_id', 'stage', 'nb', 'country_code',
       'call_year', 'thema_name_en', 'destination_name_en', 'tel_clean',
       'domaine_email', 'contact']].drop_duplicates()
print(f"size perso for merging: {len(perso)}")


### affil perso to struct -> search labo by openalex
entities_tmp = (
    entities_all.loc[((entities_all.country_code=='FRA')&(entities_all.rnsr_merged.str.len()==0)),
                    ['project_id','generalPic', 'pic', 'country_code', 'entities_id', 'entities_name']])
# entities_tmp=entities_all.loc[((entities_all.country_code=='FRA')&(entities_all.rnsr_merged.str.len()==0))|((entities_all.country_code!='FRA')&(entities_all.entities_id.str.contains('pic'))), ['project_id','generalPic', 'pic', 'country_code', 'entities_id', 'entities_name']]

pp = (perso[['project_id','generalPic', 'pic','contact', 'orcid_id']].drop_duplicates()
      .merge(entities_tmp, how='inner', on=['project_id','generalPic', 'pic']))
pp = pp.fillna('')

print(f"size pp: {len(pp)}, info sur pp with orcid: {len(pp.loc[pp.orcid_id!=''])}")
pp = pp.loc[pp.orcid_id!='', ['contact', 'orcid_id']].drop_duplicates().sort_values(['orcid_id', 'contact'])
persons_affiliation(pp)
# voire comment traiter le retour; pour l'instant ne peut plus utiliser l'api (trop de requetes)

#import result openalexApi ; next request import only "persons_author" and using variable MATCH (orcid, ame) to split data
with open(f"C:/Users/zfriant/OneDrive/PCRI/participants/data_for_matching/persons_author.pkl", 'rb') as f:
    author_orcid = pickle.load(f)
author_orcid=pd.json_normalize(author_orcid, record_path=['affiliations'], meta=['name','orcid', 'display_name', 'ids', 'match'])
author = (author_orcid
            .rename(columns={
                    'institution.id':'opa_inst_id', 
                    'institution.ror':'numero_ror',
                    'institution.display_name':'entities_name',
                    'institution.country_code':'country_code'})
            .drop(columns=['institution.type','institution.lineage']))

# author_orcid = pd.read_pickle(f"C:/Users/zfriant/OneDrive/PCRI/participants/data_for_matching/persons_author_orcid.pkl")
# author_name = pd.read_pickle(f"C:/Users/zfriant/OneDrive/PCRI/participants/data_for_matching/persons_author.pkl")


author_orcid = author_orcid.loc[author_orcid.affiliations.str.len()>0, ['name', 'orcid', 'affiliations', 'ids.orcid']]
author_orcid = author_orcid.explode('affiliations')
# author_orcid['nb'] = author_orcid.groupby(['name']).transform('size')

### affil perso to ref_all by phone
# aff_by_tel = perso.loc[~perso.tel_clean.isnull()].merge(ref_all.loc[~ref_all.tel_clean.isnull()], how='inner', on='tel_clean')
# print(f"size aff_by_tel: {len(aff_by_tel)}")

