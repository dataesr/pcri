import pandas as pd
pd.options.mode.copy_on_write = True
from config_path import PATH_CLEAN
from step7_persons.prep_persons import persons_preparation
CSV_DATE='20250121'
from step7_persons.affiliations import persons_affiliation

# persons_preparation(CSV_DATE)

perso_part = pd.read_pickle(f"{PATH_CLEAN}persons_participants.pkl")
perso_app = pd.read_pickle(f"{PATH_CLEAN}persons_applicants.pkl")

pp = pd.concat([perso_part[['contact', 'orcid_id']].drop_duplicates(), perso_app[['contact', 'orcid_id']].drop_duplicates()], ignore_index=True)
pp = pp.fillna('')

print(f"size pp: {len(pp)}, info sur pp with orcid: {len(pp.loc[pp.orcid_id!=''])}")
pp = pp.sort_values(['orcid_id', 'contact'])
persons_affiliation(pp)
# # voire comment traiter le retour; pour l'instant ne peut plus utiliser l'api (trop de requetes)

# #import result openalexApi ; next request import only "persons_author" and using variable MATCH (orcid, ame) to split data
# with open(f"C:/Users/zfriant/OneDrive/PCRI/participants/data_for_matching/persons_author.pkl", 'rb') as f:
#     author_orcid = pickle.load(f)
# author_orcid=pd.json_normalize(author_orcid, record_path=['affiliations'], meta=['name','orcid', 'display_name', 'ids', 'match'])
# author = (author_orcid
#             .rename(columns={
#                     'institution.id':'opa_inst_id', 
#                     'institution.ror':'numero_ror',
#                     'institution.display_name':'entities_name',
#                     'institution.country_code':'country_code'})
#             .drop(columns=['institution.type','institution.lineage']))

# # author_orcid = pd.read_pickle(f"C:/Users/zfriant/OneDrive/PCRI/participants/data_for_matching/persons_author_orcid.pkl")
# # author_name = pd.read_pickle(f"C:/Users/zfriant/OneDrive/PCRI/participants/data_for_matching/persons_author.pkl")


# author_orcid = author_orcid.loc[author_orcid.affiliations.str.len()>0, ['name', 'orcid', 'affiliations', 'ids.orcid']]
# author_orcid = author_orcid.explode('affiliations')
# # author_orcid['nb'] = author_orcid.groupby(['name']).transform('size')

    # df.loc[df.orcid=='', 'orcid'] = df.orcid_tmp.str.split("/").str[-1]
    # r2 = r.groupby(['name', 'orcid'])['affiliations'].apply(pd.json_normalize)
    # r2 = df[['name', 'orcid', 'affiliations']]