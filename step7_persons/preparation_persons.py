from paths import PATH_CLEAN
from step7_persons.persons_load import *
from step7_persons.clean_and_add_info import *
import pandas as pd
CSV_PERSONS = "20260616"


#================
# load persons dataset and keep some columns
pa, pp = load_perso(CSV_PERSONS)
participation = pd.read_pickle(f"{PATH_CLEAN}participation_current.pkl")
entities = pd.read_pickle(f"{PATH_CLEAN}entities_info_current2.pkl")

#================
# clean last_name, first_name, country iso2 to iso3, domaine_mail
perso_part = clean_perso(pp, participation, entities, 'successful')
perso_app = clean_perso(pa, participation, 'evaluated')

#================
#


mask = perso_part['contact'].notna()
x=perso_part.loc[mask, ['generalPic', 'last_name', 'first_name', 'contact', 'domain_email']].drop_duplicates().sort_values('contact')