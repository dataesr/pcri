from config_api import sirene_headers, scanr_headers, paysage_headers
from config_path import PATH_SOURCE, PATH_WORK, PATH_REF
from api_requests.ror import *
from api_requests.sirene import *
from api_requests.paysage import *
from dotenv import load_dotenv
load_dotenv()

def ID_getRefInfo(lid_source):
    print("### ROR data")
    r=get_ror(lid_source, ror_old=None)
    ror=ror_cleaning(r)
    file_name = f"{PATH_REF}ror_df.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(ror, file)

    siren_siret = get_siret_siege(lid_source)
    paysage, paysage_category, paysage_mires = get_paysage(lid_source, siren_siret, paysage_old=None)
    sirene = get_sirene(lid_source, sirene_old=None)

    return ror, paysage, paysage_category, paysage_mires, sirene