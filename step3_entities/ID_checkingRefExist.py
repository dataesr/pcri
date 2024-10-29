# from token_api import sirene_get_headers
from config_api import sirene_headers, scanr_headers, paysage_headers
from constant_vars import FRAMEWORK
import requests, time, re
from dotenv import load_dotenv
load_dotenv()

def check_id(check_list: list):

    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
    time.sleep(0.5)
    # sirene_headers = sirene_get_headers(os.environ.get('SIRENE_API_KEY'), os.environ.get('SIRENE_API_SECRET'))
    # scanr_headers = {"Accept":"application/json", 'Authorization': os.environ.get('SCANR_AUTH')}
    # paysage_headers = {'Content-Type': 'application/json', 'X-Api-Key': os.environ.get('X-API-KEY')}  

    base = [
    {'pat':'^[0-9]{9}$','source':'siren',
        'url':'https://api.insee.fr/entreprises/sirene/siret?q=', 'h':'sirene_headers'},
    {'pat':'^[0-9]{14}$', 'source':'siret', 
     'url':'https://api.insee.fr/entreprises/sirene/siret?q=','h':'sirene_headers'},
    {'pat':'^[W|w]([A-Z0-9]{8})[0-9]{1}$', 'source':'identifiantAssociationUniteLegale', 
    'url':'https://api.insee.fr/entreprises/sirene/siret?q=','h':'sirene_headers'},
    {'pat':'^[0-9]{9}[A-Z]{1}$','source':'rnsr',
     'url':'https://scanr-preprod.coexya.eu/api/v2/structures/structure/','h':'scanr_headers'},
    {'pat':"^R0([a-zA-Z0-9]{6})[0-9]{2}$",'source':'ror',
     'url':'https://api.ror.org/organizations?query=','h':None},
    {'pat':"^grid.",'source':'grid',
     'url':'https://api.ror.org/organizations?query.advanced=external_ids.GRID.preferred:','h':None},
    {'pat':'^([a-zA-Z0-9]{5})$','source':'paysage',
     'url':'https://api.paysage.dataesr.ovh/structures/','h':'paysage_headers'}, 
    {'pat':'^[0-9]{7}[A-Z]{1}','source':'uai',
     'url':'https://api.paysage.dataesr.ovh/structures/','h':'paysage_headers'}, 
    {'pat':'^F[0-9]+','source':'finess',
     'url':'https://public.opendatasoft.com/api/v2/catalog/datasets/finess-etablissements/records?select=','h':None}]
    
    result = []
    base = sorted(base, key=lambda x: x['source'])
    counter=0
    for id in check_list:
        for i in base:
            if re.match(i['pat'], str(id)):
                keyw = i['source']

                if (keyw == 'paysage')|(keyw == 'uai'):
                    url2 = i['url'] + id 
                    head = paysage_headers
                elif keyw == 'rnsr':
                    url2 = i['url'] + id
                    head = scanr_headers
                elif (keyw == 'ror') | (keyw == 'finess') | (keyw == 'grid'):
                    url2 = i['url']  + id[1:]
                elif (keyw == 'siren') | (keyw == 'siret') | (keyw == 'identifiantAssociationUniteLegale'):
                    url2 = i['url']  + keyw + ':' + id
                    head = sirene_headers

                if i['h']:
                    rinit = requests.get(url2, headers=head, verify=False)
                elif  i['h'] is None:
                    rinit = requests.get(url2, verify=False)

                if rinit.status_code:
                    response = {'checked_id' : id, 'source': keyw, 'code': rinit.status_code}
                    result.append(response)
                    counter+=1
                    print(counter, id, keyw, rinit.status_code, sep=" , ")
                else:
                    response = {'checked_id' : id, 'source': keyw, 'code': None}
                    result.append(response)
                    counter+=1
                    print(counter, id, keyw, sep=" , ")

    return result