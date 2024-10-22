from token_api import get_headers
from constant_vars import FRAMEWORK
import json, requests, time



def base_metadata(base=None, framework=None):
    url = "https://api.tech.ec.europa.eu/ecorda_api/v9/" + base + "?framework=" + framework
    r = requests.get(url, headers=get_headers())
    return r.json().get("metadata")

eit = base_metadata(base='proposals', framework=FRAMEWORK)
print(eit)
# token_url = "https://api.tech.ec.europa.eu/token"
# client_id = os.environ.get('client_id')
# client_secret = os.environ.get('client_secret')
# data = {'grant_type': 'client_credentials'}
# access_token_response = requests.post(token_url, data=data, auth=(client_id, client_secret))
# tokens = json.loads(access_token_response.text)

