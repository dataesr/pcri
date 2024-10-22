import requests, json, os, urllib3
from config_url import token_url, revoke_url
from dotenv import load_dotenv
load_dotenv()

def get_token():
    token_url = token_url
    client_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    data = {'grant_type': 'client_credentials'}
    
    access_token_response = requests.post(token_url, data=data, auth=(client_id, client_secret))
    return json.loads(access_token_response.text)

def expire_token(token_lib):
    revoke_url = revoke_url
    
    data = {'client_id' : os.environ.get('client_id'),
            'client_secret' : os.environ.get('client_secret'),
            "token": token_lib}
    token_revoke = requests.post(revoke_url, data=data, verify=False, allow_redirects=False)

def get_headers():
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)
    tokens = get_token()
    try:
        if tokens['expires_in'] > 0:
            api_call_headers = {'Content-Type': "application.json",'Authorization': 'Bearer ' + tokens['access_token']}
            return api_call_headers
    except:
        expire_token(tokens['access_token'])
        tokens = get_token()
        api_call_headers = {'Content-Type': "application.json", 'Authorization': 'Bearer ' + tokens['access_token']}
        return api_call_headers