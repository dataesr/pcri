import requests, json, os, urllib3, datetime
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
    
def sirene_get_headers(key, secret):
  
    token_file = 'tok/sirene_token.json'
    
    def get_new_token(key, secret):
        """Renew the sirene api token if it expires."""
        r = requests.post(
                 url='https://api.insee.fr/token',
                 data="grant_type=client_credentials",
                 auth=(
                    key,
                    secret
                 )
             )
        if r.status_code == 200:
            token = r.json()['access_token']              
            with open(token_file, 'w') as fp:
                data = {
                         "token": token,
                         "expire_date": (datetime.date.today() + datetime.timedelta(days=1)).isoformat()
                }
                json.dump(data, fp)
            return token
        else:
            error = (
                     f"Cannot get a valid token -- error from sirene api: {r.text}")
            print(error)

    def get_token(key, secret):
        """Get SIRENE API token or renew it if needed."""          
        with open(token_file, 'r') as fp:
            token = json.load(fp)
            last_token = token.get('token', None)
            expire_date = datetime.datetime.strptime(
                     token.get('expire_date', "1998-07-12"), "%Y-%m-%d")
            if last_token and (expire_date > datetime.datetime.today()):
                 return last_token
            return get_new_token(key, secret)
    
    

    api_call_headers = {'Authorization': 'Bearer ' + get_token(key, secret)}
    
    return api_call_headers