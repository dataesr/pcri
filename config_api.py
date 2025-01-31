from token_api import sirene_get_headers
import os

sirene_headers = sirene_get_headers(os.environ.get('SIRENE_API_KEY'), os.environ.get('SIRENE_API_SECRET'))
scanr_headers = {"Accept":"application/json", 'Authorization': os.environ.get('SCANR_AUTH')}
paysage_headers = {'Content-Type': 'application/json', 'X-Api-Key': os.environ.get('X-API-KEY')}
ods_headers = {"Authorization": f"apikey {os.environ.get('ODS_API')}"}

openalex_usermail='zmenesr@gmail.com'