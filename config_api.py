from token_api import sirene_get_headers
import os

sirene_headers = sirene_get_headers(os.environ.get('SIRENE_API_KEY'), os.environ.get('SIRENE_API_SECRET'))
scanr_headers = {"Accept":"application/json", 'Authorization': os.environ.get('SCANR_AUTH')}
paysage_headers = {'Content-Type': 'application/json', 'X-Api-Key': os.environ.get('X-API-KEY')}

ODS_API = '78b48c428813ef14f2fe631489455bada8df5a7f8ee1bc3189ebc3f8'