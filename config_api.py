import os

sirene_headers = {"X-INSEE-Api-Key-Integration": os.environ.get('SIRENE_API_KEY')}
scanr_headers = {"Accept":"application/json", 'Authorization': os.environ.get('SCANR_AUTH')}
scanr_185_headers = {'Authorization': os.environ.get('SCANR_AUTH')}
paysage_headers = {'Content-Type': 'application/json', 'X-Api-Key': os.environ.get('PAYSAGE_KEY')}
ods_headers = {"Authorization": f"apikey {os.environ.get('ODS_API')}"}
ror_headers = {"Client-Id": os.environ.get('ROR_API')}
grist_headers = {"accept": "application/json","Authorization": f"Bearer {os.environ.get('GRIST_KEY')}"}

openalex_usermail="zmenesr@gmail.com"