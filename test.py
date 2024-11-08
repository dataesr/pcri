
from config_path import PATH_SOURCE, PATH_WORK
import time, pandas as pd, json


x=pd.read_json(open(f"{PATH_WORK}FP7_calls.json", 'r+', encoding='utf-8'))
response=x['callData']['Calls']

call=[]
for i in response:
#     print(i)
    x = {'call_id':i.get('CallIdentifier').get('CallId'), 'call_budget':i.get('TotalIndicativeBudget').strip(',')}
    call.append(x)


with open("data_files/FP7_calls.json", "w") as final:
    json.dump(call, final)
