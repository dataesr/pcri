import pandas as pd
from functions_shared import unzip_zip
from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE, PATH_CONNECT

def date_load():
    # creation de extractDate avec la date d'extraction d'ecorda format -> '2022-12-11'
    date = unzip_zip(ZIPNAME, f"{PATH_SOURCE}{FRAMEWORK}/", 'extractionDate.json', 'utf8')
    extractDate = list(set([i['extraction_date'] for i in date if i['framework']==FRAMEWORK]))[0]
    print([i for i in date if i['framework']==FRAMEWORK]) 
    pd.DataFrame([i for i in date if i['framework']==FRAMEWORK]).to_json(f"{PATH_CONNECT}extractionDate.json", orient='records')
    return extractDate