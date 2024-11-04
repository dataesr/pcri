from constant_vars import H20_ZIP
from config_path import PATH_SOURCE
from functions_shared import unzip_zip
import pandas as pd, pyreadr

def data_load():
    destination = pd.read_json(open("data_json/destination.json", 'r', encoding='utf-8'))
    thema = pd.read_json(open("data_json/thema.json", 'r', encoding='utf-8'))
    themes = pd.read_json(open("data_json/H20_themes_fr.json", 'r', encoding='utf-8'))
    act = pd.read_json(open("data_json/actions_name.json", 'r', encoding='utf-8'))
    # topics =  pd.read_json(open(f"{PATH}topics.json", 'r', encoding='utf-8'))
    topics = unzip_zip(H20_ZIP, f"{PATH_SOURCE}H2020/", 'topics.json')

    res = pyreadr.read_r(f"{PATH_SOURCE}H2020/H2020_data.RData")
    res.keys()
    _proj = res['projects']
    part = res['all']
    print(f"size part: {len(part)}")