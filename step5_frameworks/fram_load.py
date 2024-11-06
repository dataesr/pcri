from config_path import PATH_CLEAN
import pandas as pd

def framework_load():
    h20 = pd.read_pickle(f"{PATH_CLEAN}H2020_data.pkl")
    h20 = h20.reindex(sorted(h20.columns), axis=1)
    FP7 = pd.read_pickle(f"{PATH_CLEAN}FP7_data.pkl") 
    FP6 = pd.read_pickle(f"{PATH_CLEAN}FP6_data.pkl") 

    # chargement données autres pcri
    h20_p = pd.read_pickle(f"{PATH_CLEAN}H2020_successful_projects.pkl") 
    FP7_p = pd.read_pickle(f"{PATH_CLEAN}FP7_successful_projects.pkl") 
    FP6_p = pd.read_pickle(f"{PATH_CLEAN}FP6_successful_projects.pkl") 

    return h20, FP7, FP6, h20_p, FP7_p, FP6_p