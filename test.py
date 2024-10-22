import pandas as pd
from config_path import *
from functions_shared import work_csv

ent = pd.read_pickle(f"{PATH_CLEAN}entities_info_current2.pkl")
part = pd.read_pickle(f"{PATH_CLEAN}participation_current.pkl")
lien = pd.read_pickle(f"{PATH_CLEAN}lien.pkl")
# print(part.columns)

# ent=ent.loc[ent['entities_id']=='Y7ch7', ['generalPic',  'businessName']]

# part.loc[(part.stage=='successful')&(part.generalPic.isin(ent.generalPic.unique()))][['generalPic']].value_counts()
x=part.loc[(part.stage=='successful')&(part.generalPic.isin(ent.generalPic.unique()))]
# work_csv(x, 'entreprise_fr')

print(lien)

lien.loc[lien.project_id=='101120060']

ent.loc[ent['generalPic'].isin(['999957869', '953181171'])]