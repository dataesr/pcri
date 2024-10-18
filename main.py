
from step1_mainData.data_load import *
from step1_mainData.adjustments import *
from step1_mainData.prop_fix import *


extractDate = date_load()
print(extractDate)
proj = proj_load()

proj_id_signed = proj.project_id.unique()
stage_l =  ['REJECTED' ,'NO_MONEY' ,'MAIN', 'RESERVE', 'INELIGIBLE', 'WITHDRAWN', 'INADMISSIBLE', None]

prop, prop1 = prop_load(proj_id_signed)
proj = proj_add_cols(prop1, proj)

###########################################
# projects missing from proposals
integrate_call = poj_id_missing(prop1, proj)

# if call already in proposals then add missing projects
proj1 = proj_id_miss_fixed(prop1, proj, integrate_call)

# merge proj + prop
if len(proj1)==0:
    prop2=pd.concat([proj,prop1], ignore_index= True)
else:
    prop2 = pd.concat([prop1, proj1, proj], ignore_index = True)

prop2 = prop2.loc[~((prop2.status_code=='REJECTED')&(prop2.stage=='successful'))]
print(f"3 - merged prop2: {len(prop2)}, {prop2[['stage','status_code']].value_counts()}")