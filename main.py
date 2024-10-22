
import copy
from step1_mainData.data_load import *
from step1_mainData.projects_fix import *
from step1_mainData.proposals_fix import *
from step1_mainData.proj_merged_clean import *
from step1_mainData.url_fix import *
from step1_mainData.merged_nomenclatures import *

################################
## data load / adjustements
extractDate = date_load()
print(extractDate)
proj = proj_load()

proj_id_signed = proj.project_id.unique()
stage_l =  ['REJECTED' ,'NO_MONEY' ,'MAIN', 'RESERVE', 'INELIGIBLE', 'WITHDRAWN', 'INADMISSIBLE', None]

prop, prop1 = prop_load(proj_id_signed)
proj = proj_add_cols(prop1, proj)

###########################################
# proposals fix FIX
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

merged = copy.deepcopy(prop2)
merged = dates_year(merged)
merged = strings_v(merged)
merged = url_to_clean(merged)
merged.mask(merged=='', inplace=True)
merged = empty_str_to_none(merged)      
merged.rename(columns={
    'freekw':'free_keywords',
    'callDeadlineDate':'call_deadline', 
    'callId':'call_id', 
    'submissionDate':'submission_date',
    'startDate':'start_date',
    'endDate':'end_date', 
    'ecSignatureDate':'signature_date'}, inplace=True)

if any(merged.loc[merged.stage=='successful', 'project_id'].value_counts()[merged.loc[merged.stage=='successful', 'project_id'].value_counts()> 1]):
    print(merged.loc[merged.stage=='successful', 'project_id'].value_counts()[merged.loc[merged.stage=='successful', 'project_id'].value_counts()> 1])

merged = merged_panels(merged)