
import copy
from step1_mainData.data_load import *
from step1_mainData.projects_fix import *
from step1_mainData.proposals_fix import *
from step1_mainData.proj_merged_clean import *
from step1_mainData.url_fix import *
from step1_mainData.panels import *
from step1_mainData.topics import *
from step1_mainData.actions import *
from step1_mainData.calls import *
from step2_participations.participant import *

################################
## data load / adjustements
extractDate = date_load()

proj = proj_load()
proj_id_signed = proj.project_id.unique()
stage_l =  ['REJECTED' ,'NO_MONEY' ,'MAIN', 'RESERVE', 'INELIGIBLE', 'WITHDRAWN', 'INADMISSIBLE', None]

prop, prop1 = prop_load(proj_id_signed)
proj = proj_add_cols(prop1, proj)

###########################################
# proposals fix
# projects missing from proposals
call_to_integrate = proposals_id_missing(prop1, proj, extractDate)

# if call already in proposals then add missing projects
proj1 = proj_id_miss_fixed(prop1, proj, call_to_integrate)


# merge proj + prop
print('### MERGED PROPOSLS/PROJECTS')
if len(proj1)==0:
    prop2=pd.concat([proj,prop1], ignore_index= True)
else:
    prop2 = pd.concat([prop1, proj1, proj], ignore_index = True)

prop2 = prop2.loc[~((prop2.status_code=='REJECTED')&(prop2.stage=='successful'))]
print(f"result - merged all: {len(prop2)}, {prop2[['stage','status_code']].value_counts()}\n")

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
merged = merged_topics(merged)
merged = merged_actions(merged)

# calls list
calls = call(PATH_SOURCE+FRAMEWORK+'/')

print("### CALLS+MERGED")
if len(merged.loc[merged.call_id.isnull()])>0:
        print(f"1 - ATTENTION : manque des call_id: {merged.loc[merged.call_id.isnull(), 'project_id']}")
else:
    call_id = merged[['call_id', 'call_deadline']].drop_duplicates()
    print(f"2 - CALL_ID de merged -> nb call+deadline: {len(call_id)}, nb call unique: {call_id.call_id.nunique()} ")

calls = calls_to_check(calls, call_id)

projects = projects_complete_cleaned(merged, extractDate)

##### PARTICIPANTS

part1 = participant_load(projects)