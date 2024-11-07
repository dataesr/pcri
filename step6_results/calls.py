

def calls

    '''create table for indicators by call -> calls_current'''

    call_id = (projects_current
        .groupby(['programme_name_fr','thema_code', 'thema_name_fr', 'thema_name_en', 'destination_code','destination_name_en',  'call_deadline',
                'call_id','call_year','role','action_code', 'extra_joint_organization','country_code','country_name_fr','stage', 'status_code'],  dropna = False)
        .agg({'calculated_fund':'sum', 'beneficiary_subv':'sum', 'number_involved':'sum', 'project_id': 'nunique'})
        .reset_index()
        .merge(calls.drop(columns=['call_year','missionCancer', 'missionCities', 'missionClimate', 'missionOcean',
        'missionSoil']).drop_duplicates(), how='left', on=['call_id', 'call_deadline'])
            )

    call_id.to_csv(f"{PATH_CONNECT}calls_current.csv", index=False, encoding="UTF-8", sep=";", na_rep='')