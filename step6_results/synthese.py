import pandas as pd, numpy as np
from config_path import PATH_CONNECT
from functions_shared import cols_order, zipfile_ods


def synthese_preparation(participation, countries):
    print("\n### SYNTHESE preparation")
    print("\n## regroupement des participations")

    cc = (countries
          .drop(columns=['countryCode', 'countryCode_parent','country_code'])
          .drop(columns=countries.columns[countries.columns.str.contains('2020')])
          .rename(columns={'countryCode_iso3':'country_code'})
          .drop_duplicates()
          )

    part=(participation
        .drop(columns=['orderNumber', 'generalPic', 'country_code_mapping'])
        .assign(number_involved=1)
        .groupby(['stage', 'project_id', 'role','participates_as', 'erc_role', 'cordis_is_sme', 'cordis_type_entity_acro',
        'cordis_type_entity_code', 'cordis_type_entity_name_en', 'extra_joint_organization', 'is_ejo', 'with_coord',
        'cordis_type_entity_name_fr', 'country_code'],  dropna = False)
        .sum()
        .reset_index())

    part = part.merge(cc, how='left', on='country_code')

    print(f"nouvelle longueur pour les participations regroupées: {len(part)}")

    if '{:,.1f}'.format(participation['beneficiary_fund'].sum())=='{:,.1f}'.format(part['beneficiary_fund'].sum()):
        print("Etape participation/regroupement -> beneficiary_fund OK")
    else:
        print(f"ATTENTION ! Revoir le calcul de beneficiary_fund:{'{:,.1f}'.format(part['beneficiary_fund'].sum())}")

    if '{:,.1f}'.format(participation.loc[participation.stage=='successful', 'calculated_fund'].sum())=='{:,.1f}'.format(part.loc[part.stage=='successful', 'calculated_fund'].sum()):
        print("Etape participation/regroupement -> calculated_fund_successful OK")
    else:
        print(f"ATTENTION ! Revoir le calcul de calculated_fund_successful:{'{:,.1f}'.format(part.loc[part.stage=='successful', 'calculated_fund'].sum())}")
        
    if '{:,.1f}'.format(participation.loc[participation.stage=='evaluated', 'calculated_fund'].sum())=='{:,.1f}'.format(part.loc[part.stage=='evaluated', 'calculated_fund'].sum()):
        print("Etape participation/regroupement -> calculated_fund_eval OK")
    else:
        print(f"ATTENTION ! Revoir le calcul de calculated_fund_eval:{'{:,.1f}'.format(part.loc[part.stage=='evaluated', 'calculated_fund'].sum())}")
        
    if len(participation)==part['number_involved'].sum():
        print("Etape participation/regroupement -> participant_number OK")
    else:
        print(f"ATTENTION ! Revoir le calcul de calculated_participant_number:{'{:,.1f}'.format(part['number_involved'].sum())}") 
    return part


def projects_participations(projects, part):
    print("\n### PROJECTS + PARTICIPATION")
    # si besoin ne pas relancer tout le programme -> load json files : participation_current and projects_current

    projects_current=projects[['project_id', 'total_cost', 'start_date', 'end_date',  'call_deadline',
                'duration',  'call_id', 'call_year', 'stage', 'status_code', 'topic_code', 'topic_name',
                'pilier_name_en', 'pilier_name_fr', 'programme_code', 'programme_name_en', 'programme_name_fr', 
                'thema_code', 'thema_name_fr', 'thema_name_en', 
                'destination_code', 'destination_name_en', 'destination_lib',
                'destination_detail_code', 'destination_detail_name_en',
                'action_code', 'action_name', 'action_code2', 'action_name2', 'ecorda_date']].drop_duplicates()

    act_liste = ['RIA', 'MSCA', 'IA', 'CSA', 'ERC', 'EIC']
    projects_current = projects_current.assign(action_group_code=projects_current.action_code, action_group_name=projects_current.action_name)
    projects_current.loc[~projects_current.action_code.isin(act_liste), 'action_group_code'] = 'ACT-OTHERS'
    projects_current.loc[~projects_current.action_code.isin(act_liste), 'action_group_name'] = 'Others actions'

    projects_current = projects_current.merge(part, how='inner', on=['project_id', 'stage'])

    print(f"size: {len(projects_current)}, fund_signed: {'{:,.1f}'.format(projects_current.loc[(projects_current.stage=='successful')&(projects_current.country_code=='FRA')].calculated_fund.sum())}, participant_signed: {'{:,.1f}'.format(projects_current.loc[(projects_current.stage=='successful')&(projects_current.country_code=='FRA'),'number_involved'].sum())}")

    (pd.DataFrame(projects_current)
    .drop(columns=['topic_name','ecorda_date'])
    .to_csv(f"{PATH_CONNECT}projects_participations_current.csv", index=False, encoding="UTF-8", sep=";", na_rep=''))

    return projects_current

def synthese(projects_current):
    print("\n### SYNTHESE ODS")

    tmp=(projects_current.assign(stage_name=np.where(projects_current.stage=='evaluated', 'projets évalués', 'projets lauréats'))
        [['country_name_fr', 'stage_name', 'call_year', 'programme_name_fr', 'thema_code',  'thema_name_fr', 
        'destination_code', 'destination_name_en', 'action_code',  'action_name', 'action_code2', 'action_name2',  
        'action_group_code', 'action_group_name', 'extra_joint_organization', 'is_ejo',
        'cordis_type_entity_acro', 'cordis_type_entity_code', 'cordis_type_entity_name_en', 'cordis_type_entity_name_fr',
        'role', 'pilier_name_fr',  'calculated_fund', 'coordination_number', 'number_involved','project_id', 
        'country_group_association_name_fr','country_group_association_name_en', 
        'country_name_en', 'country_code', 'country_group_association_code', 'with_coord',
        'pilier_name_en','programme_name_en','thema_name_en','stage', 'status_code', 'ecorda_date'
        ]]
            .rename(columns={ 
            'action_code':'action_id', 
            'action_name':'action_name',
            'action_code2':'action_detail_id', 
            'action_name2':'action_detail_name',
            'calculated_fund':'fund_€',
            'country_group_association_code':'country_association_code',
            'country_group_association_name_en':'country_association_name_en',
            'country_group_association_name_fr':'country_association_name_fr',
            'with_coord':'flag_coordination',
            'is_ejo':'flag_organization'
            }))

    tmp.loc[tmp.thema_code.isin(['ERC','MSCA']), ['destination_code', 'destination_name_en']] = np.nan

    # attention si changement de nom de vars -> la modifier aussi dans pcri_info_columns_order
    tmp = cols_order(tmp, 'proj_synthese')

    print(f"{'{:,.1f}'.format(tmp.loc[tmp.stage=='successful','fund_€'].sum())}")
    zipfile_ods(tmp, 'fr-esr-all-projects-synthese')


def resume(projects_current):
    print("\n### RESUME")
    glob = projects_current[['project_id', 'call_id', 'country_code', 'call_deadline']].drop_duplicates()
    call_fr = pd.Series(glob.loc[glob.country_code=='FRA', 'call_id'].nunique(), index=['call_fr'])
    call_glob = pd.Series(glob.call_id.nunique(), index=['call_glob'])
    date_start = pd.Series(min(glob['call_deadline']), index=['date_start'])
    date_end = pd.Series(max(glob['call_deadline']), index=['date_end'])

    print(f"call fr:{call_fr}, call_g: {call_glob}, deb:{date_start}, fin:{date_end}")

    def stat_count(i,country=None):
        if country:        
            w=(projects_current[(projects_current['stage']==i) & (projects_current['country_code'].isin(country))]
                    .assign(is_ejo=True)
                    .groupby('is_ejo')
                    .agg({'project_id': 'nunique', 'number_involved':'sum', 'calculated_fund':'sum'})
                    .rename(columns={'project_id':f'pres_{i}_country', 'calculated_fund':f'subv_{i}_country', 'number_involved':f'part_{i}_country'})  
                    .reset_index())

            x=(projects_current[(projects_current['stage']==i) & (projects_current['country_code'].isin(country))&(projects_current.extra_joint_organization.isnull())]
                    .assign(is_ejo=False)
                    .groupby('is_ejo')
                    .agg({'project_id': 'nunique', 'number_involved':'sum', 'calculated_fund':'sum'})
                    .rename(columns={'project_id':f'pres_{i}_country', 'calculated_fund':f'subv_{i}_country', 'number_involved':f'part_{i}_country'})  
                    .reset_index())
            x=pd.concat([w,x], axis=0)
            
            y=(projects_current[(projects_current['stage']==i) & (projects_current['country_code'].isin(country))&(projects_current.with_coord==True)]
                    .assign(is_ejo=True)
                    .groupby('is_ejo')
                    .agg({'coordination_number':'sum'})
                    .rename(columns={'coordination_number': f'coord_{i}_country'})  
                    .reset_index())

            z=(projects_current[(projects_current['stage']==i) & (projects_current['country_code'].isin(country))&(projects_current.extra_joint_organization.isnull())&(projects_current.with_coord==True)]
                    .assign(is_ejo=False)
                    .groupby('is_ejo')
                    .agg({'coordination_number':'sum'})
                    .rename(columns={'coordination_number': f'coord_{i}_country'})  
                    .reset_index())

            y=pd.concat([y,z], axis=0)
            
            return(pd.merge(x,y, how='inner', on='is_ejo'))
        else:
            x=(projects_current[projects_current['stage']==i]
                .agg({'project_id': 'nunique', 'number_involved':'sum', 'calculated_fund':'sum'})
                .rename({'project_id':f'pres_{i}', 'calculated_fund':f'subv_{i}', 'number_involved':f'part_{i}'})
                )
            y=(projects_current[(projects_current['stage']==i)&(projects_current.with_coord==True)]
                .agg({'coordination_number':'sum'})
                .rename({'coordination_number': f'coord_{i}'})  )
            return(pd.concat([x,y]))
        
    sig=stat_count('successful')
    sig_country=stat_count('successful' ,['FRA'])
    el=stat_count('evaluated')
    el_country=stat_count('evaluated', ['FRA'])

    def calcul_part(tab, tab1, status):
        for indic in ['pres', 'subv', 'part', 'coord']:
            tab[f'%{indic}_{status}']=tab[f'{indic}_{status}_country']/tab1[f'{indic}_{status}']

    def calcul_succes(tab, tab1):       
        for indic in ['pres', 'subv', 'part', 'coord']:
            if 'country' in f'{tab}':
                tab[f'succes_{indic}_country']=tab1[f'{indic}_successful_country']/tab[f'{indic}_evaluated_country']
            else:
                tab[f'succes_{indic}']=tab1[f'{indic}_successful']/tab[f'{indic}_evaluated']
                
    calcul_part(el_country, el, 'evaluated')  
    calcul_part(sig_country, sig, 'successful')  
    calcul_succes(el_country, sig_country) 
    calcul_succes(el, sig) 

    resume_country=pd.merge(el_country, sig_country, how='inner', on='is_ejo')
    resume_country=resume_country.merge(pd.concat([el, sig, call_fr, call_glob, date_start, date_end]).to_frame().T, how='cross')
    resume_country['proj_under_prep']=projects_current[projects_current['status_code']=='UNDER_PREPARATION'][['project_id']].nunique().values[0]
    resume_country['proj_signed']=projects_current[projects_current['status_code'].isin(['SIGNED','CLOSED','TERMINATED','SUSPENDED'])][['project_id']].nunique().values[0]
    resume_country.to_csv(PATH_CONNECT+"resume_country.csv", index=False, encoding="UTF-8", sep=";", na_rep='')