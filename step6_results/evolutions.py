import pandas as pd, numpy as np, json
from config_path import PATH_CONNECT
from functions_shared import zipfile_ods

def evol_preparation(FP6, FP7, h20, projects_current):
    print("### preparation EVOL")
    rFP6=(FP6
        .loc[FP6.pilier_name_en!='Euratom']
        .assign(is_ejo='Avec')
        .groupby(['framework', 'stage', 'project_id', 'call_year', 'with_coord', 'country_code', 'country_group_association_code','is_ejo'])
        .agg({'coordination_number':'sum', 'number_involved':'sum', 'calculated_fund':'sum'})
        .reset_index()
        .rename(columns={'calculated_fund':'funding'})
        )

    rFP7=(FP7
        .loc[FP7.pilier_name_en!='Euratom']
        .groupby(['framework', 'stage','project_id', 'call_year', 'with_coord', 'country_code', 'country_group_association_code', 'is_ejo'], dropna=False)
        .agg({'coordination_number':'sum', 'number_involved':'sum', 'calculated_fund':'sum'})
        .reset_index()
        .rename(columns={'calculated_fund':'funding'})
        )

    rh20=(h20
        .loc[h20.pilier_name_en!='Euratom']
        .groupby(['framework', 'stage','project_id', 'call_year', 'with_coord', 'country_code', 'country_group_association_code', 'is_ejo'], dropna=False)
        .agg({'coordination_number':'sum', 'number_involved':'sum', 'calculated_fund':'sum'})
        .reset_index()
        .rename(columns={'calculated_fund':'funding'})
        )

    print(f"subv H20 FR : {'{:,.1f}'.format(rh20[(rh20['country_code']=='FRA') & (rh20['stage']=='successful')]['funding'].sum())}")

    _temp=(projects_current
        .groupby(['stage','project_id', 'call_year', 'with_coord', 'country_code', 'country_group_association_code', 'is_ejo'], dropna=False)
        .agg({'coordination_number':'sum', 'number_involved':'sum', 'calculated_fund':'sum'})
        .reset_index()
        .rename(columns={'calculated_fund':'funding'})
        .assign(framework='Horizon Europe')
        )

    print(f"subv HE FR : {'{:,.1f}'.format(_temp[(_temp['country_code']=='FRA') & (_temp['stage']=='successful')]['funding'].sum())}")

    pc = pd.concat([_temp, rh20, rFP6, rFP7], ignore_index=True)
    return pc

def evolution_FP(pc, countries):
    print("### evolution TAB")

    cc=(countries[['countryCode_iso3', 'country_name_fr', 'country_group_association_code']]
        .rename(columns={'countryCode_iso3':'country_code', 'country_group_association_code':'asso'}).drop_duplicates())
    
    total=(pc
            .groupby(['framework', 'call_year', 'stage', 'with_coord'], dropna=False)
            .agg({'funding':'sum', 'project_id': 'nunique', 'coordination_number':'sum', 'number_involved':'sum'})
            .reset_index()
            .rename(columns={'project_id':'project_number'})
            .assign(country_code='ALL', rank_evaluated=99, rank_successful=99)
            )

    _pc_ue=(pc
            .loc[(pc.is_ejo=='Sans')&(pc.country_group_association_code=='MEMBER-ASSOCIATED')]
            .groupby(['framework', 'call_year', 'stage', 'project_id', 'is_ejo', 'with_coord'], dropna=False)
            .agg({'number_involved':'sum',  'coordination_number':'sum', 'funding':'sum'})
            .reset_index()
            .assign(country_code='UE', rank_evaluated=99, rank_successful=99)
            )

    _pc1_ue=(pc.loc[(pc.country_group_association_code=='MEMBER-ASSOCIATED')]
            .groupby(['framework', 'call_year', 'stage', 'project_id', 'with_coord'], dropna=False)
            .agg({'number_involved':'sum', 'coordination_number':'sum', 'funding':'sum'})
            .reset_index()
            .assign(country_code='UE', rank_evaluated=99, rank_successful=99, is_ejo='Avec')
            )

    _pc=(pc
            .loc[pc.is_ejo=='Sans']
            .groupby(['framework', 'call_year', 'stage', 'country_code', 'project_id', 'is_ejo', 'with_coord'], dropna=False)
            .agg({'number_involved':'sum', 'coordination_number':'sum', 'funding':'sum'})
            .reset_index()
            )

    _pc1=(pc.groupby(['framework', 'call_year', 'stage', 'country_code', 'project_id', 'with_coord'], dropna=False)
            .agg({'number_involved':'sum', 'coordination_number':'sum', 'funding':'sum'})
            .reset_index()
            .assign(is_ejo='Avec')
            )
    _pc =pd.concat([_pc, _pc1], ignore_index=True)

    country=[]
    # rang des pays par framework
    for i in ['successful', 'evaluated']:
        temp = (_pc.loc[(_pc['stage']==i )]
            .groupby(['framework', 'country_code', 'is_ejo'], dropna=False)
            .agg({'funding':'sum'})
            .sort_values(['framework','funding'], ascending=False)
            .reset_index()
        )

        temp[f'rank_{i}'] = temp.groupby(['framework', 'is_ejo'])['funding'].rank('average', ascending=False).astype(int)
        temp = temp.drop(columns='funding')
        
        _pc=_pc.merge(temp, how='left', on=['framework', 'country_code',  'is_ejo'])
        country.extend(list(_pc[_pc[f'rank_{i}']<11]['country_code'].unique()))


    for extract in ['member', '_topten']:
        if extract=='member':
            tmp=_pc.loc[_pc.is_ejo=='Avec'].merge(cc[cc.asso=='MEMBER-ASSOCIATED'], how='inner', on='country_code')
            tmp = pd.concat([tmp, total, _pc1_ue], ignore_index=True)
            tmp.loc[tmp.country_code=='UE', 'country_name_fr'] = 'Etats membres & associés'
            tmp.loc[tmp.country_code=='ALL', 'country_name_fr'] = 'Tous pays'

            tmp=(tmp.groupby(['call_year', 'framework','country_name_fr', 'country_code', 'stage'], dropna=False)
            .agg({'project_id':'nunique', 'number_involved':'sum', 'coordination_number':'sum', 'funding':'sum', 'project_number':'sum'})
            .reset_index())
            tmp.loc[tmp.project_id>0, 'project_number'] = tmp.loc[tmp.project_id>0].project_id


            all_data = tmp[tmp['country_code'] == 'ALL']

            # Créer un dictionnaire pour stocker les valeurs pour "ALL"
            all_values = {
                'number_involved': dict(zip(all_data['call_year'], all_data['number_involved'])),
                'coordination_number': dict(zip(all_data['call_year'], all_data['coordination_number'])),
                'funding': dict(zip(all_data['call_year'], all_data['funding'])),
                'project_number': dict(zip(all_data['call_year'], all_data['project_number']))
            }

            # Calculer la part pour chaque colonne
            for column in ['number_involved', 'coordination_number', 'funding', 'project_number']:
                tmp[f'share_{column}'] = tmp.apply(
                    lambda row: row[column] / all_values[column][row['call_year']], axis=1
                )

            zipfile_ods(tmp.drop(columns='project_id').sort_values(['funding'], ascending=False), "fr-esr-countries-evolution-pcri")

        if extract=='_topten':

            _pc = _pc.merge(cc.drop(columns='asso'), how='left', on='country_code')
            _pc.loc[_pc.country_code=='UE', 'country_name_fr'] = 'Etats membres & associés'

            _pc = pd.concat([_pc, total, _pc_ue, _pc1_ue], ignore_index=True)

            country = list(set(country))
            _pc = _pc[_pc['country_code'].isin(country)]

            # creation de plusieurs niveaux de periode
            _pc['mixte_periode_H']=np.where(_pc['framework'].isin(['FP6', 'FP7']), _pc['framework'], _pc['call_year'])
            _pc['mixte_periode_HE']=np.where(_pc['framework'].isin(['FP6', 'FP7', 'Horizon 2020']), _pc['framework'], _pc['call_year'])

            _pc.to_csv(PATH_CONNECT+"all_FW_resume.csv", index=False, encoding="UTF-8", sep=";", na_rep='', decimal=".")


def evolution_type(FP6, FP7, h20, projects_current):
    _FP6_type=(FP6
            .loc[FP6.pilier_name_en!='Euratom']
        .groupby(['framework','project_id', 'call_year', 'country_code', 'country_name_fr', 'stage', 'with_coord',
                    'action_code','cordis_type_entity_code'], dropna=False)
        .agg({'coordination_number':'sum', 'number_involved':'sum', 'calculated_fund':'sum'})
        .reset_index()
        .rename(columns={'calculated_fund':'funding'})
        .assign(framework='FP6')
        )

    print(f"subv FP6 FR : {'{:,.1f}'.format(_FP6_type[(_FP6_type['country_code']=='FRA') & (_FP6_type['stage']=='successful')]['funding'].sum())}")

    _FP7_type=(FP7
            .loc[FP7.pilier_name_en!='Euratom']
        .groupby(['framework','project_id', 'call_year', 'country_code', 'country_name_fr', 'stage', 'with_coord',
                    'action_code','cordis_type_entity_code', 'is_ejo'], dropna=False)
        .agg({'coordination_number':'sum', 'number_involved':'sum', 'calculated_fund':'sum'})
        .reset_index()
        .rename(columns={'calculated_fund':'funding'})
        .assign(framework='FP7')
        )

    print(f"subv FP7 FR : {'{:,.1f}'.format(_FP7_type[(_FP7_type['country_code']=='FRA') & (_FP7_type['stage']=='successful')]['funding'].sum())}")

    _h20_type=(h20
            .loc[h20.pilier_name_en!='Euratom']
        .groupby(['framework','project_id', 'call_year', 'country_code', 'country_name_fr', 'stage', 'with_coord',
                    'action_code','cordis_type_entity_code', 'is_ejo'], dropna=False)
        .agg({'coordination_number':'sum', 'number_involved':'sum', 'calculated_fund':'sum'})
        .reset_index()
        .rename(columns={'calculated_fund':'funding'})
        .assign(framework='Horizon 2020')
        )

    print(f"subv H20 FR : {'{:,.1f}'.format(_h20_type[(_h20_type['country_code']=='FRA') & (_h20_type['stage']=='successful')]['funding'].sum())}")

    _temp_type=(projects_current
        .groupby(['project_id', 'call_year', 'country_code', 'country_name_fr', 'stage', 'with_coord',
                    'action_code','cordis_type_entity_code', 'is_ejo'], dropna=False)
        .agg({'coordination_number':'sum', 'number_involved':'sum', 'calculated_fund':'sum'})
        .reset_index()
        .rename(columns={'calculated_fund':'funding'})
        .assign(framework='Horizon Europe')
        )
    print(f"subv HE FR : {'{:,.1f}'.format(_temp_type[(_temp_type['country_code']=='FRA') & (_temp_type['stage']=='successful')]['funding'].sum())}")

    _pc_type = pd.concat([_FP6_type,_FP7_type, _temp_type, _h20_type], ignore_index=True)
    print(f"subv FR : {'{:,.1f}'.format(_pc_type[(_pc_type['country_code']=='FRA') & (_pc_type['framework']=='Horizon Europe') & (_pc_type['stage']=='successful')]['funding'].sum())}")
    #######################################################################################################

    '''create tab all PC by country/year'''
    total = (_pc_type
            .groupby(['framework', 'call_year', 'stage', 'cordis_type_entity_code', 'with_coord'], dropna=False)
            .agg({'funding':'sum', 'project_id': 'nunique', 'coordination_number':'sum', 'number_involved':'sum'})
            .reset_index()
            .rename(columns={'project_id':'project_number'})
            .assign(country_code='ALL', rank_evaluated=0, rank_successful=0)
            )

    _pc_type1 = (_pc_type
            .groupby(['framework', 'call_year', 'stage', 'country_code', 'country_name_fr', 'project_id', 'cordis_type_entity_code', 'with_coord'], dropna=False)
            .agg({'number_involved':'sum', 'coordination_number':'sum', 'funding':'sum'})
            .reset_index()
            .assign(is_ejo='Avec')
            )

    _pc_type2 = (_pc_type.loc[_pc_type.is_ejo=='Sans']
            .groupby(['framework', 'call_year', 'stage', 'country_code', 'country_name_fr', 'project_id', 'cordis_type_entity_code', 'with_coord', 'is_ejo'], dropna=False)
            .agg({'number_involved':'sum', 'coordination_number':'sum', 'funding':'sum'})
            .reset_index()
            )

    _pc_type = pd.concat([_pc_type1, _pc_type2], ignore_index=True)

    country=[]

    for i in ['successful', 'evaluated']:
        temp = (_pc_type.loc[(_pc_type['stage']==i)]
            .groupby(['framework', 'country_code', 'is_ejo'], dropna=False)
            .agg({'funding':'sum'})
            .sort_values(['framework','funding'], ascending=False)
            .reset_index()
        )
        
        temp[f'rank_{i}'] = temp.groupby(['framework','is_ejo'])['funding'].rank('average', ascending=False).astype(int)
        temp = temp.drop(columns='funding')
        _pc_type = _pc_type.merge(temp, how='left', on=['framework', 'country_code', 'is_ejo'])
    #     _pc_type.loc[(_pc_type['stage']==i)&(_pc_type.country_code.isin(['ZOE','ZOI'])), f'rank_{i}'] = 0
        _pc_type.loc[(_pc_type['stage']==i), f'rank_{i}'] = 0
        country.extend(list(_pc_type[_pc_type[f'rank_{i}']<11]['country_code'].unique()))
        
    country = list(set(country))
    _pc_type = _pc_type[_pc_type['country_code'].isin(country)]

    _pc_type = pd.concat([_pc_type, total], ignore_index=True)

    # creation de plusieurs niveaux de periode
    _pc_type['mixte_periode_H']=np.where(_pc_type['framework'].isin(['FP6', 'FP7']), _pc_type['framework'], _pc_type['call_year'])
    _pc_type['mixte_periode_HE']=np.where(_pc_type['framework'].isin(['FP6', 'FP7', 'Horizon 2020']), _pc_type['framework'], _pc_type['call_year'])

    _pc_type.loc[_pc_type.project_number.isnull(), 'project_number']=0

    type_entity = json.load(open('data_files/legalEntityType.json', 'r', encoding='UTF-8'))
    type_entity = pd.DataFrame(type_entity)
    _pc_type = _pc_type.merge(type_entity, how='left', on='cordis_type_entity_code')

    _pc_type.sort_values(['country_code'], ascending=False).to_csv(f"{PATH_CONNECT}all_FW_type_resume.csv", index=False, encoding="UTF-8", sep=";", na_rep='', decimal=".")
