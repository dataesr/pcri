import pandas as pd, numpy as np, json
from config_path import PATH_CLEAN, PATH_SOURCE, PATH
from step5_frameworks.country import country_cleaning
def FP6_process():
    print("\n### FP6")
    def FP6_load():
        FP6_PATH=f'{PATH}FP6/'
        with open(f"{FP6_PATH}FP6_projects.json", 'r', encoding='ANSI') as fp:
            _FP6 = pd.DataFrame(json.load(fp))
        _FP6.columns = _FP6.columns.str.strip()
        for i in _FP6.columns:
            _FP6[i] = _FP6[i].apply(lambda x: x.strip() if isinstance(x, str) else x)
        _FP6 = _FP6.reindex(sorted(_FP6.columns), axis=1)
        print(f"- size _FP6 load: {len(_FP6)},\ncols: {_FP6.columns}")
        return _FP6

    _FP6=FP6_load()

    def nuts(_FP6):
    # gestion code nuts
        nuts = pd.read_pickle("data_files/nuts_complet.pkl")
        nuts = (nuts[['nuts_code_2013','nutsCode', 'lvl1Description', 'lvl2Description', 'lvl3Description']]
                .drop_duplicates()
                .rename(columns={'nuts_code_2013':'nuts_code_tmp', 'nutsCode':'nuts_code','lvl1Description':'region_1_name', 'lvl2Description': 'region_2_name', 'lvl3Description':'regional_unit_name'}))

        # nuts['region_1_name'] = nuts['region_1_name'].str.title()
        # print(len(nuts))

        _FP6.loc[_FP6.NutsCode.str.len()>2, 'nuts_code_tmp'] = _FP6.NutsCode
        print(f"size _FP6 with code after cleanup nuts: {len(_FP6[~_FP6.nuts_code_tmp.isnull()])}")

        nuts = nuts.loc[(nuts.nuts_code_tmp.isin(_FP6.nuts_code_tmp.unique()))&(~nuts.nuts_code_tmp.isnull())]
        _FP6 = _FP6.merge(nuts, how='left', on='nuts_code_tmp').drop_duplicates()
        print(f"nuts code without name: {len(_FP6[(~_FP6.nuts_code.isnull())&(_FP6.region_1_name.isnull())])}")
        return _FP6
    _FP6=nuts(_FP6)

    def str_cleaning(_FP6):
        _FP6=(_FP6
            .rename(columns={'year':'call_year', 'Call':'call_id', 'action2':'action_code2',
                            'participant_type_code':'cordis_type_entity_code', 'coord':'coordination_number', 
                            'date_end':'end_date', 'date_sign':'signature_date', 'date_start':'start_date'}))

        _FP6.loc[:,'project_id'] = _FP6.loc[:,'project_id'].astype(str).str.rjust(6, "0")
        for d in ['signature_date',  'start_date',  'end_date', 'call_deadline', 'submission_date']:
            _FP6[d] = pd.to_datetime(_FP6[d],format='%d/%m/%Y %H:%M:%S')

        for i in ['title']:
            _FP6[i]=_FP6[i].str.replace('\\n|\\t|\\r|\\s+', ' ', regex=True).str.strip()

        _FP6.mask(_FP6=='', inplace=True)  
        _FP6 = _FP6.assign(ecorda_date=pd.to_datetime('2021-04-30'), framework='FP6', stage_name='projets lauréats')
        return _FP6
    _FP6=str_cleaning(_FP6)

    cc = country_cleaning(_FP6, 'FP6')
    _FP6=_FP6.drop(columns=['countryCode_parent']).merge(cc, how='left', on='countryCode')
    print(f"size FP6 with country: {len(_FP6)}\ncols: {_FP6.columns}")


    def category(_FP6):
        _FP6.loc[_FP6.cordis_type_entity_code.isnull(), 'cordis_type_entity_code'] = 'NA'
        type_entity = pd.read_json(open('data_files/legalEntityType.json', 'r', encoding='UTF-8'))
        _FP6 = _FP6.merge(type_entity, how='left', on='cordis_type_entity_code')

        print(f"size FP6 with category: {len(_FP6)}")
        return _FP6
    _FP6=category(_FP6)

        
    def themes_act(_FP6):
        instr = pd.read_csv('data_files/instru_nomenclature.csv', sep=';')
        destination = pd.read_json(open("data_files/destination.json", 'r', encoding='utf-8'))
        msca_correspondence = pd.read_table('data_files/msca_correspondence.csv', sep=";").drop(columns='framework')

        # msca
        _FP6.loc[_FP6.programme=='Human resources and mobility', 'thema_code'] = 'MSCA'
        _FP6.loc[_FP6.programme=='Human resources and mobility', 'thema_name_en'] = 'Marie Skłodowska-Curie'

        x = _FP6.loc[_FP6.thema_code=='MSCA', ['programme','call_id', 'action', 'action_code2']].drop_duplicates()
        x = (x.merge(instr.drop_duplicates(), how='left', left_on='action_code2', right_on='instrument')
        .rename(columns={'instrument':'destination_code', 'instrument_name':'destination_name_en'}) 
        )
        x = x.merge(msca_correspondence, how='left', left_on=['action_code2'], right_on=['old']).rename(columns={'new':'destination_next_fp'})
        x.loc[x.call_id=='FP6-2006-MOBILITY-13', 'destination_code'] = 'NIGHT'
        x.loc[x.call_id=='FP6-2006-MOBILITY-13', 'destination_next_fp'] = 'CITIZENS'
        x.loc[(x.action=='MCA')&(x.destination_code.isnull()), 'destination_code'] = 'MSCA-OTHER'
        x.loc[(x.action=='MCA')&(x.destination_next_fp.isnull()), 'destination_next_fp'] = 'MSCA-OTHER'
        x.loc[x.action=='MCA', 'action_code'] = 'MSCA'
        x.loc[x.action_code=='MSCA', 'action_name'] = 'Marie Skłodowska-Curie actions'
        _FP6 = _FP6.merge(x[[ 'call_id', 'action_code2', 'destination_code', 'destination_name_en', 'destination_next_fp', 'action_code',
        'action_name']], how='left', on=['call_id','action_code2'])


        #euratom
        euratom = pd.read_csv('data_files/euratom_thema_all_FP.csv', sep=';', na_values='')
        _FP6 = _FP6.merge(euratom[['topic_area', 'thema_code', 'thema_name_en']], how='left', left_on='ActivityCode1', right_on='topic_area', suffixes=['', '_t'])

        #thema euratom devient destination dans FP6
        _FP6.loc[(~_FP6.thema_code_t.isnull()), 'destination_code'] = _FP6.loc[(~_FP6.thema_code_t.isnull()), 'thema_code_t']
        _FP6.loc[(~_FP6.thema_name_en_t.isnull()), 'destination_name_en'] = _FP6.loc[(~_FP6.thema_name_en_t.isnull()), 'thema_name_en_t']
        _FP6 = _FP6.filter(regex=r'.*(?<!_t)$')
        _FP6.loc[_FP6.pilier=='Euratom', 'thema_name_en'] = 'Nuclear fission and radiation protection'
        _FP6.loc[_FP6.pilier=='Euratom', 'thema_code'] = 'NFRP'


        # FP6.loc[FP6.pilier_name_en.isnull(), 'pilier_name_en'] = FP6.pilier
        # FP6.loc[FP6.programme_name_en.isnull(), 'programme_name_en'] = FP6.programme

        x = _FP6[['pilier','programme', 'ActivityCode1']].drop_duplicates()
        x['programme_code'] = x.ActivityCode1.str.split("\\.|-", regex=True, expand=True)[0]
        x = x.groupby(['pilier','programme','programme_code'], dropna=False).agg({'ActivityCode1':'count'}).reset_index()
        x = (x.sort_values(['pilier','programme','ActivityCode1'], ascending=False)
                .drop_duplicates(subset=['programme'], keep="first")
                .drop(columns='ActivityCode1'))
        x.loc[x.programme=='Euratom', 'programme_code'] = 'EURATOM'
        x.loc[x.programme=='Research infrastructures', 'programme_code'] = 'INFRA'
        _FP6 = _FP6.merge(x[['programme', 'programme_code']], how='left', on='programme')

        _FP6.loc[_FP6.programme.isin(['Research infrastructures','Human resources and mobility']), 'pilier_next_fp'] = 'Excellent Science'
        _FP6.loc[_FP6.programme=='Research infrastructures', 'programme_next_fp'] = 'INFRA'
        _FP6.loc[_FP6.programme=='Human resources and mobility', 'programme_next_fp'] = 'MSCA'

        _FP6 = _FP6.merge(destination[['destination_code', 'destination_name_en']], how='left', on='destination_code', suffixes=('', '_t'))
        _FP6.loc[(~_FP6.destination_name_en_t.isnull()), 'destination_name_en'] = _FP6.loc[(~_FP6.destination_name_en_t.isnull()), 'destination_name_en_t']
        
        _FP6 = _FP6.rename(columns={'pilier':'pilier_name_en', 'programme':'programme_name_en'})

        # finalisation action variables
        _FP6 = _FP6.merge(instr[['instrument', 'action_code', 'name']], how='left', left_on='action', right_on='instrument', suffixes=('', '_t'))

        _FP6.loc[_FP6.action_code.isnull(), 'action_code'] = _FP6.loc[_FP6.action_code.isnull(), 'action_code_t']
        _FP6.loc[_FP6.action_name.isnull(), 'action_name'] = _FP6.loc[_FP6.action_name.isnull(), 'name']

        print(f"- size FP6 after clean thema: {len(_FP6.loc[_FP6.stage=='successful'])}, fund: {'{:,.1f}'.format(_FP6.loc[_FP6.stage=='successful', 'subv_obt'].sum())}")
        return _FP6.drop(columns=['topic_area', 'destination_name_en_t', 'action_code2', 'action', 'instrument', 'name', 'action_code_t'])
    FP6=themes_act(_FP6)

    def participation(FP6):
        FP6['calculated_fund'] = np.where(FP6.stage=='successful', FP6.subv_obt, FP6.subv_dem)
        FP6 = FP6.assign(number_involved=1, with_coord=np.where(FP6.destination_next_fp.str.contains('PF', na=False), False, True))
        FP6.loc[FP6.with_coord==False, 'coordination_number'] = 0

        print(f"- size FP6 final: {len(FP6.loc[FP6.stage=='successful'])}, fund: {'{:,.1f}'.format(FP6.loc[FP6.stage=='successful', 'calculated_fund'].sum())}")
        # FP6.info()
        with open(f"{PATH_CLEAN}FP6_data.pkl", 'wb') as file:
            pd.to_pickle(FP6, file)
        return FP6
    FP6=participation(FP6)

    def ods(FP6):
        print("### FP6 ods")
        country=(FP6[['project_id','country_code','country_name_fr','country_code_mapping', 'country_name_mapping', 'nuts_code', 'region_1_name',
            'region_2_name', 'regional_unit_name']]
                .drop_duplicates()
                .groupby(['project_id'], as_index = False).agg(lambda x: ';'.join(map(str, filter(None, x))))
                .drop_duplicates())
        print(f"size country: {len(country)}")

        project = (FP6[['acronym', 'action_code', 'action_name', 'call_id', 'call_year', 'call_deadline', 
                        'destination_code', 'destination_name_en', 
                    'duration', 'ecorda_date', 'end_date', 'framework', 'pilier_next_fp', 'programme_next_fp',
                    'pilier_name_en', 'programme_name_en', 'project_cost', 'programme_code', 'destination_next_fp',
                    'project_eucontribution', 'project_id', 'project_numberofparticipants', 'submission_date',
                    'signature_date', 'stage', 'stage_name', 'start_date', 'status_code', 'thema_code', 'thema_name_en', 'title']]
            .rename(columns={'project_cost':'project_totalcost'})   
            .drop_duplicates())

        project = project.merge(country, how='inner', on='project_id')

        print(f"1 - size project lauréats: {len(project)}, fund: {'{:,.1f}'.format(project['project_eucontribution'].sum())}")

        file_name = f"{PATH_CLEAN}FP6_successful_projects.pkl"
        with open(file_name, 'wb') as file:
            pd.to_pickle(project, file)
    ods(FP6)