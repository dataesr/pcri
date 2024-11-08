import pandas as pd, numpy as np, json
from config_path import PATH_CLEAN

def FP6_process():
    print("\n### FP6")
    def FP6_load():
        FP6_PATH='C:/Users/zfriant/Documents/OneDrive/PCRI/FP6/'
        with open(f"{FP6_PATH}FP6_projects.json", 'r', encoding='ANSI') as fp:
            _FP6 = pd.DataFrame(json.load(fp))
        _FP6.columns = _FP6.columns.str.strip()
        for i in _FP6.columns:
            _FP6[i] = _FP6[i].apply(lambda x: x.strip() if isinstance(x, str) else x)
        _FP6 = _FP6.reindex(sorted(_FP6.columns), axis=1)
        print(f"size _FP6: {len(_FP6)}, cols: {_FP6.columns}")
        return _FP6

    _FP6=FP6_load()

    def nuts(_FP6):
    # gestion code nuts
        nuts = pd.read_pickle("data_files/nuts_complet.pkl")
        nuts = (nuts[['nuts_code_2013','nutsCode', 'lvl1Description', 'lvl2Description', 'lvl3Description']]
                .drop_duplicates()
                .rename(columns={'nuts_code_2013':'nuts_code_tmp', 'nutsCode':'nuts_code','lvl1Description':'region_1_name', 'lvl2Description': 'region_2_name', 'lvl3Description':'regional_unit_name'}))

        # nuts['region_1_name'] = nuts['region_1_name'].str.title()
        print(len(nuts))

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

    def country(_FP6):
        old_country = pd.read_csv('data_files/FP_old_countries.csv', sep=';', keep_default_na=False)
        old_country = old_country.loc[old_country.FP=='FP6'].drop_duplicates()

        x = (_FP6[['countryCode', 'countryCode_parent']].drop_duplicates()
            .merge(old_country[['countryName', 'countryCode','country_code_mapping']],
                    how='left', on='countryCode')
            .drop_duplicates())

        if any(x.countryCode.isnull()):
            print(x[x.countryCode.isnull()])

        country = pd.read_csv("C:/Users/zfriant/Documents/OneDrive/PCRI/eCorda_datas/datas_load/H2020/country_current.csv", sep=';', encoding='utf-8')
        x = x.merge(country[['country_code_mapping', 'country_name_mapping', 'country_code']].drop_duplicates(), how='left', on='country_code_mapping')
        x.loc[x.country_code_mapping=='ZOE', 'country_name_mapping'] = 'European organisations area'

        if any(x.country_code_mapping.isnull()):
            print(x.loc[x.country_code_mapping.isnull()])
        if any(x.country_name_mapping.isnull()):
            print(f"nom manquant dans country: {x.loc[x.country_name_mapping.isnull()].countryName.unique()}")
            x.loc[x.country_name_mapping.isnull(), 'country_name_mapping'] = x.countryName
            
        if any(x.country_code.isnull()):
            print(x.loc[x.country_code.isnull(), ['countryCode', 'country_code_mapping']])
            x.loc[x.country_code_mapping.isin(['ZOE', 'YUG']), 'country_code'] = x.country_code_mapping
            
        x = x.merge(country[['country_code', 'country_name_en', 'country_name_fr', 'article1', 'article2']].drop_duplicates(), how='left', on='country_code')

        if any(x.country_name_en.isnull()):
            print(f"sans country name: {x.loc[x.country_name_en.isnull()].country_code_mapping.unique()}")
            x.loc[x.country_code_mapping=='YUG', 'country_name_en'] = x.country_name_mapping
            x.loc[x.country_code=='YUG', 'country_name_fr'] = 'Serbie et Monténégro'
            
        x = (x
            .merge(old_country[['country_code_mapping', 'status_new']].drop_duplicates(), 
                    how='left', left_on='country_code', right_on='country_code_mapping', suffixes=['', '_fp'])
            .merge(country[['country_association_code', 'country_association_name_en',
            'country_group_association_code', 'country_group_association_name_en',
            'country_group_association_name_fr']].drop_duplicates(),
                how='left', left_on='status_new', right_on='country_association_code')
            .drop(columns='country_code_mapping_fp')
            .drop_duplicates())   

        if any(x.country_association_code.isnull()):
            print(x.loc[x.country_association_code.isnull(), ['country_code_mapping', 'country_association_code']])
            x.loc[x.country_association_code.isnull(), 'country_association_code'] = x.status_new
            x.loc[x.country_association_code=='CANDIDATE', 'country_group_association_code'] = 'MEMBER-ASSOCIATED'
            x.loc[x.country_association_code=='CANDIDATE', 'country_group_association_name_fr'] = 'Pays membres ou associés'
            x.loc[x.country_association_code=='CANDIDATE', 'country_group_association_name_en'] = 'Member States or associated'
            
        x = x.drop(columns=['countryCode_parent', 'countryName', 'status_new']).drop_duplicates()
        print(f"size x {len(x)}")
            
        _FP6 = (_FP6.merge(x, how='left', on='countryCode')
            .merge(old_country[['countryCode', 'STATUS']].drop_duplicates(), how='left', on='countryCode')
            .rename(columns={'STATUS':'fp_specific_country_status'}))

        print(f"size FP6: {len(_FP6)}\ncols: {_FP6.columns}")
        return _FP6
    _FP6=country(_FP6)

    def category(_FP6):
        _FP6.loc[_FP6.cordis_type_entity_code.isnull(), 'cordis_type_entity_code'] = 'NA'
        type_entity = pd.read_json(open('data_files/legalEntityType.json', 'r', encoding='UTF-8'))
        _FP6 = _FP6.merge(type_entity, how='left', on='cordis_type_entity_code')

        print(f"size FP6: {len(_FP6)}")
        return _FP6
    _FP6=category(_FP6)

    def themes_act(_FP6):
        instr = pd.read_csv('data_files/instru_nomenclature.csv', sep=';')
        destination = pd.read_json(open("data_files/destination.json", 'r', encoding='utf-8'))
        msca_correspondence = pd.read_table('data_files/msca_correspondence.csv', sep=";").drop(columns='framework')

        _FP6.loc[_FP6.programme=='Human resources and mobility', 'thema_code'] = 'MSCA'
        _FP6.loc[_FP6.programme=='Human resources and mobility', 'thema_name_en'] = 'Marie Skłodowska-Curie'

        x = (_FP6.loc[~_FP6.action_code2.isnull(),['action_code2']].drop_duplicates()
                .merge(msca_correspondence, how='left', left_on=['action_code2'], right_on=['old']))
        x.loc[~x.destination_detail_code.isnull(), 'destination_code'] = x.destination_detail_code.str.split('-').str[0]
        FP6 = _FP6.merge(x, how='left', on='action_code2').drop(columns='old')


        FP6.loc[FP6.call_id=='FP6-2006-MOBILITY-13', 'destination_code'] = 'CITIZENS'

        FP6.loc[(FP6.thema_code=='MSCA')&(FP6.destination_code.isnull()), 'destination_code'] = 'MSCA-OTHER'

        FP6['fp_specific_instrument'] = np.where((FP6.action=='MCA')&(~FP6.action.isnull()), FP6['action']+'-'+FP6['action_code2'], FP6['action'])

        FP6 = (FP6
            .merge(instr.drop_duplicates(), how='left', left_on='action', right_on='instrument')
            .rename(columns={'name':'action_name'}) 
            .drop(columns=['instrument', 'action_code2'])
            )

        FP6.loc[FP6.action_code=='MCA', 'action_code'] = 'MSCA'
        FP6.loc[FP6.action_code=='MSCA', 'action_name'] = 'Marie Skłodowska-Curie actions'

        #euratom
        euratom = pd.read_csv('data_files/euratom_thema_all_FP.csv', sep=';', na_values='')
        FP6 = FP6.merge(euratom[['topic_area', 'thema_code', 'thema_name_en']], how='left', left_on='ActivityCode1', right_on='topic_area', suffixes=['', '_t'])

        FP6.loc[(~FP6.thema_code_t.isnull()), 'thema_code'] = FP6.loc[(~FP6.thema_code_t.isnull()), 'thema_code_t']
        FP6.loc[(~FP6.thema_name_en_t.isnull()), 'thema_name_en'] = FP6.loc[(~FP6.thema_name_en_t.isnull()), 'thema_name_en_t']
        FP6 = FP6.filter(regex=r'.*(?<!_t)$')

        FP6.loc[FP6.pilier=='Euratom', 'pilier_name_en'] = 'Euratom'
        FP6.loc[FP6.pilier=='Euratom', 'programme_name_en'] = 'Nuclear fission and radiation protection'
        FP6.loc[FP6.pilier=='Euratom', 'programme_code'] = 'NFRP'

        FP6.loc[FP6.programme.isin(['Research infrastructures','Human resources and mobility']), 'pilier_name_en'] = 'Excellent Science'
        FP6.loc[FP6.programme=='Human resources and mobility', 'programme_name_en'] = 'Marie Skłodowska-Curie Actions (MSCA)'
        FP6.loc[FP6.programme=='Human resources and mobility', 'programme_code'] = 'MSCA'

        FP6.loc[FP6.programme=='Research infrastructures', 'programme_name_en'] = 'Research infrastructures'
        FP6.loc[FP6.programme=='Research infrastructures', 'programme_code'] = 'INFRA'

        FP6.loc[FP6.pilier_name_en.isnull(), 'pilier_name_en'] = FP6.pilier
        FP6.loc[FP6.programme_name_en.isnull(), 'programme_name_en'] = FP6.programme

        FP6 = FP6.rename(columns={'pilier':'fp_specific_pilier', 'programme':'fp_specific_programme'})

        test = FP6[['programme_code', 'programme_name_en', 'ActivityCode1']].drop_duplicates()
        test.loc[test.programme_code.isnull(), 'programme_code'] = test.ActivityCode1.str.split("\\.|-", regex=True, expand=True)[0]
        test = test.groupby(['programme_name_en','programme_code'], dropna=False).agg({'ActivityCode1':'count'}).reset_index()
        test = (test.sort_values(['programme_name_en','ActivityCode1'], ascending=False)
                .drop_duplicates(subset=['programme_name_en'], keep="first")
                .drop(columns='ActivityCode1'))
        FP6 = FP6.merge(test, how='left', on='programme_name_en', suffixes=['','_t'])
        FP6.loc[~FP6.programme_code_t.isnull(), 'programme_code'] = FP6.loc[~FP6.programme_code_t.isnull(), 'programme_code_t']
        FP6.drop(columns='programme_code_t', inplace=True)


        FP6 = FP6.merge(destination[['destination_code', 'destination_name_en']], how='left', on='destination_code')
        FP6 = (FP6.merge(destination.rename(columns={'destination_code':'destination_detail_code', 'destination_name_en':'destination_detail_name_en'})
                [['destination_detail_code', 'destination_detail_name_en']], how='left', on='destination_detail_code'))
        return FP6
    FP6=themes_act(_FP6)

    def participation(FP6):
        FP6['calculated_fund'] = np.where(FP6.stage=='successful', FP6.subv_obt, FP6.subv_dem)
        FP6 = FP6.assign(number_involved=1, with_coord=np.where(FP6.destination_code.isin(['PF']), False, True))
        FP6.loc[FP6.with_coord==False, 'coordination_number'] = 0

        print(f"1 - size project lauréats: {len(FP6.loc[FP6.stage=='successful'])}, fund: {'{:,.1f}'.format(FP6.loc[FP6.stage=='successful', 'calculated_fund'].sum())}")
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
                        'destination_code', 'destination_detail_code', 'destination_detail_name_en', 'destination_name_en', 
                    'duration', 'ecorda_date', 'end_date', 'framework',  'fp_specific_instrument', 'fp_specific_pilier',
                    'pilier_name_en', 'programme_name_en', 'project_cost', 'programme_code', 'fp_specific_programme',
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
    return FP6