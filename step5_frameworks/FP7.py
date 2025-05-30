import requests, pandas as pd
from config_path import PATH_SOURCE, PATH_CLEAN, PATH
from step3_entities.references import *
from step3_entities.merge_referentiels import *
from step3_entities.categories import *
from step3_entities.ID_getSourceRef import *
from step5_frameworks.functions_shared import *

def FP7_process():
    print("\n### FP7")
    def call_api():
        call=pd.read_json(open(f"data_files/FP7_calls.json", 'r+', encoding='utf-8'))
        call = pd.DataFrame(call)
        call['call_budget'] = call['call_budget'].str.replace(',', '').astype('float')
        return call
    call=call_api()

    def ref_select(FP):
        ref_source = ref_source_load('ref')
        # traitement ref select le FP, id non null ou/et ZONAGE non null
        ref = ref_source_2d_select(ref_source, FP)
        return ref
    ref, genPic_to_new=ref_select('FP7')

    def FP7_load():
        FP7_PATH=f'{PATH}FP7/2022/'
        _FP7 = pd.read_pickle(f"{FP7_PATH}FP7_data.pkl")
        print(f"- size _FP7 load: {len(_FP7)}")
        return _FP7
    _FP7=FP7_load()

    # country = pd.read_csv(f"{PATH_SOURCE}H2020/country_current.csv", sep=';', encoding='utf-8')
    def FP7_cleaning(_FP7):
        _FP7 = _FP7.loc[~_FP7.status_code.isin(['INELIGIBLE','WITHDRAWN'])]
        _FP7.loc[_FP7.status_code=='Project Closed', 'status_code'] = 'CLOSED'
        _FP7.loc[_FP7.status_code=='Project Terminated', 'status_code'] = 'TERMINATED'

        _FP7.loc[_FP7.participant_type_code=='N/A', 'participant_type_code'] = 'NA'
        _FP7['role'] = _FP7['role'].str.lower()
        _FP7.loc[_FP7.role=='participant', 'role'] = 'partner'
        _FP7['coordination_number']=np.where(_FP7['role']=='coordinator', 1, 0)
        _FP7.loc[(_FP7.generalPic=='998133396')&(_FP7.countryCode=='ZZ'), 'country_code_mapping'] = 'USA' # bristol meyer
        print(f"- size _FP7 after clean status: {len(_FP7)}, size with id: {len(_FP7.loc[~_FP7.id.isnull()])}")
        
        zz = _FP7.loc[(_FP7.country_code_mapping=='ZZZ')]
        print(f"- size _FP7 sans country_code: {len(zz)}")
        zz = ref.loc[ref.generalPic.isin(zz.generalPic.unique())]
        _FP7 = _FP7.merge(zz, how='left', on='generalPic', suffixes=['','_ref'])
        for i in ['id', 'country_code_mapping', 'ZONAGE']:
            _FP7.loc[~_FP7[f"{i}_ref"].isnull(), i] = _FP7[f"{i}_ref"]
        _FP7 = _FP7.drop(_FP7.filter(regex='_ref$').columns, axis=1)
        print(f"- size _FP7 with country: {len(_FP7)}, {_FP7.loc[_FP7.stage=='successful', 'funding'].sum()}")
        
        p = _FP7[['generalPic', 'country_code_mapping','country_code']].drop_duplicates()
        print(f"- size de p: {len(p)}")
        #lien part et ref
        p = p.merge(ref, how='outer', on=['generalPic', 'country_code_mapping'], indicator=True).drop_duplicates()
        p = p.loc[p._merge.isin(['both', 'left_only'])]
        # print(f"cols de p: {p.columns}")

        # p1 pic+ccm commun
        p1 = p.loc[p['_merge']=='both'].drop(columns=['_merge', 'country_code'])
        print(f"- size p1 pic+cc: {len(p1)}")

        # p2 pic cc
        p2 = (p.loc[p['_merge']=='left_only'].drop(columns=['_merge', 'id', 'ZONAGE'])
            .merge(ref.rename(columns={'country_code_mapping':'country_code'}), 
                    how='inner', on=['generalPic', 'country_code']).drop_duplicates()
            .drop(columns='country_code'))
        print(f"- size p2 pic cc_parent: {len(p2)}")

        # acteurs sans identifiant dont le pic à plusieurs pays ou le pic certaines participations ont un identifiant et pas d'autres 
        p3 = (p.loc[p['_merge']=='left_only'].drop(columns=['_merge', 'country_code_mapping', 'id', 'ZONAGE'])
            .merge(ref, how='inner', on=['generalPic']).drop_duplicates())
        if not p3.empty:
            print(f"1 - A faire si possible, vérifier pourquoi des participations avec pic identiques ont un id ou pas nb pic: {len(p3.generalPic.unique())}")

        if 'p2' in globals() or 'p2' in locals():
            p1 = pd.concat([p1,p2], ignore_index=True).drop_duplicates()
            print(f"2 - size de new p: {len(p)}, cols: {p.columns}") 

        FP7 = (_FP7.drop(columns=['id', 'ZONAGE', 'country_code'])
                .merge(p1[['generalPic', 'country_code_mapping', 'id', 'ZONAGE']], 
                    how='left', on=['generalPic', 'country_code_mapping']))
        
        print(f"- size _FP7 with ref: {len(_FP7)}, size FP7: {len(FP7)},  size with id: {len(FP7.loc[~FP7.id.isnull()])}")

        return FP7
    FP7=FP7_cleaning(_FP7)

    cc=country_cleaning(FP7, 'FP7')
    FP7=FP7.drop(columns=['countryCode_parent', 'country_code_mapping']).merge(cc, how='left', on='countryCode')
    print(f"- size FP7 with country assoc: {len(FP7)},\ncols: {FP7.columns}")    


    def FP7_entities(FP7):
        print("\n## FP7 entities")
        # part.country_code.unique()
        entities = FP7.loc[~FP7.id.isnull(), ['generalPic','id', 'country_code_mapping']].drop_duplicates()
        print(f"- size entities {len(entities)}")
        if any(entities.id.str.contains(';')):
            entities = entities.assign(id_extend=entities.id.str.split(';')).explode('id_extend')
            entities.loc[(entities.id.str.contains(';', na=False))&(entities.id_extend.str.len()==14), 'id_extend'] = entities.loc[(entities.id.str.contains(';', na=False))&(entities.id_extend.str.len()==14)].id_extend.str[:9]
            entities = entities.drop_duplicates()
            entities_size_to_keep = len(entities)
            print(f"2 - size entities si multi id -> entities_size_to_keep = {entities_size_to_keep}")

        ror = pd.read_pickle(f"{PATH_REF}ror_df.pkl")
        entities_tmp = merge_ror(entities, ror)
        print(f"size entities_tmp after add ror_info: {len(entities_tmp)}, entities_size_to_keep: {entities_size_to_keep}")


        # PAYSAGE
        ### si besoin de charger paysage pickle
        paysage = pd.read_pickle(f"{PATH_REF}paysage_df.pkl")
        if any(paysage.groupby('id')['id_clean'].transform('count')>1):
            print(f"1 - paysage doublon oublié: {paysage[paysage.groupby('id')['id_clean'].transform('count')>1][['id', 'id_clean']].sort_values('id')}")
            paysage = paysage.loc[~((paysage.id_clean=='vey7g')&(paysage.id.str.contains('265100057', na=False)))]    
        
        paysage_category = pd.read_pickle(f"{PATH_SOURCE}paysage_category.pkl")
        cat_filter = category_paysage(paysage_category)
        entities_tmp = merge_paysage(entities_tmp, paysage, cat_filter)

        sirene = pd.read_pickle(f"{PATH_REF}sirene_df.pkl")
        entities_tmp = merge_sirene(entities_tmp, sirene)

        # traitement des id identifiés mais sans referentiels liés
        entities_tmp.loc[(entities_tmp.entities_id.isnull())&(~entities_tmp.id_extend.str.contains('-', na=False)), 'entities_id'] = entities_tmp['id_extend']

        entities_tmp['siren']=entities_tmp.loc[entities_tmp.entities_id.str.contains('^[0-9]{9}$|^[0-9]{14}$', na=False)].entities_id.str[:9]
        entities_tmp.loc[entities_tmp.siren.isnull(), 'siren']=entities_tmp.paysage_siren

        #groupe

        # recuperation tous les siren pour lien avec groupe -> creation var SIREN 
        entities_tmp.loc[~entities_tmp.siren.isnull(), "siren"] = entities_tmp.loc[~entities_tmp.siren.isnull(), "siren"].str.split().apply(set).str.join(";")

        if any(entities_tmp.siren.str.contains(';', na=False)):
            print("1 - ATTENTION faire code pour traiter deux siren différents -> ce qui serait bizarre qu'il y ait 2 siren")
        else:
            ### si besoin de charger groupe
            file_name = f"{PATH_REF}H20_groupe.pkl"
            groupe = pd.read_pickle(file_name)
            print(f"2 - taille de entities_tmp avant groupe:{len(entities_tmp)}")

            entities_tmp=entities_tmp.merge(groupe, how='left', on='siren')

            # entities_tmp.loc[~entities_tmp.groupe_id.isnull(), 'entities_id']= entities_tmp.groupe_id
            # entities_tmp.loc[~entities_tmp.groupe_id.isnull(), 'entities_acronym'] = entities_tmp.groupe_acronym
            # entities_tmp.loc[~entities_tmp.groupe_id.isnull(), 'entities_name'] = entities_tmp.groupe_name

            # entities_tmp.loc[entities_tmp.entities_id.str.contains('gent', na=False), 'siren_cj'] = 'GE_ENT'
            
            # entities_tmp = entities_tmp.drop(['groupe_id','groupe_name','groupe_acronym'], axis=1).drop_duplicates()
            print(f"- size entities_tmp after groupe {len(entities_tmp)}")

        entities_tmp = entities_tmp.merge(get_source_ID(entities_tmp, 'entities_id'), how='left', on='entities_id')
            # traitement catégorie
        # entities_tmp = category_cleaning(entities_tmp, sirene)
        entities_tmp = category_woven(entities_tmp, sirene)
        entities_tmp = category_agreg(entities_tmp)
        return  entities_tmp
    entities_tmp=FP7_entities(FP7)

    # calculs
    def FP7_calcul(FP7, entities_tmp):
        print("\n## FP7 calculation")
        print(f"- size part before: {len(FP7)}")
        part1 = (FP7[['project_id', 'participant_order', 'role', 'generalPic', 'global_costs',
            'participant_type_code', 'name_source', 'acronym_source', 'countryCode', 'nutsCode',
            'funding', 'status.x', 'ADRESS', 'city', 'post_code', 'pme', 'stage', 'nom',  'vat_id',
            'country_code_mapping', 'participant_id', 'number_involved', 'coordination_number', 'id', 'ZONAGE',
            'country_name_mapping', 'country_code', 'country_name_en','country_association_code', 'country_association_name_en',
            'country_group_association_code', 'country_group_association_name_en','country_group_association_name_fr', 
            'country_name_fr', 'article1', 'article2', 'fp_specific_country_status']]
                .merge(entities_tmp, how='left', on=['generalPic', 'country_code_mapping', 'id']))

        part2=(part1.loc[part1.entities_name.isnull()].drop_duplicates())
        part3=(part2.sort_values(['name_source', 'acronym_source'], ascending=False)
            .groupby(['generalPic', 'country_code_mapping'])
            .first().reset_index()[['generalPic', 'country_code_mapping', 'name_source', 'acronym_source']]
            .rename(columns={'name_source':'entities_name', 'acronym_source':'entities_acronym'}))

        part2 = (part2.drop(columns=['entities_name', 'entities_acronym', 'nom'])
                .merge(part3, how='left', on=['generalPic', 'country_code_mapping']))
        part2['entities_name'] = part2.entities_name.str.capitalize().str.strip()
        part2['entities_id'] = "pic"+part2.generalPic.map(str)

        part1=part1.loc[~part1.entities_name.isnull()].drop_duplicates()

        part1=pd.concat([part1, part2], ignore_index=True).assign(number_involved=1)

        part1['nb'] = part1.id.str.split(';').str.len()
        for i in ['funding', 'coordination_number', 'number_involved']:
            part1[i] = np.where(part1['nb']>1, part1[i]/part1['nb'], part1[i])

        # 'requestedGrant'
        print(f"- size part after: {len(part1)}")

        if any(part1.entities_id=='nan')|any(part1.entities_id.isnull()):
            print(f"1 - attention il reste des entities sans entities_id valides")
        
        type_entity = pd.read_json(open('data_files/legalEntityType.json', 'r', encoding='UTF-8'))
        # part1.loc[part1.participant_type_code=='N/A', 'participant_type_code'] = 'NA'
        part1 = (part1.merge(type_entity, how='left', left_on='participant_type_code', right_on='cordis_type_entity_code')
        .drop(columns='participant_type_code'))
        
        # gestion code nuts
        nuts = pd.read_pickle("data_files/nuts_complet.pkl")
        nuts = (nuts[['nuts_code_2013','nutsCode', 'lvl1Description', 'lvl2Description', 'lvl3Description']]
                .drop_duplicates()
                .rename(columns={'nuts_code_2013':'nuts_code_tmp', 'nutsCode':'nuts_code','lvl1Description':'region_1_name', 'lvl2Description': 'region_2_name', 'lvl3Description':'regional_unit_name'}))
        # nuts['region_1_name'] = nuts['region_1_name'].str.title()
        print(len(nuts))

        part1['nuts_code_tmp'] = np.where(part1.nutsCode.str.len()<3, np.nan, part1.nutsCode)

        print(f"- size part1 with code after cleanup nuts: {len(part1[~part1.nuts_code_tmp.isnull()])}")

        nuts = nuts.loc[(nuts.nuts_code_tmp.isin(part1.nuts_code_tmp.unique()))&(~nuts.nuts_code_tmp.isnull())]
        part1 = part1.merge(nuts, how='left', on='nuts_code_tmp').drop_duplicates().rename(columns={'nuts_code':'participation_nuts'})
        print(f"nuts code without name: {len(part1[(~part1.participation_nuts.isnull())&(part1.region_1_name.isnull())])}")

        print(part1.groupby(['stage'], dropna=True )['participation_nuts'].size())
        print(part1.loc[part1.stage=='successful', 'funding'].sum())
        return part1
    part1=FP7_calcul(FP7, entities_tmp)


    def themes_cleaning(proj):
        proj = (FP7[['project_id', 'stage', 'acronym', 'abstract', 'title', 'call_id', 
                'call_deadline', 'instrument',  'panel_code', 'panel_name', 'call_year', 'duration', 'status_code', 
                'cost_total', 'eu_reqrec_grant', 'free_keywords', 'number_involved', 'submission_date',
                'start_date', 'signature_date', 'end_date',  'pilier', 'prog_abbr', 'prog_lib', 'area_abbr', 'area_lib']]
                .drop_duplicates())

        # # ERC
        erc_correspondence = pd.read_json(open("data_files/ERC_correspondance.json", 'r', encoding='utf-8'))

        proj.loc[proj.prog_abbr=='ERC', 'thema_code'] = 'ERC'
        proj.loc[(proj.prog_abbr=='ERC')&(proj.instrument.str.contains('POC', na=False)), 'instrument'] = 'ERC-POC'
        proj = (proj.merge(erc_correspondence, how='left', left_on=['instrument'], right_on=['old'])
                .rename(columns={'new':'destination_code'})
                .drop(columns='old'))
        proj.loc[(proj.thema_code=='ERC')&(proj.destination_code.isnull()), 'destination_code'] = 'ERC-OTHER'

        proj.loc[proj.thema_code=='ERC', 'programme_next_fp'] = 'ERC'


        # # MSCA
        df = proj.loc[(proj.prog_abbr=='PEOPLE')|(proj.instrument.str.startswith('MC-')), ['prog_abbr', 'call_id', 'instrument']].drop_duplicates()
        df['inst'] = df['instrument'].str.replace('MC-', '')

        df=thema_msca_cleaning(df, 'FP7')
        proj = proj.merge(df, how='left', on=['prog_abbr', 'call_id', 'instrument'], suffixes=('', '_t'))

        selected_columns = [col[:-2] for col in proj.columns if col.endswith('_t')]
        for i in selected_columns:
            proj.loc[~proj[f"{i}_t"].isnull(), i] = proj.loc[~proj[f"{i}_t"].isnull()][f"{i}_t"]
        proj = proj.filter(regex=r'.*(?<!_t)$')
        proj.loc[proj.thema_code=='MSCA', 'programme_next_fp'] = 'MSCA'
        print(f"- size proj after msca: {proj.loc[proj.stage=='successful'].project_id.nunique()}, nb project_id: {len(proj.loc[proj.stage=='successful'])}")

        # #euratom
        df = proj.loc[proj.pilier.isin(['EURATOM']), ['prog_abbr', 'area_abbr']].assign(topic_area=proj.area_abbr)
        df = thema_euratom_cleaning(df, 'FP7')
        proj = proj.merge(df, how='left', on=['prog_abbr', 'area_abbr'], suffixes=('', '_t'))

        selected_columns = [col[:-2] for col in proj.columns if col.endswith('_t')]
        for i in selected_columns:
            proj.loc[~proj[f"{i}_t"].isnull(), i] = proj.loc[~proj[f"{i}_t"].isnull()][f"{i}_t"]
        proj = proj.filter(regex=r'.*(?<!_t)$')

        #ju_jti
        proj.loc[proj.prog_abbr.str.contains('JTI', na=False), 'thema_code'] = 'JU-JTI'
        proj.loc[proj.prog_abbr.str.contains('JTI', na=False),  'destination_code'] = proj.loc[proj.prog_abbr.str.contains('JTI', na=False)].instrument.str.split('-').str[-1]
        proj.loc[proj.area_abbr=='JTI-CS', 'destination_code'] = 'CLEAN-SKY'

        proj.loc[(proj.destination_code=='CLEAN-SKY'), 'destination_next_fp'] = 'CLEAN-AVIATION'
        proj.loc[(proj.destination_code=='FCH'), 'destination_next_fp'] = 'CLEANH2'
        proj.loc[(proj.destination_code=='IMI'), 'destination_next_fp'] = 'IHI'
        proj.loc[(proj.destination_code.isin(['ENIAC','ARTEMIS'])), 'destination_next_fp'] = 'Chips'
        # proj.loc[proj.thema_code=='JU-JTI', 'action_code'] = proj.fp_specific_instrument.str.split('-').str[1]

        # WIDENING COST
        proj.loc[proj.area_abbr.str.contains('COST', na=False), 'thema_code'] = 'COST'
        proj.loc[proj.area_abbr.str.contains('COST', na=False), 'programme_next_fp'] = 'Widening'

        destination = pd.read_json(open("data_files/destination.json", 'r', encoding='utf-8'))
        proj.loc[(~proj.thema_code.isin(['MSCA','ERC']))&(proj.destination_code.isnull()), 'destination_code'] = proj.area_abbr
        # proj.loc[proj.destination_code.isnull(), 'destination_code'] = proj.thema_code+'-OTHER'
        proj = proj.merge(destination[['destination_code', 'destination_name_en']], how='left', on='destination_code')
        proj.loc[(~proj.destination_code.isnull())&(proj.destination_name_en.isnull()), 'destination_name_en'] = proj.area_lib

        thema = pd.read_json(open("data_files/thema.json", 'r', encoding='utf-8'))
        proj = proj.merge(thema[['thema_code', 'thema_name_en']], how='left', on='thema_code')
        proj.loc[(~proj.thema_code.isnull())&(proj.thema_name_en.isnull()), 'destination_name_en'] = proj.prog_lib


        proj.loc[proj.programme_code.isnull(), 'programme_code'] = proj.prog_abbr
        proj.loc[proj.programme_name_en.isnull(), 'programme_name_en'] = proj.prog_lib

        proj['pilier_name_en'] = proj.pilier.str.capitalize()
        proj.loc[proj.prog_abbr.isin(['PEOPLE','IDEAS', 'CAPACITIES']), 'pilier_next_fp'] = 'Excellent Science'

        # action
        instr = pd.read_csv('data_files/instru_nomenclature.csv', sep=';')
        proj = proj.merge(instr, how='left', on='instrument').drop(columns=['instrument_name']).rename(columns={'name':'action_name'})
        proj.loc[proj.destination_code=='NIGHT', 'action_next_fp'] = 'MSCA'   


        if any(proj.action_code.isnull()):
            print(proj[proj.action_code.isnull()].instrument.unique())   
            
        print(f"- size proj: {len(proj.drop_duplicates())}")

        return proj.drop_duplicates()
    proj=themes_cleaning(FP7)

    proj=ju_jti_parterships(proj, 'FP7')
    proj=eranet_partnerships(proj, 'FP7')

    def proj_cleaning(proj):
        proj=proj.assign(stage_name=np.where(proj.stage=='successful', 'projets lauréats', 'projets évalués'))
        proj = proj.merge(call, how='left', on='call_id').assign(ecorda_date=pd.to_datetime('2021-04-30'), framework='FP7')
        proj = proj.assign(ecorda_date=pd.to_datetime('2021-04-30'), framework='FP7')
        for i in ['title', 'abstract', 'free_keywords']:
            proj[i]=proj[i].str.replace('\\n|\\t|\\r|\\s+', ' ', regex=True).str.strip()

        kw = proj[['project_id', 'free_keywords']]
        kw = kw.assign(free_keywords = kw.free_keywords.str.split(';|,')).explode('free_keywords')
        kw = kw.loc[kw.free_keywords.str.len()>3].drop_duplicates()
        kw.free_keywords = kw.free_keywords.groupby(level=0).apply(lambda x: '|'.join(x.str.strip().unique()))

        proj = proj.drop(columns='free_keywords').merge(kw.drop_duplicates(), how='left', on='project_id')
        proj.mask(proj=='', inplace=True)  

        for d in ['call_deadline', 'signature_date',  'start_date',  'end_date', 'submission_date']:
            proj[d] = pd.to_datetime(proj[d],format='%d/%m/%Y %H:%M:%S')
        return proj
    proj=proj_cleaning(proj)

    def proj_ods(proj, part1):
        
        part1[['participation_nuts', 'region_1_name', 'region_2_name', 'regional_unit_name']] = part1[['participation_nuts', 'region_1_name', 'region_2_name', 'regional_unit_name']].fillna('')
        country=(part1.loc[part1.stage=='successful',
                    ['project_id','country_code','country_name_fr','country_code_mapping', 'ZONAGE',
                        'country_name_mapping', 'participation_nuts', 'region_1_name', 'region_2_name','regional_unit_name']]
            .drop_duplicates()
            .groupby(['project_id'], as_index = False).agg(lambda x: ';'.join(map(str,filter(None, x))))
            .drop_duplicates())

        prop = (proj.loc[proj.stage=='evaluated', ['project_id', 'cost_total', 'eu_reqrec_grant', 'number_involved']]
            .rename(columns={'number_involved':'proposal_numberofapplicants', 'eu_reqrec_grant':'proposal_requestedgrant', 'cost_total':'proposal_budget'})
            .drop_duplicates())

        p = (proj.loc[proj.stage=='successful', ['project_id', 'eu_reqrec_grant', 'number_involved', 'cost_total']]
            .rename(columns={'eu_reqrec_grant':'project_eucontribution', 'number_involved':'project_numberofparticipants','cost_total':'project_totalcost'})
            .drop_duplicates())

        # # PROVISOIRE quand def call refonctionnera
        # proj=proj.assign(call_budget=np.nan)

        project = (proj.loc[proj.stage=='successful', 
                ['abstract', 'acronym', 'action_code', 'action_name', 'call_budget','call_deadline', 'call_id', 'call_year',
                'destination_code', 'destination_name_en', 'duration', 'ecorda_date', 'end_date', 'framework', 'free_keywords', 
                'panel_code', 'panel_name', 'pilier_name_en', 'programme_code', 'programme_name_en', 'project_id', 'signature_date', 'stage', 'stage_name', 
                'start_date', 'status_code', 'submission_date', 'thema_code', 'thema_name_en', 'title',
                'destination_next_fp', 'programme_next_fp', 'action_next_fp', 'pilier_next_fp',
                'euro_partnerships_type','euro_partnerships_type_next_fp',	'euro_ps_name',	'euro_partnerships_flag'
                ]]
                
            .drop_duplicates())

        project = project.merge(p, how='left', on='project_id').merge(country, how='inner', on='project_id').merge(prop, how='left' , on='project_id')

        print(f"1 - size project lauréats: {len(project)}, {len(p)}, fund: {'{:,.1f}'.format(p['project_eucontribution'].sum())}")

        with open(f"{PATH_CLEAN}FP7_successful_projects.pkl", 'wb') as file:
            pd.to_pickle(project, file)
        
    proj_ods(proj, part1)

    def FP7_all(proj, part1):
        
        t = (proj.drop(columns=['cost_total', 'duration', 'end_date', 'eu_reqrec_grant',
                            'number_involved', 'signature_date', 'start_date', 'submission_date'])
            .merge(part1, how='inner', on=['project_id', 'stage']))
        
        t = (t.assign(is_ejo=np.where(t.ZONAGE.isnull(), 'Sans', 'Avec'))
            .rename(columns={'funding':'calculated_fund', 'ZONAGE':'extra_joint_organization'})
        )

        mask=(t.destination_code.isin(['ERARESORG', 'GA']))|((t.destination_next_fp.str.startswith('PF')))|((t.thema_code.isin(['ERC', 'COST'])))
        t.loc[mask, 'coordination_number'] = 0
        t=t.assign(with_coord=True)
        t.loc[mask, 'with_coord'] = False

        t.loc[t.thema_code=='ERC', 'erc_role'] = 'partner'

        t.loc[(t.destination_code=='SyG'), 'erc_role'] = 'PI'
        t.loc[(t.action_code=='ERC')&(t.destination_code!='SyG')&(t.role=='coordinator'), 'erc_role'] = 'PI'
        t.loc[(t.destination_code=='ERC-OTHER'), 'erc_role'] = np.nan


        file_name = f"{PATH_CLEAN}FP7_data.pkl"
        with open(file_name, 'wb') as file:
            pd.to_pickle(t, file)

        print(f"size proj: {t.loc[t.stage=='successful'].project_id.nunique()}, nb project_id: {len(t.loc[t.stage=='successful'])}, {t.loc[t.stage=='successful', 'calculated_fund'].sum()}")
        return t
    FP7_all(proj, part1)