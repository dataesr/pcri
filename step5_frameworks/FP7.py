import requests, pandas as pd
from config_path import PATH_SOURCE, PATH_CLEAN
from step3_entities.references import *
from step3_entities.merge_referentiels import *
from step3_entities.categories import *
from step3_entities.ID_getSourceRef import *

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
    ref=ref_select('FP7')

    def FP7_load():
        FP7_PATH='C:/Users/zfriant/Documents/OneDrive/PCRI/FP7/2022/'
        _FP7 = pd.read_pickle(f"{FP7_PATH}FP7_data.pkl")
        print(f"size _FP7: {len(_FP7)}")
        return _FP7
    _FP7=FP7_load()

    def FP7_cleaning(_FP7):
        _FP7 = _FP7.loc[~_FP7.status_code.isin(['INELIGIBLE','WITHDRAWN'])]
        _FP7.loc[_FP7.status_code=='Project Closed', 'status_code'] = 'CLOSED'
        _FP7.loc[_FP7.status_code=='Project Terminated', 'status_code'] = 'TERMINATED'

        _FP7.loc[_FP7.participant_type_code=='N/A', 'participant_type_code'] = 'NA'
        _FP7['role'] = _FP7['role'].str.lower()
        _FP7.loc[_FP7.role=='participant', 'role'] = 'partner'
        _FP7['coordination_number']=np.where(_FP7['role']=='coordinator', 1, 0)
        _FP7.loc[(_FP7.generalPic=='998133396')&(_FP7.countryCode=='ZZ'), 'country_code_mapping'] = 'USA' # bristol meyer
        print(f"size _FP7: {len(_FP7)}, size with id: {len(_FP7.loc[~_FP7.id.isnull()])}")
        
        zz = _FP7.loc[(_FP7.country_code_mapping=='ZZZ')]
        print(f"size _FP7 sans country_code: {len(zz)}")
        zz = ref.loc[ref.generalPic.isin(zz.generalPic.unique())]
        _FP7 = _FP7.merge(zz, how='left', on='generalPic', suffixes=['','_ref'])
        for i in ['id', 'country_code_mapping', 'ZONAGE']:
            _FP7.loc[~_FP7[f"{i}_ref"].isnull(), i] = _FP7[f"{i}_ref"]
        _FP7 = _FP7.drop(_FP7.filter(regex='_ref$').columns, axis=1)
        print(f"size _FP7: {len(_FP7)}, {_FP7.loc[_FP7.stage=='successful', 'funding'].sum()}")
        
        p = _FP7[['generalPic', 'country_code_mapping','country_code']].drop_duplicates()
        print(f"size de p: {len(p)}")
        #lien part et ref
        p = p.merge(ref, how='outer', on=['generalPic', 'country_code_mapping'], indicator=True).drop_duplicates()
        p = p.loc[p._merge.isin(['both', 'left_only'])]
        print(f"cols de p: {p.columns}")

        # p1 pic+ccm commun
        p1 = p.loc[p['_merge']=='both'].drop(columns=['_merge', 'country_code'])
        print(f"size p1 pic+cc: {len(p1)}")

        # p2 pic cc
        p2 = (p.loc[p['_merge']=='left_only'].drop(columns=['_merge', 'id', 'ZONAGE'])
            .merge(ref.rename(columns={'country_code_mapping':'country_code'}), 
                    how='inner', on=['generalPic', 'country_code']).drop_duplicates()
            .drop(columns='country_code'))
        print(f"size p2 pic cc_parent: {len(p2)}")

        # acteurs sans identifiant dont le pic à plusieurs pays ou le pic certaines participations ont un identifiant et pas d'autres 
        p3 = (p.loc[p['_merge']=='left_only'].drop(columns=['_merge', 'country_code_mapping', 'id', 'ZONAGE'])
            .merge(ref, how='inner', on=['generalPic']).drop_duplicates())
        if not p3.empty:
            print(f"A faire si possible, vérifier pourquoi des participations avec pic identiques ont un id ou pas nb pic: {len(p3.generalPic.unique())}")

        if 'p2' in globals() or 'p2' in locals():
            p1 = pd.concat([p1,p2], ignore_index=True).drop_duplicates()
            print(f"size de new p: {len(p)}, cols: {p.columns}") 

        FP7 = (_FP7.drop(columns=['id', 'ZONAGE', 'country_code'])
                .merge(p1[['generalPic', 'country_code_mapping', 'id', 'ZONAGE']], 
                    how='left', on=['generalPic', 'country_code_mapping']))
        print(f"size _FP7: {len(_FP7)}, size FP7: {len(FP7)},  size with id: {len(FP7.loc[~FP7.id.isnull()])}")
        
        country = pd.read_csv(f"{PATH_SOURCE}H2020/country_current.csv", sep=';', encoding='utf-8')
        FP7 = FP7.merge(country[['country_code_mapping', 'country_name_mapping', 'country_code']].drop_duplicates(), how='left', on='country_code_mapping')
        # FP7.loc[~FP7.ZONAGE.isnull(), 'country_code'] = FP7.ZONAGE
        if any(FP7.country_code.isnull()):
            print(f"country_code null {FP7.loc[FP7.country_code.isnull(), ['country_code_mapping', 'country_name_mapping']].drop_duplicates()}")
            FP7.loc[FP7.country_code_mapping=='GUF', 'country_code'] = 'FRA'
            FP7.loc[FP7.country_code_mapping=='GUF', 'country_name_mapping'] = 'French Guiana'
            FP7.loc[FP7.country_code_mapping.isin(['SGS', 'IOT']), 'country_code'] = 'GBR'
            FP7.loc[FP7.country_code_mapping=='IOT', 'country_name_mapping'] = 'British Indian Ocean Territory'
            FP7.loc[FP7.country_code_mapping=='SGS', 'country_name_mapping'] = 'South Georgia and the South Sandwich Islands'

        cc = country.drop(columns=['country_code_mapping', 'country_name_mapping', 'countryCode', 'countryCode_parent']).drop_duplicates()
        FP7 = FP7.merge(cc, how='left', on='country_code')
        FP7.loc[FP7.country_code_mapping=='ZOE', 'country_name_mapping'] = 'European organisations area'

        FP7.loc[FP7.country_code_mapping=='ZOE', 'country_code'] = 'ZOE'
        FP7.loc[FP7.country_code=='ZOE', 'country_name_fr'] = 'Union Européenne'
        FP7.loc[FP7.country_code=='ZOE', 'country_name_en'] = 'European organisations area'

        print(f"size part1: {len(FP7)}, cols: {FP7.columns}")    
        
        return FP7
    FP7=FP7_cleaning(_FP7)

    def FP7_entities(FP7):
        # part.country_code.unique()
        entities = FP7.loc[~FP7.id.isnull(), ['generalPic','id', 'country_code_mapping']].drop_duplicates()
        print(f"1 - size entities {len(entities)}")
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
        cat_filter = category_paysage(paysage)
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
            print("ATTENTION faire code pour traiter deux siren différents -> ce qui serait bizarre qu'il y ait 2 siren")
        else:
            ### si besoin de charger groupe
            file_name = f"{PATH_REF}H20_groupe.pkl"
            groupe = pd.read_pickle(file_name)
            print(f"taille de entities_tmp avant groupe:{len(entities_tmp)}")

            entities_tmp=entities_tmp.merge(groupe, how='left', on='siren')

            # entities_tmp.loc[~entities_tmp.groupe_id.isnull(), 'entities_id']= entities_tmp.groupe_id
            # entities_tmp.loc[~entities_tmp.groupe_id.isnull(), 'entities_acronym'] = entities_tmp.groupe_acronym
            # entities_tmp.loc[~entities_tmp.groupe_id.isnull(), 'entities_name'] = entities_tmp.groupe_name

            # entities_tmp.loc[entities_tmp.entities_id.str.contains('gent', na=False), 'siren_cj'] = 'GE_ENT'
            
            # entities_tmp = entities_tmp.drop(['groupe_id','groupe_name','groupe_acronym'], axis=1).drop_duplicates()
            print(f"taille de entities_tmp après groupe {len(entities_tmp)}")

        entities_tmp = entities_tmp.merge(get_source_ID(entities_tmp, 'entities_id'), how='left', on='entities_id')
            # traitement catégorie
        # entities_tmp = category_cleaning(entities_tmp, sirene)
        entities_tmp = category_woven(entities_tmp, sirene)
        entities_tmp = category_agreg(entities_tmp)
        return  entities_tmp
    entities_tmp=FP7_entities(FP7)

    # calculs
    def FP7_calcul(FP7, entities_tmp):
        print(f"size part avant: {len(FP7)}")
        part1 = (FP7[['project_id', 'participant_order', 'role', 'generalPic', 'global_costs',
            'participant_type_code', 'name_source', 'acronym_source', 'countryCode', 'nutsCode',
            'funding', 'status.x', 'ADRESS', 'city', 'post_code', 'pme', 'stage', 'nom', 'countryCode_parent', 'vat_id',
            'country_code_mapping', 'participant_id', 'number_involved', 'coordination_number', 'id', 'ZONAGE',
            'country_name_mapping', 'country_code', 'country_name_en','country_association_code', 'country_association_name_en',
            'country_group_association_code', 'country_group_association_name_en','country_group_association_name_fr', 
            'country_name_fr', 'article1', 'article2']]
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
        print(f"size part après: {len(part1)}")

        if any(part1.entities_id=='nan')|any(part1.entities_id.isnull()):
            print(f"attention il reste des entities sans entities_id valides")
        
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

        print(f"size part1 with code after cleanup nuts: {len(part1[~part1.nuts_code_tmp.isnull()])}")

        nuts = nuts.loc[(nuts.nuts_code_tmp.isin(part1.nuts_code_tmp.unique()))&(~nuts.nuts_code_tmp.isnull())]
        part1 = part1.merge(nuts, how='left', on='nuts_code_tmp').drop_duplicates()
        print(f"nuts code without name: {len(part1[(~part1.nuts_code.isnull())&(part1.region_1_name.isnull())])}")

        print(part1.groupby(['stage'], dropna=True )['nuts_code'].size())
        print(part1.loc[part1.stage=='successful', 'funding'].sum())
        return part1
    part1=FP7_calcul(FP7, entities_tmp)


    instr = pd.read_csv('data_files/instru_nomenclature.csv', sep=';')
    act=pd.read_json(open("data_files/actions_name.json", 'r', encoding='utf-8'))
    msca_correspondence = pd.read_table('data_files/msca_correspondence.csv', sep=";").drop(columns='framework')
    erc_correspondence = pd.read_json(open("data_files/ERC_correspondance.json", 'r', encoding='utf-8'))
    thema = pd.read_json(open("data_files/thema.json", 'r', encoding='utf-8'))
    destination = pd.read_json(open("data_files/destination.json", 'r', encoding='utf-8'))

    def themes_cleaning(FP7):
        proj = (FP7.assign(stage_name=np.where(FP7.stage=='successful', 'projets lauréats', 'projets évalués'))
                [['project_id', 'stage', 'acronym', 'abstract', 'title', 'call_id', 'stage_name',
                'call_deadline', 'instrument',  'panel_code', 'panel_name', 'call_year', 'duration', 'status_code', 
            'cost_total', 'eu_reqrec_grant', 'free_keywords', 'number_involved', 'submission_date',
            'start_date', 'signature_date', 'end_date',  'pilier', 'prog_abbr', 'prog_lib', 'area_abbr', 'area_lib']]
                .drop_duplicates())

        proj.loc[(proj.prog_abbr=='ERC')&(proj.instrument=='POC'), 'instrument'] = 'ERC-POC'
        proj.loc[proj.prog_abbr=='PEOPLE', 'thema_code'] = 'MSCA'
        proj.loc[proj.prog_abbr=='ERC', 'thema_code'] = 'ERC'

        print(f"1 - size proj: {len(proj)}")

        proj = proj.merge(instr, how='left', on='instrument').drop(columns=['name'])
        proj.loc[proj.instrument.str.contains('MC-'), 'action_code'] = 'MSCA'        

        if any(proj.action_code.isnull()):
            print(proj[proj.action_code.isnull()].instrument.unique())   
            
        print(f"2 - size proj: {len(proj)}")

        # ERC
        proj = proj.merge(erc_correspondence, how='left', left_on=['instrument'], right_on=['old'])

        proj.loc[(proj.thema_code=='ERC')&(proj.destination_code.isnull()), 'destination_code'] = 'ERC-OTHER'

        proj.loc[proj.thema_code=='ERC', 'programme_code'] = 'ERC'
        proj.loc[proj.thema_code=='ERC', 'programme_name_en'] = 'European Research Council (ERC)'

        # MSCA
        proj = proj.merge(msca_correspondence, how='left', left_on=['instrument'], right_on=['old'])
        proj.loc[proj.call_id.str.contains('NIGHT'), 'destination_detail_code'] = 'CITIZENS'
        proj.loc[~proj.destination_detail_code.isnull(), 'destination_code'] = proj.destination_detail_code.str.split('-').str[0]
        proj.loc[(proj.destination_code.isnull())&(proj.thema_code=='MSCA'), 'destination_code'] = 'MSCA-OTHER'
        proj.loc[proj.thema_code=='MSCA', 'programme_code'] = 'MSCA'
        proj.loc[proj.thema_code=='MSCA', 'programme_name_en'] = 'Marie Skłodowska-Curie Actions (MSCA)'

        proj.rename(columns={'instrument':'fp_specific_instrument'}, inplace=True)

        print(f"size proj: {proj.loc[proj.stage=='successful'].project_id.nunique()}, nb project_id: {len(proj.loc[proj.stage=='successful'])}")

        #euratom
        proj.loc[(proj.pilier.isin(['EURATOM']))&(proj.prog_abbr=='Fission'), 'programme_code'] = 'NFRP'
        proj.loc[(proj.pilier.isin(['EURATOM']))&(proj.programme_code=='NFRP'), 'programme_name_en'] = 'Nuclear fission and radiation protection'
        proj.loc[proj.prog_abbr=='Fusion', 'programme_code'] = 'Fusion'
        proj.loc[proj.prog_abbr=='Fusion', 'programme_name_en'] = 'Fusion Energy'

        euratom = pd.read_csv('data_files/euratom_thema_all_FP.csv', sep=';', na_values='')
        proj = proj.merge(euratom[['topic_area', 'thema_code', 'thema_name_en']], how='left', left_on='area_abbr', right_on='topic_area', suffixes=['', '_t'])
        proj.loc[(~proj.thema_code_t.isnull()), 'thema_code'] = proj.loc[(~proj.thema_code_t.isnull()), 'thema_code_t']
        proj = proj.filter(regex=r'.*(?<!_t)$')

        #ju_jti
        proj.loc[proj.prog_abbr=='SP1-JTI', 'thema_code'] = 'JU-JTI'
        proj.loc[proj.prog_abbr=='SP1-JTI', 'destination_code'] = proj.area_abbr.str.split('-').str[-1]
        proj.loc[proj.area_abbr=='JTI-CS', 'destination_code'] = 'CLEAN-AVIATION'

        proj.loc[(proj.destination_code=='CLEAN-SKY'), 'destination_code'] = 'CLEAN-AVIATION'
        proj.loc[(proj.destination_code=='FCH'), 'destination_code'] = 'CLEANH2'
        proj.loc[(proj.destination_code=='IMI'), 'destination_code'] = 'IHI'
        proj.loc[(proj.destination_code.isin(['ENIAC','ARTEMIS'])), 'destination_code'] = 'Chips'
        proj.loc[proj.thema_code=='JU-JTI', 'action_code'] = proj.fp_specific_instrument.str.split('-').str[1]

        # WIDENING COST
        proj.loc[proj.area_abbr.str.contains('COST', na=False), 'thema_code'] = 'COST'
        proj.loc[proj.area_abbr.str.contains('COST', na=False), 'programme_code'] = 'Widening'
        proj.loc[proj.area_abbr.str.contains('COST', na=False), 'programme_name_en'] = 'Widening participation and spreading excellence'

        proj.loc[proj.pilier=='EURATOM', 'pilier_name_en'] = 'Euratom'
        proj.loc[(proj.prog_abbr.isin(['PEOPLE','ERC']))|(proj.prog_abbr=='INFRA'), 'pilier_name_en'] = 'Excellent Science'
        proj.loc[proj.pilier_name_en.isnull(), 'pilier_name_en'] = proj.pilier.str.capitalize()

        proj.loc[proj.programme_code.isnull(), 'programme_code'] = proj.prog_abbr
        proj.loc[proj.programme_name_en.isnull(), 'programme_name_en'] = proj.prog_lib


        proj.loc[(~proj.thema_code.isin(['MSCA','ERC']))&(proj.destination_code.isnull()), 'destination_code'] = proj.area_abbr
        proj.loc[proj.destination_code.isnull(), 'destination_code'] = proj.thema_code+'-OTHER'
        proj = proj.merge(destination[['destination_code', 'destination_name_en']], how='left', on='destination_code')
        proj = (proj
                .merge(destination.rename(columns={'destination_code':'destination_detail_code', 'destination_name_en':'destination_detail_name_en'})
                [['destination_detail_code', 'destination_detail_name_en']], how='left', on='destination_detail_code'))

        proj.loc[(~proj.thema_code.isin(['MSCA','ERC']))&(proj.destination_name_en.isnull()), 'destination_name_en'] = proj.area_lib
        proj.loc[proj.thema_code.isnull(), 'thema_code'] = proj.prog_abbr
        proj = proj.merge(thema[['thema_code', 'thema_name_en']], how='left', on='thema_code', suffixes=['', '_t'])
        proj.loc[proj.thema_name_en.isnull(), 'thema_name_en'] = proj.thema_name_en_t
        proj.loc[proj.thema_name_en.isnull(),'thema_name_en'] = proj.prog_lib
        proj = proj.filter(regex=r'.*(?<!_t)$')

        proj = (proj.drop(columns=['area_abbr', 'area_lib'])
                .rename(columns={'prog_lib':'fp_specific_programme', 'pilier':'fp_specific_pilier'}))
        
        print(proj[['programme_code',
        'programme_name_en', 'thema_name_en', 'destination_code', 'destination_name_en',
        'destination_detail_code','destination_detail_name_en']].drop_duplicates())
        return proj
    proj=themes_cleaning(FP7)

    def proj_cleaning(proj):
        proj = proj.merge(act, how='left', on='action_code')
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
        country=(part1.loc[part1.stage=='successful',
                    ['project_id','country_code','country_name_fr','country_code_mapping', 'ZONAGE',
                        'country_name_mapping', 'nuts_code', 'region_1_name', 'region_2_name','regional_unit_name']]
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
                'destination_code','destination_detail_code', 'destination_detail_name_en', 'destination_name_en', 
                'duration', 'ecorda_date', 'end_date', 'fp_specific_instrument', 'framework', 'free_keywords', 
                'panel_code', 'panel_name', 'fp_specific_programme', 'fp_specific_pilier',
                'pilier_name_en', 'programme_code', 'programme_name_en', 'project_id', 'signature_date', 'stage', 'stage_name', 
                'start_date', 'status_code', 'submission_date', 'thema_code', 'thema_name_en', 'title']]
                
            .drop_duplicates())

        project = project.merge(p, how='left', on='project_id').merge(country, how='inner', on='project_id').merge(prop, how='left' , on='project_id')

        print(f"1 - size project lauréats: {len(project)}, {len(p)}, fund: {'{:,.1f}'.format(p['project_eucontribution'].sum())}")

        with open(f"{PATH_CLEAN}FP7_successful_projects.pkl", 'wb') as file:
            pd.to_pickle(project, file)
        return project
    proj_ods(proj, part1)

    def FP7_all(proj, part1):
        t = (proj.drop(columns=['cost_total', 'duration', 'end_date', 'eu_reqrec_grant', 'fp_specific_instrument', 
                            'fp_specific_programme', 'fp_specific_pilier',
                            'number_involved', 'signature_date', 'start_date', 'submission_date'])
            .merge(part1, how='inner', on=['project_id', 'stage'])
            .rename(columns={'funding':'calculated_fund', 'ZONAGE':'extra_joint_organization'}))
        
        t = (t.assign(is_ejo=np.where(t.extra_joint_organization.isnull(), 'Sans', 'Avec')))

        t.loc[(t.destination_code.isin(['PF', 'ERARESORG', 'GA']))|((t.thema_code.isin(['ERC', 'COST']))&(t.destination_code!='SyG')), 'coordination_number'] = 0
        t=t.assign(with_coord=True)
        t.loc[(t.destination_code.isin(['PF', 'ERARESORG', 'GA']))|((t.thema_code.isin(['ERC', 'COST']))&(t.destination_code!='SyG')), 'with_coord'] = False

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