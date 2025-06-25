def H2020_process():
    import pandas as pd, numpy as np, json
    from step3_entities.references import ref_source_load, ref_source_2d_select
    from step3_entities.merge_referentiels import merge_paysage, merge_ror, merge_sirene
    from step3_entities.categories import category_agreg, category_paysage,category_woven, cordis_type, mires
    from step3_entities.ID_getSourceRef import get_source_ID
    from step4_calculations.collaborations import collab_base, collab_cross
    from config_path import PATH_SOURCE, PATH_CLEAN, PATH_REF, PATH_CONNECT
    from functions_shared import unzip_zip, my_country_code

    def h20_nom_load():
        destination = pd.read_json(open("data_files/destination.json", 'r', encoding='utf-8'))
        thema = pd.read_json(open("data_files/thema.json", 'r', encoding='utf-8'))
        act = pd.read_json(open("data_files/actions_name.json", 'r', encoding='utf-8'))
        topics = unzip_zip('H2020_2022-12-05.json.zip', f"{PATH_SOURCE}H2020/", 'topics.json', encode='utf-8')
        pilier_fr = pd.read_json(open("data_files/H20_pilier.json", 'r', encoding='utf-8'))
        # countries = pd.read_csv(f"{PATH_SOURCE}H2020/country_current.csv", sep=';')
        countries = pd.read_pickle(f"{PATH_CLEAN}country_current.pkl")
        actions = pd.read_table(f"{PATH_CLEAN}actions_current.csv", sep=";")
        nuts = pd.read_pickle(f'{PATH_REF}nuts_complet.pkl')
        return destination, thema, act, topics, pilier_fr, countries, actions, nuts
    destination, thema, act, topics, pilier_fr, countries, actions, nuts = h20_nom_load()

    def h20_load():
        print("## LOAD bases")
        _proj=pd.read_pickle(f"{PATH_SOURCE}H2020/H2020_projects.pickle")
        _proj=pd.DataFrame(_proj)
        _proj=_proj.replace('#', np.nan)
        print(f"size _proj: {len(_proj)}")
        part=pd.read_pickle(f"{PATH_SOURCE}H2020/H2020_participation.pickle")
        part=pd.DataFrame(part)
        part=part.replace('#', np.nan)
        print(f"- size part: {len(part)}")
        entities = unzip_zip('H2020_2022-12-05.json.zip', f"{PATH_SOURCE}H2020/", "legalEntities.json", encode='utf-8')
        status = pd.read_csv(f"{PATH_SOURCE}H2020/redressement_status_code.csv", sep=';', usecols=['project_id','stat_code'], dtype='str')
        return _proj, part, entities, status
    _proj, part, entities, status = h20_load()
    print(f"involved successful:{'{:,.1f}'.format(part.loc[(part.stage=='successful'), 'generalPic'].count())}\nsubv_net_laureat:{'{:,.1f}'.format(part.loc[(part.stage=='successful'), 'subv_net'].sum())}\nsubv_laureat:{'{:,.1f}'.format(part.loc[(part.stage=='successful'), 'subv'].sum())}\nsubv_prop:{'{:,.1f}'.format(part.loc[(part.stage=='evaluated'), 'requestedGrant'].sum())}")

    country_h20 = my_country_code()

    part.loc[part.role=='participant', 'role'] = 'partner'
    # part.loc[part.countryCode=='ZZ', 'country_code_mapping'] = 'ZZZ'
    part = part[part.participates_as!='utro']
    part.rename(columns={'order_number':'orderNumber'}, inplace=True)
    print(f"size part: {len(part)}")
    part_init = part[['project_id', 'orderNumber', 'generalPic_old', 'pic', 'participates_as',
        'role', 'legalName', 'part_total_cost', 'subv', 'subv_net',
        'partner_status', 'countryCode', 'legalEntityTypeCode', 'isSme',
        'nutsCode', 'stage', 'shortName', 'requestedGrant', 'budget', 'url', 'generalPic']]
    print(f"size part_init with major cols: {len(part_init)}")


    part_init=(part_init.merge(country_h20[['iso2', 'iso3', 'parent_iso3']], how='left', left_on='countryCode', right_on='iso2')
    .rename(columns={'iso3':'country_code_mapping', 'parent_iso3':'country_code'})
    .drop(columns='iso2'))

    if any(part_init[part_init.country_code_mapping.isnull()].countryCode.unique()):
        print(part_init[part_init.country_code_mapping.isnull()].countryCode.unique())

    ##status
    _proj = _proj.merge(status, how='inner', on='project_id')
    _proj.loc[_proj.stage=='evaluated', 'status_code'] = _proj.stat_code
    _proj.drop(columns=['stat_code'], inplace=True)

    l=['RIA','IA','CSA']
    tmp=_proj.loc[(~_proj.action_id.isin(['MSCA','ERC'])&(~_proj.action_2_id.isnull())&(_proj.action_id!='SME')),
    ['action_2_id']].drop_duplicates()
    tmp['action_code'] = tmp['action_2_id'].str.extract("(" + "|".join(l) +")", expand=False)
    _proj = _proj.merge(tmp, how='left', on='action_2_id')
    _proj.loc[_proj.action_code.isnull(), 'action_code'] = _proj.action_id


    def h20_topics(df, act, actions, destination, pilier_fr, thema):

        proj = (_proj.rename(columns={'pilier':'pilier_name_en', 'topicCode':'topic_code','topicDescription':'topic_name',
                                        'action_2_id':'action_code2', 'action_2_name':'action_name2', 
                                        'action_3_id':'action_code3', 'action_3_name':'action_name3'})
            .drop(columns=['action_name'])
            .merge(pilier_fr, how='left', on='pilier_name_en')
            .merge(act, how='left', on='action_code'))

        #euratom
        proj.loc[proj.pilier_name_fr=='Euratom', 'pilier_name_en'] = 'Euratom'
        proj.loc[(proj.pilier_name_fr=='Euratom')&(proj.topic_code.str.contains('NFRP')), 'programme_code'] = 'NFRP'
        proj.loc[(proj.pilier_name_fr=='Euratom')&(proj.programme_code=='NFRP'), 'programme_name_en'] = 'Nuclear fission and radiation protection'
        proj.loc[proj.call_id=='EURATOM-Adhoc-2014-20', 'programme_code'] = 'Fusion'
        proj.loc[proj.call_id=='EURATOM-Adhoc-2014-20', 'programme_name_en'] = 'Fusion Energy'
        proj.loc[(proj.pilier_name_fr=='Euratom')&(proj.call_id!='EURATOM-Adhoc-2014-20')&(proj.programme_code!='NFRP'), 'programme_code'] = 'Euratom-other'
        proj.loc[(proj.pilier_name_fr=='Euratom')&(proj.call_id!='EURATOM-Adhoc-2014-20')&(proj.programme_code!='NFRP'), 'programme_name_en'] = 'Euratom other actions'

        euratom = pd.read_csv('data_files/euratom_thema_all_FP.csv', sep=';', na_values='')
        proj = proj.merge(euratom[['topic_area', 'thema_code', 'thema_name_en']], how='left', left_on='topic_code', right_on='topic_area', suffixes=['', '_t'])
        proj.loc[(~proj.thema_code_t.isnull()), 'thema_code'] = proj.loc[(~proj.thema_code_t.isnull()), 'thema_code_t']
        proj.loc[(~proj.thema_name_en_t.isnull()), 'thema_name_en'] = proj.loc[(~proj.thema_name_en_t.isnull()), 'thema_name_en_t']
        # proj = proj.filter(regex=r'.*(?<!_t)$').drop(columns='topic_area')

        # JU-JTI
        proj.loc[proj.action_code=='Art185', 'destination_code'] = proj.loc[proj.action_code=='Art185'].thema_code

        proj.loc[proj.thema_code.str.contains('JU', na=False), 'destination_code'] = proj.thema_code.str.replace('JU','').str.strip()
        proj.loc[(proj.destination_code=='Eurostars2'), 'destination_next_fp'] = "INNOVSMES"

        proj.loc[(proj.call_id.str.contains('PPP',na=False))|(proj.call_id.str.contains('JTI',na=False))|(proj.topic_code.str.contains('JTI',na=False)), 'destination_code'] = proj['action_code2'].str.split('-').str[0]
        proj.loc[proj.call_id.str.contains('JTI',na=False)&(proj.action_code2.isnull()), 'destination_code'] = proj['call_id'].str.split('-').str[2]
        proj.loc[(proj.thema_code=='CS2'), 'destination_code'] = 'CS2'
        proj.loc[(proj.destination_code.str.contains('BBI', na=False)), 'destination_next_fp'] = 'CBE'
        proj.loc[(proj.thema_code=='BBI'), 'thema_code'] = np.nan
        proj.loc[(proj.destination_code=='EuroHPC'), 'destination_next_fp'] = 'EUROHPC'
        proj.loc[(proj.thema_code=='ECSEL'), 'destination_code'] = 'ECSEL'
        proj.loc[(proj.destination_code=='ECSEL'), 'destination_next_fp'] = 'CHIPS'
        proj.loc[(proj.destination_code=='CS2'), 'destination_next_fp'] = 'CLEAN-AVIATION'
        proj.loc[(proj.destination_code=='FCH2'), 'destination_next_fp'] = 'CLEANH2'
        proj.loc[(proj.destination_code=='IMI2'), 'destination_next_fp'] = 'IHI'
        proj.loc[(proj.destination_code=='Shift2Rail'), 'destination_next_fp'] = "EU-RAIL"
        proj.loc[(~proj.destination_code.isnull())&(proj.destination_next_fp.isnull()), 'destination_next_fp'] = proj.loc[(~proj.destination_code.isnull())&(proj.destination_next_fp.isnull())].destination_code
        l=['KDT', 'CBE','EUROHPC', 'CLEAN-AVIATION', 'CLEANH2', 'IHI', 'CHIPS', "EU-RAIL"]
        proj.loc[(~proj.destination_next_fp.isnull()), 'thema_code'] = 'JU-JTI'

        # MSCA / ERC
        for col in ['thema_code','programme_next_fp']:
            proj.loc[proj.programme_code=='MSCA', col] = 'MSCA'
            proj.loc[proj.programme_code=='ERC', col] = 'ERC'

        # ### ajustement MSCA
        msca_correspondence = pd.read_table('data_files/msca_correspondence.csv', sep=";")

        msca_correspondence = msca_correspondence[msca_correspondence.framework=='H2020'].drop(columns='framework')
        proj.loc[(proj.thema_code=='MSCA')&(proj.action_code3.isnull()), 'action_code3'] = proj.action_code2

        proj.loc[(proj.thema_code=='MSCA'), 'destination_code'] = proj.loc[(proj.thema_code=='MSCA')].action_code3.str.replace('MSCA-', '').str.strip()
        proj.loc[(proj.thema_code=='MSCA'), 'destination_name_en'] = proj.loc[(proj.thema_code=='MSCA')].action_name2.str.replace('Marie Skłodowska-Curie', '').str.strip() +'-'+ proj.loc[(proj.thema_code=='MSCA')].action_name3.dropna()

        # m = proj.loc[(proj.action_code=='MSCA'), ['action_code3']].drop_duplicates()
        proj = proj.merge(msca_correspondence, how='left', left_on='action_code3', right_on='old')
        proj.loc[~proj.new.isnull(), 'destination_next_fp'] = proj.loc[~proj.new.isnull()].new
        proj.loc[(proj.thema_code=='MSCA')&(proj.destination_next_fp.isnull()),'destination_next_fp'] = 'MSCA-OTHER'
        # m = m.merge(actions[['destination_detail_code','destination_detail_name_en']].drop_duplicates(), how='left', on='destination_detail_code')
        proj.loc[proj.destination_code=='NIGHT', 'destination_name_en'] = "European researchers' Night"
        proj.loc[proj.destination_code=='RISE', 'destination_name_en'] = "Research and innovation staff exchange"

        # proj.loc[proj.programme_code=='MSCA', 'programme_name_en'] = 'Marie Skłodowska-Curie Actions (MSCA)'


        ### ajustement ERC
        proj.loc[proj.thema_code=='ERC', 'destination_code'] = proj.loc[proj.thema_code=='ERC'].action_code2.str.split('-').str[1]
        proj.loc[proj.destination_code=='POC-LS', 'destination_code'] = "POC"
        proj.loc[(proj.thema_code=='ERC')&(proj.destination_code.isnull()), 'destination_code'] = 'ERC-OTHER'
        proj.loc[(proj.action_code=='ERC'), 'action_code2'] = np.nan
        proj.loc[(proj.action_code=='ERC'), 'action_name2'] = np.nan

        # FET
        proj.loc[proj.programme_code=='FET', 'thema_code'] = 'PATHFINDER'
        proj.loc[(proj.programme_code=='FET')&(proj.action_code=='SGA'), 'destination_code'] = proj.loc[(proj.programme_code=='FET')&(proj.action_code=='SGA')].topic_code.str.split('-').str[2].str.upper()
        proj.loc[(proj.programme_code=='FET')&(proj.destination_code.isnull()), 'destination_code'] = proj.loc[(proj.programme_code=='FET')&(proj.destination_code.isnull())].topic_code.str.split('-').str[0].str.upper()
        proj.loc[(proj.programme_code=='FET')&(proj.topic_code.str.contains('BAT-')), 'destination_code'] = 'BATTERY'

        proj.loc[proj.programme_code=='FET', 'thema_name_en'] = np.nan

        proj.loc[(proj.call_id.str.contains("FETOPEN-2018-2019-2020"))|(proj.topic_code.str.contains("FETPROACT-EIC")), 'destination_next_fp'] = 'ACCELERATOR'

        # SMEInst
        proj.loc[(proj.topic_code.str.contains('EIC-SMEInst')), 'destination_next_fp'] = 'ACCELERATOR'

        proj.loc[proj.destination_next_fp=='ACCELERATOR', 'programme_next_fp'] = 'EIC'

        proj.loc[(proj.programme_code=='SME')&(proj.thema_code!='JU-JTI'), 'thema_name_en'] = np.nan

        # INFRA
        proj.loc[proj.programme_code=='INFRA', 'thema_code'] = proj.programme_code
        proj.loc[proj.programme_code=='INFRA', 'destination_code'] = proj.loc[proj.programme_code=='INFRA'].topic_code.str.split('-').str[0]
        proj.loc[(proj.programme_code=='INFRA')&(proj.destination_code=='LC'), 'destination_code'] = 'GREEN-DEAL'
        proj.loc[proj.destination_code.isin(['LC','IBA','SGA']), 'destination_code'] = np.nan

        # EIT
        proj.loc[proj.action_code=='KICS', 'pilier_name_en'] = 'Innovative Europe'
        proj.loc[proj.action_code=='KICS', 'programme_code'] = 'EIT'
        proj.loc[proj.action_code=='KICS', 'programme_name_en'] = 'The European Institute of Innovation and Technology (EIT)'

        # # WIDENING COST
        proj.loc[proj.programme_code.str.contains('TWINING|WIDESPREAD|NCPNET', na=False), 'thema_code'] = 'ACCESS'
        proj.loc[proj.programme_code.str.contains('ERA', na=False), 'thema_code'] = 'TALENTS'
        proj.loc[proj.programme_code.str.contains('INTNET', na=False), 'thema_code'] = 'COST'
        proj.loc[(proj.pilier_name_en=='Spreading excellence and widening participation')&(proj.programme_code!='ERA'), 'programme_code'] = 'Widening'
        proj.loc[proj.programme_code=='Widening', 'programme_name_en'] = 'Widening participation and spreading excellence'

        proj.loc[(proj.programme_code=='Widening')&(proj.thema_code.isnull()), 'thema_code'] = 'WIDENING-OTHER'

        proj.loc[(proj.programme_code.isin(['BIOTECH','ADVMANU','ADVMAT', 'NMP']))&(proj.thema_code.isnull()), 'thema_code'] = proj.loc[(proj.programme_code.isin(['BIOTECH','ADVMANU','ADVMAT', 'NMP']))&(proj.thema_code.isnull())].programme_code
        proj.loc[(proj.programme_code.isin(['BIOTECH','ADVMANU','ADVMAT', 'NMP']))&(proj.thema_name_en.isnull()), 'thema_name_en'] = proj.loc[(proj.programme_code.isin(['BIOTECH','ADVMANU','ADVMAT', 'NMP']))&(proj.thema_name_en.isnull())].programme_name_en
        proj.loc[(proj.programme_code.isin(['BIOTECH','ADVMANU','ADVMAT', 'NMP'])), 'programme_code'] = 'NMBP'
        proj.loc[proj.programme_code=='NMBP', 'programme_name_en'] = 'Nanotechnologies, Advanced Materials, Advanced Manufacturing and Processing, and Biotechnology'

        dest = destination[['destination_code', 'destination_name_en']]
        proj = proj.merge(dest, how='left', on='destination_code', suffixes=('', '_x'))
        proj.loc[proj.destination_name_en.isnull(), 'destination_name_en'] = proj.loc[proj.destination_name_en.isnull()].destination_name_en_x

        proj = proj.merge(thema.loc[~thema.dest_h20.isnull(), ['thema_code', 'dest_h20']], how='left', left_on='thema_code', right_on='dest_h20', suffixes=['','_x'])
        proj.loc[~proj.thema_code_x.isnull(), 'thema_code'] = proj.thema_code_x
        proj.drop(columns=['thema_code_x','dest_h20'], inplace=True)
        proj = proj.merge(thema, how='left', on='thema_code', suffixes=['','_x'])
        proj.loc[~proj.thema_name_en_x.isnull(), 'thema_name_en'] = proj.thema_name_en_x

        proj.drop(columns=['thema_name_en_x','dest_h20', 'destination_name_en_x', 'thema_code_t', 'thema_name_en_t', 'new', 'old'], inplace=True)
        return proj
    
    def euro_partnerships(proj):
        from step5_frameworks.functions_shared import ju_jti_parterships, eranet_partnerships
        # proj.loc[proj.action_code=='Art185', 'euro_partnerships_type'] = 'Art-185'
        # proj.loc[(proj.thema_code=='JU-JTI')&(proj.euro_partnerships_type.isnull()), 'euro_partnerships_type'] = 'Art-187'
        # proj.loc[proj.thema_code=='JU-JTI', 'euro_partnerships_type_next_fp'] = 'JU-JTI'
        proj=ju_jti_parterships(proj, 'H20')
        proj.loc[proj.programme_code=='EIT', 'euro_partnerships_type'] = 'EIT KICs'
        proj.loc[proj.programme_code=='EIT', 'euro_partnerships_type_next_fp'] = 'EIT KICs'

        # proj.loc[proj.action_code=='ERA-NET-Cofund', 'euro_partnerships_type'] = 'ERA-NET-COFUND'
        # proj.loc[proj.action_code=='ERA-NET-Cofund', 'euro_partnerships_type_next_fp'] = 'co-funded'
        proj=eranet_partnerships(proj, 'H20')
        proj.loc[proj.acronym=='CoBioTech', 'euro_ps_name'] = 'ERA CoBioTech'
        
        proj.loc[(proj.topic_code.isin(['NFRP-2018-6', 'SC1-PM-05-2016', 'EURATOM', 'NFRP-07-2015']))&(proj.action_code=='COFUND'), 'euro_partnerships_type'] = 'EJP-COFUND'
        proj.loc[(proj.action_code=='COFUND')&(proj.acronym.str.contains('EJP')), 'euro_partnerships_type'] = 'EJP-COFUND'
        proj.loc[(proj.action_code=='COFUND')&(proj.acronym.str.contains('EJP')), 'euro_partnerships_type_next_fp'] = 'co-funded'

        proj.loc[proj.topic_name.str.contains('PPP'), 'euro_partnerships_type'] = 'cPPP'
        proj.loc[(proj.topic_code.str.contains('GV-', regex=True, na=False))&(proj.programme_code=='TPT')&(proj.euro_partnerships_type.isnull()), 'euro_partnerships_type'] = 'cPPP'
        proj.loc[(proj.topic_name.str.contains('photonics', case=False, na=False))&(proj.programme_code=='ICT')&(proj.euro_partnerships_type.isnull()), 'euro_partnerships_type'] = 'cPPP'
        robotics=["ict-27-2017", "ict-28-2017", "ict-25-2016", "ict-26-2016", "ict-24-2015", "ict-23-2014"]
        for i in robotics:        
            proj.loc[(proj.programme_code=='ICT')&(proj.topic_code.str.contains(r"^"+i, case=False, regex=True)), 'euro_partnerships_type'] = 'cPPP'

        proj.loc[proj.euro_partnerships_type.str.contains('cPPP', na=False), 'euro_partnerships_type_next_fp'] = 'co-programmed'

        proj.loc[proj.euro_partnerships_type=='EIT KICs', 'euro_ps_name'] = proj.loc[proj.euro_partnerships_type=='EIT KICs'].thema_name_en
        # proj.loc[proj.euro_partnerships_type=='ERA-NET-COFUND', 'euro_ps_name'] = proj.loc[proj.euro_partnerships_type=='ERA-NET-COFUND'].acronym
        
        # proj.loc[proj.euro_partnerships_type_next_fp=='JU-JTI', 'euro_ps_name'] = proj.loc[proj.euro_partnerships_type_next_fp=='JU-JTI'].destination_code
        proj.loc[(proj.euro_partnerships_type_next_fp=='co-funded')&(proj.euro_ps_name.isnull())&(proj.destination_code.isnull()), 'euro_ps_name'] = proj.loc[(proj.euro_partnerships_type_next_fp=='co-funded')&(proj.euro_ps_name.isnull())&(proj.destination_code.isnull())].acronym
        proj.loc[(proj.euro_partnerships_type_next_fp=='co-funded')&(proj.euro_ps_name.isnull())&(~proj.destination_code.isnull()), 'euro_ps_name'] = proj.loc[(proj.euro_partnerships_type_next_fp=='co-funded')&(proj.euro_ps_name.isnull())&(~proj.destination_code.isnull())].destination_code
        proj.loc[(proj.euro_partnerships_type=='cPPP')&(proj.call_id.str.contains('EEB|SPIRE|FOF|EE', regex=True, case=False, na=False)), 'euro_ps_name'] = proj.loc[(proj.euro_partnerships_type=='cPPP')&(proj.call_id.str.contains('EEB|SPIRE|FOF|EE', regex=True, case=False, na=False))].call_id.str.split('-').str[1]
        proj.loc[(proj.euro_partnerships_type=='cPPP')&(proj.euro_ps_name.isnull()), 'euro_ps_name'] = proj.loc[(proj.euro_partnerships_type=='cPPP')&(proj.euro_ps_name.isnull())].topic_name.str.extract(r"^(.+ PPP)", expand=False)
        proj.loc[(proj.euro_partnerships_type=='cPPP')&(proj.euro_ps_name=='EE'), 'euro_ps_name'] = proj.loc[(proj.euro_partnerships_type=='cPPP')&(proj.euro_ps_name=='EE')].topic_name.str.extract(r"^(?:\()(EeB|SPIRE|FoF)", expand=False)
        proj.loc[(proj.euro_partnerships_type=='cPPP')&(proj.topic_code.str.contains('GV', na=False)), 'euro_ps_name'] = 'EGVI'
        proj.loc[(proj.euro_partnerships_type=='cPPP')&(proj.euro_ps_name=='Big data PPP'), 'euro_ps_name'] = 'BDVA'
        proj.loc[(proj.euro_partnerships_type=='cPPP')&(proj.topic_name.str.contains('photonics', case=False, na=False))&(proj.programme_code=='ICT'), 'euro_ps_name'] = 'Photonics'
        proj.loc[(proj.euro_partnerships_type=='cPPP')&(proj.programme_code=='ICT')&(proj.euro_ps_name.isnull()), 'euro_ps_name'] = 'Robotics SPARC'
        return proj.assign(euro_partnerships_flag=np.where(proj.euro_partnerships_type.isnull(), False, True)) 


    def cPPP_destination_name(proj):
        mask=(proj.euro_partnerships_type=='cPPP')
        proj.loc[mask&(proj.euro_ps_name=='EGVI'), 'destination_name'] = 'European Green Vehicles Initiative'
        proj.loc[mask&(proj.euro_ps_name=='SPIRE'), 'destination_name'] = 'Sustainable Process Industry'
        proj.loc[mask&(proj.euro_ps_name=='FoF'), 'destination_name'] = 'Factories of the future'
        proj.loc[mask&(proj.euro_ps_name=='EeB'), 'destination_name'] = 'Energy-Efficient Buildings'
        proj.loc[mask&(proj.euro_ps_name=='5G PPP'), 'destination_name'] = 'Advanced 5G Network for the future'
        proj.loc[mask&(proj.euro_ps_name=='BDVA'), 'destination_name'] = 'Big Data Value Association '
        proj.loc[mask&(proj.euro_ps_name=='Cybersecurity PPP'), 'destination_name'] = 'Connected digital single market'
        proj.loc[mask&(proj.euro_ps_name=='Photonics'), 'destination_name'] = 'Photonics21 Association'
        proj.loc[mask&(proj.euro_ps_name=='Robotics SPARC'), 'destination_name'] = 'Robotics SPARC'
        return proj

    def proj_cleaning(proj):
        print("## PROJ cleaning")
        from functions_shared import website_to_clean
        for i in ['title','abstract', 'free_keywords', 'eic_panels', 'url_project']:
            proj[i]=proj[i].str.replace('\\n|\\t|\\r|\\s+', ' ', regex=True).str.strip()
            
        kw = proj[['project_id','stage','free_keywords']].drop_duplicates()
        kw = kw.assign(free_keywords = kw.free_keywords.str.split(';|,')).explode('free_keywords')
        kw['free_keywords'] = kw.free_keywords.str.replace('\\.+', '', regex=True)
        kw = kw.loc[kw.free_keywords.str.len()>3].drop_duplicates()
        kw.free_keywords = kw.free_keywords.groupby(level=0).apply(lambda x: '|'.join(x.str.strip().unique()))
        kw = kw.drop_duplicates()

        proj = proj.drop(columns='free_keywords').merge(kw, how='left', on=['project_id','stage']).drop_duplicates()    
            
        proj.loc[proj.url_project.str.contains('project/rcn', na=False), 'url_project']=np.nan

        proj.mask(proj=='', inplace=True)  
        for i,row in proj.iterrows():
            if row.loc['url_project'] is not None:
                proj.at[i, 'project_webpage'] = website_to_clean(row['url_project'])

        proj.mask(proj=='', inplace=True)  

        for d in ['call_deadline', 'signature_date',  'start_date', 'end_date', 'submission_date', 'ecorda_date']:
            proj[d] = proj[d].astype('datetime64[ns]')

        proj['proposal_expected_number'] = proj['proposal_expected_number'].astype('float')
        return proj


    def entities_cleaning(df, country_h20, p):
        print("## ENTITIES cleaning")
        from functions_shared import gps_col, num_to_string
        df = pd.DataFrame(df)
        df = gps_col(df)
        df = df.loc[~df.generalPic.isnull()]
        
        df = (df.merge(country_h20[['iso2', 'iso3', 'parent_iso3']], how='left', left_on='countryCode', right_on='iso2')
            .drop(columns='iso2')
            .rename(columns={'parent_iso3':'country_code', 'iso3': 'country_code_mapping'}))
        print(f"parent_iso missing : {df[df.country_code.isnull()].countryCode.unique()}")
        df.loc[df.country_code.isnull(), 'country_code'] = df.loc[df.country_code.isnull()].country_code_mapping 

        c = ['pic', 'generalPic']
        df[c] = df[c].map(num_to_string)
        print(f"- size entities {len(df)}")

        if len(df[df.generalState.isnull()])>0:
            print("- entities source generalState -> new state (processing into entities_single)")
        else:
            print("- ok entities source generalState not null")

        lien_genCalcPic = p[['generalPic_old', 'pic']].drop_duplicates()
        print(f"size part without country: {len(p[['generalPic_old', 'pic']].drop_duplicates())}\nsize part with country: {len(p[['generalPic_old', 'pic', 'countryCode']].drop_duplicates())}")
        df = lien_genCalcPic.merge(df, how='inner', left_on=['generalPic_old','pic'], right_on=['generalPic','pic']).drop_duplicates()
        return df

    proj = h20_topics(_proj, act, actions, destination, pilier_fr, thema)
    proj = euro_partnerships(proj)
    proj = cPPP_destination_name(proj)
    proj = proj_cleaning(proj)
    entities = entities_cleaning(entities, country_h20, part_init)

    def ref_select(FP):
        ref_source = ref_source_load('ref')
        # traitement ref select le FP, id non null ou/et ZONAGE non null
        ref, genPic_to_new = ref_source_2d_select(ref_source, FP)
        ror = pd.read_pickle(f"{PATH_REF}ror_df.pkl")
        paysage = pd.read_pickle(f"{PATH_REF}paysage_df.pkl")
        sirene = pd.read_pickle(f"{PATH_REF}sirene_df.pkl")
        ### si besoin de charger groupe
        groupe = pd.read_pickle(f"{PATH_REF}H20_groupe.pkl")
        return ref, genPic_to_new, ror, paysage, sirene, groupe
    ref, genPic_to_new, ror, paysage, sirene, groupe = ref_select('H20')

    print(f"- si ++id pour un generalPic: {ref[ref.id.str.contains(';', na=False)]}")
    ref = (ref.merge(country_h20[['iso3', 'parent_iso3']], how='left', left_on='country_code_mapping', right_on='iso3')
        .drop(columns='iso3')
        .rename(columns={'parent_iso3':'country_code'}))
    print(f"parent_iso missing : {ref[ref.country_code.isnull()].country_code_mapping.unique()}")
    ref.loc[ref.country_code.isnull(), 'country_code'] = ref.loc[ref.country_code.isnull()].country_code_mapping 


    ########################################################################
    p=part_init[['generalPic', 'country_code_mapping', 'country_code']].drop_duplicates()
    print(f"size de p: {len(p)}")
    p = p.merge(ref, how='left', on=['generalPic', 'country_code_mapping', 'country_code'], indicator=True).drop_duplicates()
    print(f"cols de p: {p.columns}") #168 978

    # p1 pic+ccm commun
    p1 = p.loc[p['_merge']=='both'].drop(columns=['_merge'])
    print(f"size p1 pic+cc: {len(p1)}")# 62 928


    p2 = (p.loc[p['_merge']=='left_only'].drop(columns=['_merge', 'id', 'ZONAGE', 'id_secondaire'])
        .merge(ref.drop(columns=['country_code_mapping']), 
                how='inner', left_on=['generalPic', 'country_code'], right_on=['generalPic', 'country_code']).drop_duplicates()
        )
    print(f"size p2 pic cc_parent: {len(p2)}")


    # acteurs sans identifiant dont le pic à plusieurs pays ou le pic certaines participations ont un identifiant et pas d'autres 
    p3 = (p.loc[p['_merge']=='left_only'].drop(columns=['_merge', 'country_code_mapping', 'id', 'ZONAGE'])
        .merge(ref, how='inner', on=['generalPic']).drop_duplicates())
    if not p3.empty:
        print(f"A faire si possible, vérifier pourquoi des participations avec pic identiques ont un id ou pas nb pic: {len(p3.generalPic.unique())}")

    p = pd.concat([p1,p2], ignore_index=True).drop_duplicates()
    print(f"size de new p: {len(p)}, cols: {p.columns}")

    part1 = part_init.merge(p, how='left', on=['generalPic', 'country_code_mapping', 'country_code'])
    print(f"size part1: {len(part1)}, part: {len(part_init)}")

    # gestion code nuts
    part1.loc[(part1.nutsCode.str.len()>2), 'nuts_code'] = part1.nutsCode
    part1 = (part1.merge(nuts, how='left', on='nuts_code')
                .drop_duplicates()
                .rename(columns={'nuts_code':'participation_nuts'}))
    print(f"size participation after add nuts: {len(part1)}, sans nuts name: {len(part1.loc[(~part1.participation_nuts.isnull())&(part1.region_1_name.isnull())])}")


    ### entities
    entities_tmp = part1.loc[~part1.id.isnull(), ['generalPic','id','country_code_mapping']].drop_duplicates()
    print(f"- size entities {len(entities_tmp)}")
    if any(entities_tmp.id.str.contains(';')):
        entities_tmp = entities_tmp.assign(id_extend=entities_tmp.id.str.split(';')).explode('id_extend')
        ent_size_to_keep = len(entities_tmp)
        print(f"1- size ent si multi id -> ent_size_to_keep = {ent_size_to_keep}\n{entities_tmp.columns}")

    entities_tmp = merge_ror(entities_tmp, ror)
    print(f"size entities_tmp after add ror_info: {len(entities_tmp)}, entities_size_to_keep: {ent_size_to_keep}")

    # PAYSAGE
    ### si besoin de charger paysage pickle
    paysage_category = pd.read_pickle(f"{PATH_SOURCE}paysage_category.pkl")
    cat_filter = category_paysage(paysage_category)
    entities_tmp = merge_paysage(entities_tmp, paysage, cat_filter)

    # SIRENE
    ### si besoin de charger paysage pickle
    entities_tmp = merge_sirene(entities_tmp, sirene)
    entities_tmp['nb']=entities_tmp.groupby(['generalPic', 'id_extend', 'country_code_mapping'])['entities_id'].transform('count')
    if any(entities_tmp['nb']>1):
        print(f"doublons: {entities_tmp.loc[entities_tmp['nb']>1, ['generalPic', 'id_extend', 'country_code_mapping', 'entities_id', 'nb']]}")
        entities_tmp=entities_tmp.loc[~entities_tmp.entities_id.isin(['889664413', '808994164'])]

    entities_tmp.loc[(~entities_tmp.id.isnull())&(entities_tmp.entities_id.isnull()), 'entities_id'] = entities_tmp.id
    entities_tmp['siren']=entities_tmp.loc[entities_tmp.entities_id.str.contains('^[0-9]{9}$|^[0-9]{14}$', na=False)].entities_id.str[:9]
    entities_tmp.loc[entities_tmp.siren.isnull(), 'siren']=entities_tmp.paysage_siren

    #groupe entreprises
    # recuperation tous les siren pour lien avec groupe -> creation var SIREN 
    entities_tmp.loc[~entities_tmp.siren.isnull(), "siren"] = entities_tmp.loc[~entities_tmp.siren.isnull(), "siren"].str.split().apply(set).str.join(";")

    if any(entities_tmp.siren.str.contains(';', na=False)):
        print(f"ATTENTION faire code pour traiter deux siren différents -> ce qui serait bizarre qu'il y ait 2 siren\n{entities_tmp[entities_tmp.siren.str.contains(';', na=False)]}")
    # else:
    print(f"taille de entities_tmp avant groupe:{len(entities_tmp)}")
    entities_tmp=entities_tmp.merge(groupe, how='left', on='siren')

    print(f"taille de entities_tmp après groupe {len(entities_tmp)}")
    entities_tmp = entities_tmp.merge(get_source_ID(entities_tmp, 'entities_id'), how='left', on='entities_id')

    # traitement catégorie
    entities_tmp = category_woven(entities_tmp, sirene)
    entities_tmp = category_agreg(entities_tmp)
    entities_tmp = mires(entities_tmp)

    print(f"size part1 avant: {len(part1)}")
    part_tmp = part1.merge(genPic_to_new, how='left', on=['generalPic', 'country_code_mapping'])
    part_tmp = part_tmp.rename(columns={'generalPic':'pic_old', 'pic_new':'generalPic'})
    part_tmp.loc[part_tmp.generalPic.isnull(), 'generalPic'] = part_tmp.loc[part_tmp.generalPic.isnull(), 'pic_old']
    part_tmp = part_tmp.merge(entities_tmp.drop(columns='id'), how='left', on=['generalPic', 'country_code_mapping'])
    print(f"size part1 -> part_tmp: {len(part_tmp)}\n{part_tmp.columns}")

    print(len(part_tmp[(part_tmp.entities_name.isnull())]))
    part2=part_tmp.loc[(part_tmp.entities_name.isnull()), ['generalPic','entities_id', 'country_code_mapping', 'source_id']]
    part2.loc[part2.entities_id.str.contains('-', na=False), 'pic_d'] = part2.loc[part2.entities_id.str.contains('-', na=False)].entities_id.str.split('-').str[0]
    part2.loc[part2.pic_d.isnull(), 'pic_d'] = part2.loc[part2.pic_d.isnull()].generalPic

    part2 = part2.drop_duplicates()
    print(f"size part2: {len(part2)}, nb unique pic_d: {part2.pic_d.nunique()}")
    part2 = (part2.merge(entities, how='inner', left_on='pic_d', right_on='generalPic')[
                ['pic_d','entities_id','legalName', 'businessName', 'legalEntityTypeCode', 'generalState']]
            .rename(columns={'businessName':'shortName'})
            .drop_duplicates()
            )

    gen_state=['VALIDATED', 'DECLARED', 'DEPRECATED', 'SLEEPING', 'SUSPENDED', 'BLOCKED']
    part2=part2.groupby(['pic_d']).apply(lambda x: x.sort_values('generalState', key=lambda col: pd.Categorical(col, categories=gen_state, ordered=True)), include_groups=True).reset_index(drop=True)
    part2=part2.groupby(['pic_d']).head(1).drop(columns='generalState')
    print(f"size part2: {len(part2)}, nb unique pic_d: {part2.pic_d.nunique()}")

    part3=(part_tmp.loc[(~part_tmp.generalPic.isin(part2.pic_d.unique()))&(part_tmp.entities_name.isnull())]
        .sort_values(['generalPic','legalName', 'shortName'], ascending=False))
    print(part3.generalPic.nunique())

    part3=(part3.groupby(['generalPic', 'country_code_mapping'])
        .first().reset_index()[['generalPic', 'country_code_mapping', 'legalName', 'shortName', 'legalEntityTypeCode']]
        .reset_index(drop=True)
        .drop_duplicates()
        )
    print(part3.generalPic.nunique())

    part_tmp = part_tmp.merge(part2, how='left', left_on='generalPic', right_on='pic_d', suffixes=['', '_x'])
    part_tmp.loc[~part_tmp.legalName_x.isnull(), 'legalName'] = part_tmp.legalName_x
    part_tmp.loc[~part_tmp.shortName_x.isnull(), 'shortName'] = part_tmp.shortName_x
    part_tmp.loc[~part_tmp.legalEntityTypeCode_x.isnull(), 'legalEntityTypeCode'] = part_tmp.legalEntityTypeCode_x
    print(f"size part_tmp after merge part2: {len(part_tmp)}")

    part_tmp = part_tmp.merge(part3, how='left', on=['generalPic', 'country_code_mapping'], suffixes=['', '_y'])
    part_tmp.loc[~part_tmp.legalName_y.isnull(), 'legalName'] = part_tmp.legalName_y
    part_tmp.loc[~part_tmp.shortName_y.isnull(), 'shortName'] = part_tmp.shortName_y
    part_tmp.loc[~part_tmp.legalEntityTypeCode_y.isnull(), 'legalEntityTypeCode'] = part_tmp.legalEntityTypeCode_y
    part_tmp.drop(part_tmp.columns[part_tmp.columns.str.endswith(('_x','_y'))], axis=1, inplace=True)
    print(f"size part_tmp after merge part2: {len(part_tmp)}")

    liste=['legalName', 'shortName']
    for i in liste:
        part_tmp[i] = part_tmp[i].apply(lambda x: x.capitalize().strip() if isinstance(x, str) else x)

    part_tmp.loc[part_tmp.entities_name.isnull(), 'entities_name'] = part_tmp.legalName
    part_tmp.loc[part_tmp.entities_acronym.isnull(), 'entities_acronym'] = part_tmp.shortName
    part_tmp.loc[part_tmp.entities_id.isnull(), 'entities_id'] = "pic"+part_tmp.generalPic.map(str)

    part_tmp.rename(columns={'legalName':'entities_name_source',
                            'shortName':'entities_acronym_source'}, inplace=True)

    for i in ['entities_acronym', 'entities_name','entities_acronym_source', 'entities_name_source']:
        part_tmp[i] = part_tmp[i].str.replace('\\n|\\t|\\r|\\s+', ' ', regex=True).str.strip()
    print(f"size part_tmp after clean string: {len(part_tmp)}")

    ##########################################################

    # create calculated_fund and coordination_number
    part_tmp = (part_tmp
                .assign(calculated_fund=np.where(part_tmp.stage=='successful', part_tmp['subv_net'], part_tmp['requestedGrant']), 
                        coordination_number=np.where(part_tmp.role=='coordinator', 1, 0)))


    #############################################################
    ### ERC
    
    proj_erc=proj.loc[proj.action_id=='ERC', ['project_id', 'destination_code']].drop_duplicates()
    part_tmp=part_tmp.merge(proj_erc, how='left', on='project_id', indicator=True)
    part_tmp.loc[part_tmp._merge=='both', 'fund_ent_erc'] = part_tmp.loc[part_tmp._merge=='both'].calculated_fund
    
    # traitement erc ROLE
    part_tmp['erc_role'] = 'other'
    mask=(~part_tmp.destination_code.isnull())
    part_tmp.loc[mask&(part_tmp.stage=='evaluated')&(part_tmp.destination_code=='SyG')&((part_tmp.participates_as=='host')|(part_tmp.role=='coordinator')), 'erc_role'] = 'PI'
    part_tmp.loc[mask&(part_tmp.stage=='successful')&(part_tmp.destination_code=='SyG')&(part_tmp.participates_as=='beneficiary')&(pd.to_numeric(part_tmp.orderNumber, errors='coerce')<5.), 'erc_role'] = 'PI'
    part_tmp.loc[mask&(part_tmp.role=='coordinator')&(part_tmp.destination_code!='SyG'), 'erc_role'] = 'PI'
    part_tmp.loc[mask&(part_tmp.destination_code=='SyG')&(part_tmp.role=='coordinator'), 'role'] = 'CO-PI'
    part_tmp.loc[mask&(part_tmp.erc_role=='PI')&(part_tmp.role!='CO-PI'), 'role'] = 'PI'
    
    # traitement subv pour ERC
        #calcul budget ERC
    pt = part_tmp.loc[(part_tmp._merge=='both')&(part_tmp.destination_code!='SyG')]
    pt['calculated_fund'] = np.where(pt.stage=='successful', pt['subv'], pt['requestedGrant'])
    spt = pt.loc[pt.stage=='evaluated', ['project_id', 'requestedGrant']].groupby(['project_id'])['requestedGrant'].sum().reset_index()
    pt = pt.merge(spt, how='left', on='project_id', suffixes=('', '_y'))
    pt.loc[pt.stage=='evaluated', 'calculated_fund'] = pt.loc[pt.stage=='evaluated'].requestedGrant_y
    pt.loc[pt.erc_role!='PI', 'calculated_fund'] = 0

    from functions_shared import work_csv
    work_csv(pt, 'pt_20')
    ############################################

    part_tmp = pd.concat([part_tmp[~part_tmp.project_id.isin(pt.project_id.unique())], pt], ignore_index=True)
    print(f"size part_tmp after concat with erc: {len(part_tmp)}")

    part_tmp.drop(columns=['destination_code','requestedGrant_y', '_merge'], inplace=True)

    part_tmp = part_tmp.assign(number_involved=1)
    part_tmp['nb'] = part_tmp.id.str.split(';').str.len()
    for i in ['subv', 'subv_net', 'requestedGrant', 'calculated_fund', 'fund_ent_erc']:
        part_tmp[i] = np.where(part_tmp['nb']>1, part_tmp[i]/part_tmp['nb'], part_tmp[i])
    print(f"involved successful:{'{:,.1f}'.format(part_tmp.loc[(part_tmp.stage=='successful'), 'number_involved'].sum())}\nsubv_net_laureat:{'{:,.1f}'.format(part_tmp.loc[(part_tmp.stage=='successful'), 'subv_net'].sum())}\nsubv_laureat:{'{:,.1f}'.format(part_tmp.loc[(part_tmp.stage=='successful'), 'subv'].sum())}\nsubv_prop:{'{:,.1f}'.format(part_tmp.loc[(part_tmp.stage=='evaluated'), 'requestedGrant'].sum())}")


    proj_no_coord = proj[(proj.thema_code.isin(['ACCELERATOR','COST']))|(proj.destination_code.str.startswith('IF'))|(proj.action_code3.str.contains('SNLS', na=False))|(proj.thema_code=='ERC')].project_id.to_list()

    part_tmp.loc[part_tmp.project_id.isin(proj_no_coord), 'coordination_number'] = 0
    part_tmp = part_tmp.assign(with_coord=True)
    part_tmp.loc[part_tmp.project_id.isin(proj_no_coord), 'with_coord'] = False

    part_tmp.rename(columns={'ZONAGE':'extra_joint_organization'}, inplace=True)
    part_tmp = part_tmp.map(lambda x: x.strip() if isinstance(x, str) else x)

    part_tmp = part_tmp.assign(is_ejo=np.where(part_tmp.extra_joint_organization.isnull(), 'Sans', 'Avec'))

    # merge cordis type
    part_tmp.loc[part_tmp.legalEntityTypeCode.isnull(), 'legalEntityTypeCode'] = np.nan
    part_tmp = cordis_type(part_tmp)
    print(f"size part_tmp after clean codis legal type: {len(part_tmp)}")

    # merge countries 
    if any(part_tmp.country_code_mapping.isnull()):
        print(f"ATTENTION ! country_code_mapping null: {part_tmp[part_tmp.country_code_mapping.isnull()].countryCode.unique()}")
    else:
        part_tmp = (part_tmp
                    .merge(countries[['countryCode_iso3', 'country_name_en']]
                           .rename(columns={'countryCode_iso3':'country_code_mapping', 'country_name_en': 'country_name_mapping'}), 
                           how='left', on='country_code_mapping')
                    .drop_duplicates())
        print(f"size part_tmp avant: {len(part_tmp)}")

    if any(part_tmp.country_code.isnull()):
        print(f"ATTENTION ! country_code null: {part_tmp[part_tmp.country_code.isnull()].country_code_mapping.unique()}")
    else:
        cc=(countries[['countryCode_iso3', 'country_name_en',
        'country_association_code_2020', 'country_association_name_2020_en', 'country_group_association_code_2020',
        'country_group_association_name_2020_en', 'country_group_association_name_2020_fr', 'country_name_fr', 'article1',
        'article2']]
        .drop_duplicates()
        .rename(columns={'countryCode_iso3': 'country_code',
                            'country_association_code_2020':'country_association_code',
                            'country_association_name_2020_en':'country_association_name_en', 
                            'country_group_association_code_2020':'country_group_association_code',
                            'country_group_association_name_2020_en':'country_group_association_name_en',
                            'country_group_association_name_2020_fr':'country_group_association_name_fr'}))
        
        undef=pd.DataFrame(json.load(open('data_files/countries_undef.json', 'r+', encoding='UTF-8'))).drop(columns=['country_code_mapping', 'country_name_mapping'])
        cc=pd.concat([cc, undef], ignore_index=True)

        part_tmp = part_tmp.merge(cc, how='left', on='country_code')
        
    print(f"size part_tmp after merge countries: {len(part_tmp)}")

    # agregation des participants
    participation=part_tmp[
        ['project_id',  'stage', 'participates_as', 'role', 'erc_role', 'calculated_fund', 
         'fund_ent_erc', 'subv', 'subv_net', 'cordis_is_sme', 
        'requestedGrant', 'number_involved', 'coordination_number', 'with_coord', 'is_ejo',
        'cordis_type_entity_code','cordis_type_entity_name_fr', 'cordis_type_entity_acro',
        'cordis_type_entity_name_en', 'participation_nuts', 'region_1_name', 'region_2_name', 
        'regional_unit_name',
        'country_code_mapping', 'country_name_mapping', 'country_code', 'country_name_en', 
        'extra_joint_organization',
        'country_association_code','country_association_name_en', 'country_group_association_code',
        'country_group_association_name_en', 'country_group_association_name_fr', 'country_name_fr', 
        'article1','article2', 'entities_name', 'entities_acronym', 'entities_id', 'generalPic',
        'entities_name_source', 'entities_acronym_source','paysage_category_priority',
        'ror_category', 'paysage_category', 'paysage_category_id', 'category_agregation',
        'insee_cat_code', 'insee_cat_name', 'groupe_sector', 'source_id', 'entreprise_flag',
        'category_woven', 'operateur_lib', 'operateur_name', 'operateur_num',
        'groupe_name','groupe_acronym', 'groupe_id']]

    participation = participation.groupby(list(participation.columns.difference(['subv', 'subv_net', 'requestedGrant', 'number_involved', 'calculated_fund', 'fund_ent_erc'])), dropna=False, as_index=False).sum()
    print(f"involved successful:{'{:,.1f}'.format(participation.loc[(participation.stage=='successful'), 'number_involved'].sum())}\nsubv_laureat:{'{:,.1f}'.format(participation.loc[(participation.stage=='successful'), 'subv_net'].sum())}\nsubv_prop:{'{:,.1f}'.format(participation.loc[(participation.stage=='evaluated'), 'requestedGrant'].sum())}")
    participation.drop(columns=['requestedGrant', 'subv_net'], inplace=True)

    # proj pour synthese
    proj_s=proj.loc[~((proj.stage=='successful')&(proj.status_code=='REJECTED')),
        ['framework','project_id', 'call_id', 'panel_code', 'status_code', 'topic_code', 'stage', 'call_year', 'abstract', 'acronym',
        'pilier_name_en', 'pilier_name_fr','programme_name_en', 'thema_name_en', 'thema_code', 'programme_code', 'topic_name', 
        'panel_name', 'panel_regroupement_code', 'panel_regroupement_name', 'call_deadline', 'free_keywords',
        'destination_code','destination_name_en','action_code', 'action_code2', 'action_code3', 'action_name', 'action_name2', 'action_name3', 
        'euro_partnerships_type', 'euro_partnerships_type_next_fp', 'euro_ps_name', 'euro_partnerships_flag',
        'destination_next_fp', 'programme_next_fp', 'ecorda_date']]

    temp = proj_s.merge(participation, how='inner', on=['project_id', 'stage'])
    temp = temp.reindex(sorted(temp.columns), axis=1)
    print(f"involved successful:{'{:,.1f}'.format(temp.loc[(temp.stage=='successful'), 'number_involved'].sum())}\nsubv_laureat:{'{:,.1f}'.format(temp.loc[(temp.stage=='successful'), 'calculated_fund'].sum())}\nsubv_prop:{'{:,.1f}'.format(temp.loc[(temp.stage=='evaluated'), 'calculated_fund'].sum())}")
    print(len(temp))

    file_name = f"{PATH_CLEAN}H2020_data.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(temp, file)


    # sans cordis type
    cordis_type_null=[]
    for i in ['evaluated', 'successful']:
        nb_involved = temp.loc[temp.stage==i].number_involved.sum()
        nb_type_null = temp.loc[(temp.cordis_type_entity_code.isnull())&(temp.stage==i)].number_involved.sum()
        fund_type = temp.loc[temp.stage==i].calculated_fund.sum()
        part_involved_null = nb_type_null/nb_involved*100
        fund_type_null = temp.loc[(temp.cordis_type_entity_code.isnull())&(temp.stage==i)].calculated_fund.sum()
        part_fund_null = fund_type_null/fund_type*100
        d = {'framework': 'H2020', 'stage': i, 'nb_involved': nb_involved, 'nb_type_null':nb_type_null, 'fund_type':fund_type, 'fund_type_null':fund_type_null, 'part_fund_null':part_fund_null}
        cordis_type_null.append(d)
        print(f"{i} -> nb_involved {nb_involved}, nb_type_null {nb_type_null}, 'part_involved_null' {part_involved_null} fund_type {fund_type}, fund_type_null {fund_type_null}, {part_fund_null}")
    pd.DataFrame(cordis_type_null).to_csv(f"{PATH_CONNECT}cordis_type_null.csv", sep=';')

    def h20_proj_success(proj, participation):
        from config_path import PATH_CLEAN

        
        participation[['participation_nuts', 'region_1_name', 'region_2_name', 'regional_unit_name']] = participation[['participation_nuts', 'region_1_name', 'region_2_name', 'regional_unit_name']].fillna('')

        country=(participation
                .loc[participation.stage=='successful',['project_id','country_code','country_name_fr','country_code_mapping','country_name_mapping', 'participation_nuts', 'region_1_name', 'region_2_name', 'regional_unit_name']]
                .drop_duplicates()
                .groupby(['project_id'], as_index = False).agg(lambda x: ';'.join(map(str, filter(None, x))))
                .drop_duplicates())

        prop = (proj.loc[proj.stage=='evaluated', ['project_id', 'proposal_budget', 'proposal_requestedgrant', 'number_involved']]
            .rename(columns={'number_involved':'proposal_numberofapplicants'})
            .drop_duplicates())

        p = participation.loc[participation.stage=='successful', ['project_id', 'calculated_fund']].groupby('project_id', as_index=False).aggregate('sum').rename(columns={'calculated_fund':'project_eucontribution'})


        project = (proj.loc[(proj.stage=='successful')&(proj.status_code!='REJECTED'), ['project_id', 'acronym', 'title', 'abstract', 'call_id',
            'call_deadline', 'action_code', 'panel_code', 'duration', 'submission_date', 'topic_code', 'topic_name', 'status_code',
            'free_keywords', 'eic_panels', 'call_year', 'pilier_name_en', 'programme_name_en', 'thema_name_en', 'programme_code',
            'thema_code', 'panel_name', 'panel_regroupement_code', 'panel_regroupement_name', 'panel_description', 
            'destination_code','destination_name_en', 'euro_partnerships_type', 'euro_partnerships_type_next_fp', 'euro_ps_name', 'euro_partnerships_flag',
            'action_name', 'action_code2', 'action_name2', 'start_date','end_date', 'signature_date', 'project_webpage', 
            'number_involved','project_totalcost',  'proposal_expected_number', 'call_budget', 'framework', 'ecorda_date',
            'destination_next_fp', 'programme_next_fp']]
            .rename(columns={
                            'number_involved':'project_numberofparticipants',
                            'action_code2':'action_detail_code',
                            'action_name2':'action_detail_name'})
                .drop_duplicates())

        project = project.merge(p, how='left', on='project_id').merge(country, how='inner', on='project_id').merge(prop, how='left' , on='project_id')

        print(f"- size project lauréats: {len(project)}, {len(p)}, fund: {'{:,.1f}'.format(p['project_eucontribution'].sum())}")
        file_name = f"{PATH_CLEAN}H2020_successful_projects.pkl"
        with open(file_name, 'wb') as file:
            pd.to_pickle(project, file)
    h20_proj_success(proj, participation)

    # p=proj.loc[~((proj.stage=='successful')&(proj.status_code=='REJECTED')), ['stage', 'project_id']]
    # part_fr = part_tmp.loc[part_tmp.country_code=='FRA'].merge(p, how='inner', on=['stage', 'project_id']).project_id.unique()
    # print(len(part_fr))

    # collab_eval = collab_base(part_tmp.loc[part_tmp.project_id.isin(part_fr)], 'evaluated')
    # collab_signed = collab_base(part_tmp.loc[part_tmp.project_id.isin(part_fr)], 'successful')

    # col_eval = collab_cross(collab_eval)
    # col_signed = collab_cross(collab_signed)

    # collab=pd.concat([col_eval, col_signed], ignore_index=True)

    # # add countries infos
    # collab = (collab.merge(cc, how='left', on='country_code')
    #             .drop(columns=['country_association_name_en', 'country_group_association_name_fr',
    #             'country_association_code', 'country_group_association_name_en']))

    # countries_collab = cc.add_suffix('_collab')
    # collab = (collab.merge(countries_collab, how='left', on='country_code_collab')
    #             .drop(columns=['country_association_name_en_collab', 'country_group_association_name_fr_collab',
    #             'country_association_code_collab', 'country_group_association_name_en_collab']))


    # collab = collab.loc[collab.country_code=='FRA']
    # # add projects infos
    # proj_s=proj[['framework','project_id', 'call_id', 'panel_code', 'status_code', 'topic_code', 'stage', 'call_year', 'abstract',
    #             'pilier_name_en', 'pilier_name_fr','programme_name_en', 'thema_name_en', 'thema_code', 'programme_code',
    #             'panel_name', 'panel_regroupement_code', 'panel_regroupement_name', 'call_deadline', 'free_keywords',
    #             'destination_code','destination_name_en','destination_detail_code','destination_detail_name_en',
    #             'action_code', 'action_name',  'ecorda_date']]

    # collab=(collab
    #         .merge(proj_s, how='inner', on=['project_id','stage'])
    #         .drop_duplicates())

    # print(f"size collab {len(collab)}")

    # collab.to_pickle(f"{PATH_CLEAN}H2020_collab.pkl")