def persons_preparation(csv_date):

    import pandas as pd, numpy as np, warnings
    warnings.filterwarnings("ignore", "This pattern is interpreted as a regular expression, and has match groups")
    pd.options.mode.copy_on_write = True
    from constant_vars import FRAMEWORK
    from paths import PATH_SOURCE, PATH_CLEAN
    from config_url import grist_url
    from functions_shared import unzip_zip, my_country_code, country_iso_shift, prop_string
    from remote_process.grist import personsG

    ###############################
    participation = pd.read_pickle(f"{PATH_CLEAN}participation_current.pkl")
    entities = pd.read_pickle(f"{PATH_CLEAN}entities_info_current2.pkl")
    project = pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl")
    my_countries=my_country_code()

    print(f"size participation: {len(participation)}")
    ######################
    print(f"\n### IMPORT datasets")
    perso_part = unzip_zip(f'{PATH_SOURCE}{FRAMEWORK}/he_grants_ecorda_pd_{csv_date}.zip', "participant_persons.csv", 'utf-8')
    perso_part = (perso_part.loc[perso_part.FRAMEWORK=='HORIZON',
            ['PROJECT_NBR', 'GENERAL_PIC', 'PARTICIPANT_PIC', 'ROLE', 'FIRST_NAME',
            'LAST_NAME','GENDER', 'PHONE', 'EMAIL',
            'BIRTH_COUNTRY_CODE', 'NATIONALITY_COUNTRY_CODE', 'HOST_COUNTRY_CODE', 'SENDING_COUNTRY_CODE']]
                .rename(columns=str.lower)
                .rename(columns={'project_nbr':'project_id', 'general_pic':'generalPic', 'participant_pic':'pic'})
                .assign(stage='successful'))
    print(f"size perso_part import: {len(perso_part)}")

    ######################################
    perso_app = unzip_zip(f'{PATH_SOURCE}{FRAMEWORK}/he_proposals_ecorda_pd_{csv_date}.zip', "applicant_persons.csv", 'utf-8')

    perso_app = (perso_app.loc[perso_app.FRAMEWORK=='HORIZON',
        ['PROPOSAL_NBR', 'GENERAL_PIC', 'APPLICANT_PIC', 'ROLE', 'FIRST_NAME',
        'FAMILY_NAME', 'GENDER', 'PHONE', 'EMAIL',
        'RESEARCHER_ID', 'ORCID_ID', 'GOOGLE_SCHOLAR_ID','SCOPUS_AUTHOR_ID']]
                .rename(columns=str.lower)
                .rename(columns={'proposal_nbr':'project_id', 'general_pic':'generalPic', 'applicant_pic':'pic', 'family_name':'last_name'})
                .assign(stage='evaluated'))
    print(f"size perso_app import: {len(perso_app)}")

    ######################################
    print(f"\n### COUNTRY shift iso2 to iso3")
    for el in ['birth_country_code','nationality_country_code','host_country_code','sending_country_code']:
        perso_part = country_iso_shift(perso_part, el, iso2_to3=True)

    ####################################
    # print(f"\n### TITLE cleaning")
    # def title_clean(df):
    #     df.loc[~df['title'].isnull(), 'title_clean'] = df.loc[~df['title'].isnull(), 'title'].str.replace(r"[^\w\s]+", " ", regex=True)
    #     df.loc[~df['title_clean'].isnull(), 'title_clean'] = df.loc[~df['title_clean'].isnull(), 'title_clean'].str.replace(r"\s+", " ", regex=True).str.strip()
    #     df['title_clean'] = df['title_clean'].str.lower()

    #     titles = ['mrs', 'miss', 'mr', 'ms', 'ma', 'm', 'not appli']
    #     titles_sorted = sorted(titles, key=len, reverse=True)
    #     pattern = r'\b(?:' + '|'.join(titles_sorted) + r')\b'
    #     mask = df['col'].str.contains(pattern, regex=True, case=False, na=False)
    #     df.loc[mask, 'col'] = np.nan

    #     map = {"doctor": "dr", "professor": "prof", "prf": "prof", "pr": "prof"}
    #     df['col'] = df['col'].replace(map)


    #     df.mask(df == '', inplace=True)
    #     return df

    # perso_part = title_clean(perso_part)
    # perso_app = title_clean(perso_app)

    ###############################
    print(f"\n### NAME fix encoding issues")

    def fix_string(s):
        import re
        if not isinstance(s, str):
            return s
        
        prev = None
        while prev != s:          # on répète tant que ça change encore
            prev = s
            s = s.replace('\\005C', '\\')
            s = re.sub(r'\\([0-9A-Fa-f]{4})', lambda m: chr(int(m.group(1), 16)), s)
        return s

    cols = ['first_name', 'last_name']
    for c in cols:
        perso_part[c]=perso_part[c].apply(fix_string)


    ####################################
    print(f"\n### STRING cleaning")
    cols = ['role', 'first_name', 'last_name', 'gender']
    perso_part = prop_string(perso_part, cols)
    perso_app = prop_string(perso_app, cols)

    ##########
    print(f"\n### CONTACT create")
    def contact_name(df):
        for f in ['first_name', 'last_name']:
            df[f] = df[f].fillna('')
            df[f] = df[f].str.strip().str.replace(r"\s+", '-', regex=True)
            df[f] = df[f].str.strip().str.replace(r"-{2,}", '-', regex=True)

        df['contact'] = df.first_name.astype(str).str.lower() + ' ' + df.last_name.astype(str).str.lower()
        
        str_remove=['not applicable']
        df['contact'] = df['contact'].str.strip().str.replace(r"\^s+$", '-', regex=True)
        df = df.loc[~df.contact.isin(str_remove)]
        return df

    perso_app = contact_name(perso_app)
    perso_part = contact_name(perso_part)


    # ###########
    print(f"\n### PIC empty fix")
    # generalPic empty ; replace by pic or fill by generalPic participation
    def empty_pic(df, participation, stage):
        if any(df.generalPic.isnull()):
            print(f"1 - size rows with generelPic null for {stage}: {len(df[df.generalPic.isnull()])}")

            # gestion empty generalPic for principal investigator
            x=df.loc[(df.generalPic.isnull())&(df.role=='principal investigator')].project_id.unique()
            if x.size>0:
                y=participation.loc[(participation.project_id.isin(x))&(participation.stage==stage), ['project_id', 'generalPic']]
                df=df.merge(y, how='left', on=['project_id'], suffixes=('', '_y'))
                df.loc[(df.generalPic.isnull())&(~df.generalPic_y.isnull()), 'generalPic'] = df.loc[(df.generalPic.isnull())&(~df.generalPic_y.isnull()), 'generalPic_y'] 
                df.drop(columns='generalPic_y', inplace=True)
                print(f"2 - size rows with generelPic null for {stage}: {len(df[df.generalPic.isnull()])}")
        print(f"size df_{stage} after empty_pic: {len(df)}")
        return df

    perso_part = empty_pic(perso_part, participation, 'successful')
    perso_app = empty_pic(perso_app, participation, 'evaluated')

    ################
    print(f"\n### CALCULATION measures")
    def perso_measure(df):
        df['nb_pic_unique']=df.groupby(['project_id'])['generalPic'].transform('nunique') #combien de pics / projet
        df['nb_name_unique']=df.groupby(['project_id'])['last_name'].transform('nunique') #combien de pics / projet
        df['nb_row_by_pic']=df.groupby(['project_id', 'generalPic'])['last_name'].transform('count') #combien de lignes par pic
        df['nb_name_unique_by_pic']=df.groupby(['project_id', 'generalPic'])['last_name'].transform('nunique')
        df['nb_row_by_pic_name'] = df.groupby(['project_id', 'generalPic','last_name'])['last_name'].transform('count')
        df['nb_row_by_pic_name_unique'] = df.groupby(['project_id', 'generalPic','last_name'])['last_name'].transform('nunique')
        df['nb_pic_by_contact_unique'] = df.groupby(['project_id','contact'])['generalPic'].transform('count')
        
        # print(f"size df: {len(df)}\ncolumns:{df.columns}")
        print(f"size df: {len(df)}")
        return df

    perso_part = perso_measure(perso_part)
    perso_app = perso_measure(perso_app)

    ################
    print(f"\n### without PIC remove")
    def generaPic_remove(df):
        return df.loc[~((df.nb_pic_unique>0)&(df.generalPic.isnull()))]

    perso_part = generaPic_remove(perso_part)
    perso_app = generaPic_remove(perso_app)

    ##############################
    print(f"\n### NAME duplicated remove")
    def name_duplicated_remove(df):

        print(df.role.unique())
        keep_order=['principal investigator', 'fellow', 'main_contact']
        if len(df.role.unique()) > len(keep_order):
            print(f"2 - Attention ! un role nouveau dans perso -> {set(df.role.unique())-set(keep_order)}")

        tmp=pd.DataFrame()
        mask=[(df.nb_row_by_pic_name_unique==1)&(df.nb_row_by_pic_name>1)]
        for i in mask:
            x=df.loc[i]
            print(f"3 - size x before remove: {len(x)}")
            x=x.groupby(['project_id','generalPic', 'last_name']).apply(lambda i: i.sort_values('role', key=lambda col: pd.Categorical(col, categories=keep_order, ordered=True)), include_groups=True).reset_index(drop=True)
            for v in ['gender','phone','email','birth_country_code','nationality_country_code','host_country_code','sending_country_code']:
                if v in x.columns:
                    x[v]=x.groupby(['project_id', 'generalPic', 'last_name'])[v].bfill()
            x=x.groupby(['project_id', 'generalPic', 'last_name']).head(1)
            print(f"3 - size x after remove: {len(x)}")

            tmp=pd.concat([tmp, x], ignore_index=True)

        df=df.merge(tmp[['project_id', 'generalPic', 'last_name']].drop_duplicates(), how='outer', on=['project_id', 'generalPic', 'last_name'], indicator=True).query('_merge=="left_only"')
        df=pd.concat([df, tmp], ignore_index=True)

        if len(df)==0:
            print(f"ATTENTION table vide après traitement name_duplicated_remove")
        else:
            print(f"size après traitement name_duplicated_remove: {len(df)}")

        return df.drop(columns=['_merge'])

    perso_part = name_duplicated_remove(perso_part)
    perso_app = name_duplicated_remove(perso_app)

    # ####################################
    perso_part = perso_measure(perso_part)
    perso_app = perso_measure(perso_app)

    print(f"\n### PI duplicated")
    def PI_duplicated(df):
        if any(df.role=='principal investigator'):
            # select if same person and one PI in a single project 
            mask=(df.nb_pic_by_contact_unique>1)&(df.role=='principal investigator')
            pi=df.loc[mask, ['project_id', 'contact']].drop_duplicates().merge(df, how='inner')
            pi['role'] = 'principal investigator'
            for v in ['gender','birth_country_code','nationality_country_code','sending_country_code']:
                if v in df.columns:
                    pi=pi.sort_values(v)
                    pi[v]=pi.groupby(['project_id', 'contact'])[v].ffill()
            
            df=df.merge(pi[['project_id', 'generalPic', 'contact']].drop_duplicates(), how='outer', on=['project_id', 'generalPic', 'contact'], indicator=True).query('_merge=="left_only"')
            df=pd.concat([df, pi], ignore_index=True)
            print(f"-size df after cleaning pi_duplicated: {len(df)}")
            return df.drop(columns=['_merge'])
        
    perso_part=PI_duplicated(perso_part)

    #######################
    print(f"\n### PARTICIPATION+PERSO")
    def perso_participation(df, participation, project, entities, stage):
        
        df=df.loc[df.project_id.isin(participation[participation.stage==stage].project_id.unique())]
        df=df.merge(participation.loc[participation.stage==stage, ['project_id', 'generalPic', 'country_code', 'numero_national_de_structure']], how='outer', on=['project_id', 'generalPic'], indicator=True).query('_merge!="right_only"')
        df.loc[df._merge=='left_only', 'institution_shift'] = 'ended'

        if stage=='successful':
            df.loc[(df._merge=='both')&(df.host_country_code.isnull()), 'host_country_code'] = df.loc[(df._merge=='both')&(df.host_country_code.isnull()), 'country_code']

        df=df.merge(project.loc[project.stage==stage, ['project_id', 'call_year', 'thema_code', 'action_code', 'destination_code', 'panel_code', 'panel_regroupement_code']], how='inner', on=['project_id'])
        print(f"- size df after merge participation+project: {len(df)}")

        x=entities[['entities_id', 'entities_name', 'operateur_num', 'operateur_name', 'generalPic', 'country_code', 'country_code_source']].drop_duplicates()
        temp=df[~df.country_code.isnull()].merge(x, how='left', on=['generalPic', 'country_code'])
        if any(df.country_code.isnull()):
            temp2=df[df.country_code.isnull()].drop(columns='country_code').merge(x, how='left', on='generalPic')
            temp=pd.concat([temp, temp2], ignore_index=True)
            print(f"- size temp after merge entities with country_na: {len(temp)}")
        else:
            print(f"- size temp after merge entities: {len(temp)}")

        if len(temp)==0:
            print(f"ATTENTION table vide après lien avec participation")
        else:
            temp=temp.loc[~temp.country_code.isnull()]
            print(f"size temp final without country_code null: {len(temp)}\ncolumns:{temp.columns}")
        return temp.drop(columns=['_merge'])

    perso_part = perso_participation(perso_part, participation, project, entities, 'successful')
    perso_app = perso_participation(perso_app, participation, project, entities, 'evaluated')

    def iso2_add(df):
        df = (df.merge(my_countries[['iso2', 'iso3']].drop_duplicates(), how='left', left_on='country_code', right_on='iso3')
                .drop(columns='iso3')
                .rename(columns={'iso2':'country_code2'})
        )
        if any(df.country_code2.isnull()):
            print(f"country country_code2 missing for iso3 -> {df[df.country_code2.isnull()].country_code.unique()}")
        return df
    perso_part = iso2_add(perso_part)
    perso_app = iso2_add(perso_app)

    # ##################
    print(f"\n### PHONE cleaning")
    def phone_clean(df):
        y = df.loc[(df.country_code=='FRA')&(~df.phone.isnull()), ['phone']]
        y['tel_clean']=y.phone.str.replace(r"(^\++[0-9]{1,3}\s+)", '', regex=True)
        y['tel_clean']=y.tel_clean.str.replace(r"[^0-9]+", '', regex=True)
        y['tel_clean']=y.tel_clean.str.replace(r"^(33|033|0033)", '', regex=True).str.rjust(10, '0')
        y.loc[(y.tel_clean.str.len()>10)&(y.tel_clean.str[0:1]=='0'), 'tel_clean'] = y.tel_clean.str[0:10]
        y['tel_clean']=y.tel_clean.str.replace(r"^0+$", '', regex=True)
        # work_csv(y, 'tel_perso')
        return pd.concat([df, y[['tel_clean']]], axis=1)

    perso_part = phone_clean(perso_part)
    perso_app = phone_clean(perso_app)

    # #######################
    print(f"\n### MAIL cleaning")
    def mail_clean(df):
        mail_del=["gmail", "yahoo", "hotmail", "wanadoo", "aol", "free", "skynet", "outlook", "icloud", "googlemail"]

        df['domaine'] = df.email.str.split('@').str[1].str.split('.').str[:-1].fillna('').apply(' '.join)
        tmp = df.loc[~df.domaine.isnull(), ['domaine']]

        for el in mail_del:
            m = r"^"+el+r"($|\s)"
            tmp.loc[tmp['domaine'].str.contains(m, case=True, flags=0, na=None, regex=True) == True, 'domaine_email'] = ''
            tmp.loc[tmp['domaine_email'].isnull(), 'domaine_email'] = tmp['domaine']

        return pd.concat([df, tmp], axis=1).drop(columns='domaine')

    perso_app = mail_clean(perso_app)
    perso_part = mail_clean(perso_part)
    ##############

    def nationality_clean(df):
        filter_df=df.loc[(df.nationality_country_code.isnull()), ['generalPic', 'contact']].drop_duplicates().assign(fill_nat=True)
        df=df.merge(filter_df, how='left', on=['generalPic', 'contact'])
        df['rows_by_picContact']=df.groupby(['generalPic', 'contact'], dropna=False)['nationality_country_code'].transform('nunique')
        df.loc[(df.fill_nat==True)&(df.rows_by_picContact==1), 'nationality_country_code']=df.loc[(df.fill_nat==True)&(df.rows_by_picContact==1)].sort_values(['generalPic', 'contact', 'nationality_country_code']).groupby(['generalPic', 'contact'], group_keys=True)['nationality_country_code'].ffill()
        df.drop(columns='rows_by_picContact', inplace=True)
        return df
    
    perso_part = nationality_clean(perso_part)

    #################

    def vars_missing(perso_part, perso_app):
        # add orcid_id (perso_app) into perso_part
        print(f"\n### INFO missing between datasets")
        tmp=perso_app.loc[~perso_app.orcid_id.isnull(), ['project_id', 'contact', 'orcid_id']].drop_duplicates()
        perso_part=perso_part.merge(tmp, how='left', on=['project_id', 'contact']) 

        tmp=perso_part.loc[~perso_part.nationality_country_code.isnull(), ['project_id', 'contact', 'nationality_country_code']]
        perso_app=perso_app.merge(tmp, how='left', on=['project_id', 'contact'])
        return perso_part, perso_app
    
    perso_part, perso_app = vars_missing(perso_part, perso_app)
    ##################
    
    pp = pd.concat([perso_part.drop_duplicates(), perso_app.drop_duplicates()], ignore_index=True)
    # pp = pd.merge(pp, project[['project_id', 'destination_code', 'thema_code']], how='left', on=['project_id'])


    def gender_clean(df):
        replacements = {
            r'non[\s\-]?bin\w+': 'non binary',
            r'missing|andy': 'unknown'
        }

        df['gender'] = df['gender'].replace(replacements, regex=True)
        return df

    pp = gender_clean(pp)


    # fill missing value with other df part/app
    print(f"\n### GENDER missing")
    def gender_missing(pp):
        from step7_persons.gender_name import gender_by_first_name
        from remote_process.grist import add_records_to_grist
        # from remote_process.gender_determine import gender_by_first_name
        # from functions_shared import work_csv


        combined = pp[['project_id', 'contact', 'gender']].drop_duplicates()
        
        ref = (combined
                .groupby(['project_id', 'contact'])[['gender']]
                .first()  # prend la 1ère valeur NON-NULLE rencontrée dans l'ordre du df
                .reset_index()
            )
        
        print(f"- size pp before: {len(pp)}")
        pp = (pp.drop(columns=['gender']).drop_duplicates()
                .merge(ref, how='left', on=['project_id', 'contact'])
        )
        print(f"- size pp after merge gender clean: {len(pp)}")

        p = personsG['Gender_by_first_name'][['first_name', 'gender', 'drop_name']].drop_duplicates()
        
        def update_gender(df):
            df = pd.merge(df, p, how='left', on='first_name', suffixes=('', '_y'))
            df['gender'] = df['gender'].fillna(df['gender_y'])
            df.drop(columns='gender_y', inplace=True)
            return df 
        
        # Applique la fonction au DataFrame
        pp = update_gender(pp)

        l=list(set(list(pp.loc[(pp.gender.isnull())&(pp.drop_name.isnull())].first_name.unique())))
        # l=part.loc[(part.country_code=='FRA')&(part.gender.isnull())].first_name.unique()
        print(f"- size first_name list: {len(l)}")
        res=gender_by_first_name(l)

        if res:
            res = pd.DataFrame(res)[['first_name', 'gender', 'probability']].assign(drop_name=False, be_checked=1).drop_duplicates()
            print(f"- ATTENTION ! check {len(res)} first names in gender_part dataset in grist -> reload personsG and execute again persons script" )
            add_records_to_grist(res, grist_url, 'pcri', 'persons', 'gender_by_first_name')
            
        return pp

    pp = gender_missing(pp)

    #################
    def detect_nan_value_by_group(df, group_by_cols: list, var: str):
        return df.groupby(group_by_cols, dropna=False)[var].transform(
            lambda x: x.isna().any()
            )

    def nan_var_fill_by_group(df, group_by_cols: list, var: str):
        return df.groupby(group_by_cols, dropna=False)[var].transform(
            lambda x: x.ffill().bfill()
            )

    def most_common_value(df, group_by_cols: list, var: str):
        return df.groupby(group_by_cols, dropna=False)[var].transform(
            lambda x: x.mode().iloc[0] if not x.mode().empty else np.nan
            )


    def fill_var_by_group(df, var: str, cols_list: list):
        df['nb']=df.groupby(cols_list, dropna=False)[var].transform('nunique')
        value_nan = detect_nan_value_by_group(df, cols_list, var)
        value_common = most_common_value(df, cols_list, var)

        # 2. process for orcid_id number > 2 -> just one case 
        mask = (df['nb'] > 2) & value_nan
        df.loc[mask, var] = value_common[mask]
        df['nb']=df.groupby(cols_list, dropna=False)[var].transform('nunique')
        value_nan = detect_nan_value_by_group(df, cols_list, var)

        # 3. ++ rows by group with nan value and not nan ; using not nan to fill
        mask = value_nan
        df.loc[mask, var] = value_common[mask]
        df['nb']=df.groupby(cols_list, dropna=False)[var].transform('nunique')
        return df


    def fill_all_by_group(df):
        print("### var fillna")
        # fill orcid_id
        cols_list=['project_id', 'contact']
        df = fill_var_by_group(df, 'orcid_id', cols_list)

        cols_list=['generalPic', 'contact']
        df = fill_var_by_group(df, 'orcid_id', cols_list)

        # # fill title_clean
        # cols_list=['project_id', 'contact']
        # df = fill_var_by_group(df, 'title_clean', cols_list)

        # cols_list=['generalPic', 'contact']
        # df = fill_var_by_group(df, 'title_clean', cols_list)

        print(df.gender.value_counts(dropna=False))


        return df.drop(columns='nb')
    
    pp = fill_all_by_group(pp)
    print(f"- size pp after cleansing {len(pp)}")

    pp = pp.loc[pp['drop_name']!=True]
    cols_to_drop = ['drop_name', 'fill_nat', 'pic'] + [col for col in pp.columns if col.startswith('nb_')]
    pp = pp.drop(columns=cols_to_drop).drop_duplicates()

    pp = (pd.merge(pp, 
                   project[['project_id', 'stage']], 
                   how='inner', 
                   on=['project_id', 'stage'], 
                   indicator=True)
                   .query('_merge=="both"')
                   .drop(columns='_merge')
                   .drop_duplicates()
    )
    print(f"- size pp in project {len(pp)}")


    def check(df):
            
        required = {"project_id", "role", "stage"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Colonnes manquantes dans le CSV : {missing}")
    
        has_contact = "contact" in df.columns
    
        results = []
        contact_checks = []
    
        for project_id, group in df.groupby("project_id"):
            roles = set(group["role"])
            stages = set(group["stage"])
    
            for stage in stages:
                if stage == "evaluated":
                    ok = "fellow" in roles
                    expected_role = "fellow"
                elif stage == "successful":
                    ok = "principal investigator" in roles
                    expected_role = "principal investigator"
                else:
                    # stage inconnu : on ne vérifie rien mais on le signale
                    ok = None
                    expected_role = None
    
                results.append(
                    {
                        "project_id": project_id,
                        "stage": stage,
                        "expected_role": expected_role,
                        "roles_found": sorted(roles),
                        "ok": ok,
                    }
                )
    
            # --- Règle complémentaire : cas "evaluated" avec seulement main_contact ---
            if has_contact:
                evaluated_rows = group[group["stage"] == "evaluated"]
                evaluated_roles = set(evaluated_rows["role"])
    
                only_main_contact = (
                    not evaluated_rows.empty
                    and "main_contact" in evaluated_roles
                    and "fellow" not in evaluated_roles
                )
    
                if only_main_contact:
                    main_contact_names = set(
                        evaluated_rows.loc[
                            evaluated_rows["role"] == "main_contact", "contact"
                        ]
                    )
    
                    pi_rows = group[
                        (group["stage"] == "successful")
                        & (group["role"] == "principal investigator")
                    ]
                    pi_contact_names = set(pi_rows["contact"])
    
                    if not pi_rows.empty:
                        match = main_contact_names == pi_contact_names
                    else:
                        match = None  # pas de ligne successful/PI à comparer
    
                    contact_checks.append(
                        {
                            "project_id": project_id,
                            "evaluated_main_contact": sorted(main_contact_names),
                            "successful_pi_contact": sorted(pi_contact_names),
                            "contact_match": match,
                        }
                    )
    
        result_df = pd.DataFrame(results)
        contact_df = pd.DataFrame(contact_checks)
        return result_df, contact_df
    
    
    def build_filtered_df(df: pd.DataFrame, contact_col: str = None) -> pd.DataFrame:
        """
        Retourne le dataframe complet (toutes les lignes conservées) avec deux
        colonnes ajoutées :
    
        - 'keep' (bool)   : True si la ligne est jugée pertinente, False sinon
        - 'reason' (str)  : explication du tag
    
        Règles :
        - role in {'fellow', 'principal investigator'}  -> keep=True ('role_valide')
        - role == 'main_contact' avec stage == 'evaluated' et pas de fellow
        pour le projet -> keep=True ('evaluated_main_contact_seul')
        - role == 'main_contact' avec un fellow du même projet et même contact
        (entities_id par défaut, ou colonne 'contact' si présente)
        -> keep=True ('main_contact_associe_a_fellow')
        - sinon -> keep=False ('main_contact_non_justifie')
    
        contact_col: nom de la colonne à utiliser comme identifiant de contact.
                    Si None, utilise 'contact' si elle existe, sinon 'entities_id'.
        """
        if contact_col is None:
            contact_col = "contact" if "contact" in df.columns else "entities_id"
    
        df = df.copy()
        keep_mask = df["role"].isin(["fellow", "principal investigator"])
        reason = pd.Series("role_valide", index=df.index)
        reason[~keep_mask] = "main_contact_non_justifie"
    
        for project_id, group in df.groupby("project_id"):
            has_fellow = (group["role"] == "fellow").any()
            fellow_contacts = set(group.loc[group["role"] == "fellow", contact_col])
    
            main_contact_rows = group[group["role"] == "main_contact"]
            for idx, row in main_contact_rows.iterrows():
                # Cas 1 : evaluated + main_contact seul (pas de fellow du tout)
                if row["stage"] == "evaluated" and not has_fellow:
                    keep_mask.loc[idx] = True
                    reason.loc[idx] = "evaluated_main_contact_seul"
                # Cas 2 : main_contact + fellow, même contact -> on garde aussi
                elif has_fellow and row[contact_col] in fellow_contacts:
                    keep_mask.loc[idx] = True
                    reason.loc[idx] = "main_contact_associe_a_fellow"
    
        df["keep"] = keep_mask
        df["reason"] = reason
        return df.sort_values(["project_id", "stage", "role"]).reset_index(drop=True)
    
    
    def check_role_by_project(df):
        result_df, contact_df = check(df)
 
        # Lignes problématiques uniquement (ok == False)
        problems = result_df[result_df["ok"] == False]
    
        print(f"Total de (project_id, stage) vérifiés : {len(result_df)}")
        print(f"Nombre de cas non conformes (role manquant) : {len(problems)}")
    
        if not problems.empty:
            print("\n--- Cas non conformes (role manquant) ---")
            print(problems.to_string(index=False))
        else:
            print("\nAucune anomalie de role trouvée.")
    
        # Sauvegarde du détail complet et des anomalies
        result_df.to_csv("check_results_full.csv", index=False)
        problems.to_csv("check_results_problems.csv", index=False)
        print("\nRésultats complets   -> check_results_full.csv")
        print("Anomalies uniquement -> check_results_problems.csv")
    
        # --- Vérification des contacts (evaluated/main_contact vs successful/PI) ---
        if not contact_df.empty:
            contact_problems = contact_df[contact_df["contact_match"] == False]
            no_reference = contact_df[contact_df["contact_match"].isna()]
    
            print(f"\nProjets 'evaluated' avec seulement main_contact : {len(contact_df)}")
            print(f"  - contacts qui ne correspondent PAS : {len(contact_problems)}")
            print(f"  - aucune ligne successful/PI de référence : {len(no_reference)}")
    
            if not contact_problems.empty:
                print("\n--- Contacts non concordants ---")
                print(contact_problems.to_string(index=False))
    
            contact_df.to_csv("check_results_contacts.csv", index=False)
            print("\nDétail des vérifications de contact -> check_results_contacts.csv")
        else:
            print(
                "\n(Pas de colonne 'contact' dans le fichier, ou aucun cas "
                "'evaluated + main_contact seul' à vérifier.)"
            )
    
        # --- Construction du dataframe final taggué (toutes les lignes conservées) ---
        
        tagged_df = build_filtered_df(df)
        
        print(f"\nLignes d'origine       : {len(df)}")
        print(f"Lignes tagguées keep=True  : {tagged_df['keep'].sum()}")
        print(f"Lignes tagguées keep=False : {(~tagged_df['keep']).sum()}")
        print("\nRépartition par 'reason' :")
        print(tagged_df["reason"].value_counts().to_string())
        print("\nDataframe complet (taggué) -> erc_filtered.csv")
        return tagged_df

    erc = pp.loc[(pp.action_code=='ERC')&(erc.country_code=='FRA')].drop_duplicates()
    res = check_role_by_project(erc)
    res = res[['project_id', 'entities_id', 'entities_name', 
               'role', 'first_name', 'last_name',  
               'stage', 'contact',
                'country_code', 'numero_national_de_structure', 'institution_shift',
                'call_year', 'thema_code', 'action_code', 'destination_code',
                'panel_code', 'panel_regroupement_code', 
                'operateur_num', 'operateur_name', 'country_code_source',
                    'orcid_id', 'gender',
                'keep', 'reason']].drop_duplicates()

########################################################################

    print(f"\n### EXPORT final datasets")
    cols=['project_id', 'generalPic', 'role', 'first_name', 'last_name', 'contact', 'nationality_country_code',
          'gender', 'tel_clean', 'email', 'domaine_email', 'orcid_id',
          'birth_country_code', 'host_country_code', 'sending_country_code',
          'stage', 'country_code2', 'country_code', 'country_code_source',
          'institution_shift', 'entities_id', 'entities_name', 'operateur_num', 'operateur_name', 'numero_national_de_structure']

    (pp.loc[pp['stage']=='successful', cols]
        .drop(columns=['researcher_id', 'google_scholar_id', 'scopus_author_id'])
        .drop_duplicates()
        .to_pickle(f"{PATH_CLEAN}persons_part.pkl"))

    (pp[cols]
        .drop_duplicates()
        .to_pickle(f"{PATH_CLEAN}persons_all.pkl"))

