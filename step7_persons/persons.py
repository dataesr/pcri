def persons_preparation(csv_date):

    import pandas as pd
    pd.options.mode.copy_on_write = True
    from constant_vars import ZIPNAME, FRAMEWORK
    from config_path import PATH_SOURCE, PATH_CLEAN, PATH_ORG, PATH_WORK
    from functions_shared import unzip_zip


    ###############################
    participation = pd.read_pickle(f"{PATH_CLEAN}participation_current.pkl") 
    project = pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl") 

    print(f"size participation: {len(participation)}")
    ######################
    perso_part = unzip_zip(f'he_grants_ecorda_pd_{csv_date}.zip', f"{PATH_SOURCE}{FRAMEWORK}/", "participant_persons.csv", 'utf-8')
    perso_part = (perso_part.loc[perso_part.FRAMEWORK=='HORIZON',
            ['PROJECT_NBR', 'GENERAL_PIC',
       'PARTICIPANT_PIC', 'ROLE', 'FIRST_NAME',
       'LAST_NAME', 'TITLE', 'GENDER', 'PHONE', 'EMAIL',
       'BIRTH_COUNTRY_CODE', 'NATIONALITY_COUNTRY_CODE', 'HOST_COUNTRY_CODE',
       'SENDING_COUNTRY_CODE']]
                .rename(columns=str.lower)
                .rename(columns={'project_nbr':'project_id', 'general_pic':'generalPic', 'participant_pic':'pic'})
                .assign(stage='successful'))

    ######################################
        
    perso_app = unzip_zip(f'he_proposals_ecorda_pd_{csv_date}.zip', f"{PATH_SOURCE}{FRAMEWORK}/", "applicant_persons.csv", 'utf-8')

    perso_app = (perso_app.loc[perso_app.FRAMEWORK=='HORIZON',
    ['PROPOSAL_NBR', 'GENERAL_PIC', 'APPLICANT_PIC', 'ROLE', 'FIRST_NAME',
        'FAMILY_NAME', 'TITLE', 'GENDER', 'PHONE', 'EMAIL',
        'RESEARCHER_ID', 'ORCID_ID', 'GOOGLE_SCHOLAR_ID','SCOPUS_AUTHOR_ID']]
                .rename(columns=str.lower)
                .rename(columns={'proposal_nbr':'project_id', 'general_pic':'generalPic', 'applicant_pic':'pic', 'family_name':'last_name'})
                .assign(stage='evaluated'))

    #######################################
    def prop_contact(tab):
        from unidecode import unidecode
        cols = ['role', 'first_name', 'last_name','title', 'gender']
        tab[cols] = tab[cols].map(lambda s:s.casefold() if type(s) == str else s)
        for i in cols:
            tab[i] = tab[i].astype('str').apply(unidecode)
        return tab

    perso_app = prop_contact(perso_app)
    perso_part = prop_contact(perso_part)

    ###########
    def contact_name(df):
        for f in [ 'first_name', 'last_name']:
            df[f] = df[f].fillna('')
            df[f] = df[f].str.strip().str.replace(r"\s+", '-', regex=True)
            df[f] = df[f].str.strip().str.replace(r"-{2,}", '-', regex=True)

        df['contact'] = df.last_name.astype(str).str.lower() + ' ' + df.first_name.astype(str).str.lower()
        return df
    
    perso_app = contact_name(perso_app)
    perso_part = contact_name(perso_part)

    ###########
    # generalPic empty ; replace by pic or fill by generalPic participation
    def empty_pic(df, participation, stage):
        if any(df.generalPic.isnull()):
            print(f"1 - size rows with generelPic null for {stage}: {len(df[df.generalPic.isnull()])}")
            df.loc[df.generalPic.isnull(), 'generalPic'] = df.loc[df.generalPic.isnull(), 'pic']

            # gestion empty generalPic for principal investigator
            x=df.loc[(df.generalPic.isnull())&(df.role=='principal investigator')].project_id.unique()
            if x.size>0:
                y=participation.loc[(participation.project_id.isin(x))&(participation.stage==stage), ['project_id', 'generalPic']]
                df=df.merge(y, how='left', on=['project_id'], suffixes=('', '_y'))
                df.loc[(df.generalPic.isnull())&(~df.generalPic_y.isnull()), 'generalPic'] = df.loc[(df.generalPic.isnull())&(~df.generalPic_y.isnull()), 'generalPic_y'] 
                df.drop(columns='generalPic_y', inplace=True)
                print(f"2 - size rows with generelPic null for {stage}: {len(df[df.generalPic.isnull()])}")
        return df

    perso_part = empty_pic(perso_part, participation, 'successful')
    perso_app = empty_pic(perso_app, participation, 'evaluated')

    ################
    def perso_measure(df):
        df['nb_row_by_pic']=df.groupby(['project_id', 'generalPic'])['generalPic'].transform('count')
        df['nb_pic_unique']=df.groupby(['project_id'])['generalPic'].transform('nunique')
        df['nb_name']=df.groupby(['project_id'])['last_name'].transform('count')
        df['nb_row_by_name']=df.groupby(['project_id', 'last_name'])['last_name'].transform('count')
        df['nb_row_by_pic_name'] = df.groupby(['project_id', 'generalPic', 'last_name'])['last_name'].transform('count')
        df['nb_name_unique']=df.groupby(['project_id'])['last_name'].transform('nunique')
        print(f"size df: {len(df)}\ncolumns:{df.columns}")
        return df

    perso_part = perso_measure(perso_part)
    perso_app = perso_measure(perso_app)

    ################
    def generaPic_remove(df):
        return df.loc[~((df.nb_pic_unique>0)&(df.generalPic.isnull()))]
    
    perso_part = generaPic_remove(perso_part)
    perso_app = generaPic_remove(perso_app)


    ##############################
    def name_duplicated_remove(df):
        #### cleaning name duplicated by project 
        ## if by project single name but several rows
        # x[x.project_id=='101039481']

        print(x.role.unique())
        keep_order=['principal investigator', 'fellow', 'main_contact']
        if len(df.role.unique()) > len(keep_order):
            print(f"2 - Attention ! un role nouveau dans perso -> {set(df.role.unique())-set(keep_order)}")

        tmp=pd.DataFrame()
        mask=[(df.nb_name_unique==1)&(df.nb_name>1)&(df.nb_pic_unique==1), (df.nb_name_unique==1)&(df.nb_name>1)&(df.nb_pic_unique>1)&(df.nb_row_by_pic>1)]
        for i in mask:
            x=df.loc[i]
            print(f"3 - size x before remove: {len(x)}")
            x=x.groupby(['project_id', 'last_name']).apply(lambda i: i.sort_values('role', key=lambda col: pd.Categorical(col, categories=keep_order, ordered=True)), include_groups=True).reset_index(drop=True)
            x=x.groupby(['project_id', 'last_name']).head(1)
            print(f"3 - size x after remove: {len(x)}")

            tmp=pd.concat([tmp, x], ignore_index=True)
        return tmp

    tmp1 = name_duplicated_remove(perso_part)
    # perso_app = name_duplicated_remove(perso_app)








    # ####################################

    # def perso_participation(df, participation, project, stage):
    #     df = (df.merge(participation.loc[participation.stage==stage, ['project_id', 'generalPic', 'country_code']], how='inner', on=['project_id', 'generalPic'])
    #                 .merge(project.loc[participation.stage==stage, ['project_id', 'call_year', 'thema_name_en', 'destination_name_en']], how='inner', on=['project_id'])
    #     )
    #     print(f"size app lien avec participation clean : {len(df)}\ncolumns:{df.columns}")
    #     return df
    
    # perso_app = perso_participation(perso_app, participation, project, 'evaluated')
    # perso_part = perso_participation(perso_part, participation, project, 'successful')


    # ##################
    # def phone_clean(df):
    #     y = df.loc[(df.country_code=='FRA')&(~df.phone.isnull()), ['phone']]
    #     y['tel_clean']=y.phone.str.replace(r"(^\++[0-9]{1,3}\s+)", '', regex=True)
    #     y['tel_clean']=y.tel_clean.str.replace(r"[^0-9]+", '', regex=True)
    #     y['tel_clean']=y.tel_clean.str.replace(r"^(33|033|0033)", '', regex=True).str.rjust(10, '0')
    #     y.loc[(y.tel_clean.str.len()>10)&(y.tel_clean.str[0:1]=='0'), 'tel_clean'] = y.tel_clean.str[0:10]
    #     y['tel_clean']=y.tel_clean.str.replace(r"^0+$", '', regex=True)
    #     # work_csv(y, 'tel_perso')
    #     return pd.concat([df, y[['tel_clean']]], axis=1)

    # perso_app = phone_clean(perso_app)
    # perso_part = phone_clean(perso_part)

    # #######################
    # def mail_clean(df):
    #     mail_del=["gmail", "yahoo", "hotmail", "wanadoo", "aol", "free", "skynet", "outlook", "icloud", "googlemail"]

    #     df['domaine'] = df.email.str.split('@').str[1].str.split('.').str[:-1].fillna('').apply(' '.join)
    #     tmp = df.loc[~df.domaine.isnull(), ['domaine']]

    #     for el in mail_del:
    #         m = r"^"+el+r"($|\s)"
    #         tmp.loc[tmp['domaine'].str.contains(m, case=True, flags=0, na=None, regex=True) == True, 'domaine_email'] = ''
    #         tmp.loc[tmp['domaine_email'].isnull(), 'domaine_email'] = tmp['domaine']

    #     return pd.concat([df, tmp], axis=1).drop(columns='domaine')
    
    # perso_app = mail_clean(perso_app)
    # perso_part = mail_clean(perso_part)

    ###############


    # perso_app.to_pickle(f"{PATH_CLEAN}perso_app.pkl")