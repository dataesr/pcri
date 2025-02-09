def persons_preparation(csv_date):

    import pandas as pd, numpy as np, warnings
    warnings.filterwarnings("ignore", "This pattern is interpreted as a regular expression, and has match groups")
    pd.options.mode.copy_on_write = True
    from constant_vars import FRAMEWORK
    from config_path import PATH_SOURCE, PATH_CLEAN
    from functions_shared import unzip_zip

    ###############################
    participation = pd.read_pickle(f"{PATH_CLEAN}participation_current.pkl") 
    project = pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl") 
    countries = pd.read_pickle(f"{PATH_CLEAN}country_current.pkl")

    print(f"size participation: {len(participation)}")
    ######################
    perso_part = unzip_zip(f'he_grants_ecorda_pd_{csv_date}.zip', f"{PATH_SOURCE}{FRAMEWORK}/", "participant_persons.csv", 'utf-8')
    perso_part = (perso_part.loc[perso_part.FRAMEWORK=='HORIZON',
            ['PROJECT_NBR', 'GENERAL_PIC', 'PARTICIPANT_PIC', 'ROLE', 'FIRST_NAME',
            'LAST_NAME', 'TITLE', 'GENDER', 'PHONE', 'EMAIL',
            'BIRTH_COUNTRY_CODE', 'NATIONALITY_COUNTRY_CODE', 'HOST_COUNTRY_CODE', 'SENDING_COUNTRY_CODE']]
                .rename(columns=str.lower)
                .rename(columns={'project_nbr':'project_id', 'general_pic':'generalPic', 'participant_pic':'pic'})
                .assign(stage='successful'))
    print(f"size perso_part import: {len(perso_part)}")

    ######################################
    perso_app = unzip_zip(f'he_proposals_ecorda_pd_{csv_date}.zip', f"{PATH_SOURCE}{FRAMEWORK}/", "applicant_persons.csv", 'utf-8')

    perso_app = (perso_app.loc[perso_app.FRAMEWORK=='HORIZON',
        ['PROPOSAL_NBR', 'GENERAL_PIC', 'APPLICANT_PIC', 'ROLE', 'FIRST_NAME',
        'FAMILY_NAME', 'TITLE', 'GENDER', 'PHONE', 'EMAIL',
        'RESEARCHER_ID', 'ORCID_ID', 'GOOGLE_SCHOLAR_ID','SCOPUS_AUTHOR_ID']]
                .rename(columns=str.lower)
                .rename(columns={'proposal_nbr':'project_id', 'general_pic':'generalPic', 'applicant_pic':'pic', 'family_name':'last_name'})
                .assign(stage='evaluated'))
    print(f"size perso_app import: {len(perso_app)}")

    ######################################
    def country_clean(df, list_var):
        import json, pandas as pd
        from config_path import PATH_CLEAN
        countries = pd.read_pickle(f"{PATH_CLEAN}country_current.pkl")
        dict_c = countries.set_index('countryCode')['country_code_mapping'].to_dict()
        ccode=json.load(open("data_files/countryCode_match.json"))
        for c in list_var:
            for k,v in ccode.items():
                df.loc[df[c]==k, c] = v
            for k,v in dict_c.items():
                df.loc[df[c]==k, c] = v
            
            if any(df[c].str.len()<3):
                print(f"ATTENTION ! un {c} non reconnu dans df {df.loc[df[c].str.len()<3, [c]]}")
        return df

    perso_part = country_clean(perso_part, ['birth_country_code','nationality_country_code','host_country_code','sending_country_code'])

    ####################################
    def title_clean(df):
        df.loc[~df['title'].isnull(), 'title_clean'] = df.loc[~df['title'].isnull(), 'title'].str.replace(r"[^\w\s]+", " ", regex=True)
        df.loc[~df['title_clean'].isnull(), 'title_clean'] = df.loc[~df['title_clean'].isnull(), 'title_clean'].str.replace(r"\s+", " ", regex=True).str.strip()
        df.mask(df == '', inplace=True)
        return df

    perso_part = title_clean(perso_part)
    perso_app = title_clean(perso_app)

    ###############################
    def prop_string(tab):
        from unidecode import unidecode
        cols = ['role', 'first_name', 'last_name','title_clean', 'gender']
        tab.loc[~tab[cols].isnull(), [cols]] = tab.loc[~tab[cols].isnull(), [cols]].str.replace(r"[^\w\s]+", " ", regex=True)
        tab[cols] = tab[cols].map(lambda s:s.casefold() if type(s) == str else s)
        for i in cols:
            # tab[i] = tab[i].apply(unicode if type(s) == str else s)
            tab.loc[~tab[i].isnull(), i] = tab.loc[~tab[i].isnull(), i].apply(unidecode)
        return tab

    perso_part = prop_string(perso_part)
    perso_app = prop_string(perso_app)

    ##########
    def contact_name(df):
        for f in ['first_name', 'last_name']:
            df[f] = df[f].fillna('')
            df[f] = df[f].str.strip().str.replace(r"\s+", '-', regex=True)
            df[f] = df[f].str.strip().str.replace(r"-{2,}", '-', regex=True)

        df['contact'] = df.last_name.astype(str).str.lower() + ' ' + df.first_name.astype(str).str.lower()
        
        str_remove=['not applicable']
        df['contact'] = df['contact'].str.strip().str.replace(r"\^s+$", '-', regex=True)
        df = df.loc[~df.contact.isin(str_remove)]
        return df

    perso_app = contact_name(perso_app)
    perso_part = contact_name(perso_part)


    # ###########
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
        print(f"size df_{stage} after empty_pic: {len(df)}")
        return df

    perso_part = empty_pic(perso_part, participation, 'successful')
    perso_app = empty_pic(perso_app, participation, 'evaluated')

    ################
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
    def generaPic_remove(df):
        return df.loc[~((df.nb_pic_unique>0)&(df.generalPic.isnull()))]

    perso_part = generaPic_remove(perso_part)
    perso_app = generaPic_remove(perso_app)

    ##############################
    def name_duplicated_remove(df):
        #### cleaning name duplicated by project 
        ## if by project single name but several rows
        # x[x.project_id=='101039481']

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
            for v in ['title', 'gender','phone','email','birth_country_code','nationality_country_code','host_country_code','sending_country_code']:
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

    def PI_duplicated(df):
        if any(df.role=='principal investigator'):
            # select if same person and one PI in a single project 
            mask=(df.nb_pic_by_contact_unique>1)&(df.role=='principal investigator')
            pi=df.loc[mask, ['project_id', 'contact']].drop_duplicates().merge(df, how='inner')
            pi['role'] = 'principal investigator'
            for v in ['title', 'gender','birth_country_code','nationality_country_code','sending_country_code']:
                if v in df.columns:
                    pi=pi.sort_values(v)
                    pi[v]=pi.groupby(['project_id', 'contact'])[v].ffill()
            
            df=df.merge(pi[['project_id', 'generalPic', 'contact']].drop_duplicates(), how='outer', on=['project_id', 'generalPic', 'contact'], indicator=True).query('_merge=="left_only"')
            df=pd.concat([df, pi], ignore_index=True)
            print(f"-size df after cleaning pi_duplicated: {len(df)}")
            return df.drop(columns=['_merge'])
        
    perso_part=PI_duplicated(perso_part)

    #######################""
    def perso_participation(df, participation, project, stage):
        df=df.loc[df.project_id.isin(participation[participation.stage==stage].project_id.unique())]
        df=df.merge(participation.loc[participation.stage==stage, ['project_id', 'generalPic', 'country_code']], how='outer', on=['project_id', 'generalPic'], indicator=True).query('_merge!="right_only"')
        df.loc[df._merge=='left_only', 'shift'] = 'past'

        if stage=='successful':
            df.loc[(df._merge=='both')&(df.host_country_code.isnull()), 'host_country_code'] = df.loc[(df._merge=='both')&(df.host_country_code.isnull()), 'country_code']


        df=df.merge(project.loc[project.stage==stage, ['project_id', 'call_year', 'thema_code', 'destination_code']], how='inner', on=['project_id'])

        if len(df)==0:
            print(f"ATTENTION table vide après lein avec participation")
        else:
            print(f"size app lien avec participation clean : {len(df)}\ncolumns:{df.columns}")
        return df

    perso_part = perso_participation(perso_part, participation, project, 'successful')
    perso_app = perso_participation(perso_app, participation, project, 'evaluated')

    # ##################
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
    # add orcid_id (perso_app) into perso_part

    perso_part=perso_part.merge(perso_app[['project_id', 'contact', 'orcid_id']], how='left', on=['project_id', 'contact']) 
    perso_app=perso_app.merge(perso_part[['project_id', 'contact', 'nationality_country_code']], how='left', on=['project_id', 'contact'])

    ##################
    # fill missing value with other df part/app
    def gender_title_missing(part, app):
        tab=(part[['project_id', 'contact', 'gender','title_clean']]
        .merge(app[['project_id', 'contact', 'gender', 'title_clean']], 
                how='inner', on=['project_id', 'contact'], suffixes=('_x','_y'))
                .drop_duplicates())
        cl=['gender', 'title_clean']
        for i in cl:
            if any(tab.loc[(tab[f"{i}_x"].isnull())&(~tab[f"{i}_y"].isnull())]):
                tab.loc[(tab[f"{i}_x"].isnull())&(~tab[f"{i}_y"].isnull()), f"{i}_x"] = tab[f"{i}_y"]
            if any(tab.loc[(~tab[f"{i}_x"].isnull())&(tab[f"{i}_y"].isnull())]):
                tab.loc[(~tab[f"{i}_x"].isnull())&(tab[f"{i}_y"].isnull()), f"{i}_y"] = tab[f"{i}_x"]

            part = part.merge(tab[['project_id', 'contact', f"{i}_x"]].drop_duplicates(), how='left', on=['project_id', 'contact'])
            part.loc[part[i].isnull(), i] = part.loc[part[i].isnull(), f"{i}_x"]
            part.drop(columns=f"{i}_x", inplace=True)
            app = app.merge(tab[['project_id', 'contact', f"{i}_y"]].drop_duplicates(), how='left', on=['project_id', 'contact'])
            app.loc[app[i].isnull(), i] = app.loc[app[i].isnull(), f"{i}_y"]
            app.drop(columns=f"{i}_y", inplace=True)

        return part, app

    perso_part, perso_app = gender_title_missing(perso_part, perso_app)

    (perso_part[['project_id', 'generalPic', 'role', 'first_name', 'last_name',
        'title_clean', 'gender', 'email', 'tel_clean', 'domaine_email', 'orcid_id', 'birth_country_code',
        'nationality_country_code', 'host_country_code', 'sending_country_code',
        'stage', 'contact', 'country_code', 'shift', 'call_year', 'thema_name_en', 'destination_name_en']]
        .drop_duplicates()
        .to_pickle(f"{PATH_CLEAN}persons_participants.pkl"))

    (perso_app[['project_id', 'generalPic', 'role', 'first_name', 'last_name', 'nationality_country_code',
        'title_clean', 'gender', 'tel_clean', 'email', 'domaine_email', 'researcher_id', 'orcid_id',
        'google_scholar_id', 'scopus_author_id', 'stage',
        'contact', 'country_code', 'shift', 'call_year', 'thema_name_en', 'destination_name_en']]
        .drop_duplicates()
        .to_pickle(f"{PATH_CLEAN}persons_applicants.pkl"))

