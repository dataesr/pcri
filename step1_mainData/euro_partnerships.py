def euro_partnerships(df):
    import pandas as pd, json, re, numpy as np
    ################################
    # topics coprogrammés
    
    with open("data_files/euro_ps.json", "r") as f:
        eups=json.load(f)
    eups=pd.json_normalize(eups,"info", ['euro_ps'])

    coprog=eups.loc[eups.euro_ps=='co-programmed']
    def match(eups, x):
        cp = []
        for _, row in eups.iterrows():
            pat=r"(?:\(.*)("+row['pat']+r")"
            y = re.search(pat.upper(), x.upper())
            if y:
                cp.append(row['name'])
        return cp

    tp=df.loc[(df.thema_code.str.startswith("CLUSTER 4"))|(df.thema_code.str.startswith("CLUSTER 5")), ['topic_code', 'topic_name']]
    
    tp['euro_ps_name']=tp['topic_name'].apply(lambda x: match(coprog, x) if isinstance(x, str) else np.nan)
    tp.loc[~tp.euro_ps_name.isnull(), 'euro_ps_name']=tp.loc[~tp.euro_ps_name.isnull()]['euro_ps_name'].apply(lambda x: ', '.join(sorted(set(x))))
    df=df.merge(tp.loc[~tp.euro_ps_name.isnull(), ['topic_code', 'euro_ps_name']], how='left', on='topic_code')
    df=df.mask(df=='')
    
    df.loc[df.destination_code=='INFRAEOSC', 'euro_ps_name']='EOSC'
    df.loc[~df.euro_ps_name.isnull(), 'euro_partnerships_type']='co-programmed'

    # JU-JTI / EIT
    df.loc[df.thema_code=='JU-JTI', 'euro_ps_name']=df.loc[df.thema_code=='JU-JTI'].destination_code
    df.loc[df.thema_code=='JU-JTI', 'euro_partnerships_type']='JU-JTI'
    df.loc[(df.programme_code=='HORIZON.3.3')&(df.thema_code.str.startswith('KIC')), 'euro_ps_name']=df.loc[(df.programme_code=='HORIZON.3.3')&(df.thema_code.str.startswith('KIC'))].thema_name_en
    df.loc[(df.programme_code=='HORIZON.3.3')&(df.thema_code.str.startswith('KIC')), 'euro_partnerships_type']='EIT KICs'

    # co-funded
    cofund=eups.loc[eups.euro_ps=='co-funded']
    def match(eups, x):
        cp = []
        for _, row in eups.iterrows():
            pat=r".*"+row['pat']+r".*"
            y = re.search(pat.upper(), x.upper())
            if y:
                cp.append(row['name'])
        return cp
    
    tp=df.loc[df.action_code=='COFUND', ['topic_code', 'topic_name']]
    tp['euro_ps_name']=tp['topic_name'].apply(lambda x: match(cofund, x) if isinstance(x, str) else np.nan)
    tp.loc[~tp.euro_ps_name.isnull(), 'euro_ps_name']=tp.loc[~tp.euro_ps_name.isnull()]['euro_ps_name'].apply(lambda x: ', '.join(sorted(set(x))))
    
    df=df.merge(tp.loc[~tp.euro_ps_name.isnull(), ['topic_code', 'euro_ps_name']], how='left', on='topic_code', suffixes=('', '_y'))
    df.loc[df.euro_ps_name.isnull(), 'euro_ps_name'] = df.loc[df.euro_ps_name.isnull()].euro_ps_name_y
    df.loc[~df.euro_ps_name_y.isnull(), 'euro_partnerships_type'] = 'co-funded'
    df=df.mask(df=='')

    df = df.assign(euro_partnerships_flag=np.where(df.euro_partnerships_type.isnull(), False, True)) 
    
    return df.drop(columns='euro_ps_name_y')