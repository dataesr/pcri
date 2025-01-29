def organismes_back(year):
    import pandas as pd, numpy as np, requests
    from config_path import PATH_ORG
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

    lo = ['CNRS', 'CEA', 'ONERA', 'INRAE', 'INRIA']
    organisme_back = pd.DataFrame()

    for org in lo:
        globals()[f"{org}"] = pd.read_excel(f"{PATH_ORG}w/{org}_{year}.xlsx", dtype=str)
        globals()[f"{org}"] = globals()[f"{org}"].assign(org=org)
        
        cols=['numero_projet','general_pic','proposal_numero_ordre', 'pic_secondaire', 'numero_ordre', 'rôle', 'type de partenaire', 'identifiant', 'loc', 'lib', 'org']
        cols_keep=[]
        for x in cols:
            if x in globals()[f"{org}"].columns:
                cols_keep.append(x)

        globals()[f"{org}"] = globals()[f"{org}"][cols_keep]
        
        if 'loc' in globals()[f"{org}"].columns:
            globals()[f"{org}"].rename(columns={'loc':'location', }, inplace=True)
            globals()[f"{org}"]['location'] = globals()[f"{org}"].location.str.replace(r"\?*", '', regex=True)
            globals()[f"{org}"]['location'] = globals()[f"{org}"].location.str.lower().str.replace('non inrae|cedex', '', regex=True)
            globals()[f"{org}"]['cp'] = globals()[f"{org}"].location.str.extract(r"(\d{5})")
            globals()[f"{org}"]['city'] = globals()[f"{org}"].location.str.lower().str.extract(r"([^0-9/]+)")
        
        organisme_back = pd.concat([organisme_back, globals()[f"{org}"]], ignore_index=True)
        
    organisme_back = organisme_back[~organisme_back.numero_projet.isnull()]

    organisme_back = organisme_back.rename(columns={
        'numero_projet':'project_id',
        'general_pic':'generalPic',
        'pic_secondaire':'pic',
        'rôle':'role',
        'type de partenaire':'participates_as',
        'numero_ordre':'orderNumber',
        'proposal_numero_ordre':'proposal_orderNumber'})

    cols=['identifiant', 'location','lib', 'org', 'cp', 'city']
    organisme_back.rename(columns={c: c+'_back' for c in organisme_back.columns if c in cols}, inplace=True)
    organisme_back = organisme_back.map(lambda x: x.strip() if isinstance(x, str) else x)

    pattern=r"^[0-9]{9}[A-Z]{1}($|;)"
    organisme_back.loc[organisme_back.identifiant_back.str.strip().str.contains(pattern, na=False, regex=True), 'rnsr_back'] = organisme_back.identifiant_back
    organisme_back.loc[organisme_back.rnsr_back.isnull(), 'labo_back'] = organisme_back.identifiant_back
    for org in lo:
        organisme_back.loc[organisme_back.labo_back.str.contains(org, na=False), 'labo_back'] = np.nan

    if any(organisme_back.loc[organisme_back.generalPic.isnull()].org_back.unique()):
        print(f"generalPic isnull for -> {organisme_back.loc[organisme_back.generalPic.isnull()].org_back.unique()}")
        # organisme_back.loc[organisme_back.org_back=='CEA', 'generalPic'] = '999992401'  

    organisme_back.to_pickle(f"{PATH_ORG}organisme_back.pkl")