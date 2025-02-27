
def persons_files_import(thema, PATH_PERSONS):
    import re, os, pickle
    fname=''.join([filename for filename in os.listdir(PATH_PERSONS) if thema in filename])
    print(fname)

    if fname:
        with open(f"{PATH_PERSONS}{fname}", 'rb') as f:
            return pickle.load(f)

    if fname == []:
        fmax=max(int(os.path.splitext(filename)[0].split('_')[-1]) for filename in os.listdir(PATH_PERSONS) if re.search(r"persons_authors_[0-9]+",filename))
        if fmax:
            with open(f"{PATH_PERSONS}persons_authors_{fmax}.pkl", 'rb') as f:
                return pickle.load(f)


def affiliations(df, PATH_PERSONS, CSV_DATE):
    import pandas as pd, pickle
    from api_process.openalex import harvest_openalex
    
    ### search persons into openalex
    #masia odile
    oth=df.loc[~df.thema_code.isin(['ERC', 'MSCA']), ['contact2', 'orcid_id', 'iso2']].drop_duplicates().reset_index(drop=True)
    print(f"size tmp1: {len(oth)}")
    # tmp1=tmp1[:2]
    other=harvest_openalex(oth, iso2=True)
    with open(f'{PATH_PERSONS}persons_authors_other_{CSV_DATE}.pkl', 'wb') as f:
        pickle.dump(other, f)

    em=df.loc[df.thema_code.isin(['ERC', 'MSCA']), ['contact2', 'orcid_id']].drop_duplicates().reset_index(drop=True)
    print(f"size erc_msca: {len(em)}")
    # erc_msca=erc_msca[:2]
    erc_msca=harvest_openalex(em, iso2=False)
    with open(f'{PATH_PERSONS}persons_authors_erc_{CSV_DATE}.pkl', 'wb') as f:
        pickle.dump(erc_msca, f)

def persons_api_simplify(df):
    pers = [] 
    for p in df:
        # elem = {k: v for k, v in p.items() if (v and v != "NaT")}

        p['institutions'] = []
        if p.get("affiliations"):
            for aff in p["affiliations"]:  
                res={"institution_name":aff.get('institution').get("display_name"),
                "institution_ror":aff.get('institution').get("ror"),
                "institution_country2":aff.get('institution').get("country_code"),
                "years":aff.get("years")}
                p['institutions'].append(res)
    
        p["orcid_openalex"] = p["ids"].get("orcid")            

        delete=['display_name_alternatives', 'topics', 'affiliations', 'id', 'last_known_institutions', 'ids']
        for field in delete:
            if p.get(field):
                p.pop(field)

        # elem = {k: v for k, v in elem.items() if (v and v != "NaT")}
        pers.append(p)

    print(len(pers))
    return pers

def persons_results_clean(df):
    import pandas as pd
    from functions_shared import my_country_code, prop_string

    # df=pd.json_normalize(df, max_level=1)
    df=pd.json_normalize(df, record_path=['institutions'], meta=['match', 'orcid', 'display_name', 'orcid_openalex'], errors='ignore')
    df=df[~df.astype(str).duplicated()]
    cols = ['display_name']
    df = prop_string(df, cols)

    df['rows_by_name_orcid'] = df.groupby(['display_name', 'orcid'], dropna=False).transform('size')

    persName_withOrcid_noAff=df[(df.match=='full_name')&(~df.orcid.isnull())&(df.institution_name.isnull())]
    print(f"size person detect by name with an orcid but no affiliations: {len(persName_withOrcid_noAff)}")


    for i in ['orcid_openalex', 'orcid', 'institution_ror']:
        df.loc[~df[i].isnull(), i] = df.loc[~df[i].isnull()][i].str.split("/").str[-1]
    df['institution_ror'] = 'R'+ df['institution_ror'].astype(str)

    df['years']=df['years'].map(lambda liste: ';'.join(str(x) for x in liste))
    df=df[['display_name', 'orcid_openalex', 'years', 'institution_ror', 'institution_name', 'institution_country2', 'rows_by_name_orcid']]
    my_countries=my_country_code()
    df=(df.merge(my_countries[['iso2', 'iso3', 'parent_iso3']].drop_duplicates(), 
                 how='left', left_on='institution_country2', right_on='iso2')
        .drop(columns=['iso2'])
        .rename(columns={'iso3':'institution_country_map',
                         'parent_iso3':'institution_country'})
        )

    from step8_referentiels.paysage import paysage_prep
    from config_path import PATH
    DUMP_PATH=f'{PATH}referentiel/'
    paysage = paysage_prep(DUMP_PATH)
    df=(df.merge(paysage[['nom_long', 'numero_ror', 'numero_paysage', 'country_code_map', 'num_nat_struct']].drop_duplicates(), 
                 how='left', left_on='institution_ror', right_on='numero_ror'))

    print(f"-3 size df cleaned: {len(df)}")
    return df