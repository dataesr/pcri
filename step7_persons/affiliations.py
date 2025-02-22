
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


def affiliations(perso_part, perso_app, PATH_PERSONS, CSV_DATE):
    import pandas as pd, pickle
    from api_process.openalex import harvest_openalex
    #PREPRATION data for request openalex
    lvar=['contact','orcid_id','country_code', 'iso2','destination_code','thema_code','nationality_country_code']
    pp = pd.concat([perso_part[lvar].drop_duplicates(), perso_app[lvar].drop_duplicates()], ignore_index=True)

    mask=((pp.country_code=='FRA')|(pp.nationality_country_code=='FRA')|(pp.destination_code.isin(['COG', 'PF', 'STG', 'ADG', 'POC','SyG', 'PERA', 'SJI'])))&(~(pp.contact.isnull()&pp.orcid_id.isnull()))
    pp=pp.loc[mask].sort_values(['country_code','orcid_id'], ascending=False).drop_duplicates()
    pp['contact']=pp.contact.str.replace('-', ' ')
    print(f"size pp: {len(pp)}, info sur pp with orcid: {len(pp.loc[pp.orcid_id.isnull()])}")


    ### search persons into openalex
    #masia odile
    oth=pp.loc[~pp.thema_code.isin(['ERC', 'MSCA']), ['contact', 'orcid_id', 'iso2']].drop_duplicates().reset_index(drop=True)
    print(f"size tmp1: {len(oth)}")
    # tmp1=tmp1[:2]
    other=harvest_openalex(oth, iso2=True)
    with open(f'{PATH_PERSONS}persons_authors_other_{CSV_DATE}.pkl', 'wb') as f:
        pickle.dump(other, f)

    em=pp.loc[pp.thema_code.isin(['ERC', 'MSCA']), ['contact', 'orcid_id']].drop_duplicates().reset_index(drop=True)
    print(f"size erc_msca: {len(em)}")
    # erc_msca=erc_msca[:2]
    erc_msca=harvest_openalex(em, iso2=False)
    with open(f'{PATH_PERSONS}persons_authors_erc_{CSV_DATE}.pkl', 'wb') as f:
        pickle.dump(erc_msca, f)
