def persons_affiliation(perso, entities_all):
    from config_path import PATH
    import requests , pandas as pd, time

    entities_tmp=entities_all.loc[((entities_all.country_code=='FRA')&(entities_all.rnsr_merged.str.len()==0))|((entities_all.country_code!='FRA')&(entities_all.entities_id.str.contains('pic'))), ['project_id','generalPic','country_code', 'entities_id', 'entities_name']]
    perso = perso[['call_year', 'thema_name_en', 'destination_name_en' ,'project_id', 'country_code', 'generalPic', 'title', 'last_name', 'first_name', 'tel_clean', 'domaine_email', 'contact', 'orcid_id']]

    pp = perso[['project_id','generalPic' ,'contact', 'orcid_id']].drop_duplicates().merge(entities_tmp, how='inner', on=['project_id','generalPic'])
    # pp.mask(pp=='', inplace=True)
    pp = pp.fillna('')
    # pp = pp.loc[pp.orcid_id=='', ['contact', 'orcid_id']].drop_duplicates()

    print(f"size pp: {len(pp)}, info sur pp with orcid: {len(pp.loc[pp.orcid_id!=''])}")

    print(time.strftime("%H:%M:%S"))
    result = []
    n = 0
    for i, row in pp.iterrows():
        n=n+1
        if n % 100 == 0: 
            print(f"{n}", end=',')

        author = {
        "name": row['contact'],
        "orcid": row['orcid_id']
        }

        try:
            if author.get("orcid"):
            # Get author by Orcid
                    url = f"https://api.openalex.org/authors/orcid:{author.get('orcid')}?mailto:bso@recherche.gouv.fr"
                    author_openalex = requests.get(url).json()
            else:
                   url = f"https://api.openalex.org/authors?search={author.get('name')}"
                   if requests.get(url).json().get("results"):
                          author_openalex = requests.get(url).json().get("results")[0]
            
        except requests.exceptions.HTTPError as http_err:
            print(f"\n{time.strftime("%H:%M:%S")}, {row} -> HTTP error occurred: {http_err}")
        except requests.exceptions.RequestException as err:
            print(f"\n{time.strftime("%H:%M:%S")}, {row} -> Error occurred: {err}")                    
        except Exception as e:
            print(f"\n{time.strftime("%H:%M:%S")}, {row} -> An unexpected error occurred: {e}")
    
        if author_openalex:
            author.update({'affiliations':author_openalex.get('affiliations'), 'topics':author_openalex.get('topics'),  'x_concepts':author_openalex.get('x_concepts'), 'orcid_tmp':author_openalex.get('orcid'), 'display_name_alternatives':author_openalex.get('display_name_alternatives')})
            result.append(author)

    print(time.strftime("%H:%M:%S"))

    r=pd.DataFrame(result)
    r.loc[r.orcid=='', 'orcid'] = r.orcid_tmp.str.split("/").str[-1]
    # r2 = r.groupby(['name', 'orcid'])['affiliations'].apply(pd.json_normalize)
    r2 = r[['name', 'orcid', 'affiliations']]
    r2.to_pickle(f'{PATH}participants/data_for_matching/persons_author.pkl')
    return r2