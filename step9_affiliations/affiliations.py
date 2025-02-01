def openalex_name(author):
    from config_api import openalex_usermail
    import requests, time
    try:
        url = f"https://api.openalex.org/authors?filter=display_name.search:{author.get('name')}"
        nb_openalex=requests.get(url).json().get("meta").get('count')
        d=[]
        if nb_openalex>0:
            for n in range(nb_openalex):
                author_openalex = requests.get(url).json().get("results")[n]
                result = author | {'display_name':author_openalex.get('display_name'),
                                'openalex_id':author_openalex.get('id'), 
                                'affiliations':author_openalex.get('affiliations'), 
                                'topics':author_openalex.get('topics'), 
                                'x_concepts':author_openalex.get('x_concepts'), 
                                'ids':author_openalex.get('ids'), 
                                'display_name_alternatives':author_openalex.get('display_name_alternatives')}
                d.append(result)
        return d

    except requests.exceptions.HTTPError as http_err:
        print(f"\n{time.strftime("%H:%M:%S")}, {author}-> HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as err:
        print(f"\n{time.strftime("%H:%M:%S")}, {author}-> Error occurred: {err}")                    
    except Exception as e:
        print(f"\n{time.strftime("%H:%M:%S")}, {author}-> An unexpected error occurred: {e}")
    

def openalex_orcid(author):
    from config_api import openalex_usermail
    import requests, time
    try:
        url = f"https://api.openalex.org/authors/orcid:{author.get('orcid')}?mailto={openalex_usermail}"
        author_openalex = requests.get(url).json()
        result = author | {'display_name':author_openalex.get('display_name'), 
                           'openalex_id':author_openalex.get('id'), 
                           'affiliations':author_openalex.get('affiliations'), 
                           'topics':author_openalex.get('topics'),  
                           'x_concepts':author_openalex.get('x_concepts'), 
                           'ids':author_openalex.get('ids'), 
                           'display_name_alternatives':author_openalex.get('display_name_alternatives')}
        return result
    
    except requests.exceptions.HTTPError as http_err:
        print(f"\n{time.strftime("%H:%M:%S")}, {author}-> HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as err:
        print(f"\n{time.strftime("%H:%M:%S")}, {author}-> Error occurred: {err}")                    
    except Exception as e:
        print(f"\n{time.strftime("%H:%M:%S")}, {author}-> An unexpected error occurred: {e}")
    

def persons_affiliation(pp):
    from config_path import PATH
    import  pandas as pd, time
    from step9_affiliations.affiliations import openalex_name, openalex_orcid

    print(time.strftime("%H:%M:%S"))
    df=pd.DataFrame()
    n = 0
    for _, row in pp.iterrows():
        n=n+1
        # if n % 100 == 0: 
        print(f"{n}", end=',')

        author = {
        "name": row['contact'],
        "orcid": row['orcid_id']
        }

        if author.get("orcid"):
            result = openalex_orcid(author)
            if result:
                df=pd.concat([df, pd.json_normalize(result)])
            elif result is None:
                result = openalex_name(author)
                if result:
                    df=pd.concat([df, pd.json_normalize(result)], ignore_index=True)
        if author.get("orcid")=='':
            result = openalex_name(author)
            if result:
                df=pd.concat([df, pd.json_normalize(result)], ignore_index=True)
            
        if n==10000:
            df.to_pickle(f'{PATH}participants/data_for_matching/persons_author_1.pkl')

    print(time.strftime("%H:%M:%S"))

    # df.loc[df.orcid=='', 'orcid'] = df.orcid_tmp.str.split("/").str[-1]
    # r2 = r.groupby(['name', 'orcid'])['affiliations'].apply(pd.json_normalize)
    # r2 = df[['name', 'orcid', 'affiliations']]
    df.to_pickle(f'{PATH}participants/data_for_matching/persons_author.pkl')
    return df