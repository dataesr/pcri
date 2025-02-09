def openalex_name(author):
    import time, requests
    try:
        url = f"https://api.openalex.org/authors?filter=display_name.search:{author.get('name')}"
        nb_openalex=requests.get(url).json().get("meta").get('count')
        dl=[]
        if nb_openalex>0:
            for n in range(nb_openalex): 
                author_openalex = requests.get(url).json().get("results")[n]
                if author_openalex.get('affiliations')!=[]:
                    result=author | {'display_name':author_openalex.get('display_name'), 
                                    'openalex_id':author_openalex.get('id'), 
                                    'affiliations':author_openalex.get('affiliations'),
                                    'topics':author_openalex.get('topics'),
                                    'x_concepts':author_openalex.get('x_concepts'), 
                                    'ids':author_openalex.get('ids'), 
                                    'display_name_alternatives':author_openalex.get('display_name_alternatives'),
                                    'match':'name'}
                    dl.append(result)
        return dl
    
    except requests.exceptions.HTTPError as http_err:
        print(f"\n{time.strftime("%H:%M:%S")}, {author}-> HTTP error occurred: {http_err}")
        return author
    except requests.exceptions.RequestException as err:
        print(f"\n{time.strftime("%H:%M:%S")}, {author}-> Error occurred: {err}")    
        return author
    except Exception as e:
        print(f"\n{time.strftime("%H:%M:%S")}, {author}-> An unexpected error occurred: {e}")
        return author

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
                           'display_name_alternatives':author_openalex.get('display_name_alternatives'),
                           'match':'orcid'}
        return result
    
    except requests.exceptions.HTTPError as http_err:
        print(f"\n{time.strftime("%H:%M:%S")}, {author}-> HTTP error occurred: {http_err}")
        return author
    except requests.exceptions.RequestException as err:
        print(f"\n{time.strftime("%H:%M:%S")}, {author}-> Error occurred: {err}")
        return author         
    except Exception as e:
        print(f"\n{time.strftime("%H:%M:%S")}, {author}-> An unexpected error occurred: {e}")
        return author
    

def persons_affiliation(df, nb, path):
    # from config_path import PATH_API
    import time, pickle
    from step7_persons.affiliations import openalex_name, openalex_orcid

    print(time.strftime("%H:%M:%S"))
    rlist=[]
    n = 0
    for _, row in df.iterrows():
        n=n+1
        if n % 100 == 0: 
            print(f"{n}", end=',')

        author = {
        "name": row['contact'],
        "orcid": row['orcid_id']
        }

        if author.get("orcid"):
            result = openalex_orcid(author)
            if result.get('match'):
                rlist.append(result)
            else:
                result = openalex_name(author)
                rlist.extend(result)
        if author.get("orcid")=='':
            result = openalex_name(author)
            rlist.extend(result)

    nf=f"persons_author_{nb}"
    with open(f'{path}{nf}.pkl', 'wb') as f:
        pickle.dump(rlist, f)
    print(time.strftime("%H:%M:%S"))

    return rlist