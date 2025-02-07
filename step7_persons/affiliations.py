def openalex_name(author):
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
                                'display_name_alternatives':author_openalex.get('display_name_alternatives'),
                                'match':'name'}
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
                           'display_name_alternatives':author_openalex.get('display_name_alternatives'),
                           'match':'orcid'}
        return result
    
    except requests.exceptions.HTTPError as http_err:
        print(f"\n{time.strftime("%H:%M:%S")}, {author}-> HTTP error occurred: {http_err}")
    except requests.exceptions.RequestException as err:
        print(f"\n{time.strftime("%H:%M:%S")}, {author}-> Error occurred: {err}")                    
    except Exception as e:
        print(f"\n{time.strftime("%H:%M:%S")}, {author}-> An unexpected error occurred: {e}")
    

def persons_affiliation(pp):
    from config_path import PATH_SOURCE
    import time, pickle
    from step7_persons.affiliations import openalex_name, openalex_orcid

    print(time.strftime("%H:%M:%S"))
    rlist=[]
    n = 0
    for _, row in pp.iterrows():
        n=n+1
        if n % 100 == 0: 
            print(f"{n}", end=',')

        author = {
        "name": row['contact'],
        "orcid": row['orcid_id']
        }

        if author.get("orcid"):
            result = openalex_orcid(author)
            if result:
                rlist.append(result)
            elif result is None:
                result = openalex_name(author)
                if result:
                    rlist.append(result[0])
        if author.get("orcid")=='':
            result = openalex_name(author)
            if result:
                rlist.append(result[0])
            
        if n % 2000 == 0:
            with open(f'{PATH_SOURCE}persons_author_{n}.pkl', 'wb') as f:
                pickle.dump(rlist, f)

    print(time.strftime("%H:%M:%S"))
    
    with open(f'{PATH_SOURCE}persons_author.pkl', 'wb') as f:
        pickle.dump(rlist, f)