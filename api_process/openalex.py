
import math, requests, time, pickle
from retry import retry
from config_path import PATH_API

@retry(delay=100, tries=3)
def get_all_from_openalex(url):
    data = []
    res = requests.get(url).json()
    data = res['results']
    nb_res = res['meta']['count']
    nb_pages = math.ceil(nb_res/200)
    print(nb_pages)
    for page in range(2, nb_pages+2):
        print(f'{page}/{nb_pages}')
        url_paged = f'{url}&page={page}'
        res = requests.get(url_paged).json()
        if isinstance(res.get('results'), list):
            data += res['results']
    print(f'{len(data)} results found')
    return data

def parse_openalex_author(e, match):
    elt = {'match': match}
    for f in ['id', 'ids', 'orcid', 'display_name', 'display_name_alternatives', 'works_count', 
            'affiliations', 'last_known_institutions',
            'topics']:
        if e.get(f):
            elt[f] = e[f]
    return elt

def get_author_from_openalex(orcid, full_name, country_code):
    URL_AUTHORS = 'https://api.openalex.org/authors?per_page=200&filter='
    print(f"For {full_name}")
    if country_code:
        URL_AUTHORS += f'affiliations.institution.country_code:{country_code},'
    if orcid:
        url_to_use = f'{URL_AUTHORS}orcid:{orcid}'
        res = get_all_from_openalex(url_to_use)
        print(f'{len(res)} results found with orcid {orcid}')
        if len(res) > 0:
            return [parse_openalex_author(e, 'orcid') for e in res]
    if full_name:
        url_to_use = f'{URL_AUTHORS}display_name.search:{full_name}'
        res = get_all_from_openalex(url_to_use)
        print(f'{len(res)} results found with full_name {full_name}')
        if len(res) > 0:
            return [parse_openalex_author(e, 'full_name') for e in res]
    return res

def request_openalex(df, iso2):
    print(time.strftime("%H:%M:%S"))
    rlist=[]
    n = 0
    for _, row in df.iterrows():
        n=n+1
        if n % 100 == 0: 
            print(f"{n}", end=',')
        if iso2==True:
            res=get_author_from_openalex(row['orcid_id'], row['contact'], row['iso2'])
            rlist.extend(res)
        else:
            res=get_author_from_openalex(row['orcid_id'], row['contact'], '')
            rlist.extend(res)

        if n % 2000 == 0:
            a=str(int(n/1000))
            with open(f'{PATH_API}persons/persons_authors_{a}.pkl', 'wb') as f:
                pickle.dump(rlist, f)
    print(time.strftime("%H:%M:%S"))
    return rlist