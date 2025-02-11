
import math, requests, time
from retry import retry

@retry(delay=100, tries=3)
def get_all_from_openalex(url):
    data = []
    res = requests.get(url).json()
    data = res['results']
    nb_res = res['meta']['count']
    nb_pages = math.ceil(nb_res/200)
    for page in range(2, nb_pages+2):
        url_paged = f'{url}&page={page}'
        res = requests.get(url_paged).json()
        if res:
            data += res['results']
    # print(f'{len(data)} results found')
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