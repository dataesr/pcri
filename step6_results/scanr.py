import os, requests, pandas as pd, numpy as np, copy
from config_url import scanr_185
from config_api import scanr_185_headers
from dotenv import load_dotenv
load_dotenv()
apikey = os.environ.get('ODS_API')


def from_ods_to_185(data):
    program_code = "HE-Global" if data.get('pilier_name_en', "").startswith('Global Challenges') else ""
    res = {
        "id": data.get("project_id"),
        "type": data.get('framework'),
        "stage": 'project',
        "status": data.get('status_code'),
        "name": {
            "en": data.get('title')
        },
        "description": {
            "en": data.get('abstract', '')
        },
        "acronym": data.get('acronym'),
        "year": int(data.get('call_year')) if data.get('call_year') else None,
        "signature_date": data.get('signature_date') + 'T00:00:00' if data.get('signature_date') else None,
        "start_date": data.get('start_date') + 'T00:00:00' if data.get('start_date') else None,
        "end_date": data.get('end_date') + 'T00:00:00' if data.get('end_date') else None,
        "budget_total": data.get('project_totalcost'),
        "budget_financed": data.get('project_eucontribution'),
        "duration": int(data.get('duration')) if data.get('duration') else None,
        "source_url": data.get('cordis_project_webpage'),
        "number_participant": data.get('proposal_numberofapplicants'),
        "call_code": data.get('call_id'),
        "call_date": data.get('call_date'),
        "topic_code": data.get('topic_code'),
        "topic_name": data.get('topic_name'),
        "priorities": [d for d in [
            {"level": "1", "name": data.get('pilier_name_en'), "code": program_code},
            {"level": "2", "name": data.get('programme_name_en'), "code": ""},
            {"level": "21", "name": data.get('thema_name_en'), "code": data.get('thema_code')},
            {"level": "3", "name": data.get('destination_name_en'), "code": data.get('destination_code')},
            {"level": "31", "name": data.get('destination_detail_name_en'), "code": data.get('destination_detail_code')}
        ] if d.get('name')],
        "action": [d for d in [
            {"level": "1", "name": data.get('action_name'), "code": data.get('action_code')},
            {"level": "2", "name": data.get('action_detail_name'), "code": data.get('action_detail_code')},
        ] if d.get('name')],
        "keywords_en": data.get('free_keywords', "").split('|')
    }
    return {k:v for k,v in res.items() if v is not None}

def exists(_id):
    r = requests.get(scanr_185 + '/projects/' + _id, headers=scanr_185_headers)
    if r.status_code == 200:
        return r.json()['etag']
    else:
        return False

def project_to_dataesr(_id, project):
    etag = exists(_id)
    if etag:
        current_headers = scanr_185_headers.copy()
        current_headers['If-Match']=etag
        r = requests.patch(scanr_185 + '/projects/' + _id, json=project, headers=current_headers)
        if not r.ok:
            print(f'{_id} not updated -- error')
            print(r.text)
            return 0
        print(f"Project {_id} updated")
    else:
        project["id"] = _id
        r = requests.post(scanr_185 + '/projects', json=project, headers=scanr_185_headers)
        if not r.ok:
            print(f'{_id} not created -- error')
            print(r.text)
            return 0
        print(f"Project {_id} created")
    return 1

def delete_participations():
    url = f"{scanr_185}/operations/remove/participations/Horizon%20Europe"
    r = requests.get(url, headers=scanr_185_headers)
    print("delete all participants")
    print(r.json())

def from_ods_to_185_participant(data):
    res = {
        "id": data.get("id_scanr"),
        "project_id": data.get("project_id"),
        "project_type": data.get('framework'),
        "participant_id": data.get("entities_id"),
        "participant_id_type": data.get('source_id'),
        "stage": "project",
        "acronym_source": data.get('entities_acronym', ''),
        "participates_as": data.get("participates_as"),
        "funding": f"{data.get('funding', 0)}",
        "role": data.get("role"),
        "name_source": data.get("entities_name"),
        "participant_order": data.get("participant_order"),
        "participant_type_code": data.get('cordis_type_entity_acro'),
        "participant_type_name": data.get('cordis_type_entity_name_fr'),
        "address": {
            "country": data.get('country_name_en'),
            "country_code": data.get('country_code'),
            "country_level_2": data.get('country_group_association_name_fr'),
        }
    }
    return clean_json(res)

def get_id_from_paysage(identifier):
    headers = {'X-Api-Key': os.environ.get('PAYSAGE_KEY')}
    r = requests.get(f"https://api.paysage.dataesr.ovh/structures/{identifier}/identifiers", headers=headers)
    data = r.json().get('data', [])
    ids = [e for e in data if data and len(data) > 0 and e.get('type') in ['siret', 'rnsr', 'grid']]
    rnsr = [e.get('value') for e in ids if e.get('type') == 'rnsr']
    siret = [e.get('value') for e in ids if e.get('type') == 'siret']
    grid = [e.get('value') for e in ids if e.get('type') == 'grid']
    if len(rnsr) > 0:
        return rnsr[0]
    if len(siret) > 0:
        return siret[0][0:9]
    if len(rnsr) > 0:
        return grid[0]
    return identifier
    
def participations_to_dataesr(_id, participation):
    print(f'Participation {_id}')
    r = requests.post(scanr_185 + '/participations', json=participation, headers=scanr_185_headers)
    if not r.ok:
            print('-> not created -- error')
            print(r.text)
            return 0
    print(f"-> created")
    return 1

	

def clean_json(elt):
    keys = list(elt.keys()).copy()
    for f in keys:
        if isinstance(elt[f], dict):
            elt[f] = clean_json(elt[f])
        elif (not elt[f] == elt[f]) or (elt[f] is None):
            del elt[f]
        elif (isinstance(elt[f], str) and len(elt[f])==0):
            del elt[f]
        elif (isinstance(elt[f], list) and len(elt[f])==0):
            del elt[f]
    return elt

def scanr_update(entities_participation):

    # df = entities_participation.loc[(entities_participation.framework=='Horizon Europe')&(entities_participation.stage=='successful')]

    dataset_name = "fr-esr-all-projects-signed-informations"
    r_projects = requests.get(f'https://data.enseignementsup-recherche.gouv.fr/explore/dataset/{dataset_name}/download/?format=json&apikey={apikey}')
    data_projects = [p for p in [d.get('fields') for d in r_projects.json()] if (p.get('framework') == "Horizon Europe")]

    
    projets = [from_ods_to_185(el) for el in data_projects]
    projects = {}
    for projet in projets:
        _id = projet.pop('id')
        projects[_id] = projet
    projects

    ## Add to dataesr
    for k, v in projects.items():
        project_to_dataesr(k, v)

##############################################
    data_participants = entities_participation.loc[(entities_participation.framework=='Horizon Europe')&(entities_participation.stage=='successful')]
    
    data_participants["participant_order"] = data_participants["participation_linked"].str.split('-').str[-1]
    gen_part=['beneficiary', 'thirdparty', 'associated partner']
    if len(data_participants.participates_as.dropna().unique()) > len(gen_part):
        print(f"2 - Attention ! un nouveau participate_as dans data_participants -> {set(data_participants.participates_as.unique())-set(gen_part)}")
    else:
        data_participants = (
            data_participants
            .groupby(['participation_linked'])
            .apply(
                lambda x: (
                    x
                    .sort_values('participates_as', key=lambda col: pd.Categorical(col, categories=gen_part, ordered=True))
                    .assign(counter2=lambda df: df.groupby('participation_linked').cumcount())
                ),
                include_groups=True
            )
            .reset_index(drop=True)
        )
        data_participants['counter1'] = data_participants['counter2']+1

        data_participants['id_scanr'] = (
                data_participants['project_id'].astype(str) + '-' +
                data_participants['generalPic'].astype(str) + '-' +
                data_participants['participant_order'].astype(str) + '-' +
                data_participants['counter1'].astype(str) + '-' +
                data_participants['counter2'].astype(str)
        )

        if len(data_participants.id_scanr.value_counts().reset_index(name='nb').query('nb>1'))>0:
            print(f"- Attention vérification unicité id_scanr à corriger! \n {data_participants.id_scanr.value_counts().reset_index(name='nb').query('nb>1')}")

        print(f"3 - size entities after cleaning: {len(data_participants)}")
    
    data_participants['funding'] = data_participants['beneficiary_fund']
    data_participants['funding'] = np.where(data_participants.id_scanr.str.split('-').str[-1]!='0', data_participants.calculated_fund, data_participants.funding)
    data_participants['funding'] = np.where((data_participants.id_scanr.str.split('-').str[-1]!='0')&(data_participants.action_code=='ERC'), data_participants.fund_ent_erc, data_participants.funding)
    data_participants = data_participants.to_dict(orient = "records")

    
    # pour ajour labo intégrer la notion de co-participant dans participates_as
    participants = [from_ods_to_185_participant(element) for element in data_participants]

    from config_path import PATH_REF
    ror2grid = pd.read_csv(f'{PATH_REF}grid_ror.csv').to_dict("records")
    def get_grid_from_ror(identifier):
        res = next(iter([e.get('grid') for e in ror2grid if e.get('ror') == identifier]), identifier)
        return res

    identified_participants = []
    for e in participants:
        try:
            idtype = e.get('participant_id_type')
            if idtype in ['rnsr', 'grid', 'siren']:
                identified_participants.append(e)
            if idtype == 'ror':
                e['participant_id'] = get_grid_from_ror(e['participant_id'])
                identified_participants.append(e)
            if e.get('participant_id_type') == 'paysage':
                e['participant_id'] = get_id_from_paysage(e['participant_id'])
                identified_participants.append(e)
        except Exception as err:
            print(err)
            identified_participants.append(e)
            
    
    data = copy.deepcopy(identified_participants)
    participations = {}
    for p in data:
        _id = p.get('id')
        if participations.get(_id):
            print(f'WRONG _id {_id} already exists')
        participations[_id] = p
    participations


    delete_participations()

    ## Add to dataesr
    errored = {}
    for k, v in participations.items():
        res = participations_to_dataesr(k, v)
        if res == 0:
            errored[k] = v
