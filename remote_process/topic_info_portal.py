import requests, math, json, pandas as pd
from paths import PATH_WP


def fetch_page(url, headers, files, page_number: int, page_size: int = 50):
    params = {
        "apiKey": "SEDIA",
        "text": "***",
        "pageSize": page_size,
        "pageNumber": page_number
    }

    r = requests.post(url, params=params, headers=headers, files=files, timeout=30)
    r.raise_for_status()
    return r.json()


def get_topic_from_eu_portal():

    BASE_URL = "https://api.tech.ec.europa.eu/search-api/prod/rest/search"

    HEADERS = {
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://ec.europa.eu",
        "Referer": "https://ec.europa.eu/",
        "X-Requested-With": "XMLHttpRequest"
    }

    FILES = {
        "sort": ("blob", json.dumps({"order": "DESC", "field": "startDate"}), "application/json"),
        "query": ("blob", json.dumps({
            "bool": {"must": [
                {"terms": {"type": ["1", "8"]}},
                {"terms": {"status": ["31094502", "31094503", "31094501"]}},
                {"terms": {"frameworkProgramme": ["43108390"]}},
            ]}
        }), "application/json"),
        "languages": ("blob", json.dumps(["en"]), "application/json"),
        "displayFields": ("blob", json.dumps([
            "identifier", "title", "type", "typesOfAction",
            "startDate", "deadlineDate", "status", "callIdentifier", 
            "callTitle", "budgetOverview", "topicConditions"
        ]), "application/json"),
    }


    def fetch_all_metadata(text: str, page_size: int = 50):
        first = fetch_page(BASE_URL, HEADERS, FILES, 1, page_size)

        total = first["totalResults"]
        total_pages = math.ceil(total / page_size)


        # ⚠️ Adjust this key if needed (check your response structure once)
        items = first["results"]

        all_metadata = []

        def extract(item):
            return {
                "identifier": item.get("metadata").get("identifier"),
                "title": item.get("metadata").get("title"),
                "type": item.get("metadata").get("type"),
                "typesOfAction": item.get("metadata").get("typesOfAction"),
                "startDate": item.get("metadata").get("startDate"),
                "deadlineDate": item.get("metadata").get("deadlineDate"),
                "status": item.get("metadata").get("status"),
                "call_id": item.get("metadata").get("callIdentifier"),
                "call_lib": item.get("metadata").get("callTitle"),
                "budgetOverview": item.get("metadata").get("budgetOverview"),
                "topicConditions": item.get("metadata").get("topicConditions")
            }

        all_metadata.extend(extract(i) for i in items)

        for page in range(2, total_pages + 1):
            data = fetch_page(BASE_URL, HEADERS, FILES, page, page_size)
            page_items = data["results"]
            all_metadata.extend(extract(i) for i in page_items)
            print(f"Fetched page {page}/{total_pages}")

        return all_metadata

    # Run
    metadata = fetch_all_metadata(text="***", page_size=50)

    print("Total fetched:", len(metadata))
    print(metadata[0])

    with open(f"{PATH_WP}topic_info_harvest.json", 'w', encoding='UTF-8') as pl:
        json.dump(metadata, pl, indent=4)
