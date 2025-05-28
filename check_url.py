import datetime as dt, requests
def check_proj_id(project_list):
    current_date = dt.date.today()
    #### check url projects
    requests.post('http://compute.sandbox.dataesr.ovh/check_cordis', json={
        "projects": project_list, "output_file":f"check_cordis_url_{current_date}"})