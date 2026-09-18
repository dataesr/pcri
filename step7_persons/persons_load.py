from constant_vars import FRAMEWORK
from functions_shared import unzip_zip
from paths import PATH_SOURCE

__all__ = ['load_perso']

def load_perso(csv_date):

    """
    loading applicant persons (file csv)
    keeping useful columns
    
    """
    perso_app = unzip_zip(f'{PATH_SOURCE}{FRAMEWORK}/he_proposals_ecorda_pd_{csv_date}.zip', "applicant_persons.csv", 'utf-8')

    perso_app = (perso_app
                    .loc[perso_app.FRAMEWORK=='HORIZON', 
                            ['PROPOSAL_NBR', 
                            'GENERAL_PIC', 
                            'APPLICANT_PIC', 
                            'ROLE', 
                            'FIRST_NAME',
                            'FAMILY_NAME', 
                            'GENDER', 
                            'EMAIL',
                            'ORCID_ID', 
                            ]
                        ]
                .rename(columns=str.lower)
                .rename(columns={
                    'proposal_nbr':'project_id', 
                    'general_pic':'generalPic', 
                    'applicant_pic':'pic', 
                    'family_name':'last_name'
                    }
                )
                .assign(stage='evaluated')
    )
    print(f"size perso_app import: {len(perso_app)}")

    perso_part = unzip_zip(f'{PATH_SOURCE}{FRAMEWORK}/he_grants_ecorda_pd_{csv_date}.zip', "participant_persons.csv", 'utf-8')

    perso_part = (perso_part
                  .loc[perso_part.FRAMEWORK=='HORIZON', 
                            ['PROJECT_NBR', 
                            'GENERAL_PIC', 
                            'PARTICIPANT_PIC', 
                            'ROLE', 
                            'FIRST_NAME',
                            'LAST_NAME',
                            'GENDER', 
                            'EMAIL',
                            'BIRTH_COUNTRY_CODE', 
                            'NATIONALITY_COUNTRY_CODE', 
                            'HOST_COUNTRY_CODE', 
                            'SENDING_COUNTRY_CODE',
                            ]
                        ]
                .rename(columns=str.lower)
                .rename(columns={
                    'project_nbr':'project_id', 
                    'general_pic':'generalPic', 
                    'participant_pic':'pic'
                    }
                )
                .assign(stage='successful')
    )
    print(f"size perso_part import: {len(perso_part)}")

    return perso_app, perso_part