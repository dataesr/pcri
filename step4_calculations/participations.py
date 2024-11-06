import numpy as np, pandas as pd
from config_path import PATH_CLEAN

def entities_with_lien(entities_info, lien):
    print("### LIEN + entities_info -> pour calculations")
    print(f"- ETAT avant lien ->\ngeneralPic de lien={lien.generalPic.nunique()},\ngeneralPic de entities_info={entities_info.generalPic.nunique()}")

    part_step = (lien
                .merge(entities_info[['generalPic', 'cordis_is_sme', 'cordis_type_entity_code', 'cordis_type_entity_name_fr', 
                                    'cordis_type_entity_name_en', 'cordis_type_entity_acro', 'nutsCode',
                                    'country_code', 'country_code_mapping', 'extra_joint_organization']].drop_duplicates(),
                        how='inner', on='generalPic')
                .drop(columns={'n_app', 'n_part', 'participant_pic'})
                .rename(columns={ 'nutsCode':'entities_nuts', 'nuts_code':'participation_nuts'})   
            )

    if len(part_step)==len(lien):
        print(f'1- part_step ({len(part_step)}) = lien')
    else:
        print(f"2- lien={len(lien)}, part_step={len(part_step)}")
    return part_step


def participations_complete(part_prop, part_proj, proj_no_coord):
    print("### PARTICIPATIONS final")
    participation = pd.concat([part_prop, part_proj], ignore_index=True)

    print(f"- control role: {participation.role.unique()}")
    participation['coordination_number']=np.where(participation['role']=='coordinator', 1, 0)
    participation.loc[participation.project_id.isin(proj_no_coord), 'coordination_number'] = 0
    participation = participation.assign(with_coord=True)
    participation.loc[participation.project_id.isin(proj_no_coord), 'with_coord'] = False

    participation.rename(columns={'partnerType':'participates_as'}, inplace=True)

    print(f"- size participation: {len(participation)}")

    file_name = f"{PATH_CLEAN}participation_current.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(participation, file)
    return participation
    
def proj_no_coord(projects):
    return projects[(projects.thema_code.isin(['ACCELERATOR']))|(projects.destination_code.isin(['PF','COST']))|((projects.thema_code=='ERC')&(projects.destination_code!='SyG'))].project_id.to_list()