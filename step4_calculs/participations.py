from config_path import PATH_REF
import numpy as np, pandas as pd

def entities_with_lien(entities_info, lien):
    print(f"generalPic de lien={lien.generalPic.nunique()}, generalPic de entities_info={entities_info.generalPic.nunique()}")

    part_step = (lien
                .merge(entities_info[['generalPic', 'cordis_is_sme', 'cordis_type_entity_code', 'cordis_type_entity_name_fr', 
                                    'cordis_type_entity_name_en', 'cordis_type_entity_acro', 'nutsCode',
                                    'country_code', 'country_code_mapping', 'extra_joint_organization']],
                        how='inner', on='generalPic')
                .drop(columns={'n_app', 'n_part', 'participant_pic'})
                .rename(columns={ 'nutsCode':'entities_nutsCode'})
                .drop_duplicates()    
            )

    if len(part_step)==len(lien):
        print(f'part_step ({len(part_step)}) = lien')
    else:
        print(f"lien={len(lien)}, part_step={len(part_step)}")
    return part_step

def participations_nuts(part_step):
    # gestion code nuts
    nuts = pd.read_pickle(f'{PATH_REF}nuts_complet.pkl')
    # # mask=(participation.inProposal==True)&(participation.inProject==True)
    part_step['nuts_code'] = np.where(~part_step.nuts_part.isnull(), part_step.nuts_part, part_step.nuts_app)

    part_step.loc[(part_step.nuts_code.isnull())|(part_step.nuts_code.str.len()<3), 'nuts_code'] = part_step.entities_nutsCode
    part_step.loc[part_step.nuts_code.str.len()<3, 'nuts_code'] = np.nan

    part_step = part_step.merge(nuts, how='left', on='nuts_code').drop_duplicates()
    print(f"size part_step after add nuts: {len(part_step)}, sans code_nuts: {len(part_step.loc[(~part_step.nuts_code.isnull())&(part_step.region_1_name.isnull())])}")
    return part_step