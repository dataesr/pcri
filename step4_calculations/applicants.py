

def applicants_calcul(part_step, app1, proj_erc):
    import numpy as np, pandas as pd
    '''Traitement des subventions proposals -> création calculated_proposal_subv'''
    print("\n### CALCULS applicants")

    subv_p = (part_step.loc[part_step.inProposal==True, 
                ['project_id', 'generalPic','pic_old','proposal_country_code_mapping',  'proposal_orderNumber', 'projNlien', 'n_pic_cc']].drop_duplicates()
            .merge(app1.rename(columns={'generalPic':'pic_old'})[
                ['project_id', 'pic_old', 'country_code_mapping', 'orderNumber', 'role', 'partnerType', 'erc_role', 'requestedGrant']], 
                how='inner',
                left_on=['project_id', 'pic_old', 'proposal_country_code_mapping', 'proposal_orderNumber'],
                right_on=['project_id', 'pic_old', 'country_code_mapping', 'orderNumber'])
            .drop(columns='country_code_mapping')
            )

    print(f"0 - {'{:,.1f}'.format(app1['requestedGrant'].sum())}, {'{:,.1f}'.format(subv_p['requestedGrant'].sum())}")

    subv_p = subv_p.merge(proj_erc, how='left', on='project_id', indicator=True)
    tmp = subv_p.loc[subv_p._merge=='both']
    tmp['fund_ent_erc'] = tmp.requestedGrant
    subv_tmp = tmp.loc[tmp.destination_code!='SyG', ['project_id', 'requestedGrant']].groupby(['project_id'])['requestedGrant'].sum().reset_index()

    tmp = tmp.merge(subv_tmp, how='left', on='project_id', suffixes=('', '_y'))
    tmp.loc[(subv_p.destination_code!='SyG')&(subv_p.erc_role!='PI'), 'requestedGrant_y'] = 0

    subv_p=pd.concat([subv_p[subv_p._merge=='left_only'], tmp], ignore_index=True)

    subv_p['calculated_fund'] = (np.where((subv_p['projNlien']>1.)|(subv_p['n_pic_cc']>1.), 
                                subv_p['requestedGrant']/subv_p['projNlien']/subv_p['n_pic_cc'], subv_p['requestedGrant']))

    subv_p['fund_ent_erc'] = (np.where((subv_p['projNlien']>1.)|(subv_p['n_pic_cc']>1.), 
                                subv_p['fund_ent_erc']/subv_p['projNlien']/subv_p['n_pic_cc'], subv_p['fund_ent_erc']))
    subv_p.drop(['projNlien','orderNumber','requestedGrant_y'], axis=1, inplace=True)

    if len(subv_p)!=len(app1):
        print(f"1- ATTENTION ! {len(subv_p)-len(app1)} participations perdues entre app1 et subv_p")

    app_sum = '{:,.1f}'.format(app1['requestedGrant'].sum())
    
    if '{:,.1f}'.format(subv_p['requestedGrant'].sum()) == app_sum:
        print("2- requests grants = subventions proposals OK")
    else:
        print(f"3- ATTENTION ! Ecart subventions proposals -> subv_orig:{app_sum}, après fusion:{'{:,.1f}'.format(subv_p['requestedGrant'].sum())}")
        
    part_prop = (part_step.loc[part_step.inProposal==True].merge(subv_p, how='inner')[
                ['project_id',  'generalPic', 'proposal_orderNumber', 
                'erc_role', 'cordis_is_sme',  'flag_entreprise', 
                'cordis_type_entity_code', 'cordis_type_entity_name_fr', 'cordis_type_entity_name_en', 'cordis_type_entity_acro', 
                'participation_nuts', 'region_1_name', 'region_2_name', 'regional_unit_name',
                'country_code', 'proposal_country_code_mapping', 'extra_joint_organization', 'role', 'partnerType', 'calculated_fund', 'fund_ent_erc']]
                .rename(columns={'proposal_orderNumber':'orderNumber', 
                                 'proposal_country_code_mapping':'country_code_mapping'})
                .assign(stage='evaluated'))

    if '{:,.1f}'.format(part_prop['calculated_fund'].sum())==app_sum:
        print("4- Etape part_prop/subv_p -> calculated_proposal_subv OK")
    else:
        print(f"5- ATTENTION ! bien vérifier le volume de calculated_proposal_subv dans PARTICIPATION FINALE :{'{:,.1f}'.format(part_prop['calculated_fund'].sum())}, subv_orig:{app_sum}")

    print(f"- size part_prop: {len(part_prop)}")
    return part_prop

