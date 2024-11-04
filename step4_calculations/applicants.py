
import numpy as np

def applicants_calcul(part_step, app1):
    '''Traitement des subventions proposals -> création calculated_proposal_subv'''
    print("\n### CALCULS applicants")
    subv_p = (part_step.loc[part_step.inProposal==True, ['project_id', 'generalPic', 'proposal_participant_pic', 'proposal_orderNumber', 'projNlien']]
            .drop_duplicates()
            .merge(app1[['project_id', 'generalPic', 'participant_pic', 'orderNumber', 'role', 'partnerType', 'erc_role', 'requestedGrant']], 
                how='inner',
                left_on=['project_id', 'generalPic', 'proposal_participant_pic', 'proposal_orderNumber'],
                right_on=['project_id', 'generalPic', 'participant_pic', 'orderNumber']))

    print(f"0 - {'{:,.1f}'.format(app1['requestedGrant'].sum())}, {'{:,.1f}'.format(subv_p['requestedGrant'].sum())}")

    subv_p['calculated_fund'] = np.where(subv_p['projNlien']>1, subv_p['requestedGrant']/subv_p['projNlien'], subv_p['requestedGrant'])
    subv_p.drop(['projNlien', 'participant_pic', 'orderNumber'], axis=1, inplace=True)

    if len(subv_p)!=len(app1):
        print(f"1- ATTENTION ! {len(subv_p)-len(app1)} participations perdues entre app1 et subv_p")

    app_sum = '{:,.1f}'.format(app1['requestedGrant'].sum())
    
    if '{:,.1f}'.format(subv_p['requestedGrant'].sum()) == app_sum:
        print("2- requests grants = subventions proposals OK")
    else:
        print(f"3- ATTENTION ! Ecart subventions proposals -> subv_orig:{app_sum}, après fusion:{'{:,.1f}'.format(subv_p['requestedGrant'].sum())}")
        
    part_prop = (part_step.merge(subv_p, how='inner')[
                ['project_id',  'generalPic', 'proposal_orderNumber', 'proposal_participant_pic', 'participation_linked', 'cordis_is_sme',  'erc_role',
                'cordis_type_entity_code', 'cordis_type_entity_name_fr', 'cordis_type_entity_name_en', 'cordis_type_entity_acro', 
                'participation_nuts', 'region_1_name', 'region_2_name', 'regional_unit_name',
                'country_code', 'country_code_mapping', 'extra_joint_organization', 'role', 'partnerType', 'calculated_fund']]
                .rename(columns={'proposal_orderNumber':'orderNumber', 'proposal_participant_pic':'pic'})
                .assign(stage='evaluated'))

    if '{:,.1f}'.format(part_prop['calculated_fund'].sum())==app_sum:
        print("4- Etape part_prop/subv_p -> calculated_proposal_subv OK")
    else:
        print(f"5- ATTENTION ! bien vérifier le volume de calculated_proposal_subv dans PARTICIPATION FINALE :{'{:,.1f}'.format(part_prop['calculated_fund'].sum())}, subv_orig:{app_sum}")

    print(f"- size part_prop: {len(part_prop)}") 