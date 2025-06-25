def merged_partApp(app1, part):
    import pandas as pd, numpy as np
    print("\n### create LIEN")

    part2 = part[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'n_part']].drop_duplicates()
    app2 = app1[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'n_app']].drop_duplicates()
    print(f'app2: {len(app2)} part2:{len(part2)}')
    cols_part = part2.columns
    cols_app = app2.columns

    lien = pd.DataFrame(columns=['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'n_app',
       'base_only', 'n_part', 'orderNumber_p', 'participant_pic_p'])

    '''proposal uniquement'''
    lien1 = (app2
            .merge(part2[['project_id']], how='outer', on='project_id', indicator=True)
            .query('_merge == "left_only"')
            .drop('_merge', axis=1)
            .assign(base_only='prop_only'))
    app2 = (app2.merge(lien1[['project_id']], how='outer', on='project_id', indicator=True)
            .query('_merge == "left_only"')
            .drop('_merge', axis=1))
    print(f'- applicant uniquement lien1: {len(lien1)} reste à croiser -> app2: {len(app2)}')
    lien = pd.concat([lien, lien1], ignore_index=True)

    '''jointure parfaite'''
    lien2 = part2.merge(app2, how='inner')
    print(f'jointure parfaite -> lien2: {len(lien2)}')
    lien = pd.concat([lien, lien2], ignore_index=True)

    part3 = (part2.merge(lien2[cols_part], how='outer', indicator=True)
             .query('_merge == "left_only"')
             .drop('_merge', axis=1))
    app3 = (app2.merge(lien2[cols_app], how='outer', indicator=True)
            .query('_merge == "left_only"')
            .drop('_merge', axis=1))
    print(f'reste à croiser -> app3: {len(app3)} part3: {len(part3)}')

    '''jointure sans orderNumber'''
    lien3 = part3.merge(app3, how='inner', on=['project_id', 'generalPic', 'participant_pic'], suffixes=('', '_p'))
    print(f'jointure sans ordernumber -> lien3: {len(lien3)}')
    if len(lien3)>0:
        lien = pd.concat([lien, lien3], ignore_index=True)

    part4 = part3.merge(lien3[cols_part], how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
    df_app = lien3[['project_id', 'generalPic', 'participant_pic', 'orderNumber_p']].rename(columns=({'orderNumber_p':'orderNumber'}))
    app4 = (app3.merge(df_app, how='outer', indicator=True)
            .query('_merge == "left_only"')
            .drop('_merge', axis=1))
    print(f'reste à croiser -> app4: {len(app4)} part4: {len(part4)}')


    '''jointure sans participant_pic'''
    lien4 = part4.merge(app4, how='inner', on=['project_id', 'generalPic', 'orderNumber'], suffixes=('', '_p'))
    print(f'jointure sans participant_pic -> lien4: {len(lien4)}')
    if len(lien4)>0:
        lien = pd.concat([lien, lien4], ignore_index=True)

    part5 = (part4.merge(lien4[cols_part], how='outer', indicator=True)
             .query('_merge == "left_only"')
             .drop('_merge', axis=1))
    df_app = lien4[['project_id', 'generalPic', 'participant_pic_p', 'orderNumber']].rename(columns=({'participant_pic_p':'participant_pic'}))
    app5 = (app4.merge(df_app, how='outer', indicator=True)
            .query('_merge == "left_only"')
            .drop('_merge', axis=1))
    print(f'reste à croiser -> app5: {len(app5)} part5: {len(part5)}')

    '''jointure juste generalPic'''
    lien5 = part5.merge(app5, how='inner', on=['project_id', 'generalPic'], suffixes=('', '_p'))
    print(f'jointure seulement avec generalpic -> lien5: {len(lien5)}')
    if len(lien5)>0:
        lien = pd.concat([lien, lien5], ignore_index=True)

    part6 = (part5.merge(lien5[cols_part], how='outer', indicator=True)
                .query('_merge == "left_only"')
                .drop('_merge', axis=1))
    df_app = lien5[['project_id', 'generalPic', 'participant_pic_p', 'orderNumber_p']].rename(columns=({'orderNumber_p':'orderNumber', 'participant_pic_p':'participant_pic'}))
    app6 = (app5.merge(df_app, how='outer', indicator=True)
            .query('_merge == "left_only"')
            .drop('_merge', axis=1))
    print(f'reste à croiser -> app6: {len(app6)} part6: {len(part6)}')
    
    '''jointure juste participant_pic'''
    lien6 = (part6
            .merge(app6, how='inner', on=['project_id', 'participant_pic'], suffixes=('', '_p'))
            .assign(base_only='a_joindre'))
    print(f'jointure avec seulement participant_pic -> lien6: {len(lien6)}')
    if len(lien6)>0:
        lien = pd.concat([lien, lien6], ignore_index=True)
        print('code pour ajouter lien6 à la table lien finale')

        part7 = (part6.merge(lien[cols_part], how='outer', indicator=True)
                    .query('_merge == "left_only"')
                    .drop('_merge', axis=1))
        df_app = lien[['project_id', 'generalPic', 'participant_pic_p', 'orderNumber_p']].rename(columns=({'orderNumber_p':'orderNumber', 'participant_pic_p':'participant_pic'}))
        app7 = (app6.merge(df_app, how='outer', indicator=True)
                .query('_merge == "left_only"')
                .drop('_merge', axis=1))
        print(f'reste à croiser -> app7: {len(app7)} part7: {len(part7)} faire codepour lien7')

    elif (len(part6)>0)|(len(app6)>0):
        p = pd.concat([part6, app6], ignore_index=True)
        lien = pd.concat([lien, p], ignore_index=True)
    # else:
    #     '''jointure juste participant_pic'''
    #     lien6 = part5.merge(app5, how='inner', on=['project_id', 'participant_pic'], suffixes=('', '_p'))
    #     print(f'jointure avec seulement participant_pic -> lien6: {len(lien6)}')
    #     if len(lien6)>0:
    #         lien = pd.concat([lien, lien6], ignore_index=True)
    #         print('code pour ajouter lien6 à la table lien finale')


    # lien = pd.concat([lien1, lien2, lien3, lien4, app5, part5], ignore_index=True).drop_duplicates()
    lien = lien.assign(inProposal=np.where(~lien['n_part'].isnull() & lien['n_app'].isnull(), False, True)).drop_duplicates()
    lien = lien.assign(inProject=np.where(lien['n_part'].isnull() & ~lien['n_app'].isnull(), False, True))

    lien['orderNumber_p'] = np.where((lien['orderNumber_p'].isnull()) & (lien['inProposal']==True), lien['orderNumber'], lien['orderNumber_p'])
    lien['orderNumber'] = np.where(lien['inProject']==False, None, lien['orderNumber'])

    lien['participant_pic_p'] = np.where((lien['participant_pic_p'].isnull()) & (lien['inProposal']==True), lien['participant_pic'], lien['participant_pic_p'])
    lien['participant_pic'] = np.where(lien['inProject']==False, None, lien['participant_pic'])

    for x in lien.columns:
        if pd.api.types.infer_dtype(lien[x])=='string':
            lien.loc[:,x]=np.where(lien.loc[:,x].isnull(), None, lien.loc[:,x])
        
    lien.columns = ['applicant_'+k[0:-2] if k[-2:] == '_p' else k for k in list(lien.columns)]
    lien['calculated_pic'] = np.where(~lien['participant_pic'].isnull(), lien['participant_pic'], lien['applicant_participant_pic'])

    lien['projNlien'] = lien.groupby(['project_id', 'applicant_orderNumber', 'generalPic', 'calculated_pic'], dropna = False).pipe(lambda x: x.orderNumber.transform('nunique'))
    lien['propNlien'] = lien.groupby(['project_id', 'orderNumber', 'generalPic', 'calculated_pic'], dropna = False).pipe(lambda x: x.applicant_orderNumber.transform('nunique'))
    lien.loc[lien['projNlien']==0, 'projNlien']=1
    lien.loc[lien['propNlien']==0, 'propNlien']=1

    print(f'- size lien: {len(lien)}') 
    length_lien=len(lien)
    # print(lien[lien['n_lien']>1])

#     lien.loc[lien.inProject==True, 'participation_linked'] = lien['project_id']+"-"+lien['orderNumber']
#     lien.loc[lien.inProposal==True, 'participation_linked'] = lien['project_id']+"-"+lien['proposal_orderNumber']
    
    # add countryCode
    lien = (lien
            .merge(part[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'country_code_mapping']],
                   how='left', on=['project_id', 'orderNumber', 'generalPic', 'participant_pic']))

    lien = (lien
            .merge(app1[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'country_code_mapping']], 
                   how='left', left_on=['project_id', 'applicant_orderNumber', 'generalPic', 'applicant_participant_pic'],
                   right_on=['project_id', 'orderNumber', 'generalPic', 'participant_pic'],
                   suffixes=[ '','.y'])
            .drop(columns=[ 'participant_pic.y', 'orderNumber.y'])
            .rename(columns={'country_code_mapping.y':'applicant_country_code_mapping'}))

    lien.loc[lien.country_code_mapping.isnull(), 'country_code_mapping'] = lien.loc[lien.country_code_mapping.isnull(), 'applicant_country_code_mapping']

    if any(lien.country_code_mapping.isnull()):
        print(f"- ATTENTION {lien[lien.country_code_mapping.isnull()].generalPic.nunique()} countryCode missing {lien[lien.country_code_mapping.isnull()].generalPic.unique()}")


    #add contribution 
    rename_dict = {col: 'applicant_' + col for col in ['orderNumber', 'participant_pic', 'country_code_mapping', 'role', 'partnerType', 'erc_role']}

    lien=(lien
            .merge(app1[['project_id', 'generalPic', 'requestedGrant', 'orderNumber', 'participant_pic', 'country_code_mapping', 'role', 'partnerType', 'erc_role']]
                        .rename(columns=rename_dict),
            how='left', 
            on=['project_id', 'applicant_orderNumber', 'generalPic', 'applicant_participant_pic', 'applicant_country_code_mapping']))

    lien['app_fund'] = (np.where((lien['projNlien']>1.), lien['requestedGrant']/lien['projNlien'], lien['requestedGrant']))
   
    if app1['requestedGrant'].sum()==lien['app_fund'].sum():
        print("subventions app1/lien: ok")
    else:
        print(f"- check difference between requestGrant and app_fund: {'{:,.1f}'.format(app1['requestedGrant'].sum())}, {'{:,.1f}'.format(lien['app_fund'].sum())}")


    lien=(lien.merge(part[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'country_code_mapping', 'role', 'partnerType', 'erc_role', 'euContribution', 'netEuContribution']],
    how='left', 
    on=['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'country_code_mapping']))

    lien['beneficiary_fund'] = (np.where((lien['propNlien']>1.), lien['euContribution']/lien['propNlien'], lien['euContribution']))
    if part['euContribution'].sum()==lien['beneficiary_fund'].sum():
        print("subventions benef/lien: ok")
    else:
        print(f"- check difference between euContribution and benef_fund: {'{:,.1f}'.format(part['euContribution'].sum())}, {'{:,.1f}'.format(lien['beneficiary_fund'].sum())}")
    

    lien['part_fund'] = (np.where((lien['propNlien']>1.), lien['netEuContribution']/lien['propNlien'], lien['netEuContribution']))
    if part['netEuContribution'].sum()==lien['part_fund'].sum():
        print("subventions part/lien: ok")
    else:
        print(f"- check difference between netEuContribution and part_fund: {'{:,.1f}'.format(part['netEuContribution'].sum())}, {'{:,.1f}'.format(lien['part_fund'].sum())}")



    # verif que chaque obs contient un calculated pic
    lien_no_pic=len(lien[lien['calculated_pic'].isnull()])
    if lien_no_pic > 0:
        print(f'1- attention {lien_no_pic} entités dans la table LIEN n\'ont pas de pic secondaire' )

    del app2, app3, app4, app5, app6, part2, part3, part4, part5, part6, lien1, lien2, lien3, lien4, lien5, lien6
    return lien