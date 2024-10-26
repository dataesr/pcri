def merged_partApp(app1, part):
    import pandas as pd, numpy as np

    part2 = part[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'n_part']].drop_duplicates()
    app2 = app1[['project_id', 'orderNumber', 'generalPic', 'participant_pic', 'n_app']].drop_duplicates()
    print(f'app2: {len(app2)} part2:{len(part2)}')
    cols_part = part2.columns
    cols_app = app2.columns

    '''proposal uniquement'''
    lien1 = (app2
            .merge(part2[['project_id']], how='outer', on='project_id', indicator=True)
            .query('_merge == "left_only"')
            .drop('_merge', axis=1)
            .assign(base_only='prop_only'))
    app2 = (app2.merge(lien1[['project_id']], how='outer', on='project_id', indicator=True)
            .query('_merge == "left_only"')
            .drop('_merge', axis=1))
    print(f'applicant uniquement lien1: {len(lien1)} reste à croiser -> app2: {len(app2)}')

    '''jointure parfaite'''
    lien2 = part2.merge(app2, how='inner')
    print(f'jointure parfaite -> lien2: {len(lien2)}')

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

    part4 = part3.merge(lien3[cols_part], how='outer', indicator=True).query('_merge == "left_only"').drop('_merge', axis=1)
    df_app = lien3[['project_id', 'generalPic', 'participant_pic', 'orderNumber_p']].rename(columns=({'orderNumber_p':'orderNumber'}))
    app4 = (app3.merge(df_app, how='outer', indicator=True)
            .query('_merge == "left_only"')
            .drop('_merge', axis=1))
    print(f'reste à croiser -> app4: {len(app4)} part4: {len(part4)}')


    '''jointure sans participant_pic'''
    lien4 = part4.merge(app4, how='inner', on=['project_id', 'generalPic', 'orderNumber'], suffixes=('', '_p'))
    print(f'jointure sans participant_pic -> lien4: {len(lien4)}')

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
            print('code pour ajouter lien6 à la table lien finale')
    else:
        '''jointure juste participant_pic'''
        lien6 = part5.merge(app5, how='inner', on=['project_id', 'participant_pic'], suffixes=('', '_p'))
        print(f'jointure avec seulement participant_pic -> lien6: {len(lien6)}')
        if len(lien6)>0:
            print('code pour ajouter lien6 à la table lien finale')


    lien = pd.concat([lien1, lien2, lien3, lien4, app5, part5], ignore_index=True).drop_duplicates()
    lien = lien.assign(inProposal=np.where(~lien['n_part'].isnull() & lien['n_app'].isnull(), False, True))
    lien = lien.assign(inProject=np.where(lien['n_part'].isnull() & ~lien['n_app'].isnull(), False, True))

    lien['orderNumber_p'] = np.where((lien['orderNumber_p'].isnull()) & (lien['inProposal']==True), lien['orderNumber'], lien['orderNumber_p'])
    lien['orderNumber'] = np.where(lien['inProject']==False, None, lien['orderNumber'])

    lien['participant_pic_p'] = np.where((lien['participant_pic_p'].isnull()) & (lien['inProposal']==True), lien['participant_pic'], lien['participant_pic_p'])
    lien['participant_pic'] = np.where(lien['inProject']==False, None, lien['participant_pic'])

    for x in lien.columns:
        if pd.api.types.infer_dtype(lien[x])=='string':
            lien.loc[:,x]=np.where(lien.loc[:,x].isnull(), None, lien.loc[:,x])
        
    lien.columns = ['proposal_'+k[0:-2] if k[-2:] == '_p' else k for k in list(lien.columns)]
    lien['calculated_pic'] = np.where(~lien['participant_pic'].isnull(), lien['participant_pic'], lien['proposal_participant_pic'])

    lien['projNlien'] = lien.groupby(['project_id', 'proposal_orderNumber', 'generalPic', 'calculated_pic'], dropna = False).pipe(lambda x: x.orderNumber.transform('nunique'))
    lien['propNlien'] = lien.groupby(['project_id', 'orderNumber', 'generalPic', 'calculated_pic'], dropna = False).pipe(lambda x: x.proposal_orderNumber.transform('nunique'))
    lien.loc[lien['projNlien']==0, 'projNlien']=1
    lien.loc[lien['propNlien']==0, 'propNlien']=1

    print(f'lien_final: {len(lien)}') 
    length_lien=len(lien)
    # print(lien[lien['n_lien']>1])

    # verif que chaque obs contient un calculated pic
    lien_no_pic=len(lien[lien['calculated_pic'].isnull()])
    if lien_no_pic > 0:
        print(f'attention {lien_no_pic} entités dans la table LIEN n\'ont pas de pic secondaire' )

    del app2, app3, app4, app5, app6, part2, part3, part4, part5, part6, lien1, lien2, lien3, lien4, lien5, lien6
    return lien