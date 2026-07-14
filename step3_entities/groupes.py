from paths import PATH_REF


def groupe_treatment(df, output):
    import pandas as pd, numpy as np, openpyxl, warnings, copy
    warnings.simplefilter("ignore")
    # PATH_REF = "C:/Users/zfriant/Documents/OneDrive/PCRI/eCorda_datas/datas_reference/"

    liste_groupe = pd.read_excel(f"{PATH_REF}_groupes_liste.xlsx", dtype=object, keep_default_na=False, sheet_name = "liste")
    liste_groupe = liste_groupe[liste_groupe['HE_keep']!='False']
    ge = openpyxl.load_workbook(f"{PATH_REF}{df}.xlsm").sheetnames[1:]
#     ge = liste_pcri
    
    gr = pd.DataFrame()
    verif = pd.DataFrame()

    for i in ge:
        if i in list(liste_groupe['GROUPE']):
            x = pd.read_excel(f"{PATH_REF}{df}.xlsm", sheet_name=i, dtype=str)
            
            if len(x)>0:
                x.dropna(axis = 0, how = 'all', inplace = True)
                
                if 'Identifiant unité légale' in x.columns:
                    x = x.rename(columns={'Identifiant unité légale':'siren'})
                elif 'Unité légale' in x.columns:
                    x = x.rename(columns={'Unité légale':'siren'})
                    
                if 'Unité légale étrangère ?' in x.columns:  
                    x = x.loc[x['Unité légale étrangère ?']=="Non"]  
                else:
                    pass
    
                if 'Taux détention' in x.columns:
                    x = x.assign(detention = x['Taux détention'].str.replace(',', '.').astype(float))
                elif 'Taux integration' in x.columns:
                    x = x.assign(detention = x['Taux integration'].str.replace(',', '.').astype(float))
                    
                x = x.loc[~(x['detention'] < 50.)]
                        
                print(i, end=",")
                verif = pd.concat([verif, x], ignore_index=True)
            

                x['GROUPE'] = i
                x = x.merge(liste_groupe, how='inner', on="GROUPE")
                gr = pd.concat([gr, x], ignore_index=True)
        else:
            pass

    print(f"\n1 - Nb de groupes dans gr: {gr.ordre.nunique()}\nGroupes non traités (n'existent plus): {set(ge)-set(gr.GROUPE.unique())}")
    
    # verif_na <- gr[apply(gr, 2, function(x) any(is.na(x)))]
    if gr[gr.siren.isnull()].empty:
        pass
    else:
        print(f"2 - Attention des siren sont null\n{gr.loc[gr.siren.isnull(), ['Raison sociale', 'groupe_acronym', 'ordre']]}")
        gr=gr.loc[~gr.siren.isnull()]

    # # contrôle de la longueur des siren ; ajout de 0 devant si < 9
    for i in gr.columns:
        if gr[i].dtype == 'str':
            gr[i] = gr[i].map(str.strip)
        else:
            pass
        
    
    if any(9-gr.siren.str.len())>0:
        gr['siren'] = gr['siren'].str.rjust(9, fillchar='0')
    else:
        print(f"3 - autre pb avec le siren {gr[gr.siren.str.len()!=9][['siren', 'GROUPE', 'long']]}")


    groupe = copy.deepcopy(gr)[['siren', 'Etat', 'Date de fin', 'GROUPE', 'ordre', 'ex_groupe', 'groupe_name', 'groupe_acronym', 'groupe_sector']].drop_duplicates()
    print(f"4 - size groupe {len(groupe)}")

    groupe['n'] = groupe.groupby('siren', dropna=False)['siren'].transform('count')

    groupe = groupe.loc[~((groupe.n>1) & ((groupe.Etat.isin(["Cessée", "Inactive économique", "Inactive statistique"])) | ~(groupe['Date de fin'].isnull())))]
    groupe['n'] = groupe.groupby('siren', dropna=False)['siren'].transform('count')

    if any(groupe.n>1):
        print(f"vérifier dans groupe les doublons n>1\n{groupe[groupe.n>1]}")
    else:
        print(f"ok -> {len(groupe)}")


    groupe['groupe_id'] = "gent"+groupe.ordre.map(str)
    groupe = groupe[['siren', 'groupe_name', 'groupe_acronym', 'groupe_id', 'groupe_sector']]    
    groupe.siren = groupe.siren.astype(str)
    
    file_name = f"{PATH_REF}{output}.pkl"
    with open(file_name, 'wb') as file:
        pd.to_pickle(groupe, file)
    
    return groupe


def merge_groupe(entities_tmp, groupe):
    print("\n### merge avec GROUPE")
    import pandas as pd

    tmp = (entities_tmp
        .loc[entities_tmp['siren_all'].notnull(), ['siren_main', 'siren_all']])
    tmp['siren'] = tmp['siren_all'].str.split(';')
    tmp = tmp.explode('siren').drop_duplicates()


    tmp = pd.merge(tmp, groupe, how='left', on='siren')
    tmp = tmp.loc[tmp['groupe_id'].notnull()]

    tmp = tmp.groupby(['siren_main', 'siren_all'], as_index=False).agg({
        'groupe_name': lambda x: ';'.join(x.unique()),
        'groupe_acronym': lambda x: ';'.join(x.unique()),
        'groupe_sector': lambda x: ';'.join(x.unique()),
        'groupe_id': lambda x: ';'.join(x.unique())
    })

    if any(tmp['groupe_id'].str.contains(';', na=False)):
        print("ATTENTION siren dans plusieurs groupes -> à vérifier date de fin \n", tmp.loc[tmp['groupe_id'].str.contains(';', na=False)])

    print(f"size entities_tmp befor merge groupe {len(entities_tmp)}")
    entities_tmp = entities_tmp.merge(tmp, how='left', on=['siren_main', 'siren_all'])
    print(f"size entities_tmp after merge groupe {len(entities_tmp)}")
    return entities_tmp