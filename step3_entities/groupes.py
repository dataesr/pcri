
def groupe_treatment(df, output):
    import pandas as pd, numpy as np, xlrd, openpyxl, warnings, copy
    warnings.simplefilter("ignore")
    PATH_REF = "C:/Users/zfriant/Documents/OneDrive/PCRI/eCorda_datas/datas_reference/"

    liste = pd.read_excel(f"{PATH_REF}_groupes_liste.xlsx", dtype=object, keep_default_na=False, sheet_name = "liste")
    ge = openpyxl.load_workbook(f"{PATH_REF}{df}.xlsm").sheetnames[1:]
#     ge = liste_pcri
    
    gr = pd.DataFrame()
    verif = pd.DataFrame()

    for i in ge:
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
                x = x.assign(detention = x['Taux détention'].astype(float))
            elif 'Taux integration' in x.columns:
                x = x.assign(detention = x['Taux integration'].astype(float))
                
            x = x.loc[~(x['detention'] < 50)]
                      
            print(i, end=",")
            verif = pd.concat([verif, x], ignore_index=True)
        

            x['GROUPE'] = i
            x = x.merge(liste, how='inner', on="GROUPE")
            gr = pd.concat([gr, x], ignore_index=True)

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


    groupe = copy.deepcopy(gr)[['siren', 'Etat', 'Date de fin', 'GROUPE', 'ordre', 'ex_groupe', 'groupe_name', 'groupe_acronym', 'groupe_sector']]
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
    entities_tmp=entities_tmp.merge(groupe, how='left', on='siren')

    if any(entities_tmp.siren.str.contains(';', na=False)):
        print("ATTENTION faire code pour traiter deux siren différents -> ce qui serait bizarre qu'il y ait 2 siren")
    # else:
    #     entities_tmp.loc[~entities_tmp.groupe_id.isnull(), 'entities_name_source']= entities_tmp.entities_name
    #     entities_tmp.loc[~entities_tmp.groupe_id.isnull(), 'entities_acronym_source']= entities_tmp.entities_acronym
    #     # entities_tmp.loc[~entities_tmp.groupe_id.isnull(), 'entities_id']= entities_tmp.groupe_id
    #     # entities_tmp.loc[~entities_tmp.groupe_id.isnull(), 'entities_acronym'] = entities_tmp.groupe_acronym
    #     # entities_tmp.loc[~entities_tmp.groupe_id.isnull(), 'entities_name'] = entities_tmp.groupe_name

    #     # entities_tmp.loc[entities_tmp.groupe_id.str.contains('gent', na=False), 'siren_cj'] = 'GE_ENT'

        # entities_tmp = entities_tmp.drop(['groupe_name','groupe_acronym'], axis=1).drop_duplicates()
    print(f"taille de entities_tmp après groupe {len(entities_tmp)}")
    return entities_tmp