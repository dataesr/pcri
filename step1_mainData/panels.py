# table panels'''
def merged_panels(df):
    import json, pandas as pd

    with open('data_files/panels.json', 'r', encoding='UTF-8') as pl:
        panels = json.load(pl)
        
    panels_proj=list(filter(None, df['panel_code'].unique()))
    liste_panels=[elem['panel_code'] for elem in panels]

    no_panel=[x for x in panels_proj if x not in liste_panels] 
    if no_panel:
        print(f"code panel dans la table MERGED absent de la nomenclature panel\n {no_panel}\n\nno_panel lié au topic\n{df[df['panel_code'].isin(no_panel)][['topicCode']].drop_duplicates()}\n")    

    df = df.merge(pd.DataFrame(panels), how='left', on='panel_code').drop_duplicates()
    print(f"size merged after add panels: {len(df)}")
    return df