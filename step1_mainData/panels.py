# table panels'''
def merged_panels(df):
    import json, pandas as pd
    from config_path import PATH_WORK

    print("### PANELS")
    with open('data_files/panels.json', 'r', encoding='UTF-8') as pl:
        panels = json.load(pl)
        
    panels_proj=list(filter(None, df['panel_code'].unique()))
    liste_panels=[elem['panel_code'] for elem in panels]

    no_panel=[x for x in panels_proj if x not in liste_panels] 
    if no_panel:
        tmp = df[df['panel_code'].isin(no_panel)].sort_values(['call_id', 'topicCode'])
        with pd.ExcelWriter(f"{PATH_WORK}panels_for_other_pillars.xlsx") as writer:
            for i in tmp.call_id.unique():
                tmp.loc[tmp['call_id']==i, ['topicCode', 'panel_code', 'project_id', 'acronym']].to_excel(writer, sheet_name=f'{i}', index=False)

        # df[df['panel_code'].isin(no_panel)][['topicCode', 'panel_code', 'project_id', 'acronym']].drop_duplicates().to_excel
        print("- code panel existant pour d'autres programmes que erc/msca: données exportées\n")    

    df = df.merge(pd.DataFrame(panels), how='left', on='panel_code').drop_duplicates()
    print(f"- size merged after add panels: {len(df)}")
    return df