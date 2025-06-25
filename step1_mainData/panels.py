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


def parse_and_fill_text_to_dataframe():
    import numpy as np, pandas as pd
    from config_path import PATH_SOURCE
    from constant_vars import FRAMEWORK
    
    test=f"{PATH_SOURCE}{FRAMEWORK}/msca_Keywords.txt"
    with open(test, "r", encoding="utf-8") as f:
        raw_text = f.read()

    lines = [line.strip() for line in raw_text.strip().split('\n') if line.startswith('|')]
    rows = [line.strip('|').split('|') for line in lines]

    clean_filtered_rows = [
        [cell.strip() for cell in row]
        for row in rows
        if not any('---' in cell or 'scientific panel' in cell.lower() for cell in row)
    ]

    # Step 3: Use the original header (before filtering and cleaning)
    header = [cell.strip() for cell in rows[0]]

    # Step 4: Create DataFrame
    df = pd.DataFrame(clean_filtered_rows, columns=header)


    # # Step 4: Forward-fill missing values
    df = df.replace('', np.nan).fillna(method='ffill').loc[~df["Level 2 keywords"].isnull()]

    # Extract the part before the parenthesis
    df["panel_regroupement_name"] = df["Scientific panel"].str.extract(r"^(.*?)\s*\(")

    # Optional: also extract the code as before
    df["panel_regroupement_code"] = df["Scientific panel"].str.extract(r"\((.*?)\)")

    df[["panel_code_1", "panel_name_1"]] = df["Level 1 keywords"].str.split('-', n=1, expand=True)

    df = (df[['panel_regroupement_code', 'panel_regroupement_name', 'panel_code_1', 'panel_name_1', 'Level 2 keywords']]
        .rename(columns={"Level 2 keywords":"panel_keywords"})
    )
    # print(df)
    json_output_path = "data_files/msca_keywords.json"
    df.to_json(json_output_path, orient="records", indent=2, force_ascii=False)