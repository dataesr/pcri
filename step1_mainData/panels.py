def panel_lib_update(pdf_file):
    from config_path import PATH
    import PyPDF2, re, json

    with open('data_files/panels.json', 'r', encoding='UTF-8') as pl:
        panels = json.load(pl)

    def parse_pdf_to_list(pdf_path):
        # Initialize the list to hold text from each page
        pdf_text = []

        # Open the PDF file in read-binary mode
        with open(pdf_path, 'rb') as file:
            reader = PyPDF2.PdfReader(file)

            # Extract text from each page and add it to the list
            for page in reader.pages:
                text = page.extract_text()
                if text:  # Check if text was successfully extracted
                    pdf_text.append(text)

        return pdf_text

    pdf_path = f'{PATH}ERC/Fichiers ERC/{pdf_file}.pdf'
    text_large = parse_pdf_to_list(pdf_path)

    liste_texte=[]
    for text in text_large:
        # Define the pattern to match lines with two letters followed by a number
        pattern = re.compile(r'^\s?([A-Za-z]{2}\d+)\s+(.*)', re.MULTILINE)

    #     # Find all matches in the text
        matches = pattern.finditer(text)
        relevant_lines = []

        for match in matches:
    #         # Append the matched line
            relevant_lines.append(match.group(0).strip())

    #         # Find the next lines until a line with two letters, one number, and an underscore is found
            start_pos = match.end()
            text_slice = text[start_pos:]

    #         # Define the pattern to match the end condition
            end_pattern = re.compile(r'^[A-Za-z]{2}\d+_', re.MULTILINE)
            end_match = end_pattern.search(text_slice)

            if end_match:
                end_pos = end_match.start()
                relevant_lines.extend(line.strip() for line in text_slice[:end_pos].split('\n') if line.strip())

        liste_texte.extend(relevant_lines)

    # Initialize an empty list to store the dictionaries
    # panels = []

    # Regular expression to match the code_panel pattern
    code_pattern = re.compile(r'^[A-Za-z]{2}\d+')

    i = 0
    while i < len(liste_texte):
        line = liste_texte[i]
        # Check if the line starts with a code_panel pattern
        if code_pattern.match(line.split()[0]):
            # Extract code_panel and panel_name
            parts = line.split(maxsplit=1)
            code_panel = parts[0].replace(r'\s{2:}', r'\s{1}').strip()
            panel_name = parts[1].replace(r'\s{2:}', r'\s{1}').strip() if len(parts) > 1 else ''

            # Initialize description
            description_lines = []

            # Move to the next line and start collecting description
            i += 1
            while i < len(liste_texte) and not code_pattern.match(liste_texte[i].split()[0]):
                description_lines.append(liste_texte[i])
                i += 1

            # Join description lines into a single string
            description = ' '.join(description_lines)

            # Create a dictionary for the current panel
            panel_dict = {
                'panel_code': code_panel,
                'panel_name': panel_name,
                'panel_description': description
            }


            exists = False
            for index, panel in enumerate(panels):
                if panel["panel_code"] == panel_dict["panel_code"]:
                    # Replace the existing dictionary
                    panels[index] = {
                        'panel_code': panel_dict["panel_code"],
                        'panel_name': panel_dict["panel_name"],
                        'panel_description': panel_dict["panel_description"]
                    }
                    exists = True
                    break

            # If it doesn't exist, append the new dictionary to the list
            if not exists:
                panels.append({
                    'panel_code': panel_dict["panel_code"],
                    'panel_name': panel_dict["panel_name"],
                    'panel_description': panel_dict["panel_description"]
                })
        else:
            i += 1

    with open('data_files/panels.json', 'w', encoding='UTF-8') as pl:
        json.dump(panels, pl, indent=4)



# table panels'''
def merged_panels(df):
    import json, pandas as pd
    from config_path import PATH_WORK

    # "panel_regroupement"
    erc_group=pd.DataFrame.from_dict(data={
        "panel_regroupement_code": ["PE", "LS", "SH"],
        "panel_regroupement_name": ["Physical Sciences and Engineering", "Life Science", "Social Sciences and Humanities"]}, 
        orient='columns')

    print("### PANELS")
    with open('data_files/panels.json', 'r', encoding='UTF-8') as pl:
        panels = json.load(pl)

    p=df.loc[(df.call_id.str.lower().str.contains('erc|msca'))&(~df.panel_code.isnull())].panel_code.unique()   
    liste_panels=[elem['panel_code'] for elem in panels]

    if any(list(set(p).difference(set(liste_panels)))):
        return print(f"- ATTENTION UPDATE ! add panel_code to data_files/panels.json {list(set(p).difference(set(liste_panels)))}")
         
    panels_proj=list(filter(None, df['panel_code'].unique()))
    no_panel=[x for x in panels_proj if x not in liste_panels] 
    if no_panel:
        tmp = df[df['panel_code'].isin(no_panel)].sort_values(['call_id', 'topicCode'])
        with pd.ExcelWriter(f"{PATH_WORK}panels_for_other_pillars.xlsx") as writer:
            for i in tmp.call_id.unique():
                tmp.loc[tmp['call_id']==i, ['topicCode', 'panel_code', 'project_id', 'acronym']].to_excel(writer, sheet_name=f'{i}', index=False)

        # df[df['panel_code'].isin(no_panel)][['topicCode', 'panel_code', 'project_id', 'acronym']].drop_duplicates().to_excel
        print("- code panel existant pour d'autres programmes que erc/msca: données exportées\n")    

    df = df.merge(pd.DataFrame(panels), how='left', on='panel_code').drop_duplicates()
    df.loc[df.call_id.str.contains('ERC'), 'panel_regroupement_code'] = df.loc[df.call_id.str.contains('ERC')].panel_code.str.extract(r'([A-Za-z]{2})')[0]
    df = df.merge(erc_group, on='panel_regroupement_code', how='left')

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