from constant_vars import ZIPNAME, FRAMEWORK
from config_path import PATH_SOURCE
from functions_shared import unzip_zip
import pandas as pd, re, requests


def action(chemin, act_list:list):
    
    requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

    
    data = unzip_zip(ZIPNAME, chemin, "typeOfActions.json", 'utf8')
    data = pd.DataFrame(data)  
    

    data = (data[data.typeOfActionSimplifiedCode.isin(act_list)][['typeOfActionSimplifiedCode', 'typeOfActionSimplifiedDescription']]
            .drop_duplicates()
            .rename(columns={'typeOfActionSimplifiedCode':'typeOfActionCode'}))

    if len(act_list)!=len(data):
        print(f"ATTENTION ! nbre actions dans bases:{len(act_list)}, nbre actions dans nomenclature:{len(data)}")

    # # actions MSCA
    act = (data.loc[data.typeOfActionCode.str.contains('MSCA'), ['typeOfActionCode', 'typeOfActionSimplifiedDescription']]
           .drop_duplicates())
    act = act.assign(destination_detail_code=act.typeOfActionCode.str.replace('^.+MSCA-', '', regex=True, flags=re.IGNORECASE).str.upper())

   
    msca = pd.read_json(open('data_files/msca_actions.json', 'r+', encoding='utf-8'))
    msca = pd.DataFrame(msca).rename(columns={'EsCodeL':'destination_detail_code', 'EsNameL':'destination_detail_name_en'})
    act = act.merge(msca, how='left', on='destination_detail_code')

    act = act[['typeOfActionCode', 'destination_detail_code', 'destination_detail_name_en']].drop_duplicates()
    data = data.merge(act, how='left', on="typeOfActionCode") 

    act = data.loc[data['typeOfActionCode'].str.contains('EIT|EIC'), ['typeOfActionCode', 'typeOfActionSimplifiedDescription']]
    act = act.assign(CodeL2=data.typeOfActionCode.str.replace('^.*(EIT|EIC)-', '', regex=True, flags=re.IGNORECASE).str.upper())
    act.loc[act.CodeL2.str.contains('HORIZON'), 'CodeL2'] = act.CodeL2.str.replace('^.*HORIZON-', '', regex=True, flags=re.IGNORECASE).str.upper()
    act = act.assign(NameL2=data.typeOfActionSimplifiedDescription.str.replace('HORIZON', '', flags=re.IGNORECASE))

    act = act[['typeOfActionCode', 'CodeL2', 'NameL2']].drop_duplicates()
    data = data.merge(act, how='left', on="typeOfActionCode").rename(columns={'CodeL2':'action_code2', 'NameL2':'action_name2'})

    data = data.assign(action_code=None)
    data = data.assign(action_name=None)

    data.loc[data.typeOfActionCode.str.contains('MSCA') , 'action_code'] =  'MSCA'
    data.loc[data.typeOfActionCode.str.contains('MSCA') , 'action_name'] =  'MSCA actions'
    data.loc[data.typeOfActionCode.str.contains('ERC') , 'action_code'] =  'ERC'
    data.loc[data.typeOfActionCode.str.contains('ERC') , 'action_name'] =  'ERC actions'
    data.loc[data.typeOfActionCode.str.contains('EIT') , 'action_code'] =  'EIT'
    data.loc[data.typeOfActionCode.str.contains('EIT') , 'action_name'] =  'EIT actions'
    data.loc[data.typeOfActionCode.str.contains('EIC') , 'action_code'] =  'EIC'
    data.loc[data.typeOfActionCode.str.contains('EIC') , 'action_name'] =  'EIC actions'
    data.loc[data.typeOfActionCode.str.contains('HORIZON-COFUND') , 'action_code'] =  'COFUND'
    data.loc[data.typeOfActionCode.str.contains('HORIZON-COFUND') , 'action_name'] =  'COFUND actions'

    data.loc[data.action_code.isnull(), 'action_code'] = data.typeOfActionCode.str.replace('^HORIZON(-JU)?-', '', regex=True, flags=re.IGNORECASE).str.upper()
    data.loc[data.action_name.isnull(), 'action_name'] = data.typeOfActionSimplifiedDescription.str.replace('^HORIZON( )?(JU)? ', '', regex=True, flags=re.IGNORECASE)

    data.loc[data.typeOfActionCode.str.contains('JU'), 'action_code2'] = data.typeOfActionCode.str.replace('^HORIZON-', '', regex=True, flags=re.IGNORECASE).str.upper()
    data.loc[data.typeOfActionCode.str.contains('JU'), 'action_name2'] = data.typeOfActionSimplifiedDescription.str.replace('^HORIZON', '', regex=True, flags=re.IGNORECASE)

    data.loc[data.typeOfActionCode.str.contains('KIC'), 'action_code'] = 'KICS'
    data.loc[data.typeOfActionCode.str.contains('KIC'), 'action_name'] = 'Knowledge and Innovation Communities'
    
    data = data.map(lambda x: x.strip() if isinstance(x, str) else x)
    
    return data



def merged_actions(df):
    ''' table action '''
    # création de la liste des TOA présents dans propasals et projects

    if len(df[df['typeOfActionCode'].isnull()])>0:
        if any(df[df['typeOfActionCode'].isnull()][['call_id']].drop_duplicates()=='HORIZON-HLTH-2022-DISEASE-06-two-stage'):
            print(f"1 - call sans action:{df[df['typeOfActionCode'].isnull()][['call_id']].drop_duplicates()}")
            df.loc[df['call_id']=='HORIZON-HLTH-2022-DISEASE-06-two-stage', 'typeOfActionCode'] = 'HORIZON-RIA'

    df.loc[:,'typeOfActionCode']=df.loc[:,'typeOfActionCode'].str.split('\\').str[0]

    act_code = list(df.typeOfActionCode.unique())
    act_code = [item for item in act_code if not(pd.isnull(item)) == True]

    actions = action(f"{PATH_SOURCE}{FRAMEWORK}/", act_code)

    # liste seules les TOA dans les bases proposals et projects + ajout table nomenclature
    actions = (actions[actions['typeOfActionCode'].isin(act_code)][
        ['typeOfActionCode', 'action_code', 'action_name', 'action_code2', 'action_name2', 'destination_detail_code','destination_detail_name_en']]
        .drop_duplicates())

    df = (df
            .merge(pd.DataFrame(actions), how='left', on='typeOfActionCode')
            .drop_duplicates())
    print(f"size merged after add actions: {len(df)}")

    if len(df[df['typeOfActionCode'].isnull()])>0:
        print(f"ATTENTION ! il reste des calls sans actions: {df.loc[df['typeOfActionCode'].isnull(), ['stage','call_id']].drop_duplicates()}")

    # actions.to_csv(f"{PATH_CLEAN}actions_current.csv", index=False, encoding="UTF-8", sep=";", na_rep='')