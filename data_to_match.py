import pandas as pd, re, numpy as np, datetime
pd.options.mode.copy_on_write = True
from IPython.display import HTML

# from api_requests.matcher import matcher
from config_path import PATH, PATH_REF, PATH_MATCH, PATH_CLEAN
from step8_referentiels.referentiels import referentiels_load, ref_externe_preparation
from step9_affiliations.prep_entities import entities_preparation
from functions_shared import work_csv
from step9_affiliations.organismes_cleaning import organismes_back
from step9_affiliations.dataset_describe import dataset_decribe
PATH_GILB=f"C:/Users/zfriant/Gilberinette/Echanges/"
######### one time
# organismes_back('2024')


###################
S_PKL = pd.read_pickle(f'{PATH_REF}sirene_df.pkl').fillna('').sort_values('naf_et')
S_PKL = (pd.concat([S_PKL.mask(S_PKL=='')[['siren', 'naf_et']].rename(columns={'siren':'sid'}), 
                    S_PKL.mask(S_PKL=='')[['siret', 'naf_et']].rename(columns={'siret':'sid'})], 
                    ignore_index=True)
                    .drop_duplicates()
                    .sort_values(['naf_et', 'sid'], ignore_index=True)
                    )

# #prov
# # i=S_PKL.naf_et.eq('47.21Z').idxmax()
# # S_PKL=S_PKL.iloc[i:]

####### sirene_load = true si besoin de charger les données du SI sirene
# referentiels_load(S_PKL, ror_load=False, rnsr_load=True, sirene_load=False, sirene_subset=False)

# entities_preparation()
######## si reprise du code en cours chargement des pickles -> entities_all
# keep = pd.read_pickle(f'{PATH_MATCH}structure_fr.pkl')
# struct_et = pd.read_pickle(f'{PATH_MATCH}struct_et.pkl')

def data_import():
    from config_path import PATH_MATCH,  PATH_CLEAN
    proj = pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl")
    entities_all = pd.read_pickle(f'{PATH_MATCH}entities_all.pkl')
    print(f"size entities_all init: {len(entities_all)}")
    # pers = pd.read_pickle(f"{PATH_CLEAN}persons_current.pkl")
    # print(f"size persons: {len(pers)}")
    return entities_all, proj
entities_all, proj = data_import()

####################################################################################
#provisoire
countries = pd.read_pickle(f"{PATH_CLEAN}country_current.pkl")
entities_all = (entities_all
                .merge(countries[[ 'countryCode_iso3', 'country_name_en']], 
                    how='left', left_on='country_code', right_on='countryCode_iso3')
                .drop(columns=['countryCode_iso3', 'country_name_fr']))

tmp = entities_all[['city_back']].explode('city_back')
tmp['city_back'] = tmp.city_back.str.replace(r"\bst\b", 'saint', regex=True).str.strip()
tmp['city_back'] = tmp.city_back.str.replace(r"\bste\b", 'sainte', regex=True).str.strip()
tmp['city_back'] = tmp.city_back.str.replace(r"\s+|'", '-', regex=True)
tmp=tmp.groupby(level=0).agg(lambda x: ' '.join(x.dropna()))
entities_all = entities_all.drop(columns='city_back').merge(tmp, how='left', left_index=True, right_index=True)
entities_all.loc[~entities_all.street_2.isnull(),'street_2']=entities_all.loc[~entities_all.street_2.isnull(),'street_2'].map(lambda x: ' '.join(x))

entities_all['department'] = entities_all.department.str.lower().replace(r"\b(name of the department|department name)\b|\t|-|/|,|\.", " ", regex=True)
entities_all['department'] = entities_all.department.str.lower().replace(r"\s{2,}", " ", regex=True).str.strip()
#####################################


######### si actualisation -> rnsr_adr_corr = true pour nettoyer les adresses problématiques du rnsr
l=(entities_all.loc[entities_all.entities_id.str.match(r"^[0-9]{9}$|^[0-9]{14}$|^[W|w]([A-Z0-9]{8})[0-9]{1}$"), ['entities_id']].drop_duplicates()
 .merge(S_PKL, how='left', left_on='entities_id', right_on='sid', indicator=True))
# ref_externe_preparation(l, rnsr_adr_corr=False)

ref_all = pd.read_parquet(f"{PATH_MATCH}ref_all.parquet.gzip")
# ref_all['p_key'] = ref_all['p_key'].astype('str')
# ref_all = pd.read_pickle(f"{PATH_MATCH}ref_all.pkl")

###################################################

tmp=entities_all[~entities_all.astype(str).duplicated()].copy()
print(f"size entities_all: {len(tmp)}")

print("## create p_key/p_key_id")
tmp['p_key_id'] = tmp.project_id+'-'+tmp.orderNumber+'-'+tmp.generalPic
tmp['part_order'] = tmp.project_id+'-'+tmp.orderNumber
tmp['p_key']=tmp.reset_index().index+1
tmp['p_key'] = tmp['p_key'].astype(int)

### affiliations by mail
def get_id_by_var(df, var):
    temp=df.loc[(~df[var].isnull())&(df.rnsr_merged.str.len()==0)][var].str.split(' ').explode()
    res=ref_all.loc[(~ref_all[var].isnull())&(ref_all[var].isin(temp))]

    ref=list(set(res.ref))
    var_keep=[var]

    def get_id_by_ref(var_keep, ref):
        src={'paysage':['numero_paysage'], 'ror':['numero_ror'], 'rnsr':['num_nat_struct'], 'sirene':['siren', 'siret']}
        for k,v in src.items():
            if k in ref:
                var_keep.extend(v)
        return var_keep

    res=res[get_id_by_ref(var_keep, ref)]

    df=df.assign(temp_tmp=df[var].str.split(' '), nns_temp=np.nan)
    for i, row in res.iterrows():
        df.loc[~df[var].isnull(), 'nns_temp'] = df.loc[~df[var].isnull()].apply(lambda x: row['num_nat_struct'] if row[var] in x['temp_tmp'] else x['nns_temp'], axis=1)

    df['nns_temp'] = df['nns_temp'].map(lambda x: x.split(' ') if isinstance(x, str) else [])
    df.loc[(df.nns_temp.str.len()>0)&(df.method!='orga'), 'rnsr_merged'] = df.apply(lambda x: list(set(x['rnsr_merged'] + x['nns_temp'])), axis=1)
    df.loc[(df.nns_temp.str.len()>0)&(df.method.isnull()), 'method'] = var
    df.loc[(df.nns_temp.str.len()>0)&(~df.method.isin([var, 'orga'])), 'method'] = df.method+f' {var}'
    return df

print("## match mail/tel")
tmp=get_id_by_var(tmp, 'email')
tmp=get_id_by_var(tmp, 'tel_clean')


for i in ['rnsr_merged', 'org_merged', 'lab_merged','org_back','rnsr_back','labo_back','city_back']:
    tmp[i]=tmp[i].fillna('')
    tmp.loc[tmp[i].str.len()==0, i]=''

tmp[['lab_merged','rnsr_merged','org_merged','org_back','rnsr_back','labo_back']]=tmp[['lab_merged','rnsr_merged','org_merged','org_back','rnsr_back','labo_back']].map(lambda x: ' '.join(filter(None, x)))
# tmp[['lab_merged',]]=tmp[['lab_merged']].map(lambda x: '|'.join(filter(None, x)))

tmp.loc[~tmp.typ_from_lib.isnull(), 'org_merged'] = tmp.loc[~tmp.typ_from_lib.isnull(), 'org_merged'] +' '+tmp.loc[~tmp.typ_from_lib.isnull(), 'typ_from_lib'] 


###########################################
# create orig_ref
print("## create orig_ref and several vars ID by orig")
source = {'^[0-9]{9}$':'siren', 
            '^[0-9]{14}$':'siret', 
            '^[W|w]([A-Z0-9]{8})[0-9]{1}$':'rna', 
            '^[0-9]{9}[A-Z]{1}$':'rnsr', 
            '^R0([a-zA-Z0-9]{6})[0-9]{2}$':'ror', 
            '^([a-zA-Z0-9]{5})$':'paysage', 
            '^pic[0-9]{9}$':'pic', 
            '^[0-9]{7}[A-Z]{1}':'uai', 
            '^grid':'grid', 
            '^F[0-9]{2}([a-zA-Z0-9]{7})':'finess'}


for k,v in source.items():
    tmp.loc[tmp.entities_id.str.match(k, na=False), 'orig_ref'] = v
tmp.loc[tmp.orig_ref.isin(['pic','grid']), 'orig_ref']=np.nan

df =(tmp.loc[~tmp.orig_ref.isnull(), ['p_key', 'orig_ref', 'entities_id']]
    .fillna('')
    .pivot(
    index=["p_key"], columns=["orig_ref"], values=["entities_id"]
))
df.columns = [f"{b}" for a, b in df.columns]
df=df.reset_index()

tmp=tmp.merge(df, how='left', on='p_key')

print("## create rattachement/a_controler")
tmp=tmp.assign(rattachement=np.where(tmp.orig_ref.isnull(), 0,1),
               a_controler=np.where(tmp.resultat.isnull(), 0,1))

#######################################
#######################

print("## merge entities_full+departement_dup")
# tmp.loc[(~tmp.department_dup.isnull()), 'entities_full'] = tmp.loc[~tmp.department_dup.isnull()].entities_full +' '+tmp.loc[~tmp.department_dup.isnull()].department_dup
y=tmp.loc[(tmp.entities_full!=tmp.department_dup)&(~tmp.department_dup.isnull())&(~tmp.entities_full.isnull()), ['p_key', 'entities_full', 'department_dup']]

y['entities_full'] = [x1 if x2 in x1 else x1+' '+x2 for x1, x2 in zip(y['entities_full'], y['department_dup'])]

tmp=tmp.merge(y, how='left', on='p_key', suffixes=('', '_y'))
tmp.loc[~tmp.entities_full_y.isnull(), 'entities_full'] = tmp.loc[~tmp.entities_full_y.isnull(), 'entities_full_y']

tmp['entities_full'] = tmp['entities_full'].str.replace(r"[^ws]+", " ").str.strip().str.replace(r"\s{2,}", " ")


print("## create keymaster (famille) and grp")
for i in ['entities_full', 'street_2_tag', 'city_tag']:
    tmp[i]=tmp[i].fillna('')
tmp['keymaster'] = tmp.entities_full+tmp.street_2_tag+tmp.city_tag
tmp['keymaster'] = tmp.keymaster.str.replace(r"\s+", '', regex=True)
tmp['grp'] = tmp.groupby('keymaster').ngroup()
tmp['matrice'] = 0


#########################################
print("## add info project/date")
tmp=tmp.merge(proj[['project_id', 'stage', 'acronym', 'thema_name_en']], how='left', on=['project_id', 'stage'])
tmp['date_modif'] = datetime.datetime.today().strftime('%Y-%m-%d')


#provisoire
tmp=tmp.assign(entities_full_2=tmp.entities_full)

print(f"size entities complete: {len(tmp)}")

############################################
# columns rename
def cols_select_and_rename(df, table_out):
    from config_path import PATH_MATCH
    xl_path = f"{PATH_MATCH}vars_rename.xlsx"
    tt = pd.read_excel(xl_path, sheet_name=table_out)
    d = dict(zip(tt['col_in'], tt['col_out']))
    df = df[tt.col_in.tolist()]
    df = df.rename(columns=d)
    return df

################################

tmp=cols_select_and_rename(tmp, 'participants')

## prepare match with ref_all
def data_id(df, key_unit, vars_id):
    df=df.mask(df=='')
    tmp_id=pd.DataFrame()
    for i in vars_id:
        tmp_id=pd.concat([tmp_id, df.loc[~df[i].isnull(), [key_unit,  i]].rename(columns={i:'id_merge'})])
    tmp_id=tmp_id.loc[~tmp_id.id_merge.str.startswith('pic', na=False)].drop_duplicates()
    return tmp_id

vars_id=['ref_id', 'ror', 'rna', 'paysage', 'num_nat_struct', 'siret', 'siren']
tid=data_id(tmp, 'p_key', vars_id)

vars_id=['num_nat_struct', 'numero_ror', 'siren', 'siret', 'numero_rna','numero_paysage']
tidr=data_id(ref_all, 'p_key', vars_id)

x=tid.merge(tidr.rename(columns={'p_key':'p_key_ref'}), how='left', on=['id_merge'], indicator=True)

if 'left_only' in x._merge.unique():
    print(f"- ids without ref in ref_all WHY ! {x[x._merge=='left_only'].id_merge.unique()}\n- size list: {len(x[x._merge=='left_only'].id_merge.unique())}")

x['last_freq']=x.groupby('p_key_ref')['p_key'].transform('count')

# create ref_mesr
print("## REF MESR")
ref_mesr_pcrdt=(x.loc[x._merge=='both', ['p_key_ref', 'last_freq']].drop_duplicates()
                .merge(ref_all, how='inner', left_on='p_key_ref', right_on='p_key')
                .drop(columns='p_key_ref'))

ref_mesr_pcrdt = cols_select_and_rename(ref_mesr_pcrdt, 'ref_mesr_pcrdt')

ref_mesr_pcrdt = (ref_mesr_pcrdt
                .assign(
                id_ref=ref_mesr_pcrdt.reset_index().index+1, 
                date_creation=datetime.datetime.today().strftime('%Y-%m-%d'),
                date_modif=datetime.datetime.today().strftime('%Y-%m-%d'),
                fusionne=0, id_fusion=0, id_pere=0))

# ref_mesr_pcrdt['id_ref'] = ref_mesr_pcrdt['id_ref'].astype('str')
# ref_mesr_pcrdt = ref_mesr_pcrdt.fillna('')
ref_mesr_pcrdt = ref_mesr_pcrdt.mask(ref_mesr_pcrdt=='')

# add p_key_ref to tmp
y = x[['p_key', 'p_key_ref']].merge(ref_mesr_pcrdt[['id_ref', 'p_key_ref']], how='inner', on='p_key_ref')
# y['id_ref'] = y['id_ref'].astype('str')
# y = y.loc[~y.id_ref.isnull()].groupby('p_key', as_index=False).agg({'id_ref': lambda x: ' '.join(x)})


print(len(tmp))
tmp = tmp.merge(y, how='left', on='p_key')
# tmp[['p_key_ref', 'id_ref']] = tmp[['p_key_ref', 'id_ref']].astype('Int64')
# str_cols = tmp.columns[tmp.dtypes==object]
tmp = tmp.mask(tmp=='')
tmp=tmp.drop_duplicates()
print(len(tmp))
###
ref_all = cols_select_and_rename(ref_all, 'refext')
ref_all = ref_all.mask(ref_all=='')

################################################################################
# si problème avec gilberinette alors que le controle est déjà entamé

def updtate_with_code_provisoire():
    ref_new=pd.read_csv(f"{PATH_GILB}HEU_FRA/retour/referentiel.csv", sep=',')
    x=pd.read_csv(f"{PATH_GILB}HEU_FRA/retour/pcrdt_mult.csv", sep=',', header=None, names=['p_key', 'id_ref'])
    up=x.merge(ref_new, how='left', on='id_ref')[['p_key','id_ref','rna', 'paysage', 'siren', 'siret', 'num_nat_struct','ror']].sort_values('p_key')
    pcrdt=pd.read_csv(f"{PATH_GILB}HEU_FRA/retour/pcrdt.csv", sep=',', low_memory=False)
    tmp=pcrdt[['id_ref', 'rattachement', 'p_key', 'a_controler','num_nat_struct', 'method', 'siren','siret',  'paysage', 'ror', 'rna']]
    met_up=tmp[['p_key', 'method']]
    up=up.merge(met_up, how='left', on='p_key')
    p=(up[up.method=='manuelle'].sort_values(['p_key', 'id_ref']).replace('#',np.nan).groupby('p_key', dropna=False).agg(lambda x: ','.join(map(str, filter(None, x.dropna().unique())))).reset_index())
    # p['id_ref'] = p.id_ref.str.split(' ').str[0]
    p=p.mask(p=='').fillna('#')
    # check single id_ref one-one
    pcrdt=pcrdt.merge(p, how='left', on='p_key', suffixes=('','_up'), indicator=True)
    t=pcrdt.loc[pcrdt._merge=='both']
    t[['id_ref','rna', 'paysage', 'siren', 'siret', 'num_nat_struct', 'ror', 'method']]=t[['id_ref_up','rna_up', 'paysage_up', 'siren_up', 'siret_up', 'num_nat_struct_up', 'ror_up', 'method_up']]
    t.loc[t.method=='manuelle', 'method'] = 'treated'
    pcrdt=pd.concat([pcrdt.loc[pcrdt._merge!='both'], t], ignore_index=True)
    pcrdt=pcrdt.filter(regex=r'.*(?<!_up)$').drop(columns=['_merge', 'en_cours'])
    return pcrdt
tmp=updtate_with_code_provisoire()
ref_all=pd.read_csv(f"{PATH_GILB}HEU_FRA/retour/REFEXT.txt", sep='\t', low_memory=False)
ref_mesr_pcrdt=pd.read_csv(f"{PATH_GILB}HEU_FRA/retour/referentiel.csv", sep=',')
ref_all['label_num_ro_rnsr']=ref_all['label_num_ro_rnsr'].str.replace(' ', '|', regex=False)
ref_mesr_pcrdt['label_num_ro_rnsr']=ref_mesr_pcrdt['label_num_ro_rnsr'].str.replace(';', ' ', regex=False)
##############################################################################################

def fill_empty_cols(df):

    num_cols = df.select_dtypes(include=['number']).columns
    df[num_cols] = df[num_cols].fillna(0)
    df[num_cols] = df[num_cols].astype('int64')

    # columns filters
    str_cols=["pcrdt_pic" ,"paysage"  ,"num_nat_struct" ,"method" ,"siren"  ,"siret" ,"ror" ,"rna","ref_id" ,"operateur" ,"nom_entier" ,
    "adresse_2_tag" ,"ville_tag" ,"pays"  ,"pays_dept" ,"domaine_email" ,"organisme","liste_sigles" ,"rep_org","rep_labo", "rep_nns","rep_ville", 
    "statut" , "part_order" ,"programme" ,"contact", "label_num_ro_rnsr", "etabs_rnsr","orig_ref"] 
    c=[col for col in df.columns if col in str_cols]
    df[c] = df[c].fillna('#')

    df=df.fillna('')
    return df

tmp = fill_empty_cols(tmp)
ref_mesr_pcrdt = fill_empty_cols(ref_mesr_pcrdt)
ref_all = fill_empty_cols(ref_all)


# tmp['contact'] = tmp['contact'].str.replace('-', ' ')
# tmp.loc[tmp.contact.str.len()>500, 'contact'] = '#'

tmp = tmp[tmp.dtypes.sort_values().index]

# check column with nan_values 
print(f"nan values columns: {tmp.columns[tmp.isnull().any()]}")
###################################################

def dataset_decribe(df,table_output):
    import io, pandas as pd
    buffer = io.StringIO()
    df.info(buf=buffer)
    s = buffer.getvalue()

    tmp=pd.DataFrame()
    lines = s.splitlines()
    column_info = []
    # Analyser chaque ligne pour extraire les informations nécessaires
    for line in lines[5:]:  # Ignorer les premières lignes (généralement des en-têtes)
        parts = line.split()
        if len(parts) >= 5:
            col_order = int(parts[0])+1
            col_name = parts[1]
            col_nobs = parts[2]
            col_type = parts[4]
            print(line)
            if df[parts[1]].dtype == object:
                col_length = df[parts[1]].str.len().max()
            else:
                col_length = 8
        elif len(parts) == 3:
            if parts[0].isnumeric():
                col_order = int(parts[0])+1
                col_name = parts[1]
                col_nobs = df[parts[1]].count()
                col_type = parts[2]
                if df[parts[1]].dtype == object:
                    col_length = df[parts[1]].str.len().max()
                else:
                    col_length = 8
        column_info.append({'VARNUM':col_order,'code_champ':col_name,'LENGTH':col_length,'NOBS':col_nobs,'type':col_type})
        # column_info.append({'VARNUM':col_order,'code_champ':col_name,'NOBS':col_nobs,'type':col_type})

    tmp=pd.DataFrame(column_info).drop_duplicates()
    tmp.loc[tmp.type=='object', 'type']='char'
    tmp['LENGTH'] = tmp['LENGTH'].astype(int)
    tmp.loc[(tmp.type.str.lower().str.startswith('int'))|(tmp.type.str.lower().str.startswith('float')), 'type']='num'
    tmp.loc[tmp.code_champ=='method', 'LENGTH']=30
    tmp.loc[tmp.code_champ.str.startswith('date'), 'type']='date'

    tmp=tmp.assign(NPOS=tmp.LENGTH)
    tmp.NPOS=tmp.NPOS.shift(1)
    tmp.loc[tmp.VARNUM==1, 'NPOS']=0
    tmp['NPOS']=tmp.NPOS.cumsum().astype('int64')
    

    return tmp.assign(table=table_output, label_champ=tmp.code_champ)[['table','code_champ','LENGTH','VARNUM','label_champ','NPOS','NOBS','type']]

###############

def export(path, ptmp, refmp, refext, export_dos):
    p=dataset_decribe(ptmp, 'participants')
    rmp=dataset_decribe(refmp, 'ref_mesr_pcrdt')
    r=dataset_decribe(refext, 'refext')
    champs=pd.concat([p,rmp,r], ignore_index=True)
    description = [
    "modele=PCRDT_ALL\n"
    "description=PCRDT ALL\n"
    "id="+export_dos]
    with open(f'{path}{export_dos}/Description.txt', 'w') as file:
    # Write all lines at once
        file.writelines(description)

    champs.to_csv(f'{path}{export_dos}/ListeChamps.txt', sep='\t', index=False, na_rep='')
    ptmp.to_csv(f'{path}{export_dos}/PARTICIPANTS.txt', sep='\t', index=False, na_rep='')
    refmp.to_csv(f'{path}{export_dos}/REF_MESR_PCRDT.txt', sep='\t', index=False, na_rep='')
    refext.to_csv(f'{path}{export_dos}/REFEXT.txt', sep='\t', index=False, na_rep='')

## test
# l=tmp.p_key.value_counts().reset_index()[0:100].p_key.unique()
tmp_test=tmp.iloc[48919:48922]
export(PATH_GILB, tmp_test, ref_mesr_pcrdt, ref_all, 'HORIZON')

#UPDATE refext c SET c.label_num_ro_rnsr= REGEXP_REPLACE(c.label_num_ro_rnsr, ';', '\\s')

#france
export(PATH_GILB, tmp, ref_mesr_pcrdt, ref_all, "HEU_FRA")


def export_cc_select(ptmp, refmp, refext, France=True):
    import os
    if France==True:
        ptmp=ptmp.loc[ptmp.country_code=='FRA']
    else:
        ptmp=ptmp[(ptmp.country_code!='FRA')]

    for i in ptmp.country_code.unique():
        folder_path = f'{PATH_GILB}HEU_{i}'
        # Create the folder if it doesn't exist
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print(f'Folder "{folder_path}" created successfully.')
        else:
            print(f'Folder "{folder_path}" already exists.')
        export(PATH_GILB, ptmp.drop(columns='country_code'), refmp, refext, f"HEU_{i}")

export_cc_select(tmp, ref_mesr_pcrdt, ref_all, France=True)