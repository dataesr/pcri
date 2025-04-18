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

tmp=entities_all[['city_back']].explode('city_back')
tmp['city_back'] = tmp.city_back.str.replace(r"\bst\b", 'saint', regex=True).str.strip()
tmp['city_back'] = tmp.city_back.str.replace(r"\bste\b", 'sainte', regex=True).str.strip()
tmp['city_back'] = tmp.city_back.str.replace(r"\s+|'", '-', regex=True)
tmp=tmp.groupby(level=0).agg(lambda x: ' '.join(x.dropna()))
entities_all = entities_all.drop(columns='city_back').merge(tmp, how='left', left_index=True, right_index=True)
#####################################


######### si actualisation -> rnsr_adr_corr = true pour nettoyer les adresses problématiques du rnsr
l=(entities_all.loc[entities_all.entities_id.str.match(r"^[0-9]{9}$|^[0-9]{14}$|^[W|w]([A-Z0-9]{8})[0-9]{1}$"), ['entities_id']].drop_duplicates()
 .merge(S_PKL, how='left', left_on='entities_id', right_on='sid', indicator=True))
# ref_externe_preparation(l, rnsr_adr_corr=False)

ref_all = pd.read_parquet(f"{PATH_MATCH}ref_all.parquet.gzip")
# ref_all=ref_all.reset_index()
# ref_all = pd.read_pickle(f"{PATH_MATCH}ref_all.pkl")

###################################################

tmp=entities_all.copy()
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

print("## create keymaster (famille) and grp")
for i in ['entities_full', 'street_2_tag', 'city_tag']:
    tmp[i]=tmp[i].fillna('')
tmp['keymaster'] = tmp.entities_full+tmp.street_2_tag+tmp.city_tag
tmp['keymaster'] = tmp.keymaster.str.replace(r"\s+", '', regex=True)
tmp['grp'] = tmp.groupby('keymaster').ngroup()


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
def data_id(df, key_unit, var_orig, vars_id):
    tmp_id=pd.DataFrame()
    for i in vars_id:
        tmp_id=pd.concat([tmp_id, df.loc[~df[i].isnull(), [key_unit, var_orig, i]].rename(columns={i:'id_merge', var_orig:'orig'})])
    tmp_id=tmp_id.loc[~tmp_id.id_merge.str.startswith('pic', na=False)].drop_duplicates()
    return tmp_id

vars_id=['ref_id', 'ror', 'rna', 'paysage', 'num_nat_struct', 'siret', 'siren']
tid=data_id(tmp, 'p_key', 'orig_ref', vars_id)

vars_id=['num_nat_struct', 'numero_ror', 'siren', 'siret', 'numero_rna','numero_paysage']
tidr=data_id(ref_all, 'p_key', 'ref', vars_id)

x=tid.merge(tidr.rename(columns={'p_key':'p_key_ref'}), how='left', on=['orig','id_merge'], indicator=True)

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

# ref_mesr_pcrdt = ref_mesr_pcrdt.fillna('')
ref_mesr_pcrdt = ref_mesr_pcrdt.mask(ref_mesr_pcrdt=='')

# add p_key_ref to tmp
y=x[['p_key', 'p_key_ref']].merge(ref_mesr_pcrdt[['id_ref', 'p_key_ref']], how='inner', on='p_key_ref')

tmp=tmp.merge(y, how='left', on='p_key')
# tmp[['p_key_ref', 'id_ref']] = tmp[['p_key_ref', 'id_ref']].astype('Int64')
str_cols = tmp.columns[tmp.dtypes==object]
ref_all = ref_all.mask(ref_all=='')

###
ref_all = cols_select_and_rename(ref_all, 'refext')
ref_all = ref_all.mask(ref_all=='')


def fill_empty_cols(df):

    num_cols = df.select_dtypes(include=['number']).columns
    df[num_cols] = df[num_cols].fillna(0)
    df[num_cols] = df[num_cols].astype('int64')


    str_cols=["pcrdt_pic" ,"paysage"  ,"num_nat_struct" ,"method" ,"siren"  ,"siret" ,"ror" ,"ref_id" ,"operateur"  ,"nom_entier" ,
    "adresse_2_tag" ,"ville_tag" ,"pays"  ,"pays_dept" ,"domaine_email" ,"organisme","liste_sigles" ,"rep_org","rep_labo" ,"rep_ville", 
    "statut" , "part_order" ,"programme" ,"contact", "label_num_ro_rnsr", "etabs_rnsr"] 
    c=[col for col in df.columns if col in str_cols]
    df[c] = df[c].fillna('#')
    return df

tmp = fill_empty_cols(tmp)
ref_mesr_pcrdt = fill_empty_cols(ref_mesr_pcrdt)
ref_all = fill_empty_cols(ref_all)

ref_all1=ref_all[0:10].fillna('')
tmp1=tmp[0:10].fillna('')
ref_mesr_pcrdt1=ref_mesr_pcrdt[0:10].fillna('')
###################################################

def dataset_decribe(df,table_output):
    import io, pandas as pd
    buffer = io.StringIO()
    df.info(buf=buffer)
    s = buffer.getvalue()

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
    return tmp.assign(table=table_output, label_champ=tmp.code_champ)[['table','code_champ','LENGTH','VARNUM','label_champ','NOBS','type']]

###############
p=dataset_decribe(tmp, 'participants')
r=dataset_decribe(ref_all, 'refext')
rmp=dataset_decribe(ref_mesr_pcrdt, 'ref_mesr_pcrdt')
champs=pd.concat([p,r,rmp], ignore_index=True)

p=dataset_decribe(tmp1, 'participants')
r=dataset_decribe(ref_all1, 'refext')
rmp=dataset_decribe(ref_mesr_pcrdt1, 'ref_mesr_pcrdt')
champs=pd.concat([p,r,rmp], ignore_index=True)

PATH_GILB="C:/Users/zfriant/Gilberinette/Echanges/HORIZON/"
champs.to_csv(f'{PATH_GILB}ListeChamps.txt', sep='\t', index=False, na_rep='')
tmp1.to_csv(f'{PATH_GILB}PARTICIPANTS.txt', sep='\t', index=False, na_rep='')
ref_mesr_pcrdt1.to_csv(f'{PATH_GILB}REF_MESR_PCRDT.txt', sep='\t', index=False, na_rep='')
ref_all1.to_csv(f'{PATH_GILB}REFEXT.txt', sep='\t', index=False, na_rep='')