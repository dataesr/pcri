

import pandas as pd, pycountry, re
from text_to_num import text2num, alpha2digit

from IPython.display import HTML
from pathlib import Path
from config_path import PATH

# from step7_referentiels.countries import ref_countries
from functions_shared import work_csv, prep_str_col, stop_word
from step7_referentiels.ror import ror_import, ror_prep
from step7_referentiels.sirene import sirene_prep, sirene_refext
from step7_referentiels.rnsr import rnsr_import, rnsr_prep
from step7_referentiels.paysage import paysage_prep
DUMP_PATH=f'{PATH}referentiel/'


pycountry.countries.add_entry(alpha_2="XK", alpha_3="XXK", name="Kosovo")
pycountry.countries.add_entry(alpha_2="UK", alpha_3="GBR", name="United Kingdom")
pycountry.countries.add_entry(alpha_2="EL", alpha_3="GRC", name="Greece")
tmp = [c.__dict__['_fields'] for c in list(pycountry.countries)]
countries = (pd.DataFrame(tmp)[['alpha_2', 'alpha_3', 'name']]
            .rename(columns={'alpha_2':'iso2', 'alpha_3':'iso3', 'name':'country_name_en'})
            .drop_duplicates()
)
print(len(countries))

ROR_ZIPNAME = ror_import(DUMP_PATH)
ror = ror_prep(DUMP_PATH, ROR_ZIPNAME, countries)

if len(ror[ror.country_code_map.isnull()][['country.country_code']].drop_duplicates())>0:
    print(ror[ror.country_code_map.isnull()][['country.country_code']].drop_duplicates())
  
pays_fr = ["FR","BL","CP","GF","GP","MF","MQ","NC","PF","PM","RE","TF","WF","YT"]
ror.loc[ror['iso2'].isin(['MS', 'TC']), 'country_code'] ='GBR'
ror.loc[ror['iso2'].isin(['AX']), 'country_code'] ='FIN'


sirene_refext(DUMP_PATH) # -> sirene_ref_moulinette.pkl
sirene = sirene_prep(DUMP_PATH, countries)

### Extraction des données rnsr de dataESR

rnsr_import(DUMP_PATH)
rnsr = rnsr_prep(DUMP_PATH)

work_csv(rnsr.loc[(rnsr.code_postal.isnull())|(rnsr.ville.isnull()), ['num_nat_struct', 'nom_long','adresse_full', 'code_postal', 'ville']].drop_duplicates(), 'rnsr_adresse_a_completer')
add_ad = pd.read_csv(f"{DUMP_PATH}rnsr_adresse_manquante.csv",  sep=';', encoding='ANSI')
add_ad = add_ad[['num_nat_struct', 'cp_corr', 'city_corr', 'country_corr']].drop_duplicates()

rnsr = rnsr.merge(add_ad, how='left', on='num_nat_struct')
rnsr.loc[~rnsr.cp_corr.isnull(), 'code_postal'] = rnsr.cp_corr
rnsr.loc[~rnsr.city_corr.isnull(), 'ville'] = rnsr.city_corr
rnsr.loc[~rnsr.country_corr.isnull(), 'country_code_map'] = rnsr.country_corr

######
# paysage
paysage = paysage_prep(DUMP_PATH)

######
# table all
ref_all = pd.concat([ror, rnsr, sirene, paysage], ignore_index=True)
ref_all = ref_all.drop(columns=['country.country_name', 'Lieudit_BP', 'COG', 'aliases','cp_corr','city_corr','country_corr'])
ref_all.mask(ref_all=='', inplace=True)
ref_all = ref_all.sort_values(['ref', 'num_nat_struct', 'siren', 'numero_paysage', 'numero_ror'])

url='https://docs.google.com/spreadsheet/ccc?key=1FwPq5Qw7Gbgj_sBD6Za4dfDDk6ydozQ99TyRjLkW5d8&output=xls'
df_geo = pd.read_excel(url, sheet_name='LES_COMMUNES', dtype=str, na_filter=False)
ref_all = ref_all.merge(df_geo[['COM_CODE', 'ISO_3']], how='left', left_on='com_code', right_on='COM_CODE')
ref_all.loc[~ref_all.ISO_3.isnull(), 'country_code_map'] = ref_all.ISO_3

ref_all.loc[ref_all.country_code_map.isnull(), ['ref']].value_counts()

#lowercase / exochar / unicode / punct
ref_cols=['nom_long', 'sigle', 'ville', 'adresse', 'adresse_full']
ref_all = prep_str_col(ref_all, ref_cols)

y = ref_all.loc[(~ref_all.tel.isnull())&(ref_all.country_code_map=='FRA'), ['tel']]
y['tel_clean']=y.tel.apply(lambda x:[re.sub(r'[^0-9]+', '', i) for i in x])
y['tel_clean']=y.tel_clean.apply(lambda x: [re.sub(r'^(33|033)', '', i).rjust(10, '0') for i in x])
y['tel_clean']=y.tel_clean.apply(lambda x: [i[0:10] if (len(i)>10) and (i[0:1]=='0') else i for i in x])
y['tel_clean']=y.tel_clean.apply(lambda x:[re.sub(r'^0+$', '', i) for i in x])
y['tel_clean']=y.tel_clean.apply(lambda x: ';'.join(set(x))).str.strip()

ref_all = pd.concat([ref_all, y[['tel_clean']]], axis=1)

tmp = ref_all.loc[(ref_all.nom_long!=ref_all.sigle)&(~ref_all.sigle.isnull()), ['nom_long',  'sigle']]
tmp['nom_entier'] = [x1 if x2 in x1 else x1+' '+x2 for x1, x2 in zip(tmp['nom_long'], tmp['sigle'])]
ref_all = pd.concat([ref_all, tmp[['nom_entier']]], axis=1)

ref_all.loc[ref_all.nom_entier.isnull(), 'nom_entier'] = ref_all.nom_long

#suppression des mots vides comme le la les et... pour "toutes les langues"
stop_word(ref_all, 'country_code_map', ['nom_long', 'nom_entier', 'adresse', 'adresse_full'])


# extraction du code postal du champs ville 
ref_all.loc[ref_all.code_postal.isnull(), 'code_postal'] = ref_all.ville.str.extract('(\\d+)')

#traitement spécifique adresse_full du rnsr
tmp = ref_all[~ref_all['adresse_full_2'].isnull()][['adresse_full_2']]
tmp.adresse_full_2 = tmp.adresse_full_2.apply(lambda x: list(filter(None, x))).apply(lambda x: ' '.join(x))
tmp[['cp_temp', 'ville_temp']] = tmp['adresse_full_2'].str.extract(r'(\\b\\d{5})\\s?([a-z]+(?:\\s?[a-z]+)*)', expand=True)

def match(adr):
    x = re.search(r'(\\b\\d{1,4})\\s([a-z]+\\s?)+', adr)
    if x :
        return(x.group())
    
tmp['adresse_temp'] = tmp['adresse_full_2'].apply(match)
ref_all = pd.concat([ref_all.drop(columns='adresse_full_2'), tmp], axis=1)

ref_all.loc[ref_all.code_postal.isnull(), 'code_postal'] = ref_all.cp_temp
ref_all.loc[ref_all.ville.isnull(), 'ville'] = ref_all.ville_temp

ref_all.loc[ref_all.adresse_2.isnull(), 'adresse_2'] = ref_all.adresse_temp
ref_all.drop(columns=['cp_temp', 'ville_temp', 'adresse_temp'], inplace=True)

# nettoyage de ville
cedex="cedax|cedrex|cdexe|cdex|credex|cedex|cedx|cede|ceddex|cdx|cex|cexex|edex"
ref_all['ville'] = ref_all.ville.str.replace('\\d+', ' ', regex=True).str.strip()
ref_all.loc[(ref_all.country_code_map=='FRA')|(ref_all.iso2.isin(pays_fr)), 'ville'] = ref_all.ville.str.replace(cedex, ' ', regex=True).str.strip()
ref_all.loc[(ref_all.country_code_map=='FRA')|(ref_all.iso2.isin(pays_fr)), 'ville'] = ref_all.ville.str.replace('^france$', '', regex=True).str.strip()
ref_all.loc[(ref_all.country_code_map=='FRA')|(ref_all.iso2.isin(pays_fr)), 'ville'] = ref_all.ville.str.replace('\\bst\\b', 'saint', regex=True).str.strip()
ref_all.loc[(ref_all.country_code_map=='FRA')|(ref_all.iso2.isin(pays_fr)), 'ville'] = ref_all.ville.str.replace('\\bste\\b', 'sainte', regex=True).str.strip()
ref_all.loc[(ref_all.country_code_map=='FRA')|(ref_all.iso2.isin(pays_fr)), 'ville_tag'] = ref_all.loc[ref_all.country_code_map=='FRA', 'ville'].str.strip().str.replace(r'\\s+', '-', regex=True)