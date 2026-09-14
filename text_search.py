import pandas as pd
import re
from paths import PATH_CLEAN


keywords = [
 'AI Alignment',
 'AI Governance',
 'AI Safety',
 'Active Learning',
 'Adversarial Robustness',
 'Anomaly Detection',
 'Artificial General Intelligence',
 'Artificial Intelligence',
 'Attention Mechanism',
 'Autoencoders',
 'Backpropagation',
 'Benchmarking',
 'Bias Mitigation',
 'Causal Inference',
 'Chain-of-Thought Reasoning',
 'Computer Vision',
 'Constitutional AI',
 'Continual Learning',
 'Convolutional Neural Networks',
 'Data Augmentation',
 'Dataset Curation',
 'Deep Learning',
 'Diffusion Models',
 'Distillation',
 'Embodied AI',
 'Emergent Behavior',
 'Evaluation Metrics',
 'Explainable AI',
 'Fairness in AI',
 'Federated Learning',
 'Fine-Tuning',
 'Foundation Models',
 'Generalization',
 'Generative Adversarial Networks',
 'Gradient Descent',
 'Graph Neural Networks',
 'Hyperparameter Tuning',
 'Information Retrieval',
 'Interpretability',
 'Knowledge Graphs',
 'Large Language Models',
 'Long Short-Term Memory',
 'Machine Learning',
 'Mechanistic Interpretability',
 'Meta-Learning',
 'Mixture of Experts',
 'Model Compression',
 'Multi-Agent Systems',
 'Multi-Task Learning',
 'Multimodal Learning',
 'Natural Language Processing',
 'Neural Networks',
 'Neurosymbolic AI',
 'Online Learning',
 'Optimization',
 'Overfitting',
 'Prompt Engineering',
 'Quantization',
 'Recommender Systems',
 'Recurrent Neural Networks',
 'Red Teaming',
 'Regularization',
 'Reinforcement Learning',
 'Retrieval-Augmented Generation',
 'Robotics',
 'Scaling Laws',
 'Self-Supervised Learning',
 'Semi-Supervised Learning',
 'Speech Recognition',
 'Supervised Learning',
 'Time Series Analysis',
 'Transfer Learning',
 'Transformers',
 'Unsupervised Learning',
]


projects = pd.read_pickle(f"{PATH_CLEAN}projects_current.pkl")
participation = pd.read_pickle(f"{PATH_CLEAN}participation_current.pkl").drop(columns=['app_fund', 'part_fund', 'n_pic_cc'])
entities_info = pd.read_pickle(f"{PATH_CLEAN}entities_info_current2.pkl")


# 1a. table au niveau projet (une ligne par project_id)
project_level = (
    projects.loc[
        (projects['action_code'].isin(['RIA', 'IA'])) & (projects['stage'] == 'successful')
    ]
    .groupby(["project_id", "action_code", "call_id", 'pilier_name_en', 'acronym'], as_index=False)
    .agg({
        "abstract": "first",
        "free_keywords": "first",
        "title": "first",   # à adapter selon ce que tu as vérifié précédemment
    })
)

# 1b. recherche des mots-clés




search_text = (
    project_level["abstract"].fillna("") + " " + project_level["free_keywords"].fillna("") + 
    " " + project_level["title"].fillna("")
).str.lower()

for kw in keywords:
    pattern = re.escape(kw.lower())
    project_level[f"kw__{kw}"] = search_text.str.contains(pattern, regex=True, na=False)

kw_cols = [f"kw__{kw}" for kw in keywords]
project_level["matches_any_keyword"] = project_level[kw_cols].any(axis=1)

# 1d. sélection finale des projets
selected_projects = project_level[
    project_level["matches_any_keyword"]
].copy()


"""
add participation
"""
selected_participants = (participation.loc[
                        (participation['stage'] == 'successful') & 
                        (participation['project_id'].isin(selected_projects['project_id']))]
)

def ent_stage(df, stage_value:str):
    import numpy as np
    df=(df[df.stage==stage_value]
        .merge(entities_info.drop(columns=['country_code']), 
                how='left', on=['generalPic','country_code_source']))
    
    print(f"- subv {stage_value}={'{:,.1f}'.format(df.loc[(df.country_code=='FRA')&(df.stage==stage_value), 'calculated_fund'].sum())}")

    if any(df['id_first'].str.contains(' |;', na=False)):
        print(f"- ⚠️ multi id_first pour une participation, calculs sur les chiffres\n {df.loc[df['id_first'].str.contains(' |;', na=False), 'id_first'].drop_duplicates()}")
        df['entities_num'] = np.where(df['id_first'].str.contains(' |;', na=False), df['id_first'].str.split(' |;').str.len(), 1)
        for i in ['calculated_fund']:
            df[i] = df[i]/df['entities_num']
    return df


entities_signed = ent_stage(selected_participants, 'successful')
print(f"3 - subv={'{:,.1f}'.format(entities_signed.loc[(entities_signed.country_code=='FRA')&(entities_signed.stage=='successful'), 'calculated_fund'].sum())}")


entities_signed = (entities_signed[
        ['entities_id', 'entities_name', 'country_code', 'category_agregation', 'project_id', 'calculated_fund']
    ].merge(selected_projects[
        ['project_id', 'action_code', 'call_id', 'pilier_name_en', 'title', 'acronym', "abstract"]
    ], 
    how='left', 
    on='project_id')
    )

entities_signed = (entities_signed.groupby(
    ['project_id', 'action_code', 'call_id', 'pilier_name_en', 'title', 'acronym', "abstract", 'entities_id', 'entities_name', 'country_code', 'category_agregation']
    ).agg({'calculated_fund': 'sum'}
    ).reset_index()
)


"""
calculated ratio
"""
total_fund_all = entities_signed['calculated_fund'].sum()
total_fund_fr = entities_signed.loc[entities_signed['country_code']=='FRA', "calculated_fund"].sum()
pct_selection = 100 * total_fund_fr / total_fund_all if total_fund_all else 0

print(f"Fund_euro sélection : {total_fund_fr:,.0f} €")
print(f"Fund_euro total     : {total_fund_all:,.0f} €")
print(f"Part de la sélection : {pct_selection:.2f} %")


"""
proj fr
"""
fr_project_ids = set(
    entities_signed.loc[entities_signed["country_code"] == "FRA", "project_id"]
)

selected_projects_fr = entities_signed[
    entities_signed["project_id"].isin(fr_project_ids)
].copy()


"""
tot
"""
p = entities_signed.groupby(
    ['project_id']
    ).agg({'calculated_fund': 'sum'}
    ).reset_index()

selected_projects.loc[
    selected_projects['project_id'].isin(fr_project_ids),
    'france_flag'] = True

selected_projects = selected_projects.merge(
    p, how='left', on='project_id'
).rename(columns={'calculated_fund': 'Montant total des financements par projet (€)'})