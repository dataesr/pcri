# PCRI
Le *Programme-cadre de la recherche et de l'innovation* est le programme de financement de la recherche de la Commission européenne (CE).

[Work programmes under Horizon Europe](https://research-and-innovation.ec.europa.eu/funding/funding-opportunities/funding-programmes-and-open-calls/horizon-europe/horizon-europe-work-programmes_en)

[Search grants into CORDIS](https://cordis.europa.eu/search/fr)

Le Ministère chargé de l'enseignement supérieur et de la recherche a la responsabilité du suivi du programme pour la France [Horizon Europe](https://www.horizon-europe.gouv.fr/). 
Ce répertoire contient l'ensemble des traitements des données issues du SI eCorda administré par la CE.


Collecte des données
--------------------
Voire [Répertoire eCorda-data](https://github.com/dataesr/eCorda-data)



# Installation du projet
--------------------

Les données (`eCorda_data/`) sont volumineuses et **ne sont pas incluses dans le dépôt git**.
Chaque utilisateur doit donc :
1. Récupérer le code (git clone)
2. Récupérer/synchroniser les données séparément (OneDrive, disque réseau, etc.)
3. Indiquer au projet où se trouvent ces données sur sa machine

## Étapes

### 1. Cloner le projet
```bash
git clone https://github.com/dataesr/pcri.git
cd <nom_du_projet>
```

### 2. Installer les dépendances Python
```bash
pip install -r requirements.txt
```

### 3. Configurer le chemin vers les données

**Option automatique (recommandée) :**
```bash
python setup.py
```
Le script demande le chemin vers le dossier racine des données, crée `config_local.py`,
puis crée automatiquement l'arborescence de dossiers si elle n'existe pas.

**Option manuelle :**
1. Copier `config_template.py` en `config_local.py`
2. Ouvrir `config_local.py` et remplacer `"CHANGE_ME"` par le chemin réel, par exemple :
   ```python
   PATH = "C:/Users/<ton_nom>/PCRI/"
   ```

### 4. Récupérer les données
Copier / synchroniser le dossier `eCorda_data` (et ses sous-dossiers) à l'emplacement
indiqué dans `config_local.py`. Ce dossier n'est jamais versionné dans git — il doit
être transmis par un autre canal (OneDrive partagé, disque réseau, clé USB, etc.)

### 5. Vérifier
```python
from paths import PATH_REF, PATH_SOURCE, check_data_present
check_data_present()
```
Le script signale les dossiers manquants ou vides.

## Utilisation dans le code

Ne plus jamais écrire de chemin en dur. Toujours importer depuis `paths.py` :
```python
from paths import PATH_REF, PATH_SOURCE, PATH_WP, PATH_WORK, PATH_CLEAN
```

## Fichiers concernés

| Fichier | Rôle | Versionné dans git ? |
|---|---|---|
| `config_template.py` | Modèle de config | Oui |
| `config_local.py` | Config personnelle (chemin réel) | **Non** (.gitignore) |
| `paths.py` | Définit tous les PATH_ à partir de config_local | Oui |
| `setup.py` | Installation interactive | Oui |
| `eCorda_data/` | Données volumineuses | **Non** (.gitignore) |