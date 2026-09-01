"""
Process exécuté en arrière-plan par generic_runner.start("orcid", ...).
Charge le payload (df + config) depuis {job_name}_payload.pkl, lance
l'enrichissement ORCID avec checkpoint, et pousse le résultat vers
MongoDB si demandé (via mongo_runner.refresh_mongo_collection).

Ne pas lancer ce fichier directement à la main : passer par
remote_process.orcid_runner.start(...).
"""

import argparse
import pickle
import sys
from pathlib import Path

# Ce fichier vit dans pcri/remote_process/. La logique métier ORCID
# (orcid_enrichment.py) vit dans pcri/step7_persons/.
SCRIPT_DIR = Path(__file__).resolve().parent          # .../pcri/remote_process
PROJECT_ROOT = SCRIPT_DIR.parent                        # .../pcri
PERSONS_DIR = PROJECT_ROOT / "step7_persons"             # .../pcri/step7_persons

# PERSONS_DIR pour importer orcid_enrichment
# PROJECT_ROOT pour importer remote_process.mongo_runner (package du projet)
sys.path.insert(0, str(PERSONS_DIR))
sys.path.insert(0, str(PROJECT_ROOT))

from remote_process.orcid_enrichment import enrich_dataframe  # noqa: E402


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-name", required=True)
    job_name = parser.parse_args().job_name

    payload_path = SCRIPT_DIR / f"{job_name}_payload.pkl"
    with open(payload_path, "rb") as f:
        payload = pickle.load(f)

    df = payload["df"]
    first_name_col = payload["first_name_col"]
    last_name_col = payload["last_name_col"]
    orcid_col = payload["orcid_col"]
    checkpoint_path = payload["checkpoint_path"]
    checkpoint_every = payload["checkpoint_every"]
    pause = payload["pause"]

    print(f"Démarrage enrichissement ORCID sur {len(df)} lignes "
          f"(checkpoint: {checkpoint_path})", flush=True)

    enriched_df = enrich_dataframe(
        df,
        first_name_col=first_name_col,
        last_name_col=last_name_col,
        orcid_col=orcid_col,
        checkpoint_path=checkpoint_path,
        checkpoint_every=checkpoint_every,
        pause=pause,
        verbose=True,
    )

    print("Enrichissement terminé.", flush=True)


if __name__ == "__main__":
    main()