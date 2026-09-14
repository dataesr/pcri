"""
Telecharge le dump scanR persons_denormalized.jsonl.gz (~2.5 Go) en
streaming, avec barre de progression et reprise automatique si le
telechargement est interrompu (ne re-telecharge pas ce qui est deja la).

Usage :
    python download_scanr_dump.py
    python download_scanr_dump.py --dest D:/data/persons_denormalized.jsonl.gz
"""
from paths import PATH
import argparse
import os
import requests

URL = "https://scanr-data.s3.gra.io.cloud.ovh.net/production/persons_denormalized.jsonl.gz"


def download_with_resume(url: str, dest: str, chunk_size: int = 1024 * 1024):
    """
    Telecharge url vers dest en streaming. Si dest existe deja
    partiellement, reprend au bon octet via l'en-tete HTTP Range
    (fonctionne si le serveur supporte les requetes par plage, ce qui
    est generalement le cas pour un stockage S3-compatible comme OVH).
    """
    resume_byte_pos = os.path.getsize(dest) if os.path.exists(dest) else 0

    headers = {}
    mode = "wb"
    if resume_byte_pos:
        headers["Range"] = f"bytes={resume_byte_pos}-"
        mode = "ab"
        print(f"Reprise du telechargement a partir de l'octet {resume_byte_pos:,}")

    with requests.get(url, headers=headers, stream=True, timeout=30) as resp:
        resp.raise_for_status()

        total_size = int(resp.headers.get("content-length", 0)) + resume_byte_pos
        downloaded = resume_byte_pos

        with open(dest, mode) as f:
            for chunk in resp.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)

                if total_size:
                    pct = downloaded / total_size * 100
                    print(f"\r{downloaded / 1e6:,.1f} Mo / {total_size / 1e6:,.1f} Mo "
                          f"({pct:.1f}%)", end="", flush=True)
                else:
                    print(f"\r{downloaded / 1e6:,.1f} Mo telecharges", end="", flush=True)

    print(f"\nTermine : {dest}")