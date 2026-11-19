"""P2 — Flux streaming & historique 5 ans : mobilité urbaine (CityBikes).

Sous-modules :
- ingestion : collecte temps réel (2-3 min) depuis api.citybik.es/v2 vers le bronze.
- backfill  : récupération historique (fenêtre 5 ans) depuis data.citybik.es.
- quality   : règles de qualité pures (stdlib uniquement) et diagnostic de criticité.
- cleaning  : normalisation unifiée (streaming JSON + historique Parquet) -> silver et rejeu bronze.
- analytics : indicateurs et agrégations métier répondant aux 5 questions P2.
- flow      : orchestration Prefect 3 (streaming planifié, alertes, backfill).

Responsable : P2.
"""
