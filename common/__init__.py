"""Code partagé par toutes les parties de la plateforme Data Lakehouse UrbanHub.

Sous-modules :
- config    : chargement centralisé de Settings (.env) et des référentiels villes JSON.
- log       : configuration uniforme du logging structuré.
- storage   : client Data Lake S3 multi-buckets MinIO (`bronze`, `silver`, `gold`) + fallback local.
- warehouse : client analytique ClickHouse (`ClickHouseWarehouse`, `get_warehouse`, `try_get_warehouse`).
- alerts    : journalisation structurée des anomalies (logs + JSON Gold + table ClickHouse `pipeline_alertes`).
- period    : parseur de durée flexible (`1 semaine` par défaut jusqu'à `5 ans`).

Responsable : commun.
"""
