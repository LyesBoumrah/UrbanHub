"""Code partagé par toutes les parties de la plateforme UrbanHub.

Sous-modules :
- config    : chargement centralisé de Settings (.env) et des référentiels villes JSON.
- log       : configuration uniforme du logging structuré.
- storage   : client bronze S3 (MinIOStorage) et fallback disque local (LocalBronzeStorage).
- warehouse : client entrepôt ClickHouse (ClickHouseWarehouse).
- alerts    : journalisation et persistance des anomalies dans urbanhub.pipeline_alertes.

Responsable : commun.
"""
