"""Code partagé par toutes les parties de la plateforme Data Lake UrbanHub.

Sous-modules :
- config    : chargement centralisé de Settings (.env) et des référentiels villes JSON.
- log       : configuration uniforme du logging structuré.
- storage   : client Data Lake S3 (MinIOStorage) et fallback disque local (LocalBronzeStorage)
              pour les couches bronze (brut), silver (Parquet) et gold (JSON métier).
- alerts    : journalisation structurée des anomalies de pipeline.

Responsable : commun.
"""
