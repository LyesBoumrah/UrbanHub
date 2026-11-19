# UrbanHub — Jumeau numérique urbain

Projet école Big Data. Plateforme qui **collecte, stocke, traite, analyse et visualise** des données
urbaines (météo, vélos en libre-service, pollution de l'air) pour produire des indicateurs utiles aux
décideurs publics.

**Villes couvertes** : Paris, Lille, Marseille, Rouen, Lyon.

## Les 4 parties

| Partie | Type de flux | Thème | Source | Responsable |
|---|---|---|---|---|
| P1 | Batch (historique 5 ans) | Météo | [NOAA Global Hourly (ISD)](https://www.ncei.noaa.gov/products/land-based-station/integrated-surface-database) | Membre P1 |
| P2 | Streaming (toutes les 3 min) | Vélos | [CityBikes API v2](https://api.citybik.es/v2/) | Membre P2 |
| P3 | IoT (horaire) | Pollution | [OpenAQ API v3](https://docs.openaq.org) | Membre P3 |
| P4 | Analyse croisée | Météo × mobilité × pollution | P1 + P2 + P3 | Commun |

## Architecture

```
NOAA ──────► p1_meteo/ingestion.py ─────┐
CityBikes ─► p2_velos/ingestion.py ─────┼──► MinIO (bronze : données brutes immuables)
OpenAQ ────► p3_pollution/ingestion.py ─┘              │
                                                       ▼  cleaning.py + quality.py
                                          ClickHouse (silver : tables propres)
                                                       │  p4_croise/build_gold.py
                                                       ▼
                                          ClickHouse (gold : agrégats ville × heure)
                                                       │
                                                       ▼
                                          Streamlit (dashboard) + notebooks
Orchestration : Prefect (un flow par partie) — Environnement : Docker Compose
```

Toutes les données sont stockées en **UTC**. La clé de jointure entre les 3 flux est `(ville_id, heure_utc)`.

## Stack

- **Python 3.11** : requests, pandas, pyarrow
- **MinIO** : data lake (couche bronze)
- **ClickHouse** : entrepôt de données (silver / gold)
- **Prefect 3** : orchestration
- **Streamlit + Plotly** : dashboards ; **Jupyter** : analyses exploratoires
- **pytest + flake8 + GitHub Actions** : tests et CI
- **Docker Compose** : environnement complet

## Arborescence

```
├── config/          villes.json, ref_ville.json (liens ville ↔ sources)
├── common/          code partagé : config, MinIO, ClickHouse, alertes, logs
├── scripts/         check_sources.py, init_warehouse.py
├── sql/             schéma, un fichier par partie (exécutés par ordre alphabétique)
├── p1_meteo/        P1 : ingestion, cleaning, quality, flow
├── p2_velos/        P2 : ingestion, cleaning, quality, flow
├── p3_pollution/    P3 : ingestion, cleaning, quality, flow
├── p4_croise/       P4 : build_gold, flow
├── notebooks/       analyses exploratoires (un notebook par partie)
├── dashboard/       Streamlit : Accueil.py + pages/ (une page par partie)
├── tests/           tests unitaires (un fichier par partie)
└── data/            données locales (ignorées par git)
```

## Documentation des modules

### Socle commun (`config/`, `common/`, `scripts/`, `sql/`)

| Module / Fichier | Utilité |
|---|---|
| `config/villes.json` | Liste des 5 villes cœur UrbanHub (Paris, Lille, Marseille, Rouen, Lyon) et leurs coordonnées de référence. |
| `config/ref_ville.json` | Référentiel enrichi des 12 principales villes françaises (centre GPS, `bbox` de validation, `network_id` CityBikes, identifiants NOAA / OpenAQ). |
| `common/config.py` | Charge une seule fois la configuration globale (`Settings`) depuis `.env` et expose `load_villes()` / `load_ref_ville()`. |
| `common/log.py` | Fournit un logger structuré uniforme (`get_logger`) partagé par tous les pipelines. |
| `common/storage.py` | Client data lake bronze (`MinIOStorage` compatible Silo/MinIO + `LocalBronzeStorage` hors Docker) : `put_json`, `put_csv`, `put_bytes`, `get_json`, `get_bytes`, `exists`, `list`. |
| `common/warehouse.py` | Client ClickHouse (`ClickHouseWarehouse`) : `ping`, `execute`, `insert_df`, `insert_rows`, `query_df`. |
| `common/alerts.py` | Journalise et persiste les anomalies pipeline (`INFO` / `WARNING` / `CRITICAL`) dans `urbanhub.pipeline_alertes`. |
| `scripts/init_warehouse.py` | Applique les scripts `sql/*.sql` par ordre alphabétique, peuple `urbanhub.ref_ville` et initialise le bucket bronze. |
| `scripts/check_sources.py` | Vérifie la disponibilité des réseaux CityBikes configurés dans `config/ref_ville.json`. |
| `sql/00_commun.sql` | Crée la base `urbanhub` et les tables transverses `ref_ville` et `pipeline_alertes`. |
| `docker-compose.yml` | Déploie toute la stack conteneurisée (Silo/MinIO, ClickHouse, client web ClickHouse CH-UI, Prefect 3, worker P2, Streamlit) avec des versions d'images figées. |

### Partie 2 — Mobilité urbaine (`p2_velos/`, `sql/20_p2_velos.sql`, `dashboard/`, `notebooks/`, `tests/`)

Documentation détaillée dans [`p2_velos/README.md`](p2_velos/README.md).

| Module / Fichier | Utilité |
|---|---|
| `p2_velos/ingestion.py` | Collecte temps réel des stations CityBikes v2 toutes les 2–3 min et archivage JSON brut en bronze. |
| `p2_velos/backfill.py` | Récupération de l'historique sur une fenêtre de 5 ans depuis l'archive officielle Parquet `data.citybik.es`. |
| `p2_velos/quality.py` | Règles qualité pures (stdlib uniquement, exécutables en CI) : parsing timestamps, compteurs, capacité, `bbox`, fraîcheur, statut critique. |
| `p2_velos/cleaning.py` | Nettoyage et normalisation unifiés (streaming JSON + historique Parquet, rééchantillonnage `1h`, dédoublonnage) + rejeu CLI complet `bronze -> silver`. |
| `p2_velos/analytics.py` | Fonctions métier partagées répondant aux 5 questions (utilisation, sous-offre, pics horaires, déséquilibres géo, rééquilibrage). |
| `p2_velos/flow.py` | Flows Prefect 3 : streaming planifié toutes les 2m30 (`velos_streaming_flow`), alertes automatiques et backfill 5 ans (`velos_backfill_flow`). |
| `sql/20_p2_velos.sql` | Tables silver `velo_station` / `velo_releve` et 7 vues analytiques ClickHouse (Q1–Q5, tendance mensuelle, agrégat horaire P4). |
| `dashboard/Accueil.py` | Page d'accueil Streamlit (état de ClickHouse, villes configurées, dernières alertes pipeline). |
| `dashboard/pages/2_Velos.py` | Dashboard interactif P2 (5 onglets Q1–Q5, cartes Plotly, fenêtre jusqu'à 5 ans, export CSV rééquilibrage). |
| `notebooks/02_velos.ipynb` | Analyse exploratoire Jupyter répondant aux 5 questions métier avec recommandations opérationnelles. |
| `tests/test_p2_quality.py` | Tests unitaires `pytest` sur les règles qualité, la normalisation, le backfill 5 ans et le rejeu bronze. |

## Démarrage rapide

```bash
# 1. Cloner et installer
git clone https://github.com/LyesBoumrah/UrbanHub.git
cd UrbanHub
python -m venv .venv
.venv\Scripts\activate          # Windows  (Linux/Mac : source .venv/bin/activate)
pip install -r requirements.txt

# 2. Secrets : copier le modèle puis remplir les valeurs (dont OPENAQ_API_KEY)
cp .env.example .env

# 3. Lancer l'environnement (MinIO, ClickHouse, Prefect, Streamlit)
docker compose up -d

# 4. Tests
pytest
```

> Les données ne sont **pas** versionnées : chacun les régénère en lançant les scripts d'ingestion.

## Règles de travail en groupe

1. Une branche par partie : `p1-meteo`, `p2-velos`, `p3-pollution`. Fusion dans `main` par **pull request**.
2. Chacun ne modifie que **ses** fichiers : son dossier `pX_*/`, son fichier `sql/`, sa page `dashboard/pages/`,
   son notebook et ses tests.
3. Fichiers communs (`common/`, `docker-compose.yml`, `requirements.txt`) : petite PR dédiée, en prévenant le groupe.
   Dans `requirements.txt`, ajoutez vos dépendances dans votre section.
4. **Jamais** de `.env`, de clé API ni de données dans git.
