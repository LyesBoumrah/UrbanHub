# UrbanHub — Jumeau numérique urbain

Projet école Big Data. Plateforme **100 % Data Lake (Medallion : Bronze brut → Silver Parquet → Gold JSON métier)**
qui **collecte, stocke, traite, analyse et visualise** des données urbaines (météo, vélos en libre-service,
pollution de l'air) pour produire des indicateurs utiles aux décideurs publics.

**Villes couvertes** : Paris, Lille, Marseille, Rouen, Lyon, Toulouse, Bordeaux,
Nantes, Strasbourg, Montpellier, Rennes, Clermont-Ferrand.

## Les 4 parties

| Partie | Type de flux | Thème | Source | Responsable |
|---|---|---|---|---|
| P1 | Batch (historique 5 ans + récent) | Météo | [Open-Meteo Archive & Forecast API](https://archive-api.open-meteo.com/) | Membre P1 |
| P2 | Streaming (toutes les 3 min) | Vélos | [CityBikes API v2](https://api.citybik.es/v2/) | Membre P2 |
| P3 | IoT (horaire) | Pollution | [OpenAQ API v3](https://docs.openaq.org) | Membre P3 |
| P4 | Analyse croisée | Météo × mobilité × pollution | P1 + P2 + P3 | Commun |

## Architecture Data Lake (Medallion 100 % MinIO)

```
Open-Meteo ─► p1_meteo/ingestion.py ─────┐
CityBikes ─► p2_velos/ingestion.py ─────┼──► MinIO — Bucket `bronze` (données brutes immuables)
OpenAQ ────► p3_pollution/ingestion.py ─┘              │
                                                       ▼  cleaning.py + quality.py
                                          MinIO — Bucket `silver` (fichiers Parquet nettoyés uniquement)
                                                       │  p4_croise/build_gold.py
                                                       ▼
                                          MinIO — Bucket `gold` (fichiers JSON métier P1–P4 uniquement)
                                                       │
                                                       ▼
                                          Streamlit (dashboard) + notebooks Jupyter
Orchestration : Python CLI / Docker workers parallèles — Environnement : Docker Compose
```

Toutes les données sont stockées en **UTC**. La clé de jointure entre les 3 flux est `(ville_id, heure_utc)`.

## Stack

- **Python 3.11+** : requests, pandas, pyarrow
- **MinIO / Silo** : Data Lake objet S3 structuré en **3 buckets dédiés** (`s3://bronze` brut, `s3://silver` Parquet, `s3://gold` JSON métier)
- **Streamlit + Plotly** : dashboards interactifs ; **Jupyter** : analyses exploratoires
- **pytest + flake8 + GitHub Actions** : tests unitaires et CI
- **Docker Compose** : environnement complet (Silo/MinIO, 4 workers parallèles P1/P2/P3/P4, Streamlit)

## Arborescence

```
├── config/          villes.json, ref_ville.json, openaq_locations.json
├── common/          code partagé : config, MinIO / stockage local, alertes, logs
├── scripts/         check_sources.py, init_datalake.py
├── p1_meteo/        P1 : ingestion, cleaning, quality, analytics, flow
├── p2_velos/        P2 : ingestion, backfill, cleaning, quality, analytics, flow
├── p3_pollution/    P3 : openaq_client, explore, storage, ingestion, cleaning, quality, analytics, flow
├── p4_croise/       P4 : build_gold (génération des JSON métier), analytics, flow
├── notebooks/       analyses exploratoires (un notebook par partie : 01 à 04)
├── dashboard/       Streamlit : Accueil.py + pages/ (1_Meteo, 2_Velos, 3_Pollution, 4_Croise)
├── tests/           tests unitaires (test_p1_*, test_p2_*, test_p3_*, test_p4_*)
└── data/            données locales (ignorées par git)
```

## Documentation des modules

### Socle commun (`config/`, `common/`, `scripts/`)

| Module / Fichier | Utilité |
|---|---|
| `config/villes.json` | Liste des 12 métropoles françaises et leurs coordonnées de référence. |
| `config/ref_ville.json` | Référentiel enrichi des 12 principales villes françaises (centre GPS, `bbox` de validation, `network_id` CityBikes, `noaa_station_id`, `openaq_location_ids`). |
| `config/openaq_locations.json` | Sélection figée des 55 stations officielles OpenAQ v3 (164 capteurs actifs couvrant PM2.5, PM10, NO2, O3, CO). |
| `common/config.py` | Charge une seule fois la configuration globale (`Settings`) depuis `.env` et expose `load_villes()` / `load_ref_ville()`. |
| `common/log.py` | Fournit un logger structuré uniforme (`get_logger`) partagé par tous les pipelines. |
| `common/storage.py` | Client Data Lake (`MinIOStorage` compatible Silo/MinIO + `LocalBronzeStorage` hors Docker) : `put_json`, `get_json`, `put_csv`, `put_bytes`, `put_parquet`, `get_parquet`, `exists`, `list`. |
| `common/alerts.py` | Journalise et archive les anomalies pipeline (`INFO` / `WARNING` / `CRITICAL`) dans le bucket `gold` (`alertes/`). |
| `scripts/init_datalake.py` | Vérifie le référentiel villes et crée les 3 buckets MinIO dédiés (`bronze`, `silver`, `gold`). |
| `scripts/check_sources.py` | Vérifie la disponibilité des réseaux CityBikes configurés dans `config/ref_ville.json`. |
| `docker-compose.yml` | Déploie la stack conteneurisée (Silo/MinIO, worker P2, Streamlit) avec des versions d'images figées. |

### Partie 1 — Analyse météorologique urbaine (`p1_meteo/`)

> **Note sur la source météo** : L'archive NOAA Global Hourly (`/data/global-hourly/access/`) ayant cessé de publier des données récentes (arrêt en août 2025), le flux P1 s'appuie sur **Open-Meteo** (`archive-api.open-meteo.com` + `api.open-meteo.com/v1/forecast` pour les 14 derniers jours), garantissant à la fois **5 ans d'historique horaire** et des **données récentes jusqu'au jour même** pour la jointure P4 avec CityBikes et OpenAQ.

| Module / Fichier | Utilité |
|---|---|
| `p1_meteo/ingestion.py` | Téléchargement parallèle (`ThreadPoolExecutor`) des 12 métropoles françaises sur 5 ans glissants + données récentes vers `bronze/meteo/<ville_id>/<debut>_<fin>.json`. |
| `p1_meteo/cleaning.py` | Extraction des 8 variables demandées (`station_id`, `timestamp`, `temperature`, `pressure`, `wind_speed`, `wind_direction`, `precipitation`, `visibility`), conversion d'unités (`km/h` -> `m/s`), interpolation/imputation des valeurs manquantes et écriture Parquet silver (`silver/meteo/meteo_obs.parquet`, `meteo_station.parquet`) strictement depuis la couche bronze. |
| `p1_meteo/quality.py` | Règles qualité pures : bornes de plausibilité physique, détection des sauts thermiques isolés (`> 15 °C/h`), taux de complétude. |
| `p1_meteo/analytics.py` | Fonctions analytiques répondant aux 4 questions métier (Q1 anomalies z-score désaisonnalisées, Q2 corrélations visibilité, Q3 saisonnalité France & villes, Q4 journées extrêmes). |
| `p1_meteo/flow.py` | Orchestration batch complète (`bronze -> silver Parquet -> alertes`). |
| `dashboard/pages/1_Meteo.py` | Dashboard interactif P1 à 4 onglets (Q1–Q4) lisant la couche Silver Parquet. |
| `notebooks/01_meteo.ipynb` | Analyse exploratoire Jupyter P1 avec visualisations et interprétation des résultats. |
| `tests/test_p1_quality.py` | Tests unitaires `pytest` sur le téléchargement parallèle, la conversion d'unités, les règles qualité et les analyses Q1–Q4. |

### Partie 2 — Mobilité urbaine (`p2_velos/`)

Documentation détaillée dans [`p2_velos/README.md`](p2_velos/README.md).

| Module / Fichier | Utilité |
|---|---|
| `p2_velos/ingestion.py` | Collecte temps réel des stations CityBikes v2 toutes les 2–3 min et archivage JSON brut en bronze (`bronze/velos/`). |
| `p2_velos/backfill.py` | Récupération de l'historique sur une fenêtre de 5 ans depuis l'archive officielle Parquet `data.citybik.es` vers `bronze/velos_historique/` puis `silver/velos_historique/`. |
| `p2_velos/quality.py` | Règles qualité pures : parsing timestamps, compteurs, capacité, `bbox`, fraîcheur, statut critique. |
| `p2_velos/cleaning.py` | Nettoyage et normalisation unifiés (streaming JSON + historique Parquet, rééchantillonnage `1h`, dédoublonnage) -> Silver Parquet (`silver/velos/`). |
| `p2_velos/analytics.py` | Fonctions métier partagées répondant aux 5 questions (utilisation, sous-offre, pics horaires, déséquilibres géo, rééquilibrage). |
| `p2_velos/flow.py` | Orchestration Python : streaming planifié toutes les 3 min (`velos_streaming_flow`), alertes automatiques et backfill 5 ans (`velos_backfill_flow`). |
| `dashboard/pages/2_Velos.py` | Dashboard interactif P2 (5 onglets Q1–Q5, cartes Plotly, fenêtre jusqu'à 5 ans, export CSV rééquilibrage). |
| `notebooks/02_velos.ipynb` | Analyse exploratoire Jupyter répondant aux 5 questions métier avec recommandations opérationnelles. |
| `tests/test_p2_quality.py` | Tests unitaires `pytest` sur les règles qualité, la normalisation, le backfill 5 ans et le rejeu bronze. |

### Partie 3 — Pollution urbaine IoT (`p3_pollution/`)

| Module / Fichier | Utilité |
|---|---|
| `p3_pollution/openaq_client.py` | Client HTTP OpenAQ v3 thread-safe avec limiteur de débit partagé (`SharedRateLimiter`, 60 req/min) et backoff exponentiel. |
| `p3_pollution/explore.py` | Exploration et sélection gloutonne des stations officielles couvrant les 5 polluants (`PM2.5`, `PM10`, `NO2`, `O3`, `CO`) autour des 12 villes françaises. |
| `p3_pollution/storage.py` | Abstraction de stockage bronze/silver (`LocalStorage` et `MinioStorage`) supportant JSON, JSON.gz et Parquet. |
| `p3_pollution/ingestion.py` | Collecte régulière IoT (`/locations/{id}/latest`) et rattrapage historique (`/sensors/{id}/hours`) parallélisés vers `bronze/openaq*`. |
| `p3_pollution/quality.py` | Normalisation des polluants, conversion d'unités (`ppb`, `ppm`, `mg/m³` -> `µg/m³`), bornes physiques par polluant, seuils OMS 2021 et ATMO. |
| `p3_pollution/cleaning.py` | Extraction des 7 variables demandées (`sensor_id`, `longitude`, `unit`, `pollutant`, `timestamp`, `latitude`, `value`), enrichissement spatial, dédoublonnage `(sensor_id, timestamp)` -> Silver Parquet (`silver/pollution/air_mesure.parquet`, `air_capteur.parquet`). |
| `p3_pollution/analytics.py` | Fonctions analytiques répondant aux 4 questions métier (Q1 zones les plus polluées, Q2 cycle journalier, Q3 polluants dominants, Q4 épisodes anormaux). |
| `p3_pollution/flow.py` | Orchestration du flux IoT (`bronze -> silver Parquet -> alertes`). |
| `dashboard/pages/3_Pollution.py` | Dashboard interactif P3 à 4 onglets (carte des stations, cycle diurne, polluants dominants, épisodes anormaux). |
| `notebooks/03_pollution.ipynb` | Analyse exploratoire Jupyter P3 répondant aux 4 questions métier. |
| `tests/test_p3_*.py` | Tests unitaires `pytest` sur le client OpenAQ, l'exploration, l'ingestion, la conversion d'unités, la qualité et les analyses Q1–Q4. |

### Partie 4 — Analyse croisée & Couche Gold JSON (`p4_croise/`)

| Module / Fichier | Utilité |
|---|---|
| `p4_croise/build_gold.py` | Lit strictement la couche Silver Parquet (`silver/meteo/`, `silver/velos/`, `silver/pollution/`), joint Météo (P1) × Vélos (P2) × Pollution (P3) sur `(ville_id, heure_utc)`, calcule les indicateurs croisés et produit **uniquement des fichiers JSON métier** dans `gold/` (`gold/p1_meteo/q1..q4.json`, `gold/p2_velos/q1..q5.json`, `gold/p3_pollution/q1..q4.json`, `gold/p4_croise/q1..q4.json`, `gold/p4_croise/gold_ville_heure.json`, `gold/synthese_indicateurs_urbains.json`). |
| `p4_croise/analytics.py` | Fonctions analytiques croisées répondant aux 4 questions métier (Q1 météo × pollution, Q2 météo × vélos, Q3 créneaux favorables mobilité douce, Q4 interactions tripartites fortes). |
| `p4_croise/flow.py` | Orchestration du rafraîchissement de la couche Gold JSON et détection d'alertes multi-facteurs. |
| `dashboard/pages/4_Croise.py` | Dashboard interactif P4 à 4 onglets croisant météo, mobilité douce et qualité de l'air depuis la couche Gold JSON. |
| `notebooks/04_croise.ipynb` | Analyse exploratoire croisée Jupyter et recommandations pour les décideurs publics. |
| `tests/test_p4_croise.py` | Tests unitaires `pytest` sur la jointure Gold `(ville_id, heure_utc)`, le format strict Silver=Parquet / Gold=JSON et les analyses croisées Q1–Q4. |

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

# 3. Lancer l'environnement Data Lake (MinIO, worker P2, Streamlit)
docker compose up -d

# 4. Exécuter les pipelines (ou ajouter --local hors Docker)
python -m p1_meteo.flow
python -m p2_velos.flow --once
python -m p3_pollution.flow --once
python -m p4_croise.flow

# 5. Tests et qualité du code
flake8 .
pytest
```

> Les données ne sont **pas** versionnées : chacun les régénère en lançant les scripts d'ingestion.
