# Partie 2 — Mobilité urbaine (`p2_velos`)

Collecte temps réel (**toutes les 2–3 minutes** via `api.citybik.es/v2`) et backfill historique sur une fenêtre de **5 ans** (via l'archive officielle `data.citybik.es`) des stations de vélos en libre-service des 12 principales villes françaises.

---

## Architecture du flux P2

```text
Sources de données (api.citybik.es/v2 + data.citybik.es)
      │
      ▼  Ingestion parallélisée (ingestion.py / backfill.py)
Stockage Data Lake — Couche Bronze (MinIO : bronze/velos/, bronze/velos_historique/)
      │
      ▼  Traitement Data Engineering (cleaning.py + quality.py)
Stockage Data Lake — Couche Silver Parquet (MinIO : silver/velos/, silver/velos_historique/)
      │
      ▼  Chargement Entrepôt depuis Silver Parquet (load_warehouse_from_silver_parquet)
Data Warehouse ClickHouse (urbanhub.velo_station, urbanhub.velo_releve + 7 vues)
      │
      ▼  Analyse Data (analytics.py + 02_velos.ipynb)
Indicateurs urbains (Streamlit 2_Velos.py + CH-UI + alertes pipeline)
```

---

## Rôle de chaque module

### Pipeline `p2_velos/`

| Module | Rôle principal | Entrées / Sorties | Commande / Usage |
|---|---|---|---|
| `p2_velos/ingestion.py` | **Collecte temps réel (Bronze)** : interroge l'API CityBikes v2 avec retry/backoff exponentiel pour chaque ville de `config/ref_ville.json` et archive les réponses JSON brutes immuables partitionnées par ville, date et heure UTC (`bronze/velos/{ville}/dt=.../hr=.../`). | CityBikes API v2 → MinIO ou `data/bronze/velos/` | `python -m p2_velos.ingestion [--local] [--cities paris lyon]` |
| `p2_velos/backfill.py` | **Backfill historique 5 ans parallélisé par année (Bronze → Silver)** : calcule la fenêtre glissante sur 5 ans, recense et télécharge **en parallèle par année** (`ThreadPoolExecutor`, 1 worker/session HTTP/client ClickHouse par année) les dumps Parquet mensuels officiels sur `data.citybik.es/dumps/by-network/` (disponibles depuis novembre 2024), les archive en bronze (`bronze/velos_historique/`) de façon idempotente, puis délègue le nettoyage à `cleaning.py`. | `data.citybik.es` → Bronze + ClickHouse ou `data/silver/velos_historique/` | `python -m p2_velos.backfill --years 5 --max-workers 5 [--local]` |
| `p2_velos/quality.py` | **Règles de qualité pures (100 % bibliothèque standard Python)** : parse les timestamps CityBikes (y compris le suffixe `+00:00Z`), vérifie les 7 champs obligatoires, les compteurs `>= 0`, la capacité `> 0` et sa cohérence avec `extra.slots`, les coordonnées GPS + `bbox` de la ville, la fraîcheur (`<= 30 min` en streaming), et calcule le diagnostic de rééquilibrage (`PENURIE_CRITIQUE` / `SATURATION_CRITIQUE`). | Enregistrements bruts → `QualityReport` (valides, rejetés, motifs) | Importé par `cleaning.py` et testé en CI sans dépendance externe |
| `p2_velos/cleaning.py` | **Nettoyage & normalisation unifiés (Bronze → Silver)** : normalise les payloads streaming JSON (`clean_bronze_payload`) **et** les dumps historiques Parquet (`clean_historical_parquet`, vectorisé pandas avec rééchantillonnage horaire `1h`), dédoublonne sur `(ville_id, station_id, timestamp)`, enrichit les indicateurs métier et sépare dimension (`velo_station`) et faits (`velo_releve`). Permet aussi de **rejouer** le nettoyage sur toute la couche bronze. | Bronze (JSON + Parquet) → ClickHouse (`velo_station`, `velo_releve`) ou `data/silver/` | `python -m p2_velos.cleaning --all [--streaming \| --historique] [--local]` |
| `p2_velos/analytics.py` | **Couche analytique métier (DRY)** : regroupe les fonctions partagées entre le dashboard Streamlit et le notebook Jupyter répondant aux 5 questions métier (`top_utilisation_stations`, `detect_undersupplied_zones`, `daily_usage_peaks`, `geographic_imbalance`, `detect_critical_stations`). | DataFrame `velo_releve` → DataFrames agrégés Q1 à Q5 | Importé par `dashboard/pages/2_Velos.py` et `notebooks/02_velos.ipynb` |
| `p2_velos/flow.py` | **Orchestration Prefect 3** : définit le flow streaming `velos_streaming_flow` (collecte → nettoyage/qualité → chargement silver → alertes automatiques) planifié toutes les **2 min 30** (`--serve --interval 150`), une boucle locale sans serveur Prefect, et le flow de backfill `velos_backfill_flow` (`--backfill --years 5`). | Orchestre `ingestion`, `backfill`, `cleaning`, `alerts` | `python -m p2_velos.flow --serve --interval 150` / `--once` / `--backfill` |

### Fichiers associés à P2

| Fichier | Rôle |
|---|---|
| `sql/20_p2_velos.sql` | Crée les tables ClickHouse silver (`urbanhub.velo_station`, `urbanhub.velo_releve`) contenant les **7 variables demandées** (`station_id`, `station_name`, `latitude`, `longitude`, `bikes_available`, `free_slots`, `timestamp`), ainsi que **7 vues analytiques** : Q1 (`v_velo_utilisation_stations`), Q2 (`v_velo_zones_sous_offre`), Q3 (`v_velo_pics_horaires`), Q4 (`v_velo_desequilibre_geo`), Q5 (`v_velo_stations_critiques`), tendance multi-annuelle (`v_velo_tendance_mensuelle`) et agrégat horaire P4 (`v_velo_ville_heure`). |
| `dashboard/pages/2_Velos.py` | Page Streamlit interactive avec 5 onglets dédiés aux 5 questions métier, sélecteur de fenêtre jusqu'à 5 ans, cartes géographiques Plotly (`scatter_map`), bouton de collecte temps réel et export CSV du plan de rééquilibrage. |
| `notebooks/02_velos.ipynb` | Notebook Jupyter d'analyse exploratoire présentant les indicateurs, graphiques Plotly et recommandations opérationnelles pour les 5 questions métier. |
| `tests/test_p2_quality.py` | Suite `pytest` (17 tests, 100 % bibliothèque standard) couvrant parsing timestamps, règles qualité, classification criticité, pipeline bronze → silver, fenêtre de backfill 5 ans et rejeu bronze. |

---

## Correspondance avec les 5 questions métier

1. **Stations au plus fort taux d'utilisation** → `analytics.top_utilisation_stations` / vue `v_velo_utilisation_stations` (taux d'utilisation moyen `free_slots / capacity`, volatilité, amplitude et rotation totale).
2. **Zones où l'offre de vélos est insuffisante** → `analytics.detect_undersupplied_zones` / vue `v_velo_zones_sous_offre` (grille spatiale ~1 km ou ~110 m, `% temps vide` et `% temps <= 15 %` de vélos disponibles).
3. **Pics d'utilisation journaliers** → `analytics.daily_usage_peaks` / vues `v_velo_pics_horaires` et `v_velo_tendance_mensuelle` (profil 0h–23h heure locale `Europe/Paris`, semaine vs week-end + évolution mensuelle).
4. **Déséquilibres géographiques** → `analytics.geographic_imbalance` / vue `v_velo_desequilibre_geo` (comparaison Centre `< 1,5 km` vs quadrants Nord-Est, Nord-Ouest, Sud-Est, Sud-Ouest et écart à la moyenne de la ville).
5. **Stations critiques nécessitant un rééquilibrage** → `analytics.detect_critical_stations` / vue `v_velo_stations_critiques` (pénurie `<= 10 %` → `REAPPROVISIONNER`, saturation `>= 90 %` → `DESENGORGER`, score de priorité et nombre de vélos `delta_velos_equilibre` à déplacer).

---

## Attribution & note sur l'historique

- *Bike-share data by CityBikes contributors and available from https://data.citybik.es (ODbL v1.0).*
- L'API temps réel `api.citybik.es/v2` ne fournit que l'état instantané ; l'archive publique `data.citybik.es` couvre la période à partir de **novembre 2024**. La fenêtre de backfill `--years 5` récupère automatiquement l'intégralité des mois réels publiés sur les 5 dernières années.
