# UrbanHub — Jumeau numérique urbain

Projet école Big Data. Plateforme qui **collecte, stocke, traite, analyse et visualise** des données
urbaines (météo, vélos en libre-service, pollution de l'air) pour produire des indicateurs utiles aux
décideurs publics.

**Villes couvertes** : Paris, Lille, Marseille, Rouen, Lyon, Toulouse, Bordeaux,
Nantes, Strasbourg, Montpellier, Rennes, Clermont-Ferrand.

## Les 4 parties

| Partie | Type de flux | Thème | Source | Responsable |
|---|---|---|---|---|
| P1 | Batch (historique 5 ans) | Météo | [Open-Meteo Archive API](https://archive-api.open-meteo.com/) | Membre P1 |
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
