# UrbanHub — Jumeau numérique urbain

Projet école Big Data. Plateforme qui **collecte, stocke, traite, analyse et visualise** 5 ans de données
urbaines — **météo, vélos en libre-service, pollution de l'air** — sur **12 grandes villes françaises**, pour
produire des indicateurs utiles aux décideurs publics.

**Villes** : Paris, Lille, Marseille, Rouen, Lyon, Toulouse, Bordeaux, Nantes, Strasbourg, Montpellier,
Rennes, Clermont-Ferrand (`config/villes.json`, `config/ref_ville.json`).

> Ce README est tenu à jour **à chaque étape** du pipeline : il sert de support pour expliquer le projet.
> État d'avancement : ✅ Extract — ✅ Transform (silver des 3 sources + croisement P4 + analyses P1 à P4)
> — ✅ Load / Gold (ClickHouse) — ✅ Dashboard — ⏳ ML

---

## 1. Les 4 parties et les sources retenues

| Partie | Thème | Source (archive) | Période réellement disponible |
|---|---|---|---|
| **P1** | Météo | [Open-Meteo Archive API](https://open-meteo.com/en/docs/historical-weather-api) (réanalyse ERA5, sans clé) | 01/11/2021 → 31/10/2026, 0 % manquant |
| **P2** | Vélos en libre-service | [Archive CityBikes](https://data.citybik.es/) — dumps Parquet mensuels par réseau (ODbL) | **11/2024 → 09/2026** (l'archive n'existe pas avant) |
| **P3** | Pollution de l'air | [Archive OpenAQ](https://openaq.org/) sur AWS S3 (`openaq-data-archive`, sans clé) — stations de référence EEA | 01/11/2021 → 31/10/2026, avec trous |
| **P4** | Analyse croisée | P1 × P2 × P3, jointure `(ville_id, heure_utc)` | météo × pollution : 5 ans — avec vélos : 11/2024 → 09/2026 |

### Pourquoi ces sources (journal des décisions)

- **Archives uniquement, pas de temps réel** — consigne du professeur : on travaille sur l'historique des
  **5 dernières années glissantes, en mois complets** (le 20/11/2026 : du 01/11/2021 au 31/10/2026).
  La même fonction (`common/period.py`) fixe la période pour les 3 sources → dates strictement identiques pour P4.
- **Météo = Open-Meteo seul** : complet et homogène sur 5 ans pour les 12 villes. NOAA ISD (cité par le sujet)
  a été écarté : pas de fichier 2026 publié et pression/pluie renseignées à ~34 % seulement.
  *Limite* : la **visibilité** est absente d'Open-Meteo (100 % vide) → remplacée par l'**humidité relative**
  et la **couverture nuageuse**. Ce sont des valeurs **modélisées** (grille ~9-25 km), pas des mesures de station.
- **Vélos = CityBikes seul** : seule archive couvrant **les 12 villes** dans un format unique.
  Aucune source publique ne fournit 5 ans de disponibilité des stations (les opérateurs ne publient que le temps réel).
  L'archive *MaxHalford/bike-sharing-history* remonte à 08/2023 mais pour 4 villes seulement : écartée pour ne pas
  biaiser les comparaisons entre villes.
- **Pollution = archive OpenAQ** plutôt que l'API : l'API impose 1 requête par capteur, 1 000 lignes par réponse
  et 2 000 requêtes/h (≈ 4 h 30 pour 5 ans) ; l'archive S3 est publique, sans quota (≈ 8 min).
  Comparaison archive ↔ API (Lyon Centre, NO2) : valeurs **identiques** en 2024-2025 ; l'archive est **incomplète
  sur 2021-2023** (58 à 90 % des heures de l'API) → limite documentée, trous non comblés.
- **Principe « propre et juste »** : une source par variable, on **ne comble pas** les trous, on **marque**
  les valeurs suspectes (`quality_flag`) au lieu de les supprimer, et chaque couverture est **chiffrée**.

---

## 2. Architecture (médaillon bronze / silver / gold)

```
 EXTRACT                          TRANSFORM                                   LOAD
 ───────                          ─────────                                   ────
 Open-Meteo ─┐                    bronze ─► nettoyage par source ─► silver    silver ─► agrégats ─► gold
 CityBikes  ─┼─► Silo bronze/     (UTC, ville_id, unités, qualité)   Parquet  (ClickHouse)          │
 OpenAQ     ─┘   (bruts, tels      └─► croisement P4 (ville × heure)  dans Silo                      ▼
                  quels)                                                       Streamlit · conclusions · ML
```

| Couche | Stockage | Contenu | Règle |
|---|---|---|---|
| **Bronze** | Silo (S3), bucket `urbanhub`, `bronze/` | Copie **octet pour octet** des archives + un **manifeste** par exécution | Jamais modifié : tout est rejouable |
| **Silver** | Silo, `silver/`, fichiers **Parquet** partitionnés par année | Données propres, **UTC**, `ville_id`, unités harmonisées, `quality_flag`, tables de couverture, croisement P4 | Recalculable depuis le bronze |
| **Gold** | ClickHouse (base `urbanhub`) | Agrégats métier prêts pour le dashboard et le ML | Recalculable depuis le silver |

**Silo** (`pgsty/silo`) est un stockage objet compatible S3 (remplaçant open source de MinIO) :
un *bucket* contient des *objets* identifiés par une *clé* (`bronze/meteo/ville=paris/…json`). Les `/` ne sont
pas de vrais dossiers mais font partie du nom ; découper les clés en `year=…/month=…` (**partitionnement**)
permet de ne lire que les fichiers utiles. Silo expose une **API S3** (pour le code) et une **console web** (pour nous).

### Organisation du data lake

```
s3://urbanhub/
├── bronze/
│   ├── meteo/ville=<ville_id>/2021-11-01_2026-10-31.json             12 fichiers (1 par ville, 5 ans horaires)
│   ├── velos_historique/<ville_id>/<année>/<AAAAMM>-<réseau>-stats.parquet   263 fichiers mensuels
│   ├── openaq_archive/locationid=<id>/year=<YYYY>/month=<MM>/location-<id>-<YYYYMMDD>.csv.gz   69 421 fichiers
│   └── <source>/_manifests/<horodatage>_<batch>.json                 traçabilité de chaque extraction
└── silver/                                                           (étape Transform)
```

---

## 3. Étape 1 — EXTRACT ✅

**Objectif** : récupérer les 3 archives sur 5 ans et les déposer **telles quelles** dans la couche bronze.

```bash
python -m scripts.extract                 # les 3 sources, en parallèle (sources indépendantes)
python -m scripts.extract --only p3       # une seule source
python -m scripts.extract --local         # vers data/ au lieu de Silo (tests)
```

| Module | Ce qu'il fait |
|---|---|
| `common/period.py` | Période d'étude commune : 5 ans glissants en mois complets |
| `p1_meteo/extract.py` | 1 requête Open-Meteo par ville (7 variables horaires, UTC), réponse JSON copiée telle quelle ; 2 requêtes simultanées max + respect de `Retry-After` (quota pondéré) |
| `p2_velos/extract.py` | Inventaire des dumps CityBikes (réutilise `p2_velos/backfill.list_available_dumps`), téléchargement parallèle (6 threads), **contrôle de la taille** de chaque fichier |
| `p3_pollution/ingestion.py` | Listing S3 de l'archive OpenAQ par station et par année, téléchargement parallèle (16 threads, pas de quota) |
| `scripts/extract.py` | Lance les 3 extractions en parallèle et affiche un bilan |

**Propriétés communes** : copie brute ; **idempotent et reprenable** (un fichier déjà présent n'est jamais
retéléchargé — l'extraction a été interrompue puis reprise sans perte) ; une source en échec n'arrête pas les autres ;
un **manifeste JSON** par exécution (source, période, fichiers, octets, erreurs, couverture).

### Résultat (20/11/2026)

| Source | Objets bronze | Volume | Détail |
|---|---|---|---|
| Météo | 12 (+ manifeste) | 26 Mo | 43 824 heures × 7 variables × 12 villes = **3,68 M valeurs**, 0 manquante |
| Vélos | 263 (+ manifeste) | 1,49 Go | 19 à 23 mois par ville (11/2024 → 09/2026) |
| Pollution | 69 421 (+ manifestes) | 37 Mo | 55 stations, 0 erreur ; médiane de couverture des jours par ville : 63 à 87 % |

Variables météo (unités d'origine) : `temperature_2m` (°C), `relative_humidity_2m` (%), `precipitation` (mm),
`pressure_msl` (hPa), `cloud_cover` (%), `wind_speed_10m` (**km/h**), `wind_direction_10m` (°).

### Points remarquables découverts pendant l'Extract

1. **Renumérotation EEA de 2024** : 14 stations de pollution ont changé d'identifiant OpenAQ (ancien arrêté
   fin janvier / début mars 2024, nouveau au même endroit). Règle retenue : ancienne station officielle à
   **< 150 m**, arrêtée entre mi-2023 et mi-2024, polluants communs. Les anciens identifiants sont listés dans
   `config/openaq_locations.json` (`predecessor_location_ids`) et extraits aussi, sous leur propre `locationid`.
   Effet : stations sous 50 % de couverture **33 → 7** (ex. Marseille St Louis 37,6 % → 84,4 %).
2. **Horodatage OpenAQ** : l'archive date chaque mesure par la **fin** de l'heure (l'API par le début) ;
   à réaligner au Transform pour la jointure avec la météo.
3. **Problème des petits fichiers** : la pollution pèse 37 Mo mais compte ~69 000 fichiers → c'est la plus lente
   à extraire (chaque fichier = 2 allers-retours réseau). Le Transform les regroupe en quelques Parquet.
4. **Sélection des stations de pollution** (`p3_pollution/explore.py`) : stations de référence (`isMonitor`) à
   ≤ 15 km du centre, actives, couvrant le plus de polluants (PM2.5, PM10, NO2, O3, CO) → 55 stations ;
   les capteurs low-cost (AirGradient, Clarity…) sont exclus. Le CO n'est mesuré que dans 6 villes
   (Paris, Lille, Lyon, Marseille, Nantes, Rouen).

---

## 4. Étape 2 — TRANSFORM ✅

**Objectif** : transformer le bronze brut en **silver** propre et homogène, au format **Parquet**, dans Silo.
Règles communes aux 3 sources : tout en **UTC** (datetime avec fuseau), une colonne `ville_id`, des unités
harmonisées, un **contrôle de plausibilité qui marque sans supprimer** (`quality_flag` = liste de motifs,
`is_valid` = aucun motif), un dédoublonnage, une **table de couverture** et un manifeste par exécution.
Écriture **idempotente** : relancer réécrit les mêmes clés.

**Pourquoi Parquet ?** Format **en colonnes** (on ne lit que les colonnes utiles), **typé** (dates, nombres,
booléens — pas de re-parsing), **compressé** (Snappy) et lisible par pandas, DuckDB, Spark ou ClickHouse.
Exemple : la météo passe de 26,3 Mo de JSON à **4,5 Mo** de Parquet (÷ 6).

### 4.1 Silver météo ✅ — `python -m p1_meteo.transform`

| | |
|---|---|
| Entrée | `bronze/meteo/ville=*/2021-11-01_2026-10-31.json` |
| Sortie | `silver/meteo/year=<YYYY>/meteo_heure.parquet` (6 fichiers) + `silver/_couverture/meteo.parquet` |
| Grain | 1 ligne par **ville × heure UTC** → **525 888 lignes** (12 villes × 43 824 h) |
| Colonnes | `ville_id`, `heure_utc`, `temperature_c`, `humidite_pct`, `precipitation_mm`, `pression_hpa`, `nebulosite_pct`, `vent_ms`, `vent_direction_deg`, `quality_flag`, `is_valid`, `source` |
| Conversions | vent **km/h → m/s** (÷ 3,6) ; heure demandée en UTC à la source |
| Bornes | température −40/+50 °C, humidité 0-100 %, pluie 0-200 mm/h, pression 900-1080 hPa, nébulosité 0-100 %, vent 0-75 m/s, direction 0-360° |
| Résultat | **100 % des lignes valides**, 0 doublon, couverture 100 % pour chaque ville × mois |

Contrôle de cohérence (moyennes sur 5 ans) : Marseille et Montpellier les plus chaudes (≈ 16,4 °C), Marseille
la plus sèche (≈ 626 mm/an) et la plus ventée (4,2 m/s, mistral), Lille/Rouen/Strasbourg les plus fraîches (≈ 12 °C).
*Limite* : les cumuls de pluie d'une réanalyse (ERA5) ont tendance à surestimer les petites pluies par rapport
aux pluviomètres ; on raisonne donc en **comparaisons relatives** (entre villes, jours secs / pluvieux) plutôt
qu'en valeurs absolues.

### 4.2 Silver pollution ✅ — `python -m p3_pollution.transform`

| | |
|---|---|
| Entrée | 69 421 CSV.gz `bronze/openaq_archive/…` (1 par station × jour) |
| Sortie | `silver/pollution/year=<YYYY>/pollution_heure.parquet` (6 fichiers, **11,7 Mo**), `silver/pollution/stations.parquet` (dimension), `silver/_couverture/pollution.parquet` |
| Grain | 1 ligne par **station × polluant × heure UTC** → **3 463 495 lignes** (3 419 782 valides, 98,7 %) |
| Colonnes | `ville_id`, `station_id` (identifiant actuel), `location_id_source` (identifiant d'origine), `sensor_id`, `polluant`, `heure_utc`, `valeur`, `unite` (µg/m³), `quality_flag`, `is_valid`, `source` |

Traitements, dans l'ordre :

1. **Regroupement des petits fichiers** : pour chaque station, téléchargement parallèle (16 threads) de tous ses
   CSV, concaténation puis **un seul parsing** → 69 421 fichiers lus en ~2 min 45, réécrits en 6 Parquet.
2. **5 polluants du sujet** conservés (PM2.5, PM10, NO2, O3, CO) ; NO et SO2 écartés.
3. **Horodatage** : l'archive date la mesure par la **fin** de l'heure en heure locale
   (`2023-01-01T01:00:00+01:00`) → conversion UTC puis **− 1 h** : `heure_utc` = **début** de l'heure, même
   convention que la météo. *Validation physique* : à Paris (2025, heure locale), le NO2 culmine à **8-9 h**
   (pointe du trafic) et l'O3 à **16-17 h** (formation photochimique) — un décalage aurait déplacé ces pics.
4. **Renumérotation EEA 2024** : **470 945 lignes** proviennent d'anciens identifiants, rattachées à la station
   actuelle (`station_id`) en conservant l'identifiant d'origine (`location_id_source`).
5. **Dédoublonnage** (station, polluant, heure) : 1 260 doublons, priorité à l'identifiant actuel quand l'ancien et
   le nouveau mesurent la même heure (cas de Nantes en 2025).
6. **Unités** : toutes en µg/m³ dans l'archive (le code convertit mg/m³, ppb, ppm et signale toute unité inconnue).
7. **Qualité** (marquage, sans suppression) : 43 703 **valeurs négatives** (1,3 % — dérive normale des analyseurs
   autour de zéro, surtout O3 la nuit et PM) et 10 valeurs **au-dessus d'un plafond physique**.

| Polluant | Lignes | Couverture moyenne (% d'heures valides par station × mois) |
|---|---|---|
| NO2 | 1 095 347 | 69 % |
| PM10 | 859 011 | 58 % |
| PM2.5 | 706 853 | 60 % |
| O3 | 706 005 | 65 % |
| CO | 96 279 | 71 % (**6 villes** seulement : Paris, Lille, Lyon, Marseille, Nantes, Rouen) |

Premiers ordres de grandeur (moyennes 5 ans, µg/m³) : **NO2** le plus élevé à **Paris (28)**, Lyon (26) et Marseille
(25), le plus bas à Nantes (11) ; **PM2.5** entre 7 (Clermont-Ferrand) et 11 (Lille) ; **O3** plus élevé dans
l'Ouest et le Sud (Rennes, Montpellier, Toulouse ≈ 58) que dans les grandes agglomérations (Lyon 47) — l'ozone est
« consommé » par les oxydes d'azote du trafic en ville. Ces constats seront approfondis à l'analyse.

### 4.3 Silver vélos ✅ — `python -m p2_velos.transform`

| | |
|---|---|
| Entrée | 263 Parquet `bronze/velos_historique/…` (1 par réseau × mois) — **186,6 M événements** |
| Sortie | `silver/velos/ville=<id>/year=<YYYY>/month=<MM>/velos_heure.parquet` (263 fichiers, **216 Mo**), `silver/velos/stations.parquet` (3 993 stations), `silver/_couverture/velos.parquet` |
| Grain | 1 ligne par **station × heure UTC** → **48 574 030 lignes** |
| Colonnes | `ville_id`, `network_id`, `station_id`, `heure_utc`, `velos_dispo`, `bornes_libres`, `capacite`, `taux_remplissage`, `activite`, `nb_evenements`, `etat_observe`, `est_vide`, `est_pleine`, `quality_flag`, `is_valid`, `source` |
| Durée | ≈ 20 min (Paris seule ≈ 8 min : ~4-5 M événements par mois) |

**Le défi** : CityBikes n'enregistre une ligne **que lorsqu'une station change d'état** (emprunt ou retour),
à des instants irréguliers. Il faut donc **reconstituer l'état heure par heure** :

```
événements bruts (station Robidou, Rennes)   →   silver (1 ligne / heure)
15:13  3 vélos                                    15:00  vélos=2  activité = |3→1| + |1→2| = 3   observé
15:16  1 vélo                                     16:00  vélos=1  activité = |2→1| = 1           observé
15:55  2 vélos                                    17:00  vélos=1  activité = 0  (état reporté)   non observé
16:13  1 vélo                                     … report limité à 24 h, au-delà : aucune ligne
```

1. **Un fichier à la fois** (réseau × mois) pour tenir en mémoire, dans l'ordre chronologique ; l'état de fin de mois
   est **reporté** au mois suivant (pas de rupture le 1er du mois, Δ calculé à cheval sur deux mois).
2. **Validité des événements** (règles reprises de `p2_velos/cleaning.py`) : champs présents, compteurs ≥ 0,
   capacité observée > 0, coordonnées non nulles. Un événement invalide ne peut pas reconstruire un état : il est
   écarté **mais compté** → 7 112 sur 186,6 M (0,004 %).
3. **Report d'état** sur les heures sans changement (`etat_observe = False`), **24 h au maximum** : au-delà, la
   station est considérée hors service / sans donnée et le trou n'est **pas** comblé.
4. **Indicateurs** : `activite` = nombre de mouvements dans l'heure (Σ |Δ vélos|, proxy des emprunts + retours),
   `taux_remplissage` = vélos / (vélos + bornes libres), `est_vide`, `est_pleine`.
5. **Marquages** : station hors de la zone de la ville (bbox de `ref_ville`) ; capacité observée > capacité
   déclarée (`extra.slots`) + 25 %.

Correction par rapport au code P2 d'origine : il supprimait les relevés invalides, ne reportait pas l'état sur les
heures calmes (une station sans mouvement disparaissait) et appelait « taux d'utilisation » le ratio
`bornes libres / capacité`, qui mesure en fait le **vide** de la station ; il n'y avait aucune mesure d'activité.

| Ville | Mois | Stations | % heures présentes | % observées | Pics (jours ouvrés, h locale) | Activité / station / h | Remplissage | % vide | % pleine |
|---|---|---|---|---|---|---|---|---|---|
| Paris | 19 | 1 575 | 90,9 | 75,2 | 8 h / 18 h | 5,2 | 0,40 | 3,2 | 2,3 |
| Lyon | 21 | 526 | 90,9 | 71,2 | 8 h / 18 h | 6,2 | 0,42 | 10,9 | 6,7 |
| Marseille | 23 | 256 | 94,1 | 77,6 | 8 h / 17 h | 5,3 | 0,24 | 14,7 | 2,7 |
| Toulouse | 21 | 474 | 92,0 | 72,6 | 8 h / 17 h | 5,1 | 0,38 | 12,7 | 5,1 |
| Lille | 21 | 301 | 76,4 | 39,6 | 8 h / 18 h | 2,5 | 0,41 | 6,8 | 2,2 |
| Clermont-Ferrand | 23 | 95 | 87,8 | 51,9 | 8 h / 17 h | 2,4 | 0,38 | 10,7 | 1,3 |
| Bordeaux | 23 | 239 | 91,0 | 55,1 | 8 h / 17 h | 2,4 | 0,43 | 4,9 | 1,7 |
| Nantes | 21 | 190 | 84,9 | 52,2 | 8 h / 17 h | 2,2 | 0,43 | 8,0 | 3,9 |
| Rouen | 23 | 148 | 89,3 | 45,7 | 8 h / 17 h | 1,8 | 0,43 | 9,1 | 6,3 |
| Rennes | 23 | 81 | 84,5 | 49,4 | 8 h / 18 h | 1,6 | 0,37 | 2,9 | 0,4 |
| Montpellier | 22 | 61 | 82,5 | 22,5 | 8 h / 17 h | 0,5 | 0,44 | 5,8 | 3,8 |
| Strasbourg | 23 | 47 | 74,8 | 17,4 | 8 h / 17 h | 0,5 | 0,66 | 1,2 | 11,1 |

**Validation** : les **12 villes** ont leur pic du matin à **8 h** et du soir à **17-18 h** (jours ouvrés) — la
signature des trajets domicile-travail, qui valide la reconstruction horaire.

*Limites et points d'attention* :
- L'`activite` compte aussi les **rééquilibrages par camion** (activité visible vers 2 h-5 h du matin).
- **Marseille** : 12,8 % des lignes marquées `capacite_incoherente` — 191 stations sur 212 affichent souvent plus de
  vélos + bornes que leur capacité déclarée (ex. 24-26 pour 20 places) : vélos acceptés en surplus ou capacité
  déclarée obsolète. Le marquage est conservé ; le `taux_remplissage`, calculé sur les valeurs observées, n'est pas
  faussé.
- **Strasbourg (Vélhop) et Montpellier (Vélomagg)** : très peu d'événements (17-22 % d'heures observées, activité
  ≈ 0,5) — petits réseaux, Vélhop étant surtout tourné vers la location longue durée : à interpréter avec prudence.
- **Paris** n'a que 19 mois (l'archive CityBikes de Vélib' démarre plus tard que les autres réseaux).
- Couverture « présente » < 100 % : une station installée en cours de mois compte pour le mois entier, et les heures
  au-delà de 24 h sans événement ne sont pas reconstituées.

### 4.4 Silver croisé P4 ✅ — `python -m p4_croise.transform`

| | |
|---|---|
| Entrées | les 3 silver (lignes `is_valid` uniquement) |
| Sortie | `silver/croise/year=<YYYY>/ville_heure.parquet` (6 fichiers, 11 Mo) |
| Grain | 1 ligne par **ville × heure UTC** sur toute la période → **525 888 lignes**, 36 colonnes |
| Durée | 28 s |

| Groupe | Colonnes | Calcul |
|---|---|---|
| Clés | `ville_id`, `heure_utc` | grille complète 12 villes × 43 824 h |
| Météo | `temperature_c`, `humidite_pct`, `precipitation_mm`, `pression_hpa`, `nebulosite_pct`, `vent_ms`, `vent_direction_deg` | silver météo |
| Pollution | `pm25`, `pm10`, `no2`, `o3`, `co` + `nb_stations_<polluant>` | **moyenne des stations** de la ville (chaque station pèse autant) |
| Vélos | `velo_stations`, `velos_dispo_total`, `activite_totale`, `activite_par_station`, `remplissage_moyen`, `part_stations_vides`, `part_stations_pleines`, `part_heures_observees` | agrégats du réseau de la ville |
| Présence | `a_meteo`, `a_pollution`, `a_velos` | la source a-t-elle une donnée valide cette heure-là ? |
| Calendrier | `heure_locale`, `jour_semaine` (0 = lundi), `est_weekend`, `date_locale`, `mois`, `saison` | en heure de **Paris** (les comportements suivent l'heure locale) |

Principe : on part d'une **grille complète** et chaque source y est jointe à gauche sur `(ville_id, heure_utc)` ;
une heure sans donnée reste **vide** pour cette source (rien n'est comblé).

| Présence des sources | Part des heures ville × heure |
|---|---|
| Météo | 100 % |
| Pollution (au moins un polluant) | 69,8 % |
| Vélos | 35,5 % (du 01/11/2024 au 30/09/2026) |
| **Les 3 à la fois** | **148 888 heures** (de 5 265 à Nantes à 15 729 à Strasbourg) |

*Point d'attention* : Nantes n'a que 5 265 heures communes aux 3 sources, ses stations de pollution récentes ayant
une couverture faible sur la période vélos ; c'est la ville la moins fiable pour les analyses croisées.

### 4.5 Analyses et conclusions

Les analyses lisent **directement le silver** (`common/silver.py`). Les calculs sont des **fonctions pures**
(`pX_*/analytics.py`, testées) réutilisées par les notebooks et, plus tard, par le dashboard. Chaque question
du sujet donne : une méthode, un calcul, un graphique et une conclusion chiffrée.

**Règles de visualisation** (`common/viz.py`, palette de référence validée) : une seule couleur pour un classement,
un dégradé bleu pour une magnitude, bleu ↔ rouge autour d'un gris neutre pour un écart à une référence, au plus
3 couleurs de séries par graphique (au-delà : petits multiples), un seul axe Y, traits fins et grille discrète.

#### P1 — Météo ✅ — [`notebooks/01_meteo.ipynb`](notebooks/01_meteo.ipynb), `p1_meteo/analytics.py`

| Question | Méthode | Conclusion |
|---|---|---|
| **Q1** Périodes anormales | Anomalie journalière vs **normale saisonnière** (moyenne des 5 ans par jour de l'année, lissée ± 7 j), z-score par ville × mois ; épisode = ≥ 3 jours, \|z\| ≥ 1,5 ; national = ≥ 6 villes | 349 épisodes ville (172 chauds / 177 froids), 43 jours \|z\| > 3. Les 3 plus longs épisodes **nationaux** sont des **vagues de chaleur en 2026** (23-30 mai, les 12 villes ; 21-27 juin ; 6-13 juillet). Froid le plus marqué : décembre 2022 (Rouen −7 °C sous la normale pendant 10 jours) |
| **Q2** Corrélations *(remplace « visibilité », absente)* | Pearson et Spearman sur 525 888 heures | Lien le plus fort **température ↔ humidité (ρ = −0,58)** (cycle diurne). Pluie ↔ nébulosité (+0,32), pluie ↔ pression basse (−0,29), pression basse ↔ vent (−0,28) : la signature des dépressions. Corrélations modérées → toutes les variables sont utiles pour P4 et le ML |
| **Q3** Saisonnalité | Climatologie ville × mois ; comparaison sur des **années d'étude complètes (nov. → oct.)**, les années civiles 2021 et 2026 étant incomplètes | Climat **océanique** à l'ouest (amplitude Rennes 14,5 °C) vs **continental** (Lyon 20,2 °C). Les 4 premières années sont proches (13,2-13,7 °C), **2025-26 est la plus chaude (14,8 °C, ≈ +1,3 °C) dans les 12 villes**, surtout au printemps et en été |
| **Q4** Jours extrêmes | Seuils **relatifs à chaque ville** : percentile 99 (T max, vent, pluie) ou 1 (T min) sur 5 ans | Records : **41,9 °C** Montpellier (08/07/2026), **−11 °C** Strasbourg (17/12/2022), **19,4 m/s** Lille (18/02/2022, tempête Eunice), **180 mm** Marseille (20/10/2026). Chaleur extrême uniquement de juin à septembre ; pluie extrême toute l'année avec pics en juin (orages) et en automne |

*Limites P1* : données modélisées (ERA5), qui lissent les extrêmes locaux ; pas de visibilité ; 5 ans permettent de
comparer des années, pas d'établir une tendance climatique.

#### P2 — Vélos ✅ — [`notebooks/02_velos.ipynb`](notebooks/02_velos.ipynb), `p2_velos/analytics.py`

**Méthode « map → reduce »** : 48,6 M lignes ne tiennent pas en mémoire ; chaque fichier silver (ville × mois) est
réduit à des sommes et comptages additifs, puis les 263 agrégats sont additionnés (≈ 1 min).

**Contrôle des stations avant analyse** (règles explicites, stations listées et non supprimées en silence) :
7 **techniques** (nom vide, bornes « TEST », atelier de maintenance — dont une « station » sans nom à Lyon à ~8 000
mouvements/jour, un point technique), 15 **inactives** (< 1 mouvement/jour, vides 99,8 % du temps : hors service,
14 à Lille), règle **aberrante** (> 50 mouvements/jour/place pour une capacité crédible ≥ 5 places).
→ **3 843 stations exploitables**. Heures de pointe : jours ouvrés, 7 h-10 h et 17 h-20 h (heure locale).

| Question | Méthode | Conclusion |
|---|---|---|
| **Q1** Stations les plus utilisées | Mouvements / jour ; concentration (part des 10 % de stations les plus actives) | **Gares et places centrales** : Augagneur / Fosse aux Ours (544/j), Part-Dieu, Opéra (Lyon), gare Matabiau (Toulouse), gares de Lyon et Saint-Lazare (Paris). Usage le plus intense à **Lyon (144 mouvements/station/jour)**, puis Paris (122), Toulouse (116), Marseille (110) ; Strasbourg (11) et Montpellier (13) très en retrait. Le top 10 % des stations porte 20 à 33 % des mouvements |
| **Q2** Zones en sous-offre | Zones de **~500 m** ; sous-offre = stations vides ≥ 15 % du temps | **255 zones sur 2 181 (≈ 12 %)**, dont 47 sévères (≥ 25 %). **Marseille** : 49 % de ses zones ; Lyon 24 %, Clermont 23 %, Toulouse 19 %, Rouen 17 % ; Paris 2 %. La sous-offre touche les zones **proches du centre** (médiane 2,2-2,6 km vs 3,9-4,5 km), là où la demande est forte ; à Lyon, un quart sur les pentes de la Croix-Rousse |
| **Q3** Pics journaliers | Activité par heure locale × jour, profil **normalisé par ville** (Paris n'écrase pas les autres) | Pic à **8 h** et pic plus fort à **17-18 h** dans les **12 villes** (domicile-travail) ; 18 h le week-end. Pic = 10 à 23 × le creux de la nuit. Usage pendulaire marqué à Clermont-Ferrand (semaine × 1,75 vs week-end) et Nantes (× 1,6) ; usage loisir à Montpellier et Strasbourg (week-end ≥ semaine) |
| **Q4** Déséquilibres géographiques | Remplissage pointe du matin vs du soir, par distance au centre | **Le centre se remplit dans la journée** (< 1 km : 0,35 → 0,41), **la couronne 4-8 km se vide** (0,39 → 0,32) : flux pendulaire. Au-delà de 8 km, stations les plus pleines (0,45) donc sous-utilisées. → Rééquilibrage du centre vers la couronne en fin de journée |
| **Q5** Stations critiques | Score = % des heures de pointe **vide** + % **pleine** ; critique si ≥ 30 % | **Marseille (16,5 % de stations critiques)** et **Lyon (≈ 11 %)** en tête ; **Paris < 1 %** (régulation efficace). Station la plus critique : **Damrémont - Caulaincourt (Montmartre), vide 88 % des heures de pointe** — effet relief. Surtout des stations **à réapprovisionner** ; quelques-unes **à désengorger** (Jean Moulin - Saint-Étienne, Marseille, pleine 60 % du temps) |

*Limites P2* : l'activité inclut les **rééquilibrages par camion** ; 19 à 23 mois de données (archive depuis
11/2024) ; capacités déclarées parfois incohérentes (Marseille).

#### P3 — Pollution ✅ — [`notebooks/03_pollution.ipynb`](notebooks/03_pollution.ipynb), `p3_pollution/analytics.py`

**Référence** : valeurs guides **OMS 2021** — annuel : PM2.5 5, PM10 15, NO2 10 µg/m³ ; 24 h : PM2.5 15, PM10 45,
NO2 25, CO 4 000 ; O3 : max journalier de la moyenne 8 h, 100. Un jour n'est retenu que s'il a ≥ 18 h de mesure.

| Question | Méthode | Conclusion |
|---|---|---|
| **Q1** Zones les plus polluées | Moyenne et P95 par station (≥ 2 000 h), moyenne de ville = moyenne des stations ; type trafic / fond **déduit du nom** | NO2 : **Paris 27,4, Montpellier 27,2, Lyon 25,2, Marseille 24,0** ; Clermont-Ferrand 12,2. Stations les plus polluées **au bord des grands axes** : périphérique Est de Paris (43,9), périphérique de Lyon (42,9), A35, A7. Stations « trafic » ≈ **2 × plus de NO2** que le fond urbain (35 vs 17) : pollution très locale. PM2.5 : écarts faibles (7 à 10,5), pollution de **fond** |
| **Q2** Variation journalière | Moyenne par heure locale, semaine / week-end ; par mois | **NO2 et CO** : double pic **8-9 h / 19-21 h**, NO2 **−14 % le week-end** (trafic). **O3** : profil inverse, max **15-16 h** (formation photochimique). **PM2.5** : soir et nuit (chauffage). **PM10** : pic vers 10 h les jours ouvrés seulement. Hiver : NO2, CO, PM2.5 maximaux ; été : O3 maximal |
| **Q3** Polluants dominants (OMS) | Moyenne / valeur guide annuelle ; % de jours > valeur guide 24 h (au moins une station) | **Les 12 villes dépassent l'OMS pour les PM2.5 (× 1,4 à 2,1) et le NO2 (× 1,2 à 2,7)** ; PM10 proche de la valeur guide. Dominant : **NO2 dans les grandes agglomérations** (Paris, Montpellier, Lyon, Marseille, Strasbourg), **PM2.5 ailleurs**. NO2 > 25 µg/m³ à au moins une station **91 % des jours à Lyon, 88 % à Paris**. Moyennes de ville sous les valeurs limites UE (NO2 40), mais **les périphériques de Paris et Lyon les dépassent** |
| **Q4** Épisodes | Jours où ≥ 4 villes dépassent la valeur guide 24 h ; pics locaux > moyenne 30 j + 3 σ | **PM2.5 et NO2 : épisodes d'hiver** (9-16 janvier 2024 : les 12 villes ; février 2023 : jusqu'à 55 µg/m³). **O3 : épisodes d'été qui coïncident avec les vagues de chaleur de P1** (7-26 juillet 2026 et 27-31 mai 2026 : les 12 villes). Pics locaux de PM10 (Nantes 75 µg/m³ le 06/09/2023) de cause non identifiable avec nos données |

*Limites P3* : trous de l'archive en 2021-2023 (on ne compare pas le nombre d'épisodes d'une année à l'autre) ;
CO dans 6 villes ; type de station déduit du nom ; 3 à 5 stations par ville.

#### P4 — Analyse croisée ✅ — [`notebooks/04_croise.ipynb`](notebooks/04_croise.ipynb), `p4_croise/analytics.py`

**Méthode clé — l'indice relatif** : comparer directement « pollution et vent » ou « vélo et température »
mélange les cycles (la nuit : moins de vent, plus de NO2, moins de vélos ; l'hiver : plus de particules). Chaque
mesure est rapportée à la **normale de la même ville, du même mois (saison pour le vélo, faute de recul) et de la
même heure locale** : indice 1 = normal. Les écarts d'indice mesurent l'effet météo **à heure et saison égales**.

| Question | Méthode | Conclusion |
|---|---|---|
| **Q1** Météo ↔ pollution (5 ans) | Indices de pollution par classe de vent, de pluie, de température ; hiver : pression × vent | **Le vent disperse** : NO2 ÷ 3 (indice 1,34 → 0,45) et PM2.5 ÷ 2 entre vent calme et vent fort. **La pluie lave** : particules −36 % un jour très pluvieux. **L'anticyclone hivernal piège** : PM2.5 × 2,2 entre « haute pression + calme » et « dépression ventée ». **La chaleur fabrique l'ozone** : 113 µg/m³ au-dessus de 30 °C (> valeur guide OMS), +27 % même à saison égale — d'où la coïncidence des épisodes d'ozone (P3) et des vagues de chaleur (P1) |
| **Q2** Météo ↔ vélo | Indice d'activité par classe de pluie, température, vent ; jours secs / pluvieux ; par ville | **La pluie est le 1er facteur** : −12 % (bruine), −29 % (modérée), −36 % (forte) ; une journée très pluvieuse : −14 % en semaine, −18 % le week-end. **Température** : optimum 20-30 °C (+7-8 %), −19 % par gel. **Vent** : effet au-delà de 5 m/s, −18 % au-delà de 8 m/s. Usages « loisir » plus sensibles à la pluie (Montpellier −48 %, Strasbourg −39 %) que les usages « habitude » (Clermont −21 %, Lille −22 %) |
| **Q3** Conditions favorables | Score 0-3 (+1 : 10-25 °C ; +1 : pas de pluie ; +1 : vent < 5 m/s), **validé** par l'activité observée | Le score est **validé** : activité 0,81 → 0,87 → 0,98 → 1,07 (**+32 %** entre score 0 et 3). Heures de journée pleinement favorables sur 5 ans : **Bordeaux 45,5 %**, Montpellier 42,7 % … Lille 34,1 %. Paradoxe : les villes les plus favorables par le climat ne sont pas celles où l'on roule le plus (Montpellier vs Lyon) → **l'infrastructure compte plus que le climat** |
| **Q4** Interactions fortes | Jours combinant pollution élevée + météo défavorable + usage atypique ; modèle gradient boosting évalué sur la **période la plus récente** + importance par permutation | Coïncidences rares (187 jours ville sur 7 811) et **calendaires** (21 juin, 26-29 décembre) ou **caniculaires**. **La pollution ne fait pas fuir les cyclistes** (usage atypique 32 % des jours pollués vs 29 %). Le modèle explique **68 %** de l'activité horaire, presque entièrement par **le réseau et l'heure** ; la météo seule ne **prédit** pas l'écart à la normale (R² ≈ 0) : effets réels en moyenne, mais variations quotidiennes dominées par des facteurs absents des données (vacances, fériés, événements, grèves) |

*Limites P4* : période commune aux 3 sources limitée (nov. 2024 → sept. 2026) ; corrélations, pas causalités ;
pas de calendrier (vacances, jours fériés, événements) ; Nantes peu couverte en pollution.

---

## 5. Étape 3 — LOAD / GOLD (ClickHouse) ✅ — `python -m p4_croise.build_gold`

**Objectif** : charger dans **ClickHouse** des tables **agrégées, prêtes à l'emploi** pour le dashboard et le ML.
Schéma : [`sql/40_gold.sql`](sql/40_gold.sql) ; chargement : [`p4_croise/build_gold.py`](p4_croise/build_gold.py).

**Pourquoi ClickHouse ?** Base **en colonnes** (comme Parquet) : une requête « NO2 moyen par ville » ne lit que
les colonnes `ville_id` et `no2`. Elle est conçue pour **agréger des millions de lignes en quelques
millisecondes** — l'usage exact d'un dashboard. Notions utilisées :
- `ENGINE = MergeTree` : moteur standard, stockage en colonnes compressées ;
- `ORDER BY (ville_id, heure_utc)` : **clé de tri** physique → filtrer par ville puis par période est très rapide ;
- `PARTITION BY toYear(...)` : découpage physique des tables temporelles par année ;
- `LowCardinality(String)` : dictionnaire pour les colonnes à peu de valeurs (ville, polluant…) ;
- `Nullable(Float64)` : une donnée absente reste **NULL** (jamais remplacée par 0).

**Cohérence** : le gold est calculé avec **les mêmes fonctions d'analyse testées** que les notebooks
(`pX_*/analytics.py`) → mêmes chiffres partout.

**Chargement idempotent et sans coupure** : chaque table est remplie dans une copie `<nom>__new`, puis échangée
d'un coup avec l'ancienne (`EXCHANGE TABLES`, atomique) ; le dashboard ne voit jamais de table vide ou à moitié
remplie. Vérifié : deux chargements successifs → volumes identiques, **0 doublon**.

| Table gold | Lignes | Contenu | Usage |
|---|---|---|---|
| `gold_ville_heure` | 525 888 | croisement ville × heure (météo, 5 polluants, vélos, calendrier) + `score_favorabilite` | séries temporelles, ML |
| `gold_ville_jour` | 21 924 | synthèse ville × jour : météo, moyennes journalières de pollution, **dépassements OMS** (PM2.5, PM10, NO2, O3 8 h), vélos et indice d'activité, heures favorables | tableaux de bord, alertes |
| `gold_station_velo` | 3 865 | par station : statut, activité/jour, % vide / pleine, criticité, **action** (réapprovisionner / désengorger), rang | P2, carte |
| `gold_zone_velo` | 2 181 | zones de ~500 m : % du temps vide, sous-offre | P2 Q2, carte |
| `gold_velo_profil` | 2 016 | activité par jour de semaine × heure, par ville | P2 Q3 |
| `gold_station_pollution` | 160 | par station × polluant : moyenne, P95, type, ratio OMS | P3, carte |
| `gold_pollution_profil` | 2 592 | profil horaire par ville × polluant × semaine / week-end | P3 Q2 |
| `gold_kpi_ville` | 12 | une ligne par ville : climat, pollution et OMS, vélos (activité, sous-offre, criticité, effet pluie), heures favorables | page d'accueil |
| `gold_couverture` | 3 906 | % d'heures présentes / valides par source × ville × mois (× polluant) | page Qualité |
| `gold_pipeline` | 7 | objets, taille, lignes et dernier traitement de chaque couche × source | page Qualité |

Volume total ≈ 23 Mo dans ClickHouse. Durée du chargement ≈ 90 s (dont ≈ 70 s de lecture du silver, dominée par les
48,6 M lignes vélos). Requêtes de contrôle (12 villes, 526 000 lignes horaires) : **60 à 90 ms**.

Exemple de requête :

```sql
SELECT ville_id, round(no2_moy, 1) AS no2, round(ratio_oms_no2, 2) AS x_oms, polluant_dominant,
       round(velo_activite_station_jour) AS velo, effet_pluie_velo_pct
FROM urbanhub.gold_kpi_ville
ORDER BY no2 DESC;
```

---

## 6. Dashboard, conclusions et ML

### 6.1 Dashboard Streamlit ✅ — `streamlit run dashboard/Accueil.py` → http://localhost:8501

Le dashboard lit **uniquement la couche gold (ClickHouse)** — jamais le bronze ni le silver — comme dans une
architecture de production où la restitution est découplée des traitements. Les requêtes sont **mises en cache**
15 min (`st.cache_data`) : une même vue ne réinterroge pas la base à chaque clic. Graphiques **Plotly**
interactifs (survol, zoom), même palette que les notebooks.

**Filtres globaux** (barre latérale) : **ville** et **période**, conservés d'une page à l'autre
(`st.session_state`). **Pages d'analyse organisées par question métier** (onglets Q1, Q2…), chaque graphique étant
suivi de sa **conclusion chiffrée** (reprise des notebooks) et, si besoin, d'une note de méthode.

| Page | Contenu | Tables gold |
|---|---|---|
| **Accueil** | chiffres globaux, **carte des 12 villes** (indicateur au choix), tableau des indicateurs clés, ce qu'il faut retenir | `gold_kpi_ville`, `gold_ville_heure`, `gold_station_pollution` |
| **1 · Ma ville** | **fiche de synthèse** : climat, qualité de l'air (vs OMS), vélos — avec le **rang parmi les 12 villes** ; évolution sur la période (température, PM2.5, indice vélo) ; jours les plus pollués ; stations à rééquilibrer | `gold_kpi_ville`, `gold_ville_jour`, `gold_station_velo` |
| **2 · Météo** | Q1 anomalies journalières (rouge / bleu) · Q2 corrélations · Q3 saisonnalité et climat comparé · Q4 records et jours extrêmes | `gold_ville_jour`, `gold_ville_heure`, `gold_kpi_ville` |
| **3 · Vélos** | Q1 stations les plus utilisées (+ carte) · Q2 zones en sous-offre · Q3 heatmap jour × heure · Q4 remplissage matin / soir selon la distance au centre · Q5 carte des **actions de rééquilibrage** + liste prioritaire | `gold_station_velo`, `gold_zone_velo`, `gold_velo_profil`, `gold_kpi_ville` |
| **4 · Pollution** | Q1 stations et villes les plus polluées · Q2 profils horaires · Q3 ratio aux valeurs guides OMS et % de jours de dépassement · Q4 calendrier des épisodes | `gold_station_pollution`, `gold_pollution_profil`, `gold_ville_jour`, `gold_kpi_ville` |
| **5 · Croisé** | Q1 météo → pollution · Q2 météo → vélo · Q3 score de favorabilité · Q4 coïncidences pollution × météo × vélo | `gold_ville_heure`, `gold_ville_jour` (+ `p4_croise/analytics.py`) |
| **6 · Qualité & pipeline** | volumes bronze / silver / gold, compression, **couverture par source × ville × mois**, tables gold, principes qualité | `gold_pipeline`, `gold_couverture`, `system.tables` |

Deux tables gold ont été ajoutées pour la page Qualité (le dashboard ne lit que le gold) : `gold_couverture`
(3 906 lignes : % d'heures présentes / valides par source × ville × mois, et par polluant) et `gold_pipeline`
(objets, taille, lignes, lignes valides et date du dernier manifeste pour chaque couche × source).

Module commun `dashboard/lib.py` : connexion ClickHouse, cache, thème Plotly, filtres globaux, encadrés de conclusion.
Contrôle : les 7 pages s'exécutent sans erreur en mode test (`streamlit.testing`), y compris les changements de
ville, de source, de polluant et une période sans données vélo (hiver 2022).

---

## 7. Lancer le projet en local

```bash
python -m venv .venv && .venv\Scripts\activate && pip install -r requirements.txt
copy .env.example .env                     # puis renseigner OPENAQ_API_KEY (utile pour explore.py)
docker compose up -d minio clickhouse      # Silo + ClickHouse uniquement
python -m scripts.extract                  # étape 1 — Extract : 3 archives → bronze (Silo)
python -m p1_meteo.transform               # étape 2 — Transform : bronze → silver Parquet (Silo)
python -m p3_pollution.transform
python -m p2_velos.transform               #   (≈ 20 min : 186 M événements)
python -m p4_croise.transform              #   croisement ville × heure
python -m p4_croise.build_gold             # étape 3 — Load : silver → gold (ClickHouse)
streamlit run dashboard/Accueil.py         # dashboard → http://localhost:8501
pytest                                     # tests
```

Réglages locaux (non versionnés) : `docker-compose.override.yml` déplace les ports car 9000/9001/8123 sont pris
par un autre projet, et ne monte pas les scripts SQL silver de P2 (le silver vit en Parquet dans Silo).

| Service | Adresse locale | Usage |
|---|---|---|
| Silo — API S3 | `localhost:19000` | le code (`MINIO_ENDPOINT`) |
| Silo — console | http://localhost:19001 | navigateur (identifiants du `.env`) |
| ClickHouse — HTTP | `localhost:18123` | le code (`CLICKHOUSE_PORT`) |

---

## Annexe — Modules hérités de la version de groupe

La version de groupe (dépôt partagé) prévoyait une collecte **temps réel** et un silver dans ClickHouse.
Ces modules restent dans le dépôt mais ne sont pas utilisés par le pipeline d'archives ci-dessus :
`p1_meteo/ingestion.py`, `p2_velos/ingestion.py` (CityBikes temps réel), `p2_velos/backfill.py`
(téléchargement + nettoyage + chargement ClickHouse en une passe), `p2_velos/flow.py`, `sql/20_p2_velos.sql`.
La logique de nettoyage de `p2_velos/cleaning.py` et `p2_velos/quality.py` est réutilisée au Transform.
Documentation détaillée de P2 : [`p2_velos/README.md`](p2_velos/README.md).
