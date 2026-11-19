-- 00_commun : tables ref_ville, pipeline_alertes
-- Exécuté en premier (ordre alphabétique). Responsable : commun.

CREATE TABLE IF NOT EXISTS ref_ville
(
    ville_id String,
    nom      String,
    lat      Float64,
    lon      Float64
)
ENGINE = MergeTree
ORDER BY ville_id;

CREATE TABLE IF NOT EXISTS pipeline_alertes
(
    ts      DateTime64(3),
    level   String,
    source  String,
    message String
)
ENGINE = MergeTree
ORDER BY ts;
