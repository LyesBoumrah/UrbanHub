"""Tests de l'extraction P3 depuis l'archive OpenAQ (p3_pollution/ingestion.py). Responsable : P3."""
from datetime import date

from common.storage import LocalBronzeStorage
from p3_pollution.ingestion import (BRONZE_PREFIX, archive_ids, coverage_pct, default_period, extract,
                                    parse_archive_key, parse_list_response, select_keys, to_bronze_key,
                                    unique_days)

KEY = "records/csv.gz/locationid=2696/year=2023/month=01/location-2696-20230101.csv.gz"


def test_default_period_cinq_ans_glissants_en_mois_complets():
    assert default_period(5, today=date(2026, 11, 20)) == (date(2021, 11, 1), date(2026, 10, 31))
    assert default_period(5, today=date(2026, 3, 1)) == (date(2021, 3, 1), date(2026, 2, 28))


def test_archive_ids_et_jours_uniques():
    loc = {"location_id": 2162724, "predecessor_location_ids": [3985]}
    assert archive_ids(loc) == [2162724, 3985]
    assert archive_ids({"location_id": 1}) == [1]
    keys = [KEY, KEY.replace("2696", "3985"), KEY.replace("20230101", "20230102")]
    assert unique_days(keys) == 2  # même jour sur deux identifiants = compté une fois


def test_parse_archive_key():
    assert parse_archive_key(KEY) == (2696, date(2023, 1, 1))
    assert parse_archive_key("records/csv.gz/locationid=2696/year=2023/") is None


def test_to_bronze_key_conserve_l_arborescence():
    assert to_bronze_key(KEY) == (f"{BRONZE_PREFIX}/locationid=2696/year=2023/month=01/"
                                  "location-2696-20230101.csv.gz")


def test_select_keys_bornes_incluses():
    keys = [KEY.replace("20230101", d) for d in ("20201231", "20210101", "20251231", "20260101")]
    kept = [parse_archive_key(k)[1] for k in select_keys(keys, date(2021, 1, 1), date(2025, 12, 31))]
    assert kept == [date(2021, 1, 1), date(2025, 12, 31)]


def test_coverage_pct():
    assert coverage_pct(365, date(2023, 1, 1), date(2023, 12, 31)) == 100.0
    assert coverage_pct(289, date(2023, 1, 1), date(2023, 12, 31)) == 79.2


def test_parse_list_response_avec_pagination():
    xml = ('<?xml version="1.0" encoding="UTF-8"?>'
           '<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
           '<IsTruncated>true</IsTruncated><NextContinuationToken>abc</NextContinuationToken>'
           f'<Contents><Key>{KEY}</Key><Size>720</Size></Contents></ListBucketResult>')
    assert parse_list_response(xml) == ([KEY], "abc")
    assert parse_list_response(xml.replace("<IsTruncated>true", "<IsTruncated>false"))[1] is None


class FakeArchive:
    """Archive simulée : 3 jours disponibles pour la station 2696, aucun réseau."""

    def __init__(self):
        self.downloads = 0
        self.keys = [KEY.replace("20230101", d) for d in ("20230101", "20230102", "20230103")]

    def list_keys(self, prefix):
        return [k for k in self.keys if k.startswith(prefix)]

    def download(self, key):
        self.downloads += 1
        return b"contenu-brut-" + key.encode()


LOC = [{"location_id": 2696, "ville_id": "lyon", "name": "LYON Centre", "sensors": []}]


def test_extract_copie_brute_puis_reprise_sans_retelechargement(tmp_path):
    storage, archive = LocalBronzeStorage(tmp_path), FakeArchive()
    start, end = date(2023, 1, 1), date(2023, 12, 31)

    first = extract(archive, storage, LOC, start, end, workers=2)
    assert first["files_downloaded"] == 3 and first["errors"] == []
    bronze = to_bronze_key(archive.keys[0])
    assert storage.get_bytes(bronze) == b"contenu-brut-" + archive.keys[0].encode()  # copie octet pour octet

    second = extract(archive, storage, LOC, start, end, workers=2)  # relance : rien à retélécharger
    assert second["files_downloaded"] == 0 and second["files_skipped"] == 3
    assert archive.downloads == 3
    assert len(storage.list(f"{BRONZE_PREFIX}/_manifests/")) == 2  # un manifeste par exécution


def test_extract_telecharge_aussi_les_predecesseurs_sous_leur_propre_id(tmp_path):
    storage, archive = LocalBronzeStorage(tmp_path), FakeArchive()
    old_key = KEY.replace("2696", "3985").replace("20230101", "20230104")
    archive.keys.append(old_key)
    loc = [{**LOC[0], "predecessor_location_ids": [3985]}]

    stats = extract(archive, storage, loc, date(2023, 1, 1), date(2023, 12, 31), workers=2)
    assert stats["files_downloaded"] == 4
    assert storage.exists(f"{BRONZE_PREFIX}/locationid=3985/year=2023/month=01/location-3985-20230104.csv.gz")
    assert stats["locations"][2696]["archive_ids"] == [2696, 3985]
    assert stats["locations"][2696]["coverage_pct"] == coverage_pct(4, date(2023, 1, 1), date(2023, 12, 31))
