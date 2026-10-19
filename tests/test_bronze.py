import json

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from gazette.bronze import assets as bronze
from gazette.bronze.sources import Flux, Source, charger_sources

RSS = b"""<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0"><channel><title>Test</title>
<item><title>Premier</title><link>https://exemple.fr/1</link></item>
<item><title>Second</title><link>https://exemple.fr/2</link></item>
</channel></rss>"""


def reponse_rss(url: str) -> requests.Response:
    reponse = requests.Response()
    reponse.status_code = 200
    reponse._content = RSS
    reponse.url = url
    reponse.headers["Content-Type"] = "application/rss+xml"
    return reponse


def test_sources_csv_valide():
    sources = charger_sources()
    assert len(sources) == 8
    assert {s.type for s in sources} == {"PQN", "PQR"}
    assert all(f.url.startswith("https://") for s in sources for f in s.flux)


def test_bronze_ecrit_les_flux_et_isole_les_echecs(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "DATA_DIR", tmp_path)

    def faux_get(self, url, timeout):
        if "panne" in url:
            raise requests.ConnectionError("injoignable")
        return reponse_rss(url)

    monkeypatch.setattr(requests.Session, "get", faux_get)
    source = Source(
        slug="test",
        nom="Journal test",
        type="PQN",
        region="National",
        flux=(Flux("une", "https://exemple.fr/une.xml"), Flux("panne", "https://exemple.fr/panne.xml")),
    )

    resultat = dg.materialize([bronze.construire_asset_bronze(source)], partition_key="2026-10-19")

    assert resultat.success
    materialisation = resultat.asset_materializations_for_node("bronze__test")[0]
    assert materialisation.metadata["nb_articles"].value == 2
    assert materialisation.metadata["nb_flux_en_echec"].value == 1
    [check] = resultat.get_asset_check_evaluations()
    assert check.passed

    dossier = tmp_path / "bronze" / "date=2026-10-19" / "test"
    [fichier] = dossier.glob("*.parquet")
    assert fichier.name.startswith("une_")
    table = pq.read_table(fichier)
    assert table.column("title").to_pylist() == ["Premier", "Second"]
    assert table.column("link").to_pylist() == ["https://exemple.fr/1", "https://exemple.fr/2"]
    assert set(table.column("source").to_pylist()) == {"test"}
    assert table.schema.metadata[b"gazette.xml_brut"] == RSS
    assert json.loads(table.schema.metadata[b"gazette.http"])["statut_http"] == 200

    articles = json.loads(fichier.with_suffix(".json").read_text(encoding="utf-8"))
    assert [a["title"] for a in articles] == ["Premier", "Second"]
    assert articles[0]["link"] == "https://exemple.fr/1"


def test_bronze_ne_reecrit_jamais_un_fichier(tmp_path):
    fichier = tmp_path / "a.parquet"
    bronze.ecrire_immuable(fichier, bronze.en_parquet(pa.table({"x": [1]})))
    with pytest.raises(FileExistsError):
        bronze.ecrire_immuable(fichier, bronze.en_parquet(pa.table({"x": [2]})))
    assert pq.read_table(fichier).column("x").to_pylist() == [1]
