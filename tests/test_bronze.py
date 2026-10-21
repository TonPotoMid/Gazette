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


def rss_de(*liens: str) -> bytes:
    items = "".join(f"<item><title>Article {u}</title><link>{u}</link></item>" for u in liens)
    return (
        '<?xml version="1.0" encoding="UTF-8"?>'
        f'<rss version="2.0"><channel><title>Test</title>{items}</channel></rss>'
    ).encode()


def reponse_rss(url: str, contenu: bytes = RSS) -> requests.Response:
    reponse = requests.Response()
    reponse.status_code = 200
    reponse._content = contenu
    reponse.url = url
    reponse.headers["Content-Type"] = "application/rss+xml"
    return reponse


def erreur_http(statut: int) -> requests.HTTPError:
    reponse = requests.Response()
    reponse.status_code = statut
    return requests.HTTPError(response=reponse)


def test_erreurs_http_404_permanent_503_reessayable():
    assert not bronze.erreur_reessayable(erreur_http(404))
    assert bronze.erreur_reessayable(erreur_http(503))
    assert bronze.erreur_reessayable(requests.ConnectionError("injoignable"))

    with bronze.creer_session() as session:
        retry = session.get_adapter("https://").max_retries
        assert retry.status_forcelist == (503,)
        assert retry.status == bronze.HTTP_RETRIES


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
    assert materialisation.metadata["nb_recus"].value == 2
    assert materialisation.metadata["nb_ecrits"].value == 2
    assert materialisation.metadata["nb_ignores"].value == 0
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


def collecter(source: Source, partition: str) -> dg.ExecuteInProcessResult:
    return dg.materialize([bronze.construire_asset_bronze(source)], partition_key=partition)


def compteurs(resultat: dg.ExecuteInProcessResult, slug: str) -> dict[str, int]:
    metadata = resultat.asset_materializations_for_node(f"bronze__{slug}")[0].metadata
    return {nom: metadata[nom].value for nom in ("nb_recus", "nb_ecrits", "nb_ignores")}


def articles_ecrits(racine) -> list[str]:
    """Les liens de toutes les lignes bronze, tous fichiers et tous jours confondus."""
    return [
        lien
        for fichier in sorted(racine.glob("bronze/date=*/*/*.parquet"))
        for lien in pq.read_table(fichier, columns=["link"]).column("link").to_pylist()
    ]


def test_deux_passages_zero_doublon(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "DATA_DIR", tmp_path)
    flux_distant = {"contenu": rss_de("https://exemple.fr/1", "https://exemple.fr/2")}
    monkeypatch.setattr(
        requests.Session,
        "get",
        lambda self, url, timeout: reponse_rss(url, flux_distant["contenu"]),
    )
    source = Source(
        "test", "Journal test", "PQN", "National", (Flux("une", "https://exemple.fr/u"),)
    )

    premier = collecter(source, "2026-10-19")
    assert compteurs(premier, "test") == {"nb_recus": 2, "nb_ecrits": 2, "nb_ignores": 0}
    assert len(articles_ecrits(tmp_path)) == 2

    # Même partition rejouée : rien de nouveau, donc ni fichier ni article de plus.
    fichiers = sorted(tmp_path.glob("bronze/**/*"))
    second = collecter(source, "2026-10-19")
    assert compteurs(second, "test") == {"nb_recus": 2, "nb_ecrits": 0, "nb_ignores": 2}
    assert sorted(tmp_path.glob("bronze/**/*")) == fichiers
    assert len(articles_ecrits(tmp_path)) == 2
    [check] = second.get_asset_check_evaluations()
    assert check.passed  # la source répond : 0 écrit n'est pas une panne

    # Le lendemain, le flux garde les anciens articles et en ajoute un : seul celui-là est écrit.
    flux_distant["contenu"] = rss_de(
        "https://exemple.fr/3", "https://exemple.fr/1", "https://exemple.fr/2"
    )
    troisieme = collecter(source, "2026-10-20")
    assert compteurs(troisieme, "test") == {"nb_recus": 3, "nb_ecrits": 1, "nb_ignores": 2}
    ecrits = articles_ecrits(tmp_path)
    assert sorted(ecrits) == [f"https://exemple.fr/{n}" for n in (1, 2, 3)]
    nouveau = pq.read_table(next((tmp_path / "bronze" / "date=2026-10-20").glob("*/*.parquet")))
    assert nouveau.column("rang").to_pylist() == [0]  # rang d'origine dans le flux


def test_un_article_present_dans_deux_flux_n_est_ecrit_qu_une_fois(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "DATA_DIR", tmp_path)
    monkeypatch.setattr(requests.Session, "get", lambda self, url, timeout: reponse_rss(url))
    source = Source(
        "test",
        "Journal test",
        "PQN",
        "National",
        (Flux("une", "https://exemple.fr/une.xml"), Flux("tout", "https://exemple.fr/tout.xml")),
    )

    resultat = collecter(source, "2026-10-19")

    assert compteurs(resultat, "test") == {"nb_recus": 4, "nb_ecrits": 2, "nb_ignores": 2}
    assert len(articles_ecrits(tmp_path)) == 2


def test_cle_de_dedoublonnage_repli_sur_l_id_puis_jamais_ignore():
    connues: set[str] = set()
    entrees = [
        {"link": "https://exemple.fr/1", "id": "a"},
        {"link": "https://exemple.fr/1", "id": "autre"},  # même lien
        {"id": "b"},  # sans lien : l'id sert de clé
        {"id": "b"},
        {},  # ni lien ni id : impossible à reconnaître, toujours gardé
        {},
    ]

    gardes = bronze.articles_nouveaux(entrees, connues)

    assert [rang for rang, _ in gardes] == [0, 2, 4, 5]
    assert connues == {"https://exemple.fr/1", "b"}


def test_bronze_ne_reecrit_jamais_un_fichier(tmp_path):
    fichier = tmp_path / "a.parquet"
    bronze.ecrire_immuable(fichier, bronze.en_parquet(pa.table({"x": [1]})))
    with pytest.raises(FileExistsError):
        bronze.ecrire_immuable(fichier, bronze.en_parquet(pa.table({"x": [2]})))
    assert pq.read_table(fichier).column("x").to_pylist() == [1]
