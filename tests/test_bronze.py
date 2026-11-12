import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import dagster as dg
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import requests

from gazette import reactiver
from gazette.bronze import assets as bronze
from gazette.bronze.etat import EtatFlux, ecrire_etat, lire_etat
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


def test_erreurs_http_404_permanent_503_transitoire():
    assert not bronze.est_transitoire(erreur_http(404))
    assert bronze.est_transitoire(erreur_http(503))
    assert bronze.est_transitoire(requests.ConnectionError("injoignable"))

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
    assert all(check.passed for check in resultat.get_asset_check_evaluations())

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


def collecter(
    source: Source, partition: str, raise_on_error: bool = True
) -> dg.ExecuteInProcessResult:
    return dg.materialize(
        [bronze.construire_asset_bronze(source)],
        partition_key=partition,
        raise_on_error=raise_on_error,
    )


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
    # la source répond : 0 écrit n'est pas une panne
    assert all(check.passed for check in second.get_asset_check_evaluations())

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


class ServeurLocal:
    """Vrai serveur HTTP local. `statuts[chemin]` : les statuts à renvoyer, dans l'ordre ; une fois
    la liste épuisée, il répond 200 avec le flux RSS. `appels` garde chaque chemin demandé."""

    def __init__(self) -> None:
        self.statuts: dict[str, list[int]] = {}
        self.appels: list[str] = []
        serveur = self

        class Gestionnaire(BaseHTTPRequestHandler):
            def do_GET(self):
                serveur.appels.append(self.path)
                programmes = serveur.statuts.get(self.path, [])
                statut = programmes.pop(0) if programmes else 200
                corps = RSS if statut == 200 else b"erreur"
                self.send_response(statut)
                self.send_header("Content-Type", "application/rss+xml")
                self.send_header("Content-Length", str(len(corps)))
                self.end_headers()
                self.wfile.write(corps)

            def log_message(self, *args):
                pass

        self.http = ThreadingHTTPServer(("127.0.0.1", 0), Gestionnaire)
        self.url = f"http://127.0.0.1:{self.http.server_port}"
        threading.Thread(target=self.http.serve_forever, daemon=True).start()

    def source(self) -> Source:
        return Source("test", "Journal test", "PQN", "National", (Flux("une", f"{self.url}/une"),))


@pytest.fixture
def serveur(tmp_path, monkeypatch):
    monkeypatch.setattr(bronze, "DATA_DIR", tmp_path)
    # Mêmes relances que la prod, sans les secondes d'attente entre deux essais ou deux requêtes.
    monkeypatch.setattr(bronze, "HTTP_BACKOFF_S", 0)
    monkeypatch.setattr(
        bronze,
        "relances",
        dg.RetryPolicy(max_retries=3, delay=0.01, backoff=dg.Backoff.EXPONENTIAL),
    )
    serveur = ServeurLocal()
    yield serveur
    serveur.http.shutdown()
    serveur.http.server_close()


def verdicts(resultat: dg.ExecuteInProcessResult) -> dict[str, bool]:
    return {c.check_name: c.passed for c in resultat.get_asset_check_evaluations()}


def test_source_en_panne_503_reessayee_par_la_session_http(serveur, tmp_path):
    serveur.statuts["/une"] = [503, 503]

    resultat = collecter(serveur.source(), "2026-10-19")

    assert resultat.success
    assert serveur.appels == ["/une"] * 3  # deux 503, puis le 200, dans un seul essai Dagster
    assert compteurs(resultat, "test") == {"nb_recus": 2, "nb_ecrits": 2, "nb_ignores": 0}
    assert len(articles_ecrits(tmp_path)) == 2
    assert lire_etat(tmp_path, "test", "une") == EtatFlux(
        actif=True,
        echecs_consecutifs=0,
        derniere_reussite=lire_etat(tmp_path, "test", "une").derniere_reussite,
    )


def test_source_en_panne_503_reessayee_par_dagster_quand_la_session_abandonne(serveur, tmp_path):
    # La session HTTP abandonne après ses 4 requêtes : c'est la RetryPolicy de Dagster qui reprend.
    serveur.statuts["/une"] = [503] * (bronze.HTTP_RETRIES + 1)

    resultat = collecter(serveur.source(), "2026-10-19")

    assert resultat.success
    assert len(serveur.appels) == bronze.HTTP_RETRIES + 2
    assert len(articles_ecrits(tmp_path)) == 2
    assert lire_etat(tmp_path, "test", "une").echecs_consecutifs == 0


def test_source_en_panne_503_sans_fin_epuise_les_relances_sans_sommeil(serveur, tmp_path):
    serveur.statuts["/une"] = [503] * 100

    resultat = collecter(serveur.source(), "2026-10-19", raise_on_error=False)

    assert not resultat.success
    # (requête initiale + relances HTTP) x (essai initial + 3 relances Dagster)
    assert len(serveur.appels) == (bronze.HTTP_RETRIES + 1) * 4
    assert articles_ecrits(tmp_path) == []
    # Une panne passagère de l'éditeur ne met pas le flux en sommeil.
    assert lire_etat(tmp_path, "test", "une").actif
    assert lire_etat(tmp_path, "test", "une").echecs_consecutifs == 0


def test_source_en_panne_404_trois_echecs_de_suite_mettent_le_flux_en_sommeil(serveur, tmp_path):
    serveur.statuts["/une"] = [404] * 20

    for jour, echecs in (("2026-10-19", 1), ("2026-10-20", 2), ("2026-10-21", 3)):
        resultat = collecter(serveur.source(), jour, raise_on_error=False)
        assert not resultat.success
        etat = lire_etat(tmp_path, "test", "une")
        assert etat.echecs_consecutifs == echecs
        assert "404" in etat.derniere_erreur
    assert len(serveur.appels) == 3  # un 404 n'est pas relancé : un seul essai par run
    assert not lire_etat(tmp_path, "test", "une").actif

    # Le run suivant ne contacte plus le flux, et le dit : le check sources_actives échoue.
    suivant = collecter(serveur.source(), "2026-10-21")
    assert suivant.success
    assert len(serveur.appels) == 3
    assert verdicts(suivant)["sources_actives"] is False
    materialisation = suivant.asset_materializations_for_node("bronze__test")[0]
    assert materialisation.metadata["flux_en_sommeil"].value == ["une"]


def test_une_reussite_remet_le_compteur_d_echecs_a_zero(serveur, tmp_path):
    serveur.statuts["/une"] = [404, 404]

    collecter(serveur.source(), "2026-10-19", raise_on_error=False)
    collecter(serveur.source(), "2026-10-20", raise_on_error=False)
    assert lire_etat(tmp_path, "test", "une").echecs_consecutifs == 2

    assert collecter(serveur.source(), "2026-10-21").success
    etat = lire_etat(tmp_path, "test", "une")
    assert (etat.actif, etat.echecs_consecutifs, etat.derniere_erreur) == (True, 0, None)
    assert etat.derniere_reussite is not None


def test_reactiver_remet_le_flux_en_service(serveur, tmp_path):
    ecrire_etat(
        tmp_path, "test", "une", EtatFlux(actif=False, echecs_consecutifs=3, derniere_erreur="404")
    )
    assert reactiver.main(["inconnue"], racine=tmp_path) == 1  # source sans état : rien à faire

    assert reactiver.main(["test"], racine=tmp_path) == 0

    assert lire_etat(tmp_path, "test", "une").actif
    assert lire_etat(tmp_path, "test", "une").echecs_consecutifs == 0
    assert collecter(serveur.source(), "2026-10-19").success
    assert serveur.appels == ["/une"]  # le flux est de nouveau contacté


@pytest.mark.parametrize("statut", [500, 502, 503, 504, 408, 429])
def test_erreur_http_transitoire(statut):
    reponse = requests.Response()
    reponse.status_code = statut
    assert bronze.est_transitoire(requests.HTTPError(response=reponse))


@pytest.mark.parametrize("statut", [400, 401, 403, 404, 410])
def test_erreur_http_permanente(statut):
    reponse = requests.Response()
    reponse.status_code = statut
    assert not bronze.est_transitoire(requests.HTTPError(response=reponse))


def test_erreurs_reseau_transitoires_et_url_invalide_permanente():
    assert bronze.est_transitoire(requests.ConnectionError("injoignable"))
    assert bronze.est_transitoire(requests.Timeout("trop lent"))
    assert not bronze.est_transitoire(requests.exceptions.InvalidURL("pas une url"))


def test_bronze_ne_reecrit_jamais_un_fichier(tmp_path):
    fichier = tmp_path / "a.parquet"
    bronze.ecrire_immuable(fichier, bronze.en_parquet(pa.table({"x": [1]})))
    with pytest.raises(FileExistsError):
        bronze.ecrire_immuable(fichier, bronze.en_parquet(pa.table({"x": [2]})))
    assert pq.read_table(fichier).column("x").to_pylist() == [1]
