"""Couche bronze : réponses RSS brutes, un fichier Parquet par flux collecté, jamais réécrit.

Un asset par journal, pour que l'échec d'une source n'empêche pas les autres
de tourner et que chaque source ait ses propres relances et son propre check.

Arborescence produite :
    $GAZETTE_DATA_DIR/bronze/date=AAAA-MM-JJ/<slug>/<flux>_<horodatage UTC>.parquet
    $GAZETTE_DATA_DIR/bronze/date=AAAA-MM-JJ/<slug>/<flux>_<horodatage UTC>.json     (mêmes articles, lisibles)

Chaque fichier contient une ligne par article, champs RSS tels que le flux les donne
(aucun nettoyage : c'est le rôle de silver). La réponse XML complète et le contexte HTTP
sont conservés dans les métadonnées du fichier (clés gazette.xml_brut et gazette.http).

Pas de doublon : un article déjà écrit pour (source, lien) n'est pas réécrit, quel que soit le
jour ou le flux où il avait été collecté (l'id RSS sert de clé quand il n'a pas de lien). Les
clés connues sont relues dans les Parquet existants. Un flux sans aucun article nouveau
n'écrit aucun fichier : les articles ignorés sont comptés dans les métadonnées du run.
"""

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import dagster as dg
import feedparser
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from gazette.bronze.sources import Flux, Source, charger_sources
from gazette.partitions import partitions_quotidiennes

DATA_DIR = Path(os.environ.get("GAZETTE_DATA_DIR", "data"))
USER_AGENT = "GazetteBot/0.1 (projet etudiant M2Ops Efrei; lecture des flux RSS publics)"
TIMEOUT_S = 30
HTTP_RETRIES = 3
CHECK_AU_MOINS_UN_ARTICLE = "au_moins_un_article"

relances = dg.RetryPolicy(max_retries=3, delay=30, backoff=dg.Backoff.EXPONENTIAL)

SCHEMA_BRONZE = pa.schema(
    [
        ("source", pa.string()),
        ("flux", pa.string()),
        ("url_flux", pa.string()),
        ("recu_le", pa.timestamp("us", tz="UTC")),
        ("rang", pa.int32()),
        ("id", pa.string()),
        ("title", pa.string()),
        ("link", pa.string()),
        ("summary", pa.string()),
        ("published", pa.string()),
        ("updated", pa.string()),
        ("author", pa.string()),
        ("tags", pa.list_(pa.string())),
    ]
)


def ecrire_immuable(chemin: Path, contenu: bytes) -> None:
    """Écrit un fichier qui ne doit jamais exister déjà : le bronze ne se réécrit pas."""
    chemin.parent.mkdir(parents=True, exist_ok=True)
    with open(chemin, "xb") as f:
        f.write(contenu)


def en_parquet(table: pa.Table) -> bytes:
    tampon = pa.BufferOutputStream()
    pq.write_table(table, tampon, compression="zstd")
    return tampon.getvalue().to_pybytes()


def en_json(table: pa.Table) -> bytes:
    """Les mêmes articles que le Parquet, lisibles dans un éditeur (le XML brut n'y est pas)."""
    return json.dumps(table.to_pylist(), ensure_ascii=False, indent=2, default=str).encode("utf-8")


def cle_article(entree: dict) -> str | None:
    """Clé naturelle d'un article au sein d'une source : son lien, à défaut son id RSS."""
    return entree.get("link") or entree.get("id")


def cles_deja_ecrites(slug: str) -> set[str]:
    """Clés des articles déjà présents dans le bronze de cette source, tous jours confondus."""
    cles = set()
    for fichier in (DATA_DIR / "bronze").glob(f"date=*/{slug}/*.parquet"):
        for ligne in pq.read_table(fichier, columns=["link", "id"]).to_pylist():
            cle = ligne["link"] or ligne["id"]
            if cle:
                cles.add(cle)
    return cles


def articles_nouveaux(entrees: list, cles_connues: set[str]) -> list[tuple[int, dict]]:
    """Les (rang, article) jamais écrits ; leurs clés rejoignent `cles_connues`.

    Le rang reste celui de l'article dans le flux. Un article sans lien ni id ne peut pas
    être reconnu : il est toujours gardé.
    """
    nouveaux = []
    for rang, entree in enumerate(entrees):
        cle = cle_article(entree)
        if cle is not None:
            if cle in cles_connues:
                continue
            cles_connues.add(cle)
        nouveaux.append((rang, entree))
    return nouveaux


def table_bronze(
    source: str,
    flux: Flux,
    recu_le: datetime,
    xml: bytes,
    contexte: dict,
    articles: list[tuple[int, dict]],
) -> pa.Table:
    lignes = [
        {
            "source": source,
            "flux": flux.nom,
            "url_flux": flux.url,
            "recu_le": recu_le,
            "rang": rang,
            "id": entree.get("id"),
            "title": entree.get("title"),
            "link": entree.get("link"),
            "summary": entree.get("summary"),
            "published": entree.get("published"),
            "updated": entree.get("updated"),
            "author": entree.get("author"),
            "tags": [t.get("term") for t in entree.get("tags", [])],
        }
        for rang, entree in articles
    ]
    schema = SCHEMA_BRONZE.with_metadata(
        {
            "gazette.xml_brut": xml,
            "gazette.http": json.dumps(contexte, ensure_ascii=False).encode("utf-8"),
        }
    )
    return pa.Table.from_pylist(lignes, schema=schema)


def creer_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=HTTP_RETRIES,
        connect=HTTP_RETRIES,
        read=HTTP_RETRIES,
        status=HTTP_RETRIES,
        backoff_factor=1,
        status_forcelist=(503,),
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def erreur_reessayable(erreur: requests.RequestException) -> bool:
    """Un 404 est définitif ; les erreurs réseau et autres statuts restent retentables."""
    return not (
        isinstance(erreur, requests.HTTPError)
        and erreur.response is not None
        and erreur.response.status_code == 404
    )


def collecter_flux(
    session: requests.Session, source: str, flux: Flux, dossier: Path, cles_connues: set[str]
) -> dict:
    reponse = session.get(flux.url, timeout=TIMEOUT_S)
    reponse.raise_for_status()
    recu_le = datetime.now(timezone.utc)

    contexte = {
        "url": flux.url,
        "url_finale": reponse.url,
        "statut_http": reponse.status_code,
        "content_type": reponse.headers.get("Content-Type"),
        "etag": reponse.headers.get("ETag"),
        "last_modified": reponse.headers.get("Last-Modified"),
        "recu_le": recu_le.isoformat(),
        "octets": len(reponse.content),
    }
    entrees = feedparser.parse(reponse.content).entries
    articles = articles_nouveaux(entrees, cles_connues)
    compteurs = {
        "nb_recus": len(entrees),
        "nb_ecrits": len(articles),
        "nb_ignores": len(entrees) - len(articles),
    }
    if not articles:
        return {**contexte, **compteurs, "fichier": None}

    table = table_bronze(source, flux, recu_le, reponse.content, contexte, articles)
    base = dossier / f"{flux.nom}_{recu_le:%Y%m%dT%H%M%S%fZ}"
    fichier = Path(f"{base}.parquet")
    ecrire_immuable(fichier, en_parquet(table))
    ecrire_immuable(Path(f"{base}.json"), en_json(table))
    return {**contexte, **compteurs, "fichier": str(fichier)}


def construire_asset_bronze(source: Source) -> dg.AssetsDefinition:
    cle = dg.AssetKey(["bronze", source.slug])

    @dg.asset(
        key=cle,
        group_name="bronze",
        partitions_def=partitions_quotidiennes,
        retry_policy=relances,
        description=f"Flux RSS bruts de {source.nom} ({source.type}, {source.region}), Parquet immuable.",
        metadata={"journal": source.nom, "type": source.type, "region": source.region},
        check_specs=[
            dg.AssetCheckSpec(
                CHECK_AU_MOINS_UN_ARTICLE,
                asset=cle,
                description="La source a renvoyé au moins un article (même déjà collecté).",
            )
        ],
    )
    def _bronze(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        date = context.partition_key
        if date != datetime.now(ZoneInfo("Europe/Paris")).date().isoformat():
            context.log.warning(
                f"Partition {date} rejouée un autre jour : un flux RSS ne donne que ses "
                "articles du moment, les fichiers collectés reflètent donc le flux actuel."
            )

        dossier = DATA_DIR / "bronze" / f"date={date}" / source.slug
        debut = time.monotonic()
        cles_connues = cles_deja_ecrites(source.slug)
        collectes, echecs = [], []
        erreurs_reessayables = False
        with creer_session() as session:
            session.headers["User-Agent"] = USER_AGENT
            for flux in source.flux:
                try:
                    collectes.append(
                        collecter_flux(session, source.slug, flux, dossier, cles_connues)
                    )
                except requests.RequestException as erreur:
                    context.log.warning(f"Flux {flux.nom} ({flux.url}) en échec : {erreur}")
                    echecs.append(f"{flux.nom} : {erreur}")
                    erreurs_reessayables |= erreur_reessayable(erreur)
        duree_s = round(time.monotonic() - debut, 2)

        if not collectes:
            raise dg.Failure(
                description=f"Aucun flux de {source.nom} n'a répondu.",
                metadata={"flux_en_echec": echecs, "duree_s": duree_s},
                allow_retries=erreurs_reessayables,
            )

        nb_recus = sum(c["nb_recus"] for c in collectes)
        nb_ecrits = sum(c["nb_ecrits"] for c in collectes)
        nb_ignores = sum(c["nb_ignores"] for c in collectes)
        context.log.info(
            f"{source.nom} : {nb_recus} reçus, {nb_ecrits} écrits, {nb_ignores} ignorés "
            f"en {duree_s} s"
        )
        return dg.MaterializeResult(
            metadata={
                "nb_recus": nb_recus,
                "nb_ecrits": nb_ecrits,
                "nb_ignores": nb_ignores,
                "nb_flux_ok": len(collectes),
                "nb_flux_en_echec": len(echecs),
                "flux_en_echec": echecs,
                "duree_s": duree_s,
                "octets": sum(c["octets"] for c in collectes),
                "fichiers": [c["fichier"] for c in collectes if c["fichier"]],
            },
            check_results=[
                dg.AssetCheckResult(
                    check_name=CHECK_AU_MOINS_UN_ARTICLE,
                    passed=nb_recus > 0,
                    severity=dg.AssetCheckSeverity.ERROR,
                    metadata={"nb_recus": nb_recus},
                )
            ],
        )

    return _bronze


assets_bronze = [construire_asset_bronze(source) for source in charger_sources()]
