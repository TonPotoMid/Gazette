"""Couche bronze : réponses RSS brutes, un fichier Parquet par flux collecté, jamais réécrit.

Un asset par journal, pour que l'échec d'une source n'empêche pas les autres
de tourner et que chaque source ait ses propres relances et son propre check.

Arborescence produite :
    $GAZETTE_DATA_DIR/bronze/date=AAAA-MM-JJ/<slug>/<flux>_<horodatage UTC>.parquet
    $GAZETTE_DATA_DIR/bronze/date=AAAA-MM-JJ/<slug>/<flux>_<horodatage UTC>.json (idem, lisible)

Chaque fichier contient une ligne par article, champs RSS tels que le flux les donne
(aucun nettoyage : c'est le rôle de silver). La réponse XML complète et le contexte HTTP
sont conservés dans les métadonnées du fichier (clés gazette.xml_brut et gazette.http).
"""

import json
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import dagster as dg
import feedparser
import pyarrow as pa
import pyarrow.parquet as pq
import requests

from gazette.bronze.sources import Flux, Source, charger_sources
from gazette.partitions import partitions_quotidiennes

DATA_DIR = Path(os.environ.get("GAZETTE_DATA_DIR", "data"))
USER_AGENT = "GazetteBot/0.1 (projet etudiant M2Ops Efrei; lecture des flux RSS publics)"
TIMEOUT_S = 30
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


def table_bronze(
    source: str, flux: Flux, recu_le: datetime, xml: bytes, contexte: dict
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
        for rang, entree in enumerate(feedparser.parse(xml).entries)
    ]
    schema = SCHEMA_BRONZE.with_metadata(
        {
            "gazette.xml_brut": xml,
            "gazette.http": json.dumps(contexte, ensure_ascii=False).encode("utf-8"),
        }
    )
    return pa.Table.from_pylist(lignes, schema=schema)


def collecter_flux(session: requests.Session, source: str, flux: Flux, dossier: Path) -> dict:
    reponse = session.get(flux.url, timeout=TIMEOUT_S)
    reponse.raise_for_status()
    recu_le = datetime.now(UTC)

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
    table = table_bronze(source, flux, recu_le, reponse.content, contexte)
    base = dossier / f"{flux.nom}_{recu_le:%Y%m%dT%H%M%S%fZ}"
    fichier = Path(f"{base}.parquet")
    ecrire_immuable(fichier, en_parquet(table))
    ecrire_immuable(Path(f"{base}.json"), en_json(table))
    return {**contexte, "nb_articles": table.num_rows, "fichier": str(fichier)}


def construire_asset_bronze(source: Source) -> dg.AssetsDefinition:
    cle = dg.AssetKey(["bronze", source.slug])

    @dg.asset(
        key=cle,
        group_name="bronze",
        partitions_def=partitions_quotidiennes,
        retry_policy=relances,
        description=(
            f"Flux RSS bruts de {source.nom} ({source.type}, {source.region}), Parquet immuable."
        ),
        metadata={"journal": source.nom, "type": source.type, "region": source.region},
        check_specs=[
            dg.AssetCheckSpec(
                CHECK_AU_MOINS_UN_ARTICLE,
                asset=cle,
                description="La source a renvoyé au moins un article.",
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
        collectes, echecs = [], []
        with requests.Session() as session:
            session.headers["User-Agent"] = USER_AGENT
            for flux in source.flux:
                try:
                    collectes.append(collecter_flux(session, source.slug, flux, dossier))
                except requests.RequestException as erreur:
                    context.log.warning(f"Flux {flux.nom} ({flux.url}) en échec : {erreur}")
                    echecs.append(f"{flux.nom} : {erreur}")
        duree_s = round(time.monotonic() - debut, 2)

        if not collectes:
            raise dg.Failure(
                description=f"Aucun flux de {source.nom} n'a répondu.",
                metadata={"flux_en_echec": echecs, "duree_s": duree_s},
            )

        nb_articles = sum(c["nb_articles"] for c in collectes)
        context.log.info(f"{source.nom} : {nb_articles} articles en {duree_s} s")
        return dg.MaterializeResult(
            metadata={
                "nb_articles": nb_articles,
                "nb_flux_ok": len(collectes),
                "nb_flux_en_echec": len(echecs),
                "flux_en_echec": echecs,
                "duree_s": duree_s,
                "octets": sum(c["octets"] for c in collectes),
                "fichiers": [c["fichier"] for c in collectes],
            },
            check_results=[
                dg.AssetCheckResult(
                    check_name=CHECK_AU_MOINS_UN_ARTICLE,
                    passed=nb_articles > 0,
                    severity=dg.AssetCheckSeverity.ERROR,
                    metadata={"nb_articles": nb_articles},
                )
            ],
        )

    return _bronze


assets_bronze = [construire_asset_bronze(source) for source in charger_sources()]
